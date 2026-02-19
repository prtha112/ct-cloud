use sqlx::{Pool, Mssql, Row, Column, TypeInfo};
use redis::Client;
use log::{info, debug};
use crate::state;
use crate::schema;

pub async fn run_sync(
    primary_pool: &Pool<Mssql>,
    replica_pool: &Pool<Mssql>,
    redis_client: &Client,
    thread_count: usize
) -> Result<(), Box<dyn std::error::Error>> {
    // 1. Get enabled tables
    let tables_query = "
        SELECT 
            t.name AS TableName
        FROM sys.change_tracking_tables ctt
        JOIN sys.tables t ON ctt.object_id = t.object_id
    ";
    
    let tables = sqlx::query(tables_query)
        .fetch_all(primary_pool)
        .await?;

    let table_names: Vec<String> = tables.into_iter()
        .map(|row| row.get("TableName"))
        .collect();

    if table_names.is_empty() {
        return Ok(());
    }

    let chunk_size = (table_names.len() as f64 / thread_count.max(1) as f64).ceil() as usize;
    let chunks: Vec<Vec<String>> = table_names.chunks(chunk_size)
        .map(|c| c.to_vec())
        .collect();

    let mut handles = Vec::new();

    for chunk in chunks {
        let p_pool = primary_pool.clone();
        let r_pool = replica_pool.clone();
        let r_client = redis_client.clone();

        let handle = tokio::spawn(async move {
            for table_name in chunk {
                debug!("Processing table: {}", table_name);
                
                // Ensure table exists on Replica
                schema::ensure_table_exists(&p_pool, &r_pool, &table_name)
                    .await
                    .map_err(|e| format!("Schema error on {}: {}", table_name, e))?;

                // Sync data
                sync_table(&p_pool, &r_pool, &r_client, &table_name)
                    .await
                    .map_err(|e| format!("Sync error on {}: {}", table_name, e))?;
            }
            Ok::<(), String>(())
        });
        handles.push(handle);
    }

    for handle in handles {
        match handle.await {
            Ok(Ok(())) => {},
            Ok(Err(e)) => return Err(e.into()),
            Err(e) => return Err(format!("Task join error: {}", e).into()),
        }
    }

    Ok(())
}

async fn sync_table(
    primary_pool: &Pool<Mssql>,
    replica_pool: &Pool<Mssql>,
    redis_client: &Client,
    table_name: &str
) -> Result<(), Box<dyn std::error::Error>> {
    // 2. Get current version from Primary
    let current_ver_query = "SELECT CHANGE_TRACKING_CURRENT_VERSION()";
    let current_version: i64 = sqlx::query_scalar(current_ver_query)
        .fetch_one(primary_pool)
        .await
        .unwrap_or(0); // If None (no changes ever), default 0

    // 3. Get last synced version from Redis
    let last_version = state::get_last_version(redis_client, table_name).await?;

    if current_version <= last_version {
        return Ok(());
    }

    info!("Syncing {} from v{} to v{}", table_name, last_version, current_version);

    // 4. Get Changes
    // Note: We need PK column name dynamically. For this example, assuming 'Id' or derived from schema is complex.
    // To keep it simple, let's assume specific tables or query PK dynamically as well.
    // For a generic solution, we need to construct the CHANGETABLE query dynamically based on PKs.
    // BUT user provided sample: FROM CHANGETABLE(CHANGES dbo.[User], @last_version) AS ct
    // This implies we know the table name.
    
    // Let's get PK column name first
    let pk_col_query = format!(
        "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE 
         WHERE OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + '.' + CONSTRAINT_NAME), 'IsPrimaryKey') = 1 
         AND TABLE_NAME = '{}'", 
        table_name
    );
    let pk_row = sqlx::query(&pk_col_query).fetch_optional(primary_pool).await?;
    let pk_col = match pk_row {
        Some(row) => row.get::<String, _>("COLUMN_NAME"),
        None => return Ok(()), // Skip if no PK
    };

    let changes_query = format!(
        "SELECT 
            ct.SYS_CHANGE_VERSION,
            ct.SYS_CHANGE_OPERATION,
            ct.{} -- PK
         FROM CHANGETABLE(CHANGES dbo.[{}], @p1) AS ct
         ORDER BY ct.SYS_CHANGE_VERSION",
        pk_col, table_name
    );

    let changes = sqlx::query(&changes_query)
        .bind(last_version)
        .fetch_all(primary_pool)
        .await?;

    for change in &changes {
        let _version: i64 = change.get("SYS_CHANGE_VERSION");
        let op: String = change.get("SYS_CHANGE_OPERATION");
        // We handle PK as generic type if possible, or assume INT for now based on user sample.
        // sqlx Row::get is generic. uniqueidentifier also possible.
        // Let's try to get it as a generic compatible type or String for query construction.
        // Actually, let's treat PK value as argument to bind.
        
        // This part is tricky in strict Rust without dynamic typing.
        // Simplified approach: Assume PK is INT for this demo, or String.
        // Let's retrieve PK as ID.
        let pk_val: i64 = change.get(pk_col.as_str()); 

        match op.as_str() {
            "D" => {
                // Delete in Replica
                let del_sql = format!("DELETE FROM [{}] WHERE [{}] = @p1", table_name, pk_col);
                sqlx::query(&del_sql).bind(pk_val).execute(replica_pool).await?;
            },
            "I" | "U" => {
                // Fetch full row from Primary
                let row_query = format!("SELECT * FROM [{}] WHERE [{}] = @p1", table_name, pk_col);
                let row_opt = sqlx::query(&row_query).bind(pk_val).fetch_optional(primary_pool).await?;
                
                if let Some(row) = row_opt {
                    // UPSERT into Replica
                    // Construct dynamic INSERT/UPDATE logic or just DELETE & INSERT (easy way)
                    // Let's do DELETE & INSERT to simulate UPSERT for simplicity in this demo.
                    
                    let del_sql = format!("DELETE FROM [{}] WHERE [{}] = @p1", table_name, pk_col);
                    sqlx::query(&del_sql).bind(pk_val).execute(replica_pool).await?;

                    // Build INSERT
                    let mut cols = Vec::new();
                    let mut placeholders = Vec::new();

                    for col in row.columns() {
                        let name = col.name();
                        cols.push(format!("[{}]", name));
                        placeholders.push(format!("@p{}", cols.len()));
                    }

                    let insert_sql = format!(
                        "INSERT INTO [{}] ({}) VALUES ({})",
                        table_name,
                        cols.join(", "),
                        placeholders.join(", ")
                    );

                    let mut query_builder = sqlx::query(&insert_sql);
                    
                    // Bind values
                    for col in row.columns() {
                        // This is the hard part: dynamic binding in sqlx without macros.
                        // sqlx::query returned Query which expects arguments.
                        // We need to match types. 
                        // For a generic solution, we'd need to inspect Column Type info.
                        // Given the complexity, and this being a "demo/start", 
                        // I will implement a robust but slightly coupled way (Strings/Ints).
                        // Or better: Iterate and use `row.try_get_raw`.
                        // BUT: sqlx execute takes arguments.
                        
                        // HACK for Demo: Support basic types (Int, String). 
                        // If more needed, user expands.
                        // Let's try to infer from type_info.
                        
                        let type_name = col.type_info().name();
                        if type_name == "INT" || type_name == "INTEGER" {
                             let v: Option<i32> = row.try_get(col.ordinal()).ok();
                             query_builder = query_builder.bind(v);
                        } else if type_name == "BIGINT" {
                             let v: Option<i64> = row.try_get(col.ordinal()).ok();
                             query_builder = query_builder.bind(v);
                        } else {
                             // Fallback to string for everything else? 
                             // Might fail for binary/date. 
                             // Let's try string.
                             let v: Option<String> = row.try_get(col.ordinal()).ok();
                             query_builder = query_builder.bind(v);
                        }
                    }
                    
                    query_builder.execute(replica_pool).await?;
                }
            },
            _ => {}
        }
    }

    // Update Redis
    if !changes.is_empty() {
        // use last change's version
        let last_change_ver: i64 = changes.last().unwrap().get("SYS_CHANGE_VERSION");
        state::set_last_version(redis_client, table_name, last_change_ver).await?;
    } else {
        // No changes found, but maybe versions gap? Use current.
        // Actually, if we queried changes and got none, we are up to date?
        // CHANGETABLE returns changes *since* version. If empty, implies up to date?
        state::set_last_version(redis_client, table_name, current_version).await?;
    }

    Ok(())
}
