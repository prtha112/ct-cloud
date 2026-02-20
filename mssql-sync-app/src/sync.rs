use sqlx::{Pool, Mssql, Row, Column, TypeInfo};
use sqlx::mssql::MssqlRow;
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

                // 1. Initialize enabled flag in Redis if it doesn't exist
                if let Err(e) = state::init_table_enabled(&r_client, &table_name).await {
                    log::error!("Failed to initialize enabled flag for {}: {}", table_name, e);
                    continue;
                }

                // 2. Check if table synchronization is enabled
                let is_enabled = state::is_table_enabled(&r_client, &table_name).await.unwrap_or(false);
                if !is_enabled {
                    info!("Sync skipped for table: {} (mssql_sync:enabled:{} is not true)", table_name, table_name);
                    continue;
                }
                
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

    // Check for Force Full Load Flag
    let force_full_load = state::should_force_full_load(redis_client, table_name).await.unwrap_or(false);

    if !force_full_load && current_version <= last_version {
        return Ok(());
    }

    // Prepare Column List for SELECT (needed for both Full Load and Incremental)
    // CAST decimal/numeric to avoid NumericN panic
    let cols_query = format!(
        "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{}' ORDER BY ORDINAL_POSITION",
        table_name
    );
    let columns: Vec<(String, String)> = sqlx::query(&cols_query)
        .map(|row: MssqlRow| (row.get("COLUMN_NAME"), row.get("DATA_TYPE")))
        .fetch_all(primary_pool)
        .await?;
        
    let select_list = columns.iter().map(|(name, dtype)| {
        if ["decimal", "numeric", "money", "smallmoney", "float", "real", "tinyint", "smallint", "int", "bigint", "bit"].contains(&dtype.to_lowercase().as_str()) {
             // Cast to string to safely transport through sqlx (avoid NumericN panic and SQLx strict decoding panics)
             // VARCHAR(100) fits any number representation and avoids sqlx LOB stream parsing bugs
             format!("CAST([{}] AS VARCHAR(100)) AS [{}]", name, name) 
        } else if ["datetime", "datetime2", "date", "time", "smalldatetime", "datetimeoffset"].contains(&dtype.to_lowercase().as_str()) {
             // Cast to string to safely transport through sqlx (avoid DateTimeN panic)
             format!("CONVERT(VARCHAR(100), [{}], 126) AS [{}]", name, name)
        } else if ["text"].contains(&dtype.to_lowercase().as_str()) {
             // Cast deprecated text to VARCHAR(8000) to avoid unsupported data type Text panic
             // and avoid VARCHAR(MAX) which triggers sqlx LOB stream parsing bugs
             format!("CAST([{}] AS VARCHAR(8000)) AS [{}]", name, name)
        } else if ["ntext"].contains(&dtype.to_lowercase().as_str()) {
             // Cast deprecated ntext to NVARCHAR(4000) to avoid unsupported data type NText panic
             // and avoid NVARCHAR(MAX) which triggers sqlx LOB stream parsing bugs
             format!("CAST([{}] AS NVARCHAR(4000)) AS [{}]", name, name)
        } else {
             format!("[{}]", name)
        }
    }).collect::<Vec<_>>().join(", ");


    // --- IDENTITY CHECK ---
    let identity_check_query = format!(
        "SELECT OBJECTPROPERTY(OBJECT_ID('{}'), 'TableHasIdentity')",
        table_name
    );
    let has_identity_val: Option<i32> = sqlx::query_scalar(&identity_check_query)
        .fetch_optional(primary_pool)
        .await?;
    let has_identity = has_identity_val.unwrap_or(0) == 1;

    // --- FORCE FULL LOAD LOGIC ---
    if force_full_load {
        info!("FORCE FULL LOAD detected for table: {}", table_name);

        // 1. Truncate Replica
        let truncate_sql = format!("TRUNCATE TABLE [{}]", table_name);
        sqlx::query(&truncate_sql).execute(replica_pool).await?;
        
        // 2. Initial Full Load (SELECT * FROM Primary)
        let full_query = format!("SELECT {} FROM [{}]", select_list, table_name);
        let rows = sqlx::query(&full_query).fetch_all(primary_pool).await?; 
        
        info!("Force Load: Inserting {} rows into {}", rows.len(), table_name);
        
        for row in rows {
            // Build INSERT
             let mut cols = Vec::new();
             let mut placeholders = Vec::new();

             for col in row.columns() {
                 let name = col.name();
                 cols.push(format!("[{}]", name));
                 placeholders.push(format!("@p{}", cols.len()));
             }

             let insert_sql = if has_identity {
                 format!(
                     "SET IDENTITY_INSERT [{}] ON; INSERT INTO [{}] ({}) VALUES ({}); SET IDENTITY_INSERT [{}] OFF;",
                     table_name, table_name, cols.join(", "), placeholders.join(", "), table_name
                 )
             } else {
                 format!(
                     "INSERT INTO [{}] ({}) VALUES ({})",
                     table_name, cols.join(", "), placeholders.join(", ")
                 )
             };

             let mut query_builder = sqlx::query(&insert_sql);
             
             let mut bound_values_str = String::new();
             // Bind values (All fields are dynamically converted to String via select_list to avoid driver decode strictness)
             for col in row.columns() {
                  let v: Option<String> = row.try_get(col.ordinal()).ok();
                  bound_values_str.push_str(&format!("{}={:?}, ", col.name(), v));
                  query_builder = query_builder.bind(v);
             }
             log::info!("Full Load Insert -> {}", bound_values_str);
             
             query_builder.execute(replica_pool).await?;
        }
        
        // 3. Update Sync Version
        state::set_last_version(redis_client, table_name, current_version).await?;
        
        // 4. Clear Flag
        state::clear_force_full_load(redis_client, table_name).await?;
        
        info!("Force Full Load complete for table: {}", table_name);
        return Ok(());
    }
    // -----------------------------

    info!("Syncing {} from v{} to v{}", table_name, last_version, current_version);

    // 4. Get Changes (Incremental Logic)
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
            CAST(ct.[{}] AS NVARCHAR(4000)) AS pk_val_str
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
        let pk_val_str: String = change.get("pk_val_str"); 

        match op.as_str() {
            "D" => {
                // Delete in Replica
                let del_sql = format!("DELETE FROM [{}] WHERE [{}] = @p1", table_name, pk_col);
                sqlx::query(&del_sql).bind(&pk_val_str).execute(replica_pool).await?;
            },
            "I" | "U" => {
                // Fetch full row from Primary using safe SELECT list
                let row_query = format!("SELECT {} FROM [{}] WHERE [{}] = @p1", select_list, table_name, pk_col);
                let row_opt = sqlx::query(&row_query).bind(&pk_val_str).fetch_optional(primary_pool).await?;
                
                if let Some(row) = row_opt {
                    // UPSERT into Replica
                    let del_sql = format!("DELETE FROM [{}] WHERE [{}] = @p1", table_name, pk_col);
                    sqlx::query(&del_sql).bind(&pk_val_str).execute(replica_pool).await?;

                    // Build INSERT
                    let mut cols = Vec::new();
                    let mut placeholders = Vec::new();

                    for col in row.columns() {
                        let name = col.name();
                        cols.push(format!("[{}]", name));
                        placeholders.push(format!("@p{}", cols.len()));
                    }

                    let insert_sql = if has_identity {
                        format!(
                            "SET IDENTITY_INSERT [{}] ON; INSERT INTO [{}] ({}) VALUES ({}); SET IDENTITY_INSERT [{}] OFF;",
                            table_name, table_name, cols.join(", "), placeholders.join(", "), table_name
                        )
                    } else {
                        format!(
                            "INSERT INTO [{}] ({}) VALUES ({})",
                            table_name, cols.join(", "), placeholders.join(", ")
                        )
                    };

                    let mut query_builder = sqlx::query(&insert_sql);
                    
                    // Bind values (All fields are dynamically converted to String via select_list to avoid driver decode strictness)
                    for col in row.columns() {
                         let v: Option<String> = row.try_get(col.ordinal()).ok();
                         query_builder = query_builder.bind(v);
                    }
                    
                    query_builder.execute(replica_pool).await?;
                }
            },
            _ => {}
        }
    }

    // Update Redis
    if !changes.is_empty() {
        let last_change_ver: i64 = changes.last().unwrap().get("SYS_CHANGE_VERSION");
        state::set_last_version(redis_client, table_name, last_change_ver).await?;
    } else {
        state::set_last_version(redis_client, table_name, current_version).await?;
    }

    Ok(())
}
