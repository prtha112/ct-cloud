use sqlx::{Pool, Mssql, Row, Column};
use sqlx::mssql::MssqlRow;
use redis::Client;
use std::time::{SystemTime, UNIX_EPOCH};
use log::{info, debug};
use crate::state;
use crate::schema;

use tokio_util::sync::CancellationToken;

pub async fn run_single_table_sync(
    primary_pool: &Pool<Mssql>,
    replica_pool: &Pool<Mssql>,
    redis_client: &Client,
    table_name: &str,
    cancel_token: CancellationToken
) -> Result<(), Box<dyn std::error::Error>> {
    debug!("Processing table: {}", table_name);

    // 1. Initialize enabled flag in Redis if it doesn't exist
    if let Err(e) = state::init_table_enabled(redis_client, table_name).await {
        log::error!("Failed to initialize enabled flag for {}: {}", table_name, e);
        return Ok(());
    }
    
    // Initialize force full load flag in Redis if it doesn't exist
    if let Err(e) = state::init_force_full_load(redis_client, table_name).await {
        log::error!("Failed to initialize force full load flag for {}: {}", table_name, e);
        return Ok(());
    }

    // 2. Check if table synchronization is enabled
    let is_enabled = state::is_table_enabled(redis_client, table_name).await.unwrap_or(false);
    if !is_enabled {
        info!("Sync skipped for table: {} (mssql_sync:enabled:{} is not true)", table_name, table_name);
        return Ok(());
    }
    
    // Ensure table exists on Replica
    schema::ensure_table_exists(primary_pool, replica_pool, table_name)
        .await
        .map_err(|e| format!("Schema error on {}: {}", table_name, e))?;

    // Sync data
    sync_table(primary_pool, replica_pool, redis_client, table_name, cancel_token)
        .await
        .map_err(|e| format!("Sync error on {}: {}", table_name, e))?;

    Ok(())
}

async fn sync_table(
    primary_pool: &Pool<Mssql>,
    replica_pool: &Pool<Mssql>,
    redis_client: &Client,
    table_name: &str,
    cancel_token: CancellationToken
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

    // Get Total Table Count
    let total_count_query = format!("SELECT CAST(COUNT_BIG(*) AS BIGINT) FROM [{}]", table_name);
    let total_records: i64 = sqlx::query_scalar(&total_count_query).fetch_one(primary_pool).await.unwrap_or(0);
    
    // Track execution startup time accurately from thread allocation
    let started_at = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis();

    if !force_full_load && current_version <= last_version {
        // We are already fully synced
        if let Err(e) = state::set_sync_progress(redis_client, table_name, total_records, total_records, started_at).await {
             log::warn!("Failed to store sync progress: {}", e);
        }
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
    
    // Fallback: Also check replica just in case
    let replica_has_identity: Option<i32> = sqlx::query_scalar(&identity_check_query)
        .fetch_optional(replica_pool)
        .await?;
    let r_has_identity = replica_has_identity.unwrap_or(0) == 1;
    
    let has_identity = has_identity || r_has_identity;

    // --- FORCE FULL LOAD LOGIC ---
    if force_full_load {
        info!("FORCE FULL LOAD detected for table: {}", table_name);

        // 1. Truncate Replica
        let truncate_sql = format!("TRUNCATE TABLE [{}]", table_name);
        sqlx::query(&truncate_sql).execute(replica_pool).await?;
        
        // Find column for ORDER BY (required for OFFSET)
        let pk_col_query = format!(
            "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE 
             WHERE OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + '.' + CONSTRAINT_NAME), 'IsPrimaryKey') = 1 
             AND TABLE_NAME = '{}'", 
            table_name
        );
        let pk_row = sqlx::query(&pk_col_query).fetch_optional(primary_pool).await?;
        let order_col = match pk_row {
            Some(row) => row.get::<String, _>("COLUMN_NAME"),
            None => columns[0].0.clone(), // Fallback to first column
        };

        // 2. Chunked Full Load
        let chunk_size = 5000;
        let mut offset = 0;
        let mut total_inserted = 0;
        
        loop {
            if cancel_token.is_cancelled() {
                info!("Force load cancelled for {}; saving progress and aborting loop.", table_name);
                break;
            }
            let full_query = format!(
                "SELECT {} FROM [{}] ORDER BY [{}] OFFSET {} ROWS FETCH NEXT {} ROWS ONLY", 
                select_list, table_name, order_col, offset, chunk_size
            );
            
            let rows = sqlx::query(&full_query).fetch_all(primary_pool).await?; 
            let row_count = rows.len();
            
            if row_count == 0 {
                break;
            }
            
            // We use a Transaction to group thousands of single-row inserts for speed
            // This avoids the 'os error 104' (connection reset by peer) caused by massive query strings
            let mut tx = replica_pool.begin().await?;
            
            // Reusable string components for the query
            let mut cols = Vec::new();
            let mut placeholders = Vec::new();
            for col in rows[0].columns() {
                cols.push(format!("[{}]", col.name()));
                placeholders.push(format!("@p{}", cols.len()));
            }
            
            let insert_sql = if has_identity {
                format!(
                    "SET IDENTITY_INSERT [{}] ON; INSERT INTO [{}] ({}) VALUES ({});",
                     table_name, table_name, cols.join(", "), placeholders.join(", ")
                )
            } else {
                format!(
                    "INSERT INTO [{}] ({}) VALUES ({});",
                     table_name, cols.join(", "), placeholders.join(", ")
                )
            };
            
            for row in rows {
                let mut query_builder = sqlx::query(&insert_sql);
                 
                for col in row.columns() {
                     let v: Option<String> = row.try_get(col.ordinal()).ok();
                     query_builder = query_builder.bind(v);
                }
                 
                if let Err(e) = query_builder.execute(&mut *tx).await {
                    log::error!("Tx Insert Failed: {}", e);
                    tx.rollback().await?;
                    return Err(Box::new(e));
                }
            }
            
            if has_identity {
                 let disable_identity = format!("SET IDENTITY_INSERT [{}] OFF;", table_name);
                 let _ = sqlx::query(&disable_identity).execute(&mut *tx).await;
            }
            
            tx.commit().await?;
            
            total_inserted += row_count as i64;
            info!("Force Load Chunk: Table {} - Inserted {}/{} total rows", table_name, total_inserted, total_records);
            
            // Push Progress tracking to Redis!
            if let Err(e) = state::set_sync_progress(redis_client, table_name, total_inserted, total_records, started_at).await {
                log::warn!("Failed to set force-load sync progress: {}", e);
            }

            offset += chunk_size;
            
            if row_count < chunk_size {
                break;
            }
        }
        
        // 3. Update Sync Version
        state::set_last_version(redis_client, table_name, current_version).await?;
        
        // 4. Clear Flag
        state::clear_force_full_load(redis_client, table_name).await?;
        
        info!("Force Full Load complete for table: {} (Total: {})", table_name, total_inserted);
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

    info!("Fetching CHANGETABLE for {}...", table_name);
    let changes = sqlx::query(&changes_query)
        .bind(last_version)
        .fetch_all(primary_pool)
        .await?;

    let mut delete_pks = std::collections::HashSet::new();
    let mut upsert_pks = std::collections::HashSet::new();

    for change in &changes {
        let op: String = change.get("SYS_CHANGE_OPERATION");
        let pk_val_str: String = change.get("pk_val_str"); 

        // Safely escape single quotes for the IN clause
        let safe_pk = pk_val_str.replace("'", "''");

        match op.as_str() {
            "D" => {
                delete_pks.insert(safe_pk.clone());
                upsert_pks.remove(&safe_pk);
            },
            "I" | "U" => {
                upsert_pks.insert(safe_pk.clone());
                delete_pks.remove(&safe_pk);
            },
            _ => {}
        }
    }

    let delete_pks: Vec<_> = delete_pks.into_iter().collect();
    let upsert_pks: Vec<_> = upsert_pks.into_iter().collect();

    // Perform Bulk Deletes
    for chunk in delete_pks.chunks(100) {
        if cancel_token.is_cancelled() {
            info!("Incremental sync cancelled for {}; aborting delete loop.", table_name);
            break;
        }
        let in_clause = chunk.iter().map(|k| format!("'{}'", k)).collect::<Vec<_>>().join(",");
        if !in_clause.is_empty() {
            let del_sql = format!("DELETE FROM [{}] WHERE [{}] IN ({})", table_name, pk_col, in_clause);
            info!("Executing bulk DELETE chunk for {} ({} items)...", table_name, chunk.len());
            sqlx::query(&del_sql).execute(replica_pool).await?;
        }
    }

    // Perform Bulk Upserts
    for chunk in upsert_pks.chunks(100) {
        if cancel_token.is_cancelled() {
            info!("Incremental sync cancelled for {}; aborting upsert loop.", table_name);
            break;
        }
        let in_clause = chunk.iter().map(|k| format!("'{}'", k)).collect::<Vec<_>>().join(",");
        if in_clause.is_empty() {
            continue;
        }

        // Fetch full rows from Primary in bulk
        let row_query = format!("SELECT {} FROM [{}] WHERE [{}] IN ({})", select_list, table_name, pk_col, in_clause);
        info!("Executing bulk UPSERT chunk SELECT for {} ({} items)...", table_name, chunk.len());
        let rows = sqlx::query(&row_query).fetch_all(primary_pool).await?;

        if rows.is_empty() {
            continue;
        }

        // Build INSERT query structure based on the first returned row
        let mut cols = Vec::new();
        let mut placeholders = Vec::new();
        for col in rows[0].columns() {
            cols.push(format!("[{}]", col.name()));
            placeholders.push(format!("@p{}", cols.len()));
        }

        let insert_sql = if has_identity {
            format!(
                "SET IDENTITY_INSERT [{}] ON; INSERT INTO [{}] ({}) VALUES ({});",
                table_name, table_name, cols.join(", "), placeholders.join(", ")
            )
        } else {
            format!(
                "INSERT INTO [{}] ({}) VALUES ({});",
                table_name, cols.join(", "), placeholders.join(", ")
            )
        };

        // Execute bulk Upsert via Transaction (DELETE then chunked INSERT)
        let mut tx = replica_pool.begin().await?;

        // 1. Delete existing rows in Replica to prepare for Insert
        let del_sql = format!("DELETE FROM [{}] WHERE [{}] IN ({})", table_name, pk_col, in_clause);
        if let Err(e) = sqlx::query(&del_sql).execute(&mut *tx).await {
            log::error!("Tx Incremental Delete Failed: {}", e);
            tx.rollback().await?;
            return Err(Box::new(e));
        }

        // 2. Insert new rows in a tight loop over the same transaction
        info!("Executing bulk UPSERT chunk INSERTs for {} ({} rows)...", table_name, rows.len());
        for row in rows {
            let mut query_builder = sqlx::query(&insert_sql);
            for col in row.columns() {
                let v: Option<String> = row.try_get(col.ordinal()).ok();
                query_builder = query_builder.bind(v);
            }
            if let Err(e) = query_builder.execute(&mut *tx).await {
                log::error!("Tx Incremental Insert Failed: {}", e);
                tx.rollback().await?;
                return Err(Box::new(e));
            }
        }

        if has_identity {
             let disable_identity = format!("SET IDENTITY_INSERT [{}] OFF;", table_name);
             let _ = sqlx::query(&disable_identity).execute(&mut *tx).await;
        }

        tx.commit().await?;
    }

    // Update Redis
    if !changes.is_empty() {
        let last_change_ver: i64 = changes.last().unwrap().get("SYS_CHANGE_VERSION");
        state::set_last_version(redis_client, table_name, last_change_ver).await?;
    } else {
        state::set_last_version(redis_client, table_name, current_version).await?;
    }

    // Set Incremental Tracking Finished State
    if let Err(e) = state::set_sync_progress(redis_client, table_name, total_records, total_records, started_at).await {
        log::warn!("Failed to set end-of-sync progress: {}", e);
    }

    Ok(())
}
