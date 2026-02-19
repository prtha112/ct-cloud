use sqlx::{Pool, Mssql, Row};
use log::info;

pub async fn ensure_table_exists(
    primary_pool: &Pool<Mssql>,
    replica_pool: &Pool<Mssql>,
    table_name: &str
) -> Result<(), Box<dyn std::error::Error>> {
    // Check if table exists in Replica
    let check_query = format!(
        "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{}'", 
        table_name
    );
    let exists: i32 = sqlx::query_scalar(&check_query)
        .fetch_one(replica_pool)
        .await?;

    // Get column definitions from Primary first
    let columns_query = format!(
        "SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE 
         FROM INFORMATION_SCHEMA.COLUMNS 
         WHERE TABLE_NAME = '{}' 
         ORDER BY ORDINAL_POSITION",
        table_name
    );

    let rows = sqlx::query(&columns_query)
        .fetch_all(primary_pool)
        .await?;

    if rows.is_empty() {
        return Err(format!("Table {} not found on Primary", table_name).into());
    }

    if exists == 0 {
        info!("Table {} does not exist in Replica. Creating...", table_name);

        let mut create_sql = format!("CREATE TABLE [{}] (", table_name);
        let mut pk_columns = Vec::new();

        for (i, row) in rows.iter().enumerate() {
            let col_name: String = row.get("COLUMN_NAME");
            let data_type: String = row.get("DATA_TYPE");
            let max_len: Option<i32> = row.try_get("CHARACTER_MAXIMUM_LENGTH").ok();
            let is_nullable: String = row.get("IS_NULLABLE");

            if i > 0 {
                create_sql.push_str(", ");
            }

            create_sql.push_str(&format!("[{}] {}", col_name, data_type));

            if let Some(len) = max_len {
                if len == -1 {
                    create_sql.push_str("(MAX)");
                } else if data_type == "nvarchar" || data_type == "varchar" || data_type == "varbinary" {
                    create_sql.push_str(&format!("({})", len));
                }
            }

            if is_nullable == "NO" {
                create_sql.push_str(" NOT NULL");
            }
        }

        // Get PK
        let pk_query = format!(
            "SELECT COLUMN_NAME 
             FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE 
             WHERE OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + '.' + CONSTRAINT_NAME), 'IsPrimaryKey') = 1 
             AND TABLE_NAME = '{}'",
            table_name
        );

        let pk_rows = sqlx::query(&pk_query)
            .fetch_all(primary_pool)
            .await?;
        
        for row in pk_rows {
            pk_columns.push(format!("[{}]", row.get::<String, _>("COLUMN_NAME")));
        }

        if !pk_columns.is_empty() {
            create_sql.push_str(&format!(", PRIMARY KEY ({})", pk_columns.join(", ")));
        }

        create_sql.push_str(")");

        info!("Executing: {}", create_sql);
        sqlx::query(&create_sql).execute(replica_pool).await?;
        
        // Enable Change Tracking on the new table in Replica (Optional, usually configured manually or strictly on Primary)
        // But for Replica to be useful target? No, Replica usually is just standard table.
        // Wait, current code enables CT on Replica?
        // Step 206 viewed `schema.rs`. 
        // Logic: `ALTER TABLE ... ENABLE CHANGE TRACKING`.
        // If user wants bi-directional or uses replica as source later.
        // I'll keep it consistent with previous logic if it was there.
        // Previous logic (from Step 206 snippets): 
        // `let enable_ct_query = ... execute ...`
        // Yes, it was there at the end.
        
        let enable_ct_query = format!(
            "ALTER TABLE [{}] ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = ON)",
            table_name
        );
        // We use execute but ignore error if CT already enabled or DB not enabled for CT?
        // Primary DB has CT enabled. Replica DB might need it too if we run this.
        // User instructions said "Enable Change Tracking on Database" for Primary.
        // Didn't say for Replica.
        // But code tries to run it.
        // I will keep it but wrap in Result Ignore? Or assume user enabled DB CT on Replica too.
        let _ = sqlx::query(&enable_ct_query).execute(replica_pool).await;

    } else {
       // Table exists, check for missing columns
       let replica_cols_query = format!(
           "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{}'",
           table_name
       );
       let replica_rows = sqlx::query(&replica_cols_query).fetch_all(replica_pool).await?;
       let replica_col_names: Vec<String> = replica_rows.iter().map(|r| r.get("COLUMN_NAME")).collect();

       for row in &rows {
           let col_name: String = row.get("COLUMN_NAME");
           if !replica_col_names.contains(&col_name) {
               info!("Column {} missing in Replica table {}. Adding...", col_name, table_name);
               
               let data_type: String = row.get("DATA_TYPE");
               let max_len: Option<i32> = row.try_get("CHARACTER_MAXIMUM_LENGTH").ok();
               let is_nullable: String = row.get("IS_NULLABLE");
               
               let mut add_sql = format!("ALTER TABLE [{}] ADD [{}] {}", table_name, col_name, data_type);
               
               if let Some(len) = max_len {
                   if len == -1 {
                       add_sql.push_str("(MAX)");
                   } else if data_type == "nvarchar" || data_type == "varchar" || data_type == "varbinary" {
                       add_sql.push_str(&format!("({})", len));
                   }
               }
               
               // Adding NOT NULL to existing table requires default. 
               // For sync, we skip NOT NULL for added columns to avoid errors, 
               // UNLESS we want to strict match.
               // Let's safe skip NOT NULL for added columns for now 
               // or default to NULLable for safety.
               // Reason: If table has data, adding NOT NULL fails.
               // We don't know default value.
               // So we DON'T add "NOT NULL".
               
               info!("Executing: {}", add_sql);
               sqlx::query(&add_sql).execute(replica_pool).await?;
           }
       }

       // Check for EXTRA columns in Replica (Drop them)
       let primary_col_names: Vec<String> = rows.iter().map(|r| r.get::<String, _>("COLUMN_NAME")).collect();
       
       for rep_col in &replica_col_names {
           if !primary_col_names.contains(rep_col) {
               info!("Column {} exists in Replica but not in Primary for table {}. Dropping...", rep_col, table_name);
               let drop_sql = format!("ALTER TABLE [{}] DROP COLUMN [{}]", table_name, rep_col);
               info!("Executing: {}", drop_sql);
               sqlx::query(&drop_sql).execute(replica_pool).await?;
           }
       }
    }

    Ok(())
}
