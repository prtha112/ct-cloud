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
        "SELECT 
            c.COLUMN_NAME, 
            c.DATA_TYPE, 
            c.CHARACTER_MAXIMUM_LENGTH, 
            c.IS_NULLABLE,
            c.COLUMN_DEFAULT,
            c.NUMERIC_PRECISION,
            c.NUMERIC_SCALE,
            c.DATETIME_PRECISION,
            COLUMNPROPERTY(OBJECT_ID(c.TABLE_SCHEMA + '.' + c.TABLE_NAME), c.COLUMN_NAME, 'IsIdentity') as IsIdentity
         FROM INFORMATION_SCHEMA.COLUMNS c
         WHERE c.TABLE_NAME = '{}' 
         ORDER BY c.ORDINAL_POSITION",
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
            let col_default: Option<String> = row.try_get("COLUMN_DEFAULT").ok();
            let is_identity: Option<i32> = row.try_get("IsIdentity").ok();
            let dt_prec: Option<i16> = row.try_get("DATETIME_PRECISION").ok();

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
            } else if ["datetime2", "datetimeoffset", "time"].contains(&data_type.as_str()) {
                if let Some(prec) = dt_prec {
                    create_sql.push_str(&format!("({})", prec));
                }
            }

            if let Some(1) = is_identity {
                create_sql.push_str(" IDENTITY(1,1)");
            }

            if is_nullable == "NO" {
                create_sql.push_str(" NOT NULL");
            } else {
                create_sql.push_str(" NULL");
            }

            if let Some(def_val) = col_default {
                create_sql.push_str(&format!(" DEFAULT {}", def_val));
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
        
        let enable_ct_query = format!(
            "ALTER TABLE [{}] ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = ON)",
            table_name
        );
        let _ = sqlx::query(&enable_ct_query).execute(replica_pool).await;

    } else {
       // Table exists, check for missing columns and property mismatches
       let replica_cols_query = format!(
           "SELECT 
               COLUMN_NAME, 
               DATA_TYPE, 
               CHARACTER_MAXIMUM_LENGTH, 
               IS_NULLABLE, 
               COLUMN_DEFAULT,
               NUMERIC_PRECISION,
               NUMERIC_SCALE,
               DATETIME_PRECISION
            FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{}'",
           table_name
       );
       let replica_rows = sqlx::query(&replica_cols_query).fetch_all(replica_pool).await?;
       let replica_col_names: Vec<String> = replica_rows.iter().map(|r| r.get("COLUMN_NAME")).collect();

       for row in &rows {
           let col_name: String = row.get("COLUMN_NAME");
           
           if !replica_col_names.contains(&col_name) {
               // Column missing logic (same as before)
               info!("Column {} missing in Replica table {}. Adding...", col_name, table_name);
               
               let data_type: String = row.get("DATA_TYPE");
               let max_len: Option<i32> = row.try_get("CHARACTER_MAXIMUM_LENGTH").ok();
               let is_nullable: String = row.get("IS_NULLABLE");
               let col_default: Option<String> = row.try_get("COLUMN_DEFAULT").ok();
               let is_identity: Option<i32> = row.try_get("IsIdentity").ok();
               let numeric_precision: Option<u8> = row.try_get("NUMERIC_PRECISION").ok();
               let numeric_scale: Option<i32> = row.try_get("NUMERIC_SCALE").ok();
               let dt_prec: Option<i16> = row.try_get("DATETIME_PRECISION").ok();
               
               let mut add_sql = format!("ALTER TABLE [{}] ADD [{}] {}", table_name, col_name, data_type);
               
               if data_type == "decimal" || data_type == "numeric" {
                   if let (Some(p), Some(s)) = (numeric_precision, numeric_scale) {
                       add_sql.push_str(&format!("({}, {})", p, s));
                   }
               } else if let Some(len) = max_len {
                   if len == -1 {
                       add_sql.push_str("(MAX)");
                   } else if ["nvarchar", "varchar", "varbinary", "char", "nchar"].contains(&data_type.as_str()) {
                       add_sql.push_str(&format!("({})", len));
                   }
               } else if ["datetime2", "datetimeoffset", "time"].contains(&data_type.as_str()) {
                   if let Some(prec) = dt_prec {
                       add_sql.push_str(&format!("({})", prec));
                   }
               }

               if let Some(1) = is_identity {
                   add_sql.push_str(" IDENTITY(1,1)");
               }
               
               if is_nullable == "NO" {
                   add_sql.push_str(" NOT NULL");
               } else {
                   add_sql.push_str(" NULL");
               }
               
               if let Some(def_val) = &col_default {
                   add_sql.push_str(&format!(" DEFAULT {}", def_val));
               }
               
               info!("Executing: {}", add_sql);
               sqlx::query(&add_sql).execute(replica_pool).await?;
           } else {
               // Column exists, check for Property mismatch
               let rep_row = replica_rows.iter().find(|r| r.get::<String, _>("COLUMN_NAME") == col_name).unwrap();
               
               // Properties to check
               let p_type: String = row.get("DATA_TYPE");
               let p_len: Option<i32> = row.try_get("CHARACTER_MAXIMUM_LENGTH").ok();
               let p_null: String = row.get("IS_NULLABLE");
               let p_prec: Option<u8> = row.try_get("NUMERIC_PRECISION").ok();
               let p_scale: Option<i32> = row.try_get("NUMERIC_SCALE").ok();
               let p_dt_prec: Option<i16> = row.try_get("DATETIME_PRECISION").ok();
               
               let r_type: String = rep_row.get("DATA_TYPE");
               let r_len: Option<i32> = rep_row.try_get("CHARACTER_MAXIMUM_LENGTH").ok();
               let r_null: String = rep_row.get("IS_NULLABLE");
               let r_prec: Option<u8> = rep_row.try_get("NUMERIC_PRECISION").ok();
               let r_scale: Option<i32> = rep_row.try_get("NUMERIC_SCALE").ok();
               let r_dt_prec: Option<i16> = rep_row.try_get("DATETIME_PRECISION").ok();
               
               let mut properties_changed = false;
               
               if p_type != r_type {
                   properties_changed = true;
               } else if ["nvarchar", "varchar", "varbinary", "char", "nchar"].contains(&p_type.as_str()) && p_len != r_len {
                   properties_changed = true;
               } else if (p_type == "decimal" || p_type == "numeric") && (p_prec != r_prec || p_scale != r_scale) {
                   properties_changed = true;
               } else if ["datetime2", "datetimeoffset", "time"].contains(&p_type.as_str()) && p_dt_prec != r_dt_prec {
                   properties_changed = true;
               } else if p_null != r_null {
                   properties_changed = true;
               }
               
               if properties_changed {
                   
                   // Drop Default Constraint first (always safe before alter column)
                   let constraint_query = format!(
                       "SELECT name FROM sys.default_constraints 
                        WHERE parent_object_id = OBJECT_ID('{}') 
                        AND parent_column_id = COLUMNPROPERTY(OBJECT_ID('{}'), '{}', 'ColumnId')",
                       table_name, table_name, col_name
                   );
                   let constraint_name: Option<String> = sqlx::query_scalar(&constraint_query)
                       .fetch_optional(replica_pool)
                       .await?;
                       
                   if let Some(name) = constraint_name {
                       let drop_c_sql = format!("ALTER TABLE [{}] DROP CONSTRAINT [{}]", table_name, name);
                       info!("Executing (Pre-Alter Drop Constraint): {}", drop_c_sql);
                       sqlx::query(&drop_c_sql).execute(replica_pool).await?;
                   }
                   
                   // Alter Column
                   let mut alter_sql = format!("ALTER TABLE [{}] ALTER COLUMN [{}] {}", table_name, col_name, p_type);
                   
                   if p_type == "decimal" || p_type == "numeric" {
                       if let (Some(p), Some(s)) = (p_prec, p_scale) {
                           alter_sql.push_str(&format!("({}, {})", p, s));
                       }
                   } else if let Some(len) = p_len {
                       if len == -1 {
                           alter_sql.push_str("(MAX)");
                       } else if ["nvarchar", "varchar", "varbinary", "char", "nchar"].contains(&p_type.as_str()) {
                           alter_sql.push_str(&format!("({})", len));
                       }
                   } else if ["datetime2", "datetimeoffset", "time"].contains(&p_type.as_str()) {
                       if let Some(prec) = p_dt_prec {
                           alter_sql.push_str(&format!("({})", prec));
                       }
                   }

                   if p_null == "NO" {
                       alter_sql.push_str(" NOT NULL");
                   } else {
                       alter_sql.push_str(" NULL");
                   }
                   
                   info!("Executing: {}", alter_sql);
                   if let Err(e) = sqlx::query(&alter_sql).execute(replica_pool).await {
                       info!("Failed to alter column {}: {}. Might require dropping other dependencies.", col_name, e);
                   }
               }
               
               // Check Default Constraint
               let primary_default: Option<String> = row.try_get("COLUMN_DEFAULT").ok();
               let replica_default: Option<String> = rep_row.try_get("COLUMN_DEFAULT").ok();
               
               // Logic: If properties changed, we dropped constraint. We must restore if Primary has default.
               // Or if defaults mismatch.
               // Note: If properties changed, replica_default is stale (we dropped it).
               // So existing `replica_default` variable is stale if `properties_changed` is true.
               // But our logic is simple: Drop if exists -> Add New.
               
               let should_update_default = if properties_changed {
                   primary_default.is_some() // Always restore if Primary has one
               } else {
                   primary_default != replica_default // Only if mismatch
               };
               
               if should_update_default {
                   if !properties_changed {
                       info!("Default mismatch for {}.{}: P={:?}, R={:?}. Syncing...", table_name, col_name, primary_default, replica_default);
                   }
                   
                   // Drop existing constraint (Check again, might be dropped already if properties_changed=true code ran, or not exists)
                   // Efficient to check again? Yes.
                   let constraint_query = format!(
                       "SELECT name FROM sys.default_constraints 
                        WHERE parent_object_id = OBJECT_ID('{}') 
                        AND parent_column_id = COLUMNPROPERTY(OBJECT_ID('{}'), '{}', 'ColumnId')",
                       table_name, table_name, col_name
                   );
                   let constraint_name: Option<String> = sqlx::query_scalar(&constraint_query)
                       .fetch_optional(replica_pool)
                       .await?;
                       
                   if let Some(name) = constraint_name {
                       let drop_c_sql = format!("ALTER TABLE [{}] DROP CONSTRAINT [{}]", table_name, name);
                       info!("Executing: {}", drop_c_sql);
                       sqlx::query(&drop_c_sql).execute(replica_pool).await?;
                   }
                   
                   // Add new constraint
                   if let Some(def_val) = primary_default {
                       let add_sql = format!("ALTER TABLE [{}] ADD DEFAULT {} FOR [{}]", table_name, def_val, col_name);
                       info!("Executing: {}", add_sql);
                       sqlx::query(&add_sql).execute(replica_pool).await?;
                   }
               }
           }
       }

       // Check for EXTRA columns in Replica (Drop them)
       let primary_col_names: Vec<String> = rows.iter().map(|r| r.get::<String, _>("COLUMN_NAME")).collect();
       
       for rep_col in &replica_col_names {
           if !primary_col_names.contains(rep_col) {
               info!("Column {} exists in Replica but not in Primary for table {}. Dropping...", rep_col, table_name);
               // Check if column has default constraint before dropping? 
               // MSSQL might return error if dropping column with constraint.
               // Let's safe drop constraint first.
               let constraint_query = format!(
                   "SELECT name FROM sys.default_constraints 
                    WHERE parent_object_id = OBJECT_ID('{}') 
                    AND parent_column_id = COLUMNPROPERTY(OBJECT_ID('{}'), '{}', 'ColumnId')",
                   table_name, table_name, rep_col
               );
               let constraint_name: Option<String> = sqlx::query_scalar(&constraint_query)
                   .fetch_optional(replica_pool)
                   .await?;
                   
               if let Some(name) = constraint_name {
                   let drop_c_sql = format!("ALTER TABLE [{}] DROP CONSTRAINT [{}]", table_name, name);
                   info!("Executing: {}", drop_c_sql);
                   sqlx::query(&drop_c_sql).execute(replica_pool).await?;
               }
               
               let drop_sql = format!("ALTER TABLE [{}] DROP COLUMN [{}]", table_name, rep_col);
               info!("Executing: {}", drop_sql);
               sqlx::query(&drop_sql).execute(replica_pool).await?;
           }
       }
    }

    Ok(())
}
