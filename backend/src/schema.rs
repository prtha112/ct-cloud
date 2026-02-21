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
           }
       }
    }

    // Sync schema objects (Indexes, Unique constraints, Foreign keys)
    sync_schema_objects(primary_pool, replica_pool, table_name).await?;

    Ok(())
}

pub async fn sync_schema_objects(
    primary_pool: &Pool<Mssql>,
    replica_pool: &Pool<Mssql>,
    table_name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    // 1. Fetch Indexes & Unique Constraints
    let idx_query = format!(
        "SELECT 
            i.name as IndexName, 
            CAST(i.is_unique AS BIT) as IsUnique,
            CAST(i.is_unique_constraint AS BIT) as IsUniqueConstraint,
            i.type_desc as TypeDesc,
            CAST(STUFF((
                SELECT ', [' + c.name + ']' + CASE WHEN ic.is_descending_key = 1 THEN ' DESC' ELSE '' END
                FROM sys.index_columns ic
                JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
                WHERE ic.object_id = i.object_id AND ic.index_id = i.index_id
                ORDER BY ic.key_ordinal
                FOR XML PATH('')
            ), 1, 2, '') AS NVARCHAR(4000)) as Columns
         FROM sys.indexes i
         WHERE i.object_id = OBJECT_ID('{}') 
         AND i.is_primary_key = 0 
         AND i.type > 0",
        table_name
    );

    let p_indexes = sqlx::query(&idx_query).fetch_all(primary_pool).await?;
    let r_indexes = sqlx::query(&idx_query).fetch_all(replica_pool).await?;

    let p_idx_names: Vec<String> = p_indexes.iter().map(|r| r.get("IndexName")).collect();
    let r_idx_names: Vec<String> = r_indexes.iter().map(|r| r.get("IndexName")).collect();

    // 2. Fetch Foreign Keys
    let fk_query = format!(
        "SELECT 
            fk.name AS ForeignKeyName,
            OBJECT_NAME(fk.referenced_object_id) AS ReferencedTableName,
            CAST(STUFF((
                SELECT ', [' + c.name + ']'
                FROM sys.foreign_key_columns fkc
                JOIN sys.columns c ON fkc.parent_object_id = c.object_id AND fkc.parent_column_id = c.column_id
                WHERE fkc.constraint_object_id = fk.object_id
                ORDER BY fkc.constraint_column_id
                FOR XML PATH('')
            ), 1, 2, '') AS NVARCHAR(4000)) AS ParentColumns,
            CAST(STUFF((
                SELECT ', [' + c.name + ']'
                FROM sys.foreign_key_columns fkc
                JOIN sys.columns c ON fkc.referenced_object_id = c.object_id AND fkc.referenced_column_id = c.column_id
                WHERE fkc.constraint_object_id = fk.object_id
                ORDER BY fkc.constraint_column_id
                FOR XML PATH('')
            ), 1, 2, '') AS NVARCHAR(4000)) AS ReferencedColumns,
            fk.delete_referential_action_desc AS DeleteAction,
            fk.update_referential_action_desc AS UpdateAction
        FROM sys.foreign_keys fk
        WHERE fk.parent_object_id = OBJECT_ID('{}')",
        table_name
    );

    let p_fks = sqlx::query(&fk_query).fetch_all(primary_pool).await?;
    let r_fks = sqlx::query(&fk_query).fetch_all(replica_pool).await?;

    let p_fk_names: Vec<String> = p_fks.iter().map(|r| r.get("ForeignKeyName")).collect();
    let r_fk_names: Vec<String> = r_fks.iter().map(|r| r.get("ForeignKeyName")).collect();

    // --- DROP MISSING OBJECTS ---
    // 3. Drop missing Foreign Keys first (to avoid dependency conflicts on indexes)
    for r_row in &r_fks {
        let name: String = r_row.get("ForeignKeyName");
        if !p_fk_names.contains(&name) {
            info!("Dropping Foreign Key {} on table {}", name, table_name);
            let drop_sql = format!("ALTER TABLE [{}] DROP CONSTRAINT [{}]", table_name, name);
            if let Err(e) = sqlx::query(&drop_sql).execute(replica_pool).await {
                log::warn!("Failed to drop foreign key {}: {}", name, e);
            }
        }
    }

    // 4. Drop missing Indexes & Constraints
    for r_row in &r_indexes {
        let name: String = r_row.get("IndexName");
        let is_unique_constraint: bool = r_row.get("IsUniqueConstraint");
        
        if !p_idx_names.contains(&name) {
            info!("Dropping index/constraint {} on table {}", name, table_name);
            let drop_sql = if is_unique_constraint {
                format!("ALTER TABLE [{}] DROP CONSTRAINT [{}]", table_name, name)
            } else {
                format!("DROP INDEX [{}] ON [{}]", name, table_name)
            };
            if let Err(e) = sqlx::query(&drop_sql).execute(replica_pool).await {
                log::warn!("Failed to drop index/constraint {}: {}", name, e);
            }
        }
    }

    // --- CREATE MISSING OBJECTS ---
    // 5. Create missing Indexes / Unique Constraints
    for p_row in &p_indexes {
        let name: String = p_row.get("IndexName");
        let is_unique: bool = p_row.get("IsUnique");
        let is_unique_constraint: bool = p_row.get("IsUniqueConstraint");
        let columns: Option<String> = p_row.try_get("Columns").ok();

        if !r_idx_names.contains(&name) {
            if let Some(cols) = columns {
                info!("Creating index/constraint {} on table {}", name, table_name);
                let create_sql = if is_unique_constraint {
                    format!("ALTER TABLE [{}] ADD CONSTRAINT [{}] UNIQUE ({})", table_name, name, cols)
                } else {
                    let unique_str = if is_unique { "UNIQUE " } else { "" };
                    format!("CREATE {}INDEX [{}] ON [{}] ({})", unique_str, name, table_name, cols)
                };

                if let Err(e) = sqlx::query(&create_sql).execute(replica_pool).await {
                    log::warn!("Failed to create index {}: {}", name, e);
                }
            }
        }
    }

    // 6. Create missing Foreign Keys
    for p_row in &p_fks {
        let name: String = p_row.get("ForeignKeyName");
        let ref_table: Option<String> = p_row.try_get("ReferencedTableName").ok();
        let p_cols: Option<String> = p_row.try_get("ParentColumns").ok();
        let r_cols: Option<String> = p_row.try_get("ReferencedColumns").ok();
        let del_action: Option<String> = p_row.try_get("DeleteAction").ok();
        let upd_action: Option<String> = p_row.try_get("UpdateAction").ok();

        if !r_fk_names.contains(&name) {
            if let (Some(rt), Some(pc), Some(rc)) = (ref_table, p_cols, r_cols) {
                info!("Creating Foreign Key {} on table {}", name, table_name);
                let mut create_sql = format!(
                    "ALTER TABLE [{}] ADD CONSTRAINT [{}] FOREIGN KEY ({}) REFERENCES [{}] ({})",
                    table_name, name, pc, rt, rc
                );

                if let Some(da) = del_action {
                    let da_str = da.replace("_", " ");
                    if da_str != "NO ACTION" {
                        create_sql.push_str(&format!(" ON DELETE {}", da_str));
                    }
                }
                if let Some(ua) = upd_action {
                    let ua_str = ua.replace("_", " ");
                    if ua_str != "NO ACTION" {
                        create_sql.push_str(&format!(" ON UPDATE {}", ua_str));
                    }
                }

                if let Err(e) = sqlx::query(&create_sql).execute(replica_pool).await {
                    log::warn!("Failed to create foreign key {} (referenced table might not exist yet): {}", name, e);
                }
            }
        }
    }

    Ok(())
}

pub async fn sync_views(
    primary_pool: &Pool<Mssql>,
    replica_pool: &Pool<Mssql>,
) -> Result<(), Box<dyn std::error::Error>> {
    let views_query = "
        SELECT 
            v.name as ViewName, 
            s.name as SchemaName, 
            CAST(m.definition AS NVARCHAR(4000)) as Definition 
        FROM sys.views v 
        JOIN sys.sql_modules m ON v.object_id = m.object_id 
        JOIN sys.schemas s ON v.schema_id = s.schema_id
    ";

    let p_views = sqlx::query(views_query).fetch_all(primary_pool).await?;
    let r_views = sqlx::query(views_query).fetch_all(replica_pool).await?;

    let mut p_map = std::collections::HashMap::new();
    for row in &p_views {
        let name: String = row.get::<String, _>("ViewName");
        let schema: String = row.get::<String, _>("SchemaName");
        let def: Option<String> = row.try_get("Definition").ok();
        p_map.insert(format!("{}.{}", schema, name), def.unwrap_or_default());
    }

    let mut r_map = std::collections::HashMap::new();
    for row in &r_views {
        let name: String = row.get::<String, _>("ViewName");
        let schema: String = row.get::<String, _>("SchemaName");
        let def: Option<String> = row.try_get("Definition").ok();
        r_map.insert(format!("{}.{}", schema, name), def.unwrap_or_default());
    }

    // Drop missing views on replica
    for (r_key, _) in &r_map {
        if !p_map.contains_key(r_key) {
            info!("Dropping View {}", r_key);
            let drop_sql = format!("DROP VIEW [{}]", r_key.replace(".", "].["));
            if let Err(e) = sqlx::query(&drop_sql).execute(replica_pool).await {
                log::warn!("Failed to drop view {}: {}", r_key, e);
            }
        }
    }

    // Create or Alter views on replica
    for (p_key, p_def) in &p_map {
        let should_sync = match r_map.get(p_key) {
            Some(r_def) => p_def != r_def,
            None => true,
        };

        if should_sync {
            info!("Syncing View {}", p_key);
            // Drop so we can recreate
            if r_map.contains_key(p_key) {
                let drop_sql = format!("DROP VIEW [{}]", p_key.replace(".", "].["));
                let _ = sqlx::query(&drop_sql).execute(replica_pool).await;
            }
            if let Err(e) = sqlx::query(p_def).execute(replica_pool).await {
                log::warn!("Failed to sync view {}: {}", p_key, e);
            }
        }
    }

    Ok(())
}

pub async fn sync_routines(
    primary_pool: &Pool<Mssql>,
    replica_pool: &Pool<Mssql>,
) -> Result<(), Box<dyn std::error::Error>> {
    let routines_query = "
        SELECT 
            o.name as ObjectName, 
            s.name as SchemaName, 
            o.type as ObjectType,
            CAST(m.definition AS NVARCHAR(4000)) as Definition 
        FROM sys.objects o 
        JOIN sys.sql_modules m ON o.object_id = m.object_id 
        JOIN sys.schemas s ON o.schema_id = s.schema_id
        WHERE o.type IN ('P', 'FN', 'IF', 'TF')
    ";

    let p_routines = sqlx::query(routines_query).fetch_all(primary_pool).await?;
    let r_routines = sqlx::query(routines_query).fetch_all(replica_pool).await?;

    let mut p_map = std::collections::HashMap::new();
    for row in &p_routines {
        let name: String = row.get::<String, _>("ObjectName");
        let schema: String = row.get::<String, _>("SchemaName");
        let obj_type: String = row.get::<String, _>("ObjectType");
        let def: Option<String> = row.try_get("Definition").ok();
        p_map.insert(format!("{}.{}", schema, name), (obj_type.trim().to_string(), def.unwrap_or_default()));
    }

    let mut r_map = std::collections::HashMap::new();
    for row in &r_routines {
        let name: String = row.get::<String, _>("ObjectName");
        let schema: String = row.get::<String, _>("SchemaName");
        let obj_type: String = row.get::<String, _>("ObjectType");
        let def: Option<String> = row.try_get("Definition").ok();
        r_map.insert(format!("{}.{}", schema, name), (obj_type.trim().to_string(), def.unwrap_or_default()));
    }

    // Helper to determine DROP statement
    let get_drop_type = |obj_type: &str| -> &str {
        match obj_type {
            "P" => "PROCEDURE",
            "FN" | "IF" | "TF" => "FUNCTION",
            _ => "PROCEDURE", // Fallback, though shouldn't happen based on IN clause
        }
    };

    // Drop missing routines on replica
    for (r_key, (r_type, _)) in &r_map {
        if !p_map.contains_key(r_key) {
            let drop_term = get_drop_type(r_type);
            info!("Dropping {} {}", drop_term, r_key);
            let drop_sql = format!("DROP {} [{}]", drop_term, r_key.replace(".", "].["));
            if let Err(e) = sqlx::query(&drop_sql).execute(replica_pool).await {
                log::warn!("Failed to drop {} {}: {}", drop_term, r_key, e);
            }
        }
    }

    // Create or Alter routines on replica
    for (p_key, (p_type, p_def)) in &p_map {
        let should_sync = match r_map.get(p_key) {
            Some((_, r_def)) => p_def != r_def,
            None => true,
        };

        if should_sync {
            let drop_term = get_drop_type(p_type);
            info!("Syncing {} {}", drop_term, p_key);
            
            // Drop so we can recreate if it exists on replica
            if r_map.contains_key(p_key) {
                let drop_sql = format!("DROP {} [{}]", drop_term, p_key.replace(".", "].["));
                let _ = sqlx::query(&drop_sql).execute(replica_pool).await;
            }
            if let Err(e) = sqlx::query(p_def).execute(replica_pool).await {
                log::warn!("Failed to sync {} {}: {}", drop_term, p_key, e);
            }
        }
    }

    Ok(())
}

