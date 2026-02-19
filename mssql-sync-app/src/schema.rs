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

    if exists > 0 {
        return Ok(());
    }

    info!("Table {} does not exist in Replica. Creating...", table_name);

    // Get column definitions from Primary
    // This is a simplified schema extraction. For production, you need more detailed types, nullability, etc.
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

    Ok(())
}
