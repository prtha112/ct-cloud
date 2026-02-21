use redis::{Client, Commands, RedisResult};

pub async fn get_last_version(client: &Client, table_name: &str) -> RedisResult<i64> {
    let mut con = client.get_connection()?;
    let key = format!("mssql_sync:version:{}", table_name);
    let version: Option<i64> = con.get(key)?;
    Ok(version.unwrap_or(0))
}

pub async fn set_last_version(client: &Client, table_name: &str, version: i64) -> RedisResult<()> {
    let mut con = client.get_connection()?;
    let key = format!("mssql_sync:version:{}", table_name);
    let _: () = con.set(key, version)?;
    Ok(())
}

pub async fn should_force_full_load(client: &Client, table_name: &str) -> RedisResult<bool> {
    let mut con = client.get_connection()?;
    let key = format!("mssql_sync:force_full_load:{}", table_name);
    let val: Option<String> = con.get(key)?;
    Ok(val.as_deref() == Some("true"))
}

pub async fn clear_force_full_load(client: &Client, table_name: &str) -> RedisResult<()> {
    let mut con = client.get_connection()?;
    let key = format!("mssql_sync:force_full_load:{}", table_name);
    let _: () = con.set(key, "false")?;
    Ok(())
}

pub async fn init_force_full_load(client: &Client, table_name: &str) -> RedisResult<()> {
    let mut con = client.get_connection()?;
    let key = format!("mssql_sync:force_full_load:{}", table_name);
    // SETNX will only set the key if it does not already exist
    let _: () = redis::cmd("SETNX").arg(key).arg("false").query(&mut con)?;
    Ok(())
}

pub async fn init_table_enabled(client: &Client, table_name: &str) -> RedisResult<()> {
    let mut con = client.get_connection()?;
    let key = format!("mssql_sync:enabled:{}", table_name);
    // SETNX will only set the key if it does not already exist
    let _: () = redis::cmd("SETNX").arg(key).arg("false").query(&mut con)?;
    Ok(())
}

pub async fn is_table_enabled(client: &Client, table_name: &str) -> RedisResult<bool> {
    let mut con = client.get_connection()?;
    let key = format!("mssql_sync:enabled:{}", table_name);
    let enabled_str: Option<String> = con.get(key)?;
    Ok(enabled_str.as_deref() == Some("true"))
}

pub async fn set_config(client: &Client, config_key: &str, value: &str) -> RedisResult<()> {
    let mut con = client.get_connection()?;
    let key = format!("mssql_sync:config:{}", config_key);
    let _: () = con.set(key, value)?;
    Ok(())
}

pub async fn set_sync_progress(client: &Client, table_name: &str, synced: i64, total: i64) -> RedisResult<()> {
    let mut con = client.get_connection()?;
    let key = format!("mssql_sync:progress:{}", table_name);
    // Simple manual JSON string to avoid heavy dependencies for just one format
    let progress_json = format!(r#"{{"synced":{},"total":{}}}"#, synced, total);
    let _: () = con.set(key, progress_json)?;
    Ok(())
}
