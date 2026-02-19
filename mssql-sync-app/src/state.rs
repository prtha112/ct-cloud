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
    let exists: bool = con.exists(key)?;
    Ok(exists)
}

pub async fn clear_force_full_load(client: &Client, table_name: &str) -> RedisResult<()> {
    let mut con = client.get_connection()?;
    let key = format!("mssql_sync:force_full_load:{}", table_name);
    let _: () = con.del(key)?;
    Ok(())
}
