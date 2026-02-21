use std::env;
use std::time::Duration;
use sqlx::mssql::MssqlPoolOptions;
use redis::Client;
use dotenv::dotenv;
use log::{info, error};

mod state;
mod schema;
mod sync;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenv().ok();
    env_logger::init();

    let primary_url = env::var("MSSQL_PRIMARY_URL")
        .expect("MSSQL_PRIMARY_URL must be set");
    let replica_url = env::var("MSSQL_REPLICA_URL")
        .expect("MSSQL_REPLICA_URL must be set");
        
    if primary_url == replica_url {
        panic!("MSSQL_PRIMARY_URL and MSSQL_REPLICA_URL cannot be the same!");
    }

    let redis_url = env::var("REDIS_URL")
        .expect("REDIS_URL must be set");

    info!("Connecting to Primary MSSQL...");
    let primary_pool = MssqlPoolOptions::new()
        .max_connections(5)
        .connect(&primary_url)
        .await?;

    info!("Connecting to Replica MSSQL...");
    let replica_pool = MssqlPoolOptions::new()
        .max_connections(5)
        .connect(&replica_url)
        .await?;

    info!("Connecting to Redis...");
    let redis_client = Client::open(redis_url)?;
    
    let thread_count = env::var("SYNC_THREADS")
        .unwrap_or_else(|_| "1".to_string())
        .parse::<usize>()
        .unwrap_or(1);
    
    info!("Starting replication service with {} threads...", thread_count);
    
    loop {
        if let Err(e) = sync::run_sync(&primary_pool, &replica_pool, &redis_client, thread_count).await {
            error!("Sync error: {}", e);
        }

        // Sync views after all tables are processed (to avoid missing table dependencies)
        if let Err(e) = schema::sync_views(&primary_pool, &replica_pool).await {
            error!("View sync error: {}", e);
        }

        // Sync stored procedures and functions
        if let Err(e) = schema::sync_routines(&primary_pool, &replica_pool).await {
            error!("Routine sync error: {}", e);
        }

        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}
