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
    
    info!("Starting replication service...");
    
    loop {
        // In a real scenario, you might want to iterate over a list of tables to sync
        // For this example, let's assume we sync 'Users' table or get list from config
        // Here we can query tracked tables from Primary
        
        // This is a simplified loop. 
        // We will implement sync for a specific table "Users" first as proof of concept if needed,
        // or query sys.change_tracking_tables to find all enabled tables.
        
        if let Err(e) = sync::run_sync(&primary_pool, &replica_pool, &redis_client).await {
            error!("Sync error: {}", e);
        }

        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}
