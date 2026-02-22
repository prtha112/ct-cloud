use std::env;
use std::time::Duration;
use std::sync::Arc;
use std::collections::HashSet;
use tokio::sync::{Semaphore, Mutex as TokioMutex};
use sqlx::mssql::MssqlPoolOptions;
use redis::Client;
use dotenv::dotenv;
use log::{info, error, debug};
use tokio_util::sync::CancellationToken;

mod state;
mod schema;
mod sync;
mod ddl_events;

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

    // Save sanitized config to Redis for the Frontend to display
    // E.g. mssql://sa:Password123!@localhost:1433/testct -> mssql://localhost:1433/testct
    let sanitize_url = |url: &str| -> String {
        if let Some(at_idx) = url.find('@') {
            if let Some(protocol_idx) = url.find("://") {
                let protocol = &url[0..protocol_idx + 3];
                let rest = &url[at_idx + 1..];
                return format!("{}{}", protocol, rest);
            }
        }
        url.to_string()
    };

    let safe_primary = sanitize_url(&primary_url);
    let safe_replica = sanitize_url(&replica_url);
    
    if let Err(e) = state::set_config(&redis_client, "primary_url", &safe_primary).await {
        error!("Failed to save primary config to Redis: {}", e);
    }
    if let Err(e) = state::set_config(&redis_client, "replica_url", &safe_replica).await {
        error!("Failed to save replica config to Redis: {}", e);
    }
    
    let thread_count = env::var("SYNC_THREADS")
        .unwrap_or_else(|_| "1".to_string())
        .parse::<usize>()
        .unwrap_or(1);
    
    let cancel_token = CancellationToken::new();

    // Spawn a graceful shutdown listener
    let cancel_clone = cancel_token.clone();
    tokio::spawn(async move {
        match tokio::signal::ctrl_c().await {
            Ok(()) => {
                info!("SIGTERM/Ctrl-C received, initiating graceful shutdown...");
                cancel_clone.cancel();
            },
            Err(e) => {
                error!("Failed to listen for shutdown signal: {}", e);
            },
        }
    });

    info!("Starting replication service with {} threads...", thread_count);

    let ddl_primary = primary_pool.clone();
    let ddl_replica = replica_pool.clone();
    let ddl_redis = redis_client.clone();
    let ddl_token = cancel_token.clone();
    tokio::spawn(async move {
        ddl_events::start_consumer_loop(ddl_primary, ddl_replica, ddl_redis, ddl_token).await;
    });
    
    // Global Concurrency State
    let semaphore = Arc::new(Semaphore::new(thread_count));
    let active_tasks: Arc<TokioMutex<HashSet<String>>> = Arc::new(TokioMutex::new(HashSet::new()));

    loop {
        if cancel_token.is_cancelled() {
            info!("Shutting down main replication service loop...");
            break;
        }

        // Fetch all tracked tables
        let tables_query = "
            SELECT t.name AS TableName
            FROM sys.change_tracking_tables ctt
            JOIN sys.tables t ON ctt.object_id = t.object_id
        ";
        
        let tables_res = sqlx::query(tables_query).fetch_all(&primary_pool).await;
        
        match tables_res {
            Ok(tables) => {
                for row in tables {
                    let table_name: String = sqlx::Row::get(&row, "TableName");
                    
                    // Check if table is currently syncing, skip if it is
                    let mut tasks_guard = active_tasks.lock().await;
                    if tasks_guard.contains(&table_name) {
                        debug!("Table {} is already syncing, skipping iteration.", table_name);
                        continue;
                    }
                    
                    // Not syncing: mark as active and spawn detached task
                    tasks_guard.insert(table_name.clone());
                    drop(tasks_guard);

                    let p_pool = primary_pool.clone();
                    let r_pool = replica_pool.clone();
                    let r_client = redis_client.clone();
                    let sem_clone = Arc::clone(&semaphore);
                    let active_clone = Arc::clone(&active_tasks);
                    let table_token = cancel_token.clone();

                    tokio::spawn(async move {
                        // Attempt to acquire a permit. This will hang here if SYNC_THREADS is exhausted
                        // but it won't block the main loop from checking and querying other things.
                        let _permit = match sem_clone.acquire().await {
                            Ok(p) => p,
                            Err(_) => {
                                active_clone.lock().await.remove(&table_name);
                                return;
                            }
                        };
                        
                        // Pass off to sync process
                        if let Err(e) = sync::run_single_table_sync(&p_pool, &r_pool, &r_client, &table_name, table_token).await {
                            error!("Sync error on table {}: {}", table_name, e);
                        }

                        // Detach from active list
                        active_clone.lock().await.remove(&table_name);
                    });
                }
            },
            Err(e) => error!("Failed to fetch table list: {}", e),
        }

        // We run Views & Routines sequentially in the main loop every 5s as they are cheap DDL
        if let Err(e) = schema::sync_views(&primary_pool, &replica_pool).await {
            error!("View sync error: {}", e);
        }

        if let Err(e) = schema::sync_routines(&primary_pool, &replica_pool).await {
            error!("Routine sync error: {}", e);
        }

        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(5)) => {}
            _ = cancel_token.cancelled() => {
                info!("Shutting down main replication service loop during sleep delay...");
                break;
            }
        }
    }

    Ok(())
}
