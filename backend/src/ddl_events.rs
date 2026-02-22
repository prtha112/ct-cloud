use sqlx::{Pool, Mssql, Row};
use std::time::Duration;
use log::{info, error, warn};
use redis::Client;
use tokio::time::sleep;
use crate::state;

pub async fn start_consumer_loop(
    primary_pool: Pool<Mssql>,
    replica_pool: Pool<Mssql>,
    redis_client: Client
) {
    info!("Starting DDL Event consumer loop...");
    
    loop {
        if let Err(e) = consume_events(&primary_pool, &replica_pool, &redis_client).await {
            error!("Error consuming DDL events: {}", e);
            sleep(Duration::from_secs(5)).await;
        }
    }
}

async fn consume_events(
    primary_pool: &Pool<Mssql>,
    replica_pool: &Pool<Mssql>,
    redis_client: &Client,
) -> anyhow::Result<()> {
    let receive_sql = "
        WAITFOR (
            RECEIVE TOP(1) 
                message_type_name, 
                CAST(message_body AS NVARCHAR(MAX)) AS message_body 
            FROM SyncDDLQueue
        ), TIMEOUT 5000;
    ";

    let row = sqlx::query(receive_sql).fetch_optional(primary_pool).await?;

    if let Some(r) = row {
        let msg_type: String = r.get("message_type_name");
        
        // Handle Event Notifications
        if msg_type == "http://schemas.microsoft.com/SQL/Notifications/EventNotification" {
            let msg_body: String = r.get("message_body");
            
            // Extract <CommandText>, <EventType>, and <ObjectName> manually to avoid heavy XML parsers
            if let (Some(cmd_start), Some(cmd_end)) = (msg_body.find("<CommandText>"), msg_body.find("</CommandText>")) {
                let mut cmd = msg_body[cmd_start + 13..cmd_end].to_string();
                
                let mut event_type = "UNKNOWN".to_string();
                if let (Some(ev_start), Some(ev_end)) = (msg_body.find("<EventType>"), msg_body.find("</EventType>")) {
                    event_type = msg_body[ev_start + 11..ev_end].to_string();
                }
                
                if let (Some(obj_start), Some(obj_end)) = (msg_body.find("<ObjectName>"), msg_body.find("</ObjectName>")) {
                    let mut obj_name = &msg_body[obj_start + 12..obj_end];
                    
                    // RENAME events place the column name in ObjectName, and table in TargetObjectName
                    // INDEX events (CREATE_INDEX, ALTER_INDEX) place the index name in ObjectName, and table in TargetObjectName
                    if let (Some(targ_start), Some(targ_end)) = (msg_body.find("<TargetObjectName>"), msg_body.find("</TargetObjectName>")) {
                        obj_name = &msg_body[targ_start + 18..targ_end];
                    }

                    // Quick decode XML entities for TSQL cmd
                    cmd = cmd.replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">").replace("&quot;", "\"").replace("&apos;", "'");

                    // Verify if this table is enabled for sync
                    if state::is_table_enabled(redis_client, obj_name).await.unwrap_or(false) {
                        info!("Applying DDL Event [{}] to {}: {}", event_type, obj_name, cmd);
                        
                        match sqlx::query(&cmd).execute(replica_pool).await {
                            Ok(_) => info!("DDL Event [{}] executed successfully on replica.", event_type),
                            Err(e) => warn!("Failed to execute DDL [{}] on replica: {}. Query was: {}", event_type, e, cmd)
                        }
                    } else {
                        info!("Ignoring DDL Event [{}] for table {} (sync is disabled).", event_type, obj_name);
                    }
                }
            }
        }
    }

    Ok(())
}
