# MSSQL Change Tracking Replication (Rust)

A Rust application that replicates data from a **Primary MSSQL** instance to a **Replica MSSQL** instance using **Change Tracking**. Synchronization state is managed via **Redis**.

## Prerequisites

- Docker & Docker Compose

## Quick Start

1.  **Start Services**
    ```bash
    docker-compose up -d --build
    ```

2.  **Initialize Database (One-time Setup)**
    Connect to **Primary MSSQL** (`localhost:1434`, User: `sa`, Pass: `Password123!`) and run:

    ```sql
    -- 1. Create Database
    CREATE DATABASE testct;
    GO
    USE testct;
    GO
    
    -- 2. Enable Change Tracking on Database
    ALTER DATABASE testct
    SET CHANGE_TRACKING = ON
    (CHANGE_RETENTION = 2 DAYS, AUTO_CLEANUP = ON);
    GO

    -- 3. Create Table & Enable Change Tracking
    CREATE TABLE [User] (
        id BIGINT PRIMARY KEY,
        username VARCHAR(100),
        email VARCHAR(100)
    );
    GO
    
    ALTER TABLE [User]
    ENABLE CHANGE_TRACKING
    WITH (TRACK_COLUMNS_UPDATED = ON);
    GO
    ```

3.  **Test Replication**
    Insert data into **Primary**:
    ```sql
    INSERT INTO [User] (id, username, email) VALUES (1, 'alice', 'alice@example.com');
    ```
    
    By default, synchronization is paused. You must enable it in Redis to see the data on the **Replica** (`localhost:1435`):
    ```bash
    # Enable synchronization for the 'User' table
    docker exec redis_sync_state redis-cli SET mssql_sync:enabled:User "true"
    ```

## Enable Table Synchronization

By default, any new table discovered with Change Tracking enabled will be paused. To start schema creation and data replication for a specific table, you must set its flag in Redis:

```bash
docker exec redis_sync_state redis-cli SET mssql_sync:enabled:TableName "true"
```

## Force Full Re-Sync

To force a full synchronization (TRUNCATE -> FULL LOAD) for a specific table, set a Redis key:

```bash
# Example: Force reload for 'Product' table
docker exec redis_sync_state redis-cli SET mssql_sync:force_full_load:Product "true"
```

The app will detect this flag, reload the table on the Replica, and automatically remove the key when finished.

> **Note on Large Tables (Chunked Sync):** 
> To prevent `Out of Memory` errors when syncing tables with millions of rows, the Full Re-Sync feature uses **Keyset Pagination**. It automatically detects the table's Primary Key (or falls back to the first column) and fetches records in chunks of 5,000 rows at a time until the entire table is seamlessly replicated.

## Architecture

- **Primary**: MSSQL 2022 (Port 1434)
- **Replica**: MSSQL 2022 (Port 1435)
- **Redis**: Stores last synced version (Internal Port 6379)
- **App**: Rust service polling changes every 5 seconds.