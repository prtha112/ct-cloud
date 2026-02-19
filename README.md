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
    
    Check **Replica** (`localhost:1435`) - the `[User]` table will be created and data synced automatically.

## Force Full Re-Sync

To force a full synchronization (TRUNCATE -> FULL LOAD) for a specific table, set a Redis key:

```bash
# Example: Force reload for 'Product' table
redis-cli SET mssql_sync:force_full_load:Product "true"
```

The app will detect this flag, reload the table on the Replica, and automatically remove the key.

## Architecture

- **Primary**: MSSQL 2022 (Port 1434)
- **Replica**: MSSQL 2022 (Port 1435)
- **Redis**: Stores last synced version (Internal Port 6379)
- **App**: Rust service polling changes every 5 seconds.