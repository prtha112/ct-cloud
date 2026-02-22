# MSSQL Change Tracking Replication (Rust)

![Dashboard UI Preview](./ui.png)

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
    
    -- Enable Service Broker (Required for DDL Event Notifications)
    ALTER DATABASE testct SET ENABLE_BROKER WITH ROLLBACK IMMEDIATE;
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

    CREATE TABLE [Customer] (
        CustomerId int IDENTITY(1,1) NOT NULL,
        ExternalCode nvarchar(50) NOT NULL,
        FullName nvarchar(200) NOT NULL,
        Email nvarchar(200) NULL,
        Status tinyint DEFAULT 1 NOT NULL,
        CreatedAt datetime2(0) DEFAULT sysutcdatetime() NOT NULL,
        UpdatedAt datetime2(0) NULL,
        PRIMARY KEY (CustomerId),
        UNIQUE (ExternalCode)
    );
    GO

    CREATE TABLE [Product] (
        id bigint NOT NULL,
        Name text NULL,
        Category varchar(100) NULL,
        Price numeric(10,2) NULL,
        PRIMARY KEY (id)
    );
    GO

    ALTER TABLE [User]
    ENABLE CHANGE_TRACKING
    WITH (TRACK_COLUMNS_UPDATED = ON);
    GO

    ALTER TABLE [Product]
    ENABLE CHANGE_TRACKING
    WITH (TRACK_COLUMNS_UPDATED = ON);
    GO

    -- 4. Setup DDL Event Capture (For handling column renames/drops safely)
    CREATE QUEUE SyncDDLQueue;
    GO
    
    CREATE SERVICE SyncDDLService 
    ON QUEUE SyncDDLQueue 
    ([http://schemas.microsoft.com/SQL/Notifications/PostEventNotification]);
    GO
    
    CREATE EVENT NOTIFICATION SyncDDLEvents
    ON DATABASE
    FOR DDL_TABLE_EVENTS, RENAME
    TO SERVICE 'SyncDDLService', 'current database';
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

## Force Full Re-Sync/Deploying to Production

When deploying this application to a real production database where Change Tracking has been running for a long time, the app should **not** replay the entire history from version 0. Instead, you should use the Force Full Load feature table by table to snapshot the current state.

1. **Start the App:** Once running, the app creates schema clones on the replica but sets all synchronization (`mssql_sync:enabled:[Table]` and `mssql_sync:force_full_load:[Table]`) to `"false"` by default.
2. **Force Full Load (Small/Medium Tables):** 
   Set the `force_full_load` flag to `"true"`, followed by setting `enabled` to `"true"`. The app will truncate the replica table, chunk-insert all current data, and seamlessly transition into incremental sync for future changes while resetting the `force_full_load` flag back to `"false"`.
   ```bash
   docker exec redis_sync_state redis-cli SET mssql_sync:force_full_load:Product "true"
   docker exec redis_sync_state redis-cli SET mssql_sync:enabled:Product "true"
   ```
3. **Huge Tables (Manual Snapshot):** 
   For extremely large tables, avoid querying the entire table via the app. Instead, perform a manual backup/restore to the Replica. Note the `CHANGE_TRACKING_CURRENT_VERSION()` from the Primary at the time of backup, and manually set it in Redis:
   ```bash
   docker exec redis_sync_state redis-cli SET mssql_sync:version:HugeTable "850550"
   docker exec redis_sync_state redis-cli SET mssql_sync:enabled:HugeTable "true"
   ```

> **Note on Large Tables (Chunked Sync):** 
> To prevent `Out of Memory` errors when syncing tables with millions of rows, the Full Re-Sync feature uses **Keyset Pagination**. It automatically detects the table's Primary Key (or falls back to the first column) and fetches records in chunks of 5,000 rows at a time until the entire table is seamlessly replicated.

## Fault Tolerance & Idempotency

This replication service is heavily designed to be **idempotent**, meaning that unexpected container restarts or disconnections will not result in duplicated or corrupted data.
- **Incremental Sync Interruptions:** Changes are fetched from the `CHANGETABLE` and applied locally using an `UPSERT` pattern (Delete matching PK, then Insert). The tracked `version` in Redis is only updated **after** an entire transaction batch completes successfully. If the application crashes midway, it simply replays the exact same batch on startup with identical results.
- **Force Load Interruptions:** Full-load progress is tracked per table. If an interruption occurs mid-load, the flag `force_full_load` remains `true` in Redis. On the next startup, the crawler will simply `TRUNCATE` the replica table strings again and re-initiate the batch insertion fresh from the start, guaranteeing zero duplications.

## View Synchronization

While tables rely on MSSQL Change Tracking for row-level synchronization, **SQL Views** are automatically kept in sync via definition comparisons.

The application continuously queries `sys.views` and `sys.sql_modules` to compare `CREATE VIEW` statements between the Primary and Replica databases. it will dynamically:
- Create new Views on the Replica if they are found on the Primary.
- Drop and Recreate Views on the Replica if their underlying query definition changes on the Primary.
- Drop Views on the Replica if they are no longer present on the Primary.

## Stored Procedure & Function Synchronization

Along with Views, the app actively monitors and syncs Stored Procedures and User-Defined Functions (`Scalar`, `Table-Valued`, and `Inline Table-Valued`). 

- Using `sys.objects` and `sys.sql_modules`, the logic tracks definition alterations for these routines.
- Changes or additions on the Primary are matched on the Replica by determining the correct drop types (`DROP PROCEDURE` or `DROP FUNCTION`) and recreating the script.
- **Triggers** (`TR`) are strictly ignored from this sync process to prevent event duplication loops or unwanted data cascading effects on the Replica.

## 🚀 Performance & Safety Optimizations

### 🏎️ Independent Table Concurrency (Non-Blocking Sync)
- **Decoupled Architecture:** The backend completely shatters traditional sequential queue processing. Each table is dynamically assigned its own independent asynchronous background process (`tokio::spawn`) that negotiates with an intelligent global `Semaphore` lock pool.
- **Queue Jump:** Modest tables (e.g., configurations or users) will sprint instantly through the data pipeline and synchronize within milliseconds, securely bypassing massive operational tables (e.g., hundreds of thousands of active products) which securely occupy a long-running thread limit seat. Fast syncs no longer suffer bottleneck gridlock.

### ⚡ Accelerated Bulk Synchronization (Full Load Batch Insert)
The historic `Force Full Load` feature no longer relies on executing row-by-row sequenced queries. It has been entirely re-architected to leverage **Massive Database Transactions** (`BEGIN TRAN ... COMMIT`).
- The system binds thousands of single-row insert parameters simultaneously and submits them under an encapsulated, localized database transaction block.
- This Batch Processing methodology evaluates and processes hundreds of thousands of rows (e.g. `Product` and `Customer` tables) instantaneously over the network.
- This specific transactional structure guarantees that the pipeline bypasses SQL Server's undocumented generic limitation (which often forcefully rips and resets the TCP connection (`os error 104`) when attempting to pipeline query strings of excessive lengths).

### 🛡️ Ironclad Data Protection (DDL Event Capture)
- **Blind-Drop Prevention:** The declarative engine historically monitored schema structure. If a user renamed a column on the primary using `sp_rename`, the scanner perceived a "missing" column and eagerly issued a `DROP COLUMN` on the replica—causing catastrophic data loss. This has been patched with a **Soft Drop Safety** toggle; automated blind drops are suspended to ensure 100% data preservation during transit.
- **Real-Time Event Capturing:** To structurally mirror genuine DDL operations, the system deploys **MSSQL Service Broker (Event Notification)** architecture on the primary database. A standalone Rust background worker continuously polls the `<SyncDDLQueue>`. Upon intercepting structural events (`DDL_TABLE_EVENTS`, `RENAME`), it unwraps the XML payload and deterministically replays the precise T-SQL script against the replica database.
- **Identity Constraints Reliability:** During massive Batch Inserts, the system perfectly synchronizes the required `SET IDENTITY_INSERT ON` flag by meticulously encapsulating it into the specific `sqlx::query` transaction block, removing pesky identity parsing conflicts entirely.

## Architecture

- **Primary**: MSSQL 2022 (Port 1434)
- **Replica**: MSSQL 2022 (Port 1435)
- **Redis**: Stores last synced version (Internal Port 6379)
- **App**: Rust service polling changes every 5 seconds.