# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Is

MSSQL Change Tracking replication service: continuously mirrors a **Primary MSSQL** to a **Replica MSSQL**, with Redis tracking sync state. Consists of three components:

| Component | Tech | Purpose |
|-----------|------|---------|
| `backend/` | Rust (tokio + sqlx + redis) | Core replication engine |
| `frontend/` | Next.js 16 + TypeScript + Tailwind v4 | Dashboard UI |
| `seed/` | Rust | One-time test data seeder |

## Commands

### Full stack (Docker)
```bash
docker-compose up -d --build   # start all services
docker-compose logs -f backend # tail replication logs
```

### Backend (local dev)
```bash
cd backend
MSSQL_PRIMARY_URL=... MSSQL_REPLICA_URL=... REDIS_URL=... cargo run
cargo build --release
```

### Frontend (local dev)
```bash
cd frontend
npm install
npm run dev        # http://localhost:3000
npm run lint
npm run build
```

### Seed test data
```bash
cd seed
MSSQL_PRIMARY_URL=... cargo run
```

### Redis state management
```bash
# Enable sync for a table
docker exec redis_sync_state redis-cli SET mssql_sync:enabled:TableName "true"

# Trigger force full load
docker exec redis_sync_state redis-cli SET mssql_sync:force_full_load:TableName "true"

# Manually set version (for huge tables after manual snapshot)
docker exec redis_sync_state redis-cli SET mssql_sync:version:TableName "850550"
```

## Architecture

### Backend Concurrency Model

`main.rs` runs a 5-second polling loop that queries `sys.change_tracking_tables`. For each tracked table:
1. A `tokio::spawn` task is created (non-blocking)
2. The task acquires a permit from a global `Semaphore(SYNC_THREADS)` before doing real work
3. An `Arc<TokioMutex<HashSet<String>>>` (`active_tasks`) prevents duplicate spawns for the same table

Two additional background workers run independently:
- **DDL consumer** (`ddl_events.rs`): polls `SyncDDLQueue` via Service Broker `WAITFOR RECEIVE` with 5s timeout, replays DDL events on replica
- **Views/Routines sync** (`schema.rs`): runs in the main loop every 5s via `sys.views` / `sys.sql_modules` diff

### Redis Key Namespace

```
mssql_sync:version:{Table}         # last synced CT version (i64)
mssql_sync:enabled:{Table}         # "true"/"false" — gate for all sync ops
mssql_sync:force_full_load:{Table} # "true"/"false" — triggers full TRUNCATE+INSERT
mssql_sync:progress:{Table}        # JSON: {synced, total, startedAt, updatedAt}
mssql_sync:config:primary_url      # sanitized URL shown in frontend
mssql_sync:config:replica_url
```

New tables discovered via CT default to `enabled=false` and `force_full_load=false` (SETNX).

### sqlx Type Handling Quirks

All columns in SELECT are cast to `VARCHAR`/`NVARCHAR` to avoid sqlx panics:
- Numeric types → `CAST AS VARCHAR(100)`
- Datetime types → `CONVERT(VARCHAR(100), ..., 126)` (ISO 8601)
- `text` → `CAST AS VARCHAR(8000)` (avoids sqlx LOB stream bug)
- `ntext` → `CAST AS NVARCHAR(4000)` (same reason)

All values are bound as `Option<String>` and re-inserted as strings on the replica.

### Upsert Pattern

Incremental sync does **not** use MERGE. Pattern: bulk `DELETE FROM replica WHERE PK IN (...)` then `INSERT`. Tables with identity columns wrap inserts in `SET IDENTITY_INSERT ON/OFF` within the same transaction.

### Frontend API Routes

- `GET /api/tables` — reads all `mssql_sync:enabled:*` keys from Redis
- `GET /api/tables/[id]` — reads progress/version/flags for a single table
- `POST /api/tables/[id]` — toggles `enabled` or `force_full_load` flags
- `GET /api/config` — reads `mssql_sync:config:*` for display

Frontend connects to Redis directly via `ioredis` (server-side only, in Route Handlers).

## Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `MSSQL_PRIMARY_URL` | Yes | — | `mssql://sa:pass@host:port/db` |
| `MSSQL_REPLICA_URL` | Yes | — | Must differ from PRIMARY |
| `REDIS_URL` | Yes | — | `redis://host:port` |
| `SYNC_THREADS` | No | `1` | Semaphore concurrency limit |
| `RUST_LOG` | No | — | e.g. `error,backend=info` |

Frontend uses `REDIS_URL` only.

## Ports (docker-compose)

| Service | Host Port |
|---------|-----------|
| Primary MSSQL | 1434 |
| Replica MSSQL | 1435 |
| Redis | 6380 |
| Frontend | 3000 |

Credentials: `sa` / `Password123!`
