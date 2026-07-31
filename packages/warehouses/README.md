# Warehouse clients

Package contains connectors to different data warehouses.

## Postgres / Redshift

Warehouse queries use a one-shot `client.query` (no named portals / `pg-cursor`).
That keeps connections in `ReadyForQuery` after success or error, which is what
transaction-mode poolers (pgbouncer, pgdog) expect.

Tradeoff: the full result set is buffered in Node memory per query. Chart and
dashboard traffic is usually fine under existing query row limits; very large
unlimited exports may need a separate streaming path later (`COPY`, session-mode
pooler). Concurrent streams are capped with
`LIGHTDASH_POSTGRES_WAREHOUSE_MAX_CONCURRENT` (default 10).
