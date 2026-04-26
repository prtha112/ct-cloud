#!/bin/bash
set -e

/opt/mssql/bin/sqlservr &
PID=$!

echo "Waiting for SQL Server to be ready..."
for i in $(seq 1 30); do
    /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P "${MSSQL_SA_PASSWORD}" -Q "SELECT 1" > /dev/null 2>&1 && break
    sleep 2
done

if [ "${RUN_INIT:-false}" = "true" ]; then
    INIT_FILE="${INIT_FILE:-/init/init.sql}"
    echo "Running ${INIT_FILE}..."
    /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P "${MSSQL_SA_PASSWORD}" -i "${INIT_FILE}"
    echo "Init complete."
fi

wait $PID
