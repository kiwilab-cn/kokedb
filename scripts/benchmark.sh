#!/usr/bin/env bash
#
# kokedb query-acceleration benchmark.
#
# Compares an OLAP aggregation run three ways on the same 1M-row table:
#   1. baseline    — directly against the source PostgreSQL (EXPLAIN ANALYZE)
#   2. kokedb cold — first run through kokedb (scans the parquet snapshot)
#   3. kokedb warm — repeated run (served from the result cache)
#
# It brings the full stack up (docker compose), seeds data, runs the queries,
# and prints a summary from the server-recorded timings (system.sql_stats) and
# the Prometheus /metrics endpoint.
#
# Usage:
#   scripts/benchmark.sh
#   ROWS=5000000 PG_PORT=25433 MYSQL_PORT=3307 METRICS_PORT=9091 scripts/benchmark.sh
#   KEEP=1 scripts/benchmark.sh        # leave the stack running afterwards
#
# Override ports if the defaults collide with something already running.
set -euo pipefail
cd "$(dirname "$0")/.."

ROWS="${ROWS:-1000000}"
PG_PORT="${PG_PORT:-25432}"
MYSQL_PORT="${MYSQL_PORT:-3306}"
METRICS_PORT="${METRICS_PORT:-9090}"
WARM_RUNS="${WARM_RUNS:-10}"
PG_CONTAINER="${PG_CONTAINER:-kokedb-postgres}"
SERVER_CONTAINER="${SERVER_CONTAINER:-kokedb-server}"

export PG_PORT MYSQL_PORT METRICS_PORT

QUERY="SELECT category, count(*), sum(amount) FROM benchcat.public.bench GROUP BY category"
PG_QUERY="SELECT category, count(*), sum(amount) FROM public.bench GROUP BY category"

psql_pg() { docker exec "$PG_CONTAINER" psql -U postgres -d postgres -tAc "$1"; }
psql_meta() { docker exec "$PG_CONTAINER" psql -U postgres -d kokedb -tAc "$1"; }
run_mysql() { mysql -h 127.0.0.1 -P "$MYSQL_PORT" -u root --connect-timeout=20 -e "$1" 2>/dev/null; }

echo "==> Starting stack (PG_PORT=$PG_PORT MYSQL_PORT=$MYSQL_PORT METRICS_PORT=$METRICS_PORT)"
make up >/dev/null

echo "==> Waiting for server to become healthy"
for _ in $(seq 1 60); do
  [ "$(docker inspect -f '{{.State.Health.Status}}' "$SERVER_CONTAINER" 2>/dev/null)" = "healthy" ] && break
  sleep 2
done

echo "==> Seeding $ROWS rows into public.bench"
psql_pg "DROP TABLE IF EXISTS public.bench;" >/dev/null
psql_pg "CREATE TABLE public.bench AS
         SELECT g AS id, 'cat' || (g % 10) AS category, (random()*1000)::float8 AS amount
         FROM generate_series(1, $ROWS) g;" >/dev/null
echo "    rows: $(psql_pg "SELECT count(*) FROM public.bench;")"

echo "==> [1] Baseline: direct PostgreSQL (EXPLAIN ANALYZE x3)"
for _ in 1 2 3; do
  docker exec "$PG_CONTAINER" psql -U postgres -d postgres -c \
    "EXPLAIN (ANALYZE, TIMING OFF) $PG_QUERY" 2>/dev/null | grep "Execution Time" | sed 's/^/    /'
done

echo "==> Creating kokedb catalog (cache_policy=all) and waiting for sync"
run_mysql "DROP CATALOG IF EXISTS benchcat;" || true
run_mysql "CREATE CATALOG benchcat USING postgresql://postgres:123456@postgres:5432/postgres with properties(cache_policy=\"all\");"
for _ in $(seq 1 60); do
  P="$(psql_meta "SELECT local_path FROM system.table_arrow_schema WHERE table_name='bench';")"
  [ -n "$P" ] && { echo "    synced -> $P"; break; }
  sleep 2
done

echo "==> [2] kokedb COLD run (parquet scan)"
run_mysql "$QUERY" | sed 's/^/    /'

echo "==> waiting for async result-cache write"
sleep 4

echo "==> [3] kokedb WARM runs (cached result) x$WARM_RUNS"
for _ in $(seq 1 "$WARM_RUNS"); do run_mysql "$QUERY" >/dev/null; done

echo
echo "================= RESULTS ================="
echo "Rows: $ROWS    Query: GROUP BY category -> count, sum"
echo
echo "Baseline (direct PostgreSQL, warm): see [1] Execution Time above (~tens of ms)"
echo
echo "kokedb server-recorded timings (system.sql_stats, ms):"
psql_meta "SELECT 'count='||count||'  min_ms='||min_time||'  max_ms(cold)='||max_time||
           '  total_ms='||execution_time
           FROM system.sql_stats WHERE sql_text LIKE '%benchcat.public.bench%';" | sed 's/^/    /'
echo
echo "Prometheus /metrics (cache hits vs misses):"
curl -s "http://127.0.0.1:$METRICS_PORT/metrics" | grep -E "kokedb_cache_(hits|misses)_total " | sed 's/^/    /'
echo "==========================================="
echo
echo "min_ms ~ warm (result-cache hit);  max_ms ~ cold (parquet scan)."
if [ "${KEEP:-0}" = "1" ]; then
  echo "Stack left running. Tear down with: PG_PORT=$PG_PORT MYSQL_PORT=$MYSQL_PORT METRICS_PORT=$METRICS_PORT make reset"
else
  echo "==> Tearing down"
  make reset >/dev/null
fi
