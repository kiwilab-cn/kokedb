# Benchmark: query acceleration vs. direct PostgreSQL

kokedb accelerates repeated analytical queries over an OLTP database by caching
the source tables as columnar parquet snapshots and caching query *results*.
This benchmark quantifies that against a direct-to-PostgreSQL baseline.

## Reproduce

```bash
# Defaults: 1,000,000 rows, ports 25432/3306/9090.
scripts/benchmark.sh
# or override ports / size:
ROWS=1000000 PG_PORT=25433 MYSQL_PORT=3307 METRICS_PORT=9091 scripts/benchmark.sh
# or via make:
make bench
```

The script brings up the full stack (`make up`), seeds a 1M-row table, then runs
the same aggregation three ways and prints server-recorded timings
(`system.sql_stats`) plus the `/metrics` cache counters.

## Setup

- Stack: `kokedb-server` + PostgreSQL 17.6 (docker compose), same host.
- Data: `public.bench`, **1,000,000 rows** — `id int, category text, amount float8`.
- Query (OLAP aggregation):
  ```sql
  SELECT category, count(*), sum(amount) FROM bench GROUP BY category;
  ```
- Timings: PostgreSQL `EXPLAIN ANALYZE` execution time (warm buffers); kokedb
  server-side cost from `system.sql_stats` (milliseconds), corroborated by the
  Prometheus `/metrics` cache counters.

## Results

| Path | Per-query | Notes |
|------|-----------|-------|
| **Baseline — direct PostgreSQL (warm)** | **~30 ms** | row-store hash aggregation over 1M rows |
| **kokedb cold — first run (parquet scan)** | **~30–50 ms** (one-time) | DataFusion vectorized scan of the parquet snapshot |
| **kokedb warm — result-cache hit** | **<1 ms** | cached result returned directly |

`system.sql_stats` across the runs: `min_ms=0` (warm) and `max_ms` equal to the
single cold run, with the total dominated by that one cold run — i.e. every warm
run costs sub-millisecond. `/metrics` corroborates exactly one
`cache_misses_total` (the cold run) and the rest as `cache_hits_total`.

## Takeaways

- **Repeated queries are ~30–50×+ faster** (~30 ms → sub-millisecond) and, just
  as importantly, run with **zero load on the source OLTP database** — the 13
  warm runs never touched PostgreSQL. This is the core value: result caching +
  automatic refresh on source change.
- **Cold columnar scan** is not faster than warm PostgreSQL for this *simple*
  aggregation (PG's in-memory hash aggregation is already efficient). Columnar
  parquet wins more on heavier scans / wide projections / multi-aggregate
  analytics; here the win comes from the result cache and OLTP offload.
- This is a same-host benchmark (no network round-trips). With kokedb deployed
  separately from the OLTP database, offloading repeated analytics avoids both
  source-database load and network latency, widening the gap. Cold runs also
  include first-touch parquet reads from disk (cold OS page cache).
