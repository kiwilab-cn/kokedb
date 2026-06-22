//! End-to-end integration test for the intelligent-sync stack, driving the exact
//! server path (init_shared_services -> create_session_context -> parser ->
//! query) in-process against a live PostgreSQL. Realistic scenario: an
//! e-commerce catalog with an updatable orders table (DB-enforced watermark),
//! an append-only events log (identity PK), and a small keyless dimension.
//!
//! Run with a live DB (see Makefile `integration-test`):
//!   KOKEDB_TEST_DSN=...   (source) and PG_META_DSN=...  (meta)
//!   cargo test -p kokedb-query --test e2e_intelligent_sync -- --ignored --test-threads=1

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{Array, Int64Array, LargeStringArray, RecordBatch, StringArray, StringViewArray};
use kokedb_cache::foyer_hybrid::LruResultCache;
use kokedb_common::hash::get_plan_hash;
use kokedb_query::binder::{parser, query};
use kokedb_query::context::{create_session_context, init_shared_services, SharedServices};
use sqlx::postgres::PgPool;
use sqlx::Row;

fn source_dsn() -> String {
    std::env::var("KOKEDB_TEST_DSN")
        .unwrap_or_else(|_| "postgresql://postgres:123456@127.0.0.1:25432/postgres".into())
}

fn meta_dsn() -> String {
    std::env::var("PG_META_DSN")
        .unwrap_or_else(|_| "postgresql://postgres:123456@127.0.0.1:25432/kokedb".into())
}

/// Runs a SQL statement through the full server pipeline and collects all rows.
async fn run(shared: &SharedServices, sql: &str) -> Vec<RecordBatch> {
    use futures::TryStreamExt;
    let ctx = Arc::new(create_session_context(shared).expect("session ctx"));
    let plan = parser(sql).unwrap_or_else(|e| panic!("parse `{sql}`: {e}"));
    let key = get_plan_hash(&plan).unwrap();
    let stream = query(ctx, &plan, key, None)
        .await
        .unwrap_or_else(|e| panic!("exec `{sql}`: {e}"));
    stream.try_collect().await.expect("collect")
}

/// Like `run`, but with a freshness requirement (seconds): a cached table whose
/// snapshot is older than `secs` is read live from its source instead.
async fn run_fresh(shared: &SharedServices, sql: &str, secs: u64) -> Vec<RecordBatch> {
    use futures::TryStreamExt;
    let ctx = Arc::new(create_session_context(shared).expect("session ctx"));
    let plan = parser(sql).unwrap_or_else(|e| panic!("parse `{sql}`: {e}"));
    let key = get_plan_hash(&plan).unwrap();
    let stream = query(ctx, &plan, key, Some(secs))
        .await
        .unwrap_or_else(|e| panic!("exec `{sql}`: {e}"));
    stream.try_collect().await.expect("collect")
}

fn scalar_count(batches: &[RecordBatch]) -> i64 {
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("int64 count")
        .value(0)
}

/// Collects all values of a string column across batches.
fn collect_col(batches: &[RecordBatch], col: &str) -> Vec<String> {
    let mut out = Vec::new();
    for b in batches {
        let Ok(idx) = b.schema().index_of(col) else { continue };
        let arr: &dyn Array = b.column(idx);
        for i in 0..b.num_rows() {
            let v = if let Some(a) = arr.as_any().downcast_ref::<StringArray>() {
                a.value(i).to_string()
            } else if let Some(a) = arr.as_any().downcast_ref::<LargeStringArray>() {
                a.value(i).to_string()
            } else if let Some(a) = arr.as_any().downcast_ref::<StringViewArray>() {
                a.value(i).to_string()
            } else {
                continue;
            };
            out.push(v);
        }
    }
    out
}

/// Reads a string column from the single-row `SHOW TABLE METADATA` result.
fn meta_str(batches: &[RecordBatch], col: &str) -> String {
    let b = &batches[0];
    let idx = b.schema().index_of(col).unwrap_or_else(|_| panic!("no col {col}"));
    let arr: &dyn Array = b.column(idx);
    if let Some(a) = arr.as_any().downcast_ref::<StringArray>() {
        a.value(0).to_string()
    } else if let Some(a) = arr.as_any().downcast_ref::<LargeStringArray>() {
        a.value(0).to_string()
    } else if let Some(a) = arr.as_any().downcast_ref::<StringViewArray>() {
        a.value(0).to_string()
    } else {
        panic!("col {col} is not a string array: {:?}", arr.data_type())
    }
}

async fn seed_source(pool: &PgPool) {
    for stmt in [
        "DROP TABLE IF EXISTS e2e_orders, e2e_events, e2e_dim CASCADE",
        "DROP FUNCTION IF EXISTS e2e_touch() CASCADE",
        // Updatable table with a DB-enforced watermark (DEFAULT now() + trigger).
        "CREATE TABLE e2e_orders (id int PRIMARY KEY, amount numeric, status text, \
         updated_at timestamptz NOT NULL DEFAULT now())",
        "CREATE FUNCTION e2e_touch() RETURNS trigger AS $$ \
         BEGIN NEW.updated_at = now(); RETURN NEW; END; $$ LANGUAGE plpgsql",
        "CREATE TRIGGER e2e_orders_touch BEFORE UPDATE ON e2e_orders \
         FOR EACH ROW EXECUTE FUNCTION e2e_touch()",
        "INSERT INTO e2e_orders(id, amount, status) \
         SELECT g, (g*1.5)::numeric, 'new' FROM generate_series(1, 300) g",
        // Append-only log with an identity PK and no timestamp -> APPEND.
        "CREATE TABLE e2e_events (id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, \
         kind text, payload text)",
        "INSERT INTO e2e_events(kind, payload) SELECT 'click', 'p' FROM generate_series(1, 150)",
        // Small keyless dimension -> full refresh.
        "CREATE TABLE e2e_dim (code text, name text)",
        "INSERT INTO e2e_dim(code, name) VALUES ('US','United States'),('CN','China'),('JP','Japan')",
        "ANALYZE e2e_orders",
        "ANALYZE e2e_events",
    ] {
        sqlx::query(stmt).execute(pool).await.unwrap();
    }
}

async fn clean_meta(pool: &PgPool, catalog: &str) {
    for sql in [
        "DELETE FROM system.catalog WHERE name = $1",
        "DELETE FROM system.table_sync_policy WHERE catalog = $1",
        "DELETE FROM system.table_sync_state WHERE catalog = $1",
        "DELETE FROM system.table_arrow_schema WHERE catalog_name = $1",
        "DELETE FROM system.cache_job WHERE catalog = $1",
        "DELETE FROM system.database_policy WHERE catalog = $1",
    ] {
        let _ = sqlx::query(sql).bind(catalog).execute(pool).await;
    }
}

/// Polls until `table` has a committed snapshot (sync completed) or times out.
async fn wait_until_cached(meta: &PgPool, catalog: &str, table: &str, timeout: Duration) -> bool {
    let start = Instant::now();
    while start.elapsed() < timeout {
        let row = sqlx::query(
            "SELECT local_path FROM system.table_arrow_schema \
             WHERE catalog_name = $1 AND table_name = $2",
        )
        .bind(catalog)
        .bind(table)
        .fetch_optional(meta)
        .await
        .ok()
        .flatten();
        if let Some(r) = row {
            let p: Option<String> = r.try_get("local_path").ok();
            if p.map(|s| !s.is_empty()).unwrap_or(false) {
                return true;
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    false
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires PostgreSQL; run via KOKEDB_TEST_DSN + PG_META_DSN"]
async fn intelligent_sync_end_to_end() {
    let catalog = "e2e_shop";
    std::env::set_var("PG_META_DSN", meta_dsn());
    // Keep the scheduler quiet during the short test window; we rely on the
    // synchronous initial sync + reeval that CREATE CATALOG performs.
    std::env::set_var("KOKEDB_REEVALUATE_INTERVAL_MIN", "60");

    let source = PgPool::connect(&source_dsn()).await.unwrap();
    let meta = PgPool::connect(&meta_dsn()).await.unwrap();
    seed_source(&source).await;
    clean_meta(&meta, catalog).await;

    let result_cache = LruResultCache::new(2000, 40000).await.unwrap();
    let shared = init_shared_services(result_cache).await.unwrap();

    // 1. Create the catalog (triggers initial sync + adaptive reeval + inference).
    let create = format!(
        "CREATE CATALOG {catalog} USING '{}' WITH properties(cache_policy=\"select\", \
         table_set=\"public.e2e_orders,public.e2e_events,public.e2e_dim\")",
        source_dsn()
    );
    run(&shared, &create).await;

    // 2. Wait for the snapshots to land, then verify queries return correct data.
    assert!(
        wait_until_cached(&meta, catalog, "e2e_orders", Duration::from_secs(60)).await,
        "e2e_orders was not cached in time"
    );
    wait_until_cached(&meta, catalog, "e2e_events", Duration::from_secs(30)).await;
    wait_until_cached(&meta, catalog, "e2e_dim", Duration::from_secs(30)).await;

    let orders = run(&shared, &format!("SELECT count(*) FROM {catalog}.public.e2e_orders")).await;
    assert_eq!(scalar_count(&orders), 300, "orders count");
    let events = run(&shared, &format!("SELECT count(*) FROM {catalog}.public.e2e_events")).await;
    assert_eq!(scalar_count(&events), 150, "events count");
    let dim = run(&shared, &format!("SELECT count(*) FROM {catalog}.public.e2e_dim")).await;
    assert_eq!(scalar_count(&dim), 3, "dim count");

    // A filtered aggregate (exercises the resolver + read path end to end).
    let filtered = run(
        &shared,
        &format!("SELECT count(*) FROM {catalog}.public.e2e_orders WHERE id <= 100"),
    )
    .await;
    assert_eq!(scalar_count(&filtered), 100, "filtered orders");

    // 2b. The sync runs are recorded as cache jobs.
    let jobs = run(&shared, &format!("SHOW CACHE JOBS FROM CATALOG {catalog}")).await;
    let job_count: i64 = jobs.iter().map(|b| b.num_rows() as i64).sum();
    assert!(job_count >= 3, "expected a cache job per table, got {job_count}");
    // The orders sync should have succeeded.
    let orders_jobs =
        run(&shared, &format!("SHOW CACHE JOBS FROM TABLE {catalog}.public.e2e_orders")).await;
    assert!(orders_jobs[0].num_rows() >= 1, "no job for orders");
    assert_eq!(meta_str(&orders_jobs, "status"), "success", "orders sync status");

    // 3. Inference populated the metadata correctly.
    let md_orders = run(&shared, &format!("SHOW TABLE METADATA FROM {catalog}.public.e2e_orders")).await;
    assert_eq!(meta_str(&md_orders, "inc_mode"), "upsert", "orders -> upsert");
    assert_eq!(meta_str(&md_orders, "inc_tier"), "trusted", "DB-enforced -> trusted");
    assert_eq!(meta_str(&md_orders, "watermark_column"), "updated_at");

    let md_events = run(&shared, &format!("SHOW TABLE METADATA FROM {catalog}.public.e2e_events")).await;
    assert_eq!(meta_str(&md_events, "inc_mode"), "append", "events -> append");

    let md_dim = run(&shared, &format!("SHOW TABLE METADATA FROM {catalog}.public.e2e_dim")).await;
    assert_eq!(meta_str(&md_dim, "inc_mode"), "full", "keyless dim -> full");

    // 4. User override is honored and sticky.
    run(
        &shared,
        &format!("ALTER TABLE {catalog}.public.e2e_orders SET CACHE POLICY WITH (inc_mode = 'full')"),
    )
    .await;
    let md_after = run(&shared, &format!("SHOW TABLE METADATA FROM {catalog}.public.e2e_orders")).await;
    assert_eq!(meta_str(&md_after, "inc_mode"), "full", "override applied");
    assert_eq!(meta_str(&md_after, "source"), "user", "override source");

    // 5. Manual refresh returns a queued task and re-syncs new data.
    sqlx::query("INSERT INTO e2e_orders(id, amount, status) \
                 SELECT g, (g*1.5)::numeric, 'new' FROM generate_series(301, 350) g")
        .execute(&source)
        .await
        .unwrap();
    let refresh = run(
        &shared,
        &format!("REFRESH CACHE FROM TABLE {catalog}.public.e2e_orders"),
    )
    .await;
    assert_eq!(meta_str(&refresh, "status"), "QUEUED", "refresh queued");

    // The re-sync runs in the background; poll until the cached count catches up.
    let mut updated = false;
    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(60) {
        let c = run(&shared, &format!("SELECT count(*) FROM {catalog}.public.e2e_orders")).await;
        if scalar_count(&c) == 350 {
            updated = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert!(updated, "manual refresh did not pick up the 50 new orders");

    // 5a-fresh. Query-time freshness guard. Add rows to the SOURCE only, so the
    // cached snapshot (350) trails the live table (360).
    sqlx::query(
        "INSERT INTO e2e_orders(id, amount, status) \
         SELECT g, (g*1.5)::numeric, 'fresh' FROM generate_series(351, 360) g",
    )
    .execute(&source)
    .await
    .unwrap();

    // No freshness requirement -> the cached snapshot is served (still 350).
    let cached = run(&shared, &format!("SELECT count(id) FROM {catalog}.public.e2e_orders")).await;
    assert_eq!(scalar_count(&cached), 350, "default path serves the cached snapshot");

    // Make the snapshot look old, then demand freshness -> read live (360).
    sqlx::query(
        "UPDATE system.table_sync_state SET last_sync_at = now() - interval '1 hour' \
         WHERE catalog=$1 AND schema_name='public' AND table_name='e2e_orders'",
    )
    .bind(catalog)
    .execute(&meta)
    .await
    .unwrap();
    let fresh = run_fresh(
        &shared,
        &format!("SELECT count(id) FROM {catalog}.public.e2e_orders"),
        1,
    )
    .await;
    assert_eq!(scalar_count(&fresh), 360, "freshness-demanding query reads the live source");

    // Restore the source so the later fixed-count assertions hold.
    sqlx::query("DELETE FROM e2e_orders WHERE id > 350")
        .execute(&source)
        .await
        .unwrap();

    // 5b. Catalog-wide refresh queues every selected table.
    let cat_refresh = run(&shared, &format!("REFRESH CACHE FROM CATALOG {catalog}")).await;
    let queued: usize = cat_refresh.iter().map(|b| b.num_rows()).sum();
    assert_eq!(queued, 3, "REFRESH CACHE FROM CATALOG should queue all 3 tables");

    // 5c. Pin a refresh cadence; SHOW CACHE SCHEDULE reflects it and reeval won't
    //     overwrite it.
    run(
        &shared,
        &format!("ALTER TABLE {catalog}.public.e2e_orders SET CACHE POLICY WITH (refresh_interval = '2m')"),
    )
    .await;
    let (pinned_secs, pinned): (i32, bool) = sqlx::query_as(
        "SELECT refresh_interval_sec, cadence_pinned FROM system.table_sync_policy \
         WHERE catalog=$1 AND schema_name='public' AND table_name='e2e_orders'",
    )
    .bind(catalog)
    .fetch_one(&meta)
    .await
    .unwrap();
    assert_eq!(pinned_secs, 120, "refresh_interval pinned to 2m");
    assert!(pinned, "cadence should be pinned");

    let sched = run(&shared, &format!("SHOW CACHE SCHEDULE FROM CATALOG {catalog}")).await;
    let sched_tables = collect_col(&sched, "table");
    assert!(sched_tables.iter().any(|t| t == "e2e_orders"), "schedule lists orders");

    // 5d. Pause a table; DIAGNOSE CACHE flags it.
    run(
        &shared,
        &format!("PAUSE CACHE SCHEDULE FOR TABLE {catalog}.public.e2e_events"),
    )
    .await;
    let diag = run(&shared, &format!("DIAGNOSE CACHE FROM CATALOG {catalog}")).await;
    let issues = collect_col(&diag, "issue");
    let diag_tables = collect_col(&diag, "table");
    assert!(
        issues.iter().zip(&diag_tables).any(|(i, t)| i == "PAUSED" && t == "e2e_events"),
        "DIAGNOSE should report the paused table: issues={issues:?} tables={diag_tables:?}"
    );

    // 6. Disabling the database evicts its cached tables: the cache metadata is
    //    cleared (queries fall back to the live remote source), but the data is
    //    still correct.
    let cached_before: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM system.table_arrow_schema WHERE catalog_name = $1 AND schema_name = 'public'",
    )
    .bind(catalog)
    .fetch_one(&meta)
    .await
    .unwrap();
    assert!(cached_before >= 3, "tables should be cached before disable");

    run(
        &shared,
        &format!("ALTER DATABASE {catalog}.public SET CACHE POLICY WITH (mode = 'none')"),
    )
    .await;

    let cached_after: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM system.table_arrow_schema WHERE catalog_name = $1 AND schema_name = 'public'",
    )
    .bind(catalog)
    .fetch_one(&meta)
    .await
    .unwrap();
    assert_eq!(cached_after, 0, "disabling the database must evict its cached tables");

    // Queries still work, now served live from the remote source. (Use count(id)
    // rather than count(*): the remote provider can't yet scan a zero-column
    // projection — a separate, pre-existing limitation of the live read path.)
    let after_evict = run(&shared, &format!("SELECT count(id) FROM {catalog}.public.e2e_orders")).await;
    assert_eq!(scalar_count(&after_evict), 350, "remote fallback returns correct data");

    // Cleanup.
    clean_meta(&meta, catalog).await;
    for stmt in [
        "DROP TABLE IF EXISTS e2e_orders, e2e_events, e2e_dim CASCADE",
        "DROP FUNCTION IF EXISTS e2e_touch() CASCADE",
    ] {
        let _ = sqlx::query(stmt).execute(&source).await;
    }
}
