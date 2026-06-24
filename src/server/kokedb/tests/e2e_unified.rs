//! Verifies the unified server runs both wire protocols from a single
//! `SharedServices` (one sync scheduler): a PostgreSQL client drives
//! CREATE CATALOG + a query through the engine, and the MySQL front-end is
//! listening on the same instance.
//!
//!   KOKEDB_TEST_DSN=postgresql://postgres:123456@127.0.0.1:25433/postgres
//!   PG_META_DSN=postgresql://postgres:123456@127.0.0.1:25433/kokedb
//!   cargo test -p kokedb-server --test e2e_unified -- --ignored --test-threads=1

use std::time::Duration;

use kokedb_cache::result_cache::ResultCache;
use kokedb_query::context::init_shared_services;
use sqlx::postgres::PgPool;
use sqlx::Row as _;
use tokio::net::{TcpListener, TcpStream};
use tokio_postgres::SimpleQueryMessage;

fn source_dsn() -> String {
    std::env::var("KOKEDB_TEST_DSN")
        .unwrap_or_else(|_| "postgresql://postgres:123456@127.0.0.1:25433/postgres".into())
}
fn meta_dsn() -> String {
    std::env::var("PG_META_DSN")
        .unwrap_or_else(|_| "postgresql://postgres:123456@127.0.0.1:25433/kokedb".into())
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

async fn wait_until_cached(meta: &PgPool, catalog: &str, table: &str, timeout: Duration) -> bool {
    let start = std::time::Instant::now();
    while start.elapsed() < timeout {
        if let Some(r) = sqlx::query(
            "SELECT local_path FROM system.table_arrow_schema \
             WHERE catalog_name = $1 AND table_name = $2",
        )
        .bind(catalog)
        .bind(table)
        .fetch_optional(meta)
        .await
        .ok()
        .flatten()
        {
            if r.try_get::<Option<String>, _>("local_path")
                .ok()
                .flatten()
                .map(|s| !s.is_empty())
                .unwrap_or(false)
            {
                return true;
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    false
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires PostgreSQL (source + meta)"]
async fn unified_serves_both_protocols_from_one_instance() {
    let catalog = "unified_shop";
    std::env::set_var("PG_META_DSN", meta_dsn());
    std::env::set_var("KOKEDB_REEVALUATE_INTERVAL_MIN", "60");

    let source = PgPool::connect(&source_dsn()).await.unwrap();
    for stmt in [
        "DROP TABLE IF EXISTS uni_items",
        "CREATE TABLE uni_items (id int PRIMARY KEY, name text)",
        "INSERT INTO uni_items SELECT g, 'n' || g FROM generate_series(1, 30) g",
    ] {
        sqlx::query(stmt).execute(&source).await.unwrap();
    }
    let meta = PgPool::connect(&meta_dsn()).await.unwrap();
    clean_meta(&meta, catalog).await;

    // One shared services instance for both front-ends.
    let result_cache = ResultCache::local(64, 64).await.unwrap();
    let shared = init_shared_services(result_cache).await.unwrap();

    // Bind both listeners on ephemeral ports, then hand them to the serve loops.
    let pg_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mysql_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let pg_addr = pg_listener.local_addr().unwrap();
    let mysql_addr = mysql_listener.local_addr().unwrap();
    drop(pg_listener);
    drop(mysql_listener);

    let s1 = shared.clone();
    let pg_bind = format!("127.0.0.1:{}", pg_addr.port());
    tokio::spawn(async move {
        let _ = kokedb_pg_svc::serve(s1, pg_bind).await;
    });
    let mysql_bind = format!("127.0.0.1:{}", mysql_addr.port());
    tokio::spawn(async move {
        let _ = kokedb_mysql_svc::serve(shared, mysql_bind).await;
    });
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Drive CREATE CATALOG + a query over the PostgreSQL wire.
    let conn_str = format!("host=127.0.0.1 port={} user=postgres dbname=kokedb", pg_addr.port());
    let (client, connection) = tokio_postgres::connect(&conn_str, tokio_postgres::NoTls)
        .await
        .expect("connect to unified pg wire");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .simple_query(&format!(
            "CREATE CATALOG {catalog} USING '{}' WITH properties(cache_policy=\"select\", \
             table_set=\"public.uni_items\")",
            source_dsn()
        ))
        .await
        .unwrap();
    assert!(
        wait_until_cached(&meta, catalog, "uni_items", Duration::from_secs(60)).await,
        "uni_items not cached via the unified server"
    );
    let rows = client
        .simple_query(&format!("SELECT count(*) AS c FROM {catalog}.public.uni_items"))
        .await
        .unwrap();
    let count = rows.iter().find_map(|m| match m {
        SimpleQueryMessage::Row(r) => r.get("c").map(|s| s.to_string()),
        _ => None,
    });
    assert_eq!(count.as_deref(), Some("30"), "query over the PG front-end");

    // The MySQL front-end is listening on the same instance.
    assert!(
        TcpStream::connect(("127.0.0.1", mysql_addr.port())).await.is_ok(),
        "MySQL front-end should be accepting connections on the same instance"
    );

    clean_meta(&meta, catalog).await;
    let _ = sqlx::query("DROP TABLE IF EXISTS uni_items").execute(&source).await;
}
