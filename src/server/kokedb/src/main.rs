//! Unified kokedb server: builds the shared services (meta store, sync
//! scheduler, plan + result caches) exactly once and serves both the MySQL and
//! PostgreSQL wire protocols from that single instance — so the background sync
//! scheduler runs once, not once per protocol.

use kokedb_cache::result_cache::ResultCache;
use kokedb_common::opentelemetry::init_logger;
use kokedb_query::context::init_shared_services;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_logger().ok();

    // Prometheus metrics endpoint (best-effort), started once for the process.
    let metrics_addr =
        std::env::var("KOKEDB_METRICS_ADDR").unwrap_or_else(|_| "0.0.0.0:9090".to_string());
    if !metrics_addr.is_empty() {
        tokio::spawn(kokedb_mysql_svc::metrics::serve_metrics(metrics_addr));
    }

    // One set of shared services for every front-end (single sync scheduler).
    let result_cache = ResultCache::from_env(2000, 40000).await?;
    let shared = init_shared_services(result_cache).await?;

    let mysql_addr = std::env::var("KOKEDB_BIND_ADDR").unwrap_or_else(|_| "0.0.0.0:3306".to_string());
    let pg_addr =
        std::env::var("KOKEDB_PG_BIND_ADDR").unwrap_or_else(|_| "0.0.0.0:5433".to_string());

    let mysql = tokio::spawn({
        let shared = shared.clone();
        async move { kokedb_mysql_svc::serve(shared, mysql_addr).await }
    });
    let pg = tokio::spawn(async move { kokedb_pg_svc::serve(shared, pg_addr).await });

    // Either listener exiting (only happens on a fatal accept/bind error) brings
    // the process down so the supervisor can restart it. The outer `?` unwraps
    // the JoinError, the inner one the server error; both coerce to Box<dyn Error>.
    tokio::select! {
        r = mysql => { r??; }
        r = pg => { r??; }
    }
    Ok(())
}
