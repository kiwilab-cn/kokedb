use kokedb_cache::result_cache::ResultCache;
use kokedb_common::opentelemetry::init_logger;
use kokedb_mysql_svc::error::MysqlServerError;
use kokedb_mysql_svc::{metrics, serve};
use kokedb_query::context::init_shared_services;
use log::error;

/// Standalone MySQL-only server. The unified `kokedb-server` binary builds the
/// shared services once and runs this alongside the PostgreSQL front-end.
#[tokio::main]
async fn main() -> Result<(), MysqlServerError> {
    init_logger().unwrap();

    // Prometheus metrics endpoint (best-effort; non-fatal if it can't bind).
    let metrics_addr =
        std::env::var("KOKEDB_METRICS_ADDR").unwrap_or_else(|_| "0.0.0.0:9090".to_string());
    if !metrics_addr.is_empty() {
        tokio::spawn(metrics::serve_metrics(metrics_addr));
    }

    let result_cache = ResultCache::from_env(2000, 40000).await?;
    let shared = init_shared_services(result_cache).await.map_err(|e| {
        error!("Failed to init shared services: {e}");
        MysqlServerError::InternalError(e.to_string())
    })?;

    let bind_addr = std::env::var("KOKEDB_BIND_ADDR").unwrap_or_else(|_| "0.0.0.0:3306".to_string());
    serve(shared, bind_addr).await
}
