use kokedb_cache::result_cache::ResultCache;
use kokedb_common::opentelemetry::init_logger;
use kokedb_query::context::init_shared_services;

/// Standalone PostgreSQL-only server. The unified `kokedb-server` binary builds
/// the shared services once and runs this alongside the MySQL front-end.
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_logger().ok();

    let bind_addr =
        std::env::var("KOKEDB_PG_BIND_ADDR").unwrap_or_else(|_| "0.0.0.0:5433".to_string());

    let result_cache = ResultCache::from_env(2000, 40000).await?;
    let shared = init_shared_services(result_cache).await?;

    kokedb_pg_svc::serve(shared, bind_addr).await?;
    Ok(())
}
