use kokedb_cache::result_cache::ResultCache;
use kokedb_pg_svc::handler::KokedbHandlers;
use kokedb_common::opentelemetry::init_logger;
use kokedb_query::context::init_shared_services;
use pgwire::tokio::process_socket;
use tokio::net::TcpListener;

/// kokedb's PostgreSQL wire-protocol server. Speaks the simple query protocol to
/// libpq clients (psql, drivers) and runs queries through the same engine as the
/// MySQL server (shared meta store, plan cache, and result cache).
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_logger().ok();

    let bind_addr =
        std::env::var("KOKEDB_PG_BIND_ADDR").unwrap_or_else(|_| "0.0.0.0:5433".to_string());

    let result_cache = ResultCache::from_env(2000, 40000).await?;
    let shared = init_shared_services(result_cache).await?;
    let handlers = KokedbHandlers::new(shared);

    let listener = TcpListener::bind(&bind_addr).await?;
    log::info!("kokedb PostgreSQL wire server listening on {bind_addr}");

    loop {
        let (socket, _) = listener.accept().await?;
        let handlers = handlers.clone();
        tokio::spawn(async move {
            if let Err(e) = process_socket(socket, None, handlers).await {
                log::error!("PostgreSQL connection error: {e}");
            }
        });
    }
}
