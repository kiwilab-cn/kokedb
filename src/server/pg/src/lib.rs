pub mod auth;
pub mod encode;
pub mod handler;

use kokedb_query::context::SharedServices;
use pgwire::tokio::process_socket;
use tokio::net::TcpListener;

use crate::handler::KokedbHandlers;

/// Runs the PostgreSQL wire server on `bind_addr` using already-initialized
/// shared services, so it can share one `SharedServices` with other front-ends
/// in a unified process.
pub async fn serve(shared: SharedServices, bind_addr: String) -> std::io::Result<()> {
    // Auth is enforced when any application user exists in the meta store.
    let auth_enabled = shared.meta().count_app_users().await.unwrap_or(0) > 0;
    if auth_enabled {
        log::info!("PostgreSQL authentication enabled (system.app_user)");
    } else {
        log::warn!("PostgreSQL authentication disabled (no rows in system.app_user)");
    }
    let handlers = KokedbHandlers::new(shared, auth_enabled);
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
