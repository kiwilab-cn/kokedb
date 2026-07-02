pub mod auth;
pub mod encode;
pub mod handler;
pub mod tls;

use kokedb_query::context::SharedServices;
use pgwire::tokio::process_socket;
use tokio::net::TcpListener;

use crate::handler::KokedbHandlers;

/// Runs the PostgreSQL wire server on `bind_addr` using already-initialized
/// shared services, so it can share one `SharedServices` with other front-ends
/// in a unified process.
pub async fn serve(shared: SharedServices, bind_addr: String) -> std::io::Result<()> {
    // Auth is dynamic: enforced whenever `system.app_user` is non-empty,
    // re-checked per connection so CREATE USER takes effect without a restart.
    if shared.meta().count_app_users().await.unwrap_or(0) > 0 {
        log::info!("PostgreSQL authentication enabled (system.app_user)");
    } else {
        log::warn!("PostgreSQL authentication disabled (no rows in system.app_user)");
    }

    // Opt-in TLS: set KOKEDB_PG_TLS_CERT + KOKEDB_PG_TLS_KEY (PEM paths).
    // Misconfiguration is a hard error — silently downgrading to plaintext
    // when the operator asked for TLS would be worse than failing to start.
    let tls_acceptor = tls::acceptor_from_env()?;
    if tls_acceptor.is_some() {
        log::info!("PostgreSQL TLS enabled");
    }

    let handlers = KokedbHandlers::new(shared);
    let listener = TcpListener::bind(&bind_addr).await?;
    log::info!("kokedb PostgreSQL wire server listening on {bind_addr}");
    loop {
        let (socket, _) = listener.accept().await?;
        // Per-connection clone: gives the connection its own session state.
        let handlers = handlers.clone();
        let tls = tls_acceptor.clone();
        tokio::spawn(async move {
            if let Err(e) = process_socket(socket, tls, handlers).await {
                log::error!("PostgreSQL connection error: {e}");
            }
        });
    }
}
