//! PostgreSQL startup/authentication handler backed by `system.app_user`.
//!
//! Uses cleartext password auth (the client sends the plaintext, which we
//! re-hash and compare to the stored `mysql_native_password` digest, so one
//! credential works for both wire protocols). On success the user's catalog
//! allow-list is stashed in the connection metadata for the query handler to
//! apply. When no users exist authentication is disabled (open access).

use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use futures::sink::{Sink, SinkExt};
use kokedb_query::context::SharedServices;
use pgwire::api::auth::{
    finish_authentication, protocol_negotiation, save_startup_parameters_to_metadata,
    DefaultServerParameterProvider, LoginInfo, StartupHandler,
};
use pgwire::api::{ClientInfo, PgWireConnectionState, PidSecretKeyGenerator,
    RandomPidSecretKeyGenerator};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::startup::Authentication;
use pgwire::messages::{PgWireBackendMessage, PgWireFrontendMessage};

/// Metadata keys used to carry the authenticated identity to the query handler.
pub const META_SUPERUSER: &str = "kokedb_superuser";
pub const META_ALLOWED_CATALOGS: &str = "kokedb_allowed_catalogs";

pub struct KokedbStartupHandler {
    shared: SharedServices,
}

impl KokedbStartupHandler {
    pub fn new(shared: SharedServices) -> Self {
        Self { shared }
    }

    /// Authentication is enforced whenever any account exists — checked per
    /// connection, so `CREATE USER` takes effect without a restart.
    async fn auth_enabled(&self) -> bool {
        self.shared.meta().count_app_users().await.unwrap_or(0) > 0
    }
}

fn auth_err(user: &str) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "FATAL".to_string(),
        "28P01".to_string(),
        format!("password authentication failed for user \"{user}\""),
    )))
}

#[async_trait]
impl StartupHandler for KokedbStartupHandler {
    async fn on_startup<C>(&self, client: &mut C, message: PgWireFrontendMessage) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        match message {
            PgWireFrontendMessage::Startup(ref startup) => {
                protocol_negotiation(client, startup).await?;
                save_startup_parameters_to_metadata(client, startup);
                if self.auth_enabled().await {
                    client.set_state(PgWireConnectionState::AuthenticationInProgress);
                    client
                        .send(PgWireBackendMessage::Authentication(
                            Authentication::CleartextPassword,
                        ))
                        .await?;
                } else {
                    let (pid, secret_key) =
                        RandomPidSecretKeyGenerator::default().generate(client);
                    client.set_pid_and_secret_key(pid, secret_key);
                    finish_authentication(client, &DefaultServerParameterProvider::default()).await?;
                }
            }
            PgWireFrontendMessage::PasswordMessageFamily(pwd) => {
                let pwd = pwd.into_password()?;
                let user = LoginInfo::from_client_info(client)
                    .user()
                    .unwrap_or_default()
                    .to_string();

                let app_user = self
                    .shared
                    .meta()
                    .get_app_user(&user)
                    .await
                    .map_err(|e| PgWireError::ApiError(Box::new(std::io::Error::other(e.to_string()))))?;

                let Some(app_user) = app_user else {
                    return Err(auth_err(&user));
                };
                if !kokedb_common::auth::verify_cleartext(&app_user.auth_digest, &pwd.password) {
                    return Err(auth_err(&user));
                }

                // Stash the identity for the query handler to scope catalogs.
                let superuser = app_user.allowed_catalogs.is_none();
                client
                    .metadata_mut()
                    .insert(META_SUPERUSER.to_string(), superuser.to_string());
                if let Some(set) = &app_user.allowed_catalogs {
                    let mut names: Vec<&str> = set.iter().map(|s| s.as_str()).collect();
                    names.sort_unstable();
                    client
                        .metadata_mut()
                        .insert(META_ALLOWED_CATALOGS.to_string(), names.join(","));
                }

                let (pid, secret_key) = RandomPidSecretKeyGenerator::default().generate(client);
                client.set_pid_and_secret_key(pid, secret_key);
                finish_authentication(client, &DefaultServerParameterProvider::default()).await?;
            }
            _ => {}
        }
        Ok(())
    }
}

/// Derives the catalog allow-list from connection metadata set at auth time.
/// `None` means unrestricted (superuser or authentication disabled).
pub fn acl_from_client<C: ClientInfo>(
    client: &C,
) -> Option<Arc<std::collections::HashSet<String>>> {
    let meta = client.metadata();
    if meta.get(META_SUPERUSER).map(|v| v == "true").unwrap_or(false) {
        return None;
    }
    match meta.get(META_ALLOWED_CATALOGS) {
        Some(csv) => Some(Arc::new(
            csv.split(',')
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
                .map(|s| s.to_string())
                .collect(),
        )),
        // No identity stashed → authentication disabled → unrestricted.
        None => None,
    }
}
