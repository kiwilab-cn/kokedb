//! PostgreSQL startup/authentication handler backed by `system.app_user`.
//!
//! Uses cleartext password auth (the client sends the plaintext, which we
//! re-hash and compare to the stored `mysql_native_password` digest, so one
//! credential works for both wire protocols). On success the session's
//! authorization — grant scopes plus parsed row-level security policies — is
//! published into the connection's shared [`AuthSlot`], which the query
//! handler of the same connection reads. When no users exist authentication
//! is disabled (open access) and the slot stays unrestricted.

use std::fmt::Debug;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use futures::sink::{Sink, SinkExt};
use kokedb_catalog::acl::RowPolicy;
use kokedb_query::context::{parse_row_policies, SharedServices};
use pgwire::api::auth::{
    finish_authentication, protocol_negotiation, save_startup_parameters_to_metadata,
    DefaultServerParameterProvider, LoginInfo, StartupHandler,
};
use pgwire::api::{ClientInfo, PgWireConnectionState, PidSecretKeyGenerator,
    RandomPidSecretKeyGenerator};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::startup::Authentication;
use pgwire::messages::{PgWireBackendMessage, PgWireFrontendMessage};

/// The authenticated session's authorization state.
#[derive(Debug, Clone, Default)]
pub struct SessionAuth {
    /// Grant scopes; `None` = unrestricted (superuser / auth disabled).
    pub scopes: Option<Arc<std::collections::HashSet<String>>>,
    /// Parsed row-level security policies (empty for unrestricted sessions).
    pub policies: Vec<RowPolicy>,
}

/// Shared between one connection's startup handler and query handler: the
/// startup handler writes it once on successful authentication; the query
/// handler reads it per statement. An empty slot (auth disabled, or auth not
/// yet completed — pgwire won't dispatch queries before then) means
/// unrestricted.
pub type AuthSlot = Arc<Mutex<Option<SessionAuth>>>;

pub struct KokedbStartupHandler {
    shared: SharedServices,
    slot: AuthSlot,
}

impl KokedbStartupHandler {
    pub fn new(shared: SharedServices, slot: AuthSlot) -> Self {
        Self { shared, slot }
    }

    /// Authentication is enforced whenever any account exists — checked per
    /// connection, so `CREATE USER` takes effect without a restart.
    async fn auth_enabled(&self) -> bool {
        self.shared.meta().count_app_users().await.unwrap_or(0) > 0
    }

    fn publish(&self, auth: SessionAuth) {
        if let Ok(mut slot) = self.slot.lock() {
            *slot = Some(auth);
        }
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

                // Publish the session's authorization for the query handler.
                // Row policies only apply to restricted sessions; failing to
                // load them rejects the login (fail-closed).
                let scopes = app_user.allowed_catalogs.map(Arc::new);
                let policies = if scopes.is_some() {
                    let rows = self.shared.meta().list_row_policies(&user).await.map_err(|e| {
                        PgWireError::ApiError(Box::new(std::io::Error::other(format!(
                            "failed to load row policies: {e}"
                        ))))
                    })?;
                    parse_row_policies(rows)
                } else {
                    Vec::new()
                };
                self.publish(SessionAuth { scopes, policies });

                let (pid, secret_key) = RandomPidSecretKeyGenerator::default().generate(client);
                client.set_pid_and_secret_key(pid, secret_key);
                finish_authentication(client, &DefaultServerParameterProvider::default()).await?;
            }
            _ => {}
        }
        Ok(())
    }
}
