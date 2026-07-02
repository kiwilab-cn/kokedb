//! User management commands (`CREATE USER` / `DROP USER` / `SHOW USERS` /
//! `GRANT CATALOG` / `REVOKE CATALOG`), backed by `system.app_user`.
//!
//! Authorization model: only *unrestricted* sessions — superusers, or any
//! session while authentication is disabled — may manage users. Sessions
//! scoped to a catalog allow-list are denied. Changes take effect live: the
//! wire front-ends consult `system.app_user` on every authentication attempt.

use kokedb_common::auth::password_digest;

use crate::display::UserDisplay;
use crate::error::{CatalogError, CatalogResult};
use crate::manager::CatalogManager;
use crate::utils::match_pattern;

impl CatalogManager {
    /// Rejects ACL-scoped sessions: user management is a superuser operation.
    fn require_admin(&self) -> CatalogResult<()> {
        if self.is_restricted() {
            return Err(CatalogError::NotSupported(
                "user management requires superuser privileges".to_string(),
            ));
        }
        Ok(())
    }

    /// Backs `CREATE USER name WITH properties(password="...",
    /// superuser="true", catalogs="a,b")`. An omitted password creates a
    /// passwordless account; omitted catalogs create a user with no catalog
    /// access (grant later).
    pub async fn create_user(
        &self,
        username: &str,
        options: Vec<(String, String)>,
    ) -> CatalogResult<()> {
        self.require_admin()?;
        if username.is_empty() {
            return Err(CatalogError::InvalidArgument(
                "username must not be empty".to_string(),
            ));
        }

        let mut password: Option<String> = None;
        let mut superuser = false;
        let mut catalogs = String::new();
        for (key, value) in options {
            match key.to_ascii_lowercase().as_str() {
                "password" => password = Some(value),
                "superuser" => superuser = value.eq_ignore_ascii_case("true"),
                "catalogs" => catalogs = value,
                other => {
                    return Err(CatalogError::InvalidArgument(format!(
                        "unknown user property '{other}' (expected password, superuser, catalogs)"
                    )))
                }
            }
        }

        let meta = self.state()?.dynamic_catalog_list.clone();
        let exists = meta
            .get_app_user(username)
            .await
            .map_err(|e| CatalogError::External(format!("Failed to look up user: {e}")))?
            .is_some();
        if exists {
            return Err(CatalogError::AlreadyExists("user", username.to_string()));
        }

        // Store the mysql_native_password digest, never the plaintext.
        let digest = password.as_deref().map(password_digest).unwrap_or_default();
        meta.upsert_app_user(username, &digest, superuser, &catalogs)
            .await
            .map_err(|e| CatalogError::External(format!("Failed to create user: {e}")))?;
        Ok(())
    }

    /// Backs `DROP USER [IF EXISTS] name`.
    pub async fn drop_user(&self, username: &str, if_exists: bool) -> CatalogResult<()> {
        self.require_admin()?;
        let meta = self.state()?.dynamic_catalog_list.clone();
        let deleted = meta
            .delete_app_user(username)
            .await
            .map_err(|e| CatalogError::External(format!("Failed to drop user: {e}")))?;
        if !deleted && !if_exists {
            return Err(CatalogError::NotFound("user", username.to_string()));
        }
        Ok(())
    }

    /// Backs `SHOW USERS [LIKE 'pattern']`. Password digests are not exposed.
    pub async fn list_users(&self, pattern: Option<&str>) -> CatalogResult<Vec<UserDisplay>> {
        self.require_admin()?;
        let meta = self.state()?.dynamic_catalog_list.clone();
        let rows = meta
            .list_app_users()
            .await
            .map_err(|e| CatalogError::External(format!("Failed to list users: {e}")))?;
        Ok(rows
            .into_iter()
            .filter(|(name, _, _)| match_pattern(name, pattern))
            .map(|(username, superuser, allowed_catalogs)| UserDisplay {
                username,
                superuser,
                allowed_catalogs: if superuser {
                    "*".to_string()
                } else {
                    allowed_catalogs
                },
            })
            .collect())
    }

    /// Backs `GRANT CATALOG cat TO user`: adds the catalog to the user's
    /// allow-list. A no-op for superusers / `*` users (already unrestricted).
    pub async fn grant_catalog(&self, catalog: &str, username: &str) -> CatalogResult<()> {
        self.require_admin()?;
        let meta = self.state()?.dynamic_catalog_list.clone();
        let user = meta
            .get_app_user(username)
            .await
            .map_err(|e| CatalogError::External(format!("Failed to look up user: {e}")))?
            .ok_or_else(|| CatalogError::NotFound("user", username.to_string()))?;

        match user.allowed_catalogs {
            // Superuser or explicit `*`: already has access to everything.
            None => Ok(()),
            Some(mut set) => {
                set.insert(catalog.to_string());
                let mut list: Vec<String> = set.into_iter().collect();
                list.sort_unstable();
                meta.set_app_user_catalogs(username, &list.join(","))
                    .await
                    .map_err(|e| CatalogError::External(format!("Failed to grant: {e}")))?;
                Ok(())
            }
        }
    }

    /// Backs `REVOKE CATALOG cat FROM user`: removes the catalog from the
    /// user's allow-list (idempotent).
    pub async fn revoke_catalog(&self, catalog: &str, username: &str) -> CatalogResult<()> {
        self.require_admin()?;
        let meta = self.state()?.dynamic_catalog_list.clone();
        let user = meta
            .get_app_user(username)
            .await
            .map_err(|e| CatalogError::External(format!("Failed to look up user: {e}")))?
            .ok_or_else(|| CatalogError::NotFound("user", username.to_string()))?;

        match user.allowed_catalogs {
            None => Err(CatalogError::NotSupported(format!(
                "user '{username}' has unrestricted access (superuser or '*'); \
                 set an explicit catalog list instead of revoking"
            ))),
            Some(mut set) => {
                set.remove(catalog);
                let mut list: Vec<String> = set.into_iter().collect();
                list.sort_unstable();
                meta.set_app_user_catalogs(username, &list.join(","))
                    .await
                    .map_err(|e| CatalogError::External(format!("Failed to revoke: {e}")))?;
                Ok(())
            }
        }
    }
}
