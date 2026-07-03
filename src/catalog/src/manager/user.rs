//! User management commands (`CREATE USER` / `DROP USER` / `SHOW USERS` /
//! `GRANT CATALOG` / `REVOKE CATALOG`), backed by `system.app_user`.
//!
//! Authorization model: only *unrestricted* sessions — superusers, or any
//! session while authentication is disabled — may manage users. Sessions
//! scoped to a catalog allow-list are denied. Changes take effect live: the
//! wire front-ends consult `system.app_user` on every authentication attempt.

use kokedb_common::auth::password_digest;

use crate::display::{AuditLogDisplay, ColumnPolicyDisplay, RowPolicyDisplay, UserDisplay};
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
                "catalogs" => {
                    // Comma-separated grant scopes; each is validated the same
                    // way as GRANT (catalog[.schema[.table]] or `*`).
                    for entry in value.split(',').map(str::trim).filter(|s| !s.is_empty()) {
                        if entry == "*" {
                            continue;
                        }
                        let parts: Vec<String> =
                            entry.split('.').map(|s| s.to_string()).collect();
                        Self::scope_string(&parts)?;
                    }
                    catalogs = value;
                }
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

    /// Backs `DROP USER [IF EXISTS] name`. Also removes the user's row
    /// policies so a re-created user of the same name starts clean.
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
        if deleted {
            meta.delete_row_policies_for_user(username)
                .await
                .map_err(|e| {
                    CatalogError::External(format!("Failed to drop user's row policies: {e}"))
                })?;
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

    /// Validates a grant scope reference and renders it as the stored dotted
    /// form: `catalog`, `catalog.schema`, or `catalog.schema.table`. Scopes
    /// are stored fully qualified — they are global user attributes, never
    /// relative to the granting session's defaults.
    fn scope_string(scope: &[String]) -> CatalogResult<String> {
        if scope.is_empty() || scope.len() > 3 {
            return Err(CatalogError::InvalidArgument(format!(
                "a grant scope has 1-3 parts (catalog[.schema[.table]]), got {}",
                scope.len()
            )));
        }
        if scope.iter().any(|part| part.is_empty() || part.contains(['.', ','])) {
            return Err(CatalogError::InvalidArgument(
                "grant scope parts must be non-empty and free of '.'/','".to_string(),
            ));
        }
        Ok(scope.join("."))
    }

    /// Backs `GRANT CATALOG c | DATABASE c.s | TABLE c.s.t TO user`: adds the
    /// scope to the user's allow-list. A no-op for superusers / `*` users
    /// (already unrestricted).
    pub async fn grant_scope(&self, scope: &[String], username: &str) -> CatalogResult<()> {
        self.require_admin()?;
        let scope = Self::scope_string(scope)?;
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
                set.insert(scope);
                let mut list: Vec<String> = set.into_iter().collect();
                list.sort_unstable();
                meta.set_app_user_catalogs(username, &list.join(","))
                    .await
                    .map_err(|e| CatalogError::External(format!("Failed to grant: {e}")))?;
                Ok(())
            }
        }
    }

    /// Backs `CREATE ROW POLICY ON c.s.t FOR user USING 'predicate'`. The
    /// table must be fully qualified; the predicate was validated by the
    /// analyzer. Creating a policy for an existing `(user, table)` replaces it.
    pub async fn create_row_policy(
        &self,
        table: &[String],
        username: &str,
        filter: &str,
    ) -> CatalogResult<()> {
        self.require_admin()?;
        let [catalog, schema, table_name] = table else {
            return Err(CatalogError::InvalidArgument(
                "row policies require a fully qualified catalog.schema.table".to_string(),
            ));
        };
        let meta = self.state()?.dynamic_catalog_list.clone();
        // Policies are per-user; the user must exist.
        meta.get_app_user(username)
            .await
            .map_err(|e| CatalogError::External(format!("Failed to look up user: {e}")))?
            .ok_or_else(|| CatalogError::NotFound("user", username.to_string()))?;
        meta.upsert_row_policy(username, catalog, schema, table_name, filter)
            .await
            .map_err(|e| CatalogError::External(format!("Failed to create row policy: {e}")))?;
        Ok(())
    }

    /// Backs `DROP ROW POLICY ON c.s.t FOR user`.
    pub async fn drop_row_policy(&self, table: &[String], username: &str) -> CatalogResult<()> {
        self.require_admin()?;
        let [catalog, schema, table_name] = table else {
            return Err(CatalogError::InvalidArgument(
                "row policies require a fully qualified catalog.schema.table".to_string(),
            ));
        };
        let meta = self.state()?.dynamic_catalog_list.clone();
        let deleted = meta
            .delete_row_policy(username, catalog, schema, table_name)
            .await
            .map_err(|e| CatalogError::External(format!("Failed to drop row policy: {e}")))?;
        if !deleted {
            return Err(CatalogError::NotFound(
                "row policy",
                format!("{catalog}.{schema}.{table_name} for {username}"),
            ));
        }
        Ok(())
    }

    /// Backs `SHOW ROW POLICIES`.
    pub async fn list_row_policies(&self) -> CatalogResult<Vec<RowPolicyDisplay>> {
        self.require_admin()?;
        let meta = self.state()?.dynamic_catalog_list.clone();
        let rows = meta
            .list_all_row_policies()
            .await
            .map_err(|e| CatalogError::External(format!("Failed to list row policies: {e}")))?;
        Ok(rows
            .into_iter()
            .map(|(username, catalog, schema, table, filter)| RowPolicyDisplay {
                username,
                table: format!("{catalog}.{schema}.{table}"),
                filter,
            })
            .collect())
    }

    /// Backs `CREATE COLUMN POLICY ON c.s.t FOR user USING 'a,b'`. The listed
    /// columns read as NULL for that user. Creating a policy for an existing
    /// `(user, table)` replaces it.
    pub async fn create_column_policy(
        &self,
        table: &[String],
        username: &str,
        columns: &str,
    ) -> CatalogResult<()> {
        self.require_admin()?;
        let [catalog, schema, table_name] = table else {
            return Err(CatalogError::InvalidArgument(
                "column policies require a fully qualified catalog.schema.table".to_string(),
            ));
        };
        let meta = self.state()?.dynamic_catalog_list.clone();
        meta.get_app_user(username)
            .await
            .map_err(|e| CatalogError::External(format!("Failed to look up user: {e}")))?
            .ok_or_else(|| CatalogError::NotFound("user", username.to_string()))?;
        meta.upsert_column_policy(username, catalog, schema, table_name, columns)
            .await
            .map_err(|e| {
                CatalogError::External(format!("Failed to create column policy: {e}"))
            })?;
        Ok(())
    }

    /// Backs `DROP COLUMN POLICY ON c.s.t FOR user`.
    pub async fn drop_column_policy(&self, table: &[String], username: &str) -> CatalogResult<()> {
        self.require_admin()?;
        let [catalog, schema, table_name] = table else {
            return Err(CatalogError::InvalidArgument(
                "column policies require a fully qualified catalog.schema.table".to_string(),
            ));
        };
        let meta = self.state()?.dynamic_catalog_list.clone();
        let deleted = meta
            .delete_column_policy(username, catalog, schema, table_name)
            .await
            .map_err(|e| CatalogError::External(format!("Failed to drop column policy: {e}")))?;
        if !deleted {
            return Err(CatalogError::NotFound(
                "column policy",
                format!("{catalog}.{schema}.{table_name} for {username}"),
            ));
        }
        Ok(())
    }

    /// Backs `SHOW AUDIT LOG`: the 200 most recent audit entries.
    pub async fn list_audit_log(&self) -> CatalogResult<Vec<AuditLogDisplay>> {
        self.require_admin()?;
        let meta = self.state()?.dynamic_catalog_list.clone();
        let rows = meta
            .list_audit_log(200)
            .await
            .map_err(|e| CatalogError::External(format!("Failed to list audit log: {e}")))?;
        Ok(rows
            .into_iter()
            .map(
                |(
                    event_time,
                    username,
                    client_addr,
                    protocol,
                    event_type,
                    statement,
                    success,
                    error,
                    duration_ms,
                )| AuditLogDisplay {
                    event_time: event_time.to_rfc3339(),
                    username,
                    client_addr,
                    protocol,
                    event_type,
                    statement,
                    success,
                    error,
                    duration_ms,
                },
            )
            .collect())
    }

    /// Backs `SHOW COLUMN POLICIES`.
    pub async fn list_column_policies(&self) -> CatalogResult<Vec<ColumnPolicyDisplay>> {
        self.require_admin()?;
        let meta = self.state()?.dynamic_catalog_list.clone();
        let rows = meta
            .list_all_column_policies()
            .await
            .map_err(|e| CatalogError::External(format!("Failed to list column policies: {e}")))?;
        Ok(rows
            .into_iter()
            .map(
                |(username, catalog, schema, table, masked_columns)| ColumnPolicyDisplay {
                    username,
                    table: format!("{catalog}.{schema}.{table}"),
                    masked_columns,
                },
            )
            .collect())
    }

    /// Backs `REVOKE CATALOG c | DATABASE c.s | TABLE c.s.t FROM user`:
    /// removes the *exact* scope from the user's allow-list (idempotent).
    /// Revoking `c.s.t` does not carve a hole out of a broader `c` grant —
    /// scopes are additive; replace the broad grant with narrower ones instead.
    pub async fn revoke_scope(&self, scope: &[String], username: &str) -> CatalogResult<()> {
        self.require_admin()?;
        let scope = Self::scope_string(scope)?;
        let meta = self.state()?.dynamic_catalog_list.clone();
        let user = meta
            .get_app_user(username)
            .await
            .map_err(|e| CatalogError::External(format!("Failed to look up user: {e}")))?
            .ok_or_else(|| CatalogError::NotFound("user", username.to_string()))?;

        match user.allowed_catalogs {
            None => Err(CatalogError::NotSupported(format!(
                "user '{username}' has unrestricted access (superuser or '*'); \
                 set an explicit scope list instead of revoking"
            ))),
            Some(mut set) => {
                set.remove(&scope);
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
