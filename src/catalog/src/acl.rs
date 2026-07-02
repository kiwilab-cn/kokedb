//! Session-scoped authorization: a parsed set of grant scopes.
//!
//! A scope string is a dotted path with 1–3 parts:
//!
//! * `catalog`               — the whole catalog
//! * `catalog.schema`        — one database
//! * `catalog.schema.table`  — one table
//!
//! Grants are hierarchical downward (a catalog grant covers all of its
//! databases and tables) and confer *visibility* upward (a table grant makes
//! its database and catalog resolvable/listable, but only that table
//! readable). Malformed scopes (empty parts, >3 segments) are ignored rather
//! than granting anything.

use std::collections::{HashMap, HashSet};

use kokedb_common::spec;

/// A row-level security policy attached to the session: reads of `table` are
/// AND-ed with `filter`. `filter: None` means the stored policy failed to
/// parse — the table is then denied entirely (fail-closed), never served
/// unfiltered.
#[derive(Debug, Clone)]
pub struct RowPolicy {
    pub catalog: String,
    pub schema: String,
    pub table: String,
    pub filter: Option<spec::Expr>,
}

/// Parsed grant scopes for one session. Construct via [`CatalogAcl::from_scopes`].
#[derive(Debug, Default)]
pub struct CatalogAcl {
    /// Full-catalog grants.
    catalogs: HashSet<String>,
    /// Full-database grants: `(catalog, schema)`.
    databases: HashSet<(String, String)>,
    /// Single-table grants: `(catalog, schema, table)`.
    tables: HashSet<(String, String, String)>,
    /// Catalogs visible for resolution/listing (mentioned by ANY grant).
    visible_catalogs: HashSet<String>,
    /// Databases visible for resolution/listing (full grants + parents of
    /// table grants).
    visible_databases: HashSet<(String, String)>,
    /// Row-level security filters, keyed by `(catalog, schema, table)`.
    policies: HashMap<(String, String, String), Option<spec::Expr>>,
}

impl CatalogAcl {
    pub fn from_scopes<I, S>(scopes: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut acl = Self::default();
        for scope in scopes {
            let parts: Vec<&str> = scope.as_ref().split('.').map(str::trim).collect();
            match parts.as_slice() {
                [c] if !c.is_empty() => {
                    acl.catalogs.insert(c.to_string());
                    acl.visible_catalogs.insert(c.to_string());
                }
                [c, s] if !c.is_empty() && !s.is_empty() => {
                    acl.databases.insert((c.to_string(), s.to_string()));
                    acl.visible_catalogs.insert(c.to_string());
                    acl.visible_databases.insert((c.to_string(), s.to_string()));
                }
                [c, s, t] if !c.is_empty() && !s.is_empty() && !t.is_empty() => {
                    acl.tables
                        .insert((c.to_string(), s.to_string(), t.to_string()));
                    acl.visible_catalogs.insert(c.to_string());
                    acl.visible_databases.insert((c.to_string(), s.to_string()));
                }
                // Malformed scopes never grant access.
                _ => {}
            }
        }
        acl
    }

    /// Whether the catalog may be resolved/listed (any grant beneath it).
    pub fn allows_catalog(&self, catalog: &str) -> bool {
        self.visible_catalogs.contains(catalog)
    }

    /// Whether the database may be resolved/listed: covered by a catalog
    /// grant, granted itself, or the parent of a table grant.
    pub fn allows_database(&self, catalog: &str, schema: &str) -> bool {
        self.catalogs.contains(catalog)
            || self
                .visible_databases
                .contains(&(catalog.to_string(), schema.to_string()))
    }

    /// Whether the table may be read: covered by a catalog grant, a database
    /// grant, or granted directly.
    pub fn allows_table(&self, catalog: &str, schema: &str, table: &str) -> bool {
        self.catalogs.contains(catalog)
            || self
                .databases
                .contains(&(catalog.to_string(), schema.to_string()))
            || self.tables.contains(&(
                catalog.to_string(),
                schema.to_string(),
                table.to_string(),
            ))
    }

    /// Attaches the session's row-level security policies.
    pub fn with_policies(mut self, policies: Vec<RowPolicy>) -> Self {
        for p in policies {
            self.policies
                .insert((p.catalog, p.schema, p.table), p.filter);
        }
        self
    }

    /// The row filter for a table, if any. `Some(None)` marks a policy whose
    /// stored predicate failed to parse — the caller must deny the read
    /// (fail-closed), never serve the table unfiltered.
    pub fn row_policy(&self, catalog: &str, schema: &str, table: &str) -> Option<&Option<spec::Expr>> {
        self.policies
            .get(&(catalog.to_string(), schema.to_string(), table.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalog_grant_covers_everything_below() {
        let acl = CatalogAcl::from_scopes(["shop"]);
        assert!(acl.allows_catalog("shop"));
        assert!(acl.allows_database("shop", "public"));
        assert!(acl.allows_table("shop", "public", "orders"));
        assert!(!acl.allows_catalog("other"));
        assert!(!acl.allows_table("other", "public", "orders"));
    }

    #[test]
    fn database_grant_scopes_to_one_schema() {
        let acl = CatalogAcl::from_scopes(["shop.public"]);
        assert!(acl.allows_catalog("shop"), "db grant makes catalog visible");
        assert!(acl.allows_database("shop", "public"));
        assert!(acl.allows_table("shop", "public", "orders"));
        assert!(!acl.allows_database("shop", "internal"));
        assert!(!acl.allows_table("shop", "internal", "orders"));
    }

    #[test]
    fn table_grant_scopes_to_one_table_with_upward_visibility() {
        let acl = CatalogAcl::from_scopes(["shop.public.orders"]);
        assert!(acl.allows_catalog("shop"));
        assert!(acl.allows_database("shop", "public"), "parent db is visible");
        assert!(acl.allows_table("shop", "public", "orders"));
        assert!(!acl.allows_table("shop", "public", "users"), "sibling denied");
        assert!(!acl.allows_database("shop", "internal"));
    }

    #[test]
    fn scopes_combine() {
        let acl = CatalogAcl::from_scopes(["warehouse", "shop.public.orders"]);
        assert!(acl.allows_table("warehouse", "any", "thing"));
        assert!(acl.allows_table("shop", "public", "orders"));
        assert!(!acl.allows_table("shop", "public", "users"));
    }

    #[test]
    fn malformed_scopes_grant_nothing() {
        let acl = CatalogAcl::from_scopes(["", "a..b", "a.b.c.d", "."]);
        assert!(!acl.allows_catalog("a"));
        assert!(!acl.allows_table("a", "b", "c"));
    }
}
