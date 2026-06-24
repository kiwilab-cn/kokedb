//! Identifies a remote source database from its DSN scheme, so the live-read,
//! schema-inference, and sync paths can dispatch to the right connector.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceKind {
    Postgres,
    Mysql,
}

/// The source kind for a DSN. Defaults to Postgres for `postgresql://` (and
/// anything unrecognized, preserving prior behavior).
pub fn source_kind(dsn: &str) -> SourceKind {
    let lower = dsn.trim_start().to_ascii_lowercase();
    if lower.starts_with("mysql://") {
        SourceKind::Mysql
    } else {
        SourceKind::Postgres
    }
}

pub fn is_mysql(dsn: &str) -> bool {
    matches!(source_kind(dsn), SourceKind::Mysql)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_source_kind_from_scheme() {
        assert_eq!(source_kind("mysql://u:p@h:3306/db"), SourceKind::Mysql);
        assert_eq!(
            source_kind("postgresql://u:p@h:5432/db"),
            SourceKind::Postgres
        );
        // Unknown / default -> Postgres (unchanged behavior).
        assert_eq!(source_kind("postgres://u@h/db"), SourceKind::Postgres);
        assert!(is_mysql("MySQL://H/db"));
    }
}
