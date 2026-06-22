//! Credential redaction for safe logging of connection strings.

/// Masks the password in a DSN/URL so it can be logged safely, e.g.
/// `postgresql://user:secret@host:5432/db` -> `postgresql://user:***@host:5432/db`.
/// Leaves a DSN without credentials unchanged.
pub fn redact_dsn(dsn: &str) -> String {
    // Find the authority section between "://" and the first '@'.
    let Some(scheme_end) = dsn.find("://") else {
        return dsn.to_string();
    };
    let auth_start = scheme_end + 3;
    let rest = &dsn[auth_start..];
    let Some(at) = rest.find('@') else {
        return dsn.to_string();
    };
    let creds = &rest[..at];
    // Only a password (after the first ':') is sensitive; keep the username.
    match creds.find(':') {
        Some(colon) => {
            let user = &creds[..colon];
            format!("{}{}:***@{}", &dsn[..auth_start], user, &rest[at + 1..])
        }
        None => dsn.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn masks_password() {
        assert_eq!(
            redact_dsn("postgresql://user:secret@host:5432/db"),
            "postgresql://user:***@host:5432/db"
        );
    }

    #[test]
    fn keeps_dsn_without_password() {
        assert_eq!(redact_dsn("postgresql://host:5432/db"), "postgresql://host:5432/db");
        assert_eq!(redact_dsn("postgresql://user@host/db"), "postgresql://user@host/db");
    }

    #[test]
    fn non_url_unchanged() {
        assert_eq!(redact_dsn("not a url"), "not a url");
    }

    #[test]
    fn percent_encoded_password() {
        assert_eq!(
            redact_dsn("postgresql://u:p%40ss@h/db"),
            "postgresql://u:***@h/db"
        );
    }
}
