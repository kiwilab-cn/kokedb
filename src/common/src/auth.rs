//! Password verification shared by the MySQL and PostgreSQL front-ends.
//!
//! Credentials are stored once as the MySQL `mysql_native_password` digest —
//! the uppercase hex of `SHA1(SHA1(password))` — which lets a single stored
//! value authenticate both protocols:
//!
//! * MySQL `mysql_native_password` verifies the challenge/response without ever
//!   needing the plaintext (it recovers `SHA1(password)` from the token and
//!   re-hashes it).
//! * PostgreSQL cleartext auth receives the plaintext and re-derives the same
//!   digest for a constant comparison.
//!
//! An empty stored digest marks a passwordless account (any password accepted),
//! mirroring the previous "no password configured" behaviour.

use sha1::{Digest, Sha1};

/// Computes the `mysql_native_password` digest of `password`: uppercase hex of
/// `SHA1(SHA1(password))`. This is the value stored in `system.app_user`.
pub fn password_digest(password: &str) -> String {
    let stage1 = Sha1::digest(password.as_bytes());
    let stage2 = Sha1::digest(stage1);
    hex::encode_upper(stage2)
}

/// Verifies a MySQL `mysql_native_password` response against a stored digest:
///   `token == SHA1(password) XOR SHA1(salt + SHA1(SHA1(password)))`
/// recovered as `SHA1(token XOR SHA1(salt + stored)) == stored`.
pub fn verify_mysql_native(stored_digest_hex: &str, salt: &[u8], token: &[u8]) -> bool {
    if stored_digest_hex.is_empty() {
        return true; // passwordless account
    }
    if token.len() != 20 {
        return false;
    }
    let Ok(stored) = hex::decode(stored_digest_hex) else {
        return false;
    };
    if stored.len() != 20 {
        return false;
    }
    let mut hasher = Sha1::new();
    hasher.update(salt);
    hasher.update(&stored);
    let scramble = hasher.finalize();
    let recovered: Vec<u8> = scramble
        .iter()
        .zip(token.iter())
        .map(|(s, t)| s ^ t)
        .collect();
    Sha1::digest(&recovered).as_slice() == stored.as_slice()
}

/// Verifies a cleartext password (PostgreSQL) against a stored digest by
/// re-deriving the digest and comparing case-insensitively.
pub fn verify_cleartext(stored_digest_hex: &str, password: &str) -> bool {
    if stored_digest_hex.is_empty() {
        return true; // passwordless account
    }
    password_digest(password).eq_ignore_ascii_case(stored_digest_hex)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Builds the client token a MySQL client would send for `mysql_native_password`.
    fn client_token(password: &str, salt: &[u8]) -> Vec<u8> {
        let hash1 = Sha1::digest(password.as_bytes());
        let hash2 = Sha1::digest(hash1);
        let mut h = Sha1::new();
        h.update(salt);
        h.update(hash2);
        let scramble = h.finalize();
        scramble
            .iter()
            .zip(hash1.iter())
            .map(|(s, t)| s ^ t)
            .collect()
    }

    #[test]
    fn mysql_native_roundtrip() {
        let digest = password_digest("s3cret");
        let salt = b"01234567890123456789";
        assert!(verify_mysql_native(&digest, salt, &client_token("s3cret", salt)));
        assert!(!verify_mysql_native(&digest, salt, &client_token("wrong", salt)));
        assert!(!verify_mysql_native(&digest, salt, b"short"));
    }

    #[test]
    fn cleartext_roundtrip() {
        let digest = password_digest("s3cret");
        assert!(verify_cleartext(&digest, "s3cret"));
        assert!(!verify_cleartext(&digest, "nope"));
        // Stored digest is case-insensitive hex.
        assert!(verify_cleartext(&digest.to_lowercase(), "s3cret"));
    }

    #[test]
    fn empty_digest_is_passwordless() {
        assert!(verify_cleartext("", "anything"));
        assert!(verify_mysql_native("", b"saltsaltsaltsaltsalt", b"anytokenanytokenany!"));
    }
}
