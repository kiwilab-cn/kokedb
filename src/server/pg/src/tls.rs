//! Opt-in TLS for the PostgreSQL wire server.
//!
//! Set `KOKEDB_PG_TLS_CERT` and `KOKEDB_PG_TLS_KEY` to PEM file paths (a full
//! certificate chain and a PKCS#8 or PKCS#1 private key) to enable TLS; leave
//! both unset for plaintext. pgwire handles the `SSLRequest` negotiation: with
//! an acceptor configured, clients that request SSL get an encrypted session
//! (libpq `sslmode=require` works), others stay plaintext.

use std::fs::File;
use std::io::{BufReader, Error as IoError, ErrorKind};
use std::sync::Arc;

use rustls_pemfile::{certs, private_key};
use rustls_pki_types::CertificateDer;
use tokio_rustls::rustls::ServerConfig;
use tokio_rustls::TlsAcceptor;

/// Builds a [`TlsAcceptor`] from the environment. `Ok(None)` when TLS is not
/// configured; an error when it is configured but the cert/key are unusable
/// (misconfiguration must fail startup, not silently downgrade to plaintext).
pub fn acceptor_from_env() -> std::io::Result<Option<TlsAcceptor>> {
    let (cert_path, key_path) = match (
        std::env::var("KOKEDB_PG_TLS_CERT"),
        std::env::var("KOKEDB_PG_TLS_KEY"),
    ) {
        (Ok(c), Ok(k)) if !c.is_empty() && !k.is_empty() => (c, k),
        (Err(_) | Ok(_), Err(_) | Ok(_)) => return Ok(None),
    };
    build_acceptor(&cert_path, &key_path).map(Some)
}

fn build_acceptor(cert_path: &str, key_path: &str) -> std::io::Result<TlsAcceptor> {
    let cert_chain = certs(&mut BufReader::new(File::open(cert_path).map_err(|e| {
        IoError::new(e.kind(), format!("KOKEDB_PG_TLS_CERT '{cert_path}': {e}"))
    })?))
    .collect::<Result<Vec<CertificateDer>, IoError>>()?;
    if cert_chain.is_empty() {
        return Err(IoError::new(
            ErrorKind::InvalidData,
            format!("KOKEDB_PG_TLS_CERT '{cert_path}' contains no certificates"),
        ));
    }

    let key = private_key(&mut BufReader::new(File::open(key_path).map_err(|e| {
        IoError::new(e.kind(), format!("KOKEDB_PG_TLS_KEY '{key_path}': {e}"))
    })?))?
    .ok_or_else(|| {
        IoError::new(
            ErrorKind::InvalidData,
            format!("KOKEDB_PG_TLS_KEY '{key_path}' contains no private key"),
        )
    })?;

    let mut config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(cert_chain, key)
        .map_err(|e| IoError::new(ErrorKind::InvalidInput, format!("TLS config: {e}")))?;
    // Advertised for direct-TLS clients (PostgreSQL 17+ ALPN).
    config.alpn_protocols = vec![b"postgresql".to_vec()];

    Ok(TlsAcceptor::from(Arc::new(config)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unset_env_disables_tls() {
        std::env::remove_var("KOKEDB_PG_TLS_CERT");
        std::env::remove_var("KOKEDB_PG_TLS_KEY");
        assert!(acceptor_from_env().expect("no TLS -> Ok(None)").is_none());
    }

    #[test]
    fn missing_files_error_instead_of_downgrading() {
        assert!(build_acceptor("/nonexistent/server.crt", "/nonexistent/server.key").is_err());
    }
}
