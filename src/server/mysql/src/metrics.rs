//! Minimal Prometheus metrics for the MySQL server.
//!
//! Counters/gauges are plain atomics in a process-global [`METRICS`]; a tiny
//! HTTP/1.1 responder serves the Prometheus text exposition format at any path
//! (conventionally `/metrics`). No HTTP framework dependency is pulled in.

use std::fmt::Write as _;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

use log::{error, info};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

pub static METRICS: Metrics = Metrics::new();

pub struct Metrics {
    /// Total queries received (text + prepared execute).
    pub queries_total: AtomicU64,
    /// Queries served from the result cache.
    pub cache_hits_total: AtomicU64,
    /// Queries that missed the result cache and were executed.
    pub cache_misses_total: AtomicU64,
    /// Queries that failed.
    pub query_errors_total: AtomicU64,
    /// Currently open client connections.
    pub active_connections: AtomicI64,
}

impl Metrics {
    const fn new() -> Self {
        Self {
            queries_total: AtomicU64::new(0),
            cache_hits_total: AtomicU64::new(0),
            cache_misses_total: AtomicU64::new(0),
            query_errors_total: AtomicU64::new(0),
            active_connections: AtomicI64::new(0),
        }
    }

    /// Renders the Prometheus text exposition format (v0.0.4).
    pub fn render(&self) -> String {
        let mut out = String::with_capacity(512);
        let counter = |out: &mut String, name: &str, help: &str, value: u64| {
            let _ = writeln!(out, "# HELP {name} {help}");
            let _ = writeln!(out, "# TYPE {name} counter");
            let _ = writeln!(out, "{name} {value}");
        };

        counter(
            &mut out,
            "kokedb_queries_total",
            "Total queries received.",
            self.queries_total.load(Ordering::Relaxed),
        );
        counter(
            &mut out,
            "kokedb_cache_hits_total",
            "Queries served from the result cache.",
            self.cache_hits_total.load(Ordering::Relaxed),
        );
        counter(
            &mut out,
            "kokedb_cache_misses_total",
            "Queries that missed the result cache.",
            self.cache_misses_total.load(Ordering::Relaxed),
        );
        counter(
            &mut out,
            "kokedb_query_errors_total",
            "Queries that returned an error.",
            self.query_errors_total.load(Ordering::Relaxed),
        );

        let _ = writeln!(
            out,
            "# HELP kokedb_active_connections Currently open client connections."
        );
        let _ = writeln!(out, "# TYPE kokedb_active_connections gauge");
        let _ = writeln!(
            out,
            "kokedb_active_connections {}",
            self.active_connections.load(Ordering::Relaxed).max(0)
        );

        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn render_emits_prometheus_format() {
        let m = Metrics::new();
        m.queries_total.store(7, Ordering::Relaxed);
        m.cache_hits_total.store(3, Ordering::Relaxed);
        m.active_connections.store(2, Ordering::Relaxed);
        let out = m.render();
        assert!(out.contains("# TYPE kokedb_queries_total counter"));
        assert!(out.contains("kokedb_queries_total 7"));
        assert!(out.contains("kokedb_cache_hits_total 3"));
        assert!(out.contains("# TYPE kokedb_active_connections gauge"));
        assert!(out.contains("kokedb_active_connections 2"));
    }
}

pub fn inc_queries() {
    METRICS.queries_total.fetch_add(1, Ordering::Relaxed);
}
pub fn inc_cache_hit() {
    METRICS.cache_hits_total.fetch_add(1, Ordering::Relaxed);
}
pub fn inc_cache_miss() {
    METRICS.cache_misses_total.fetch_add(1, Ordering::Relaxed);
}
pub fn inc_query_error() {
    METRICS.query_errors_total.fetch_add(1, Ordering::Relaxed);
}

/// Serves the metrics endpoint until the process exits. A bind failure is
/// logged and the function returns (the SQL server keeps running).
pub async fn serve_metrics(addr: String) {
    let listener = match TcpListener::bind(&addr).await {
        Ok(listener) => listener,
        Err(e) => {
            error!("Metrics endpoint bind to {addr} failed: {e}");
            return;
        }
    };
    info!("Metrics endpoint listening on http://{addr}/metrics");

    loop {
        let Ok((mut stream, _)) = listener.accept().await else {
            continue;
        };
        tokio::spawn(async move {
            // Drain the request line/headers; the response is the same for any
            // path, so we do not parse it.
            let mut buf = [0u8; 1024];
            let _ = stream.read(&mut buf).await;

            let body = METRICS.render();
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            let _ = stream.write_all(response.as_bytes()).await;
        });
    }
}
