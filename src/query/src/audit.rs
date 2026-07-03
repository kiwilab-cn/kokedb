//! Audit trail for authentication attempts and statement executions.
//!
//! Events are pushed onto a bounded channel and written to
//! `system.audit_log` in batches by a background flusher, so the query hot
//! path never waits on the meta store. Each event is also emitted to the
//! normal log stream under the `audit` target, so ops can ship it through
//! the existing log pipeline.
//!
//! Configuration:
//! * `KOKEDB_AUDIT_LOG` — `true` (default) / `false`.
//! * `KOKEDB_AUDIT_RETENTION_DAYS` — prune entries older than this
//!   (default 30; 0 keeps everything).
//!
//! Delivery is best-effort by design: if the channel is full (meta store
//! stalled), events are dropped and counted rather than blocking or failing
//! queries — an audit outage must not take the data path down with it.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use kokedb_common::env::get_env_as;
use kokedb_meta::catalog_list::{AuditEvent, PostgreSQLMetaCatalogProviderList};

/// Statement text cap per event (keeps giant IN-lists from bloating the log).
const MAX_STATEMENT_LEN: usize = 4096;
/// Error text cap per event.
const MAX_ERROR_LEN: usize = 1024;
/// Flush when this many events are buffered, or on the periodic tick.
const FLUSH_BATCH: usize = 200;
/// Channel capacity before events are dropped (best-effort delivery).
const CHANNEL_CAPACITY: usize = 8192;

/// Handle for recording audit events. Cheap to clone; `log()` never blocks.
#[derive(Clone)]
pub struct AuditLogger {
    tx: Option<tokio::sync::mpsc::Sender<AuditEvent>>,
    dropped: Arc<AtomicU64>,
}

impl AuditLogger {
    /// Disabled logger (drops everything); used when `KOKEDB_AUDIT_LOG=false`
    /// and in tests.
    pub fn disabled() -> Self {
        Self {
            tx: None,
            dropped: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Builds the logger and spawns the background flusher.
    pub fn new(meta: Arc<PostgreSQLMetaCatalogProviderList>) -> Self {
        if !get_env_as("KOKEDB_AUDIT_LOG", true) {
            log::info!("Audit log disabled (KOKEDB_AUDIT_LOG=false)");
            return Self::disabled();
        }
        let (tx, mut rx) = tokio::sync::mpsc::channel::<AuditEvent>(CHANNEL_CAPACITY);
        tokio::spawn(async move {
            let retention_days = get_env_as("KOKEDB_AUDIT_RETENTION_DAYS", 30u64);
            let mut buffer: Vec<AuditEvent> = Vec::with_capacity(FLUSH_BATCH);
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(1));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            let mut last_purge = std::time::Instant::now();
            loop {
                tokio::select! {
                    received = rx.recv() => match received {
                        Some(event) => {
                            buffer.push(event);
                            if buffer.len() >= FLUSH_BATCH {
                                flush(&meta, &mut buffer).await;
                            }
                        }
                        // All senders gone: final flush, then stop.
                        None => {
                            flush(&meta, &mut buffer).await;
                            break;
                        }
                    },
                    _ = tick.tick() => {
                        flush(&meta, &mut buffer).await;
                        // Retention pruning piggybacks on the tick, hourly.
                        if retention_days > 0
                            && last_purge.elapsed() > std::time::Duration::from_secs(3600)
                        {
                            last_purge = std::time::Instant::now();
                            match meta.purge_audit_log(retention_days).await {
                                Ok(0) => {}
                                Ok(n) => log::info!("Audit log retention: pruned {n} entries"),
                                Err(e) => log::warn!("Audit log retention purge failed: {e}"),
                            }
                        }
                    }
                }
            }
        });
        Self {
            tx: Some(tx),
            dropped: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Records an event. Non-blocking: on a full channel the event is dropped
    /// and counted (audit must never stall the data path).
    pub fn log(&self, mut event: AuditEvent) {
        truncate_in_place(&mut event.statement, MAX_STATEMENT_LEN);
        if let Some(error) = event.error.as_mut() {
            truncate_in_place(error, MAX_ERROR_LEN);
        }
        // Mirror to the ordinary log stream for pipeline shipping.
        log::info!(
            target: "audit",
            "user={} addr={} proto={} type={} ok={} duration_ms={} stmt={}",
            event.username,
            event.client_addr,
            event.protocol,
            event.event_type,
            event.success,
            event.duration_ms.unwrap_or(-1),
            event.statement,
        );
        let Some(tx) = &self.tx else { return };
        if tx.try_send(event).is_err() {
            let dropped = self.dropped.fetch_add(1, Ordering::Relaxed) + 1;
            // Log the first drop and every 1000th after, not every one.
            if dropped == 1 || dropped % 1000 == 0 {
                log::warn!("Audit channel full: {dropped} events dropped so far");
            }
        }
    }

    /// The audit-safe statement text: queries keep their SQL; commands are
    /// reduced to a keyword prefix because command text can carry secrets
    /// (`CREATE USER ... password=...`, `CREATE CATALOG ... dsn`).
    pub fn statement_for(sql: &str, is_command: bool) -> String {
        if !is_command {
            return sql.to_string();
        }
        let prefix: Vec<&str> = sql.split_whitespace().take(3).collect();
        format!("{} …", prefix.join(" "))
    }
}

async fn flush(meta: &PostgreSQLMetaCatalogProviderList, buffer: &mut Vec<AuditEvent>) {
    if buffer.is_empty() {
        return;
    }
    if let Err(e) = meta.insert_audit_events(buffer).await {
        log::warn!("Failed to persist {} audit events: {e}", buffer.len());
    }
    buffer.clear();
}

/// Truncates on a char boundary without reallocating.
fn truncate_in_place(s: &mut String, max: usize) {
    if s.len() <= max {
        return;
    }
    let mut cut = max;
    while cut > 0 && !s.is_char_boundary(cut) {
        cut -= 1;
    }
    s.truncate(cut);
    s.push('…');
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn command_statements_are_reduced_to_a_prefix() {
        let s = AuditLogger::statement_for(
            "CREATE USER alice WITH properties(password=\"s3cret\")",
            true,
        );
        assert_eq!(s, "CREATE USER alice …");
        assert!(!s.contains("s3cret"));
        // Queries keep their text.
        assert_eq!(
            AuditLogger::statement_for("SELECT * FROM t WHERE id = 1", false),
            "SELECT * FROM t WHERE id = 1"
        );
    }

    #[test]
    fn truncation_respects_char_boundaries() {
        let mut s = "数据审计日志".to_string();
        truncate_in_place(&mut s, 7); // lands mid-char; must back up
        assert!(s.starts_with("数据"));
        assert!(s.ends_with('…'));
    }
}
