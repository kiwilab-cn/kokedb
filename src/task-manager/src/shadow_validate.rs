//! Shadow validation runner (Phase 2c).
//!
//! For a table whose incremental strategy is still being proven, independently
//! materialize the *full* truth and the *incremental candidate* (previous
//! snapshot + delta merge) into temp dirs and compare them exactly. The pure
//! state machine in [`crate::validation`] then promotes, demotes, or reschedules
//! the table. This is fully isolated from the live cache (read-only, except a
//! force-full nudge to self-heal on divergence), so it can never corrupt a
//! served snapshot.

use kokedb_common::env::get_env_as;
use kokedb_meta::catalog_list::PostgreSQLMetaCatalogProviderList;
use log::{info, warn};
use sqlx::PgPool;
use uuid::Uuid;

use crate::error::TaskError;
use crate::incremental::{
    compare_snapshots, filter_snapshot_window, incremental_where, merge_snapshot,
};
use crate::read_postgres::{convert_postgres_to_parquet, PostgresToParquetConverter};
use crate::validation::{apply_audit, AuditOutcome, AuditState, AuditThresholds};

fn split_schema_table(name: &str) -> (String, String) {
    match name.split_once('.') {
        Some((s, t)) => (s.to_string(), t.to_string()),
        None => ("public".to_string(), name.to_string()),
    }
}

fn thresholds() -> AuditThresholds {
    AuditThresholds {
        probation_passes: get_env_as("KOKEDB_PROBATION_PASSES", 2u32),
        divergence_max: get_env_as("KOKEDB_AUDIT_DIVERGENCE_MAX", 2u32),
        base_audit_secs: get_env_as("KOKEDB_AUDIT_BASE_INTERVAL_MIN", 1440i64) * 60,
    }
}

/// Picks the audit-due tables for a catalog and validates up to a small budget
/// per call (audits are low-priority and must not crowd out real syncs).
pub async fn sweep_audits(catalog: &str, dsn: &str) -> Result<(), TaskError> {
    let meta = PostgreSQLMetaCatalogProviderList::new().await.map_err(|e| {
        TaskError::MetaReqeustError(format!("Failed to create meta client: {e}"))
    })?;
    let due = meta.get_due_audit_tables(catalog).await.map_err(|e| {
        TaskError::MetaReqeustError(format!("Failed to query audit-due tables: {e}"))
    })?;
    let budget = get_env_as("KOKEDB_AUDIT_MAX_PER_TICK", 2usize);
    for table in due.into_iter().take(budget) {
        if let Err(e) = validate_table(catalog, dsn, &table, &meta).await {
            warn!("Shadow validation failed for {catalog}.{table}: {e}");
        }
    }
    Ok(())
}

/// Runs one shadow validation for a single table and applies the outcome.
pub async fn validate_table(
    catalog: &str,
    dsn: &str,
    source_table: &str,
    meta: &PostgreSQLMetaCatalogProviderList,
) -> Result<(), TaskError> {
    let (schema, table) = split_schema_table(source_table);

    let inc = match meta.get_table_inc_policy(catalog, &schema, &table).await {
        Ok(Some(p)) => p,
        _ => return Ok(()),
    };
    // Only inferred, not-yet-rejected strategies are validatable.
    if inc.inc_mode == "full" || inc.inc_status == "rejected" {
        return Ok(());
    }
    let (Some(watermark), Some(pk_csv)) = (inc.watermark_column.clone(), inc.pk_columns.clone())
    else {
        return Ok(());
    };
    let pk_columns: Vec<String> = pk_csv.split(',').map(|s| s.to_string()).collect();

    // Need a prior snapshot + watermark to reconstruct the incremental candidate.
    let sync_state = meta
        .get_table_sync_state(catalog, &schema, &table)
        .await
        .ok()
        .flatten()
        .unwrap_or_default();
    let prev_path = meta
        .get_table_schema(catalog, &schema, &table)
        .await
        .map(|(_, path)| path)
        .unwrap_or_default();
    let Some(last_watermark) = sync_state.last_watermark.clone() else {
        return Ok(());
    };
    if prev_path.is_empty() {
        return Ok(());
    }

    // Isolated temp workspace.
    let base = std::env::temp_dir().join(format!("kokedb-validate/{}", Uuid::new_v4()));
    let full_dir = base.join("full");
    let delta_dir = base.join("delta");
    let inc_dir = base.join("inc");
    let (full_s, delta_s, inc_s) = (
        full_dir.to_string_lossy().to_string(),
        delta_dir.to_string_lossy().to_string(),
        inc_dir.to_string_lossy().to_string(),
    );

    let result = run_validation(
        dsn,
        source_table,
        &watermark,
        &last_watermark,
        &prev_path,
        &pk_columns,
        &full_s,
        &delta_s,
        &inc_s,
    )
    .await;
    let _ = tokio::fs::remove_dir_all(&base).await;

    let matched = result?;
    let outcome = if matched {
        AuditOutcome::Pass
    } else {
        AuditOutcome::Diverge
    };

    let state = AuditState {
        status: inc.inc_status.clone(),
        tier: inc.inc_tier.clone(),
        passes: inc.audit_passes.max(0) as u32,
        divergence: inc.divergence_count.max(0) as u32,
    };
    let t = apply_audit(&state, outcome, &thresholds());

    meta.update_audit_result(
        catalog,
        &schema,
        &table,
        &t.status,
        t.passes as i32,
        t.divergence as i32,
        t.next_audit_secs,
    )
    .await
    .map_err(|e| TaskError::MetaReqeustError(format!("Failed to persist audit result: {e}")))?;

    if t.self_heal_full {
        force_full_next_sync(meta, catalog, &schema, &table, &sync_state).await;
    }

    info!(
        "Shadow audit {catalog}.{schema}.{table}: {outcome:?} -> status={} passes={} div={}",
        t.status, t.passes, t.divergence
    );
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn run_validation(
    dsn: &str,
    source_table: &str,
    watermark: &str,
    last_watermark: &str,
    prev_path: &str,
    pk_columns: &[String],
    full_dir: &str,
    delta_dir: &str,
    inc_dir: &str,
) -> Result<bool, TaskError> {
    // Bound the audit cost to a recent window when the watermark is a timestamp:
    // pull/compare only rows newer than `boundary`. Older rows are reconciled by
    // the periodic full refresh (force_full_every), so this trades a little
    // detection latency for a much cheaper audit on large historical tables.
    // For non-timestamp watermarks (e.g. APPEND on a bigint id) the boundary
    // query fails to cast and we fall back to a full validation.
    let window = get_env_as("KOKEDB_AUDIT_WINDOW", "30 days".to_string());
    let boundary: Option<String> = if window.trim().is_empty() || window.trim() == "0" {
        None
    } else {
        match PgPool::connect(dsn).await {
            Ok(pool) => {
                let b = sqlx::query_scalar::<_, Option<String>>(
                    "SELECT LEAST(now() - $2::interval, $1::timestamptz)::text",
                )
                .bind(last_watermark)
                .bind(&window)
                .fetch_one(&pool)
                .await
                .ok()
                .flatten();
                pool.close().await;
                b
            }
            Err(_) => None,
        }
    };

    // Ensure temp dirs exist so empty windows still register as 0-row tables.
    let prev_window_dir = format!("{inc_dir}__prevwin");
    for d in [full_dir, delta_dir, inc_dir, &prev_window_dir] {
        let _ = tokio::fs::create_dir_all(d).await;
    }

    let converter = PostgresToParquetConverter::new(dsn).await?;

    // 1. Truth: the whole table, or just the recent window.
    let schema = match &boundary {
        Some(b) => {
            let where_truth = incremental_where(watermark, b);
            converter
                .convert_table_to_parquet_where(source_table, full_dir, Some(&where_truth), Some(watermark))
                .await?
        }
        None => convert_postgres_to_parquet(dsn, source_table, full_dir, Some(watermark)).await?,
    };

    // 2. Delta since the last watermark (always a subset of the window).
    converter
        .convert_table_to_parquet_where(
            source_table,
            delta_dir,
            Some(&incremental_where(watermark, last_watermark)),
            Some(watermark),
        )
        .await?;

    // 3. Incremental candidate = previous snapshot (windowed to match the truth)
    //    merged with the delta.
    let prev_for_merge = match &boundary {
        Some(b) => {
            filter_snapshot_window(prev_path, &prev_window_dir, watermark, b, schema.clone()).await?;
            prev_window_dir.as_str()
        }
        None => prev_path,
    };
    merge_snapshot(
        prev_for_merge,
        delta_dir,
        inc_dir,
        pk_columns,
        schema.clone(),
        Some(watermark),
    )
    .await?;

    // 4. Exact comparison over the (windowed) row sets.
    let diff = compare_snapshots(full_dir, inc_dir, schema).await?;
    if !diff.matches() {
        warn!(
            "Shadow diff for {source_table} (window={}): truth={} inc={} differing={}",
            boundary.as_deref().unwrap_or("full"),
            diff.count_a,
            diff.count_b,
            diff.diff_rows
        );
    }
    Ok(diff.matches())
}

/// Nudges the next sync to be a full reconcile by pushing the incremental-run
/// counter past the force-full threshold, so a divergent cache self-heals.
async fn force_full_next_sync(
    meta: &PostgreSQLMetaCatalogProviderList,
    catalog: &str,
    schema: &str,
    table: &str,
    sync_state: &kokedb_meta::catalog_list::TableSyncState,
) {
    let mut state = sync_state.clone();
    state.incremental_runs = i32::MAX / 2;
    if let Err(e) = meta
        .upsert_table_sync_state(catalog, schema, table, &state)
        .await
    {
        warn!("Failed to force full sync for {catalog}.{schema}.{table}: {e}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::{PgPool, Row};

    fn test_dsn() -> String {
        std::env::var("KOKEDB_TEST_DSN")
            .unwrap_or_else(|_| "postgresql://postgres:123456@127.0.0.1:25432/postgres".into())
    }

    // The core safety property: shadow validation passes when the incremental
    // candidate equals the full truth, and CATCHES a silent update (a row changed
    // without bumping the watermark — exactly what watermark incremental misses).
    #[tokio::test]
    #[ignore = "requires PostgreSQL; run via KOKEDB_TEST_DSN"]
    async fn run_validation_passes_clean_and_catches_silent_update() {
        std::env::set_var("KOKEDB_AUDIT_WINDOW", "30 days");
        let dsn = test_dsn();
        let pool = PgPool::connect(&dsn).await.unwrap();
        for stmt in [
            "DROP TABLE IF EXISTS it_val",
            "CREATE TABLE it_val (id int PRIMARY KEY, v int, \
             updated_at timestamptz NOT NULL DEFAULT now())",
            "INSERT INTO it_val(id, v) SELECT g, g FROM generate_series(1, 100) g",
        ] {
            sqlx::query(stmt).execute(&pool).await.unwrap();
        }

        let base = std::env::temp_dir().join(format!("kokedb-valtest/{}", Uuid::new_v4()));
        let prev = base.join("prev").to_string_lossy().to_string();
        let full = base.join("full").to_string_lossy().to_string();
        let delta = base.join("delta").to_string_lossy().to_string();
        let inc = base.join("inc").to_string_lossy().to_string();

        // Snapshot the current state as the "previous" snapshot, record watermark.
        convert_postgres_to_parquet(&dsn, "public.it_val", &prev, Some("updated_at"))
            .await
            .unwrap();
        let last_wm: String = sqlx::query("SELECT max(updated_at)::text AS wm FROM it_val")
            .fetch_one(&pool)
            .await
            .unwrap()
            .get("wm");

        // Append new rows (a legitimate delta the watermark sees).
        sqlx::query("INSERT INTO it_val(id, v) SELECT g, g FROM generate_series(101, 130) g")
            .execute(&pool)
            .await
            .unwrap();

        let pk = vec!["id".to_string()];
        let clean = run_validation(
            &dsn, "public.it_val", "updated_at", &last_wm, &prev, &pk, &full, &delta, &inc,
        )
        .await
        .unwrap();
        assert!(clean, "clean append must validate as a match");

        // Silent update: change an old row WITHOUT bumping updated_at. Reuse the
        // same `prev` snapshot but fresh output dirs.
        sqlx::query("UPDATE it_val SET v = -1 WHERE id = 5")
            .execute(&pool)
            .await
            .unwrap();
        let full2 = base.join("full2").to_string_lossy().to_string();
        let delta2 = base.join("delta2").to_string_lossy().to_string();
        let inc2 = base.join("inc2").to_string_lossy().to_string();
        let caught = run_validation(
            &dsn, "public.it_val", "updated_at", &last_wm, &prev, &pk, &full2, &delta2, &inc2,
        )
        .await
        .unwrap();
        assert!(!caught, "silent update must be caught as a divergence");

        let _ = tokio::fs::remove_dir_all(&base).await;
        sqlx::query("DROP TABLE IF EXISTS it_val").execute(&pool).await.unwrap();
    }

    // The windowed audit compares only rows newer than the boundary: a silent
    // update INSIDE the window is caught; one OUTSIDE it is not (and is left for
    // the periodic full reconcile). Verifies the cost/latency tradeoff boundary.
    #[tokio::test]
    #[ignore = "requires PostgreSQL; run via KOKEDB_TEST_DSN"]
    async fn windowed_audit_scopes_to_recent_rows() {
        std::env::set_var("KOKEDB_AUDIT_WINDOW", "7 days");
        let dsn = test_dsn();
        let pool = PgPool::connect(&dsn).await.unwrap();
        for stmt in [
            "DROP TABLE IF EXISTS it_win",
            // No trigger: we control updated_at directly.
            "CREATE TABLE it_win (id int PRIMARY KEY, v int, updated_at timestamptz NOT NULL)",
            "INSERT INTO it_win SELECT g, g, now() - interval '60 days' FROM generate_series(1, 50) g",
            "INSERT INTO it_win SELECT g, g, now() FROM generate_series(51, 100) g",
        ] {
            sqlx::query(stmt).execute(&pool).await.unwrap();
        }

        let base = std::env::temp_dir().join(format!("kokedb-wintest/{}", Uuid::new_v4()));
        let prev = base.join("prev").to_string_lossy().to_string();
        convert_postgres_to_parquet(&dsn, "public.it_win", &prev, Some("updated_at"))
            .await
            .unwrap();
        let last_wm: String = sqlx::query("SELECT max(updated_at)::text AS wm FROM it_win")
            .fetch_one(&pool)
            .await
            .unwrap()
            .get("wm");
        let pk = vec!["id".to_string()];

        let dirs = |n: &str| {
            (
                base.join(format!("{n}f")).to_string_lossy().to_string(),
                base.join(format!("{n}d")).to_string_lossy().to_string(),
                base.join(format!("{n}i")).to_string_lossy().to_string(),
            )
        };

        // Silent update to an OLD row (updated_at 60d ago, outside the 7d window).
        sqlx::query("UPDATE it_win SET v = -1 WHERE id = 10")
            .execute(&pool)
            .await
            .unwrap();
        let (f, d, i) = dirs("old");
        let old_pass = run_validation(
            &dsn, "public.it_win", "updated_at", &last_wm, &prev, &pk, &f, &d, &i,
        )
        .await
        .unwrap();
        assert!(old_pass, "an update outside the window is not audited (left to full reconcile)");

        // Silent update to a RECENT row (updated_at now, inside the window).
        sqlx::query("UPDATE it_win SET v = -1 WHERE id = 60")
            .execute(&pool)
            .await
            .unwrap();
        let (f, d, i) = dirs("new");
        let recent_caught = run_validation(
            &dsn, "public.it_win", "updated_at", &last_wm, &prev, &pk, &f, &d, &i,
        )
        .await
        .unwrap();
        assert!(!recent_caught, "an update inside the window must be caught");

        let _ = tokio::fs::remove_dir_all(&base).await;
        sqlx::query("DROP TABLE IF EXISTS it_win").execute(&pool).await.unwrap();
    }
}
