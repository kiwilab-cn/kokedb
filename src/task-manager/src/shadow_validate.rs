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
use uuid::Uuid;

use crate::error::TaskError;
use crate::incremental::{compare_snapshots, incremental_where, merge_snapshot};
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
    // 1. Full truth.
    let schema = convert_postgres_to_parquet(dsn, source_table, full_dir, Some(watermark)).await?;

    // 2. Delta since the last watermark.
    let converter = PostgresToParquetConverter::new(dsn).await?;
    let where_clause = incremental_where(watermark, last_watermark);
    converter
        .convert_table_to_parquet_where(
            source_table,
            delta_dir,
            Some(&where_clause),
            Some(watermark),
        )
        .await?;

    // 3. Incremental candidate = previous snapshot merged with the delta.
    merge_snapshot(
        prev_path,
        delta_dir,
        inc_dir,
        pk_columns,
        schema.clone(),
        Some(watermark),
    )
    .await?;

    // 4. Exact comparison.
    let diff = compare_snapshots(full_dir, inc_dir, schema).await?;
    if !diff.matches() {
        warn!(
            "Shadow diff for {source_table}: full={} inc={} differing={}",
            diff.count_a, diff.count_b, diff.diff_rows
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
}
