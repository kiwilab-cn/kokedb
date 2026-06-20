use std::{collections::HashSet, sync::Arc};

use kokedb_common::cache_policy::CachePolicy;
use kokedb_meta::catalog_list::PostgreSQLMetaCatalogProviderList;
use log::{info, warn};

use crate::{
    error::TaskError,
    postgres_table_analyzer::{get_postgres_all_tables, get_postgres_top_tables},
    task_manager::{CacheTableTaskConfig, TaskManager},
};

pub async fn execute_catalog_sync_task(
    dsn: &str,
    catalog: &str,
    catalog_task_manager: Arc<TaskManager>,
    cache_policy: CachePolicy,
) -> Result<(), TaskError> {
    info!(
        "Begin running catalog sync task with catalog {} and dsn {} with policy: {}",
        catalog,
        dsn,
        cache_policy.to_string()
    );

    match cache_policy {
        CachePolicy::TopK { k } => {
            create_topk_table_task(catalog, dsn, k, catalog_task_manager.clone()).await?
        }
        CachePolicy::All => {
            create_all_table_task(catalog, dsn, catalog_task_manager.clone()).await?
        }
        CachePolicy::Select { table_set } => {
            create_select_table_task(catalog, dsn, &table_set, catalog_task_manager.clone()).await?
        }
        CachePolicy::Smart => {
            create_smart_table_task(catalog, dsn, catalog_task_manager.clone()).await?
        }
    }

    info!(
        "Finished running catalog sync task with catalog {} and dsn {}",
        catalog, dsn
    );

    Ok(())
}

/// Enqueues a sync task for a single table and returns its task id. Backs the
/// manual `REFRESH CACHE FROM TABLE` command. Whether the run is full or
/// incremental is decided later by the runner from detected metadata and the
/// persisted sync state, so a manual refresh reuses the same code path as the
/// scheduled catalog sync.
pub async fn refresh_single_table(
    catalog: &str,
    dsn: &str,
    source_table: &str,
    catalog_task_manager: Arc<TaskManager>,
) -> Result<String, TaskError> {
    let config = CacheTableTaskConfig::new(
        catalog.to_string(),
        dsn.to_string(),
        source_table.to_string(),
        source_table.to_string(),
    );
    catalog_task_manager.add_task(config).await
}

async fn add_table_sync_task(
    tables: Vec<String>,
    catalog: &str,
    dsn: &str,
    catalog_task_manager: Arc<TaskManager>,
) -> Result<(), TaskError> {
    for table in tables {
        let config = CacheTableTaskConfig::new(
            catalog.to_string(),
            dsn.to_string(),
            table.clone(),
            table.clone(),
        );
        let ret = catalog_task_manager.add_task(config).await;
        if ret.is_err() {
            warn!(
                "Failed to add cache dsn: {} table: {} task with error: {:?}",
                &dsn,
                &table,
                ret.err()
            );
        }
    }
    Ok(())
}

async fn create_smart_table_task(
    catalog: &str,
    dsn: &str,
    catalog_task_manager: Arc<TaskManager>,
) -> Result<(), TaskError> {
    let merged_tables = select_smart_tables(catalog, dsn).await?;
    add_table_sync_task(merged_tables, catalog, dsn, catalog_task_manager).await
}

async fn create_all_table_task(
    catalog: &str,
    dsn: &str,
    catalog_task_manager: Arc<TaskManager>,
) -> Result<(), TaskError> {
    let postgres_all_table = select_all_tables(catalog, dsn).await?;
    add_table_sync_task(postgres_all_table, catalog, dsn, catalog_task_manager).await
}

async fn create_topk_table_task(
    catalog: &str,
    dsn: &str,
    k: u32,
    catalog_task_manager: Arc<TaskManager>,
) -> Result<(), TaskError> {
    let postgres_topk_table = select_topk_tables(dsn, k).await?;
    add_table_sync_task(postgres_topk_table, catalog, dsn, catalog_task_manager).await
}

async fn create_select_table_task(
    catalog: &str,
    dsn: &str,
    table_set: &str,
    catalog_task_manager: Arc<TaskManager>,
) -> Result<(), TaskError> {
    let table_set = select_listed_tables(table_set)?;
    add_table_sync_task(table_set, catalog, dsn, catalog_task_manager).await
}

async fn select_smart_tables(catalog: &str, dsn: &str) -> Result<Vec<String>, TaskError> {
    let meta_client = PostgreSQLMetaCatalogProviderList::new()
        .await
        .map_err(|x| {
            TaskError::MetaReqeustError(format!("Failed to create meta client with error:{:?}", x))
        })?;

    let hot_tables = meta_client
        .get_recent_table_stats(catalog)
        .await
        .map_err(|x| {
            TaskError::MetaReqeustError(format!(
                "Failed to query meta hot tables with error:{:?}",
                x
            ))
        })?;

    let postgres_topk_table = get_postgres_top_tables(dsn, 10).await?;
    Ok(postgres_topk_table
        .into_iter()
        .chain(hot_tables)
        .collect::<HashSet<_>>()
        .into_iter()
        .collect::<Vec<String>>())
}

async fn select_all_tables(catalog: &str, dsn: &str) -> Result<Vec<String>, TaskError> {
    let postgres_all_table = get_postgres_all_tables(dsn).await?;
    if postgres_all_table.len() > 100 {
        warn!(
            "Found catalog: {} with dsn: {} set to cache all tables and the size is too big: {}",
            catalog,
            dsn,
            postgres_all_table.len()
        );
    }
    Ok(postgres_all_table)
}

async fn select_topk_tables(dsn: &str, k: u32) -> Result<Vec<String>, TaskError> {
    if k == 0 {
        return Err(TaskError::InvalideTaskArgment(
            "The k must be large than 0.".to_string(),
        ));
    }
    get_postgres_top_tables(dsn, k as usize).await
}

fn select_listed_tables(table_set: &str) -> Result<Vec<String>, TaskError> {
    if table_set.is_empty() {
        return Err(TaskError::InvalideTaskArgment(
            "The table_set config in cache_policy must be unempty.".to_string(),
        ));
    }
    Ok(table_set.split(',').map(|x| x.to_string()).collect())
}

/// Returns the `schema.table` names a cache policy selects, without enqueuing
/// any sync. Used by the adaptive re-evaluator to learn the cached-table set.
pub async fn select_tables_for_policy(
    catalog: &str,
    dsn: &str,
    cache_policy: &CachePolicy,
) -> Result<Vec<String>, TaskError> {
    match cache_policy {
        CachePolicy::TopK { k } => select_topk_tables(dsn, *k).await,
        CachePolicy::All => select_all_tables(catalog, dsn).await,
        CachePolicy::Select { table_set } => select_listed_tables(table_set),
        CachePolicy::Smart => select_smart_tables(catalog, dsn).await,
    }
}
