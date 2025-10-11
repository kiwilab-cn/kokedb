use std::sync::Arc;

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

    if hot_tables.is_empty() {
        warn!("Found empty hot table, so the dsn: {} is not cached.", dsn);
        return Ok(());
    }

    add_table_sync_task(hot_tables, catalog, dsn, catalog_task_manager).await
}

async fn create_all_table_task(
    catalog: &str,
    dsn: &str,
    catalog_task_manager: Arc<TaskManager>,
) -> Result<(), TaskError> {
    let postgres_all_table = get_postgres_all_tables(dsn).await?;
    if postgres_all_table.len() > 100 {
        warn!(
            "Found catalog: {} with dsn: {} set to cache all tables and the size is too big: {}",
            catalog,
            dsn,
            postgres_all_table.len()
        );
    }

    add_table_sync_task(postgres_all_table, catalog, dsn, catalog_task_manager).await
}

async fn create_topk_table_task(
    catalog: &str,
    dsn: &str,
    k: u32,
    catalog_task_manager: Arc<TaskManager>,
) -> Result<(), TaskError> {
    if k == 0 {
        return Err(TaskError::InvalideTaskArgment(
            "The k must be large than 0.".to_string(),
        ));
    }

    let postgres_topk_table = get_postgres_top_tables(dsn, k as usize).await?;

    add_table_sync_task(postgres_topk_table, catalog, dsn, catalog_task_manager).await
}

async fn create_select_table_task(
    catalog: &str,
    dsn: &str,
    table_set: &str,
    catalog_task_manager: Arc<TaskManager>,
) -> Result<(), TaskError> {
    if table_set.is_empty() {
        return Err(TaskError::InvalideTaskArgment(
            "The table_set config in cache_policy must be unempty.".to_string(),
        ));
    }

    let table_set: Vec<String> = table_set.split(',').map(|x| x.to_string()).collect();

    add_table_sync_task(table_set, catalog, dsn, catalog_task_manager).await
}
