use std::sync::Arc;

use kokedb_common::cache_policy::CachePolicy;
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
    }

    info!(
        "Finished running catalog sync task with catalog {} and dsn {}",
        catalog, dsn
    );

    Ok(())
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

    for table in postgres_all_table {
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

    for table in postgres_topk_table {
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

    let table_set: Vec<&str> = table_set.split(',').collect();
    for table in table_set {
        let config = CacheTableTaskConfig::new(
            catalog.to_string(),
            dsn.to_string(),
            table.to_string(),
            table.to_string(),
        );
        let ret = catalog_task_manager.add_task(config).await;
        if ret.is_err() {
            warn!(
                "Failed to add cache dsn: {} table: {} task with error: {:?}",
                &dsn,
                table,
                ret.err()
            );
        }
    }

    Ok(())
}
