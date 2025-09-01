use std::{any::Any, sync::Arc};

use datafusion::catalog::{CatalogProvider, CatalogProviderList, SchemaProvider, TableProvider};
use datafusion_common::DataFusionError;
use sqlx::PgPool;

#[derive(Debug)]
pub struct MetaCatalogProviderList {
    pub store: Arc<PgPool>,
}

const DEFAULT_CATALOG: &str = "kokedb";

impl CatalogProviderList for MetaCatalogProviderList {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn register_catalog(
        &self,
        _name: String,
        _catalog: Arc<dyn datafusion::catalog::CatalogProvider>,
    ) -> Option<Arc<dyn datafusion::catalog::CatalogProvider>> {
        None
    }

    fn catalog_names(&self) -> Vec<String> {
        let pool = self.store.clone();
    }

    fn catalog(&self, name: &str) -> Option<Arc<dyn datafusion::catalog::CatalogProvider>> {
        Some(Arc::new(MetaCatalogProvider {
            store: self.store.clone(),
            catalog: name.to_string(),
        }))
    }
}

#[derive(Debug, Clone)]
struct MetaCatalogProvider {
    pub store: Arc<PgPool>,
    pub catalog: String,
}

impl CatalogProvider for MetaCatalogProvider {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        todo!()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn datafusion::catalog::SchemaProvider>> {
        todo!()
    }
}

#[derive(Debug)]
struct MetaSchemaProvider {
    pub store: Arc<PgPool>,
    pub catalog: String,
    pub schema: String,
}

impl SchemaProvider for MetaSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        todo!()
    }

    // TODO: improve code for result handling
    async fn table(&self, _name: &str) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        todo!()
    }

    fn register_table(
        &self,
        _name: String,
        _table: Arc<dyn TableProvider>,
    ) -> datafusion::error::Result<Option<Arc<dyn TableProvider>>> {
        Err(DataFusionError::NotImplemented(
            "Please use meta client to create table".to_string(),
        ))
    }

    fn deregister_table(
        &self,
        _name: &str,
    ) -> datafusion::error::Result<Option<Arc<dyn TableProvider>>> {
        Err(DataFusionError::NotImplemented(
            "Please use meta client to delete table".to_string(),
        ))
    }

    fn table_exist(&self, name: &str) -> bool {
        todo!()
    }
}
