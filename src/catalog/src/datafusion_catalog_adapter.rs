use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::CatalogProvider;

use crate::{
    error::{CatalogError, CatalogResult},
    provider::{
        CreateDatabaseOptions, CreateTableOptions, CreateViewOptions, DatabaseStatus,
        DropDatabaseOptions, DropTableOptions, DropViewOptions, Namespace, TableColumnStatus,
        TableKind, TableStatus,
    },
};

pub struct DataFusionCatalogAdapter {
    inner: Arc<dyn CatalogProvider>,
    catalog_name: String,
}

impl DataFusionCatalogAdapter {
    pub fn new(inner: Arc<dyn CatalogProvider>, catalog_name: String) -> Self {
        Self {
            inner,
            catalog_name,
        }
    }

    pub fn inner(&self) -> &dyn CatalogProvider {
        self.inner.as_ref()
    }

    fn create_database_status(&self, schema_name: &str) -> DatabaseStatus {
        DatabaseStatus {
            catalog: self.catalog_name.clone(),
            database: vec![schema_name.to_string()],
            comment: None,
            location: None,
            properties: vec![],
        }
    }

    async fn create_table_status(
        &self,
        schema_name: &str,
        table_name: &str,
        table_provider: Arc<dyn datafusion::datasource::TableProvider>,
    ) -> TableStatus {
        let schema = table_provider.schema();
        let columns = schema
            .fields()
            .iter()
            .map(|field| TableColumnStatus {
                name: field.name().clone(),
                data_type: field.data_type().clone(),
                nullable: field.is_nullable(),
                comment: None,
                default: None,
                generated_always_as: None,
                is_partition: false,
                is_bucket: false,
                is_cluster: false,
            })
            .collect();

        //TODO: must move to common.
        let table_path = format!(
            "file:///tmp/{}/{}/{}",
            self.catalog_name, schema_name, table_name
        );
        TableStatus {
            name: table_name.to_string(),
            kind: TableKind::Table {
                catalog: self.catalog_name.clone(),
                database: vec![schema_name.to_string()],
                columns,
                comment: None,
                constraints: vec![],
                location: Some(table_path),
                format: "parquet".to_string(),
                partition_by: vec![],
                sort_by: vec![],
                bucket_by: None,
                options: vec![],
                properties: vec![],
            },
        }
    }
}

#[async_trait]
impl crate::provider::CatalogProvider for DataFusionCatalogAdapter {
    fn get_name(&self) -> &str {
        &self.catalog_name
    }

    async fn create_database(
        &self,
        _database: &Namespace,
        _options: CreateDatabaseOptions,
    ) -> CatalogResult<DatabaseStatus> {
        Err(CatalogError::NotSupported("create_database".to_string()))
    }

    async fn drop_database(
        &self,
        _database: &Namespace,
        _options: DropDatabaseOptions,
    ) -> CatalogResult<()> {
        Err(CatalogError::NotSupported("drop_database".to_string()))
    }

    async fn get_database(&self, database: &Namespace) -> CatalogResult<DatabaseStatus> {
        let schema_name = &database.head;

        if self.inner.schema(schema_name).is_some() {
            Ok(self.create_database_status(schema_name))
        } else {
            Err(CatalogError::NotFound("database", database.to_string()))
        }
    }

    async fn list_databases(
        &self,
        _prefix: Option<&Namespace>,
    ) -> CatalogResult<Vec<DatabaseStatus>> {
        let schema_names = self.inner.schema_names();
        let databases = schema_names
            .into_iter()
            .map(|name| self.create_database_status(&name))
            .collect();

        Ok(databases)
    }

    async fn create_table(
        &self,
        _database: &Namespace,
        _table: &str,
        _options: CreateTableOptions,
    ) -> CatalogResult<TableStatus> {
        Err(CatalogError::NotSupported("create_table".to_string()))
    }

    async fn get_table(&self, database: &Namespace, table: &str) -> CatalogResult<TableStatus> {
        let schema_name = &database.head;

        if let Some(schema) = self.inner.schema(schema_name) {
            if let Some(table_provider) = schema
                .table(table)
                .await
                .map_err(|e| CatalogError::Internal(format!("DataFusion error: {}", e)))?
            {
                Ok(self
                    .create_table_status(schema_name, table, table_provider)
                    .await)
            } else {
                Err(CatalogError::NotFound("table", table.to_string()))
            }
        } else {
            Err(CatalogError::NotFound("database", schema_name.to_string()))
        }
    }

    async fn list_tables(&self, database: &Namespace) -> CatalogResult<Vec<TableStatus>> {
        let schema_name = &database.head;

        if let Some(schema) = self.inner.schema(schema_name) {
            let table_names = schema.table_names();
            let mut tables = Vec::new();

            for table_name in table_names {
                if let Ok(table_status) = self.get_table(database, &table_name).await {
                    tables.push(table_status);
                }
            }

            Ok(tables)
        } else {
            Err(CatalogError::NotFound("database", schema_name.to_string()))
        }
    }

    async fn drop_table(
        &self,
        _database: &Namespace,
        _table: &str,
        _options: DropTableOptions,
    ) -> CatalogResult<()> {
        Err(CatalogError::NotSupported("drop_table".to_string()))
    }

    async fn create_view(
        &self,
        _: &Namespace,
        _: &str,
        _: CreateViewOptions,
    ) -> CatalogResult<TableStatus> {
        Err(CatalogError::NotSupported("create_view".to_string()))
    }

    async fn get_view(&self, _: &Namespace, _: &str) -> CatalogResult<TableStatus> {
        Err(CatalogError::NotSupported("get_view".to_string()))
    }

    async fn list_views(&self, _: &Namespace) -> CatalogResult<Vec<TableStatus>> {
        Ok(vec![])
    }

    async fn drop_view(&self, _: &Namespace, _: &str, _: DropViewOptions) -> CatalogResult<()> {
        Err(CatalogError::NotSupported("drop_view".to_string()))
    }
}
