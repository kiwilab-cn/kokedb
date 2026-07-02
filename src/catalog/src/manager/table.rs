use crate::error::{CatalogError, CatalogResult};
use crate::manager::CatalogManager;
use crate::provider::{CreateTableOptions, DropTableOptions, TableStatus};
use crate::utils::match_pattern;

impl CatalogManager {
    pub async fn create_table<T: AsRef<str>>(
        &self,
        table: &[T],
        options: CreateTableOptions,
    ) -> CatalogResult<TableStatus> {
        let (provider, database, table) = self.resolve_object(table)?;
        provider.create_table(&database, &table, options).await
    }

    pub async fn get_table<T: AsRef<str>>(&self, table: &[T]) -> CatalogResult<TableStatus> {
        let (provider, database, table) = self.resolve_object(table)?;
        provider.get_table(&database, &table).await
    }

    pub async fn list_tables<T: AsRef<str>>(
        &self,
        database: &[T],
        pattern: Option<&str>,
    ) -> CatalogResult<Vec<TableStatus>> {
        let (provider, database) = if database.is_empty() {
            self.resolve_default_database()?
        } else {
            self.resolve_database(database)?
        };
        let tables = provider.list_tables(&database).await?;
        // Scoped sessions only see tables their grants cover (a table-level
        // grant makes the database listable, but not its sibling tables).
        let state = self.state()?;
        let catalog = provider.get_name();
        Ok(tables
            .into_iter()
            .filter(|x| state.acl_allows_table(&catalog, &database.head, &x.name))
            .filter(|x| match_pattern(&x.name, pattern))
            .collect())
    }

    pub async fn list_tables_and_temporary_views<T: AsRef<str>>(
        &self,
        database: &[T],
        pattern: Option<&str>,
    ) -> CatalogResult<Vec<TableStatus>> {
        // Spark *global* temporary views should be put in the "global temporary" database, and they will be
        // included in the output if the database name matches.
        let mut output = if self.state()?.is_global_temporary_view_database(database) {
            self.list_global_temporary_views(pattern).await?
        } else {
            self.list_tables(database, pattern).await?
        };
        // Spark (local) temporary views are session-scoped and are not associated with a catalog.
        // We should include the temporary views in the output.
        output.extend(self.list_temporary_views(pattern).await?);
        Ok(output)
    }

    pub async fn drop_table<T: AsRef<str>>(
        &self,
        table: &[T],
        options: DropTableOptions,
    ) -> CatalogResult<()> {
        let (provider, database, table) = self.resolve_object(table)?;
        provider.drop_table(&database, &table, options).await
    }

    pub async fn get_table_or_view<T: AsRef<str>>(
        &self,
        reference: &[T],
    ) -> CatalogResult<TableStatus> {
        if let [name] = reference {
            match self.get_temporary_view(name.as_ref()).await {
                Ok(x) => return Ok(x),
                Err(CatalogError::NotFound(_, _)) => {}
                Err(e) => return Err(e),
            }
        }
        if let [x @ .., name] = reference {
            if self.state()?.is_global_temporary_view_database(x) {
                return self.get_global_temporary_view(name.as_ref()).await;
            }
        }
        match self.get_table(reference).await {
            Ok(x) => return Ok(x),
            Err(CatalogError::NotFound(_, _)) => {}
            Err(e) => return Err(e),
        }
        self.get_view(reference).await
    }

    pub async fn save_hash_key<T: AsRef<str>>(
        &self,
        table: &[T],
        cache_key: u128,
    ) -> CatalogResult<()> {
        let (provider, database, table) = self.resolve_object(table)?;
        let catalog = provider.get_name();
        let schema = database.head;

        let remote_catalog = {
            let state = self.state()?;
            state.dynamic_catalog_list.clone()
        };

        remote_catalog
            .save_table_cache_key(catalog, &schema, &table, cache_key)
            .await
            .map_err(|x| {
                CatalogError::External(format!(
                    "Failed to save table cache key with error:{}",
                    x.to_string()
                ))
            })?;
        Ok(())
    }

    /// Cost-routing inputs for a table: `(estimated_rows, pk_columns)` from the
    /// sync policy. Empty/None when no policy exists yet.
    pub async fn get_table_routing_info<T: AsRef<str>>(
        &self,
        table: &[T],
    ) -> CatalogResult<(Option<usize>, Vec<String>)> {
        let (provider, database, table) = self.resolve_object(table)?;
        let catalog = provider.get_name();
        let schema = database.head;

        let remote_catalog = {
            let state = self.state()?;
            state.dynamic_catalog_list.clone()
        };

        let info = remote_catalog
            .get_table_routing_info(catalog, &schema, &table)
            .await
            .map_err(|x| CatalogError::External(format!("Failed to read routing info: {x}")))?;

        let Some((est_rows, pk_csv)) = info else {
            return Ok((None, Vec::new()));
        };
        let rows = est_rows.filter(|n| *n >= 0).map(|n| n as usize);
        let pk = pk_csv
            .map(|s| {
                s.split(',')
                    .map(|c| c.trim().to_string())
                    .filter(|c| !c.is_empty())
                    .collect()
            })
            .unwrap_or_default();
        Ok((rows, pk))
    }

    /// Timestamp of the table's last completed sync, or `None` if never synced.
    /// Consulted by the query-time freshness guard.
    pub async fn get_table_last_sync_at<T: AsRef<str>>(
        &self,
        table: &[T],
    ) -> CatalogResult<Option<chrono::DateTime<chrono::Utc>>> {
        let (provider, database, table) = self.resolve_object(table)?;
        let catalog = provider.get_name();
        let schema = database.head;

        let remote_catalog = {
            let state = self.state()?;
            state.dynamic_catalog_list.clone()
        };

        remote_catalog
            .get_table_last_sync_at(catalog, &schema, &table)
            .await
            .map_err(|x| {
                CatalogError::External(format!("Failed to read table last_sync_at: {x}"))
            })
    }
}
