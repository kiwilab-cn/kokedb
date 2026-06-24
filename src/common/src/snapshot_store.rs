//! Optional shared object-store backend for cached table snapshots.
//!
//! By default snapshots live on the local filesystem (under
//! `get_remote_catalog_parent_local_path`), which only the writing node can
//! read. Setting `KOKEDB_SNAPSHOT_URI` to an object-store base (e.g.
//! `s3://bucket/kokedb/snapshots`) makes the sync runner upload each finished
//! snapshot there and record its `s3://…` URI, so any node can read it and the
//! snapshots survive a node restart. Credentials come from the standard cloud
//! environment variables (`AWS_*`, etc.).
//!
//! This module is object-store-only (no DataFusion); registering the store into
//! a DataFusion `RuntimeEnv` for the read path is done by the query crate.

use std::sync::Arc;

use bytes::Bytes;
use futures::StreamExt;
use object_store::{path::Path as StorePath, ObjectStore, PutPayload};
use url::Url;

use crate::error::{CommonError, CommonResult};

/// The configured snapshot base URI, or `None` for the local-filesystem default
/// (also `None` when it points at the local filesystem via `file:`).
pub fn snapshot_base_uri() -> Option<String> {
    let uri = std::env::var("KOKEDB_SNAPSHOT_URI")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())?;
    if uri.starts_with("file:") || uri.starts_with('/') {
        None
    } else {
        Some(uri)
    }
}

/// Whether snapshots are stored in a shared object store rather than locally.
pub fn is_remote() -> bool {
    snapshot_base_uri().is_some()
}

fn err<E: std::fmt::Display>(ctx: &str, e: E) -> CommonError {
    CommonError::InternalError(format!("snapshot store: {ctx}: {e}"))
}

/// A handle to the shared object store backing cached snapshots.
#[derive(Clone)]
pub struct SnapshotStore {
    store: Arc<dyn ObjectStore>,
    /// Scheme + authority only (e.g. `s3://bucket`), the key DataFusion reads
    /// register their object store under.
    register_url: Url,
    /// Path prefix within the store (e.g. `kokedb/snapshots`), no leading slash.
    base_prefix: String,
    /// Full base, e.g. `s3://bucket/kokedb/snapshots`, used to build snapshot URIs.
    base_uri: String,
}

impl SnapshotStore {
    /// Builds the store from `KOKEDB_SNAPSHOT_URI` and cloud env credentials, or
    /// `None` when snapshots are local. Cloud config keys are taken from the
    /// environment (e.g. `AWS_ACCESS_KEY_ID`, `AWS_ENDPOINT`, `AWS_ALLOW_HTTP`).
    pub fn from_env() -> CommonResult<Option<Self>> {
        let Some(uri) = snapshot_base_uri() else {
            return Ok(None);
        };
        let base = uri.trim_end_matches('/').to_string();
        let url = Url::parse(&base).map_err(|e| err("invalid KOKEDB_SNAPSHOT_URI", e))?;

        // Build the backing store. For S3 (incl. S3-compatible like MinIO) use
        // the builder's env support, which understands the AWS_* conventions
        // (AWS_ENDPOINT, AWS_ALLOW_HTTP, …). For other schemes pass through the
        // cloud-prefixed env vars only, so unrelated vars (PATH, …) are ignored.
        let store: Arc<dyn ObjectStore> = match url.scheme() {
            "s3" => {
                let s3 = object_store::aws::AmazonS3Builder::from_env()
                    .with_url(base.as_str())
                    .build()
                    .map_err(|e| err("build s3 store", e))?;
                Arc::new(s3)
            }
            _ => {
                let opts = std::env::vars()
                    .map(|(k, v)| (k.to_ascii_lowercase(), v))
                    .filter(|(k, _)| {
                        k.starts_with("aws_")
                            || k.starts_with("azure_")
                            || k.starts_with("google_")
                            || k.starts_with("gcp_")
                    });
                let (store, _) =
                    object_store::parse_url_opts(&url, opts).map_err(|e| err("parse_url_opts", e))?;
                Arc::from(store)
            }
        };

        // Path prefix within the store (e.g. `kokedb/snapshots`); the bucket /
        // container is the URL authority and becomes the registration key.
        let base_prefix = url.path().trim_matches('/').to_string();
        let mut register_url = url.clone();
        register_url.set_path("");
        register_url.set_query(None);

        Ok(Some(Self {
            store,
            register_url,
            base_prefix,
            base_uri: base,
        }))
    }

    /// The object store and the URL it should be registered under in a
    /// DataFusion `RuntimeEnv` (so the read path can resolve snapshot URIs).
    pub fn for_registration(&self) -> (Url, Arc<dyn ObjectStore>) {
        (self.register_url.clone(), self.store.clone())
    }

    #[cfg(test)]
    fn for_test(store: Arc<dyn ObjectStore>, base_uri: &str, base_prefix: &str) -> Self {
        Self {
            store,
            register_url: Url::parse("memory:///").unwrap(),
            base_prefix: base_prefix.to_string(),
            base_uri: base_uri.to_string(),
        }
    }

    fn object_path(&self, rel: &str) -> StorePath {
        let joined = if self.base_prefix.is_empty() {
            rel.trim_start_matches('/').to_string()
        } else {
            format!("{}/{}", self.base_prefix, rel.trim_start_matches('/'))
        };
        StorePath::from(joined)
    }

    /// Uploads every file under `local_dir` (recursively, preserving the Hive
    /// partition layout) to `{base}/{rel}/` and returns the snapshot's full URI.
    pub async fn upload_dir(&self, local_dir: &str, rel: &str) -> CommonResult<String> {
        let rel = rel.trim_matches('/');
        let mut files = Vec::new();
        collect_files(std::path::Path::new(local_dir), std::path::Path::new(local_dir), &mut files)
            .await?;
        for (abs, relative) in files {
            let bytes = tokio::fs::read(&abs)
                .await
                .map_err(|e| err("read snapshot file", e))?;
            let dest = self.object_path(&format!("{rel}/{relative}"));
            self.store
                .put(&dest, PutPayload::from_bytes(Bytes::from(bytes)))
                .await
                .map_err(|e| err("put", e))?;
        }
        Ok(format!("{}/{}/", self.base_uri, rel))
    }

    /// Keeps the newest `keep` snapshot directories under `{base}/{table_rel}/`,
    /// never deleting `except_rel`, and removes the rest. Snapshot directories
    /// are the immediate children of the table prefix (one per sync).
    pub async fn prune_snapshots(
        &self,
        table_rel: &str,
        keep: usize,
        except_rel: &str,
    ) -> CommonResult<()> {
        let table_rel = table_rel.trim_matches('/');
        let prefix = self.object_path(table_rel);
        let except_dir = except_rel.trim_matches('/').rsplit('/').next().unwrap_or("");

        // Group objects by their first path segment under the table prefix (the
        // per-sync snapshot dir), tracking the newest modification time.
        let mut dirs: std::collections::HashMap<String, chrono::DateTime<chrono::Utc>> =
            std::collections::HashMap::new();
        let mut listing = self.store.list(Some(&prefix));
        let prefix_str = prefix.as_ref().to_string();
        while let Some(meta) = listing.next().await {
            let meta = meta.map_err(|e| err("list", e))?;
            let loc = meta.location.as_ref();
            let Some(rest) = loc.strip_prefix(&prefix_str) else {
                continue;
            };
            let Some(dir) = rest.trim_start_matches('/').split('/').next() else {
                continue;
            };
            if dir.is_empty() {
                continue;
            }
            let entry = dirs.entry(dir.to_string()).or_insert(meta.last_modified);
            if meta.last_modified > *entry {
                *entry = meta.last_modified;
            }
        }

        let mut ordered: Vec<(String, chrono::DateTime<chrono::Utc>)> = dirs.into_iter().collect();
        ordered.sort_by(|a, b| b.1.cmp(&a.1)); // newest first

        let mut kept = 0usize;
        for (dir, _) in ordered {
            if dir == except_dir {
                continue;
            }
            kept += 1;
            if kept <= keep {
                continue;
            }
            self.delete_dir(&format!("{table_rel}/{dir}")).await?;
        }
        Ok(())
    }

    /// Deletes every object under `{base}/{rel}/`.
    async fn delete_dir(&self, rel: &str) -> CommonResult<()> {
        let prefix = self.object_path(rel.trim_matches('/'));
        let mut listing = self.store.list(Some(&prefix));
        let mut locations = Vec::new();
        while let Some(meta) = listing.next().await {
            locations.push(meta.map_err(|e| err("list for delete", e))?.location);
        }
        for loc in locations {
            self.store.delete(&loc).await.map_err(|e| err("delete", e))?;
        }
        Ok(())
    }
}

/// Recursively collects `(absolute_path, path_relative_to_root)` for every file.
async fn collect_files(
    root: &std::path::Path,
    dir: &std::path::Path,
    out: &mut Vec<(std::path::PathBuf, String)>,
) -> CommonResult<()> {
    let mut entries = tokio::fs::read_dir(dir)
        .await
        .map_err(|e| err("read_dir", e))?;
    while let Some(entry) = entries.next_entry().await.map_err(|e| err("read_dir entry", e))? {
        let path = entry.path();
        let ft = entry.file_type().await.map_err(|e| err("file_type", e))?;
        if ft.is_dir() {
            Box::pin(collect_files(root, &path, out)).await?;
        } else if ft.is_file() {
            let rel = path
                .strip_prefix(root)
                .map_err(|e| err("strip_prefix", e))?
                .to_string_lossy()
                .replace('\\', "/");
            out.push((path, rel));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base_uri_treats_local_paths_as_none() {
        std::env::set_var("KOKEDB_SNAPSHOT_URI", "/var/kokedb/snap");
        assert_eq!(snapshot_base_uri(), None);
        std::env::set_var("KOKEDB_SNAPSHOT_URI", "file:///var/kokedb/snap");
        assert_eq!(snapshot_base_uri(), None);
        std::env::set_var("KOKEDB_SNAPSHOT_URI", "  ");
        assert_eq!(snapshot_base_uri(), None);
        std::env::set_var("KOKEDB_SNAPSHOT_URI", "s3://bucket/kokedb/snap");
        assert_eq!(
            snapshot_base_uri().as_deref(),
            Some("s3://bucket/kokedb/snap")
        );
        std::env::remove_var("KOKEDB_SNAPSHOT_URI");
        assert_eq!(snapshot_base_uri(), None);
    }

    // Upload preserves the (possibly partitioned) layout and returns the URI;
    // prune removes non-current snapshot dirs. Uses an in-memory store, so it is
    // deterministic and needs no S3/MinIO.
    #[tokio::test]
    async fn upload_preserves_layout_and_prune_removes_old() {
        use object_store::memory::InMemory;
        use object_store::ObjectStore as _;

        // Build a local snapshot dir with a flat file and a Hive partition dir.
        let root = std::env::temp_dir().join("kokedb-snapstore-test");
        let _ = tokio::fs::remove_dir_all(&root).await;
        let dir1 = root.join("uuid1");
        tokio::fs::create_dir_all(dir1.join("dt=2024-01-01")).await.unwrap();
        tokio::fs::write(dir1.join("a.parquet"), b"AAA").await.unwrap();
        tokio::fs::write(dir1.join("dt=2024-01-01/b.parquet"), b"BBB").await.unwrap();

        let store = Arc::new(InMemory::new());
        let snap = SnapshotStore::for_test(store.clone(), "mem://kokedb/snap", "kokedb/snap");

        let uri = snap
            .upload_dir(dir1.to_str().unwrap(), "cat/public/orders/uuid1")
            .await
            .unwrap();
        assert_eq!(uri, "mem://kokedb/snap/cat/public/orders/uuid1/");

        // Both files land under the namespaced prefix, partition layout intact.
        let keys: Vec<String> = {
            use futures::StreamExt;
            store
                .list(None)
                .map(|m| m.unwrap().location.as_ref().to_string())
                .collect()
                .await
        };
        assert!(keys.contains(&"kokedb/snap/cat/public/orders/uuid1/a.parquet".to_string()));
        assert!(keys
            .contains(&"kokedb/snap/cat/public/orders/uuid1/dt=2024-01-01/b.parquet".to_string()));

        // Add a second snapshot, then prune keeping none but the current (uuid2):
        // uuid1 must be deleted, uuid2 must survive.
        let dir2 = root.join("uuid2");
        tokio::fs::create_dir_all(&dir2).await.unwrap();
        tokio::fs::write(dir2.join("a.parquet"), b"CCC").await.unwrap();
        snap.upload_dir(dir2.to_str().unwrap(), "cat/public/orders/uuid2")
            .await
            .unwrap();

        snap.prune_snapshots("cat/public/orders", 0, "mem://kokedb/snap/cat/public/orders/uuid2/")
            .await
            .unwrap();

        let keys_after: Vec<String> = {
            use futures::StreamExt;
            store
                .list(None)
                .map(|m| m.unwrap().location.as_ref().to_string())
                .collect()
                .await
        };
        assert!(
            !keys_after.iter().any(|k| k.contains("/uuid1/")),
            "uuid1 should be pruned: {keys_after:?}"
        );
        assert!(
            keys_after.iter().any(|k| k.contains("/uuid2/")),
            "uuid2 (current) must survive: {keys_after:?}"
        );

        let _ = tokio::fs::remove_dir_all(&root).await;
    }
}
