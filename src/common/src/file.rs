use std::{
    io,
    path::{Path, PathBuf},
};

use chrono::{DateTime, Local};
use tokio::fs;

pub async fn ensure_dir_exists<P: AsRef<Path>>(path: P) -> io::Result<()> {
    let path = path.as_ref();

    match fs::metadata(path).await {
        Ok(metadata) => {
            if metadata.is_dir() {
                Ok(())
            } else {
                Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    format!("Path '{}' exists but is not a directory", path.display()),
                ))
            }
        }
        Err(e) if matches!(e.kind(), io::ErrorKind::NotFound) => fs::create_dir_all(path).await,
        Err(e) => Err(e),
    }
}

pub fn get_remote_catalog_local_path(catalog: &str, schema: &str, table: &str) -> String {
    format!("/tmp/remote_catalog/{}/{}/{}", catalog, schema, table)
}

pub fn get_remote_catalog_parent_local_path() -> String {
    format!("/tmp/remote_catalog")
}

pub async fn cleanup_old_directories(
    dir_path: &str,
    mut keep_count: usize,
    except_path: Option<&str>,
) -> io::Result<()> {
    log::debug!("Starting cleanup for directory: {}", dir_path);

    // Read all entries in the directory
    let mut entries = fs::read_dir(dir_path).await?;

    // Collect all subdirectories with their metadata
    let mut directories: Vec<(PathBuf, DateTime<Local>)> = Vec::new();

    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();

        if let Some(except) = except_path {
            let except_path_buf = PathBuf::from(except);
            if path == except_path_buf {
                keep_count = keep_count.saturating_sub(1);
                log::debug!(
                    "Skipping excluded directory: {}/{:?}",
                    dir_path,
                    path.file_name().unwrap_or_default()
                );
                continue;
            }
        }

        let metadata = fs::metadata(&path).await?;
        if metadata.is_dir() {
            // Get creation time (or modified time as fallback)
            let created = metadata.created().or_else(|_| metadata.modified())?;

            let created_datetime: DateTime<Local> = created.into();
            directories.push((path.clone(), created_datetime));

            log::debug!(
                "Found directory: {:?}, created at: {}",
                path.file_name().unwrap_or_default(),
                created_datetime.format("%Y-%m-%d %H:%M:%S")
            );
        }
    }

    log::debug!(
        "Found path: {} {} directories in total",
        dir_path,
        directories.len()
    );

    // Sort directories by creation time (newest first)
    directories.sort_by(|a, b| b.1.cmp(&a.1));

    // Keep the newest N directories and delete the rest
    if directories.len() > keep_count {
        let to_delete = &directories[keep_count..];

        log::info!(
            "Keeping {} newest directories, deleting {} old directories in path: {}",
            keep_count,
            to_delete.len(),
            dir_path,
        );

        for (path, created_time) in to_delete {
            match fs::remove_dir_all(&path).await {
                Ok(_) => {
                    log::info!(
                        "Successfully deleted directory: {}/{:?} (created at: {})",
                        dir_path,
                        path.file_name().unwrap_or_default(),
                        created_time.format("%Y-%m-%d %H:%M:%S")
                    );
                }
                Err(e) => {
                    log::error!("Failed to delete directory: {:?}, error: {}", path, e);
                }
            }
        }
    } else {
        log::info!(
            "Only {} directories found in path: {}, no cleanup needed (keeping {})",
            directories.len(),
            dir_path,
            keep_count
        );
    }

    log::debug!("Cleanup completed for directory: {}", dir_path);
    Ok(())
}

/// RAII guard for a working directory: removes it on drop unless `disarm`ed.
/// Use for intermediate sync/validation dirs (always cleaned up, even on early
/// return or panic), and for a snapshot dir that must be deleted on failure but
/// kept on success (`disarm` once the snapshot is committed).
pub struct TempPath {
    path: PathBuf,
    armed: bool,
}

impl TempPath {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            armed: true,
        }
    }

    /// Keep the directory (it has been committed / is no longer temporary).
    pub fn disarm(&mut self) {
        self.armed = false;
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempPath {
    fn drop(&mut self) {
        if self.armed {
            // Best-effort, synchronous (Drop can't await). Removing a local temp
            // dir is cheap; failures are non-fatal.
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }
}
