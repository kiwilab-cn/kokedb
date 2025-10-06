use std::{fs, io, path::Path};

pub fn ensure_dir_exists<P: AsRef<Path>>(path: P) -> io::Result<()> {
    let path = path.as_ref();

    match fs::metadata(path) {
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
        Err(e) if matches!(e.kind(), io::ErrorKind::NotFound) => fs::create_dir_all(path),
        Err(e) => Err(e),
    }
}

pub fn get_remote_catalog_local_path(catalog: &str, schema: &str, table: &str) -> String {
    format!("/tmp/remote_catalog/{}/{}/{}", catalog, schema, table)
}

pub fn get_remote_catalog_parent_local_path() -> String {
    format!("/tmp/remote_catalog")
}
