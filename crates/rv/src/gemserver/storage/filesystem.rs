use super::{Blob, Error, Result, Storage};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::path::{Component, Path, PathBuf};

/// Internal struct for JSON serialization of blob metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
struct MetadataFile {
    bytesize: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    etag: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    sha256: Option<String>,
}

/// Filesystem-based storage implementation
///
/// Stores blobs as files on the filesystem with metadata stored in separate files.
/// For each blob, creates:
/// - Main file: Contains the content
/// - `.etag` file: Contains the ETag if present
///
/// # Example
///
/// ```rust
/// use crate::gemserver::storage::{FilesystemStorage, Storage, Blob};
/// use std::path::PathBuf;
/// use tempfile::TempDir;
///
/// #[tokio::main]
/// async fn main() {
///     let temp_dir = TempDir::new().unwrap();
///     let storage = FilesystemStorage::new(temp_dir.path().to_path_buf());
///
///     let blob = Blob::new(b"content".to_vec())
///         .with_etag("etag123".to_string());
///     storage.write_blob("test", &blob).await.unwrap();
///
///     let read_blob = storage.read_blob("test").await.unwrap();
///     assert_eq!(read_blob.content, b"content");
///     assert_eq!(read_blob.etag(), Some("etag123"));
/// }
/// ```
pub struct FilesystemStorage {
    base_path: PathBuf,
}

impl FilesystemStorage {
    pub fn new(base_path: PathBuf) -> Self {
        Self { base_path }
    }

    fn resolve_path(&self, key: &str) -> Result<PathBuf> {
        validate_key(key)?;
        Ok(self.base_path.join(key))
    }

    fn metadata_path(&self, key: &str) -> Result<PathBuf> {
        validate_key(key)?;
        Ok(self
            .base_path
            .join("metadata")
            .join(format!("{}.json", key)))
    }
}

/// Keys are joined onto the storage root, so every segment must be an ordinary
/// name. `Path::starts_with` compares lexically and would accept `base/info/../..`,
/// hence the component-wise check.
fn validate_key(key: &str) -> Result<()> {
    let valid = !key.is_empty()
        && !key.contains('\\')
        && !key.contains('\0')
        && Path::new(key)
            .components()
            .all(|component| matches!(component, Component::Normal(_)));
    if valid {
        Ok(())
    } else {
        Err(Error::InvalidKey(key.to_owned()))
    }
}

impl FilesystemStorage {
    // Private helper methods for implementation
    async fn read(&self, key: &str) -> Result<Vec<u8>> {
        Ok(tokio::fs::read(self.resolve_path(key)?).await?)
    }

    async fn write(&self, key: &str, content: &[u8]) -> Result<()> {
        let path = self.resolve_path(key)?;

        // Create parent directory if needed
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        Ok(tokio::fs::write(&path, content).await?)
    }

    async fn write_metadata_file(&self, key: &str, metadata: &MetadataFile) -> Result<()> {
        let path = self.metadata_path(key)?;

        // Create metadata directory if needed
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        let json = serde_json::to_string_pretty(metadata)?;

        Ok(tokio::fs::write(&path, json).await?)
    }

    async fn read_metadata_file(&self, key: &str) -> Option<MetadataFile> {
        let path = self.metadata_path(key).ok()?;

        // Try reading metadata JSON directly - no need to check existence first
        // This eliminates one async fs::metadata() call per read
        if let Ok(json) = tokio::fs::read_to_string(&path).await
            && let Ok(metadata) = serde_json::from_str(&json)
        {
            return Some(metadata);
        }

        None
    }
}

#[async_trait]
impl Storage for FilesystemStorage {
    async fn exists(&self, key: &str) -> bool {
        match self.resolve_path(key) {
            Ok(path) => tokio::fs::metadata(path).await.is_ok(),
            Err(_) => false,
        }
    }

    async fn read_blob(&self, key: &str) -> Result<Blob> {
        let content = self.read(key).await?;
        let metadata_file = self.read_metadata_file(key).await;

        Ok(Blob {
            content,
            etag: metadata_file.as_ref().and_then(|m| m.etag.clone()),
            sha256: metadata_file.as_ref().and_then(|m| m.sha256.clone()),
        })
    }

    async fn write_blob(&self, key: &str, blob: &Blob) -> Result<()> {
        self.write(key, &blob.content).await?;

        let metadata = MetadataFile {
            bytesize: blob.size(),
            etag: blob.etag.clone(),
            sha256: blob.sha256.clone(),
        };

        self.write_metadata_file(key, &metadata).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_filesystem_storage_exists() {
        let temp_dir = TempDir::new().unwrap();
        let storage = FilesystemStorage::new(temp_dir.path().to_path_buf());

        // File doesn't exist yet
        assert!(!storage.exists("testfile").await);

        // Create file
        fs::write(temp_dir.path().join("testfile"), b"content").unwrap();

        // Now it exists
        assert!(storage.exists("testfile").await);
    }

    #[tokio::test]
    async fn test_filesystem_storage_read_blob() {
        let temp_dir = TempDir::new().unwrap();
        let storage = FilesystemStorage::new(temp_dir.path().to_path_buf());

        let content = b"test content";
        fs::write(temp_dir.path().join("testfile"), content).unwrap();

        let blob = storage.read_blob("testfile").await.unwrap();
        assert_eq!(blob.content, content);
        assert_eq!(blob.etag(), None);
    }

    #[tokio::test]
    async fn test_filesystem_storage_read_blob_nonexistent() {
        let temp_dir = TempDir::new().unwrap();
        let storage = FilesystemStorage::new(temp_dir.path().to_path_buf());

        let result = storage.read_blob("nonexistent");
        assert!(result.await.is_err());
    }

    #[tokio::test]
    async fn test_filesystem_storage_write_blob() {
        let temp_dir = TempDir::new().unwrap();
        let storage = FilesystemStorage::new(temp_dir.path().to_path_buf());

        let blob = Blob::new(b"test content".to_vec());
        storage.write_blob("testfile", &blob).await.unwrap();

        let read_content = fs::read(temp_dir.path().join("testfile")).unwrap();
        assert_eq!(read_content, b"test content");
    }

    #[tokio::test]
    async fn test_filesystem_storage_blob_operations() {
        let temp_dir = TempDir::new().unwrap();
        let storage = FilesystemStorage::new(temp_dir.path().to_path_buf());

        let blob = Blob::new(b"test content".to_vec()).with_etag("test-etag".to_string());
        storage.write_blob("testfile", &blob).await.unwrap();

        let read_blob = storage.read_blob("testfile").await.unwrap();
        assert_eq!(read_blob.content, blob.content);
        assert_eq!(read_blob.etag(), Some("test-etag"));
    }

    #[tokio::test]
    async fn test_filesystem_storage_blob_without_etag() {
        let temp_dir = TempDir::new().unwrap();
        let storage = FilesystemStorage::new(temp_dir.path().to_path_buf());

        // Write blob without etag
        let blob = Blob::new(b"content".to_vec());
        storage.write_blob("testfile", &blob).await.unwrap();

        // Read it back - should have no etag
        let read_blob = storage.read_blob("testfile").await.unwrap();
        assert_eq!(read_blob.content, b"content");
        assert_eq!(read_blob.etag, None);
    }

    #[tokio::test]
    async fn test_filesystem_storage_rejects_traversal_keys() {
        let temp_dir = TempDir::new().unwrap();
        let base = temp_dir.path().join("cache");
        fs::create_dir_all(&base).unwrap();
        let storage = FilesystemStorage::new(base);
        let marker = temp_dir.path().join("marker");
        fs::write(&marker, b"original").unwrap();

        let blob = Blob::new(b"PWNED".to_vec());
        for key in [
            "info/../../marker",
            "../marker",
            "/tmp/marker",
            "info\\..\\..\\marker",
        ] {
            assert!(matches!(
                storage.write_blob(key, &blob).await,
                Err(Error::InvalidKey(_))
            ));
            assert!(!storage.exists(key).await);
            assert!(matches!(
                storage.read_blob(key).await,
                Err(Error::InvalidKey(_))
            ));
        }
        assert_eq!(fs::read(&marker).unwrap(), b"original");
    }
}
