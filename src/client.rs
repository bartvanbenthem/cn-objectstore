use std::sync::Arc;

use bytes::Bytes;
use futures::TryStreamExt;
use object_store::aws::AmazonS3Builder;
use object_store::azure::MicrosoftAzureBuilder;
use object_store::path::Path;
use object_store::{ObjectStore, PutPayload};
use tracing::{info, instrument};

use crate::config::{ObjectStoreConfig, Provider};
use crate::error::{ObjectStoreClientError, Result};
use crate::meta::ObjectMeta;

/// High-level client wrapping an [`ObjectStore`] backend.
///
/// Construct via [`ObjectStoreClient::new`] or [`ObjectStoreClient::from_store`] (useful in
/// tests with an in-memory store).
///
/// All methods are `async` and cheap to clone; the inner store is reference-counted.
#[derive(Clone)]
pub struct ObjectStoreClient {
    store: Arc<dyn ObjectStore>,
    bucket: String,
}

impl std::fmt::Debug for ObjectStoreClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObjectStoreClient")
            .field("bucket", &self.bucket)
            .finish()
    }
}

impl ObjectStoreClient {
    // --- Constructors ---

    /// Create a client from the given [`ObjectStoreConfig`].
    pub async fn new(config: ObjectStoreConfig) -> Result<Self> {
        let store: Arc<dyn ObjectStore> = match &config.provider {
            Provider::Azure => build_azure_store(&config)?,
            Provider::S3 { endpoint_url } => {
                build_s3_store(&config, endpoint_url.as_deref())?
            }
        };

        Ok(Self {
            store,
            bucket: config.bucket,
        })
    }

    /// Create a client from any object store implementation (useful for testing).
    pub fn from_store(store: Arc<dyn ObjectStore>, bucket: impl Into<String>) -> Self {
        Self {
            store,
            bucket: bucket.into(),
        }
    }

    /// Expose the inner store (e.g. for advanced operations).
    pub fn inner(&self) -> Arc<dyn ObjectStore> {
        self.store.clone()
    }

    // --- Core CRUD ---

    /// Write `data` to `path`. Overwrites any existing object at that path.
    #[instrument(skip(self, data), fields(path, bytes = data.len()))]
    pub async fn write(&self, path: &str, data: &[u8]) -> Result<ObjectMeta> {
        let key = Path::from(path);
        let payload = PutPayload::from(Bytes::from(data.to_vec()));

        info!(bucket = %self.bucket, %path, bytes = data.len(), "Writing object");

        let put_result = self.store.put(&key, payload).await?;

        // HEAD to return consistent metadata.
        let meta = self.store.head(&key).await?;
        info!(e_tag = ?put_result.e_tag, "Write complete");

        Ok(meta.into())
    }

    /// Read the full contents of `path`. Returns `None` if the object does not exist.
    #[instrument(skip(self), fields(path))]
    pub async fn read(&self, path: &str) -> Result<Option<Bytes>> {
        let key = Path::from(path);

        match self.store.get(&key).await {
            Ok(result) => {
                let bytes = result.bytes().await?;
                info!(%path, bytes = bytes.len(), "Read complete");
                Ok(Some(bytes))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Read the full contents of `path`, returning an error if it does not exist.
    pub async fn read_required(&self, path: &str) -> Result<Bytes> {
        self.read(path).await?.ok_or_else(|| {
            ObjectStoreClientError::NotFound(path.to_owned())
        })
    }

    /// Delete the object at `path`. Returns `Ok(false)` if it did not exist.
    #[instrument(skip(self), fields(path))]
    pub async fn delete(&self, path: &str) -> Result<bool> {
        let key = Path::from(path);

        match self.store.delete(&key).await {
            Ok(()) => {
                info!(%path, "Deleted object");
                Ok(true)
            }
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    /// Retrieve metadata for a single object. Returns `None` if not found.
    #[instrument(skip(self), fields(path))]
    pub async fn metadata(&self, path: &str) -> Result<Option<ObjectMeta>> {
        let key = Path::from(path);

        match self.store.head(&key).await {
            Ok(meta) => Ok(Some(meta.into())),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Check whether an object exists without fetching its content.
    pub async fn exists(&self, path: &str) -> Result<bool> {
        Ok(self.metadata(path).await?.is_some())
    }

    // --- Listing ---

    /// List all objects under `prefix`. Returns an empty vec if none exist.
    #[instrument(skip(self), fields(prefix))]
    pub async fn list(&self, prefix: &str) -> Result<Vec<ObjectMeta>> {
        let path = Path::from(prefix);
        let objects: Vec<_> = self
            .store
            .list(Some(&path))
            .try_collect()
            .await?;

        let metas = objects.into_iter().map(ObjectMeta::from).collect();
        Ok(metas)
    }

    /// Return metadata for the most recently modified object under `prefix`,
    /// or `None` if the prefix is empty.
    pub async fn latest(&self, prefix: &str) -> Result<Option<ObjectMeta>> {
        let objects = self.list(prefix).await?;
        Ok(objects
            .into_iter()
            .max_by_key(|m| m.last_modified))
    }

    /// Download the content of the most recently modified object under `prefix`.
    /// Returns `None` if the prefix is empty.
    pub async fn read_latest(&self, prefix: &str) -> Result<Option<Bytes>> {
        match self.latest(prefix).await? {
            Some(meta) => self.read(&meta.path).await,
            None => Ok(None),
        }
    }

    // --- Batch / Convenience ---

    /// Copy `source` to `destination` within the same store.
    pub async fn copy(&self, source: &str, destination: &str) -> Result<ObjectMeta> {
        let data = self.read_required(source).await?;
        self.write(destination, &data).await
    }

    /// Move `source` to `destination` (copy then delete source).
    pub async fn rename(&self, source: &str, destination: &str) -> Result<ObjectMeta> {
        let meta = self.copy(source, destination).await?;
        self.delete(source).await?;
        Ok(meta)
    }

    /// Delete all objects whose path starts with `prefix`.
    /// Returns the number of objects deleted.
    pub async fn delete_prefix(&self, prefix: &str) -> Result<usize> {
        let objects = self.list(prefix).await?;
        let count = objects.len();

        for meta in objects {
            self.delete(&meta.path).await?;
        }

        Ok(count)
    }

    /// Write `data` only if no object currently exists at `path`.
    /// Returns `Ok(true)` if written, `Ok(false)` if it was already present.
    pub async fn write_if_absent(&self, path: &str, data: &[u8]) -> Result<bool> {
        if self.exists(path).await? {
            return Ok(false);
        }
        self.write(path, data).await?;
        Ok(true)
    }
}

// --- Store builders (private) ---

fn build_azure_store(config: &ObjectStoreConfig) -> Result<Arc<dyn ObjectStore>> {
    info!("Building Azure Blob Storage client (container: {})", config.bucket);

    let store = MicrosoftAzureBuilder::new()
        .with_account(&config.account)
        .with_access_key(&config.secret)
        .with_container_name(&config.bucket)
        .build()
        .map_err(|e| ObjectStoreClientError::StoreBuildError(e.to_string()))?;

    Ok(Arc::new(store))
}

fn build_s3_store(
    config: &ObjectStoreConfig,
    endpoint_url: Option<&str>,
) -> Result<Arc<dyn ObjectStore>> {
    info!("Building S3-compatible client (bucket: {})", config.bucket);

    let mut builder = AmazonS3Builder::new()
        .with_access_key_id(&config.account)
        .with_secret_access_key(&config.secret)
        .with_bucket_name(&config.bucket);

    if let Some(endpoint) = endpoint_url {
        info!("Using custom S3 endpoint: {}", endpoint);
        builder = builder.with_endpoint(endpoint).with_allow_http(true);
    } else {
        info!("Using AWS S3 (region sourced from environment or defaults)");
    }

    let store = builder
        .build()
        .map_err(|e| ObjectStoreClientError::StoreBuildError(e.to_string()))?;

    Ok(Arc::new(store))
}
