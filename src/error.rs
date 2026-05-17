use thiserror::Error;

#[derive(Debug, Error)]
pub enum ObjectStoreClientError {
    #[error("Missing environment variable: {0}")]
    MissingEnvVar(String),

    #[error("Unsupported provider '{0}'. Use 'azure' or 's3'.")]
    UnsupportedProvider(String),

    #[error("Failed to build store: {0}")]
    StoreBuildError(String),

    #[error("Object not found at path: {0}")]
    NotFound(String),

    #[error("Object store operation failed: {0}")]
    StoreError(#[from] object_store::Error),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("{0}")]
    Other(#[from] anyhow::Error),
}

pub type Result<T> = std::result::Result<T, ObjectStoreClientError>;
