//! # kube-objstore
//!
//! A reusable, provider-agnostic object store client library for Kubernetes operators.
//!
//! Supports Azure Blob Storage and S3-compatible stores (AWS S3, MinIO, Cloudian, etc.)
//! via environment-variable-driven configuration.
//!
//! ## Quick Start
//!
//! ```no_run
//! use kube_objstore::{ObjectStoreClient, ObjectStoreConfig};
//! use std::time::Duration;
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     let config = ObjectStoreConfig::from_env()?;
//!     let client = ObjectStoreClient::new(config).await?;
//!
//!     // Write some data
//!     client.write("my-prefix/my-file.bin", b"hello world").await?;
//!
//!     // Read it back
//!     if let Some(bytes) = client.read("my-prefix/my-file.bin").await? {
//!         println!("Read {} bytes", bytes.len());
//!     }
//!
//!     Ok(())
//! }
//! ```
//!
//! ## Environment Variables
//!
//! | Variable               | Required | Description                                 |
//! |------------------------|----------|---------------------------------------------|
//! | `CLOUD_PROVIDER`       | Yes      | `"azure"` or `"s3"`                         |
//! | `OBJECT_STORAGE_BUCKET`| Yes      | Bucket/container name                        |
//! | `OBJECT_STORAGE_ACCOUNT`| Yes     | Account name (Azure) or access key ID (S3)  |
//! | `OBJECT_STORAGE_SECRET`| Yes      | Access key (Azure) or secret key (S3)       |
//! | `S3_ENDPOINT_URL`      | No       | Custom S3 endpoint (MinIO, Cloudian, etc.)  |

pub mod client;
pub mod config;
pub mod error;
pub mod meta;
pub mod watcher;

pub use client::ObjectStoreClient;
pub use config::ObjectStoreConfig;
pub use error::ObjectStoreClientError;
pub use meta::ObjectMeta;
pub use watcher::ObjectStoreWatcher;

#[cfg(test)]
#[path = "tests/mod.rs"]
mod tests;