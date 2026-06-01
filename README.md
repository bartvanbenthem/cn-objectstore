# Cloud Native ObjectStore

A reusable, provider agnostic object store client library for Cloud Native applications and Kubernetes operators.

Wraps [`object_store`](https://docs.rs/object_store) with a simpler, higher-level API covering
CRUD, prefix listing, latest-file retrieval, and background change detection — all driven by
environment variables so the same operator binary works against Azure Blob Storage, AWS S3, MinIO,
Cloudian, or any S3-compatible vendor with no code changes.

---

## Features

- **Azure Blob Storage** and **S3-compatible** backends (AWS S3, MinIO, Cloudian, …)
- Environment-variable-driven config — no credentials in code
- Typed error enum — pattern-match on `NotFound`, `StoreBuildError`, etc.
- Convenience methods: `exists`, `copy`, `rename`, `delete_prefix`, `write_if_absent`, `read_latest`
- Background prefix watcher with configurable poll interval
- `Clone`-able client backed by `Arc<dyn ObjectStore>` — share freely across tasks
- Fully testable via `ObjectStoreClient::from_store` with `object_store::memory::InMemory`

---

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
kube-objstore = { path = "../kube-objstore" }   # local
# or once published:
# kube-objstore = "0.1.0"
```

---

## Environment variables

| Variable                | Required | Description                                              |
|-------------------------|----------|----------------------------------------------------------|
| `CLOUD_PROVIDER`        | Yes      | `azure` or `s3`                                         |
| `OBJECT_STORAGE_BUCKET` | Yes      | Bucket (S3) or container name (Azure)                   |
| `OBJECT_STORAGE_ACCOUNT`| Yes      | Storage account name (Azure) or access key ID (S3)      |
| `OBJECT_STORAGE_SECRET` | Yes      | Storage access key (Azure) or secret access key (S3)    |
| `S3_ENDPOINT_URL`       | No       | Custom S3 endpoint for MinIO / Cloudian / other vendors |

For AWS S3 the region is sourced from the standard `AWS_DEFAULT_REGION` environment variable or
the AWS SDK default chain.

---

## Quick start

```rust
use kube_objstore::{ObjectStoreClient, ObjectStoreConfig};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = ObjectStoreConfig::from_env()?;
    let client = ObjectStoreClient::new(config).await?;

    // Write
    client.write("snapshots/v1.bin", b"my data").await?;

    // Read
    if let Some(bytes) = client.read("snapshots/v1.bin").await? {
        println!("Read {} bytes", bytes.len());
    }

    // Read the most recently modified object under a prefix
    if let Some(bytes) = client.read_latest("snapshots/").await? {
        println!("Latest snapshot: {} bytes", bytes.len());
    }

    Ok(())
}
```

---

## API reference

### `ObjectStoreClient`

| Method | Description |
|---|---|
| `new(config)` | Build a client from `ObjectStoreConfig` |
| `from_store(store, bucket)` | Wrap any `ObjectStore` (useful in tests) |
| `inner()` | Access the underlying `Arc<dyn ObjectStore>` |
| **CRUD** | |
| `write(path, data)` | Write bytes, overwriting any existing object. Returns `ObjectMeta`. |
| `read(path)` | Read bytes. Returns `None` if not found. |
| `read_required(path)` | Read bytes. Returns `Err(NotFound)` if not found. |
| `delete(path)` | Delete an object. Returns `false` if it did not exist. |
| `metadata(path)` | HEAD request — returns `ObjectMeta` or `None`. |
| `exists(path)` | `true` if the object exists, no content downloaded. |
| **Listing** | |
| `list(prefix)` | All objects under a prefix. |
| `latest(prefix)` | Metadata of the most recently modified object under a prefix. |
| `read_latest(prefix)` | Content of the most recently modified object under a prefix. |
| **Batch / convenience** | |
| `copy(src, dst)` | Copy an object within the same store. |
| `rename(src, dst)` | Copy then delete source. |
| `delete_prefix(prefix)` | Delete all objects under a prefix. Returns count deleted. |
| `write_if_absent(path, data)` | Write only if the object does not already exist. |

### `ObjectStoreWatcher`

Polls a prefix on a configurable interval and sends a `()` signal on a Tokio `mpsc` channel
whenever objects are added or removed. The first poll establishes a baseline without firing.
The background task shuts down automatically when all receivers are dropped.

```rust
use kube_objstore::{ObjectStoreClient, ObjectStoreConfig, ObjectStoreWatcher};
use tokio::sync::mpsc;
use std::time::Duration;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = ObjectStoreConfig::from_env()?;
    let client = ObjectStoreClient::new(config).await?;

    let (tx, mut rx) = mpsc::channel(8);

    let _handle = ObjectStoreWatcher::new(client, "configs/")
        .with_interval(Duration::from_secs(15))
        .spawn(tx);

    while let Some(()) = rx.recv().await {
        println!("Prefix changed — trigger reconcile");
    }

    Ok(())
}
```

### `ObjectStoreConfig`

Build from environment variables:

```rust
let config = ObjectStoreConfig::from_env()?;
```

Or programmatically with the builder:

```rust
use kube_objstore::config::{ObjectStoreConfigBuilder, Provider};
use std::time::Duration;

let config = ObjectStoreConfigBuilder::default()
    .provider(Provider::S3 { endpoint_url: Some("http://minio:9000".into()) })
    .bucket("my-bucket")
    .account("minioadmin")
    .secret("minioadmin")
    .poll_interval(Duration::from_secs(60))
    .build()?;
```

---

## Error handling

All methods return `Result<T, ObjectStoreClientError>`. The error enum covers:

```rust
pub enum ObjectStoreClientError {
    MissingEnvVar(String),
    UnsupportedProvider(String),
    StoreBuildError(String),
    NotFound(String),
    StoreError(object_store::Error),
    Io(std::io::Error),
    Other(anyhow::Error),
}
```

---

## Testing

Tests use `object_store::memory::InMemory` — no cloud credentials or network needed:

```bash
cargo test
```

Enable log output:

```bash
RUST_LOG=debug cargo test -- --nocapture
```

To write your own tests against the library:

```rust
use std::sync::Arc;
use object_store::memory::InMemory;
use kube_objstore::ObjectStoreClient;

fn test_client() -> ObjectStoreClient {
    ObjectStoreClient::from_store(Arc::new(InMemory::new()), "test-bucket")
}
```

---

## Provider examples

### Azure Blob Storage

```bash
export CLOUD_PROVIDER=azure
export OBJECT_STORAGE_BUCKET=my-container
export OBJECT_STORAGE_ACCOUNT=mystorageaccount
export OBJECT_STORAGE_SECRET=base64accesskey==
```

### AWS S3

```bash
export CLOUD_PROVIDER=s3
export OBJECT_STORAGE_BUCKET=my-bucket
export OBJECT_STORAGE_ACCOUNT=AKIAIOSFODNN7EXAMPLE
export OBJECT_STORAGE_SECRET=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
export AWS_DEFAULT_REGION=eu-west-1
```

### S3-compatible

```bash
export CLOUD_PROVIDER=s3
export OBJECT_STORAGE_BUCKET=my-bucket
export OBJECT_STORAGE_ACCOUNT=minioadmin
export OBJECT_STORAGE_SECRET=minioadmin
export S3_ENDPOINT_URL=http://minio.minio-ns.svc.cluster.local:9000
```

---

## Logging

The library uses [`tracing`](https://docs.rs/tracing). Add a subscriber in your operator's
`main.rs` to see structured logs from the library:

```rust
tracing_subscriber::fmt()
    .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
    .init();
```

Then set `RUST_LOG=kube_objstore=debug` for verbose output.

---

## License

MIT
