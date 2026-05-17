use std::env;
use std::time::Duration;

use crate::error::{ObjectStoreClientError, Result};

/// Selects the cloud storage backend.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Provider {
    Azure,
    S3 { endpoint_url: Option<String> },
}

impl Provider {
    /// Parse a provider string (`"azure"` or `"s3"`).
    pub fn from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "azure" => Ok(Provider::Azure),
            "s3" => {
                let endpoint_url = env::var("S3_ENDPOINT_URL").ok();
                Ok(Provider::S3 { endpoint_url })
            }
            other => Err(ObjectStoreClientError::UnsupportedProvider(
                other.to_owned(),
            )),
        }
    }
}

/// Configuration for the [`ObjectStoreClient`](crate::ObjectStoreClient).
///
/// Build it with [`ObjectStoreConfig::from_env`] or [`ObjectStoreConfig::builder`].
#[derive(Debug, Clone)]
pub struct ObjectStoreConfig {
    /// Cloud provider / backend.
    pub provider: Provider,
    /// Bucket or container name.
    pub bucket: String,
    /// Account name (Azure) or access key ID (S3/compatible).
    pub account: String,
    /// Access key (Azure) or secret key (S3/compatible).
    pub secret: String,
    /// Default polling interval used by the watcher. Defaults to 30 s.
    pub default_poll_interval: Duration,
}

impl ObjectStoreConfig {
    /// Build configuration from environment variables.
    ///
    /// | Variable               | Required | Description                                |
    /// |------------------------|----------|--------------------------------------------|
    /// | `CLOUD_PROVIDER`       | Yes      | `"azure"` or `"s3"`                        |
    /// | `OBJECT_STORAGE_BUCKET`| Yes      | Bucket / container name                    |
    /// | `OBJECT_STORAGE_ACCOUNT`| Yes     | Account name / S3 access key ID            |
    /// | `OBJECT_STORAGE_SECRET`| Yes      | Access key / S3 secret key                 |
    /// | `S3_ENDPOINT_URL`      | No       | Custom S3 endpoint for compatible stores   |
    pub fn from_env() -> Result<Self> {
        let provider_str = required_env("CLOUD_PROVIDER")?;
        let provider = Provider::from_str(&provider_str)?;
        let bucket = required_env("OBJECT_STORAGE_BUCKET")?;
        let account = required_env("OBJECT_STORAGE_ACCOUNT")?;
        let secret = required_env("OBJECT_STORAGE_SECRET")?;

        Ok(Self {
            provider,
            bucket,
            account,
            secret,
            default_poll_interval: Duration::from_secs(30),
        })
    }

    /// Return a builder for programmatic configuration.
    pub fn builder() -> ObjectStoreConfigBuilder {
        ObjectStoreConfigBuilder::default()
    }

    /// Override the default polling interval.
    pub fn with_poll_interval(mut self, interval: Duration) -> Self {
        self.default_poll_interval = interval;
        self
    }
}

// --- Builder ---

/// Fluent builder for [`ObjectStoreConfig`].
#[derive(Debug, Default)]
pub struct ObjectStoreConfigBuilder {
    provider: Option<Provider>,
    bucket: Option<String>,
    account: Option<String>,
    secret: Option<String>,
    poll_interval: Option<Duration>,
}

impl ObjectStoreConfigBuilder {
    pub fn provider(mut self, provider: Provider) -> Self {
        self.provider = Some(provider);
        self
    }

    pub fn bucket(mut self, bucket: impl Into<String>) -> Self {
        self.bucket = Some(bucket.into());
        self
    }

    pub fn account(mut self, account: impl Into<String>) -> Self {
        self.account = Some(account.into());
        self
    }

    pub fn secret(mut self, secret: impl Into<String>) -> Self {
        self.secret = Some(secret.into());
        self
    }

    pub fn poll_interval(mut self, interval: Duration) -> Self {
        self.poll_interval = Some(interval);
        self
    }

    pub fn build(self) -> Result<ObjectStoreConfig> {
        Ok(ObjectStoreConfig {
            provider: self
                .provider
                .ok_or_else(|| ObjectStoreClientError::MissingEnvVar("provider".into()))?,
            bucket: self
                .bucket
                .ok_or_else(|| ObjectStoreClientError::MissingEnvVar("bucket".into()))?,
            account: self
                .account
                .ok_or_else(|| ObjectStoreClientError::MissingEnvVar("account".into()))?,
            secret: self
                .secret
                .ok_or_else(|| ObjectStoreClientError::MissingEnvVar("secret".into()))?,
            default_poll_interval: self.poll_interval.unwrap_or(Duration::from_secs(30)),
        })
    }
}

// --- Helpers ---

fn required_env(key: &str) -> Result<String> {
    env::var(key).map_err(|_| ObjectStoreClientError::MissingEnvVar(key.to_owned()))
}
