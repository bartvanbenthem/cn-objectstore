use chrono::{DateTime, Utc};
use object_store::ObjectMeta as StoreObjectMeta;

/// Metadata for a single object in the store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectMeta {
    /// Full object path (key).
    pub path: String,
    /// Timestamp of the last modification.
    pub last_modified: DateTime<Utc>,
    /// Object size in bytes.
    pub size: usize,
    /// ETag (if provided by the backend).
    pub e_tag: Option<String>,
}

impl From<StoreObjectMeta> for ObjectMeta {
    fn from(m: StoreObjectMeta) -> Self {
        Self {
            path: m.location.to_string(),
            last_modified: m.last_modified,
            size: m.size,
            e_tag: m.e_tag,
        }
    }
}
