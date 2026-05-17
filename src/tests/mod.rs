/// Tests use `object_store::memory::InMemory` as the backend so no cloud
/// credentials or network access are required.
///
/// Run with:
///   cargo test
///
/// Enable log output:
///   RUST_LOG=debug cargo test -- --nocapture
#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use bytes::Bytes;
    use object_store::memory::InMemory;
    use tokio::sync::mpsc;

    use crate::client::ObjectStoreClient;
    use crate::error::ObjectStoreClientError;
    use crate::watcher::ObjectStoreWatcher;

    // --- Helpers ---

    fn in_memory_client() -> ObjectStoreClient {
        ObjectStoreClient::from_store(Arc::new(InMemory::new()), "test-bucket")
    }

    fn bytes(s: &str) -> Bytes {
        Bytes::from(s.to_owned())
    }

    // =========================================================================
    // Write / Read
    // =========================================================================

    #[tokio::test]
    async fn write_then_read_round_trip() {
        let client = in_memory_client();

        client.write("prefix/hello.txt", b"hello world").await.unwrap();

        let content = client.read("prefix/hello.txt").await.unwrap().unwrap();
        assert_eq!(content, bytes("hello world"));
    }

    #[tokio::test]
    async fn read_missing_returns_none() {
        let client = in_memory_client();
        let result = client.read("no-such-file.bin").await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn read_required_errors_on_missing() {
        let client = in_memory_client();
        let err = client.read_required("ghost.txt").await.unwrap_err();
        assert!(matches!(err, ObjectStoreClientError::NotFound(_)));
    }

    #[tokio::test]
    async fn overwrite_replaces_content() {
        let client = in_memory_client();

        client.write("file.txt", b"v1").await.unwrap();
        client.write("file.txt", b"v2").await.unwrap();

        let content = client.read("file.txt").await.unwrap().unwrap();
        assert_eq!(content, bytes("v2"));
    }

    #[tokio::test]
    async fn write_returns_correct_metadata() {
        let client = in_memory_client();
        let data = b"metadata check";

        let meta = client.write("meta/test.bin", data).await.unwrap();
        assert_eq!(meta.path, "meta/test.bin");
        assert_eq!(meta.size, data.len());
    }

    // =========================================================================
    // Exists / Metadata
    // =========================================================================

    #[tokio::test]
    async fn exists_returns_false_for_missing() {
        let client = in_memory_client();
        assert!(!client.exists("missing.txt").await.unwrap());
    }

    #[tokio::test]
    async fn exists_returns_true_after_write() {
        let client = in_memory_client();
        client.write("exists.txt", b"data").await.unwrap();
        assert!(client.exists("exists.txt").await.unwrap());
    }

    #[tokio::test]
    async fn metadata_returns_none_for_missing() {
        let client = in_memory_client();
        assert!(client.metadata("nope.bin").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn metadata_size_matches_written_data() {
        let client = in_memory_client();
        let data = b"1234567890";
        client.write("sized.bin", data).await.unwrap();

        let meta = client.metadata("sized.bin").await.unwrap().unwrap();
        assert_eq!(meta.size, data.len());
    }

    // =========================================================================
    // Delete
    // =========================================================================

    #[tokio::test]
    async fn delete_removes_object() {
        let client = in_memory_client();
        client.write("del.txt", b"bye").await.unwrap();

        let deleted = client.delete("del.txt").await.unwrap();
        assert!(deleted);
        assert!(!client.exists("del.txt").await.unwrap());
    }

    #[tokio::test]
    async fn delete_missing_is_idempotent() {
        // InMemory returns Ok(()) for missing keys so we cannot rely on the
        // bool return; just verify the object is absent after the call.
        let client = in_memory_client();
        client.delete("phantom.txt").await.unwrap();
        assert!(!client.exists("phantom.txt").await.unwrap());
    }

    // =========================================================================
    // List
    // =========================================================================

    #[tokio::test]
    async fn list_returns_empty_for_unknown_prefix() {
        let client = in_memory_client();
        let objects = client.list("nonexistent/prefix/").await.unwrap();
        assert!(objects.is_empty());
    }

    #[tokio::test]
    async fn list_returns_all_objects_under_prefix() {
        let client = in_memory_client();

        client.write("logs/a.log", b"a").await.unwrap();
        client.write("logs/b.log", b"b").await.unwrap();
        client.write("logs/c.log", b"c").await.unwrap();
        client.write("other/x.dat", b"x").await.unwrap(); // different prefix

        let objects = client.list("logs/").await.unwrap();
        let paths: Vec<_> = objects.iter().map(|m| m.path.as_str()).collect();

        assert_eq!(objects.len(), 3);
        assert!(paths.contains(&"logs/a.log"));
        assert!(paths.contains(&"logs/b.log"));
        assert!(paths.contains(&"logs/c.log"));
    }

    #[tokio::test]
    async fn list_does_not_cross_prefix_boundary() {
        let client = in_memory_client();
        client.write("alpha/1.txt", b"1").await.unwrap();
        client.write("beta/2.txt", b"2").await.unwrap();

        let alpha = client.list("alpha/").await.unwrap();
        assert_eq!(alpha.len(), 1);
        assert_eq!(alpha[0].path, "alpha/1.txt");
    }

    // =========================================================================
    // Latest
    // =========================================================================

    #[tokio::test]
    async fn latest_returns_none_for_empty_prefix() {
        let client = in_memory_client();
        let latest = client.latest("empty/").await.unwrap();
        assert!(latest.is_none());
    }

    #[tokio::test]
    async fn latest_returns_most_recently_modified() {
        let client = in_memory_client();

        // Write two objects; the second write will be more recent.
        client.write("snap/v1.snap", b"snapshot 1").await.unwrap();
        tokio::time::sleep(Duration::from_millis(5)).await;
        client.write("snap/v2.snap", b"snapshot 2").await.unwrap();

        let latest = client.latest("snap/").await.unwrap().unwrap();
        assert_eq!(latest.path, "snap/v2.snap");
    }

    #[tokio::test]
    async fn read_latest_returns_content_of_newest_object() {
        let client = in_memory_client();

        client.write("data/old.bin", b"old data").await.unwrap();
        tokio::time::sleep(Duration::from_millis(5)).await;
        client.write("data/new.bin", b"new data").await.unwrap();

        let content = client.read_latest("data/").await.unwrap().unwrap();
        assert_eq!(content, bytes("new data"));
    }

    // =========================================================================
    // Copy / Rename
    // =========================================================================

    #[tokio::test]
    async fn copy_duplicates_content() {
        let client = in_memory_client();
        client.write("src/file.txt", b"copy me").await.unwrap();

        client.copy("src/file.txt", "dst/file.txt").await.unwrap();

        let src = client.read("src/file.txt").await.unwrap().unwrap();
        let dst = client.read("dst/file.txt").await.unwrap().unwrap();
        assert_eq!(src, bytes("copy me"));
        assert_eq!(dst, bytes("copy me"));
    }

    #[tokio::test]
    async fn rename_moves_and_removes_source() {
        let client = in_memory_client();
        client.write("old/name.txt", b"move me").await.unwrap();

        client.rename("old/name.txt", "new/name.txt").await.unwrap();

        assert!(!client.exists("old/name.txt").await.unwrap());
        let content = client.read("new/name.txt").await.unwrap().unwrap();
        assert_eq!(content, bytes("move me"));
    }

    // =========================================================================
    // Delete prefix
    // =========================================================================

    #[tokio::test]
    async fn delete_prefix_removes_all_matching_objects() {
        let client = in_memory_client();

        client.write("tmp/a.txt", b"a").await.unwrap();
        client.write("tmp/b.txt", b"b").await.unwrap();
        client.write("keep/c.txt", b"c").await.unwrap();

        let deleted = client.delete_prefix("tmp/").await.unwrap();
        assert_eq!(deleted, 2);

        assert!(client.list("tmp/").await.unwrap().is_empty());
        assert_eq!(client.list("keep/").await.unwrap().len(), 1);
    }

    // =========================================================================
    // Write-if-absent
    // =========================================================================

    #[tokio::test]
    async fn write_if_absent_creates_when_missing() {
        let client = in_memory_client();
        let written = client.write_if_absent("lock.txt", b"acquired").await.unwrap();
        assert!(written);
        assert!(client.exists("lock.txt").await.unwrap());
    }

    #[tokio::test]
    async fn write_if_absent_skips_when_present() {
        let client = in_memory_client();
        client.write("lock.txt", b"original").await.unwrap();

        let written = client.write_if_absent("lock.txt", b"new").await.unwrap();
        assert!(!written);

        // Content must remain unchanged.
        let content = client.read("lock.txt").await.unwrap().unwrap();
        assert_eq!(content, bytes("original"));
    }

    // =========================================================================
    // Watcher
    // =========================================================================

    #[tokio::test]
    async fn watcher_signals_on_new_object() {
        let client = in_memory_client();
        let watch_client = client.clone();

        let (tx, mut rx) = mpsc::channel(4);

        let _handle = ObjectStoreWatcher::new(watch_client, "watch/")
            .with_interval(Duration::from_millis(50))
            .spawn(tx);

        // Let the baseline poll run.
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Write a new object.
        client.write("watch/event.txt", b"boom").await.unwrap();

        // Expect a signal within a reasonable timeout.
        let signal = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("timed out waiting for watcher signal");
        assert!(signal.is_some());
    }

    #[tokio::test]
    async fn watcher_signals_on_deleted_object() {
        let client = in_memory_client();
        client.write("watch2/pre.txt", b"present").await.unwrap();

        let watch_client = client.clone();
        let (tx, mut rx) = mpsc::channel(4);

        let _handle = ObjectStoreWatcher::new(watch_client, "watch2/")
            .with_interval(Duration::from_millis(50))
            .spawn(tx);

        // Let the baseline poll establish.
        tokio::time::sleep(Duration::from_millis(100)).await;

        client.delete("watch2/pre.txt").await.unwrap();

        let signal = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("timed out waiting for watcher signal");
        assert!(signal.is_some());
    }

    #[tokio::test]
    async fn watcher_does_not_signal_when_no_change() {
        let client = in_memory_client();
        client.write("stable/obj.txt", b"no change").await.unwrap();

        let watch_client = client.clone();
        let (tx, mut rx) = mpsc::channel(4);

        let _handle = ObjectStoreWatcher::new(watch_client, "stable/")
            .with_interval(Duration::from_millis(50))
            .spawn(tx);

        // Let multiple polls run without any mutation.
        tokio::time::sleep(Duration::from_millis(250)).await;

        // Channel should be empty — no spurious signals.
        let maybe_signal = rx.try_recv();
        assert!(maybe_signal.is_err(), "Expected no signal but got one");
    }

    // =========================================================================
    // Config
    // =========================================================================

    #[test]
    fn config_builder_errors_on_missing_required_field() {
        use crate::config::ObjectStoreConfigBuilder;

        let result = ObjectStoreConfigBuilder::default()
            .bucket("my-bucket")
            .account("key")
            .secret("secret")
            // provider intentionally omitted
            .build();

        assert!(result.is_err());
    }

    #[test]
    fn config_builder_succeeds_with_all_fields() {
        use crate::config::{ObjectStoreConfigBuilder, Provider};

        let config = ObjectStoreConfigBuilder::default()
            .provider(Provider::S3 { endpoint_url: None })
            .bucket("bucket")
            .account("key")
            .secret("secret")
            .poll_interval(Duration::from_secs(60))
            .build()
            .unwrap();

        assert_eq!(config.bucket, "bucket");
        assert_eq!(config.default_poll_interval, Duration::from_secs(60));
    }
}