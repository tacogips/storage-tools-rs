use anyhow::{anyhow, Error, Result};
use cloud_storage::bucket::{Location, MultiRegion};
use cloud_storage::{Bucket, ListRequest, NewBucket, Object};

use futures::future;
use futures::stream::TryStreamExt;
use futures_util::future::TryFutureExt;
use log;
use std::convert::Into;

use crate::mime::MimeType;

pub async fn object_exists(bucket: &str, path: &str) -> Result<bool> {
    find_object(bucket, path)
        .await
        .and_then(|object_if_found| Ok(object_if_found.is_some()))
}

fn list_prefix_request(prefix: String) -> ListRequest {
    ListRequest {
        /// When specified, allows the `list` to operate like a directory listing by splitting the
        /// object location on this delimiter.
        delimiter: None,

        /// Filter results to objects whose names are lexicographically before `end_offset`.
        /// If `start_offset` is also set, the objects listed have names between `start_offset`
        /// (inclusive) and `end_offset` (exclusive).
        end_offset: None,

        /// If true, objects that end in exactly one instance of `delimiter` have their metadata
        /// included in `items` in addition to the relevant part of the object name appearing in
        /// `prefixes`.
        include_trailing_delimiter: None,

        /// Maximum combined number of entries in `items` and `prefixes` to return in a single
        /// page of responses. Because duplicate entries in `prefixes` are omitted, fewer total
        /// results may be returned than requested. The service uses this parameter or 1,000
        /// items, whichever is smaller.
        max_results: None,

        /// A previously-returned page token representing part of the larger set of results to view.
        /// The `page_token` is an encoded field that marks the name and generation of the last object
        /// in the returned list. In a subsequent request using the `page_token`, items that come after
        /// the `page_token` are shown (up to `max_results`).
        ///
        /// If the page token is provided, all objects starting at that page token are queried
        page_token: None,

        /// Filter results to include only objects whose names begin with this prefix.
        prefix: Some(prefix),

        /// Set of properties to return. Defaults to `NoAcl`.
        projection: None,

        /// Filter results to objects whose names are lexicographically equal to or after
        /// `start_offset`. If `end_offset` is also set, the objects listed have names between
        /// `start_offset` (inclusive) and `end_offset` (exclusive).
        start_offset: None,

        /// If true, lists all versions of an object as distinct results in order of increasing
        /// generation number. The default value for versions is false. For more information, see
        /// Object Versioning.
        versions: None,
    }
}

pub async fn find_object(bucket: &str, name: &str) -> Result<Option<Object>> {
    if name.ends_with("/") {
        return Err(anyhow!("its not object path {}", name));
    }

    //TODO(tacogips) it's unsafficient to use `await` for performance
    let object_chunks = Object::list(bucket, list_prefix_request(name.to_string()))
        .and_then(|objs_stream| objs_stream.try_collect::<Vec<_>>())
        .await?;
    for each_objs in object_chunks.into_iter() {
        let found = each_objs
            .items
            .into_iter()
            .find(|each_obj| each_obj.name == name);
        if found.is_some() {
            return Ok(found);
        }
    }
    Ok(None)
}

pub async fn list_objects(bucket: &str, name: &str) -> Result<Vec<Object>> {
    if name.ends_with("/") {
        return Err(anyhow!("its not object path {}", name));
    }

    //TODO(tacogips) it's unsafficient to use `await` for performance
    let object_chunks = Object::list(bucket, list_prefix_request(name.to_string()))
        .and_then(|objs_stream| objs_stream.try_collect::<Vec<_>>())
        .await?;

    let mut result = Vec::<Object>::new();
    for mut each_objs_list in object_chunks.into_iter() {
        result.append(&mut each_objs_list.items);
    }
    Ok(result)
}

pub async fn download_object(bucket: &str, name: &str) -> Result<Vec<u8>> {
    if name.ends_with("/") {
        return Err(anyhow!("it's not gs file path {}", name));
    }

    Object::download(bucket, name).await.map_err(Error::msg)
}

pub async fn create_object(
    bucket: &str,
    path: &str,
    body: Vec<u8>,
    mime_type: MimeType,
) -> Result<Object> {
    Object::create(bucket, body, path, mime_type.into())
        .await
        .map_err(Error::msg)
}

pub async fn delete_object(bucket: &str, path: &str) -> Result<()> {
    Object::delete(bucket, path).await.map_err(Error::msg)
}

pub async fn create_bucket(bucket: &str) -> Result<Bucket> {
    let new_bucket = NewBucket {
        name: bucket.to_owned(), // this is the only mandatory field
        location: Location::Multi(MultiRegion::Asia),
        ..Default::default()
    };

    Bucket::create(&new_bucket).await.map_err(Error::msg)
}

pub async fn bucket_exists(bucket: &str) -> bool {
    let a = find_bucket(bucket)
        .and_then(|found_or_not| future::ok(found_or_not.is_some()))
        .await;
    a.unwrap_or_else(|e| {
        log::warn!("bucket exists error {}", e);
        false
    })
}

pub async fn find_bucket(bucket: &str) -> Result<Option<Bucket>> {
    let buckets = Bucket::list().await?;
    Ok(buckets
        .into_iter()
        .find(|each_bucket| each_bucket.name == bucket))
}

/// cloud-storage.rs has a problem with the global reqwest Client
/// that cause `dispatch dropped without returning error` error.
/// https://github.com/hyperium/hyper/issues/2112
/// We use Mutex lock that let just single test run to avoid it.
#[cfg(test)]
mod tests {
    use crate::test_util::{set_test_service_account_to_env, TEST_BUCKET_NAME};
    use lazy_static::lazy_static;
    use std::sync::Mutex;
    use tokio;
    use uuid::Uuid;
    lazy_static! {
        static ref TEST_BUCKET_MUTEX: Mutex<()> = Mutex::new(());
    }

    #[tokio::test]
    async fn bucket_exists() {
        let _lock = TEST_BUCKET_MUTEX.lock();
        set_test_service_account_to_env();

        let actual = super::bucket_exists(TEST_BUCKET_NAME).await;
        assert_eq!(true, actual)
    }

    #[tokio::test]
    async fn object_exists_create_delete() {
        let _lock = TEST_BUCKET_MUTEX.lock();
        set_test_service_account_to_env();
        let test_objects_name = format!("file_manager_test/{}/test_file", Uuid::new_v4().to_urn());

        assert_eq!(
            false,
            super::object_exists(TEST_BUCKET_NAME, &test_objects_name)
                .await
                .unwrap(),
            "assert file is not exists yet.",
        );

        let body_str = String::from("これはテストabc;[[;!");
        assert_eq!(
            true,
            super::create_object(
                TEST_BUCKET_NAME,
                &test_objects_name,
                body_str.into_bytes(),
                super::MimeType::OctetStream
            )
            .await
            .is_ok(),
            "file is created without error",
        );

        assert_eq!(
            true,
            super::object_exists(TEST_BUCKET_NAME, &test_objects_name)
                .await
                .unwrap(),
            "find the created file ",
        );

        assert_eq!(
            true,
            super::delete_object(TEST_BUCKET_NAME, &test_objects_name)
                .await
                .is_ok(),
            "remove created file ",
        );
    }
}
