// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Streams a socket body into the object store: one PutObject for a small body, otherwise a
//! multipart upload that is aborted on any failure so a partial stream never becomes an object.

use crate::common::{client::s3::StorageBackend, gateway::S3Action, session::SessionContext};
use bytes::Bytes;
use rustfs_gateway_stream::ByteStream;
use rustfs_gateway_types::dto::*;
use rustfs_gateway_types::{BucketName, ObjectKey, UploadIdClaim};
use std::time::Duration;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt};

/// One in-flight part per upload; the next part is never buffered while the backend consumes this one.
pub(super) const PART_SIZE: usize = 16 * 1024 * 1024;
const MAX_PARTS: usize = 10_000;
const ABORT_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Error)]
pub(super) enum UploadError {
    #[error("access denied")]
    Denied,
    #[error("invalid bucket or object key: {0}")]
    InvalidName(String),
    #[error("stream exceeds the {limit} byte object size limit")]
    TooLarge { limit: u64 },
    #[error("stream needs more than {MAX_PARTS} parts")]
    TooManyParts,
    #[error("client stalled for longer than the idle timeout")]
    IdleTimeout,
    #[error("stream read failed")]
    Read(#[source] std::io::Error),
    #[error("storage backend failed: {0}")]
    Backend(String),
}

fn backend_error(error: impl std::fmt::Display) -> UploadError {
    UploadError::Backend(error.to_string())
}

async fn authorize<S: StorageBackend>(
    storage: &S,
    session: &SessionContext,
    action: S3Action,
    bucket: &str,
    key: &str,
) -> Result<(), UploadError> {
    storage
        .authorize_operation(session, &action, bucket, Some(key))
        .await
        .map_err(|_| UploadError::Denied)
}

/// Read up to `PART_SIZE` bytes. An empty result means the client closed its write side.
async fn read_part(reader: &mut (impl AsyncRead + Unpin), idle: Duration) -> Result<Vec<u8>, UploadError> {
    let mut part = Vec::new();
    while part.len() < PART_SIZE {
        let remaining = u64::try_from(PART_SIZE - part.len()).map_err(backend_error)?;
        let read = tokio::time::timeout(idle, reader.take(remaining).read_buf(&mut part))
            .await
            .map_err(|_| UploadError::IdleTimeout)?
            .map_err(UploadError::Read)?;
        if read == 0 {
            break;
        }
    }
    Ok(part)
}

fn body(part: Vec<u8>) -> ByteStream {
    ByteStream::from_bytes(Bytes::from(part))
}

fn names(bucket: &str, key: &str) -> Result<(BucketName, ObjectKey), UploadError> {
    let bucket = BucketName::new(bucket.to_owned()).map_err(|e| UploadError::InvalidName(e.to_string()))?;
    let key = ObjectKey::new(key.to_owned()).map_err(|e| UploadError::InvalidName(e.to_string()))?;
    Ok((bucket, key))
}

fn check_size(total: u64, limit: u64) -> Result<(), UploadError> {
    if total > limit {
        return Err(UploadError::TooLarge { limit });
    }
    Ok(())
}

/// Upload `reader` to `bucket`/`key`, returning the stored size.
///
/// The object becomes visible only after the reader reaches a clean EOF within `max_size`.
/// Cancelling this future mid-multipart leaves an incomplete upload that needs an
/// `AbortIncompleteMultipartUpload` lifecycle rule to reclaim.
pub(super) async fn upload<S: StorageBackend + 'static>(
    storage: &S,
    session: &SessionContext,
    mut reader: impl AsyncRead + Unpin,
    bucket: &str,
    key: &str,
    max_size: u64,
    idle: Duration,
) -> Result<u64, UploadError> {
    let (bucket_name, object_key) = names(bucket, key)?;
    let mut part = read_part(&mut reader, idle).await?;
    if part.len() < PART_SIZE {
        let size = u64::try_from(part.len()).map_err(backend_error)?;
        check_size(size, max_size)?;
        authorize(storage, session, S3Action::PutObject, bucket, key).await?;
        let input = PutObjectInput::builder()
            .bucket(bucket_name)
            .key(object_key)
            .content_length(i64::try_from(size).map_err(backend_error)?)
            .body(body(part))
            .build();
        storage
            .put_object(input, session.credentials())
            .await
            .map_err(backend_error)?;
        return Ok(size);
    }

    check_size(u64::try_from(part.len()).map_err(backend_error)?, max_size)?;
    authorize(storage, session, S3Action::CreateMultipartUpload, bucket, key).await?;
    let output = storage
        .create_multipart_upload(
            CreateMultipartUploadInput::builder()
                .bucket(bucket_name.clone())
                .key(object_key.clone())
                .build(),
            session.credentials(),
        )
        .await
        .map_err(backend_error)?;
    if output.upload_id.is_empty() {
        return Err(backend_error("CreateMultipartUpload returned no upload ID"));
    }
    let upload_id = output.upload_id;

    let result = async {
        let mut parts = Vec::new();
        let mut total = 0u64;
        while !part.is_empty() {
            if parts.len() >= MAX_PARTS {
                return Err(UploadError::TooManyParts);
            }
            let length = u64::try_from(part.len()).map_err(backend_error)?;
            total = total.checked_add(length).ok_or(UploadError::TooLarge { limit: max_size })?;
            check_size(total, max_size)?;
            let number = i32::try_from(parts.len() + 1).map_err(backend_error)?;
            authorize(storage, session, S3Action::UploadPart, bucket, key).await?;
            let output = storage
                .upload_part(
                    UploadPartInput::builder()
                        .bucket(bucket_name.clone())
                        .key(object_key.clone())
                        .upload_id(UploadIdClaim::from_wire(upload_id.clone()))
                        .part_number(number)
                        .content_length(i64::try_from(length).map_err(backend_error)?)
                        .body(body(std::mem::take(&mut part)))
                        .build(),
                    session.credentials(),
                )
                .await
                .map_err(backend_error)?;
            if output.e_tag == rustfs_gateway_types::ETag::default() {
                return Err(backend_error("UploadPart returned no ETag"));
            }
            parts.push(CompletedPart {
                e_tag: Some(output.e_tag),
                part_number: number,
                ..Default::default()
            });
            part = read_part(&mut reader, idle).await?;
        }
        authorize(storage, session, S3Action::CompleteMultipartUpload, bucket, key).await?;
        storage
            .complete_multipart_upload(
                CompleteMultipartUploadInput::builder()
                    .bucket(bucket_name.clone())
                    .key(object_key.clone())
                    .upload_id(UploadIdClaim::from_wire(upload_id.clone()))
                    .multipart_upload(CompletedMultipartUpload { parts })
                    .build(),
                session.credentials(),
            )
            .await
            .map_err(backend_error)?;
        Ok(total)
    }
    .await;

    if result.is_err() {
        abort(storage, session, bucket_name, object_key, upload_id, bucket, key).await;
    }
    result
}

async fn abort<S: StorageBackend>(
    storage: &S,
    session: &SessionContext,
    bucket_name: BucketName,
    object_key: ObjectKey,
    upload_id: String,
    bucket: &str,
    key: &str,
) {
    let cleanup = async {
        authorize(storage, session, S3Action::AbortMultipartUpload, bucket, key).await?;
        storage
            .abort_multipart_upload(
                AbortMultipartUploadInput::builder()
                    .bucket(bucket_name)
                    .key(object_key)
                    .upload_id(UploadIdClaim::from_wire(upload_id))
                    .build(),
                session.credentials(),
            )
            .await
            .map_err(backend_error)?;
        Ok::<_, UploadError>(())
    };
    if !matches!(tokio::time::timeout(ABORT_TIMEOUT, cleanup).await, Ok(Ok(()))) {
        tracing::warn!(
            event = "tcp_ingest_upload_cleanup",
            component = "protocols",
            subsystem = "tcp_ingest",
            result = "abort_failed",
            "TCP ingest incomplete upload requires lifecycle cleanup"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::{
        dummy_storage::DummyBackend,
        gateway::is_operation_supported,
        session::{Protocol, test_session},
    };
    use tokio::io::AsyncWriteExt;

    const IDLE: Duration = Duration::from_secs(5);

    async fn run(backend: &DummyBackend, data: Vec<u8>, max: u64) -> Result<u64, UploadError> {
        let session = test_session(Protocol::TcpIngest);
        upload(backend, &session, std::io::Cursor::new(data), "b", "dir/k", max, IDLE).await
    }

    #[test]
    fn protocol_only_permits_object_creation() {
        for action in [S3Action::PutObject, S3Action::UploadPart, S3Action::CompleteMultipartUpload] {
            assert!(is_operation_supported(Protocol::TcpIngest, &action));
        }
        for action in [
            S3Action::GetObject,
            S3Action::DeleteObject,
            S3Action::ListBucket,
            S3Action::ListBuckets,
        ] {
            assert!(!is_operation_supported(Protocol::TcpIngest, &action));
        }
    }

    #[tokio::test]
    async fn small_stream_commits_with_single_put() {
        let backend = DummyBackend::new();
        assert_eq!(run(&backend, vec![7; 1000], 4096).await.expect("upload"), 1000);
        let calls = backend.put_object_calls();
        assert_eq!((calls[0].bucket.as_str(), calls[0].key.as_str()), ("b", "dir/k"));
    }

    #[tokio::test]
    async fn oversize_stream_is_discarded() {
        let backend = DummyBackend::new();
        let error = run(&backend, vec![0; 100], 99).await.expect_err("over limit");
        assert!(matches!(error, UploadError::TooLarge { limit: 99 }));
        assert!(backend.put_object_calls().is_empty());
    }

    #[tokio::test]
    async fn large_stream_uses_multipart_and_aborts_when_over_limit() {
        let backend = DummyBackend::new();
        backend.queue_create_multipart_upload_ok("u1");
        backend.queue_upload_part_ok("e1");
        backend.queue_complete_multipart_upload_ok();
        let size = PART_SIZE + 5;
        let stored = run(&backend, vec![1; size], u64::try_from(size).expect("size"))
            .await
            .expect("multipart");
        assert_eq!(stored, u64::try_from(size).expect("size"));
        assert_eq!(backend.complete_multipart_calls().len(), 1);
        assert!(backend.abort_multipart_calls().is_empty());

        let backend = DummyBackend::new();
        backend.queue_create_multipart_upload_ok("u2");
        backend.queue_upload_part_ok("e1");
        let limit = u64::try_from(PART_SIZE).expect("size");
        let error = run(&backend, vec![1; PART_SIZE + 1], limit).await.expect_err("over limit");
        assert!(matches!(error, UploadError::TooLarge { .. }));
        assert!(backend.complete_multipart_calls().is_empty());
        assert_eq!(backend.abort_multipart_calls().len(), 1);
    }

    #[tokio::test]
    async fn stalled_client_times_out_without_storing() {
        let backend = DummyBackend::new();
        let session = test_session(Protocol::TcpIngest);
        let (mut client, server) = tokio::io::duplex(64);
        client.write_all(b"partial").await.expect("write");
        let error = upload(&backend, &session, server, "b", "k", 1024, Duration::from_millis(50))
            .await
            .expect_err("idle");
        assert!(matches!(error, UploadError::IdleTimeout));
        assert!(backend.put_object_calls().is_empty());
    }
}
