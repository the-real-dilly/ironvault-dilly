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

//! Client side of the one-way TCP ingest protocol.

use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpStream, ToSocketAddrs};

/// Client-side failure while sending a stream.
#[derive(Debug, Error)]
pub enum TcpIngestSendError {
    #[error("invalid {field}: {reason}")]
    InvalidField { field: &'static str, reason: &'static str },
    #[error("connection failed")]
    Io(#[from] std::io::Error),
}

/// Credentials and destination for one upload.
#[derive(Debug, Clone)]
pub struct TcpIngestTarget<'a> {
    pub access_key: &'a str,
    pub secret_key: &'a str,
    pub bucket: &'a str,
    pub key: &'a str,
}

fn check(field: &'static str, value: &str, allow_space: bool) -> Result<(), TcpIngestSendError> {
    let reason = if value.is_empty() {
        "must not be empty"
    } else if value.chars().any(char::is_control) {
        "must not contain control characters"
    } else if !allow_space && value.contains(' ') {
        "must not contain spaces"
    } else {
        return Ok(());
    };
    Err(TcpIngestSendError::InvalidField { field, reason })
}

impl TcpIngestTarget<'_> {
    fn header(&self) -> Result<String, TcpIngestSendError> {
        check("access key", self.access_key, false)?;
        check("secret key", self.secret_key, false)?;
        check("bucket", self.bucket, false)?;
        check("object key", self.key, true)?;
        Ok(format!("UPLOAD {} {} {} {}\n", self.access_key, self.secret_key, self.bucket, self.key))
    }
}

/// Send `reader` to the ingest server at `addr` and return the number of body bytes written.
///
/// The server never replies. After a clean half-close this waits for the server to close its
/// side, which means it has finished reading the stream, but it does **not** mean the object was
/// stored: a rejected or failed upload closes the connection the same way. Verify with an S3
/// `HEAD` if confirmation matters.
///
/// # Errors
/// Returns [`TcpIngestSendError::InvalidField`] for a malformed header field and
/// [`TcpIngestSendError::Io`] for connection or read/write failures.
pub async fn send(
    addr: impl ToSocketAddrs,
    target: &TcpIngestTarget<'_>,
    mut reader: impl AsyncRead + Unpin,
) -> Result<u64, TcpIngestSendError> {
    let header = target.header()?;
    let mut stream = TcpStream::connect(addr).await?;
    stream.write_all(header.as_bytes()).await?;
    let sent = tokio::io::copy(&mut reader, &mut stream).await?;
    stream.shutdown().await?;
    let mut drain = Vec::new();
    stream.read_to_end(&mut drain).await?;
    Ok(sent)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;

    fn target<'a>(key: &'a str) -> TcpIngestTarget<'a> {
        TcpIngestTarget {
            access_key: "ak",
            secret_key: "sk",
            bucket: "test-bucket",
            key,
        }
    }

    #[tokio::test]
    async fn sends_header_then_body_then_clean_close() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept");
            let mut received = Vec::new();
            // Returns only when the client half-closed cleanly.
            socket.read_to_end(&mut received).await.expect("read");
            received
        });
        let sent = send(addr, &target("dir/my file.bin"), std::io::Cursor::new(b"hello".to_vec()))
            .await
            .expect("send");
        assert_eq!(sent, 5);
        assert_eq!(server.await.expect("server"), b"UPLOAD ak sk test-bucket dir/my file.bin\nhello");
    }

    #[tokio::test]
    async fn rejects_fields_that_would_corrupt_the_header() {
        let empty = std::io::Cursor::new(Vec::new());
        let bad = TcpIngestTarget {
            bucket: "has space",
            ..target("k")
        };
        assert!(matches!(
            send("127.0.0.1:1", &bad, empty).await,
            Err(TcpIngestSendError::InvalidField { field: "bucket", .. })
        ));
        let newline = std::io::Cursor::new(Vec::new());
        assert!(matches!(
            send("127.0.0.1:1", &target("a\nUPLOAD x"), newline).await,
            Err(TcpIngestSendError::InvalidField { field: "object key", .. })
        ));
    }
}
