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

use super::config::{TcpIngestConfig, TcpIngestInitError};
use super::upload::{UploadError, upload};
use crate::common::client::s3::StorageBackend;
use crate::common::session::{Protocol, ProtocolPrincipal, SessionContext, is_temporary_credential};
use crate::constants::network::AUTH_FAILURE_DELAY_MS;
use rustfs_utils::MaskedAccessKey;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;
use subtle::ConstantTimeEq;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Semaphore, broadcast};
use tokio::task::JoinSet;
use tracing::{debug, info, warn};

const LOG_COMPONENT_PROTOCOLS: &str = "protocols";
const LOG_SUBSYSTEM_TCP_INGEST: &str = "tcp_ingest";
const EVENT_TCP_INGEST_SERVER_STATE: &str = "tcp_ingest_server_state";
const EVENT_TCP_INGEST_CONNECTION_STATE: &str = "tcp_ingest_connection_state";

const COMMAND: &str = "UPLOAD";
/// Longest accepted header line, newline included.
const MAX_HEADER_BYTES: u64 = 4096;
/// How long in-flight uploads may keep running after a shutdown signal.
const SHUTDOWN_GRACE: Duration = Duration::from_secs(30);

/// Parsed `UPLOAD` header line.
#[derive(Debug, PartialEq, Eq)]
struct Header {
    access_key: String,
    secret_key: String,
    bucket: String,
    key: String,
}

fn has_control(value: &str) -> bool {
    value.chars().any(char::is_control)
}

fn parse_header(line: &str) -> Option<Header> {
    let line = line.strip_suffix('\n')?;
    let line = line.strip_suffix('\r').unwrap_or(line);
    let mut fields = line.splitn(5, ' ');
    if fields.next()? != COMMAND {
        return None;
    }
    let access_key = fields.next()?;
    let secret_key = fields.next()?;
    let bucket = fields.next()?;
    let key = fields.next()?;
    if [access_key, secret_key, bucket, key]
        .iter()
        .any(|f| f.is_empty() || has_control(f))
    {
        return None;
    }
    Some(Header {
        access_key: access_key.to_owned(),
        secret_key: secret_key.to_owned(),
        bucket: bucket.to_owned(),
        key: key.to_owned(),
    })
}

/// Read the header line, leaving any body bytes already buffered in `reader`.
async fn read_header(reader: &mut BufReader<TcpStream>, idle: Duration) -> Option<Header> {
    let mut line = Vec::new();
    let mut limited = (&mut *reader).take(MAX_HEADER_BYTES);
    tokio::time::timeout(idle, limited.read_until(b'\n', &mut line))
        .await
        .ok()?
        .ok()?;
    parse_header(std::str::from_utf8(&line).ok()?)
}

/// Check the access/secret pair against IAM. Mirrors the FTPS and SFTP password logins:
/// temporary STS credentials are refused because this stream cannot carry a session token.
async fn authenticate(header: &Header, source_ip: IpAddr) -> Option<SessionContext> {
    let masked = MaskedAccessKey(&header.access_key);
    let iam_sys = rustfs_iam::get().ok()?;
    let (identity, valid) = iam_sys.check_key(&header.access_key).await.ok()?;
    let identity = identity.filter(|_| valid);
    let accepted = identity.filter(|identity| {
        !is_temporary_credential(&identity.credentials)
            && bool::from(identity.credentials.secret_key.as_bytes().ct_eq(header.secret_key.as_bytes()))
    });
    match accepted {
        Some(identity) => Some(SessionContext::new(
            ProtocolPrincipal::new(Arc::new(identity)),
            Protocol::TcpIngest,
            source_ip,
        )),
        None => {
            warn!(
                event = EVENT_TCP_INGEST_CONNECTION_STATE,
                component = LOG_COMPONENT_PROTOCOLS,
                subsystem = LOG_SUBSYSTEM_TCP_INGEST,
                result = "auth_rejected",
                username = %masked,
                "TCP ingest rejected credentials"
            );
            tokio::time::sleep(Duration::from_millis(AUTH_FAILURE_DELAY_MS)).await;
            None
        }
    }
}

/// TCP ingest server: accepts connections and stores each stream as one object.
pub struct TcpIngestServer<S: StorageBackend + 'static> {
    config: TcpIngestConfig,
    storage: Arc<S>,
}

impl<S: StorageBackend + 'static> TcpIngestServer<S> {
    /// Create a server after validating `config`.
    ///
    /// # Errors
    /// Returns [`TcpIngestInitError::InvalidConfig`] for an invalid configuration.
    pub fn new(config: TcpIngestConfig, storage: S) -> Result<Self, TcpIngestInitError> {
        config.validate()?;
        Ok(Self {
            config,
            storage: Arc::new(storage),
        })
    }

    pub fn config(&self) -> &TcpIngestConfig {
        &self.config
    }

    /// Bind the configured address and serve until `shutdown_rx` fires.
    ///
    /// # Errors
    /// Returns [`TcpIngestInitError::Bind`] when the address cannot be bound.
    pub async fn start(&self, shutdown_rx: broadcast::Receiver<()>) -> Result<(), TcpIngestInitError> {
        let listener = TcpListener::bind(self.config.bind_addr).await?;
        self.serve(listener, shutdown_rx).await;
        Ok(())
    }

    /// Serve an already bound listener; tests use this to learn the ephemeral port.
    pub async fn serve(&self, listener: TcpListener, mut shutdown_rx: broadcast::Receiver<()>) {
        info!(
            event = EVENT_TCP_INGEST_SERVER_STATE,
            component = LOG_COMPONENT_PROTOCOLS,
            subsystem = LOG_SUBSYSTEM_TCP_INGEST,
            state = "started",
            bind_addr = ?listener.local_addr().ok(),
            "TCP ingest server started"
        );
        let permits = Arc::new(Semaphore::new(self.config.max_connections));
        let mut connections = JoinSet::new();
        loop {
            tokio::select! {
                _ = shutdown_rx.recv() => break,
                // Reap finished connections so the set does not grow with traffic.
                Some(_) = connections.join_next(), if !connections.is_empty() => {}
                accepted = listener.accept() => {
                    let Ok((stream, peer)) = accepted else { continue };
                    let Ok(permit) = Arc::clone(&permits).try_acquire_owned() else {
                        debug!(
                            event = EVENT_TCP_INGEST_CONNECTION_STATE,
                            component = LOG_COMPONENT_PROTOCOLS,
                            subsystem = LOG_SUBSYSTEM_TCP_INGEST,
                            result = "connection_limit",
                            "TCP ingest connection limit reached"
                        );
                        continue;
                    };
                    let storage = Arc::clone(&self.storage);
                    let config = self.config.clone();
                    connections.spawn(async move {
                        let _permit = permit;
                        handle_connection(storage, config, stream, peer).await;
                    });
                }
            }
        }
        drop(listener);
        if tokio::time::timeout(SHUTDOWN_GRACE, async { while connections.join_next().await.is_some() {} })
            .await
            .is_err()
        {
            warn!(
                event = EVENT_TCP_INGEST_SERVER_STATE,
                component = LOG_COMPONENT_PROTOCOLS,
                subsystem = LOG_SUBSYSTEM_TCP_INGEST,
                state = "shutdown_grace_expired",
                "TCP ingest aborting in-flight uploads"
            );
        }
        info!(
            event = EVENT_TCP_INGEST_SERVER_STATE,
            component = LOG_COMPONENT_PROTOCOLS,
            subsystem = LOG_SUBSYSTEM_TCP_INGEST,
            state = "stopped",
            "TCP ingest server stopped"
        );
    }
}

async fn handle_connection<S: StorageBackend + 'static>(
    storage: Arc<S>,
    config: TcpIngestConfig,
    stream: TcpStream,
    peer: SocketAddr,
) {
    let mut reader = BufReader::new(stream);
    let Some(header) = read_header(&mut reader, config.idle_timeout).await else {
        debug!(
            event = EVENT_TCP_INGEST_CONNECTION_STATE,
            component = LOG_COMPONENT_PROTOCOLS,
            subsystem = LOG_SUBSYSTEM_TCP_INGEST,
            result = "bad_header",
            "TCP ingest rejected header"
        );
        return;
    };
    let Some(session) = authenticate(&header, peer.ip()).await else {
        return;
    };
    let result = upload(
        storage.as_ref(),
        &session,
        reader,
        &header.bucket,
        &header.key,
        config.max_object_size,
        config.idle_timeout,
    )
    .await;
    match result {
        Ok(size) => info!(
            event = EVENT_TCP_INGEST_CONNECTION_STATE,
            component = LOG_COMPONENT_PROTOCOLS,
            subsystem = LOG_SUBSYSTEM_TCP_INGEST,
            result = "stored",
            username = %MaskedAccessKey(session.access_key()),
            bucket = %header.bucket,
            size = size,
            "TCP ingest stored object"
        ),
        Err(error) => warn!(
            event = EVENT_TCP_INGEST_CONNECTION_STATE,
            component = LOG_COMPONENT_PROTOCOLS,
            subsystem = LOG_SUBSYSTEM_TCP_INGEST,
            result = upload_result(&error),
            username = %MaskedAccessKey(session.access_key()),
            bucket = %header.bucket,
            error = %error,
            "TCP ingest discarded upload"
        ),
    }
}

fn upload_result(error: &UploadError) -> &'static str {
    match error {
        UploadError::Denied => "denied",
        UploadError::InvalidName(_) => "invalid_name",
        UploadError::TooLarge { .. } | UploadError::TooManyParts => "too_large",
        UploadError::IdleTimeout => "idle_timeout",
        UploadError::Read(_) => "read_failed",
        UploadError::Backend(_) => "backend_failed",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_header_with_spaces_in_key() {
        let header = parse_header("UPLOAD ak sk bucket dir/my file.bin\r\n").expect("valid header");
        assert_eq!(
            header,
            Header {
                access_key: "ak".into(),
                secret_key: "sk".into(),
                bucket: "bucket".into(),
                key: "dir/my file.bin".into()
            }
        );
    }

    #[test]
    fn rejects_malformed_headers() {
        for line in [
            "UPLOAD ak sk bucket\n",       // no key
            "DOWNLOAD ak sk bucket key\n", // wrong command
            "UPLOAD  sk bucket key\n",     // empty access key
            "UPLOAD ak sk bucket ke\ty\n", // control byte in key
            "UPLOAD ak sk bucket key",     // unterminated line
        ] {
            assert!(parse_header(line).is_none(), "accepted {line:?}");
        }
    }
}
