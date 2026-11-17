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

use std::net::SocketAddr;
use std::time::Duration;
use thiserror::Error;

/// TCP ingest initialization error.
#[derive(Debug, Error)]
pub enum TcpIngestInitError {
    #[error("failed to bind address {0}")]
    Bind(#[from] std::io::Error),
    #[error("invalid TCP ingest configuration: {0}")]
    InvalidConfig(String),
}

/// TCP ingest server configuration.
#[derive(Debug, Clone)]
pub struct TcpIngestConfig {
    /// Listener bind address.
    pub bind_addr: SocketAddr,
    /// Largest object accepted on one stream, in bytes. A longer stream is discarded.
    pub max_object_size: u64,
    /// Maximum concurrent connections; excess connections are closed immediately.
    pub max_connections: usize,
    /// Longest the client may take to send the header or stall between reads.
    pub idle_timeout: Duration,
}

impl TcpIngestConfig {
    /// Validate the configuration.
    ///
    /// # Errors
    /// Returns [`TcpIngestInitError::InvalidConfig`] when a limit is zero.
    pub fn validate(&self) -> Result<(), TcpIngestInitError> {
        if self.max_object_size == 0 {
            return Err(TcpIngestInitError::InvalidConfig("max object size must be greater than zero".to_owned()));
        }
        if self.max_connections == 0 {
            return Err(TcpIngestInitError::InvalidConfig("max connections must be greater than zero".to_owned()));
        }
        if self.idle_timeout.is_zero() {
            return Err(TcpIngestInitError::InvalidConfig("idle timeout must be greater than zero".to_owned()));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid() -> TcpIngestConfig {
        TcpIngestConfig {
            bind_addr: "127.0.0.1:0".parse().expect("addr"),
            max_object_size: 1,
            max_connections: 1,
            idle_timeout: Duration::from_secs(1),
        }
    }

    #[test]
    fn accepts_valid_and_rejects_zero_limits() {
        assert!(valid().validate().is_ok());
        assert!(
            TcpIngestConfig {
                max_object_size: 0,
                ..valid()
            }
            .validate()
            .is_err()
        );
        assert!(
            TcpIngestConfig {
                max_connections: 0,
                ..valid()
            }
            .validate()
            .is_err()
        );
        assert!(
            TcpIngestConfig {
                idle_timeout: Duration::ZERO,
                ..valid()
            }
            .validate()
            .is_err()
        );
    }
}
