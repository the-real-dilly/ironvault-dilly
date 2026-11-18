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

//! One-way TCP ingest: upload an object by writing to a plain TCP socket.
//!
//! Wire format, client to server only (the server never writes to the socket):
//!
//! ```text
//! UPLOAD <access_key> <secret_key> <bucket> <object key>\n
//! <object bytes...>
//! <clean half-close of the write side>
//! ```
//!
//! The object key is everything after the bucket up to the newline, so it may contain spaces.
//! The object is committed only when the client closes its write side cleanly within the size
//! limit; any error, timeout, or oversize stream discards the data. Because the server sends no
//! reply, clients learn of rejection only by the object not appearing, and the credential
//! travels in cleartext: bind to loopback or a trusted network, or tunnel the connection.

pub mod client;
pub mod config;
mod server;
mod upload;

pub use config::{TcpIngestConfig, TcpIngestInitError};
pub use server::TcpIngestServer;
