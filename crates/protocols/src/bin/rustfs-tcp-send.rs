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

//! `rustfs-tcp-send`: upload files to a RustFS TCP ingest listener.

use rustfs_protocols::tcp_ingest::client::{TcpIngestTarget, send};
use std::path::Path;
use std::process::ExitCode;

const USAGE: &str = "\
Usage: rustfs-tcp-send --addr HOST:PORT --bucket BUCKET [--prefix PREFIX] FILE...
       rustfs-tcp-send --addr HOST:PORT --bucket BUCKET --key KEY -     (read stdin)

Credentials come from RUSTFS_ACCESS_KEY and RUSTFS_SECRET_KEY, never from arguments.
Each file is stored as PREFIX + file name. The secret key is sent in cleartext: use a
trusted network or a tunnel. The server sends no reply, so verify uploads separately.";

struct Args {
    addr: String,
    bucket: String,
    prefix: String,
    key: Option<String>,
    inputs: Vec<String>,
}

fn parse_args(mut args: impl Iterator<Item = String>) -> Result<Args, String> {
    let (mut addr, mut bucket, mut key) = (None, None, None);
    let mut prefix = String::new();
    let mut inputs = Vec::new();
    while let Some(arg) = args.next() {
        let mut value = |name: &str| args.next().ok_or_else(|| format!("{name} needs a value"));
        match arg.as_str() {
            "--addr" => addr = Some(value("--addr")?),
            "--bucket" => bucket = Some(value("--bucket")?),
            "--prefix" => prefix = value("--prefix")?,
            "--key" => key = Some(value("--key")?),
            "-h" | "--help" => return Err(String::new()),
            "-" => inputs.push(arg),
            flag if flag.starts_with("--") => return Err(format!("unknown option {flag}")),
            _ => inputs.push(arg),
        }
    }
    let args = Args {
        addr: addr.ok_or("--addr is required")?,
        bucket: bucket.ok_or("--bucket is required")?,
        prefix,
        key,
        inputs,
    };
    if args.inputs.is_empty() {
        return Err("no input files".to_owned());
    }
    if args.key.is_some() && args.inputs.len() != 1 {
        return Err("--key works with exactly one input".to_owned());
    }
    if args.inputs.iter().any(|i| i == "-") && args.key.is_none() {
        return Err("reading stdin requires --key".to_owned());
    }
    Ok(args)
}

fn object_key(args: &Args, input: &str) -> Result<String, String> {
    if let Some(key) = &args.key {
        return Ok(key.clone());
    }
    let name = Path::new(input)
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| format!("cannot derive an object name from {input}"))?;
    Ok(format!("{}{name}", args.prefix))
}

async fn run(args: Args) -> Result<(), String> {
    let access_key = std::env::var("RUSTFS_ACCESS_KEY").map_err(|_| "RUSTFS_ACCESS_KEY is not set")?;
    let secret_key = std::env::var("RUSTFS_SECRET_KEY").map_err(|_| "RUSTFS_SECRET_KEY is not set")?;
    for input in &args.inputs {
        let key = object_key(&args, input)?;
        let target = TcpIngestTarget {
            access_key: &access_key,
            secret_key: &secret_key,
            bucket: &args.bucket,
            key: &key,
        };
        let sent = if input == "-" {
            send(&args.addr, &target, tokio::io::stdin()).await
        } else {
            let file = tokio::fs::File::open(input).await.map_err(|e| format!("{input}: {e}"))?;
            send(&args.addr, &target, file).await
        }
        .map_err(|e| format!("{input}: {e}"))?;
        println!("sent {input} -> {}/{key} ({sent} bytes)", args.bucket);
    }
    Ok(())
}

fn main() -> ExitCode {
    let args = match parse_args(std::env::args().skip(1)) {
        Ok(args) => args,
        Err(message) => {
            if !message.is_empty() {
                eprintln!("error: {message}\n");
            }
            eprintln!("{USAGE}");
            return ExitCode::from(2);
        }
    };
    let runtime = match tokio::runtime::Builder::new_current_thread().enable_all().build() {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("error: {error}");
            return ExitCode::FAILURE;
        }
    };
    match runtime.block_on(run(args)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(message) => {
            eprintln!("error: {message}");
            ExitCode::FAILURE
        }
    }
}
