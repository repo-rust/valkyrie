#![allow(dead_code)]
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::{Child, Command as StdCommand, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use assert_cmd::cargo::{self};

/// Test helper that starts/stops a Valkyrie server for integration tests.
pub struct ValkyrieServerTest {
    child: Child,
    addr: String,
}

impl ValkyrieServerTest {
    /// Start the server on an ephemeral localhost port with given handler/shard counts.
    pub fn start(tcp_handlers: usize, shards: usize) -> anyhow::Result<Self> {
        // Choose a free local port to avoid conflicts across tests/machines.
        let port = {
            let l = TcpListener::bind("127.0.0.1:0")?;
            let p = l.local_addr()?.port();
            drop(l);
            p
        };
        let addr = format!("127.0.0.1:{port}");

        // Spawn the server binary with the CLI flags expected by the current codebase.
        let bin_path = cargo::cargo_bin!("valkyrie");
        let mut child = StdCommand::new(bin_path)
            .arg("--address")
            .arg(&addr)
            .arg("--tcp-handlers")
            .arg(tcp_handlers.to_string())
            .arg("--shards")
            .arg(shards.to_string())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?;

        // Wait until the server starts accepting connections on the chosen port.
        let start = Instant::now();
        loop {
            match TcpStream::connect(&addr) {
                Ok(_) => break,
                Err(_) => {
                    if start.elapsed() > Duration::from_secs(5) {
                        let _ = child.kill();
                        let _ = child.wait();
                        anyhow::bail!(
                            "Timed out waiting for server to accept connections on {addr}"
                        );
                    }
                    thread::sleep(Duration::from_millis(50));
                }
            }
        }

        Ok(Self { child, addr })
    }

    /// Open a new TCP connection to the running server.
    pub fn connect(&self) -> std::io::Result<TcpStream> {
        let stream = TcpStream::connect(&self.addr)?;
        stream.set_read_timeout(Some(Duration::from_secs(3)))?;
        Ok(stream)
    }
}

impl Drop for ValkyrieServerTest {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Test client helper that keeps the server process alive and provides simple RESP helpers.
pub struct ValkyrieClientTest {
    // Keep the server alive for the lifetime of the client to avoid dropping the child process.
    _server: ValkyrieServerTest,
    stream: TcpStream,
    reader: BufReader<TcpStream>,
}

impl ValkyrieClientTest {
    pub fn new(server: ValkyrieServerTest) -> Self {
        // Connect to server
        let stream = server.connect().expect("connect to server");
        let reader = BufReader::new(stream.try_clone().expect("clone stream for reading"));

        Self {
            _server: server,
            stream,
            reader,
        }
    }

    /// Low-level: send raw request bytes and flush.
    pub fn send(&mut self, request: &[u8]) -> std::io::Result<()> {
        self.stream.write_all(request)?;
        self.stream.flush()
    }

    /// Read a single line (terminated by CRLF) and return it.
    pub fn read_line(&mut self) -> std::io::Result<String> {
        let mut line = String::new();
        self.reader.read_line(&mut line)?;
        Ok(line)
    }

    pub fn assert_command_response2(&mut self, actual_request: &str, expected_response: &str) {
        self.send(actual_request.as_bytes()).expect("write request");
        let line = self.read_line().expect("read response");
        assert_eq!(line, expected_response, "Unexpected response");
    }

    /// Read a RESP Bulk String or Null Bulk String from the reader.
    /// - Returns Some(String) when a Bulk String is received
    /// - Returns None when a Null Bulk String ($-1) is received
    pub fn read_bulk_or_null(&mut self) -> Option<String> {
        // Read header line: either "$<len>\r\n" or "$-1\r\n"
        let mut header = String::new();
        self.reader.read_line(&mut header).expect("read header");
        if header == "$-1\r\n" {
            return None;
        }
        assert!(
            header.starts_with('$'),
            "Expected bulk string header, got: {header:?}"
        );
        let len: usize = header[1..].trim().parse().expect("parse bulk length");

        // Read payload
        let mut payload = vec![0u8; len];
        self.reader.read_exact(&mut payload).expect("read payload");

        // Read trailing \r\n
        let mut terminator = [0u8; 2];
        self.reader
            .read_exact(&mut terminator)
            .expect("read bulk terminator");
        assert_eq!(&terminator, b"\r\n", "Bulk string not properly terminated");

        Some(String::from_utf8(payload).expect("payload utf8"))
    }
}
