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
