use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::panic::{self, AssertUnwindSafe};
use std::process::{Command as StdCommand, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use assert_cmd::cargo::{self};

#[test]
fn run_valkyrie_and_send_few_commands() {
    // Choose a free local port to avoid conflicts across tests/machines.
    let port = {
        let l = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
        let p = l.local_addr().unwrap().port();
        drop(l);
        p
    };
    let addr = format!("127.0.0.1:{port}");

    // Spawn the server binary with the new CLI arguments.
    // Recent changes removed the positional 'reuseport' arg in favor of flags.
    let bin_path = cargo::cargo_bin!("valkyrie");
    let mut child = StdCommand::new(bin_path)
        .arg("--address")
        .arg(&addr)
        .arg("--tcp-handlers")
        .arg("3")
        .arg("--shards")
        .arg("3")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn valkyrie");

    // Run the rest of the test in a catch_unwind scope so we can always clean up the child,
    // even if an assertion panics.
    let result = panic::catch_unwind(AssertUnwindSafe(|| {
        // Wait until the server starts accepting connections on the chosen port.
        let start = Instant::now();
        let mut stream = loop {
            match TcpStream::connect(&addr) {
                Ok(s) => break s,
                Err(_) => {
                    if start.elapsed() > Duration::from_secs(5) {
                        let _ = child.kill();
                        let _ = child.wait();
                        panic!("Timed out waiting for server to accept connections on {addr}");
                    }
                    thread::sleep(Duration::from_millis(50));
                }
            }
        };
        let _ = stream.set_read_timeout(Some(Duration::from_secs(3)));
        let mut reader = BufReader::new(stream.try_clone().expect("clone stream for reading"));

        // 1) PING (no argument) using RESP array: *1\r\n$4\r\nPING\r\n
        // Expect: +PONG\r\n
        stream
            .write_all(b"*1\r\n$4\r\nPING\r\n")
            .expect("write PING");
        let _ = stream.flush();

        let mut line = String::new();
        reader.read_line(&mut line).expect("read PING response");
        assert_eq!(line, "+PONG\r\n", "Unexpected PING response");

        // 2) ECHO hello using RESP array: *2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n
        // Expect bulk string: $5\r\nhello\r\n
        let msg = "hello";
        let echo_req = format!("*2\r\n$4\r\nECHO\r\n${}\r\n{}\r\n", msg.len(), msg);
        stream.write_all(echo_req.as_bytes()).expect("write ECHO");
        let _ = stream.flush();

        // Read bulk header line: $<len>\r\n
        let mut header = String::new();
        reader.read_line(&mut header).expect("read bulk header");
        assert!(
            header.starts_with('$'),
            "Bulk string header must start with '$', got: {header:?}"
        );
        let len: usize = header[1..].trim().parse().expect("parse bulk length");

        // Read <len> bytes payload
        let mut payload = vec![0u8; len];
        reader.read_exact(&mut payload).expect("read bulk payload");
        // Read trailing \r\n
        let mut terminator = [0u8; 2];
        reader
            .read_exact(&mut terminator)
            .expect("read bulk terminator");
        assert_eq!(&terminator, b"\r\n", "Bulk string not properly terminated");
        assert_eq!(
            str::from_utf8(&payload).unwrap(),
            msg,
            "Unexpected ECHO payload"
        );
    }));

    // Always cleanup the server process, even if the test body panicked.
    let _ = child.kill();
    let _ = child.wait();

    // If the body panicked, resume unwinding to fail the test.
    if let Err(e) = result {
        panic::resume_unwind(e);
    }
}
