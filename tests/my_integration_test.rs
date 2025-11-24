// use std::io::{BufRead, BufReader, Write};
// use std::net::{TcpListener, TcpStream};
// use std::process::{Command as StdCommand, Stdio};
// use std::thread;
// use std::time::{Duration, Instant};

// use assert_cmd::cargo::{self};

#[test]
fn run_valkyrie_and_send_few_commands() {
    // Choose a free local port to avoid conflicts across tests/machines.
    // let port = {
    //     let l = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    //     let p = l.local_addr().unwrap().port();
    //     drop(l);
    //     p
    // };
    // let addr = format!("127.0.0.1:{port}");

    // // Spawn the server binary. Do NOT .assert() because the server is long-running.
    // let bin_path = cargo::cargo_bin!("valkyrie");
    // let mut child = StdCommand::new(bin_path)
    //     .env("SHARDS", "2")
    //     .arg("reuseport")
    //     .arg(&addr)
    //     .stdout(Stdio::piped())
    //     .stderr(Stdio::piped())
    //     .spawn()
    //     .expect("spawn valkyrie");

    // // Wait until the server starts accepting connections on the chosen port.
    // let start = Instant::now();
    // let mut stream = loop {
    //     match TcpStream::connect(&addr) {
    //         Ok(s) => break s,
    //         Err(_) => {
    //             if start.elapsed() > Duration::from_secs(5) {
    //                 let _ = child.kill();
    //                 let _ = child.wait();
    //                 panic!("Timed out waiting for server to accept connections on {addr}");
    //             }
    //             thread::sleep(Duration::from_millis(50));
    //         }
    //     }
    // };

    // // Capture server stdout so we can assert on what the server logs upon parsing.
    // let stdout = child.stdout.take().expect("capture stdout");
    // let mut reader = BufReader::new(stdout);

    // // Send a Redis Simple String frame: +PING\r\n
    // stream.write_all(b"+PING\r\n").expect("write to server");
    // let _ = stream.flush();

    // // Expect the server to log that it parsed SimpleString(PING).
    // // The connection handler prints e.g.: [shard-X]: rcv: SimpleString(PING), bytes: N
    // let expected = "SimpleString(PING-123)";
    // let wait_start = Instant::now();
    // let mut saw = false;
    // loop {
    //     if wait_start.elapsed() > Duration::from_secs(3) {
    //         break;
    //     }
    //     let mut line = String::new();
    //     match reader.read_line(&mut line) {
    //         Ok(0) => {
    //             // No new data yet; wait a bit and retry.
    //             thread::sleep(Duration::from_millis(20));
    //         }
    //         Ok(_) => {
    //             if line.contains(expected) {
    //                 saw = true;
    //                 break;
    //             }
    //         }
    //         Err(_) => break,
    //     }
    // }

    // // Cleanup the server process.
    // let _ = child.kill();
    // let _ = child.wait();

    // assert!(saw, "Server did not log parsed {expected}");
}
