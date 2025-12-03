mod common;

use std::io::{BufRead, BufReader, Write};

#[test]
fn ping_no_arg_returns_pong() {
    // Start server with minimal resources
    let server = common::ValkyrieServerTest::start(2, 3).expect("start server");

    // Connect to server
    let mut stream = server.connect().expect("connect to server");
    let mut reader = BufReader::new(stream.try_clone().expect("clone stream for reading"));

    // Send RESP array: *1\r\n$4\r\nPING\r\n
    stream
        .write_all(b"*1\r\n$4\r\nPING\r\n")
        .expect("write PING");
    let _ = stream.flush();

    // Expect: +PONG\r\n
    let mut line = String::new();
    reader.read_line(&mut line).expect("read PING response");
    assert_eq!(line, "+PONG\r\n", "Unexpected PING response");
}
