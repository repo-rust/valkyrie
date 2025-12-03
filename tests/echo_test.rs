mod common;

use std::io::{BufRead, BufReader, Read, Write};

#[test]
fn echo_returns_same_payload() {
    // Start server with minimal resources
    let server = common::ValkyrieServerTest::start(2, 3).expect("start server");

    // Connect to server
    let mut stream = server.connect().expect("connect to server");
    let mut reader = BufReader::new(stream.try_clone().expect("clone stream for reading"));

    // Send ECHO hello as RESP array: *2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n
    let msg = "hello";
    let echo_req = format!("*2\r\n$4\r\nECHO\r\n${}\r\n{}\r\n", msg.len(), msg);
    stream
        .write_all(echo_req.as_bytes())
        .expect("write ECHO request");
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

    // Validate payload
    assert_eq!(
        std::str::from_utf8(&payload).unwrap(),
        msg,
        "Unexpected ECHO payload"
    );
}
