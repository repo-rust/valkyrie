mod common;

use std::io::{BufRead, BufReader, Read, Write};
use std::thread;
use std::time::Duration;

/// Read a RESP Bulk String or Null Bulk String from the reader.
/// - Returns Some(String) when a Bulk String is received
/// - Returns None when a Null Bulk String ($-1) is received
fn read_bulk_or_null(reader: &mut BufReader<std::net::TcpStream>) -> Option<String> {
    // Read header line: either "$<len>\r\n" or "$-1\r\n"
    let mut header = String::new();
    reader.read_line(&mut header).expect("read header");
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
    reader.read_exact(&mut payload).expect("read payload");

    // Read trailing \r\n
    let mut terminator = [0u8; 2];
    reader
        .read_exact(&mut terminator)
        .expect("read bulk terminator");
    assert_eq!(&terminator, b"\r\n", "Bulk string not properly terminated");

    Some(String::from_utf8(payload).expect("payload utf8"))
}

#[test]
fn set_and_get_roundtrip_no_expiration() {
    let server = common::ValkyrieServerTest::start(1, 1).expect("start server");
    let mut stream = server.connect().expect("connect to server");
    let mut reader = BufReader::new(stream.try_clone().expect("clone stream"));

    let key = "mykey";
    let value = "myvalue";

    // SET mykey myvalue
    let set_req = format!(
        "*3\r\n$3\r\nSET\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
        key.len(),
        key,
        value.len(),
        value
    );
    stream
        .write_all(set_req.as_bytes())
        .expect("write SET request");
    let _ = stream.flush();

    // Expect +OK\r\n
    let mut line = String::new();
    reader.read_line(&mut line).expect("read SET response");
    assert_eq!(line, "+OK\r\n", "Unexpected SET response");

    // GET mykey
    let get_req = format!("*2\r\n$3\r\nGET\r\n${}\r\n{}\r\n", key.len(), key);
    stream
        .write_all(get_req.as_bytes())
        .expect("write GET request");
    let _ = stream.flush();

    let got = read_bulk_or_null(&mut reader);
    assert_eq!(
        got.as_deref(),
        Some(value),
        "GET should return stored value"
    );
}

#[test]
fn set_with_ex_expiration_expires() {
    let server = common::ValkyrieServerTest::start(1, 1).expect("start server");
    let mut stream = server.connect().expect("connect to server");
    let mut reader = BufReader::new(stream.try_clone().expect("clone stream"));

    let key = "ex_key";
    let value = "ex_value";
    let seconds = 1; // EX 1 second

    // SET ex_key ex_value EX 1
    let set_req = format!(
        "*5\r\n$3\r\nSET\r\n${}\r\n{}\r\n${}\r\n{}\r\n$2\r\nEX\r\n${}\r\n{}\r\n",
        key.len(),
        key,
        value.len(),
        value,
        seconds.to_string().len(),
        seconds
    );
    stream
        .write_all(set_req.as_bytes())
        .expect("write SET EX request");
    let _ = stream.flush();

    // Expect +OK\r\n
    let mut line = String::new();
    reader.read_line(&mut line).expect("read SET EX response");
    assert_eq!(line, "+OK\r\n", "Unexpected SET EX response");

    // Immediate GET should return value
    let get_req = format!("*2\r\n$3\r\nGET\r\n${}\r\n{}\r\n", key.len(), key);
    stream
        .write_all(get_req.as_bytes())
        .expect("write GET request");
    let _ = stream.flush();
    let got_now = read_bulk_or_null(&mut reader);
    assert_eq!(
        got_now.as_deref(),
        Some(value),
        "GET immediately after EX should return value"
    );

    // Wait beyond expiration and GET again
    thread::sleep(Duration::from_millis(1200));
    let get_req2 = format!("*2\r\n$3\r\nGET\r\n${}\r\n{}\r\n", key.len(), key);
    stream
        .write_all(get_req2.as_bytes())
        .expect("write GET request post-expiration");
    let _ = stream.flush();
    let got_late = read_bulk_or_null(&mut reader);
    assert!(
        got_late.is_none(),
        "GET after expiration should return Null Bulk String"
    );
}

#[test]
fn set_with_px_expiration_expires() {
    let server = common::ValkyrieServerTest::start(1, 1).expect("start server");
    let mut stream = server.connect().expect("connect to server");
    let mut reader = BufReader::new(stream.try_clone().expect("clone stream"));

    let key = "px_key";
    let value = "px_value";
    let millis = 300u64; // PX 300 ms

    // SET px_key px_value PX 300
    let set_req = format!(
        "*5\r\n$3\r\nSET\r\n${}\r\n{}\r\n${}\r\n{}\r\n$2\r\nPX\r\n${}\r\n{}\r\n",
        key.len(),
        key,
        value.len(),
        value,
        millis.to_string().len(),
        millis
    );
    stream
        .write_all(set_req.as_bytes())
        .expect("write SET PX request");
    let _ = stream.flush();

    // Expect +OK\r\n
    let mut line = String::new();
    reader.read_line(&mut line).expect("read SET PX response");
    assert_eq!(line, "+OK\r\n", "Unexpected SET PX response");

    // Immediate GET should return value
    let get_req = format!("*2\r\n$3\r\nGET\r\n${}\r\n{}\r\n", key.len(), key);
    stream
        .write_all(get_req.as_bytes())
        .expect("write GET request");
    let _ = stream.flush();
    let got_now = read_bulk_or_null(&mut reader);
    assert_eq!(
        got_now.as_deref(),
        Some(value),
        "GET immediately after PX should return value"
    );

    // Wait beyond expiration and GET again
    thread::sleep(Duration::from_millis(millis + 200));
    let get_req2 = format!("*2\r\n$3\r\nGET\r\n${}\r\n{}\r\n", key.len(), key);
    stream
        .write_all(get_req2.as_bytes())
        .expect("write GET request post-expiration");
    let _ = stream.flush();
    let got_late = read_bulk_or_null(&mut reader);
    assert!(
        got_late.is_none(),
        "GET after PX expiration should return Null Bulk String"
    );
}
