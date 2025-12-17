mod common;

use crate::common::ValkyrieClientTest;

// https://redis.io/docs/latest/commands/blpop/
// BLPOP key [key ...] timeout
//
// Behavior in this codebase (see src/command/blpop.rs and storage):
// - Returns Array [key, value] when an element is popped
// - Returns Null Array (*-1) on timeout/no keys ready
// - Error messages follow parser messages in blpop.rs
// - Operating on a string key yields error: "'<key>' is not a list."

// Non-existent key with positive timeout returns Null Array
#[test]
fn blpop_nonexistent_key_times_out_returns_null_array() {
    let server = common::ValkyrieServerTest::start(2, 3).expect("start server");
    let mut client = ValkyrieClientTest::new(server);

    // BLPOP mylist 0.05 (50ms)
    let req = "*3\r\n$5\r\nBLPOP\r\n$6\r\nmylist\r\n$4\r\n0.05\r\n";
    client.assert_command_response(req, "*-1\r\n");
}

// Pop from existing list returns [key, value]
#[test]
fn blpop_single_key_pops_head_returns_key_and_value() {
    let server = common::ValkyrieServerTest::start(2, 3).expect("start server");
    let mut client = ValkyrieClientTest::new(server);

    // RPUSH mylist a b c
    let rpush_req = "*5\r\n$5\r\nRPUSH\r\n$6\r\nmylist\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n";
    client.assert_command_response(rpush_req, ":3\r\n");

    // BLPOP mylist 1
    let blpop_req = "*3\r\n$5\r\nBLPOP\r\n$6\r\nmylist\r\n$1\r\n1\r\n";
    let blpop_resp = "*2\r\n$6\r\nmylist\r\n$1\r\na\r\n";
    client.assert_command_response(blpop_req, blpop_resp);
}

// Multi-key: returns first non-empty list name and value
#[test]
fn blpop_multiple_keys_returns_first_non_empty() {
    let server = common::ValkyrieServerTest::start(2, 3).expect("start server");
    let mut client = ValkyrieClientTest::new(server);

    // Ensure list1 is empty/non-existent, list2 has an element
    let rpush_req = "*3\r\n$5\r\nRPUSH\r\n$5\r\nlist2\r\n$1\r\nx\r\n";
    client.assert_command_response(rpush_req, ":1\r\n");

    // BLPOP list1 list2 1 -> should return [list2, x]
    let blpop_req = "*4\r\n$5\r\nBLPOP\r\n$5\r\nlist1\r\n$5\r\nlist2\r\n$1\r\n1\r\n";
    let blpop_resp = "*2\r\n$5\r\nlist2\r\n$1\r\nx\r\n";
    client.assert_command_response(blpop_req, blpop_resp);
}

// Blocking with timeout=0 unblocks when another client pushes to the list
#[test]
fn blpop_block_then_unblock_with_push_from_other_client() {
    use std::io::{BufRead, BufReader, Read, Write};
    use std::time::Duration;

    // Start server
    let server = common::ValkyrieServerTest::start(2, 3).expect("start server");

    // Open first connection that will BLPOP
    let mut c1 = server.connect().expect("c1 connect");
    c1.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
    let mut c1_reader = BufReader::new(c1.try_clone().expect("clone c1 for reader"));

    // Send BLPOP mylist 0 (block indefinitely)
    let blpop_req = "*3\r\n$5\r\nBLPOP\r\n$6\r\nmylist\r\n$1\r\n0\r\n";
    c1.write_all(blpop_req.as_bytes()).expect("write blpop");
    c1.flush().expect("flush blpop");

    // Open second connection to perform RPUSH
    let mut c2 = server.connect().expect("c2 connect");
    c2.set_read_timeout(Some(Duration::from_secs(2))).unwrap();

    // RPUSH mylist v
    let rpush_req = "*3\r\n$5\r\nRPUSH\r\n$6\r\nmylist\r\n$1\r\nv\r\n";
    c2.write_all(rpush_req.as_bytes()).expect("write rpush");
    c2.flush().expect("flush rpush");

    // Read RPUSH integer reply to avoid leaving unread bytes
    let mut rpush_reply = [0u8; 4]; // e.g., ':1\r\n'
    c2.read_exact(&mut rpush_reply).expect("read rpush reply");

    // Now c1 should be unblocked and should receive Array [\"mylist\", \"v\"]
    let mut first_line = String::new();
    c1_reader
        .read_line(&mut first_line)
        .expect("read array header");
    assert_eq!(first_line, "*2\r\n", "Expected Array of length 2");

    // Helper to read a single Bulk String from c1
    fn read_bulk(reader: &mut BufReader<std::net::TcpStream>) -> String {
        let mut header = String::new();
        reader.read_line(&mut header).expect("read bulk header");
        assert!(
            header.starts_with('$'),
            "Expected bulk string header, got: {header:?}"
        );
        let len: usize = header[1..].trim().parse().expect("parse bulk length");

        let mut payload = vec![0u8; len];
        reader.read_exact(&mut payload).expect("read bulk payload");

        let mut terminator = [0u8; 2];
        reader
            .read_exact(&mut terminator)
            .expect("read bulk terminator");
        assert_eq!(&terminator, b"\r\n");

        String::from_utf8(payload).expect("payload utf8")
    }

    let list_name = read_bulk(&mut c1_reader);
    let value = read_bulk(&mut c1_reader);
    assert_eq!(list_name, "mylist");
    assert_eq!(value, "v");
}

// Error: not enough arguments
#[test]
fn blpop_not_enough_arguments() {
    let server = common::ValkyrieServerTest::start(2, 3).expect("start server");
    let mut client = ValkyrieClientTest::new(server);

    // BLPOP
    let req = "*1\r\n$5\r\nBLPOP\r\n";
    let expected = "-Incomplete BLPOP command, expected at least 3 values: 'BLPOP key timeout'\r\n";
    client.assert_command_response(req, expected);
}

// Error: key must be BulkString
#[test]
fn blpop_key_wrong_type_integer() {
    let server = common::ValkyrieServerTest::start(2, 3).expect("start server");
    let mut client = ValkyrieClientTest::new(server);

    // BLPOP :1 1
    let req = "*3\r\n$5\r\nBLPOP\r\n:1\r\n$1\r\n1\r\n";
    client.assert_command_response(req, "-BLPOP incorrect list key, not BulkString\r\n");
}

// Error: timeout wrong type (SimpleString)
#[test]
fn blpop_timeout_wrong_type_simplestring() {
    let server = common::ValkyrieServerTest::start(2, 3).expect("start server");
    let mut client = ValkyrieClientTest::new(server);

    // BLPOP mykey +foo
    let req = "*3\r\n$5\r\nBLPOP\r\n$5\r\nmykey\r\n+foo\r\n";
    client.assert_command_response(req, "-BLPOP incorrect 'timeout' argument\r\n");
}

// Error: timeout not a number (BulkString but invalid float)
#[test]
fn blpop_timeout_invalid_nonnumeric() {
    let server = common::ValkyrieServerTest::start(2, 3).expect("start server");
    let mut client = ValkyrieClientTest::new(server);

    // BLPOP mykey abc
    let req = "*3\r\n$5\r\nBLPOP\r\n$5\r\nmykey\r\n$3\r\nabc\r\n";
    // Note: message comes from convert_float_str_seconds_to_ms() with a minor typo ('numbe')
    let expected = "-BLPOP 'timeout' must be a finite, non-negative numbe\r\n";
    client.assert_command_response(req, expected);
}

// Error: timeout negative number
#[test]
fn blpop_timeout_negative_number() {
    let server = common::ValkyrieServerTest::start(2, 3).expect("start server");
    let mut client = ValkyrieClientTest::new(server);

    // BLPOP mykey -1
    let req = "*3\r\n$5\r\nBLPOP\r\n$5\r\nmykey\r\n$2\r\n-1\r\n";
    let expected = "-BLPOP 'timeout' must be a finite, non-negative number\r\n";
    client.assert_command_response(req, expected);
}

// Error: operate on a string key
#[test]
fn blpop_on_string_key_fails() {
    let server = common::ValkyrieServerTest::start(2, 3).expect("start server");
    let mut client = ValkyrieClientTest::new(server);

    // SET skey sval
    let key = "skey";
    let value = "sval";
    let set_req = format!(
        "*3\r\n$3\r\nSET\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
        key.len(),
        key,
        value.len(),
        value
    );
    client.assert_command_response(&set_req, "+OK\r\n");

    // BLPOP skey 1 -> error
    let blpop_req = "*3\r\n$5\r\nBLPOP\r\n$4\r\nskey\r\n$1\r\n1\r\n";
    client.assert_command_response(blpop_req, "-'skey' is not a list.\r\n");
}
