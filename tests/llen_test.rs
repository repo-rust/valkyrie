mod common;

use crate::common::ValkyrieClientTest;

// https://redis.io/docs/latest/commands/llen/

// Happy path: LLEN on non-existing key returns 0
#[test]
fn llen_non_existing_key_returns_zero() {
    let server = common::ValkyrieServerTest::start(2, 3).expect("start server");
    let mut client_test = ValkyrieClientTest::new(server);

    // LLEN mylist
    let req = "*2\r\n$4\r\nLLEN\r\n$6\r\nmylist\r\n";
    client_test.assert_command_response(req, ":0\r\n");
}

// Happy path: LLEN after pushes returns correct length
#[test]
fn llen_after_pushes_returns_length() {
    let server = common::ValkyrieServerTest::start(2, 3).expect("start server");
    let mut client_test = ValkyrieClientTest::new(server);

    // RPUSH mylist a b
    let rpush_req = "*4\r\n$5\r\nRPUSH\r\n$6\r\nmylist\r\n$1\r\na\r\n$1\r\nb\r\n";
    client_test.assert_command_response(rpush_req, ":2\r\n");

    // LLEN mylist -> 2
    let llen_req = "*2\r\n$4\r\nLLEN\r\n$6\r\nmylist\r\n";
    client_test.assert_command_response(llen_req, ":2\r\n");

    // LPUSH mylist c -> list becomes [c, a, b] length 3
    let lpush_req = "*3\r\n$5\r\nLPUSH\r\n$6\r\nmylist\r\n$1\r\nc\r\n";
    client_test.assert_command_response(lpush_req, ":3\r\n");

    // LLEN mylist -> 3
    client_test.assert_command_response(llen_req, ":3\r\n");
}

// Error: LLEN on a String key should return an error
#[test]
fn llen_on_string_key_fails() {
    let server = common::ValkyrieServerTest::start(2, 3).expect("start server");
    let mut client_test = ValkyrieClientTest::new(server);

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
    client_test.assert_command_response(&set_req, "+OK\r\n");

    // LLEN skey -> error "'skey' is not a list."
    let llen_req = "*2\r\n$4\r\nLLEN\r\n$4\r\nskey\r\n";
    client_test.assert_command_response(llen_req, "-'skey' is not a list.\r\n");
}

// Error: not enough arguments
#[test]
fn llen_not_enough_arguments() {
    let server = common::ValkyrieServerTest::start(2, 3).expect("start server");
    let mut client_test = ValkyrieClientTest::new(server);

    // LLEN (no key)
    let req = "*1\r\n$4\r\nLLEN\r\n";
    client_test.assert_command_response(req, "-Not enough arguments for LLEN command\r\n");
}

// Error: key must be a BulkString
#[test]
fn llen_key_wrong_type_integer() {
    let server = common::ValkyrieServerTest::start(2, 3).expect("start server");
    let mut client_test = ValkyrieClientTest::new(server);

    // LLEN :1
    let req = "*2\r\n$4\r\nLLEN\r\n:1\r\n";
    client_test.assert_command_response(req, "-LLEN argument is not a BulkString\r\n");
}

// Command name is case-insensitive
#[test]
fn llen_case_insensitive_command_name() {
    let server = common::ValkyrieServerTest::start(2, 3).expect("start server");
    let mut client_test = ValkyrieClientTest::new(server);

    // "llen"
    let req_lower = "*2\r\n$4\r\nllen\r\n$4\r\nlist\r\n";
    client_test.assert_command_response(req_lower, ":0\r\n");

    // Prepare a list, then "LLen"
    let rpush_req = "*3\r\n$5\r\nRPUSH\r\n$4\r\nlist\r\n$1\r\na\r\n";
    client_test.assert_command_response(rpush_req, ":1\r\n");

    let req_mixed = "*2\r\n$4\r\nLLen\r\n$4\r\nlist\r\n";
    client_test.assert_command_response(req_mixed, ":1\r\n");
}
