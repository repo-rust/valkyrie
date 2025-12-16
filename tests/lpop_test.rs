mod common;

use crate::common::ValkyrieClientTest;

// https://redis.io/docs/latest/commands/lpop/

// Null reply when key does not exist (no count)
#[test]
fn lpop_nonexistent_key_no_count_returns_null() {
    let server = common::ValkyrieServerTest::start(2, 3).expect("start server");
    let mut client = ValkyrieClientTest::new(server);

    // LPOP mylist
    let req = "*2\r\n$4\r\nLPOP\r\n$6\r\nmylist\r\n";
    client.assert_command_response(req, "$-1\r\n");
}

// Null reply (Null Array) when key does not exist and count is provided
#[test]
fn lpop_nonexistent_key_with_count_returns_null_array() {
    let server = common::ValkyrieServerTest::start(2, 3).expect("start server");
    let mut client = ValkyrieClientTest::new(server);

    // LPOP mylist 2
    let req = "*3\r\n$4\r\nLPOP\r\n$6\r\nmylist\r\n$1\r\n2\r\n";
    client.assert_command_response(req, "*-1\r\n");
}

// Pop single element from head
#[test]
fn lpop_single_element() {
    let server = common::ValkyrieServerTest::start(2, 3).expect("start server");
    let mut client = ValkyrieClientTest::new(server);

    // RPUSH mylist a b c
    let rpush_req = "*5\r\n$5\r\nRPUSH\r\n$6\r\nmylist\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n";
    client.assert_command_response(rpush_req, ":3\r\n");

    // LPOP mylist -> "a"
    let lpop_req = "*2\r\n$4\r\nLPOP\r\n$6\r\nmylist\r\n";
    let lpop_resp = "$1\r\na\r\n";
    client.assert_command_response(lpop_req, lpop_resp);
}

// Pop multiple elements from head with count
#[test]
fn lpop_multiple_elements_with_count() {
    let server = common::ValkyrieServerTest::start(2, 3).expect("start server");
    let mut client = ValkyrieClientTest::new(server);

    // RPUSH nums 1 2 3
    let rpush_req = "*5\r\n$5\r\nRPUSH\r\n$4\r\nnums\r\n$1\r\n1\r\n$1\r\n2\r\n$1\r\n3\r\n";
    client.assert_command_response(rpush_req, ":3\r\n");

    // LPOP nums 2 -> [1, 2]
    let lpop_req = "*3\r\n$4\r\nLPOP\r\n$4\r\nnums\r\n$1\r\n2\r\n";
    let lpop_resp = "*2\r\n$1\r\n1\r\n$1\r\n2\r\n";
    client.assert_command_response(lpop_req, lpop_resp);
}

// Pop with count larger than list length -> returns only available elements
#[test]
fn lpop_count_larger_than_length() {
    let server = common::ValkyrieServerTest::start(2, 3).expect("start server");
    let mut client = ValkyrieClientTest::new(server);

    // RPUSH xs a b
    let rpush_req = "*4\r\n$5\r\nRPUSH\r\n$2\r\nxs\r\n$1\r\na\r\n$1\r\nb\r\n";
    client.assert_command_response(rpush_req, ":2\r\n");

    // LPOP xs 10 -> [a, b]
    let lpop_req = "*3\r\n$4\r\nLPOP\r\n$2\r\nxs\r\n$2\r\n10\r\n";
    let lpop_resp = "*2\r\n$1\r\na\r\n$1\r\nb\r\n";
    client.assert_command_response(lpop_req, lpop_resp);

    // LPOP xs (now empty) -> $-1
    let lpop_again = "*2\r\n$4\r\nLPOP\r\n$2\r\nxs\r\n";
    client.assert_command_response(lpop_again, "$-1\r\n");
}

// Count = 0 -> empty array
#[test]
fn lpop_count_zero_returns_empty_array() {
    let server = common::ValkyrieServerTest::start(2, 3).expect("start server");
    let mut client = ValkyrieClientTest::new(server);

    // RPUSH q v1
    let rpush_req = "*3\r\n$5\r\nRPUSH\r\n$1\r\nq\r\n$2\r\nv1\r\n";
    client.assert_command_response(rpush_req, ":1\r\n");

    // LPOP q 0 -> *0
    let lpop_zero = "*3\r\n$4\r\nLPOP\r\n$1\r\nq\r\n$1\r\n0\r\n";
    client.assert_command_response(lpop_zero, "*0\r\n");
}

// Error: not enough arguments
#[test]
fn lpop_not_enough_arguments() {
    let server = common::ValkyrieServerTest::start(2, 3).expect("start server");
    let mut client = ValkyrieClientTest::new(server);

    // LPOP (no key)
    let req = "*1\r\n$4\r\nLPOP\r\n";
    client.assert_command_response(req, "-Not enough arguments for LPOP command\r\n");
}

// Error: key must be BulkString
#[test]
fn lpop_key_wrong_type_integer() {
    let server = common::ValkyrieServerTest::start(2, 3).expect("start server");
    let mut client = ValkyrieClientTest::new(server);

    // LPOP :1
    let req = "*2\r\n$4\r\nLPOP\r\n:1\r\n";
    client.assert_command_response(req, "-LPOP key is not BulkString\r\n");
}

// Error: count wrong type (SimpleString)
#[test]
fn lpop_count_wrong_type_simplestring() {
    let server = common::ValkyrieServerTest::start(2, 3).expect("start server");
    let mut client = ValkyrieClientTest::new(server);

    // LPOP mykey +foo
    let req = "*3\r\n$4\r\nLPOP\r\n$5\r\nmykey\r\n+foo\r\n";
    client.assert_command_response(req, "-LPOP count is not BulkString\r\n");
}

// Error: operate on a string key
#[test]
fn lpop_on_string_key_fails() {
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

    // LPOP skey -> error
    let lpop_req = "*2\r\n$4\r\nLPOP\r\n$4\r\nskey\r\n";
    client.assert_command_response(lpop_req, "-'skey' is not a list.\r\n");
}
