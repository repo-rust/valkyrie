mod common;

use crate::common::ValkyrieClientTest;

// https://redis.io/docs/latest/commands/rpush/

// Happy path: single BulkString value to a new list -> length 1
#[test]
fn rpush_single_value_new_list() {
    let server = common::ValkyrieServerTest::start(2, 3).expect("start server");
    let mut client_test = ValkyrieClientTest::new(server);

    // RPUSH mylist one
    let req = "*3\r\n$5\r\nRPUSH\r\n$6\r\nmylist\r\n$3\r\none\r\n";
    client_test.assert_command_response(req, ":1\r\n");
}

// Happy path: multiple BulkString values to a new list -> length equals number of values
#[test]
fn rpush_multiple_values_new_list() {
    let server = common::ValkyrieServerTest::start(2, 3).expect("start server");
    let mut client_test = ValkyrieClientTest::new(server);

    // RPUSH mylist a bb
    let req = "*4\r\n$5\r\nRPUSH\r\n$6\r\nmylist\r\n$1\r\na\r\n$2\r\nbb\r\n";
    client_test.assert_command_response(req, ":2\r\n");
}

// Happy path: integer elements are accepted as values (converted to strings)
#[test]
fn rpush_integer_values_accepted() {
    let server = common::ValkyrieServerTest::start(2, 3).expect("start server");
    let mut client_test = ValkyrieClientTest::new(server);

    // RPUSH num :1 :2
    let req = "*4\r\n$5\r\nRPUSH\r\n$3\r\nnum\r\n:1\r\n:2\r\n";
    client_test.assert_command_response(req, ":2\r\n");
}

// Happy path: mixed BulkString and Integer values
#[test]
fn rpush_mixed_bulk_and_integer_values() {
    let server = common::ValkyrieServerTest::start(2, 3).expect("start server");
    let mut client_test = ValkyrieClientTest::new(server);

    // RPUSH mixlist one 2 three
    let req = "*5\r\n$5\r\nRPUSH\r\n$7\r\nmixlist\r\n$3\r\none\r\n:2\r\n$5\r\nthree\r\n";
    client_test.assert_command_response(req, ":3\r\n");
}

// Happy path: empty BulkString value is allowed
#[test]
fn rpush_empty_string_value() {
    let server = common::ValkyrieServerTest::start(2, 3).expect("start server");
    let mut client_test = ValkyrieClientTest::new(server);

    // RPUSH emptys ""
    let req = "*3\r\n$5\r\nRPUSH\r\n$6\r\nemptys\r\n$0\r\n\r\n";
    client_test.assert_command_response(req, ":1\r\n");
}

// Happy path: cumulative pushes increase length
#[test]
fn rpush_on_existing_list_increases_length() {
    let server = common::ValkyrieServerTest::start(2, 3).expect("start server");
    let mut client_test = ValkyrieClientTest::new(server);

    // RPUSH mylist x y  -> 2
    let req1 = "*4\r\n$5\r\nRPUSH\r\n$6\r\nmylist\r\n$1\r\nx\r\n$1\r\ny\r\n";
    client_test.assert_command_response(req1, ":2\r\n");

    // RPUSH mylist z    -> 3
    let req2 = "*3\r\n$5\r\nRPUSH\r\n$6\r\nmylist\r\n$1\r\nz\r\n";
    client_test.assert_command_response(req2, ":3\r\n");
}

// Interop: GET on a list key should return Null Bulk String (lists are not returned by GET)
#[test]
fn rpush_then_get_returns_null_bulk_string() {
    let server = common::ValkyrieServerTest::start(2, 3).expect("start server");
    let mut client_test = ValkyrieClientTest::new(server);

    // RPUSH lkey v1
    let req = "*3\r\n$5\r\nRPUSH\r\n$4\r\nlkey\r\n$2\r\nv1\r\n";
    client_test.assert_command_response(req, ":1\r\n");

    // GET lkey -> $-1
    let get_req = "*2\r\n$3\r\nGET\r\n$4\r\nlkey\r\n";
    client_test.assert_command_response(get_req, "$-1\r\n");
}

// Error: not enough arguments (requires at least key and one value)
#[test]
fn rpush_not_enough_arguments_only_command() {
    let server = common::ValkyrieServerTest::start(2, 3).expect("start server");
    let mut client_test = ValkyrieClientTest::new(server);

    // RPUSH (no key, no values)
    let req = "*1\r\n$5\r\nRPUSH\r\n";
    client_test.assert_command_response(req, "-Not enough arguments for RPUSH command\r\n");
}

// Error: not enough arguments (has key, but no values)
#[test]
fn rpush_not_enough_arguments_only_key() {
    let server = common::ValkyrieServerTest::start(2, 3).expect("start server");
    let mut client_test = ValkyrieClientTest::new(server);

    // RPUSH mylist (no values)
    let req = "*2\r\n$5\r\nRPUSH\r\n$6\r\nmylist\r\n";
    client_test.assert_command_response(req, "-Not enough arguments for RPUSH command\r\n");
}

// Error: key must be a BulkString (Integer provided instead)
#[test]
fn rpush_key_wrong_type_integer() {
    let server = common::ValkyrieServerTest::start(2, 3).expect("start server");
    let mut client_test = ValkyrieClientTest::new(server);

    // RPUSH :1 value
    let req = "*3\r\n$5\r\nRPUSH\r\n:1\r\n$5\r\nvalue\r\n";
    client_test.assert_command_response(req, "-RPUSH key is not BulkString\r\n");
}

// Error: value is neither BulkString nor Integer (SimpleString used)
#[test]
fn rpush_wrong_value_type_simplestring() {
    let server = common::ValkyrieServerTest::start(2, 3).expect("start server");
    let mut client_test = ValkyrieClientTest::new(server);

    // RPUSH mykey +foo
    let req = "*3\r\n$5\r\nRPUSH\r\n$5\r\nmykey\r\n+foo\r\n";
    client_test.assert_command_response(req, "-RPUSH argument is not BulkString or Integer\r\n");
}

// Error: value Null Bulk String is not accepted by current parser/command logic
#[test]
fn rpush_wrong_value_type_null_bulk_string() {
    let server = common::ValkyrieServerTest::start(2, 3).expect("start server");
    let mut client_test = ValkyrieClientTest::new(server);

    // RPUSH mykey $-1
    let req = "*3\r\n$5\r\nRPUSH\r\n$5\r\nmykey\r\n$-1\r\n";
    client_test.assert_command_response(req, "-RPUSH argument is not BulkString or Integer\r\n");
}

// Error: pushing into a key that holds a String value
#[test]
fn rpush_on_string_key_fails() {
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

    // RPUSH skey v1 -> error
    let rpush_req = "*3\r\n$5\r\nRPUSH\r\n$4\r\nskey\r\n$2\r\nv1\r\n";
    client_test.assert_command_response(
        rpush_req,
        "-Can't execute Right Push for a String value, should be List\r\n",
    );
}

// Command name is case-insensitive
#[test]
fn rpush_case_insensitive_command_name() {
    let server = common::ValkyrieServerTest::start(2, 3).expect("start server");
    let mut client_test = ValkyrieClientTest::new(server);

    // "rpush"
    let req_lower = "*3\r\n$5\r\nrpush\r\n$4\r\nlist\r\n$1\r\na\r\n";
    client_test.assert_command_response(req_lower, ":1\r\n");

    // "RPush"
    let req_mixed = "*3\r\n$5\r\nRPush\r\n$4\r\nlist\r\n$1\r\nb\r\n";
    client_test.assert_command_response(req_mixed, ":2\r\n");
}
