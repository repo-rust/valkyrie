mod common;

use crate::common::ValkyrieClientTest;

// https://redis.io/docs/latest/commands/lrange/

// Error: non-existent list returns an error
#[test]
fn lrange_nonexistent_list_returns_error() {
    let server = common::ValkyrieServerTest::start(2, 3).expect("start server");
    let mut client_test = ValkyrieClientTest::new(server);

    // LRANGE mylist 0 -1
    let req = "*4\r\n$6\r\nLRANGE\r\n$6\r\nmylist\r\n$1\r\n0\r\n$2\r\n-1\r\n";
    client_test.assert_command_response(req, "-No list found with name 'mylist'\r\n");
}

// Error: running LRANGE on a String key should fail
#[test]
fn lrange_on_string_key_fails() {
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

    // LRANGE skey 0 -1 -> error
    let lrange_req = "*4\r\n$6\r\nLRANGE\r\n$4\r\nskey\r\n$1\r\n0\r\n$2\r\n-1\r\n";
    client_test.assert_command_response(lrange_req, "-'skey' is not a list.\r\n");
}

// Happy path: typical ranges (positive, negative, out-of-bounds)
#[test]
fn lrange_basic_ranges() {
    let server = common::ValkyrieServerTest::start(2, 3).expect("start server");
    let mut client_test = ValkyrieClientTest::new(server);

    // Prepare: RPUSH list a bb ccc  -> 3
    let push_req = "*5\r\n$5\r\nRPUSH\r\n$4\r\nlist\r\n$1\r\na\r\n$2\r\nbb\r\n$3\r\nccc\r\n";
    client_test.assert_command_response(push_req, ":3\r\n");

    // LRANGE list 0 -1 -> [a, bb, ccc]
    let req_all = "*4\r\n$6\r\nLRANGE\r\n$4\r\nlist\r\n$1\r\n0\r\n$2\r\n-1\r\n";
    let resp_all = "*3\r\n$1\r\na\r\n$2\r\nbb\r\n$3\r\nccc\r\n";
    client_test.assert_command_response(req_all, resp_all);

    // LRANGE list 0 1 -> [a, bb]
    let req_0_1 = "*4\r\n$6\r\nLRANGE\r\n$4\r\nlist\r\n$1\r\n0\r\n$1\r\n1\r\n";
    let resp_0_1 = "*2\r\n$1\r\na\r\n$2\r\nbb\r\n";
    client_test.assert_command_response(req_0_1, resp_0_1);

    // LRANGE list 1 10 -> [bb, ccc] (end clamped)
    let req_1_10 = "*4\r\n$6\r\nLRANGE\r\n$4\r\nlist\r\n$1\r\n1\r\n$2\r\n10\r\n";
    let resp_1_10 = "*2\r\n$2\r\nbb\r\n$3\r\nccc\r\n";
    client_test.assert_command_response(req_1_10, resp_1_10);

    // LRANGE list 5 10 -> [] (start beyond end)
    let req_5_10 = "*4\r\n$6\r\nLRANGE\r\n$4\r\nlist\r\n$1\r\n5\r\n$2\r\n10\r\n";
    client_test.assert_command_response(req_5_10, "*0\r\n");

    // LRANGE list -2 -1 -> [bb, ccc] (negative indices)
    let req_neg = "*4\r\n$6\r\nLRANGE\r\n$4\r\nlist\r\n$2\r\n-2\r\n$2\r\n-1\r\n";
    let resp_neg = "*2\r\n$2\r\nbb\r\n$3\r\nccc\r\n";
    client_test.assert_command_response(req_neg, resp_neg);
}

// Error: not enough arguments
#[test]
fn lrange_not_enough_arguments() {
    let server = common::ValkyrieServerTest::start(2, 3).expect("start server");
    let mut client_test = ValkyrieClientTest::new(server);

    // LRANGE (no args)
    let req0 = "*1\r\n$6\r\nLRANGE\r\n";
    client_test.assert_command_response(req0, "-Not enough arguments for LRANGE command\r\n");

    // LRANGE key start (missing end)
    let req1 = "*3\r\n$6\r\nLRANGE\r\n$3\r\nkey\r\n$1\r\n0\r\n";
    client_test.assert_command_response(req1, "-Not enough arguments for LRANGE command\r\n");
}

// Error: incorrect parameter types and parsing errors
#[test]
fn lrange_wrong_types_and_parse_errors() {
    let server = common::ValkyrieServerTest::start(2, 3).expect("start server");
    let mut client_test = ValkyrieClientTest::new(server);

    // Key is Integer instead of BulkString
    let wrong_key = "*4\r\n$6\r\nLRANGE\r\n:1\r\n$1\r\n0\r\n$2\r\n-1\r\n";
    client_test.assert_command_response(
        wrong_key,
        "-LRANGE incorrect parameter types, expected BulkString, BulkString, BulkString\r\n",
    );

    // start is not an integer (parse error)
    let bad_start = "*4\r\n$6\r\nLRANGE\r\n$2\r\nmy\r\n$1\r\na\r\n$2\r\n-1\r\n";
    client_test.assert_command_response(
        bad_start,
        "-Failed to parse LRANGE start parameter 'a' as integer\r\n",
    );

    // end is not an integer (parse error)
    let bad_end = "*4\r\n$6\r\nLRANGE\r\n$2\r\nmy\r\n$1\r\n0\r\n$1\r\nb\r\n";
    client_test.assert_command_response(
        bad_end,
        "-Failed to parse LRANGE end parameter 'b' as integer\r\n",
    );
}

// Command name is case-insensitive
#[test]
fn lrange_case_insensitive_command_name() {
    let server = common::ValkyrieServerTest::start(2, 3).expect("start server");
    let mut client_test = ValkyrieClientTest::new(server);

    // Prepare: RPUSH list a bb ccc  -> 3
    let push_req = "*5\r\n$5\r\nRPUSH\r\n$4\r\nlist\r\n$1\r\na\r\n$2\r\nbb\r\n$3\r\nccc\r\n";
    client_test.assert_command_response(push_req, ":3\r\n");

    // "lrange"
    let req_lower = "*4\r\n$6\r\nlrange\r\n$4\r\nlist\r\n$1\r\n0\r\n$2\r\n-1\r\n";
    let resp_all = "*3\r\n$1\r\na\r\n$2\r\nbb\r\n$3\r\nccc\r\n";
    client_test.assert_command_response(req_lower, resp_all);

    // "LRange"
    let req_mixed = "*4\r\n$6\r\nLRange\r\n$4\r\nlist\r\n$1\r\n0\r\n$2\r\n-1\r\n";
    client_test.assert_command_response(req_mixed, resp_all);
}
