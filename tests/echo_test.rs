mod common;

use crate::common::ValkyrieClientTest;

///
/// https://redis.io/docs/latest/commands/echo/
///
/// Happy path: BulkString argument echoes back as BulkString
#[test]
fn echo_with_argument() {
    let server = common::ValkyrieServerTest::start(2, 3).expect("start server");

    let mut client_test = ValkyrieClientTest::new(server);

    // ECHO hello as RESP array: *2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n
    let msg = "hello";
    let echo_req = format!("*2\r\n$4\r\nECHO\r\n${}\r\n{}\r\n", msg.len(), msg);
    let echo_response = format!("${}\r\n{}\r\n", msg.len(), msg);

    client_test.assert_command_response(&echo_req, &echo_response);
}

/// Error: no argument
#[test]
fn echo_no_argument() {
    let server = common::ValkyrieServerTest::start(2, 3).expect("start server");

    let mut client_test = ValkyrieClientTest::new(server);

    client_test
        .assert_command_response("*1\r\n$4\r\nECHO\r\n", "-No argument for ECHO command\r\n");
}

/// Happy path: empty BulkString echoes empty BulkString
#[test]
fn echo_empty_string_argument() {
    let server = common::ValkyrieServerTest::start(2, 3).expect("start server");
    let mut client_test = ValkyrieClientTest::new(server);

    // $0\r\n\r\n is an empty bulk string
    client_test.assert_command_response("*2\r\n$4\r\nECHO\r\n$0\r\n\r\n", "$0\r\n\r\n");
}

/// Error: too many arguments -> matches parser's "No argument ..." for len != 2
#[test]
fn echo_too_many_arguments() {
    let server = common::ValkyrieServerTest::start(2, 3).expect("start server");
    let mut client_test = ValkyrieClientTest::new(server);

    // ECHO hello !
    client_test.assert_command_response(
        "*3\r\n$4\r\nECHO\r\n$5\r\nhello\r\n$1\r\n!\r\n",
        "-No argument for ECHO command\r\n",
    );
}

/// Error: wrong type for argument (Integer instead of BulkString)
#[test]
fn echo_wrong_type_integer_argument() {
    let server = common::ValkyrieServerTest::start(2, 3).expect("start server");
    let mut client_test = ValkyrieClientTest::new(server);

    client_test.assert_command_response(
        "*2\r\n$4\r\nECHO\r\n:1\r\n",
        "-ECHO argument is not a BulkString\r\n",
    );
}

/// Error: null bulk string as argument is not accepted by current parser
#[test]
fn echo_null_bulk_string_argument() {
    let server = common::ValkyrieServerTest::start(2, 3).expect("start server");
    let mut client_test = ValkyrieClientTest::new(server);

    client_test.assert_command_response(
        "*2\r\n$4\r\nECHO\r\n$-1\r\n",
        "-ECHO argument is not a BulkString\r\n",
    );
}

/// Command name is case-insensitive
#[test]
fn echo_case_insensitive_command_name() {
    let server = common::ValkyrieServerTest::start(2, 3).expect("start server");
    let mut client_test = ValkyrieClientTest::new(server);

    // "echo"
    client_test.assert_command_response("*2\r\n$4\r\necho\r\n$5\r\nworld\r\n", "$5\r\nworld\r\n");

    // "Echo"
    client_test.assert_command_response("*2\r\n$4\r\nEcho\r\n$1\r\n!\r\n", "$1\r\n!\r\n");
}
