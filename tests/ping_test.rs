mod common;

use crate::common::ValkyrieClientTest;

//
// https://redis.io/docs/latest/commands/ping/
//
#[test]
fn ping_no_arguments() {
    let server = common::ValkyrieServerTest::start(2, 3).expect("start server");

    let mut client_test = ValkyrieClientTest::new(server);

    client_test.assert_command_response2("*1\r\n$4\r\nPING\r\n", "+PONG\r\n");
}

#[test]
fn ping_with_arguments() {
    let server = common::ValkyrieServerTest::start(2, 3).expect("start server");

    let mut client_test = ValkyrieClientTest::new(server);

    client_test
        .send("*2\r\n$4\r\nPING\r\n$5\r\nWorld\r\n".as_bytes())
        .expect("write ECHO request");

    let payload = client_test
        .read_bulk_or_null()
        .expect("expected bulk string");

    assert_eq!(payload, "World", "Unexpected ECHO payload");
}
