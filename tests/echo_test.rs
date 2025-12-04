mod common;

use crate::common::ValkyrieClientTest;

#[test]
fn echo_returns_same_payload() {
    // Start server with minimal resources
    let server = common::ValkyrieServerTest::start(2, 3).expect("start server");

    // Use test client helper
    let mut client = ValkyrieClientTest::new(server);

    // Send ECHO hello as RESP array: *2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n
    let msg = "hello";
    let echo_req = format!("*2\r\n$4\r\nECHO\r\n${}\r\n{}\r\n", msg.len(), msg);
    client
        .send(echo_req.as_bytes())
        .expect("write ECHO request");

    // Read bulk string response and validate payload
    let payload = client.read_bulk_or_null().expect("expected bulk string");
    assert_eq!(payload, msg, "Unexpected ECHO payload");
}
