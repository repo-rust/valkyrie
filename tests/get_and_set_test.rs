mod common;

use crate::common::ValkyrieClientTest;
use std::thread;
use std::time::Duration;

#[test]
fn set_and_get_roundtrip_no_expiration() {
    let server = common::ValkyrieServerTest::start(2, 3).expect("start server");
    let mut client_test = ValkyrieClientTest::new(server);

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
    client_test.assert_command_response(&set_req, "+OK\r\n");

    // GET mykey
    let get_req = format!("*2\r\n$3\r\nGET\r\n${}\r\n{}\r\n", key.len(), key);
    let get_resp = format!("${}\r\n{}\r\n", value.len(), value);
    client_test.assert_command_response(&get_req, &get_resp);
}

#[test]
fn set_with_ex_expiration_expires() {
    let server = common::ValkyrieServerTest::start(2, 3).expect("start server");
    let mut client_test = ValkyrieClientTest::new(server);

    let key = "ex_key";
    let value = "ex_value";
    let seconds = 1u64; // EX 1 second
    let seconds_str = seconds.to_string();

    // SET ex_key ex_value EX 1
    let set_req = format!(
        "*5\r\n$3\r\nSET\r\n${}\r\n{}\r\n${}\r\n{}\r\n$2\r\nEX\r\n${}\r\n{}\r\n",
        key.len(),
        key,
        value.len(),
        value,
        seconds_str.len(),
        seconds_str
    );
    client_test.assert_command_response(&set_req, "+OK\r\n");

    // Immediate GET should return value
    let get_req = format!("*2\r\n$3\r\nGET\r\n${}\r\n{}\r\n", key.len(), key);
    let get_resp = format!("${}\r\n{}\r\n", value.len(), value);
    client_test.assert_command_response(&get_req, &get_resp);

    // Wait beyond expiration and GET again -> Null Bulk String
    thread::sleep(Duration::from_millis(1200));
    let get_req2 = format!("*2\r\n$3\r\nGET\r\n${}\r\n{}\r\n", key.len(), key);
    client_test.assert_command_response(&get_req2, "$-1\r\n");
}

#[test]
fn set_with_px_expiration_expires() {
    let server = common::ValkyrieServerTest::start(2, 3).expect("start server");
    let mut client_test = ValkyrieClientTest::new(server);

    let key = "px_key";
    let value = "px_value";
    let millis = 300u64; // PX 300 ms
    let millis_str = millis.to_string();

    // SET px_key px_value PX 300
    let set_req = format!(
        "*5\r\n$3\r\nSET\r\n${}\r\n{}\r\n${}\r\n{}\r\n$2\r\nPX\r\n${}\r\n{}\r\n",
        key.len(),
        key,
        value.len(),
        value,
        millis_str.len(),
        millis_str
    );
    client_test.assert_command_response(&set_req, "+OK\r\n");

    // Immediate GET should return value
    let get_req = format!("*2\r\n$3\r\nGET\r\n${}\r\n{}\r\n", key.len(), key);
    let get_resp = format!("${}\r\n{}\r\n", value.len(), value);
    client_test.assert_command_response(&get_req, &get_resp);

    // Wait beyond expiration and GET again -> Null Bulk String
    thread::sleep(Duration::from_millis(millis + 200));
    let get_req2 = format!("*2\r\n$3\r\nGET\r\n${}\r\n{}\r\n", key.len(), key);
    client_test.assert_command_response(&get_req2, "$-1\r\n");
}
