mod common;

use crate::common::ValkyrieClientTest;
use std::thread;
use std::time::Duration;

#[test]
fn set_and_get_roundtrip_no_expiration() {
    let server = common::ValkyrieServerTest::start(1, 1).expect("start server");
    let mut client = ValkyrieClientTest::new(server);

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
    client.send(set_req.as_bytes()).expect("write SET request");

    // Expect +OK\r\n
    let line = client.read_line().expect("read SET response");
    assert_eq!(line, "+OK\r\n", "Unexpected SET response");

    // GET mykey
    let get_req = format!("*2\r\n$3\r\nGET\r\n${}\r\n{}\r\n", key.len(), key);
    client.send(get_req.as_bytes()).expect("write GET request");

    let got = client.read_bulk_or_null();
    assert_eq!(
        got.as_deref(),
        Some(value),
        "GET should return stored value"
    );
}

#[test]
fn set_with_ex_expiration_expires() {
    let server = common::ValkyrieServerTest::start(1, 1).expect("start server");
    let mut client = ValkyrieClientTest::new(server);

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
    client
        .send(set_req.as_bytes())
        .expect("write SET EX request");

    // Expect +OK\r\n
    let line = client.read_line().expect("read SET EX response");
    assert_eq!(line, "+OK\r\n", "Unexpected SET EX response");

    // Immediate GET should return value
    let get_req = format!("*2\r\n$3\r\nGET\r\n${}\r\n{}\r\n", key.len(), key);
    client.send(get_req.as_bytes()).expect("write GET request");
    let got_now = client.read_bulk_or_null();
    assert_eq!(
        got_now.as_deref(),
        Some(value),
        "GET immediately after EX should return value"
    );

    // Wait beyond expiration and GET again
    thread::sleep(Duration::from_millis(1200));
    let get_req2 = format!("*2\r\n$3\r\nGET\r\n${}\r\n{}\r\n", key.len(), key);
    client
        .send(get_req2.as_bytes())
        .expect("write GET request post-expiration");
    let got_late = client.read_bulk_or_null();
    assert!(
        got_late.is_none(),
        "GET after expiration should return Null Bulk String"
    );
}

#[test]
fn set_with_px_expiration_expires() {
    let server = common::ValkyrieServerTest::start(1, 1).expect("start server");
    let mut client = ValkyrieClientTest::new(server);

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
    client
        .send(set_req.as_bytes())
        .expect("write SET PX request");

    // Expect +OK\r\n
    let line = client.read_line().expect("read SET PX response");
    assert_eq!(line, "+OK\r\n", "Unexpected SET PX response");

    // Immediate GET should return value
    let get_req = format!("*2\r\n$3\r\nGET\r\n${}\r\n{}\r\n", key.len(), key);
    client.send(get_req.as_bytes()).expect("write GET request");
    let got_now = client.read_bulk_or_null();
    assert_eq!(
        got_now.as_deref(),
        Some(value),
        "GET immediately after PX should return value"
    );

    // Wait beyond expiration and GET again
    thread::sleep(Duration::from_millis(millis + 200));
    let get_req2 = format!("*2\r\n$3\r\nGET\r\n${}\r\n{}\r\n", key.len(), key);
    client
        .send(get_req2.as_bytes())
        .expect("write GET request post-expiration");
    let got_late = client.read_bulk_or_null();
    assert!(
        got_late.is_none(),
        "GET after PX expiration should return Null Bulk String"
    );
}
