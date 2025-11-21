use std::io;

use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

// Per-connection handler: read into a reusable BytesMut buffer and echo back.
// Uses clear() to retain capacity and avoid reallocations on subsequent reads.
pub async fn handle_connection(mut stream: TcpStream, shard_id: usize) -> io::Result<()> {
    let mut buf = BytesMut::with_capacity(64 * 1024);

    loop {
        // Ensure some spare capacity; read_buf will grow if needed.
        buf.reserve(4096);

        let received_bytes_cnt = stream.read_buf(&mut buf).await?;

        if received_bytes_cnt == 0 {
            break;
        }

        // Convert received bytes to UTF-8 (lossy) string and print it.
        println!(
            "[shard-{}]: rcv: \"{}\", bytes: {}",
            shard_id,
            String::from_utf8_lossy(&buf),
            received_bytes_cnt
        );

        // Echo back the bytes we have; write_all guarantees full write.
        stream.write_all(&buf).await?;
        buf.clear();
    }

    // Flush is implicit for TCP; close on drop.
    Ok(())
}
