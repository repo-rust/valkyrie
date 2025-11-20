use std::io;

use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

// Per-connection handler: read into a reusable BytesMut buffer and echo back.
// Uses clear() to retain capacity and avoid reallocations on subsequent reads.
pub async fn handle_connection(mut stream: TcpStream, _shard_id: usize) -> io::Result<()> {
    let mut buf = BytesMut::with_capacity(64 * 1024);

    loop {
        // Ensure some spare capacity; read_buf will grow if needed.
        buf.reserve(4096);
        let n = stream.read_buf(&mut buf).await?;
        if n == 0 {
            break;
        }

        // Echo back the bytes we have; write_all guarantees full write.
        stream.write_all(&buf).await?;
        buf.clear();
    }

    // Flush is implicit for TCP; close on drop.
    Ok(())
}
