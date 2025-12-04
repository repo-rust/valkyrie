use bytes::BytesMut;
use tokio::{io::AsyncWriteExt, net::TcpStream};

#[derive(Debug, PartialEq)]
pub enum RedisType {
    SimpleString(String),
    BulkString(String),
    NullBulkString,
    Array(Vec<RedisType>),
    NullArray,
    Integer(i32),
    InvalidType(String),
    SimpleError(String),
    #[allow(dead_code)]
    Null,
}

/// Trait to encode a RedisType into RESP bytes.
pub trait ToRespBytes {
    /// Encode the value into a newly allocated Vec<u8> containing the RESP2/RESP3 bytes.
    fn to_resp_bytes(&self) -> Vec<u8>;
}

impl From<&str> for RedisType {
    fn from(value: &str) -> Self {
        let mut forward_buf = ForwardBuf {
            buf: &BytesMut::from(value.as_bytes()),
            offset: 0,
        };
        try_parse_type_forward(&mut forward_buf).expect("")
    }
}

impl From<String> for RedisType {
    fn from(value: String) -> Self {
        value.as_str().into()
    }
}

const RESP_TERMINATOR: &[u8] = b"\r\n";

impl ToRespBytes for RedisType {
    // TODO: Check if we can use externally allocated `let mut out_buf = BytesMut::with_capacity(10 * 1024);`
    // here instead of creating Vector every time.
    fn to_resp_bytes(&self) -> Vec<u8> {
        match self {
            // -Error message\r\n
            RedisType::SimpleError(error_msg) => {
                let mut v = Vec::with_capacity(1 + error_msg.len() + 2);
                v.extend_from_slice(b"-");
                v.extend_from_slice(error_msg.as_bytes());
                v.extend_from_slice(RESP_TERMINATOR);
                v
            }

            RedisType::SimpleString(s) => {
                let mut v = Vec::with_capacity(1 + s.len() + 2);
                v.push(b'+');
                v.extend_from_slice(s.as_bytes());
                v.extend_from_slice(RESP_TERMINATOR);
                v
            }
            RedisType::BulkString(s) => {
                let data = s.as_bytes();
                let len = data.len().to_string();
                let mut v = Vec::with_capacity(1 + len.len() + 2 + data.len() + 2);
                v.push(b'$');
                v.extend_from_slice(len.as_bytes());
                v.extend_from_slice(RESP_TERMINATOR);
                v.extend_from_slice(data);
                v.extend_from_slice(RESP_TERMINATOR);
                v
            }
            RedisType::NullBulkString => b"$-1\r\n".to_vec(),
            RedisType::Array(elements) => {
                let len = elements.len().to_string();
                // Prefix with header, then concatenate element encodings.
                let mut v = Vec::new();
                v.push(b'*');
                v.extend_from_slice(len.as_bytes());
                v.extend_from_slice(RESP_TERMINATOR);
                for single_element in elements {
                    v.extend_from_slice(&single_element.to_resp_bytes());
                }
                v
            }
            RedisType::NullArray => b"*-1\r\n".to_vec(),
            RedisType::Integer(i) => {
                let s = i.to_string();
                let mut v = Vec::with_capacity(1 + s.len() + 2);
                v.push(b':');
                v.extend_from_slice(s.as_bytes());
                v.extend_from_slice(RESP_TERMINATOR);
                v
            }
            // Encode invalid type as a simple error to comply with RESP.
            RedisType::InvalidType(msg) => {
                let mut v = Vec::with_capacity(1 + msg.len() + 2);
                v.push(b'-');
                v.extend_from_slice(msg.as_bytes());
                v.extend_from_slice(RESP_TERMINATOR);
                v
            }
            //  _\r\n
            // https://redis.io/docs/latest/develop/reference/protocol-spec/#nulls
            RedisType::Null => {
                let mut v = Vec::with_capacity(1 + 2);
                v.push(b'_');
                v.extend_from_slice(RESP_TERMINATOR);
                v
            }
        }
    }
}

// Helper to write into an existing TcpStream.
impl RedisType {
    pub async fn write_resp_bytes(&self, stream: &mut TcpStream) -> anyhow::Result<()> {
        let resp_bytes = self.to_resp_bytes();
        stream.write_all(&resp_bytes).await?;
        Ok(())
    }
}

pub fn try_parse_frame(buf: &BytesMut) -> Option<(RedisType, usize)> {
    if buf.is_empty() {
        return None;
    }
    // Parse from the current buffer start and return how many bytes were consumed.
    let mut fwd = ForwardBuf { buf, offset: 0 };
    let parsed_redis_type = try_parse_type_forward(&mut fwd)?;
    Some((parsed_redis_type, fwd.offset))
}

/// Used inside unit tests ONLY.
#[allow(dead_code)]
fn try_parse_type(buf: &BytesMut) -> Option<RedisType> {
    try_parse_frame(buf).map(|(parsed_redis_type, _)| parsed_redis_type)
}

fn try_parse_type_forward(buf: &mut ForwardBuf) -> Option<RedisType> {
    if buf.at_end() {
        return None;
    }

    let marker_byte = buf.consume_byte();

    match marker_byte {
        //
        // Simple strings: +<string>\r\n
        // https://redis.io/docs/latest/develop/reference/protocol-spec/#simple-strings
        //
        b'+' => {
            // buf.advance(1);
            let value = buf.consume_part()?;
            Some(RedisType::SimpleString(value))
        }
        //
        // Arrays: *<number-of-elements>\r\n<element-1>...<element-n>
        // Null array: *-1\r\n
        // https://redis.io/docs/latest/develop/reference/protocol-spec/#arrays
        //
        b'*' => {
            let arr_length = buf.consume_part()?;
            if let Ok(len) = arr_length.parse::<isize>() {
                if len == -1 {
                    return Some(RedisType::NullArray);
                }

                if len < 0 {
                    return Some(RedisType::InvalidType(
                        format!("Invalid array length {len}").to_owned(),
                    ));
                }

                let mut elements = Vec::with_capacity(len as usize);

                // Read all array elements recursively
                for _i in 0..len {
                    match try_parse_type_forward(buf) {
                        Some(elem) => elements.push(elem),
                        None => {
                            // Incomplete input: propagate None so the caller can read more bytes.
                            return None;
                        }
                    }
                }

                Some(RedisType::Array(elements))
            } else {
                tracing::warn!("Can't parse array length {arr_length}");

                Some(RedisType::InvalidType(
                    format!("Array length not a number {arr_length}").to_owned(),
                ))
            }
        }

        //
        // Bulk String: $<length>\r\n<data>\r\n
        // Null Bulk String: $-1\r\n
        // https://redis.io/docs/latest/develop/reference/protocol-spec/#bulk-strings
        b'$' => {
            let len_value = buf.consume_part()?;
            if let Ok(len) = len_value.parse::<isize>() {
                if len == -1 {
                    return Some(RedisType::NullBulkString);
                }

                if len < 0 {
                    return Some(RedisType::InvalidType(
                        format!("Invalid bulk string length {len}").to_owned(),
                    ));
                }

                if let Some(str_value) = buf.consume_part() {
                    // Note: For compatibility with previous implementation, we do not enforce
                    // that str_value.len() == len here.
                    Some(RedisType::BulkString(str_value))
                } else {
                    tracing::warn!("Can't fully read BulkString");
                    None
                }
            } else {
                Some(RedisType::InvalidType(
                    format!("Bulk string length not a number {len_value}").to_owned(),
                ))
            }
        }
        // Integer :[<+|->]<value>\r\n
        // https://redis.io/docs/latest/develop/reference/protocol-spec/#integers
        b':' => {
            if let Some(integer_as_str) = buf.consume_part() {
                if let Ok(integer_value) = integer_as_str.parse::<i32>() {
                    Some(RedisType::Integer(integer_value))
                } else {
                    Some(RedisType::InvalidType(
                        format!("Invalid integer {integer_as_str}").to_owned(),
                    ))
                }
            } else {
                Some(RedisType::InvalidType("Can't read integer".to_owned()))
            }
        }
        _ => {
            todo!("Unsupported type marker byte: '{}'", marker_byte as char)
        }
    }
}

struct ForwardBuf<'a> {
    buf: &'a BytesMut,
    offset: usize,
}

impl ForwardBuf<'_> {
    fn at_end(&self) -> bool {
        self.offset >= self.buf.len()
    }

    fn consume_byte(&mut self) -> u8 {
        let value = self.buf[self.offset];
        self.offset += 1;
        value
    }

    fn find_delimiters_position(&self) -> i32 {
        if self.buf.len().saturating_sub(self.offset) < 2 {
            return -1;
        }
        for i in self.offset..(self.buf.len() - 1) {
            if self.buf[i] == RESP_TERMINATOR[0] && self.buf[i + 1] == RESP_TERMINATOR[1] {
                return i as i32;
            }
        }
        -1
    }

    // Reads from current offset to CRLF and advances offset past CRLF
    fn consume_part(&mut self) -> Option<String> {
        let pos = self.find_delimiters_position();
        if pos < 0 {
            return None;
        }
        let end = pos as usize;
        let s = String::from_utf8_lossy(&self.buf[self.offset..end]).into_owned();
        // advance past content and CRLF
        self.offset = end + 2;
        Some(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    //
    // Test Simple String parsing
    //
    #[test]
    fn parse_simple_string() {
        assert_for_content("+OK\r\n", RedisType::SimpleString("OK".to_owned()));
        assert_for_content("+Ping\r\n", RedisType::SimpleString("Ping".to_owned()));
    }

    #[test]
    fn parse_simple_string_incorrect() {
        assert_none_for_content("+OK");

        assert_none_for_content("+OK\r");

        assert_none_for_content("+OK\n");

        assert_none_for_content("+OK\n\r");
    }

    //
    // Test Bulk String parsing
    //
    #[test]
    fn parse_bulk_string() {
        assert_for_content("$4\r\nbulk\r\n", RedisType::BulkString("bulk".to_owned()));
        assert_for_content("$0\r\n\r\n", RedisType::BulkString("".to_owned()));
    }

    #[test]
    fn parse_bulk_string_null() {
        assert_for_content("$-1\r\n", RedisType::NullBulkString);
    }

    #[test]
    fn parse_bulk_string_incorrect() {
        assert_none_for_content("$4\r\nbulk");

        assert_none_for_content("$4\r\nbulk\r");

        assert_none_for_content("$4\r\nbulk\n");

        assert_none_for_content("$4\r\nbulk\n\r");

        assert_for_content(
            "$-2\r\nbulk\r\n",
            RedisType::InvalidType("Invalid bulk string length -2".to_owned()),
        );

        assert_for_content(
            "$abc\r\nbulk\r\n",
            RedisType::InvalidType("Bulk string length not a number abc".to_owned()),
        );
    }

    //
    // Test Array parsing
    //
    #[test]
    fn parse_array() {
        // Empty array
        assert_for_content("*0\r\n", RedisType::Array(vec![]));

        // Array of simple strings
        assert_for_content(
            "*2\r\n+OK\r\n+Ping\r\n",
            RedisType::Array(vec![
                RedisType::SimpleString("OK".to_owned()),
                RedisType::SimpleString("Ping".to_owned()),
            ]),
        );

        // Mixed types: simple, bulk, empty bulk
        assert_for_content(
            "*3\r\n+PONG\r\n$4\r\nbulk\r\n$0\r\n\r\n",
            RedisType::Array(vec![
                RedisType::SimpleString("PONG".to_owned()),
                RedisType::BulkString("bulk".to_owned()),
                RedisType::BulkString("".to_owned()),
            ]),
        );

        // Contains null bulk string
        assert_for_content(
            "*2\r\n$-1\r\n+OK\r\n",
            RedisType::Array(vec![
                RedisType::NullBulkString,
                RedisType::SimpleString("OK".to_owned()),
            ]),
        );

        // Contains null array as an element
        assert_for_content(
            "*1\r\n*-1\r\n",
            RedisType::Array(vec![RedisType::NullArray]),
        );
    }

    #[test]
    fn parse_array_with_different_types() {
        assert_for_content(
            "*5\r\n$5\r\nhello\r\n$-1\r\n$5\r\nworld\r\n$8\r\nvalkyrie\r\n$11\r\nis the best\r\n",
            RedisType::Array(vec![
                RedisType::BulkString("hello".to_owned()),
                RedisType::NullBulkString,
                RedisType::BulkString("world".to_owned()),
                RedisType::BulkString("valkyrie".to_owned()),
                RedisType::BulkString("is the best".to_owned()),
            ]),
        );
    }

    #[test]
    fn parse_array_null() {
        assert_for_content("*-1\r\n", RedisType::NullArray);
    }

    #[test]
    fn parse_array_nested() {
        // [[ "A", "B" ], "abc"]
        assert_for_content(
            "*2\r\n*2\r\n+A\r\n+B\r\n$3\r\nabc\r\n",
            RedisType::Array(vec![
                RedisType::Array(vec![
                    RedisType::SimpleString("A".to_owned()),
                    RedisType::SimpleString("B".to_owned()),
                ]),
                RedisType::BulkString("abc".to_owned()),
            ]),
        );

        // [ ["X"], null array, "" ]
        assert_for_content(
            "*3\r\n*1\r\n+X\r\n*-1\r\n$0\r\n\r\n",
            RedisType::Array(vec![
                RedisType::Array(vec![RedisType::SimpleString("X".to_owned())]),
                RedisType::NullArray,
                RedisType::BulkString("".to_owned()),
            ]),
        );
    }

    #[test]
    fn parse_array_incorrect() {
        // missing one element
        assert_none_for_content("*1\r\n");

        // Missing second element
        assert_none_for_content("*2\r\n+OK\r\n");

        // Incomplete element inside array
        assert_none_for_content("*1\r\n$4\r\nbulk"); // bulk string not fully terminated
        assert_none_for_content("*1\r\n+OK"); // simple string not fully terminated

        // Partially terminated element delimiters
        assert_none_for_content("*1\r\n+OK\r");
        assert_none_for_content("*1\r\n+OK\n");
        assert_none_for_content("*1\r\n+OK\n\r");

        // Invalid negative length (except -1)
        assert_for_content(
            "*-2\r\n",
            RedisType::InvalidType("Invalid array length -2".to_owned()),
        );

        // Non-numeric length
        assert_for_content(
            "*abc\r\n",
            RedisType::InvalidType("Array length not a number abc".to_owned()),
        );
    }

    //
    // Integer parsing
    //
    #[test]
    fn parse_integer() {
        assert_for_content(":0\r\n", RedisType::Integer(0));
        assert_for_content(":+0\r\n", RedisType::Integer(0));
        assert_for_content(":42\r\n", RedisType::Integer(42));
        assert_for_content(":+42\r\n", RedisType::Integer(42));
        assert_for_content(":-42\r\n", RedisType::Integer(-42));
        assert_for_content(":2147483647\r\n", RedisType::Integer(2147483647));
        assert_for_content(":-2147483647\r\n", RedisType::Integer(-2147483647));
        assert_for_content(":-2147483648\r\n", RedisType::Integer(-2147483648));
    }

    #[test]
    fn parse_integer_incorrect() {
        // Empty integer after marker
        assert_for_content(
            ":\r\n",
            RedisType::InvalidType("Invalid integer ".to_owned()),
        );

        // Sign only
        assert_for_content(
            ":+\r\n",
            RedisType::InvalidType("Invalid integer +".to_owned()),
        );
        assert_for_content(
            ":-\r\n",
            RedisType::InvalidType("Invalid integer -".to_owned()),
        );

        // Non-digit characters in value
        assert_for_content(
            ":12a\r\n",
            RedisType::InvalidType("Invalid integer 12a".to_owned()),
        );
        assert_for_content(
            ":a12\r\n",
            RedisType::InvalidType("Invalid integer a12".to_owned()),
        );
        assert_for_content(
            ":+-12\r\n",
            RedisType::InvalidType("Invalid integer +-12".to_owned()),
        );
        assert_for_content(
            ":-+12\r\n",
            RedisType::InvalidType("Invalid integer -+12".to_owned()),
        );
    }

    #[test]
    fn parse_integer_incomplete_or_overflow() {
        // Incomplete (missing CRLF) -> parser returns InvalidType("Can't read integer")
        assert_for_content(
            ":0",
            RedisType::InvalidType("Can't read integer".to_owned()),
        );
        assert_for_content(
            ":123\r",
            RedisType::InvalidType("Can't read integer".to_owned()),
        );
        assert_for_content(
            ":123\n",
            RedisType::InvalidType("Can't read integer".to_owned()),
        );
        assert_for_content(
            ":123\n\r",
            RedisType::InvalidType("Can't read integer".to_owned()),
        );

        // // Overflow/underflow: values outside i32 range
        assert_for_content(
            ":2147483648\r\n",
            RedisType::InvalidType("Invalid integer 2147483648".to_owned()),
        );

        assert_for_content(
            ":-2147483649\r\n",
            RedisType::InvalidType("Invalid integer -2147483649".to_owned()),
        );
    }

    //
    // Assertion helpers
    //
    fn assert_for_content(raw_content: &str, expected: RedisType) {
        let parse_result = parse_raw_value(raw_content);
        assert!(parse_result.is_some());
        let actual = parse_result.unwrap();
        assert_eq!(expected, actual);
    }

    fn assert_none_for_content(raw_content: &str) {
        let parse_result = parse_raw_value(raw_content);
        assert!(parse_result.is_none());
    }

    fn parse_raw_value(raw_content: &str) -> Option<RedisType> {
        let buf = BytesMut::from(raw_content);
        try_parse_type(&buf)
    }

    //
    // Encoding tests
    //
    #[test]
    fn encode_simple_string() {
        let v = RedisType::SimpleString("OK".to_owned()).to_resp_bytes();
        assert_eq!(b"+OK\r\n", v.as_slice());
    }

    #[test]
    fn encode_bulk_string() {
        let v = RedisType::BulkString("bulk".to_owned()).to_resp_bytes();
        assert_eq!(b"$4\r\nbulk\r\n", v.as_slice());
    }

    #[test]
    fn encode_null_bulk_string() {
        let v = RedisType::NullBulkString.to_resp_bytes();
        assert_eq!(b"$-1\r\n", v.as_slice());
    }

    #[test]
    fn encode_integer() {
        let v = RedisType::Integer(-42).to_resp_bytes();
        assert_eq!(b":-42\r\n", v.as_slice());
    }

    #[test]
    fn encode_array_simple() {
        let v = RedisType::Array(vec![
            RedisType::SimpleString("OK".to_owned()),
            RedisType::SimpleString("Ping".to_owned()),
        ])
        .to_resp_bytes();
        assert_eq!(b"*2\r\n+OK\r\n+Ping\r\n", v.as_slice());
    }

    #[test]
    fn encode_array_mixed() {
        let v = RedisType::Array(vec![
            RedisType::SimpleString("PONG".to_owned()),
            RedisType::BulkString("bulk".to_owned()),
            RedisType::BulkString("".to_owned()),
        ])
        .to_resp_bytes();
        assert_eq!(b"*3\r\n+PONG\r\n$4\r\nbulk\r\n$0\r\n\r\n", v.as_slice());
    }

    #[test]
    fn encode_array_nested_and_nulls() {
        let v = RedisType::Array(vec![
            RedisType::Array(vec![
                RedisType::SimpleString("A".to_owned()),
                RedisType::SimpleString("B".to_owned()),
            ]),
            RedisType::BulkString("abc".to_owned()),
            RedisType::NullArray,
            RedisType::NullBulkString,
        ])
        .to_resp_bytes();
        // Expected:
        // *4\r\n
        // *2\r\n+A\r\n+B\r\n
        // $3\r\nabc\r\n
        // *-1\r\n
        // $-1\r\n
        let expected = b"*4\r\n*2\r\n+A\r\n+B\r\n$3\r\nabc\r\n*-1\r\n$-1\r\n";
        assert_eq!(expected, v.as_slice());
    }

    #[test]
    fn encode_invalid_type_as_error() {
        let v = RedisType::InvalidType("Invalid integer 12a".to_owned()).to_resp_bytes();
        assert_eq!(b"-Invalid integer 12a\r\n", v.as_slice());
    }

    #[test]
    fn froms_str_conversation() {
        // Simple String: +<string>\r\n
        assert_eq!(
            RedisType::SimpleString("Hello".to_string()),
            "+Hello\r\n".into()
        );

        // Integers: :[<+|->]<value>\r\n
        assert_eq!(RedisType::Integer(123), ":+123\r\n".into());
        assert_eq!(RedisType::Integer(-777), ":-777\r\n".into());

        // Bulk String: $<length>\r\n<data>\r\n
        assert_eq!(
            RedisType::BulkString("bulk".to_string()),
            "$4\r\nbulk\r\n".into()
        );
        assert_eq!(RedisType::BulkString("".to_string()), "$0\r\n\r\n".into());

        // Null Bulk String: $-1\r\n
        assert_eq!(RedisType::NullBulkString, "$-1\r\n".into());

        // Arrays: *<number-of-elements>\r\n<element-1>...<element-n>
        assert_eq!(
            RedisType::Array(vec![
                RedisType::SimpleString("OK".to_owned()),
                RedisType::SimpleString("Ping".to_owned()),
            ]),
            "*2\r\n+OK\r\n+Ping\r\n".into()
        );

        // Arrays with mixed elements
        assert_eq!(
            RedisType::Array(vec![
                RedisType::BulkString("hello".to_owned()),
                RedisType::Integer(42),
            ]),
            "*2\r\n$5\r\nhello\r\n:42\r\n".into()
        );

        // Null Array: *-1\r\n
        assert_eq!(RedisType::NullArray, "*-1\r\n".into());

        // Nested array
        assert_eq!(
            RedisType::Array(vec![
                RedisType::Array(vec![RedisType::SimpleString("A".to_owned())]),
                RedisType::BulkString("abc".to_owned()),
            ]),
            "*2\r\n*1\r\n+A\r\n$3\r\nabc\r\n".into()
        );
    }
}
