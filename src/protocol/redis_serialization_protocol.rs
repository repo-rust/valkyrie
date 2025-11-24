use bytes::BytesMut;

#[derive(Debug, PartialEq)]
pub enum RedisType {
    SimpleString(String),
    BulkString(String),
    NullBulkString,
    Array(Vec<RedisType>),
    NullArray,
    InvalidType(String),
}

pub fn try_parse_type(buf: &BytesMut) -> Option<RedisType> {
    let buf_content: String = String::from_utf8_lossy(&buf[..]).into_owned();
    let buf_content = buf_content.replace("\r\n", "\\r\\n");

    println!("Parsing raw content =======> {buf_content}");

    let mut fwd = ForwardBuf { buf, offset: 0 };

    try_parse_type_forward(&mut fwd)
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
                for i in 0..len {
                    if let Some(elem) = try_parse_type_forward(buf) {
                        elements.push(elem);
                    } else {
                        return Some(RedisType::InvalidType(
                            format!("Can't read {i}-th array element").to_owned(),
                        ));
                    }
                }

                Some(RedisType::Array(elements))
            } else {
                println!("Can't parse array length {arr_length}");

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
                    println!("Can't fully read BulkString");
                    None
                }
            } else {
                Some(RedisType::InvalidType(
                    format!("Bulk string length not a number {len_value}").to_owned(),
                ))
            }
        }
        _ => {
            todo!("Unimplemented type")
        }
    }
}

struct ForwardBuf<'a> {
    buf: &'a BytesMut,
    offset: usize,
}

impl<'a> ForwardBuf<'a> {
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
            if self.buf[i] == b'\r' && self.buf[i + 1] == b'\n' {
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

    #[test]
    fn parse_simple_string() {
        assert_for_content("+OK\r\n", RedisType::SimpleString("OK".to_owned()));
        assert_for_content("+Ping\r\n", RedisType::SimpleString("Ping".to_owned()));
    }

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

    #[test]
    fn parse_simple_string_incorrect() {
        assert_none_for_content("+OK");

        assert_none_for_content("+OK\r");

        assert_none_for_content("+OK\n");

        assert_none_for_content("+OK\n\r");
    }

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
        assert_for_content(
            "*1\r\n",
            RedisType::InvalidType("Can't read 0-th array element".to_owned()),
        );

        // Missing second element
        assert_for_content(
            "*2\r\n+OK\r\n",
            RedisType::InvalidType("Can't read 1-th array element".to_owned()),
        );

        // Incomplete element inside array
        assert_for_content(
            "*1\r\n$4\r\nbulk",
            RedisType::InvalidType("Can't read 0-th array element".to_owned()),
        ); // bulk string not fully terminated
        assert_for_content(
            "*1\r\n+OK",
            RedisType::InvalidType("Can't read 0-th array element".to_owned()),
        ); // simple string not fully terminated

        // Partially terminated element delimiters
        assert_for_content(
            "*1\r\n+OK\r",
            RedisType::InvalidType("Can't read 0-th array element".to_owned()),
        );
        assert_for_content(
            "*1\r\n+OK\n",
            RedisType::InvalidType("Can't read 0-th array element".to_owned()),
        );
        assert_for_content(
            "*1\r\n+OK\n\r",
            RedisType::InvalidType("Can't read 0-th array element".to_owned()),
        );

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
}
