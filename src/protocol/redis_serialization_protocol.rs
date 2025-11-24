use bytes::BytesMut;

#[derive(Debug)]
pub enum RedisType {
    SimpleString(String),
    BulkString(String),
    NullBulkString,
    Array(Vec<RedisType>),
}

pub fn try_parse_type(buf: &BytesMut) -> Option<(RedisType, usize)> {
    let buf_content = String::from_utf8_lossy(&buf[..]).into_owned();
    let buf_content = buf_content.replace("\r\n", "\\r\\n");

    println!("Parsing raw content =======> {buf_content}");

    try_parse_type_with_offset(buf, 0)
}

pub fn try_parse_type_with_offset(buf: &BytesMut, mut offset: usize) -> Option<(RedisType, usize)> {
    if buf.is_empty() {
        return None;
    }

    // Simple strings
    // https://redis.io/docs/latest/develop/reference/protocol-spec/#simple-strings
    //
    if buf[offset] == b'+' {
        offset += 1;

        let value = read_part(buf, offset)?;

        offset += value.len() + 2;

        return Some((RedisType::SimpleString(value), offset));
    }
    // Arrays
    // https://redis.io/docs/latest/develop/reference/protocol-spec/#arrays
    //
    // Array cases:
    // Empty array: *0\r\n
    // Standard: *<number-of-elements>\r\n<element-1>...<element-n>
    // Null array: *-1\r\n
    else if buf[offset] == b'*' {
        offset += 1;

        let arr_length = read_part(buf, offset)?;
        offset += arr_length.len() + 2;

        if let Ok(len) = arr_length.parse::<isize>() {
            let mut elements = Vec::with_capacity(len as usize);

            // Read all array elememnts here recursively
            for _ in 0..len {
                let elem_with_offset = try_parse_type_with_offset(buf, offset)?;
                elements.push(elem_with_offset.0);
                offset = elem_with_offset.1;
            }

            return Some((RedisType::Array(elements), offset));
        } else {
            println!("Can't parse array length {arr_length}");
        }
    }
    // Bulk strings
    // https://redis.io/docs/latest/develop/reference/protocol-spec/#bulk-strings
    //
    else if buf[offset] == b'$' {
        offset += 1;
        //
        // Bulk String:
        //
        // 1. null bulk string: $-1\r\n
        // 2. ordinary bulk string: $<len>\r\n<data>\r\n
        //
        let len_value = read_part(buf, offset)?;
        offset += len_value.len() + 2;

        if let Ok(len) = len_value.parse::<isize>() {
            if len == -1 {
                return Some((RedisType::NullBulkString, offset));
            }

            if let Some(str_value) = read_part(buf, offset) {
                offset += str_value.len() + 2;
                return Some((RedisType::BulkString(str_value), offset));
            } else {
                println!("Can't fully read BulkString");
            }
        } else {
            println!("Incorrect length for BulkString");
        }
    } else {
        todo!("Unimplemented type")
    }

    None
}

fn read_part(buf: &BytesMut, offset: usize) -> Option<String> {
    let delimiters_pos = find_delimiters_position(buf, offset);
    if delimiters_pos >= 0 {
        return Some(String::from_utf8_lossy(&buf[offset..delimiters_pos as usize]).into_owned());
    }

    None
}

fn find_delimiters_position(buf: &BytesMut, offset: usize) -> i32 {
    if buf.len() - offset < 2 {
        return -1;
    }

    for i in offset..(buf.len() - 1) {
        if buf[i] == b'\r' && buf[i + 1] == b'\n' {
            return i as i32;
        }
    }

    -1
}
