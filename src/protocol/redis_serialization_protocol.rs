use bytes::BytesMut;

pub enum RedisType {
    SimpleString(String),
    BulkString(String),
    NullBulkString,
    Array(Vec<RedisType>),
}

pub fn try_parse_type(buf: &BytesMut) -> Option<RedisType> {
    if buf.is_empty() {
        return None;
    }

    let mut offset = 0;

    if buf[offset] == b'+' {
        offset += 1;

        if let Some(value) = read_raw_value_till_delimiters_from_offset(buf, offset) {
            return Some(RedisType::SimpleString(value));
        }
    } else if buf[offset] == b'*' {
        offset += 1;
        todo!("RedisType::Array not implemented yet")
    } else if buf[offset] == b'$' {
        offset += 1;
        //
        // Bulk String:
        //
        // 1. null bulk string: $-1\r\n
        // 2. ordinary bulk string: $<len>\r\n<data>\r\n
        //
        if let Some(len_value) = read_raw_value_till_delimiters_from_offset(buf, offset) {
            if let Ok(len) = len_value.parse::<isize>() {
                if len == -1 {
                    return Some(RedisType::NullBulkString);
                }

                offset += len_value.len() + 2;

                if let Some(str_value) = read_raw_value_till_delimiters_from_offset(buf, offset) {
                    return Some(RedisType::BulkString(str_value));
                } else {
                    println!("Can't fully read BulkString");
                }
            } else {
                println!("Incorrect length for BulkString");
            }
        }
    } else {
        todo!("Unimplemented type")
    }

    None
}

fn read_raw_value_till_delimiters_from_offset(buf: &BytesMut, offset: usize) -> Option<String> {
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
