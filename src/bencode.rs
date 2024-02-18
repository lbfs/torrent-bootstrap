// Notes: https://www.bittorrent.org/beps/bep_0003.html#bencoding
#[derive(Debug)]
pub struct BencodeString {
    pub value: Vec<u8>,
    start_position: usize,
    end_position: usize,
    continuation_position: usize
}

#[derive(Debug)]
pub struct BencodeInteger {
    pub value: isize,
    start_position: usize,
    end_position: usize,
    continuation_position: usize
}

#[derive(Debug)]
pub struct BencodeList {
    pub value: Vec<BencodeToken>,
    start_position: usize,
    end_position: usize,
    continuation_position: usize
}

#[derive(Debug)]
pub struct BencodeDictionary {
    pub value: Vec<(BencodeString, BencodeToken)>,
    start_position: usize,
    end_position: usize,
    continuation_position: usize
}

#[derive(Debug)]
pub enum BencodeError {
    ValidationException(String)
}

#[derive(Debug)]
pub enum BencodeToken {
    String(BencodeString),
    List(BencodeList),
    Integer(BencodeInteger),
    Dictionary(BencodeDictionary)
}


pub struct Bencode;
impl Bencode {
    pub fn decode(bytes: &[u8]) -> Result<BencodeToken, BencodeError> {
        Bencode::decode_at_position(bytes, 0)
    }

    fn decode_at_position(bytes: &[u8], start_position: usize) -> Result<BencodeToken, BencodeError> {
        if start_position >= bytes.len() { 
            return Err(BencodeError::ValidationException("Start position exceeds provided byte string boundaries.".to_string()));
        }
    
        match bytes[start_position] {
            b'0'..=b'9' => Ok(BencodeToken::String(Bencode::decode_string(&bytes, start_position)?)),
            b'i' => Ok(BencodeToken::Integer(Bencode::decode_integer(&bytes, start_position)?)),
            b'l' => Ok(BencodeToken::List(Bencode::decode_list(&bytes, start_position)?)),
            b'd' => Ok(BencodeToken::Dictionary(Bencode::decode_dictionary(&bytes, start_position)?)),
            _ => { Err(BencodeError::ValidationException("Unexpected character when detecting type to evaluate".to_string())) }
        }
    }

    fn get_continuation_position(token: &BencodeToken) -> usize {
        match token {
            BencodeToken::String(value) => value.continuation_position,
            BencodeToken::List(value) => value.continuation_position,
            BencodeToken::Integer(value) => value.continuation_position,
            BencodeToken::Dictionary(value) => value.continuation_position,
        }
    }
    
    // Do we need to protect against integer overflows?
    // I see nothing in the spec about what the allowed size is....
    fn decode_string(bytes: &[u8], start_position: usize) -> Result<BencodeString, BencodeError> {
        let mut position: usize = start_position;
        let mut evaluated_size: usize = 0;
        
        loop {
            if position >= bytes.len() {
                return Err(BencodeError::ValidationException(format!("Unexpected end of byte stream when parsing from start_position {}.", start_position)));
            }
    
            let byte = bytes[position];
            match byte {
                b'0'..=b'9' => {
                    evaluated_size = (evaluated_size * 10) + (bytes[position] - b'0') as usize;
                    position += 1;
                }
                b':' if position > 0 => {
                    position += 1;
                    break;
                },
                _ => {
                    return Err(BencodeError::ValidationException(format!("Unexpected character in bytes at position {}", position)));
                }
            }
        }
    
        let end_position = position + evaluated_size - 1;
    
        if end_position > bytes.len() { 
            return Err(BencodeError::ValidationException(format!("Detected end position is larger than available bytes. End Position: {}, Length: {}", end_position, bytes.len())));
        }
    
        if end_position < position {         
            return Ok(BencodeString {
                start_position,
                end_position,
                continuation_position: end_position + 1,
                value: Vec::new()
            });
        }
    
        Ok(BencodeString {
            start_position,
            end_position,
            continuation_position: end_position + 1,
            value: bytes[position..=end_position].to_vec()
        })
    }
    
    fn decode_integer(bytes: &[u8], start_position: usize) -> Result<BencodeInteger, BencodeError> {
        let mut position: usize = start_position;
    
        let mut result: isize = 0;
        let mut result_sign: isize = 1;
    
        let mut first_digit: Option<isize> = None;
    
        loop {
            if position >= bytes.len() {
                return Err(BencodeError::ValidationException(format!("Unexpected end of byte stream when parsing from start_position {}.", start_position)));
            }
    
            let byte = bytes[position];
            match byte {
                b'i' if start_position == position => {
                    position += 1; 
                },
                b'-' if start_position + 1 == position => {
                    result_sign = -1;
                    position += 1;
                },
                b'0'..=b'9' => {
                    let number = (byte - b'0') as isize;
    
                    if first_digit.is_none() {
                        if result_sign == -1 && number == 0 {
                            return Err(BencodeError::ValidationException(format!("Integer has illegal negative for number starting with 0 at start position {}.", start_position)));
                        }
                        first_digit = Some(number);
                    } else if first_digit.unwrap() == 0 {
                        return Err(BencodeError::ValidationException(format!("Leading zeros are disallowed unless the number is explicitly 0 at start position {}.", start_position)));
                    }
    
                    result = (result * 10) + number;
                    position += 1;
                },
                b'e' if position >= start_position + 2 => {
                    break;
                },
                _ => {
                    return Err(BencodeError::ValidationException(format!("Unexpected character in bytes at position {}", position)));
                }
            }
        }
    
        result = result * result_sign;
    
        Ok(BencodeInteger {
            value: result,
            start_position: start_position,
            end_position: position,
            continuation_position: position + 1,
        })
    }
    
    fn decode_list(bytes: &[u8], start_position: usize) -> Result<BencodeList, BencodeError> {
        let mut position: usize = start_position;
        let mut tokens: Vec<BencodeToken> = Vec::new();
    
        loop {
            if position >= bytes.len() {
                return Err(BencodeError::ValidationException(format!("Unexpected end of byte stream when parsing from start_position {}.", start_position)));
            }
    
            let byte = bytes[position];
            match byte {
                b'l' if position == start_position => {
                    position += 1;
                }
                b'0'..=b'9' | b'i' | b'l' | b'd' => {
                    let token = Bencode::decode_at_position(bytes, position)?;
                    position = Bencode::get_continuation_position(&token);
                    tokens.push(token);
                }
                b'e' if position > start_position => {
                    break;
                }
                _ => {
                    return Err(BencodeError::ValidationException(format!("Unexpected character in bytes at position {}", position)));
                }
            }
        }
    
        Ok(BencodeList {
            value: tokens,
            start_position: start_position,
            end_position: position,
            continuation_position: position + 1
        })
    }
    
    fn decode_dictionary(bytes: &[u8], start_position: usize) -> Result<BencodeDictionary, BencodeError> {
        let mut position: usize = start_position;
        let mut key_seen: Option<BencodeString> = None;
        let mut tokens: Vec<(BencodeString, BencodeToken)> = Vec::new();
    
        loop {
            if position >= bytes.len() {
                return Err(BencodeError::ValidationException(format!("Unexpected end of byte stream when parsing from start_position {}.", start_position)));
            }
    
            let byte = bytes[position];
            match byte {
                b'd' if position == start_position => {
                    position += 1;
                }
                b'0'..=b'9' if key_seen.is_none() => {
                    let token = Bencode::decode_string(bytes, position)?;
                    position = token.continuation_position;
                    key_seen = Some(token);
                },
                b'0'..=b'9' | b'i' | b'l' | b'd' if key_seen.is_some() => {
                    let value = Bencode::decode_at_position(bytes, position)?;
                    let key = key_seen.take().unwrap();
                    position = Bencode::get_continuation_position(&value);
                    tokens.push((key, value));
                }
                b'e' if position > start_position => {
                    break;
                }
                _ => {
                    return Err(BencodeError::ValidationException(format!("Unexpected character in bytes at position {}", position)));
                }
            }
        }
    
        for post_index in 1..tokens.len() {
            let pre_index = post_index - 1;
    
            let (pre_key, _) = &tokens[pre_index];
            let (post_key, _) = &tokens[post_index];
    
            match pre_key.value.cmp(&post_key.value) {
                std::cmp::Ordering::Less => (),
                std::cmp::Ordering::Equal => {
                    return Err(BencodeError::ValidationException(format!("Duplicate key entries are not allowed for dictionary at dictionary with start_position {}", start_position)));
                },
                std::cmp::Ordering::Greater => {
                    return Err(BencodeError::ValidationException(format!("Key entries are not in lexicographical order at dictionary with start_position {}", start_position)));
                }
            }
        }
    
        Ok(BencodeDictionary {
            value: tokens,
            start_position: start_position,
            end_position: position,
            continuation_position: position + 1,
        })
    }

}
