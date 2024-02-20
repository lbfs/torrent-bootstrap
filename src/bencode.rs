use serde::{ser::{SerializeMap, SerializeSeq}, Serialize};

/**
 * Reference: http://bittorrent.org/beps/bep_0003.html
 * Strings are length-prefixed base ten followed by a colon and the string. For example 4:spam corresponds to 'spam'.
 * Integers are represented by an 'i' followed by the number in base 10 followed by an 'e'. For example i3e corresponds to 3 and i-3e corresponds to -3. Integers have no size limitation. i-0e is invalid. All encodings with a leading zero, such as i03e, are invalid, other than i0e, which of course corresponds to 0.
 * Lists are encoded as an 'l' followed by their elements (also bencoded) followed by an 'e'. For example l4:spam4:eggse corresponds to ['spam', 'eggs'].
 * Dictionaries are encoded as a 'd' followed by a list of alternating keys and their corresponding values followed by an 'e'. For example, d3:cow3:moo4:spam4:eggse corresponds to {'cow': 'moo', 'spam': 'eggs'} and d4:spaml1:a1:bee corresponds to {'spam': ['a', 'b']}. Keys must be strings and appear in sorted order (sorted as raw strings, not alphanumerics).
*/

#[derive(Debug)]
pub struct BencodeString {
    pub value: Vec<u8>,
    pub start_position: usize,
    pub end_position: usize,
    continuation_position: usize
}

impl Serialize for BencodeString {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer, 
    {
        // Ideally we always keep this bytes, but for simplicity let's try to make a UTF-8 string where possible.
        // Otherwise, it is a massive pain in the ass to move this data around.
        match String::from_utf8(self.value.clone()) {
            Ok(output) => serializer.serialize_str(&output),
            Err(_) => serializer.serialize_bytes(&self.value),
        }
    }
}

#[derive(Debug)]
pub struct BencodeInteger {
    pub value: i64,
    pub start_position: usize,
    pub end_position: usize,
    continuation_position: usize
}

impl Serialize for BencodeInteger {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer, 
    {
        serializer.serialize_i64(self.value as i64)
    }
}

#[derive(Debug)]
pub struct BencodeList {
    pub value: Vec<BencodeToken>,
    pub start_position: usize,
    pub end_position: usize,
    continuation_position: usize
}

impl Serialize for BencodeList {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer, 
    {
        let mut s = serializer.serialize_seq(Some(self.value.len()))?;
        for token in &self.value {
            s.serialize_element(token)?
        }
        s.end()
    }
}

#[derive(Debug)]
pub struct BencodeDictionary {
    pub value: Vec<(BencodeString, BencodeToken)>,
    pub start_position: usize,
    pub end_position: usize,
    continuation_position: usize
}

impl Serialize for BencodeDictionary {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer, 
    {
        let mut s = serializer.serialize_map(Some(self.value.len()))?;
        for (token_key, token_value) in &self.value {
            s.serialize_entry(token_key, token_value)?
        }
        s.end()
    }
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

impl Serialize for BencodeToken {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer, 
    {
        match self {
            BencodeToken::String(value) => value.serialize(serializer),
            BencodeToken::List(value) => value.serialize(serializer),
            BencodeToken::Integer(value) => value.serialize(serializer),
            BencodeToken::Dictionary(value) => value.serialize(serializer),
        }
    }
}

pub struct Bencode;
impl Bencode {
    pub fn decode(bytes: &[u8]) -> Result<BencodeToken, BencodeError> {
        let token = Bencode::decode_at_position(bytes, 0)?;
        let continuation_position = Bencode::get_continuation_position(&token);
        
        if continuation_position != bytes.len() {
            return Err(BencodeError::ValidationException("Unexpected end of file. Continuation token position is not at the end of the bytes array.".to_string()));
        }

        return Ok(token);
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
                b':' if position > start_position => {
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
    
        let mut result: i64 = 0;
        let mut result_sign: i64 = 1;

        let mut first_digit: Option<i64> = None;
    
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
                b'0'..=b'9' if position > start_position => {
                    let number = (byte - b'0') as i64;
    
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
                b'0'..=b'9' | b'i' | b'l' | b'd' if position > start_position => {
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
                b'0'..=b'9' if key_seen.is_none() && position > start_position => {
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
                b'e' if position > start_position && key_seen.is_none() => {
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
