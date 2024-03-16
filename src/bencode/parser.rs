use super::error::BencodeErrorKind;
use super::BencodeToken;
use super::BencodeDictionary;
use super::BencodeList;
use super::BencodeString;
use super::BencodeError;
use super::BencodeInteger;


#[derive(Debug)]
enum StringState {
    FirstDigit,
    Digit,
    Separator
}

#[derive(Debug)]
enum IntegerState {
    Start,
    FirstDigit,
    NonZeroDigit,
    Digit,
    Done
}

#[derive(Debug)]
enum ListState {
    Start,
    Entry
}

#[derive(Debug)]
enum DictionaryState {
    Start,
    KeyEntry,
    ValueEntry
}

/**
 * Reference: http://bittorrent.org/beps/bep_0003.html
 * Strings are length-prefixed base ten followed by a colon and the string. For example 4:spam corresponds to 'spam'.
 * Integers are represented by an 'i' followed by the number in base 10 followed by an 'e'. For example i3e corresponds to 3 and i-3e corresponds to -3. Integers have no size limitation. i-0e is invalid. All encodings with a leading zero, such as i03e, are invalid, other than i0e, which of course corresponds to 0.
 * Lists are encoded as an 'l' followed by their elements (also bencoded) followed by an 'e'. For example l4:spam4:eggse corresponds to ['spam', 'eggs'].
 * Dictionaries are encoded as a 'd' followed by a list of alternating keys and their corresponding values followed by an 'e'. For example, d3:cow3:moo4:spam4:eggse corresponds to {'cow': 'moo', 'spam': 'eggs'} and d4:spaml1:a1:bee corresponds to {'spam': ['a', 'b']}. Keys must be strings and appear in sorted order (sorted as raw strings, not alphanumerics).
*/
pub struct Parser;
impl Parser {
    pub fn decode(bytes: &[u8]) -> Result<BencodeToken, BencodeError> {
        let token = Parser::decode_at_position(bytes, 0)?;
        let continuation_position = Parser::get_continuation_position(&token);

        if continuation_position != bytes.len() {
            return Err(BencodeError::new(BencodeErrorKind::MalformedData, "Unexpected end of file. Token continuation position is not at the end of the bytes array.".to_string()));
        }

        Ok(token)
    }

    fn decode_at_position(bytes: &[u8], start_position: usize) -> Result<BencodeToken, BencodeError> {
        if start_position >= bytes.len() { 
            return Err(BencodeError::new(BencodeErrorKind::MalformedData, "Start position exceeds provided byte string boundaries.".to_string()));
        }
    
        match bytes[start_position] {
            b'0'..=b'9' => Ok(BencodeToken::String(Parser::decode_string(bytes, start_position)?)),
            b'i' => Ok(BencodeToken::Integer(Parser::decode_integer(bytes, start_position)?)),
            b'l' => Ok(BencodeToken::List(Parser::decode_list(bytes, start_position)?)),
            b'd' => Ok(BencodeToken::Dictionary(Parser::decode_dictionary(bytes, start_position)?)),
            _ => { Err(BencodeError::new(BencodeErrorKind::MalformedData, "Unexpected character when detecting type to evaluate".to_string())) }
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
    
    fn decode_string(bytes: &[u8], start_position: usize) -> Result<BencodeString, BencodeError> {
        let mut position: usize = start_position;
        let mut state = StringState::FirstDigit;

        let mut length_buffer: Vec<u8> = Vec::new();
        let mut character_buffer: Vec<u8> = Vec::new();
        let characters_to_read: usize;

        loop {
            if position >= bytes.len() {
                return Err(BencodeError::new(BencodeErrorKind::MalformedData, format!("Unexpected end of byte stream when parsing from start_position {}.", start_position)));
            }
    
            let byte = bytes[position];
            match byte {
                b'0' if matches!(state, StringState::FirstDigit) => {
                    length_buffer.push(byte);
                    state = StringState::Separator;
                    position += 1;
                },
                b'1'..=b'9' if matches!(state, StringState::FirstDigit) => {
                    length_buffer.push(byte);
                    state = StringState::Digit;
                    position += 1;
                },
                b'0'..=b'9' if matches!(state, StringState::Digit) => {
                    length_buffer.push(byte);
                    position += 1;
                    
                },
                b':' if matches!(state, StringState::Digit) || matches!(state, StringState::Separator) => {
                    characters_to_read = std::str::from_utf8(&length_buffer)
                        .expect("Detected non UTF-8 string during string decode. This should never happen.")
                        .parse::<usize>()
                        .map_err(|err| BencodeError::new(BencodeErrorKind::MalformedData, err.to_string()))?;

                    position += 1;
                    break;
                },
                _ => {
                    return Err(BencodeError::new(BencodeErrorKind::MalformedData, format!("Unexpected character in bytes at position {} with character {} with state {:#?}", position, bytes[position], state)));
                }
            }
        }

        while character_buffer.len() < characters_to_read {
            if position >= bytes.len() {
                return Err(BencodeError::new(BencodeErrorKind::MalformedData, format!("Unexpected end of byte stream when parsing from start_position {}.", start_position)));
            }
    
            let byte = bytes[position];
            character_buffer.push(byte);
            position += 1;
        }

        let res = BencodeString {
            start_position,
            end_position: position - 1,
            continuation_position: position,
            value: character_buffer
        };

        Ok(res)
    }

    fn decode_integer(bytes: &[u8], start_position: usize) -> Result<BencodeInteger, BencodeError> {
        let mut position: usize = start_position;
        let mut result: Vec<u8> = Vec::new();
        let mut state: IntegerState = IntegerState::Start;
    
        loop {
            if position >= bytes.len() {
                return Err(BencodeError::new(BencodeErrorKind::MalformedData, format!("Unexpected end of byte stream when parsing from start_position {}.", start_position)));
            }
    
            let byte = bytes[position];
            match byte {
                b'i' if matches!(state, IntegerState::Start) => { state = IntegerState::FirstDigit; },
                b'0' if matches!(state, IntegerState::FirstDigit) => {
                    result.push(byte);
                    state = IntegerState::Done; 
                },
                b'-' if matches!(state, IntegerState::FirstDigit) => { 
                    result.push(byte);
                    state = IntegerState::NonZeroDigit; 
                }
                b'1'..=b'9' if matches!(state, IntegerState::NonZeroDigit) || matches!(state, IntegerState::FirstDigit) => { 
                    result.push(byte);
                    state = IntegerState::Digit; 
                }
                b'0'..=b'9' if matches!(state, IntegerState::Digit) => { 
                    result.push(byte);
                    state = IntegerState::Digit 
                },
                b'e' if matches!(state, IntegerState::Done) || matches!(state, IntegerState::Digit) => {
                    break;
                },
                _ => {
                    return Err(BencodeError::new(BencodeErrorKind::MalformedData, format!("Unexpected character in bytes at position {} with character {} with state {:#?}", position, bytes[position], state)));
                }
            }

            position += 1;
        }
    
        Ok(BencodeInteger {
            value: result,
            start_position,
            end_position: position,
            continuation_position: position + 1,
        })
    }
    
    fn decode_list(bytes: &[u8], start_position: usize) -> Result<BencodeList, BencodeError> {
        let mut position: usize = start_position;
        let mut tokens: Vec<BencodeToken> = Vec::new();
        let mut state: ListState = ListState::Start;
    
        loop {
            if position >= bytes.len() {
                return Err(BencodeError::new(BencodeErrorKind::MalformedData, format!("Unexpected end of byte stream when parsing from start_position {}.", start_position)));
            }
    
            let byte = bytes[position];
            match byte {
                b'l' if matches!(state, ListState::Start) => {
                    state = ListState::Entry;
                    position += 1;
                }
                b'0'..=b'9' | b'i' | b'l' | b'd' if matches!(state, ListState::Entry) => {
                    let token = Parser::decode_at_position(bytes, position)?;
                    position = Parser::get_continuation_position(&token);
                    tokens.push(token);
                }
                b'e' if matches!(state, ListState::Entry) => {
                    break;
                }
                _ => {
                    return Err(BencodeError::new(BencodeErrorKind::MalformedData, format!("Unexpected character in bytes at position {}", position)));
                }
            }
        }
    
        Ok(BencodeList {
            value: tokens,
            start_position,
            end_position: position,
            continuation_position: position + 1
        })
    }
    
    fn decode_dictionary(bytes: &[u8], start_position: usize) -> Result<BencodeDictionary, BencodeError> {
        let mut position: usize = start_position;
        let mut state: DictionaryState = DictionaryState::Start;

        let mut keys: Vec<BencodeString> = Vec::new();
        let mut values: Vec<BencodeToken> = Vec::new();

        loop {
            if position >= bytes.len() {
                return Err(BencodeError::new(BencodeErrorKind::MalformedData, format!("Unexpected end of byte stream when parsing from start_position {}.", start_position)));
            }
    
            let byte = bytes[position];
            match byte {
                b'd' if matches!(state, DictionaryState::Start) => {
                    state = DictionaryState::KeyEntry;
                    position += 1;
                }
                b'0'..=b'9' if matches!(state, DictionaryState::KeyEntry) => {
                    let token = Parser::decode_string(bytes, position)?;
                    position = token.continuation_position;

                    if keys.len() > 0 {
                        let last = keys.last().unwrap();
                        match last.value.cmp(&token.value) {
                            std::cmp::Ordering::Less => (),
                            std::cmp::Ordering::Equal => {
                                return Err(BencodeError::new(BencodeErrorKind::MalformedData, format!("Duplicate key entries are not allowed for dictionary at dictionary with start_position {}", start_position)));
                            },
                            std::cmp::Ordering::Greater => {
                                return Err(BencodeError::new(BencodeErrorKind::MalformedData, format!("Key entries are not in lexicographical order at dictionary with start_position {}", start_position)));
                            }
                        }
                    }

                    keys.push(token);
                    state = DictionaryState::ValueEntry;
                },
                b'0'..=b'9' | b'i' | b'l' | b'd' if matches!(state, DictionaryState::ValueEntry) => {
                    let value = Parser::decode_at_position(bytes, position)?;
                    position = Parser::get_continuation_position(&value);

                    values.push(value);
                    state = DictionaryState::KeyEntry;
                }
                b'e' if matches!(state, DictionaryState::KeyEntry) => {
                    break;
                }
                _ => {
                    return Err(BencodeError::new(BencodeErrorKind::MalformedData, format!("Unexpected character in bytes at position {}", position)));
                }
            }
        }

        Ok(BencodeDictionary {
            value: keys.into_iter().zip(values).collect(),
            start_position,
            end_position: position,
            continuation_position: position + 1,
        })
    }

}
