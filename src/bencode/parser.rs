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
    DigitOrSeperator,
    Character
}

#[derive(Debug)]
enum IntegerState {
    StartCharacter,
    FirstDigit,
    NonZeroDigit,
    NegativeDigit,
    Digit,
    StopCharacter
}

#[derive(Debug)]
enum ListState {
    Start,
    Entry,
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

fn format_overflow_error(position: usize, byte: u8) -> BencodeError {
    BencodeError::new(BencodeErrorKind::MalformedData, format!("Cannot read next integer byte {:#04x} at position {} as it would cause integer overflow.", byte, position))
}

fn format_unexpected_eof(position: usize) -> BencodeError {
    BencodeError::new(BencodeErrorKind::MalformedData, format!("Unexpected end of file at position {}", position))
}

fn format_unexpected_character(byte: u8, position: usize, expected: &'static str) -> BencodeError {
    BencodeError::new(BencodeErrorKind::MalformedData, format!("Unexpected character {:#04x} at position {}, expected one of ({})", byte, position, expected))
}

fn format_remaining_bytes_error(position: usize) -> BencodeError {
    BencodeError::new(BencodeErrorKind::MalformedData, format!("Parsing completed, but extra data was found at position {}", position))
}

pub struct Parser;
impl Parser {
    pub fn decode(bytes: &[u8]) -> Result<BencodeToken, BencodeError> {
        let token = Parser::decode_any(&bytes, 0)?;

        let continuation_position = Parser::get_continuation_position(&token);
        match bytes.get(continuation_position) {
            Some(_) => {
                return Err(format_remaining_bytes_error(continuation_position));
            },
            None => {
                return Ok(token);
            }
        }
    }

    fn decode_any(bytes: &[u8], start_position: usize) -> Result<BencodeToken, BencodeError> {
        let byte = match bytes.get(start_position) {
            Some(byte) => {
                *byte
            },
            None => {
                return Err(format_unexpected_eof(start_position));
            }
        };

        let token = match byte {
            b'0'..=b'9' => BencodeToken::String(Parser::decode_string(bytes, start_position)?),
            b'i' => BencodeToken::Integer(Parser::decode_integer(bytes, start_position)?),
            b'l' => BencodeToken::List(Parser::decode_list(bytes, start_position)?),
            b'd' => BencodeToken::Dictionary(Parser::decode_dictionary(bytes, start_position)?),
            _ => { return Err(format_unexpected_character(byte, start_position, "b'0'..=b'9', b'i', b'l', b'd'")) }
        };

        Ok(token)
    }

    fn decode_integer(bytes: &[u8], start_position: usize) -> Result<BencodeInteger, BencodeError> {
        let mut position = start_position;

        let mut result: i64 = 0;
        let mut state: IntegerState = IntegerState::StartCharacter;
    
        loop {
            let byte = match bytes.get(position) {
                Some(byte) => {
                    *byte
                },
                None => {
                    return Err(format_unexpected_eof(position));
                }
            };

            match state {
                IntegerState::Digit => {
                    match byte {
                        b'0'..=b'9' => { 
                            result = result.checked_mul(10)
                                .ok_or_else(|| format_overflow_error(position, byte))?
                                .checked_add(byte as i64 - b'0' as i64)
                                .ok_or_else(|| format_overflow_error(position, byte))?;
                            position += 1;
                        },
                        b'e' => {
                            position += 1;
                            break;
                        }
                        _ => { return Err(format_unexpected_character(byte, position, "b'0'..=b'9', b'e'")); }
                    }
                },
                IntegerState::NegativeDigit => {
                    match byte {
                        b'0'..=b'9' => { 
                            result = result.checked_mul(10)
                                .ok_or_else(|| format_overflow_error(position, byte))?
                                .checked_sub(byte as i64 - b'0' as i64)
                                .ok_or_else(|| format_overflow_error(position, byte))?;
                            position += 1;
                        },
                        b'e' => {
                            position += 1;
                            break;
                        }
                        _ => { return Err(format_unexpected_character(byte, position, "b'0'..=b'9', b'e'")); }
                    }
                },
                IntegerState::NonZeroDigit => {
                    match byte {
                        b'1'..=b'9' => { 
                            result = -(byte as i64 - b'0' as i64);
                            state = IntegerState::NegativeDigit; 
                            position += 1;
                        }
                        _ => { return Err(format_unexpected_character(byte, position, "b'1'..=b'9'")); }
                    }
                },
                IntegerState::StopCharacter => {
                    match byte {
                        b'e' => {
                            position += 1;
                            break;
                        }
                        _ => { return Err(format_unexpected_character(byte, position, "b'e'")); }
                    }
                },
                IntegerState::FirstDigit => {
                    match byte {
                        b'1'..=b'9' => { 
                            result = byte as i64 - b'0' as i64;
                            state = IntegerState::Digit;
                            position += 1;
                        }
                        b'0' => {
                            result = 0;
                            state = IntegerState::StopCharacter; 
                            position += 1;
                        },
                        b'-' => {
                            state = IntegerState::NonZeroDigit; 
                            position += 1;
                        }
                        _ => { return Err(format_unexpected_character(byte, position, "b'1'..=b'9', b'0', b'-'")); }
                    }
                },
                IntegerState::StartCharacter => {
                    match byte {
                        b'i' => { 
                            state = IntegerState::FirstDigit;
                            position += 1;
                        }
                        _ => { return Err(format_unexpected_character(byte, position, "b'i'")); }
                    }
                }
            }
        }

        Ok(BencodeInteger {
            value: result,
            start_position,
            continuation_position: position,
        })
    }

    fn decode_string(bytes: &[u8], start_position: usize) -> Result<BencodeString, BencodeError> {
        let mut position = start_position;

        let mut characters_to_read: usize = 0;
        let mut characters: Vec<u8> = Vec::new();
        let mut state: StringState = StringState::FirstDigit;

        loop {
            let byte = match bytes.get(position) {
                Some(byte) => {
                    *byte
                },
                None => {
                    return Err(format_unexpected_eof(position));
                }
            };

            match state {
                StringState::Character => {
                    if characters.len() < characters_to_read {
                        characters.push(byte);
                        position += 1;
                    }

                    if characters.len() == characters_to_read {
                        break;
                    }
                } 
                StringState::DigitOrSeperator => {
                    match byte {
                        b'0'..=b'9' => {
                            characters_to_read = characters_to_read.checked_mul(10)
                                .ok_or_else(|| format_overflow_error(position, byte))?
                                .checked_add(byte as usize - b'0' as usize)
                                .ok_or_else(|| format_overflow_error(position, byte))?;
                            state = StringState::DigitOrSeperator;
                            position += 1;
                        }
                        b':' => {
                            characters = Vec::with_capacity(characters_to_read);
                            state = StringState::Character;
                            position += 1;
                        }
                        _ => { return Err(format_unexpected_character(byte, position, "b'0'..=b'9', b':'")); }
                    }
                },
                StringState::FirstDigit => {
                    match byte {
                        b'1'..=b'9' => {
                            characters_to_read = byte as usize - b'0' as usize;
                            state = StringState::DigitOrSeperator;
                            position += 1;
                        }
                        _ => { return Err(format_unexpected_character(byte, position, "b'1'..=b'9'")); }
                    }
                }
            }
        }

        Ok(BencodeString {
            start_position,
            continuation_position: position,
            value: characters
        })
    }

    fn decode_list(bytes: &[u8], start_position: usize) -> Result<BencodeList, BencodeError> {
        let mut position = start_position;
        let mut tokens: Vec<BencodeToken> = Vec::new();
        let mut state: ListState = ListState::Start;

        loop {
            let byte = match bytes.get(position) {
                Some(byte) => {
                    *byte
                },
                None => {
                    return Err(format_unexpected_eof(position));
                }
            };

            match state {
                ListState::Entry => {
                    match byte {
                        b'0'..=b'9' | b'i' | b'l' | b'd' => {
                            let token = Parser::decode_any(bytes, position)?;
                            position = Parser::get_continuation_position(&token);
                            tokens.push(token);
                        }
                        b'e' => {
                            position += 1;
                            break;
                        }
                        _ => { return Err(format_unexpected_character(byte, position, "b'0'..=b'9', b'i', 'b'l', b'd', b'e'")); }
                    }
                },
                ListState::Start => {
                    match byte {
                        b'l' => {
                            state = ListState::Entry;
                            position += 1;
                        }
                        _ => { return Err(format_unexpected_character(byte, position, "b'l'")); }
                    }
                }
            }
        }

        Ok(BencodeList {
            value: tokens,
            start_position,
            continuation_position: position
        })
    }

    fn decode_dictionary(bytes: &[u8], start_position: usize) -> Result<BencodeDictionary, BencodeError> {
        let mut position: usize = start_position;
        let mut state: DictionaryState = DictionaryState::Start;

        let mut keys: Vec<BencodeString> = Vec::new();
        let mut values: Vec<BencodeToken> = Vec::new();

        loop {
            let byte = match bytes.get(position) {
                Some(byte) => {
                    *byte
                },
                None => {
                    return Err(format_unexpected_eof(position));
                }
            };

            match state {
                DictionaryState::KeyEntry => {
                    match byte {
                        b'0'..=b'9' => {
                            let token = Parser::decode_string(bytes, position)?;

                            if keys.len() > 0 {
                                let last = keys.last().unwrap();
                                match last.value.cmp(&token.value) {
                                    std::cmp::Ordering::Less => (),
                                    std::cmp::Ordering::Equal => {
                                        return Err(BencodeError::new(BencodeErrorKind::MalformedData, format!("Duplicate key entries are not allowed for dictionary at position {}", position)));
                                    },
                                    std::cmp::Ordering::Greater => {
                                        return Err(BencodeError::new(BencodeErrorKind::MalformedData, format!("Key entries are not in lexicographical order at dictionary at position {}", position)));
                                    }
                                }
                            }

                            position = token.continuation_position;
                            keys.push(token);
                            state = DictionaryState::ValueEntry;
                        },
                        b'e' => {
                            position += 1;
                            break;
                        }
                        _ => { return Err(format_unexpected_character(byte, position, "b'0'..=b'9', b'e'")); }
                    }
                },
                DictionaryState::ValueEntry => {
                    match byte {
                        b'0'..=b'9' | b'i' | b'l' | b'd' => {
                            let token = Parser::decode_any(bytes, position)?;
                            position = Parser::get_continuation_position(&token);
                            values.push(token);
                            state = DictionaryState::KeyEntry;
                        }
                        _ => { return Err(format_unexpected_character(byte, position, "b'0'..=b'9', b'i', 'b'l', b'd'")); }
                    }
                },
                DictionaryState::Start => {
                    match byte {
                        b'd' => {
                            state = DictionaryState::KeyEntry;
                            position += 1;
                        }
                        _ => { return Err(format_unexpected_character(byte, position, "b'd'")); }
                    }
                },
            }
        }

        Ok(BencodeDictionary {
            keys: keys,
            values: values,
            start_position,
            continuation_position: position
        })
    }

    fn get_continuation_position(token: &BencodeToken) -> usize {
        match token {
            BencodeToken::String(value) => value.continuation_position,
            BencodeToken::List(value) => value.continuation_position,
            BencodeToken::Integer(value) => value.continuation_position,
            BencodeToken::Dictionary(value) => value.continuation_position,
        }
    }
}
