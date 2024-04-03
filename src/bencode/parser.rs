use std::io::Bytes;
use std::io::Read;

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

// Iterator that allows you to query the current element multiple times.
struct RevisitableIterator<T> {
    byte: Option<u8>,
    position: Option<usize>,
    iterator: Bytes<T>
}

impl<T: Read> RevisitableIterator<T> {
    pub fn new(reader: T) -> Result<RevisitableIterator<T>, BencodeError> {
        let bytes = reader.bytes();

        let position = None;
        let byte = None;

        let mut iterator = RevisitableIterator {
            byte,
            position,
            iterator: bytes,
        };

        iterator.advance_init()?;
        Ok(iterator)
    }

    pub fn current_byte(&self) -> Option<u8> {
        self.byte
    }

    pub fn current_position(&self) -> Option<usize> {
        self.position
    }

    // Special case for the first iterator read where position should always be 0
    fn advance_init(&mut self) -> Result<Option<u8>, BencodeError> {
        match self.iterator.next() {
            Some(value) => {
                let byte = 
                    value.map_err(|e| BencodeError::new(BencodeErrorKind::MalformedData, e.to_string()))?;

                self.position = Some(0);
                self.byte = Some(byte);
                Ok(Some(byte))
            }
            None => {
                self.byte = None;
                self.position = None;
                Ok(None)
            }
        }
    }

    pub fn advance(&mut self) -> Result<Option<u8>, BencodeError> {
        match self.iterator.next() {
            Some(value) => {
                let byte = 
                    value.map_err(|e| BencodeError::new(BencodeErrorKind::MalformedData, e.to_string()))?;

                let position = self.position.unwrap();
                let position = position + 1;

                self.byte = Some(byte);
                self.position = Some(position);
                Ok(Some(byte))
            }   
            None => {
                self.byte = None;
                self.position = None;
                Ok(None)
            }
        }
    }
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
    pub fn from_reader<T: Read>(reader: T) -> Result<BencodeToken, BencodeError> {
        let mut iterator = RevisitableIterator::new(reader)?;
        let token = Parser::decode_any(&mut iterator)?;

        if let Some(_) = iterator.current_byte() {
            return Err(BencodeError::new(BencodeErrorKind::MalformedData, "Parser did not evaluate all elements of the byte stream. Results may be incomplete or wrong.".to_string()))
        }

        Ok(token)
    }

    fn decode_any<T: Read>(iterator: &mut RevisitableIterator<T>) -> Result<BencodeToken, BencodeError> {
        match iterator.current_byte() {
            Some(byte) => {
                match byte {
                    b'0'..=b'9' => Ok(BencodeToken::String(Parser::decode_string(iterator)?)),
                    b'i' => Ok(BencodeToken::Integer(Parser::decode_integer(iterator)?)),
                    b'l' => Ok(BencodeToken::List(Parser::decode_list(iterator)?)),
                    b'd' => Ok(BencodeToken::Dictionary(Parser::decode_dictionary(iterator)?)),
                    _ => { Err(BencodeError::new(BencodeErrorKind::MalformedData, format!("Unexpected character at position {}", iterator.current_position().unwrap()))) }
                }
            }
            None => Err(BencodeError::new(BencodeErrorKind::MalformedData, format!("Unexpected end of file at position {}", iterator.current_position().unwrap())))
        }
    }

    fn decode_string<T: Read>(iterator: &mut RevisitableIterator<T>) -> Result<BencodeString, BencodeError> {
        let start_position = iterator.current_position().unwrap();
        let mut state = StringState::FirstDigit;

        let mut length_buffer: Vec<u8> = Vec::new();
        let mut character_buffer: Vec<u8> = Vec::new();
        let characters_to_read: usize;

        loop {
            if let None = iterator.current_byte() {
                return Err(BencodeError::new(BencodeErrorKind::MalformedData, format!("Unexpected end of byte stream starting from position {}.", start_position)));
            }
    
            let byte = iterator.current_byte().unwrap();
            match byte {
                b'0' if matches!(state, StringState::FirstDigit) => {
                    length_buffer.push(byte);
                    state = StringState::Separator;

                    iterator.advance()?;
                },
                b'1'..=b'9' if matches!(state, StringState::FirstDigit) => {
                    length_buffer.push(byte);
                    state = StringState::Digit;
                    iterator.advance()?;
                },
                b'0'..=b'9' if matches!(state, StringState::Digit) => {
                    length_buffer.push(byte);
                    iterator.advance()?;
                },
                b':' if matches!(state, StringState::Digit) || matches!(state, StringState::Separator) => {
                    characters_to_read = std::str::from_utf8(&length_buffer)
                        .expect("Detected non UTF-8 string during string decode. This should never happen.")
                        .parse::<usize>()
                        .map_err(|err| BencodeError::new(BencodeErrorKind::MalformedData, err.to_string()))?;

                    iterator.advance()?;
                    break;
                },
                _ => {
                    return Err(BencodeError::new(BencodeErrorKind::MalformedData, format!("Unexpected character at position {}", iterator.current_position().unwrap())));
                }
            }
        }

        while character_buffer.len() < characters_to_read {
            if let None = iterator.current_byte() {
                return Err(BencodeError::new(BencodeErrorKind::MalformedData, format!("Unexpected end of byte stream starting from position {}.", start_position)));
            }
    
            let byte = iterator.current_byte().unwrap();
            character_buffer.push(byte);
            iterator.advance()?;
        }

        Ok(BencodeString {
            start_position,
            continuation_position: iterator.current_position(),
            value: character_buffer
        })
    }

    fn decode_integer<T: Read>(iterator: &mut RevisitableIterator<T>) -> Result<BencodeInteger, BencodeError> {
        let start_position = iterator.current_position().unwrap();

        let mut result: Vec<u8> = Vec::new();
        let mut state: IntegerState = IntegerState::Start;
    
        loop {
            if let None = iterator.current_byte() {
                return Err(BencodeError::new(BencodeErrorKind::MalformedData, format!("Unexpected end of byte stream starting from position {}.", start_position)));
            }
    
            let byte = iterator.current_byte().unwrap();
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
                    return Err(BencodeError::new(BencodeErrorKind::MalformedData, format!("Unexpected character at position {}", iterator.current_position().unwrap())));
                }
            }

            iterator.advance()?;
        }

        iterator.advance()?;

        Ok(BencodeInteger {
            value: result,
            start_position,
            continuation_position: iterator.current_position(),
        })
    }
    
    fn decode_list<T: Read>(iterator: &mut RevisitableIterator<T>) -> Result<BencodeList, BencodeError> {
        let start_position = iterator.current_position().unwrap();
    
        let mut tokens: Vec<BencodeToken> = Vec::new();
        let mut state: ListState = ListState::Start;
    
        loop {
            if let None = iterator.current_byte() {
                return Err(BencodeError::new(BencodeErrorKind::MalformedData, format!("Unexpected end of byte stream starting from position {}.", start_position)));
            }
    
            let byte = iterator.current_byte().unwrap();
            match byte {
                b'l' if matches!(state, ListState::Start) => {
                    state = ListState::Entry;
                    iterator.advance()?;
                }
                b'0'..=b'9' | b'i' | b'l' | b'd' if matches!(state, ListState::Entry) => {
                    let token = Parser::decode_any(iterator)?;
                    tokens.push(token);
                }
                b'e' if matches!(state, ListState::Entry) => {
                    break;
                }
                _ => {
                    return Err(BencodeError::new(BencodeErrorKind::MalformedData, format!("Unexpected character at position {}", iterator.current_position().unwrap())));
                }
            }
        }
    
        iterator.advance()?;

        Ok(BencodeList {
            value: tokens,
            start_position,
            continuation_position: iterator.current_position()
        })
    }
    
    fn decode_dictionary<T: Read>(iterator: &mut RevisitableIterator<T>) -> Result<BencodeDictionary, BencodeError> {
        let start_position = iterator.current_position().unwrap();
        let mut state: DictionaryState = DictionaryState::Start;

        let mut keys: Vec<BencodeString> = Vec::new();
        let mut values: Vec<BencodeToken> = Vec::new();

        loop {
            if let None = iterator.current_byte() {
                return Err(BencodeError::new(BencodeErrorKind::MalformedData, format!("Unexpected end of byte stream starting from position {}.", start_position)));
            }
    
            let byte = iterator.current_byte().unwrap();
            match byte {
                b'd' if matches!(state, DictionaryState::Start) => {
                    state = DictionaryState::KeyEntry;
                    iterator.advance()?;
                }
                b'0'..=b'9' if matches!(state, DictionaryState::KeyEntry) => {
                    let token = Parser::decode_string(iterator)?;

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
                    let value = Parser::decode_any(iterator)?;

                    values.push(value);
                    state = DictionaryState::KeyEntry;
                }
                b'e' if matches!(state, DictionaryState::KeyEntry) => {
                    break;
                }
                _ => {
                    return Err(BencodeError::new(BencodeErrorKind::MalformedData, format!("Unexpected character at position {}", iterator.current_position().unwrap())));
                }
            }
        }

        iterator.advance()?;

        Ok(BencodeDictionary {
            value: keys.into_iter().zip(values).collect(),
            start_position,
            continuation_position: iterator.current_position()
        })
    }

}
