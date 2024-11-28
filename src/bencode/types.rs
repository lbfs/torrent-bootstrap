use std::str::from_utf8;
use super::{error::BencodeErrorKind, BencodeError};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BencodeString {
    pub value: Vec<u8>,
    pub start_position: usize,
    pub continuation_position: usize
}

impl BencodeString {
    pub fn as_utf8<'a>(&'a self) -> Result<&'a str, BencodeError> {
        let value = from_utf8(&self.value)
            .map_err(|err| BencodeError::new(BencodeErrorKind::MalformedData, err.to_string()));

        value
    }
}

#[derive(Debug, Clone)]
pub struct BencodeInteger {
    pub value: i64,
    pub start_position: usize,
    pub continuation_position: usize
}

#[derive(Debug, Clone)]
pub struct BencodeList {
    pub value: Vec<BencodeToken>,
    pub start_position: usize,
    pub continuation_position: usize
}

#[derive(Debug, Clone)]
pub struct BencodeDictionary {
    pub keys: Vec<BencodeString>,
    pub values: Vec<BencodeToken>,
    pub start_position: usize,
    pub continuation_position: usize
}

#[derive(Debug, Clone)]
pub enum BencodeToken {
    String(BencodeString),
    List(BencodeList),
    Integer(BencodeInteger),
    Dictionary(BencodeDictionary)
}

impl BencodeDictionary {
    pub fn find_dictionary_value<'a>(&'a self, target_key: &str) -> Result<&'a BencodeDictionary, BencodeError> {
        let token = BencodeDictionary::find_value_required(target_key, self)?;

        if let BencodeToken::Dictionary(value) = token {
            return Ok(value);
        }

        Err(BencodeError::new(BencodeErrorKind::MalformedData, format!("Required key {} is not a dictionary", target_key)))
    }

    pub fn find_list_value<'a>(&'a self, target_key: &str) -> Result<&'a BencodeList, BencodeError> {
        let token = BencodeDictionary::find_value_required(target_key, self)?;

        if let BencodeToken::List(value) = token {
            return Ok(value);
        }

        Err(BencodeError::new(BencodeErrorKind::MalformedData, format!("Required key {} is not a list", target_key)))
    }

    pub fn find_integer_value<'a>(&'a self, target_key: &str) -> Result<&'a BencodeInteger, BencodeError> {
        let token = BencodeDictionary::find_value_required(target_key, self)?;

        if let BencodeToken::Integer(value) = token {
            return Ok(value);
        }

        Err(BencodeError::new(BencodeErrorKind::MalformedData, format!("Required key {} is not a integer", target_key)))
    }

    pub fn find_string_value<'a>(&'a self, target_key: &str) -> Result<&'a BencodeString, BencodeError> {
        let token = BencodeDictionary::find_value_required(target_key, self)?;

        if let BencodeToken::String(value) = token {
            return Ok(value);
        }

        Err(BencodeError::new(BencodeErrorKind::MalformedData, format!("Required key {} is not a string", target_key)))
    }

    fn find_value_required<'a>(target_key: &str, dictionary: &'a BencodeDictionary) -> Result<&'a BencodeToken, BencodeError> {
        if let Some(value) = BencodeDictionary::find_value(target_key, dictionary) {
            return Ok(value);
        }

        Err(BencodeError::new(BencodeErrorKind::MalformedData, format!("Required key {} is not found in dictionary", target_key)))
    }

    fn find_value<'a>(target_key: &str, dictionary: &'a BencodeDictionary) -> Option<&'a BencodeToken> {
        for (token_key, token_value) in dictionary.keys.iter().zip(&dictionary.values) {
            if target_key.as_bytes().cmp(&token_key.value).is_eq() {
                return Some(token_value);
            }
        }
       
        None
    }
}