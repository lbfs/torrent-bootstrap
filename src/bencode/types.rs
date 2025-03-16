use super::{error::BencodeErrorKind, BencodeError};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BencodeString {
    pub value: Vec<u8>,
    pub start_position: usize,
    pub continuation_position: usize
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BencodeInteger {
    pub value: i128,
    pub start_position: usize,
    pub continuation_position: usize
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BencodeList {
    pub value: Vec<BencodeToken>,
    pub start_position: usize,
    pub continuation_position: usize
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BencodeDictionary {
    pub keys: Vec<BencodeString>,
    pub values: Vec<BencodeToken>,
    pub start_position: usize,
    pub continuation_position: usize
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum BencodeToken {
    String(BencodeString),
    List(BencodeList),
    Integer(BencodeInteger),
    Dictionary(BencodeDictionary)
}

impl BencodeDictionary {
    pub fn find_dictionary_value<'a>(&'a self, target_key: &[u8]) -> Result<&'a BencodeDictionary, BencodeError> {
        let token = self.find_value_required(target_key)?;

        if let BencodeToken::Dictionary(value) = token {
            return Ok(value);
        }

        let target_key = String::from_utf8_lossy(target_key).to_owned();
        Err(BencodeError::new(BencodeErrorKind::MalformedData, format!("Required key {} is not a dictionary", target_key)))
    }

    pub fn find_list_value<'a>(&'a self, target_key: &[u8]) -> Result<&'a BencodeList, BencodeError> {
        let token = self.find_value_required(target_key)?;

        if let BencodeToken::List(value) = token {
            return Ok(value);
        }

        let target_key = String::from_utf8_lossy(target_key).to_owned();
        Err(BencodeError::new(BencodeErrorKind::MalformedData, format!("Required key {} is not a list", target_key)))
    }

    pub fn find_integer_value<'a>(&'a self, target_key: &[u8]) -> Result<&'a BencodeInteger, BencodeError> {
        let token = self.find_value_required(target_key)?;

        if let BencodeToken::Integer(value) = token {
            return Ok(value);
        }

        let target_key = String::from_utf8_lossy(target_key).to_owned();
        Err(BencodeError::new(BencodeErrorKind::MalformedData, format!("Required key {} is not a integer", target_key)))
    }

    pub fn find_string_value<'a>(&'a self, target_key: &[u8]) -> Result<&'a BencodeString, BencodeError> {
        let token = self.find_value_required(target_key)?;

        if let BencodeToken::String(value) = token {
            return Ok(value);
        }

        let target_key = String::from_utf8_lossy(target_key).to_owned();
        Err(BencodeError::new(BencodeErrorKind::MalformedData, format!("Required key {} is not a string", target_key)))
    }

    fn find_value_required<'a>(&'a self, target_key: &[u8]) -> Result<&'a BencodeToken, BencodeError> {
        if let Some(value) = self.find_value(target_key) {
            return Ok(value);
        }

        let target_key = String::from_utf8_lossy(target_key).to_owned();
        Err(BencodeError::new(BencodeErrorKind::MalformedData, format!("Required key {} is not found in dictionary", target_key)))
    }

    fn find_value<'a>(&'a self, target_key: &[u8]) -> Option<&'a BencodeToken> {
        for (token_key, token_value) in self.keys.iter().zip(&self.values) {
            if target_key.cmp(&token_key.value).is_eq() {
                return Some(token_value);
            }
        }
       
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn get_string_key() -> BencodeString {
        BencodeString { 
            value: b"string".to_vec(), 
            start_position: 0, 
            continuation_position: 0
        }
    }

    fn get_integer_key() -> BencodeString {
        BencodeString { 
            value: b"integer".to_vec(), 
            start_position: 0, 
            continuation_position: 0
        }
    }

    fn get_list_key() -> BencodeString {
        BencodeString { 
            value: b"list".to_vec(), 
            start_position: 0, 
            continuation_position: 0
        }
    }

    fn get_dictionary_key() -> BencodeString {
        BencodeString { 
            value: b"dictionary".to_vec(), 
            start_position: 0, 
            continuation_position: 0
        }
    }

    fn get_string_value() -> BencodeString {
        BencodeString { 
            value: b"helloworld".to_vec(), 
            start_position: 0, 
            continuation_position: 0
        }
    }

    fn get_integer_value() -> BencodeInteger {
        BencodeInteger { 
            value: 0, 
            start_position: 0, 
            continuation_position: 0
        }
    }

    fn get_list_value() -> BencodeList {
        BencodeList { 
            value: Vec::new(), 
            start_position: 0, 
            continuation_position: 0
        }
    }

    fn get_dictionary_value() -> BencodeDictionary {
        BencodeDictionary { 
            keys: Vec::new(),
            values: Vec::new(), 
            start_position: 0, 
            continuation_position: 0
        }
    }

    fn get_test_data() -> BencodeDictionary {
        BencodeDictionary {
            keys: vec![
                get_string_key(),
                get_integer_key(),
                get_list_key(),
                get_dictionary_key()
            ],
            values: vec![
                BencodeToken::String(get_string_value()),
                BencodeToken::Integer(get_integer_value()),
                BencodeToken::List(get_list_value()),
                BencodeToken::Dictionary(get_dictionary_value())
            ],
            start_position: 0,
            continuation_position: 0
        }
    }

    #[test]
    fn find_string_valid_should_succeed() {
        let token = get_test_data();
        let actual = token.find_string_value(b"string");
        let actual = actual.unwrap();

        let expected = get_string_value();
        assert_eq!(expected, *actual);
    }

    #[test]
    fn find_string_invalid_type_should_fail() {
        let token = get_test_data();
        let actual = token.find_string_value(b"integer");
        
        assert!(actual.is_err());
    }

    #[test]
    fn find_string_unknown_key_should_fail() {
        let token = get_test_data();
        let actual = token.find_string_value(b"unknown");
        
        assert!(actual.is_err());
    }

    #[test]
    fn find_integer_valid_should_succeed() {
        let token = get_test_data();
        let actual = token.find_integer_value(b"integer");
        let actual = actual.unwrap();

        let expected = get_integer_value();
        assert_eq!(expected, *actual);
    }

    #[test]
    fn find_integer_invalid_type_should_fail() {
        let token = get_test_data();
        let actual = token.find_integer_value(b"string");
        
        assert!(actual.is_err());
    }

    #[test]
    fn find_integer_unknown_key_should_fail() {
        let token = get_test_data();
        let actual = token.find_integer_value(b"unknown");
        
        assert!(actual.is_err());
    }

    #[test]
    fn find_list_valid_should_succeed() {
        let token = get_test_data();
        let actual = token.find_list_value(b"list");
        let actual = actual.unwrap();

        let expected = get_list_value();
        assert_eq!(expected, *actual);
    }

    #[test]
    fn find_list_invalid_type_should_fail() {
        let token = get_test_data();
        let actual = token.find_list_value(b"string");
        
        assert!(actual.is_err());
    }

    #[test]
    fn find_list_unknown_key_should_fail() {
        let token = get_test_data();
        let actual = token.find_list_value(b"unknown");
        
        assert!(actual.is_err());
    }

    #[test]
    fn find_dictionary_valid_should_succeed() {
        let token = get_test_data();
        let actual = token.find_dictionary_value(b"dictionary");
        let actual = actual.unwrap();

        let expected = get_dictionary_value();
        assert_eq!(expected, *actual);
    }

    #[test]
    fn find_dictionary_invalid_type_should_fail() {
        let token = get_test_data();
        let actual = token.find_dictionary_value(b"string");
        
        assert!(actual.is_err());
    }

    #[test]
    fn find_dictionary_unknown_key_should_fail() {
        let token = get_test_data();
        let actual = token.find_dictionary_value(b"unknown");
        
        assert!(actual.is_err());
    }
}