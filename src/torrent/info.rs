use std::fmt::Write as FmtWrite;
use sha1::{Digest, Sha1};

use crate::BencodeDictionary;

pub fn calculate_info_hash(info: &BencodeDictionary, bytes: &[u8]) -> Vec<u8> {
    let mut hasher = Sha1::new();
    hasher.update(&bytes[info.start_position..info.continuation_position]);
    hasher.finalize().to_vec()
}

pub fn get_sha1_hexdigest(bytes: &[u8]) -> String {
    let mut output = String::new();
    for byte in bytes {
        write!(&mut output, "{:02x?}", byte).expect("Unable to write");
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    const EXPECTED_HASH: [u8; 20] = [79, 24, 196, 139, 13, 130, 147, 71, 144, 199, 252, 22, 35, 74, 190, 56, 163, 8, 18, 123];

    #[test]
    fn calculate_sha1_info_hash_should_succeed() {
        let input = vec![
            0x64, 0x36, 0x3A, 0x6C, 0x65, 0x6E, 0x67, 0x74, 0x68, 0x69, 0x33, 0x39,
            0x33, 0x33, 0x33, 0x39, 0x65, 0x34, 0x3A, 0x6E, 0x61, 0x6D, 0x65, 0x31,
            0x31, 0x3A, 0x65, 0x78, 0x61, 0x6D, 0x70, 0x6C, 0x65, 0x2E, 0x70, 0x6E,
            0x67, 0x31, 0x32, 0x3A, 0x70, 0x69, 0x65, 0x63, 0x65, 0x20, 0x6C, 0x65,
            0x6E, 0x67, 0x74, 0x68, 0x69, 0x35, 0x32, 0x34, 0x32, 0x38, 0x38, 0x65,
            0x36, 0x3A, 0x70, 0x69, 0x65, 0x63, 0x65, 0x73, 0x32, 0x30, 0x3A, 0x3D,
            0x03, 0xE5, 0x59, 0x31, 0x44, 0x14, 0x52, 0xF6, 0x2F, 0x9D, 0xA1, 0x9B,
            0x61, 0xEB, 0xD4, 0x40, 0x58, 0xE3, 0xFF, 0x65
        ];

        let info_token = BencodeDictionary {
            keys: Vec::new(),
            values: Vec::new(),
            start_position: 0,
            continuation_position: input.len()
        };

        assert_eq!(EXPECTED_HASH, *calculate_info_hash(&info_token, &input))
    }

    #[test]
    fn get_sha1_hexdigest_should_succeed() {
        assert_eq!("4f18c48b0d82934790c7fc16234abe38a308127b", get_sha1_hexdigest(&EXPECTED_HASH))
    }
}