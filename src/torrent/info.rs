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