use std::io::Write;

use sha1::{Digest, Sha1};

use crate::{BencodeDictionary, BencodeInteger, BencodeList, BencodeString, BencodeToken};

fn hash_string(item: &BencodeString, hasher: &mut Sha1) {
    hasher.write_all(item.value.len().to_string().as_bytes()).unwrap();
    hasher.write_all(&[b':']).unwrap();
    hasher.write_all(&item.value).unwrap();
}

fn hash_integer(item: &BencodeInteger, hasher: &mut Sha1) {
    hasher.write_all(&[b'i']).unwrap();
    hasher.write_all(&item.value).unwrap();
    hasher.write_all(&[b'e']).unwrap();
}

fn hash_dictionary(item: &BencodeDictionary, hasher: &mut Sha1) {
    hasher.write_all(&[b'd']).unwrap();

    for (key, value) in &item.value {
        hash_string(key, hasher);
        hash_token(value, hasher);
    }

    hasher.write_all(&[b'e']).unwrap();
}

fn hash_list(item: &BencodeList, hasher: &mut Sha1) {
    hasher.write_all(&[b'l']).unwrap();

    for entry in &item.value {
        hash_token(entry, hasher);
    }

    hasher.write_all(&[b'e']).unwrap();
}

fn hash_token(container: &BencodeToken, hasher: &mut Sha1) {
    match container {
        BencodeToken::String(item) => hash_string(item, hasher),
        BencodeToken::List(item) => hash_list(item, hasher),
        BencodeToken::Integer(item) => hash_integer(item, hasher),
        BencodeToken::Dictionary(item) => hash_dictionary(item, hasher),
    }
}

pub fn calculate_info_hash(info: &BencodeDictionary) -> Vec<u8> {
    let mut hasher = Sha1::new();
    hash_dictionary(info, &mut hasher);
    hasher.finalize().to_vec()
}