use crypto::{digest::Digest, sha1::Sha1};

use crate::bencode::{BencodeDictionary, BencodeList, BencodeString, BencodeToken, Parser};
use super::{error::TorrentErrorKind, TorrentError};

#[derive(Debug)]
pub struct Torrent {
    pub announce: String,
    pub announce_list: Option<Vec<Vec<String>>>,
    pub info: Info,
    pub creation_date: Option<isize>,
    pub comment: Option<String>,
    pub created_by: Option<String>,
    // Not a field in the exported torrent file
    // But needs to be calculated before token disposal since we do not support writing back out... yet?
    pub info_hash: String
}

#[derive(Debug)]
pub struct File {
    pub length: usize,
    pub path: Vec<String>
}

#[derive(Debug)]
pub struct Info {
    pub name: String,
    pub length: Option<usize>,
    pub files: Option<Vec<File>>,
    pub piece_length: usize,
    pub pieces: Vec<u8>,
    pub private: Option<isize>
}

// Converter
impl Torrent {
    pub fn from_bytes(bytes: &[u8]) -> Result<Torrent, TorrentError> {
        let token = match Parser::decode(bytes) {
            Ok(token) => token,
            Err(err) => return Err(TorrentError::new(TorrentErrorKind::MalformedData, format!("{}", &err.message))),
        };

        if let BencodeToken::Dictionary(root) = token {
            let announce_token = root.find_string_value("announce")
                .map_err(|err| TorrentError::new(TorrentErrorKind::MalformedData, format!("{}", &err.message)))?;

            let info_token = root.find_dictionary_value("info")
                .map_err(|err| TorrentError::new(TorrentErrorKind::MalformedData, format!("{}", &err.message)))?;

            let announce_list_token = root.find_list_value("announce-list").ok();
            let creation_date_token = root.find_integer_value("creation date").ok();
            let comment_token = root.find_string_value("comment").ok();
            let created_by_token = root.find_string_value("created by").ok();

            // Convert torrent tokens to usable values
            let announce = Torrent::utf8_string_token(&announce_token)?;
            let info = Torrent::info_dictionary(&info_token)?;

            let announce_list = Torrent::announce_list_optional(announce_list_token)?;
            let creation_date = if let Some(value) = creation_date_token { Some(value.value) } else { None };
            let comment = Torrent::utf8_string_token_optional(comment_token)?;
            let created_by = Torrent::utf8_string_token_optional(created_by_token)?;

            // Get Info Hash
            let mut hasher = Sha1::new();
            hasher.input(&bytes[info_token.start_position..=info_token.end_position]);
            let info_hash = hasher.result_str();

            return Ok(Torrent {
                announce,
                announce_list,
                info,
                creation_date,
                comment,
                created_by,
                info_hash
            });
        }

        Err(TorrentError::new(TorrentErrorKind::MalformedData, format!("Root token is not a dictionary token.")))
    }

    fn info_dictionary(info: &BencodeDictionary) -> Result<Info, TorrentError> {
        let name_token = info.find_string_value("name")
            .map_err(|err| TorrentError::new(TorrentErrorKind::MalformedData, format!("{}", &err.message)))?;

        let length_token = info.find_integer_value("length").ok();
        let files_token = info.find_list_value("files").ok();

        let piece_length_token = info.find_integer_value("piece length")
            .map_err(|err| TorrentError::new(TorrentErrorKind::MalformedData, format!("{}", &err.message)))?;

        let pieces_token = info.find_string_value("pieces")
            .map_err(|err| TorrentError::new(TorrentErrorKind::MalformedData, format!("{}", &err.message)))?;

        let private_token = info.find_integer_value("private").ok();
        
        // Convert info tokens to usable values
        let name = Torrent::utf8_string_token(name_token)?;
        let private = if let Some(value) = private_token { Some(value.value) } else { None };

        let mut length = None;
        let mut files = None;

        if length_token.is_some() && files_token.is_some() {
            return Err(TorrentError::new(TorrentErrorKind::MalformedData, format!("Info dictionary contains properties of a single-file and multiple-file torrent. It can only be one.")))
        }

        if length_token.is_none() && files_token.is_none() {
            return Err(TorrentError::new(TorrentErrorKind::MalformedData, format!("Info dictionary does not contain properties of a single-file or multiple-file torrent.")));
        }

        if let Some(length_value) = length_token {
            if length_value.value <= 0 {
                return Err(TorrentError::new(TorrentErrorKind::MalformedData, format!("Length must be a non-zero positive number.")));
            }

            length = Some(length_value.value as usize);
        }

        if let Some(files_value) = files_token {
            let result = Torrent::file_list(files_value)?;

            if result.len() == 0 {
                return Err(TorrentError::new(TorrentErrorKind::MalformedData, format!("Files must contain at-least one file.")));
            }

            files = Some(result);
        }

        // Validate some shit
        if piece_length_token.value <= 0 {
            return Err(TorrentError::new(TorrentErrorKind::MalformedData, format!("Piece length must be a non-zero positive number.")));
        }
        let piece_length = piece_length_token.value as usize;

        if pieces_token.value.len() == 0 {
            return Err(TorrentError::new(TorrentErrorKind::MalformedData, format!("Number of pieces must be a non-zero positive number.")));
        }
        let pieces = pieces_token.value.clone(); // TODO: Remove this clone

        let total_size = match &files {
            Some(multiple) => {
                multiple.iter()
                    .map(|file| file.length as usize)
                    .sum()
            },
            None => {
                length.unwrap() as usize
            }
        };

        let upper_bound = (total_size as f64 / piece_length as f64).ceil() as usize;
        let lower_bound = (total_size as f64 / piece_length as f64).floor() as usize;
        let pieces_count = (pieces.len() as f64 / 20.0).ceil() as usize;

        if !(lower_bound < pieces_count && pieces_count <= upper_bound || lower_bound == pieces_count && pieces_count == upper_bound) {
            // Make sure there is a final hash (even if not complete, within this bound, it should never be equal or lower to the lower bound.)
            // However, if there is an equal divisor, then upper and lower bound should be identical to number of pieces.
            return Err(TorrentError::new(TorrentErrorKind::MalformedData, format!("Piece count does not fall with-in the expected piece boundary.")));
        }

        Ok(Info {
            name,
            length,
            files,
            piece_length,
            pieces,
            private,
        })
    }

    fn announce_list_optional(token: Option<&BencodeList>) -> Result<Option<Vec<Vec<String>>>, TorrentError> {
        if let Some(token) = token {
            return Ok(Some(Torrent::announce_list(token)?));
        }

        Ok(None)
    }

    fn announce_list(announce_list_token: &BencodeList) -> Result<Vec<Vec<String>>, TorrentError> {
        let mut results: Vec<Vec<String>> = Vec::new();

        for list_entry_token in &announce_list_token.value {
            match list_entry_token {
                BencodeToken::List(list_token) => {
                    let mut sublist_result: Vec<String> = Vec::new();

                    for sublist_entry_token in &list_token.value {
                        match sublist_entry_token {
                            BencodeToken::String(string_value) => {
                                sublist_result.push(Torrent::utf8_string_token(string_value)?);
                            },
                            _ => return Err(TorrentError::new(TorrentErrorKind::MalformedData, format!("Announce list sub-list token is not a string token.")))
                        }
                    }
        
                    results.push(sublist_result);
                }
                _ => return Err(TorrentError::new(TorrentErrorKind::MalformedData, format!("Announce list token is not a list token.")))
            }
        }

        Ok(results)
    }

    fn file_list(files_token: &BencodeList) -> Result<Vec<File>, TorrentError> {
        let mut results: Vec<File> = Vec::new();

        for file_token in &files_token.value {
            match file_token {
                BencodeToken::Dictionary(files) => {
                    let file_result = Torrent::file_dictionary(files)?;
                    results.push(file_result);
                },
                _ => return Err(TorrentError::new(TorrentErrorKind::MalformedData, format!("File token is not a dictionary token.")))
            }
        }

        Ok(results)
    }

    fn file_dictionary(file_token: &BencodeDictionary) -> Result<File, TorrentError> {
        // Validate Length
        let length_token = file_token.find_integer_value("length")
            .map_err(|err| TorrentError::new(TorrentErrorKind::MalformedData, format!("{}", &err.message)))?;

        if length_token.value <= 0 {
            return Err(TorrentError::new(TorrentErrorKind::MalformedData, format!("File entry at position {} is not a non-zero positive number.", length_token.value)));
        }

        // Get the path for this file
        let path_token = file_token.find_list_value("path")
            .map_err(|err| TorrentError::new(TorrentErrorKind::MalformedData, format!("{}", &err.message)))?;

        let mut result_paths: Vec<String> = Vec::new();
        for path_entry_token in &path_token.value {
            match path_entry_token {
                BencodeToken::String(value) => {
                    result_paths.push(Torrent::utf8_string_token(value)?)
                }
                _ => return Err(TorrentError::new(TorrentErrorKind::MalformedData, format!("Path token is not a string token.")))
            }
        }

        Ok(File {
            length: length_token.value as usize,
            path: result_paths
        })
    }

    fn utf8_string_token_optional(token: Option<&BencodeString>) -> Result<Option<String>, TorrentError> {
        if let Some(token) = token {
            return Ok(Some(Torrent::utf8_string_token(token)?));
        }

        Ok(None)
    }

    fn utf8_string_token(token: &BencodeString) -> Result<String, TorrentError> {
        String::from_utf8(token.value.clone())
            .map_err(|_| TorrentError::new(TorrentErrorKind::MalformedData, format!("String token at position {} is not valid UTF-8", token.start_position)))
    }


}