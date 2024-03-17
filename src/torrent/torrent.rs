use std::fmt::Write;

use crypto::{digest::Digest, sha1::Sha1};

use crate::bencode::{BencodeDictionary, BencodeError, BencodeErrorKind, BencodeList, BencodeString, BencodeToken, Parser};
use super::{error::TorrentErrorKind, TorrentError};

#[derive(Debug)]
pub struct Torrent {
    pub announce: String,
    pub announce_list: Option<Vec<Vec<String>>>,
    pub info: Info,
    pub creation_date: Option<i64>,
    pub comment: Option<String>,
    pub created_by: Option<String>,
    // Not a field in the exported torrent file
    // But needs to be calculated before token disposal since we do not support writing back out... yet?
    pub info_hash: Vec<u8>
}

#[derive(Debug)]
pub struct File {
    pub length: u64,
    pub path: Vec<String>
}

#[derive(Debug)]
pub struct Info {
    pub name: String,
    pub length: Option<u64>,
    pub files: Option<Vec<File>>,
    pub piece_length: u64,
    pub pieces: Vec<Vec<u8>>,
    pub private: Option<i64>
}

// Converter
impl Torrent {
    pub fn from_bytes(bytes: &[u8]) -> Result<Torrent, TorrentError> {
        let token = match Parser::decode(bytes) {
            Ok(token) => token,
            Err(err) => return Err(TorrentError::new(TorrentErrorKind::MalformedData, format!("{}", &err.message))),
        };

        if let BencodeToken::Dictionary(root) = token {
            return Torrent::evaluate_root(&root, &bytes);
        }

        Err(TorrentError::new(TorrentErrorKind::MalformedData, format!("Unexpected token at root. Expected dictionary token")))
    }

    fn evaluate_root(root: &BencodeDictionary, bytes: &[u8]) -> Result<Torrent, TorrentError> {
        // Required
        let announce = root.find_string_value("announce")
            .map_err(|err| Torrent::convert_error(err))?
            .as_utf8()
            .map_err(|err| Torrent::convert_error(err))?
            .to_string();
        
        let info = root.find_dictionary_value("info")
            .map_err(|err| Torrent::convert_error(err))?;

        // Get Info Hash
        let mut hasher = Sha1::new();
        let mut info_hash = vec![0u8; 20];

        hasher.input(&bytes[info.start_position..=info.end_position]);
        hasher.result(&mut info_hash);

        // Evaluate Info
        let info = Torrent::evaluate_info(info)?;

        // Optional
        let announce_list = if let Ok(value) = root.find_list_value("announce-list") {
            Some(Torrent::evaluate_announce(value)?)
        } else { None };

        let creation_date = if let Ok(value) = root.find_integer_value("creation date") {
            Some(value.evaluate().map_err(|err| Torrent::convert_error(err))?)
        } else { None };

        let comment = if let Ok(value) = root.find_string_value("comment") {
            Some(value.as_utf8().map_err(|err| Torrent::convert_error(err))?.to_string())
        } else { None };

        let created_by = if let Ok(value) = root.find_string_value("created by") {
            Some(value.as_utf8().map_err(|err| Torrent::convert_error(err))?.to_string())
        } else { None };

        Ok(Torrent {
            announce,
            announce_list,
            info,
            creation_date,
            comment,
            created_by,
            info_hash
        })        
    }

    fn evaluate_info(info: &BencodeDictionary) -> Result<Info, TorrentError> {
        let name = info.find_string_value("name")
            .map_err(|err| Torrent::convert_error(err))?
            .as_utf8()
            .map_err(|err| Torrent::convert_error(err))?
            .to_string();

        let pieces = info.find_string_value("pieces")
            .map_err(|err| Torrent::convert_error(err))?
            .value
            .chunks(20)
            .map(|slice| slice.to_vec())
            .collect();

        let piece_length = info.find_integer_value("piece length")
            .map_err(|err| Torrent::convert_error(err))?
            .evaluate()
            .map_err(|err| Torrent::convert_error(err))?;

        // One or the other is required, but not both or neither.
        let length = info.find_integer_value("length");
        let files = info.find_list_value("files");
        
        if length.is_ok() && files.is_ok() {
            return Err(TorrentError::new(TorrentErrorKind::MalformedData, format!("Info contains length and file properties. Only one must be present.")));
        }

        if length.is_err() && files.is_err() {
            return Err(TorrentError::new(TorrentErrorKind::MalformedData, format!("Info does not contain length or file properties. One must be present.")));
        }

        let length = if let Ok(length) = length {
            Some(length.evaluate().map_err(|err| Torrent::convert_error(err))?)
        } else { None };

        let files = if let Ok(files) = files {
            Some(Torrent::evaluate_files(files)?)
        } else { None };

        // Optional
        let private = if let Ok(value) = info.find_integer_value("private") {
            Some(value.evaluate().map_err(|err| Torrent::convert_error(err))?)
        } else { None };

        Ok(Info {
            name,
            files,
            length,
            piece_length,
            pieces,
            private
        })
    }

    fn evaluate_announce(announce: &BencodeList) -> Result<Vec<Vec<String>>, TorrentError> {
        let mut announce_result: Vec<Vec<String>> = Vec::new();

        for entry in &announce.value {
            match entry {
                BencodeToken::List(tier) => {
                    let mut tier_result: Vec<String> = Vec::new();

                    for tracker_entry in &tier.value {
                        match tracker_entry {
                            BencodeToken::String(tracker) => {
                                let result = tracker.as_utf8()
                                    .map_err(|err| Torrent::convert_error(err))?
                                    .to_string();

                                tier_result.push(result);
                            },
                            _ => {
                                return Err(TorrentError::new(TorrentErrorKind::MalformedData, "Unexpected token in tracker tier list. Expected a string token.".to_string()));
                            }
                        }
                    }

                    announce_result.push(tier_result);
                },
                _ => {
                    return Err(TorrentError::new(TorrentErrorKind::MalformedData, "Unexpected token in tracker announce list. Expected a list token.".to_string()));
                }
            }
        }

        Ok(announce_result)
    }

    fn evaluate_files(files: &BencodeList) -> Result<Vec<File>, TorrentError> {
        let mut files_result: Vec<File> = Vec::new(); 

        for file_entry in &files.value {
            match file_entry {
                BencodeToken::Dictionary(file) => {
                    let result = Torrent::evaluate_file(file)?;
                    files_result.push(result);
                },
                _ => {
                    return Err(TorrentError::new(TorrentErrorKind::MalformedData, "Unexpected token in files list. Expected a dictionary token.".to_string()));
                }
            }
        }

        Ok(files_result)
    }

    fn evaluate_file(file: &BencodeDictionary) -> Result<File, TorrentError> {
        let length = file.find_integer_value("length")
            .map_err(|err| Torrent::convert_error(err))?
            .evaluate::<u64>()
            .map_err(|err| Torrent::convert_error(err))?;
    
        let paths = file.find_list_value("path")
            .map_err(|err| Torrent::convert_error(err))?;

        let mut result_paths: Vec<String> = Vec::new();

        for path_entry in &paths.value {
            match path_entry {
                BencodeToken::String(path) => {
                    let result = path.as_utf8()
                        .map_err(|err| Torrent::convert_error(err))?
                        .to_string();

                    result_paths.push(result);
                },
                _ => {
                    return Err(TorrentError::new(TorrentErrorKind::MalformedData, "Unexpected token in path list. Expected a string token.".to_string()));
                }
            }
        }

        Ok(File {
            length,
            path: result_paths
        })
    }

    fn convert_error(err: BencodeError) -> TorrentError {
        let kind = match err.kind {
            BencodeErrorKind::MalformedData => TorrentErrorKind::MalformedData
        };

        TorrentError::new(kind, err.message)
    }
}