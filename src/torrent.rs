use std::{fmt::Write, io::ErrorKind, path::{Path, PathBuf}};

use crypto::{digest::Digest, sha1::Sha1};
use serde::Deserialize;
use crate::bencode::{Bencode, BencodeToken};

#[derive(Debug, Deserialize, Clone)]
struct IntermediateFile {
    length: i64,
    path: Vec<String>
}

#[derive(Debug, Deserialize)]
struct IntermediateInfo {
    files: Option<Vec<IntermediateFile>>,
    name: String,
    #[serde(alias = "piece length")]
    piece_length: i64,
    pieces: Vec<u8>,
    length: Option<i64>
}

#[derive(Debug, Deserialize)]
struct IntermediateTorrent {
    info: IntermediateInfo
}

// TODO: This will require a bigger refactor, let's create an intermediary Torrent type that models the actual file, but then report the original structure
// when lava torrent was still in-place.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct File {
    pub length: u64,
    pub path: PathBuf
}

#[derive(Debug, Clone)]
pub struct PieceFile {
    pub read_length: u64,
    pub read_start_position: u64,
    pub file_path: PathBuf,
    pub file_length: u64,
}

#[derive(Debug, Clone)]
pub struct Piece {
    pub position: usize,
    pub files: Vec<PieceFile>,
    pub piece_hash: String,
    pub length: u64,
}

#[derive(Debug)]
pub struct Torrent {
    pub files: Vec<File>,
    pub pieces: Vec<Piece>,
    pub piece_length: usize,
    pub info_hash: String,
    pub path: PathBuf
}

impl Torrent {
    pub fn from(path: &Path) -> Result<Torrent, std::io::Error> {
        let bytes = std::fs::read(path)
            .map_err(|err| std::io::Error::new(ErrorKind::InvalidData, err.to_string()))?;       

        let tokens = match Bencode::decode(&bytes) {
            Ok(tokens) => tokens,
            Err(_) => return Err(std::io::Error::new(ErrorKind::InvalidData, "Unable to decode bencoded data."))
        };

        // TODO: Find a way to remove this...? I don't like having to reference the underlying structure when we're going to deserialize it...
        let info_hash = {
            let value = match &tokens {
                BencodeToken::Dictionary(root) => {
                    let info = &root.value;
                    let found = info
                        .iter()
                        .find(|key| key.0.value.as_slice().cmp("info".as_bytes()).is_eq());

                    if found.is_none() {
                        Err(std::io::Error::new(ErrorKind::InvalidData, "Info token is not in dictionary."))
                    } else {
                        let (_, value) = found.unwrap();
                        match value {
                            BencodeToken::Dictionary(info) => {
                                let mut hasher = Sha1::new();
                                hasher.input(&bytes[info.start_position..=info.end_position]);
                                Ok(hasher.result_str())
                            },
                            _ => Err(std::io::Error::new(ErrorKind::InvalidData, "Info token is not a dictionary."))
                        }
                    }
                },
                _ => Err(std::io::Error::new(ErrorKind::InvalidData, "Root token is not a dictionary."))
            };

            value?
        };

        let value = serde_json::to_value(tokens)
            .map_err(|err| std::io::Error::new(ErrorKind::InvalidData, err.to_string()))?;   

        let torrent: IntermediateTorrent = serde_json::from_value(value)
            .map_err(|err| std::io::Error::new(ErrorKind::InvalidData, err.to_string()))?;       

        let total_size: u64 = match &torrent.info.files {
            Some(multiple) => {
                multiple.iter()
                    .map(|file| file.length as u64)
                    .sum()
            },
            None => {
                match torrent.info.length {
                    Some(length) => length as u64,
                    None => return Err(std::io::Error::new(ErrorKind::InvalidData, "Unable to detect total file length in torrent."))
                }
            }
        };

        let upper_bound = (total_size as f64 / torrent.info.piece_length as f64).ceil() as u64;
        let lower_bound = (total_size as f64 / torrent.info.piece_length as f64).floor() as u64;
        let pieces_count = (torrent.info.pieces.len() as f64 / 20.0).ceil() as u64;

        if !(lower_bound < pieces_count && pieces_count <= upper_bound) && !(lower_bound == pieces_count && pieces_count == upper_bound) {
            // Make sure there is a final hash (even if not complete, within this bound, it should never be equal or lower to the lower bound.)
            // However, if there is an equal divisor, then upper and lower bound should be identical to number of pieces.
            return Err(std::io::Error::new(ErrorKind::InvalidData, "Piece count does not fall with-in the expected piece boundary. This torrent is malformed."))
        }

        let piece_hashes: Vec<Vec<u8>> = torrent.info.pieces
            .chunks(20)
            .map(|slice| slice.to_vec())
            .collect();

        let files = Torrent::format_files(&torrent, total_size);
        let pieces = Torrent::construct_pieces(&piece_hashes, &files, &torrent);
        let pieces_len = pieces.len();

        Ok(Torrent {
            files: files,
            pieces: pieces,
            piece_length: pieces_len, // this is wrong..., rename this when we refactor,
            info_hash: info_hash,
            path: path.to_path_buf()
        })
    }

    fn format_content_layout(torrent: &IntermediateTorrent, paths: Option<&PathBuf>) -> PathBuf {
        let mut content_path = PathBuf::from(&torrent.info.name);

        if let Some(paths) = paths {
            for path  in paths {
                content_path.push(Path::new(path));
            } 
        }

        content_path
    }

    fn format_files(torrent: &IntermediateTorrent, total_length: u64) -> Vec<File> {
        let files = torrent.info.files.clone();
        let files = match files {
            Some(files) => {
                files.into_iter().map(|file| {
                    File {
                        path: Torrent::format_content_layout(torrent, Some(&file.path.iter().collect())),
                        length: file.length as u64,
                    }
                })
                .collect()
            },
            None => {
                let mut files = Vec::new();
                files.push(File {
                    path: Torrent::format_content_layout(torrent, None),
                    length: total_length
                });
                files
            }
        };


        files
    }
    
    fn get_sha1_hexdigest(bytes: &[u8]) -> String {
        let mut output = String::new();
        for byte in bytes {
            write!(&mut output, "{:02x?}", byte).expect("Unable to write");
        }
        output
    }

    fn construct_pieces(piece_hashes: &Vec<Vec<u8>>, files: &Vec<File>, torrent: &IntermediateTorrent) -> Vec<Piece> {
        let piece_length = torrent.info.piece_length;

        let mut pieces: Vec<Piece> = Vec::with_capacity(piece_hashes.len());

        let file_count = files.len();

        let mut file_index = 0;
        let mut file_remaining_lengths = Vec::with_capacity(file_count);
        for file in files {
            file_remaining_lengths.push(file.length);
        }

        while pieces.len() < piece_hashes.len() {
            let mut piece_files: Vec<PieceFile> = Vec::new();
            let mut piece_counted_length: u64 = 0;

            while piece_counted_length < piece_length as u64 {
                if file_index == file_count {
                    break;
                }

                let current_file = &files[file_index];
                let mut current_remaining = file_remaining_lengths[file_index];

                let remainder = piece_length as u64 - piece_counted_length;
                if current_remaining >= remainder {
                    current_remaining -= remainder;
                    piece_counted_length = piece_length as u64;
                } else {
                    piece_counted_length += current_remaining;
                    current_remaining = 0;
                }

                piece_files.push(PieceFile {
                    read_start_position: (current_file.length - file_remaining_lengths[file_index])
                        as u64,
                    read_length: (file_remaining_lengths[file_index] - current_remaining) as u64,
                    file_length: current_file.length as u64,
                    file_path: current_file.path.clone()
                });

                file_remaining_lengths[file_index] = current_remaining;
                if file_remaining_lengths[file_index] == 0 {
                    file_index += 1;
                }
            }

            let mut total_length = 0;
            for piece_file in &piece_files {
                total_length += piece_file.read_length;
            }

            pieces.push(Piece {
                position: pieces.len(),
                files: piece_files,
                piece_hash: Torrent::get_sha1_hexdigest(&piece_hashes[pieces.len()]) ,
                length: total_length,
            })
        }

        pieces
    }
}
