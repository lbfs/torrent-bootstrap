use std::path::{Path, PathBuf};

use super::Torrent;

#[derive(Debug)]
pub struct PieceFile {
    pub read_length: u64,
    pub read_start_position: u64,
    pub file_path: PathBuf,
    pub file_length: u64,
}

#[derive(Debug)]
pub struct Piece {
    pub position: usize,
    pub files: Vec<PieceFile>,
    pub hash: Vec<u8>,
    pub length: u64,
}

pub struct Pieces;
impl Pieces {
    pub fn from_torrent(torrent: &Torrent) -> Vec<Piece> {
        Pieces::construct_pieces(torrent)
    }
        
    fn construct_pieces(torrent: &Torrent) -> Vec<Piece> {
        if torrent.info.length.is_some() {
            Pieces::construct_pieces_single_file(torrent)
        } else if torrent.info.files.is_some() {
            Pieces::construct_pieces_multiple_file(torrent)
        } else {
            panic!("Neither single-file or multiple-files option is available, unable to construct pieces.");
        }
    }

    fn construct_pieces_multiple_file(torrent: &Torrent) -> Vec<Piece> {
        let piece_length = torrent.info.piece_length;

        let files = torrent.info.files.as_ref().unwrap();
        let file_count = files.len();
        let mut file_index = 0;
        let mut file_remaining_length = files.get(0)
            .expect("Files should always have atleast 1 entry")
            .length;

        let mut pieces: Vec<Piece> = Vec::with_capacity(torrent.info.pieces.len());
        for hash in &torrent.info.pieces {
            let mut piece_files: Vec<PieceFile> = Vec::new();
            let mut piece_counted_length = 0;

            while piece_counted_length < piece_length {
                let current = &files[file_index];
                let mut current_remaining = file_remaining_length;

                let remainder = piece_length - piece_counted_length;
                if current_remaining >= remainder {
                    current_remaining -= remainder;
                    piece_counted_length = piece_length;
                } else {
                    piece_counted_length += current_remaining;
                    current_remaining = 0;
                }

                piece_files.push(PieceFile {
                    read_start_position: (current.length - file_remaining_length),
                    read_length: (file_remaining_length - current_remaining),
                    file_length: current.length,
                    file_path: current.path.clone() // TODO: Remove this clone
                });

                file_remaining_length = current_remaining;
                if file_remaining_length == 0 {
                    file_index += 1;
                    
                    if file_index == file_count {
                        break;
                    }

                    file_remaining_length = files[file_index].length;
                }
            }

            let mut length = 0;
            for file in &piece_files {
                length += file.read_length;
            }

            pieces.push(Piece {
                position: pieces.len(),
                files: piece_files,
                hash: hash.clone(),
                length: length
            });
        }

        pieces
    }

    fn construct_pieces_single_file(torrent: &Torrent) -> Vec<Piece> {
        let mut pieces: Vec<Piece> = Vec::with_capacity(torrent.info.pieces.len());

        let mut read_start_position = 0;
        let mut file_remaining_length = torrent.info.length.unwrap();
        let piece_length = torrent.info.piece_length;

        for hash in &torrent.info.pieces {
            let read_length = if file_remaining_length < piece_length {
                file_remaining_length
            } else { 
                piece_length 
            };

            pieces.push(Piece {
                position: pieces.len(),
                files: vec![PieceFile {
                    read_start_position: read_start_position,
                    read_length: read_length,
                    file_length: torrent.info.length.unwrap(),
                    file_path: Path::new(&torrent.info.name).to_path_buf()
                }],
                hash: hash.clone(),
                length: read_length
            });

            file_remaining_length -= read_length;
            read_start_position += read_length;
        }

        pieces
    } 
}