use super::Torrent;

#[derive(PartialEq, Eq, Debug)]
pub struct PieceFile {
    pub read_length: u64,
    pub read_start_position: u64,
    pub file_index: usize,
    pub file_length: u64
}

#[derive(PartialEq, Eq, Debug)]
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
        } else {
            Pieces::construct_pieces_multiple_file(torrent)
        }
    }

    fn construct_pieces_multiple_file(torrent: &Torrent) -> Vec<Piece> {
        let piece_length = torrent.info.piece_length;

        let files = torrent.info.files.as_ref().unwrap();
        let file_count = files.len();
        let mut file_index = 0;
        let mut file_remaining_length = files.first()
            .unwrap()
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
                    file_index
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
                length
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
                    read_start_position,
                    read_length,
                    file_length: torrent.info.length.unwrap(),
                    file_index: 0
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

#[cfg(test)]
mod tests {
    use crate::{File, Info};

    use super::*;

    #[test]
    fn construct_pieces_multiple_file_should_succeed() {
        let torrent = Torrent {
            info: Info {
                name: "Example".to_string(),
                length: None,
                files: Some(vec![
                    File {
                        length: 262540,
                        path: vec!["1.png".to_string()],
                    },
                    File {
                        length: 557338,
                        path: vec!["2.jpeg".to_string()],
                    },
                ]),
                piece_length: 524288,
                pieces: vec![
                    vec![205, 113, 172, 214, 185, 177, 13, 52, 20, 24, 149, 41, 222, 64, 164, 229, 154, 232, 64, 198],
                    vec![222, 220, 208, 9, 117, 139, 87, 43, 47, 57, 191, 94, 78, 142, 68, 176, 66, 206, 40, 67],
                ],
            },
            info_hash: vec![158, 107, 242, 157, 198, 208, 115, 71, 243, 8, 84, 55, 8, 17, 60, 86, 152, 141, 19, 186],
        };

        let actual = Pieces::from_torrent(&torrent);

        let expected: Vec<Piece> = vec![
            Piece {
                position: 0,
                files: vec![
                    PieceFile {
                        read_length: 262540,
                        read_start_position: 0,
                        file_index: 0,
                        file_length: 262540,
                    },
                    PieceFile {
                        read_length: 261748,
                        read_start_position: 0,
                        file_index: 1,
                        file_length: 557338,
                    },
                ],
                hash: vec![205, 113, 172, 214, 185, 177, 13, 52, 20, 24, 149, 41, 222, 64, 164, 229, 154, 232, 64, 198],
                length: 524288,
            },
            Piece {
                position: 1,
                files: vec![PieceFile {
                    read_length: 295590,
                    read_start_position: 261748,
                    file_index: 1,
                    file_length: 557338,
                }],
                hash: vec![222, 220, 208, 9, 117, 139, 87, 43, 47, 57, 191, 94, 78, 142, 68, 176, 66, 206, 40, 67],
                length: 295590,
            },
        ];

        assert_eq!(expected, actual);
    }

    #[test]
    fn construct_pieces_single_file_should_succeed() {
        let torrent = Torrent {
            info: Info {
                name: "1.png".to_string(),
                length: Some(262540),
                files: None,
                piece_length: 131072,
                pieces: vec![
                    vec![
                        64, 130, 19, 100, 17, 41, 244, 154, 238, 44, 197, 197, 249, 130, 222, 79, 160, 252, 114, 195
                    ],
                    vec![
                        41, 171, 65, 2, 191, 39, 185, 197, 162, 144, 29, 204, 204, 17, 252, 6, 214, 131, 198, 99
                    ],
                    vec![
                        145, 103, 77, 168, 208, 237, 195, 161, 115, 88, 170, 201, 20, 164, 210, 40, 71, 176, 91, 105
                    ],
                ],
            },
            info_hash: vec![
                222, 16, 92, 167, 219, 78, 170, 190, 18, 50, 30, 43, 240, 88, 62, 206, 226, 0, 163, 166,
            ],
        };

        let actual = Pieces::from_torrent(&torrent);

        let expected: Vec<Piece> = vec![
            Piece {
                position: 0,
                files: vec![PieceFile {
                    read_length: 131072,
                    read_start_position: 0,
                    file_index: 0,
                    file_length: 262540,
                }],
                hash: vec![
                    64, 130, 19, 100, 17, 41, 244, 154, 238, 44, 197, 197, 249, 130, 222, 79, 160,
                    252, 114, 195,
                ],
                length: 131072,
            },
            Piece {
                position: 1,
                files: vec![PieceFile {
                    read_length: 131072,
                    read_start_position: 131072,
                    file_index: 0,
                    file_length: 262540,
                }],
                hash: vec![
                    41, 171, 65, 2, 191, 39, 185, 197, 162, 144, 29, 204, 204, 17, 252, 6, 214,
                    131, 198, 99,
                ],
                length: 131072,
            },
            Piece {
                position: 2,
                files: vec![PieceFile {
                    read_length: 396,
                    read_start_position: 262144,
                    file_index: 0,
                    file_length: 262540,
                }],
                hash: vec![
                    145, 103, 77, 168, 208, 237, 195, 161, 115, 88, 170, 201, 20, 164, 210, 40, 71,
                    176, 91, 105,
                ],
                length: 396,
            },
        ];

        assert_eq!(expected, actual);
    }
}