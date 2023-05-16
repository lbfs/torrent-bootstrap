use lava_torrent::torrent::v1::Torrent as LavaTorrent;
use std::{
    fmt::Write,
    io::ErrorKind,
    path::{Path, PathBuf},
};

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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct File {
    pub length: u64,
    pub path: PathBuf,
}

impl Torrent {
    pub fn from(path: &Path) -> Result<Torrent, std::io::Error> {
        let lava_torrent = Torrent::load_torrent(path)?;

        // Map lava_torrent Files to internal File structure
        let files = Torrent::format_files(&lava_torrent);

        // Get the overall metadata for this torrent
        let info_hash = lava_torrent.info_hash();

        // Build our final torrent object
        let mut torrent = Torrent {
            files: files,
            pieces: Vec::new(),
            piece_length: 0,
            info_hash: info_hash,
            path: path.to_path_buf()
        };

        // Generate file read boundries and hashes for individual hashes
        torrent.pieces = Torrent::construct_pieces(&lava_torrent, &torrent.files);
        torrent.piece_length = torrent.pieces.len();

        Ok(torrent)
    }

    fn load_torrent(path: &Path) -> Result<LavaTorrent, std::io::Error> {
        let torrent = LavaTorrent::read_from_file(path)
            .map_err(|err| std::io::Error::new(ErrorKind::InvalidData, err.to_string()))?;

        Ok(torrent)
    }

    fn format_content_layout(lava_torrent: &LavaTorrent, file_path: Option<&Path>) -> PathBuf {
        let mut path = PathBuf::from(&lava_torrent.name);

        if let Some(file_path) = file_path {
            path.push(file_path);
        }

        path
    }

    fn format_files(lava_torrent: &LavaTorrent) -> Vec<File> {
        match lava_torrent.files.clone() {
            Some(files) => {
                files.into_iter().map(|lava_file| {
                    File {
                        path: Torrent::format_content_layout(lava_torrent, Some(&lava_file.path)),
                        length: lava_file.length as u64,
                    }
                })
                .collect()
            },
            None => {
                let mut files = Vec::new();
                files.push(File {
                    path: Torrent::format_content_layout(lava_torrent, None),
                    length: lava_torrent.length as u64,
                });
                files
            }
        }
    }

    fn get_sha1_hexdigest(torrent: &LavaTorrent, index: usize) -> String {
        let bytes = torrent.pieces.get(index).unwrap();
        let mut output = String::new();
        for byte in bytes {
            write!(&mut output, "{:02x?}", byte).expect("Unable to write");
        }
        output
    }

    fn construct_pieces(lava_torrent: &LavaTorrent, files: &Vec<File>) -> Vec<Piece> {
        let piece_length = lava_torrent.piece_length as u64;
        let piece_count = lava_torrent.pieces.len();
        let mut pieces: Vec<Piece> = Vec::with_capacity(piece_count);

        let file_count = files.len();

        let mut file_index = 0;
        let mut file_remaining_lengths = Vec::with_capacity(file_count);
        for file in files {
            file_remaining_lengths.push(file.length);
        }

        while pieces.len() < piece_count {
            let mut piece_files: Vec<PieceFile> = Vec::new();
            let mut piece_counted_length = 0;

            while piece_counted_length < piece_length {
                if file_index == file_count {
                    break;
                }

                let current_file = &files[file_index];
                let mut current_remaining = file_remaining_lengths[file_index];

                let remainder = piece_length - piece_counted_length;
                if current_remaining >= remainder {
                    current_remaining -= remainder;
                    piece_counted_length = piece_length;
                } else {
                    piece_counted_length += current_remaining;
                    current_remaining = 0;
                }

                piece_files.push(PieceFile {
                    read_start_position: (current_file.length - file_remaining_lengths[file_index])
                        as u64,
                    read_length: (file_remaining_lengths[file_index] - current_remaining) as u64,
                    file_length: current_file.length as u64,
                    file_path: current_file.path.clone(),
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
                piece_hash: Torrent::get_sha1_hexdigest(&lava_torrent, pieces.len()),
                length: total_length,
            })
        }

        pieces
    }
}
