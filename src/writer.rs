use std::{collections::HashMap, fmt::Write, fs::{self, OpenOptions}, io::{Seek, SeekFrom, Write as IoWrite}, os::unix::ffi::OsStrExt, path::{Path, PathBuf}, sync::Mutex};

use crate::torrent::Torrent;

use super::orchestrator::OrchestratorPiece;

pub struct PieceState {
    written_pieces: usize,
    failed_pieces: usize,
    total_piece_count: usize
}

pub struct PieceWriter {
    state: Mutex<PieceState>,
    lookup: HashMap<Vec<u8>, PathBuf>
}

impl PieceWriter {
    pub fn new(total_piece_count: usize, export_directory: &Path, torrents: &[Torrent]) -> PieceWriter {
        // Construct export paths
        let mut lookup: HashMap<Vec<u8>, PathBuf> = HashMap::new();
        let data = Path::new("Data");
        for torrent in torrents.iter() {
            let info_hash_as_human = PieceWriter::get_sha1_hexdigest(&torrent.info_hash);
            let info_hash_path = Path::new(&info_hash_as_human);
            let torrent_name = Path::new(&torrent.info.name);

            let export_directory_with_torrent = if torrent.info.files.is_some() {
                [export_directory, info_hash_path, data, torrent_name].iter().collect()
            } else {
                [export_directory, info_hash_path, data].iter().collect()
            };

            lookup.insert(torrent.info_hash.clone(), export_directory_with_torrent);
        }

        PieceWriter {
            lookup,
            state: Mutex::new(PieceState {
                written_pieces: 0,
                failed_pieces: 0,
                total_piece_count
            })
        }
    }

    pub fn write(&self, orchestrator_piece: OrchestratorPiece) -> Result<(), std::io::Error> {
        let piece = &orchestrator_piece.piece;
        let mut state = self.state.lock().unwrap();

        if let Some(result) = &orchestrator_piece.result {
            let mut start_position = 0;
            let export_directory = self.lookup.get(orchestrator_piece.info_hash.as_ref());

            match export_directory {
                Some(export_directory) => {
                    for piece_file in piece.files.iter() {
                        let end_position = start_position + piece_file.read_length;
                        let complete_path: PathBuf = [export_directory, piece_file.file_path.as_path()].iter().collect();

                        fs::create_dir_all(complete_path.parent().unwrap())?;

                        let mut handle = OpenOptions::new().write(true).create(true).open(&complete_path)?;
                        handle.set_len(piece_file.file_length)?;
                        handle.seek(SeekFrom::Start(piece_file.read_start_position))?;
                        handle.write_all(&result.bytes[(start_position as usize)..(end_position as usize)])?;

                        start_position = end_position;
                    }
        
                    state.written_pieces += 1;
                    println!("{} of {} total pieces written - {:.02}% (failed {})", state.written_pieces, state.total_piece_count, (state.written_pieces as f64 / state.total_piece_count as f64) * 100 as f64, state.failed_pieces);

                },
                None => Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, format!("Write method called for info hash {} that was not added during init time!", PieceWriter::get_sha1_hexdigest(&orchestrator_piece.info_hash))))?
            }
        } else {
            state.failed_pieces += 1;
            println!("{} of {} total pieces written - {:.02}% (failed {})", state.written_pieces, state.total_piece_count, (state.written_pieces as f64 / state.total_piece_count as f64) * 100 as f64, state.failed_pieces);
        };

        Ok(())
    }

    fn get_sha1_hexdigest(bytes: &[u8]) -> String {
        let mut output = String::new();
        for byte in bytes {
            write!(&mut output, "{:02x?}", byte).expect("Unable to write");
        }
        output
    }
}
