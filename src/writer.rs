use std::{fmt::Write, fs::{self, OpenOptions}, io::{Seek, SeekFrom, Write as IoWrite}, path::{Path, PathBuf}, sync::Mutex};

use super::orchestrator::OrchestratorPiece;

pub(super) struct PieceState {
    written_pieces: usize,
    failed_pieces: usize,
    total_piece_count: usize
}

pub(super) struct PieceWriter {
    state: Mutex<PieceState>,
    export_directory: PathBuf
}

impl PieceWriter {
    pub fn new(export_directory: PathBuf, total_piece_count: usize) -> PieceWriter {
        PieceWriter {
            state: Mutex::new(PieceState {
                written_pieces: 0,
                failed_pieces: 0,
                total_piece_count
            }),
            export_directory: export_directory
        }
    }

    pub fn write(&self, orchestrator_piece: OrchestratorPiece) -> Result<(), std::io::Error> {
        let piece = &orchestrator_piece.piece;
        let mut state = self.state.lock().unwrap();

        if let Some(result) = &orchestrator_piece.result {
            let mut start_position = 0;

            // Build Folder Path
            let mut base_path: Vec<PathBuf> = Vec::new();
            base_path.push(self.export_directory.clone());
            base_path.push(PathBuf::from(PieceWriter::get_sha1_hexdigest(&orchestrator_piece.torrent_hash)));
            base_path.push(PathBuf::from("Data"));
            base_path.push(Path::new(orchestrator_piece.torrent_name.as_str()).to_path_buf());
            let base_path: PathBuf = base_path.into_iter().collect();

            for piece_file in &piece.files {
                // Build File Path
                let mut complete_path: Vec<PathBuf> = Vec::new();
                complete_path.push(base_path.clone());
                for path in piece_file.file_path.iter() {
                    complete_path.push(PathBuf::from(path));
                }
                let complete_path: PathBuf = complete_path.into_iter().collect();
                fs::create_dir_all(complete_path.parent().unwrap())?;

                let mut handle = OpenOptions::new().write(true).create(true).open(&complete_path)?;
                handle.set_len(piece_file.file_length)?;
                handle.seek(SeekFrom::Start(piece_file.read_start_position))?;
                
                let end_position = start_position + piece_file.read_length;

                handle.write_all(&result.bytes[(start_position as usize)..(end_position as usize)])?;

                start_position = end_position;
            }

            state.written_pieces += 1;
            println!("{} of {} total pieces written - {:.02}% (failed {})", state.written_pieces, state.total_piece_count, (state.written_pieces as f64 / state.total_piece_count as f64) * 100 as f64, state.failed_pieces);
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
