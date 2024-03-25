use std::{fs::{self, OpenOptions}, io::{Seek, SeekFrom, Write as IoWrite}, sync::Mutex};

use crate::orchestrator::OrchestrationPiece;

pub struct PieceState {
    written_pieces: usize,
    failed_pieces: usize,
    total_piece_count: usize
}

pub struct PieceWriter {
    state: Mutex<PieceState>,
}

impl PieceWriter {
    pub fn new(total_piece_count: usize) -> PieceWriter {
        PieceWriter {
            state: Mutex::new(PieceState {
                written_pieces: 0,
                failed_pieces: 0,
                total_piece_count
            })
        }
    }

    pub fn write(&self, entry: Option<OrchestrationPiece>) -> Result<(), std::io::Error> {
        let mut state = self.state.lock().unwrap();

        if let Some(entry) = entry {
            for file in entry.files.iter() {
                if let Some(source) = &file.source {
                    if !source.eq(&file.export) {
                        if let Some(bytes) = &file.bytes {
                            fs::create_dir_all(file.export.parent().unwrap())?;

                            let mut handle = OpenOptions::new().write(true).create(true).open(&file.export)?;
                            handle.set_len(file.file_length)?;
                            handle.seek(SeekFrom::Start(file.read_start_position))?;
                            handle.write_all(&bytes)?;
                        }
                    }
                }
            }

            state.written_pieces += 1;
            println!("{} of {} total pieces written - {:.02}% (failed {})", state.written_pieces, state.total_piece_count, (state.written_pieces as f64 / state.total_piece_count as f64) * 100 as f64, state.failed_pieces);

        } else {
            state.failed_pieces += 1;
            println!("{} of {} total pieces written - {:.02}% (failed {})", state.written_pieces, state.total_piece_count, (state.written_pieces as f64 / state.total_piece_count as f64) * 100 as f64, state.failed_pieces);
        };

        Ok(())
    }


}
