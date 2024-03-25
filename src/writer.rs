use std::{fs::{self, OpenOptions}, io::{Seek, SeekFrom, Write as IoWrite}, sync::Mutex};

use super::orchestrator::OrchestratorPiece;

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

    pub fn write(&self, orchestrator_piece: OrchestratorPiece) -> Result<(), std::io::Error> {
        let mut state = self.state.lock().unwrap();

        let piece = &orchestrator_piece.piece;

        if let Some(result) = &orchestrator_piece.result {
            let mut start_position = 0;

            for (index, piece_file) in piece.files.iter().enumerate() {
                let end_position = start_position + piece_file.read_length;
                let export_path = orchestrator_piece.export_paths.get(index).unwrap();

                if !export_path.eq(result.paths.get(index).unwrap()) {
                    fs::create_dir_all(export_path.parent().unwrap())?;

                    let mut handle = OpenOptions::new().write(true).create(true).open(&export_path)?;
                    handle.set_len(piece_file.file_length)?;
                    handle.seek(SeekFrom::Start(piece_file.read_start_position))?;
                    handle.write_all(&result.bytes[(start_position as usize)..(end_position as usize)])?;
                }

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


}
