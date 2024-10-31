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
        if let Some(entry) = entry {
            for file in entry.files.iter() {
                if file.is_padding_file { continue; }
                if file.source.is_some() && file.source.as_ref().unwrap().eq(&file.export) { continue; }

                let bytes =  file.bytes.as_ref().unwrap();
                fs::create_dir_all(file.export.parent().unwrap())?;

                let mut handle = OpenOptions::new().write(true).create(true).open(&file.export)?;
                handle.set_len(file.file_length)?;
                handle.seek(SeekFrom::Start(file.read_start_position))?;
                handle.write_all(bytes)?;
            }

            let mut state = self.state.lock().unwrap();

            state.written_pieces += 1;
            println!("{} of {} total pieces written - {:.02}% (failed {})", state.written_pieces, state.total_piece_count, (state.written_pieces as f64 / state.total_piece_count as f64) * 100 as f64, state.failed_pieces);

        } else {
            let mut state = self.state.lock().unwrap();

            state.failed_pieces += 1;
            println!("{} of {} total pieces written - {:.02}% (failed {})", state.written_pieces, state.total_piece_count, (state.written_pieces as f64 / state.total_piece_count as f64) * 100 as f64, state.failed_pieces);
        };

        Ok(())
    }


}
