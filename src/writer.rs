use std::{fs::{self, OpenOptions}, io::{Seek, SeekFrom, Write as IoWrite}, sync::Arc};

use crate::solver::task::SolverMetadata;

pub struct FileWriter {
    solver_metadata: Arc<SolverMetadata>
}

impl FileWriter {
    pub fn new(solver_metadata: Arc<SolverMetadata>) -> FileWriter {
        FileWriter {
            solver_metadata
        }
    }

    pub fn write(&mut self, piece_id: usize, output_paths: &Vec<Option<usize>>, output_bytes: &Vec<u8>) -> Result<bool, std::io::Error> {
        let mut next_start_position = 0;
        let mut wrote_to_disk = false;

        let piece = &self.solver_metadata.torrent_pieces[piece_id];

        for (piece_file, source_path) in piece.files.iter().zip(output_paths) {
            let file = &self.solver_metadata.torrent_files[piece_file.file_id];

            // Always update the next position in-case we have to exit-early
            let start_position = next_start_position;
            let end_position = start_position + piece_file.read_length as usize;
            next_start_position = end_position;

            // Take ownership of the current processing state
            /*
            let mut processing_state = file.metadata.processing_state
                .lock()
                .expect("Should always lock the processing state.");
            */

            // Check if we can skip writing to disk
            let is_padding_file = file.padding;
            let file_export = file.export_target;

            // The file is all zeros, do not write anything.
            if is_padding_file {
                // processing_state.ignored_pieces += 1;
                continue; 
            }
    
            // The path on disk is the same as the discovered path, therefore, skip writing. 
            if source_path.is_some() && file_export.eq(source_path.as_ref().unwrap()) {
                // processing_state.ignored_pieces += 1;
                continue;
            }

            // This is new byte content, write it to disk.
            let result: Result<bool, std::io::Error> = {
                let file_export = self.solver_metadata.path_interner.get(file_export);

                fs::create_dir_all(file_export.parent().unwrap())?;

                let mut handle = OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(false)
                    .open(file_export)?;

                handle.set_len(file.file_length)?;
                handle.seek(SeekFrom::Start(piece_file.read_start_position))?;
                handle.write_all(&output_bytes[start_position..end_position])?;
                Ok(true)
            };

            match &result {
                Ok(found) => {
                    let found = *found;
                    //processing_state.writable_pieces += found as usize;
                    //processing_state.ignored_pieces += !found as usize;
                    if found { wrote_to_disk = true; } 
                },
                Err(_) => { return result; },
            }
        }

        Ok(wrote_to_disk)
    }    
}

