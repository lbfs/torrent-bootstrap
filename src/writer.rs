use std::{fs::{self, OpenOptions}, io::{Seek, SeekFrom, Write as IoWrite}, path::PathBuf, sync::Arc};

use crate::{get_sha1_hexdigest, orchestrator::OrchestrationPiece};

pub struct FileWriter {}

impl FileWriter {
    pub fn new() -> FileWriter {
        FileWriter {}
    }

    pub fn write(&self, item: &OrchestrationPiece, output_paths: &Vec<Option<Arc<PathBuf>>>, output_bytes: &Vec<u8>) -> Result<(), std::io::Error> {
        let mut next_start_position = 0;

        for (file, source_path) in item.files.iter().zip(output_paths) {
            // Always update the next position in-case we have to exit-early
            let start_position = next_start_position;
            let end_position = start_position + file.read_length as usize;
            next_start_position = end_position;

            // Take ownership of the current processing state
            let mut processing_state = file.metadata.processing_state
                .lock()
                .expect("Should always lock the processing state.");

            // Check if we can skip writing to disk
            let is_padding_file = file.metadata.is_padding_file;
            let file_export = &file.metadata.full_target;

            // The file is all zeros, do not write anything.
            if is_padding_file {
                processing_state.ignored_pieces += 1;
                continue; 
            }
    
            // The path on disk is the same as the discovered path, therefore, skip writing. 
            if source_path.is_some() && file_export.eq(source_path.as_ref().unwrap().as_ref()) {
                processing_state.ignored_pieces += 1;
                continue;
            }

            // This is new byte content, write it to disk.
            fs::create_dir_all(file_export.parent().unwrap())?;

            let mut handle = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(false)
                .open(file_export)?;
            
            handle.set_len(file.metadata.file_length)?;
            handle.seek(SeekFrom::Start(file.read_start_position))?;
            handle.write_all(&output_bytes[start_position..end_position])?;
            processing_state.writable_pieces += 1;
        }
    
        for file in &item.files {
            let processing_state = file.metadata.processing_state
                .lock()
                .expect("Should always lock the processing state.");

            if processing_state.writable_pieces + processing_state.ignored_pieces == processing_state.total_pieces {
                println!(
                    "Completely processed file at {:#?} for torrent {} with {} ignored pieces, {} writable pieces of {} total pieces", 
                    file.metadata.full_target, 
                    get_sha1_hexdigest(&file.metadata.info_hash),
                    processing_state.ignored_pieces,
                    processing_state.writable_pieces,
                    processing_state.total_pieces
                )
            }
        }

        Ok(())
    }    
}

