use std::{fs::{self, File, OpenOptions}, io::{Seek, SeekFrom, Write as IoWrite}, path::PathBuf, sync::Arc};

use hashlru::Cache;
use crate::orchestrator::OrchestrationPiece;

pub struct FileWriter {
    cache: Cache<usize, File>
}

impl FileWriter {
    pub fn new(pieces: &[OrchestrationPiece], threads: usize) -> FileWriter {
        let max_files = pieces
            .iter()
            .map(|piece| piece.files.len())
            .max()
            .unwrap_or(0);

        let max_opened_files = std::cmp::max(max_files, threads);
        let cache: Cache<usize, File> = Cache::new(max_opened_files);

        FileWriter {
            cache
        }
    }

    pub fn write(&mut self, item: &OrchestrationPiece, output_paths: &Vec<Option<Arc<PathBuf>>>, output_bytes: &Vec<u8>) -> Result<bool, std::io::Error> {
        let mut next_start_position = 0;
        let mut wrote_to_disk = false;

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
            let result: Result<bool, std::io::Error> = {
                let id = file.metadata.id;
                if let None = self.cache.get_mut(&id) {
                    fs::create_dir_all(file_export.parent().unwrap())?;

                    let handle = OpenOptions::new()
                        .write(true)
                        .create(true)
                        .truncate(false)
                        .open(file_export)?;

                    handle.set_len(file.metadata.file_length)?;
                    self.cache.insert(id, handle);
                }

                let handle = self.cache.get_mut(&file.metadata.id).unwrap();
                handle.seek(SeekFrom::Start(file.read_start_position))?;
                handle.write_all(&output_bytes[start_position..end_position])?;

                Ok(true)
            };

            match &result {
                Ok(found) => {
                    let found = *found;
                    processing_state.writable_pieces += found as usize;
                    processing_state.ignored_pieces += !found as usize;
                    if found { wrote_to_disk = true; } 
                },
                Err(_) => { return result; },
            }
        }

        Ok(wrote_to_disk)
    }    
}

