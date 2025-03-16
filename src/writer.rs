use std::{collections::HashMap, fs::{self, OpenOptions}, io::{Seek, SeekFrom, Write as IoWrite}, sync::Mutex};

use crate::{finder::TorrentMetadataEntry, orchestrator::OrchestrationPiece, solver::PieceMatchResult};

pub struct FileWriter {
    locks: HashMap<usize, Mutex<()>>
}

impl FileWriter {
    pub fn new(metadata: &[TorrentMetadataEntry]) -> FileWriter {
        let mut locks  = HashMap::new();

        for entry in metadata {
            locks.insert(entry.id, Mutex::new(()));
        }

        FileWriter {
            locks: locks
        }
    }

    pub fn write(&self, item: &OrchestrationPiece, result: &PieceMatchResult) -> Result<(), std::io::Error> {
        let mut start_position = 0;
    
        for (file, source_path) in item.files.iter().zip(&result.source) {
            if file.metadata.is_padding_file { continue; }
    
            let file_length = file.metadata.file_length;
            let file_export = &file.metadata.full_target;
    
            if source_path.is_some() && file_export.eq(source_path.as_ref().unwrap().as_ref()) { continue; }

            let end_position = start_position + file.read_length as usize;
    
            let _file_write_guard = self.locks[&file.metadata.id]
                .lock()
                .expect("Should always lock.");

            fs::create_dir_all(file_export.parent().unwrap())?;

            let mut handle = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(false)
                .open(file_export)?;
            
            handle.set_len(file_length)?;
            handle.seek(SeekFrom::Start(file.read_start_position))?;
            handle.write_all(&result.bytes[start_position..end_position])?;

            drop(_file_write_guard);

            start_position = end_position;
        }
    
        Ok(())
    }    
}

