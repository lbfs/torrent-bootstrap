use std::{collections::HashMap, fs::{self, OpenOptions}, io::{Seek, SeekFrom, Write as IoWrite}, sync::Mutex};

use crate::{finder::FileFinder, orchestrator::OrchestrationPiece, solver::PieceMatchResult};

pub struct FileWriter {
    export_id_to_lock: HashMap<usize, Mutex<()>>
}

impl FileWriter {
    pub fn new(finder: &FileFinder) -> FileWriter {
        let mut export_id_to_lock: HashMap<usize, Mutex<()>> = HashMap::new();

        for index in 0..finder.index_to_path.len() {
            export_id_to_lock.insert(index, Mutex::new(()));
        }       

        FileWriter {
            export_id_to_lock
        }
    }

    pub fn write(&self, item: &OrchestrationPiece, result: &PieceMatchResult, finder: &FileFinder) -> Result<(), std::io::Error> {
        let mut start_position = 0;
    
        for (file, source_path) in item.files.iter().zip(&result.source) {
            if file.is_padding_file { continue; }
    
            let file_length = finder.find_length(file.export_index);
            let file_export = finder.find_path_from_index(file.export_index);
    
            if source_path.is_some() && file_export.eq(source_path.unwrap()) { continue; }
    
            let lock = &self.export_id_to_lock[&file.export_index];
            let _file_write_guard = lock.lock().expect("Should always lock.");

            fs::create_dir_all(file_export.parent().unwrap())?;
    
            let end_position = start_position + file.read_length as usize;
    
            let mut handle = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(false)
                .open(file_export)?;
            
            handle.set_len(file_length)?;
            handle.seek(SeekFrom::Start(file.read_start_position))?;
            handle.write_all(&result.bytes[start_position..end_position])?;
    
            start_position = end_position;
        }
    
        Ok(())
    }    
}

