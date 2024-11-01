use std::{fs::{self, OpenOptions}, io::{Seek, SeekFrom, Write as IoWrite}};

use crate::{finder::FileFinder, orchestrator::OrchestrationPiece};

pub fn write(item: &OrchestrationPiece, finder: &FileFinder) -> Result<(), std::io::Error> {
    for file in item.files.iter() {
        if file.is_padding_file { continue; }

        let file_length = finder.find_length(file.export_index);
        let file_export = finder.find_path_from_index(file.export_index);

        if file.source.is_some() && file.source.as_ref().unwrap().eq(file_export) { continue; }

        let bytes =  file.bytes.as_ref().unwrap();
        fs::create_dir_all(file_export.parent().unwrap())?;

        let mut handle = OpenOptions::new().write(true).create(true).open(file_export)?;
        handle.set_len(file_length)?;
        handle.seek(SeekFrom::Start(file.read_start_position))?;
        handle.write_all(bytes)?;
    }

    Ok(())
}
