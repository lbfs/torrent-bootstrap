use std::{fs::{self, OpenOptions}, io::{Seek, SeekFrom, Write as IoWrite}};

use crate::{finder::FileFinder, orchestrator::OrchestrationPiece, solver::PieceMatchResult};

pub fn write(item: &OrchestrationPiece, result: &PieceMatchResult, finder: &FileFinder) -> Result<(), std::io::Error> {
    let mut start_position = 0;

    for (file, source_path) in item.files.iter().zip(&result.source) {
        if file.is_padding_file { continue; }

        let file_length = finder.find_length(file.export_index);
        let file_export = finder.find_path_from_index(file.export_index);

        if source_path.is_some() && file_export.eq(source_path.unwrap()) { continue; }

        fs::create_dir_all(file_export.parent().unwrap())?;

        let end_position = start_position + file.read_length as usize;

        let mut handle = OpenOptions::new().write(true).create(true).open(file_export)?;
        handle.set_len(file_length)?;
        handle.seek(SeekFrom::Start(file.read_start_position))?;
        handle.write_all(&result.bytes[start_position..end_position])?;

        start_position = end_position;
    }

    Ok(())
}
