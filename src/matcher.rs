use std::{
    fs::File,
    io::{Read, Seek, SeekFrom},
    path::PathBuf
};

use crate::{
    finder::LengthFileFinder,
    torrent::Piece,
};
use sha1::{Digest, Sha1};


pub struct PieceMatchResult {
    pub bytes: Vec<u8>,
    pub paths: Vec<PathBuf>,
}

pub struct MultiFilePieceMatcher;
impl MultiFilePieceMatcher {
    pub fn count_choices(finder: &LengthFileFinder, piece: &Piece) -> usize {
        let mut result: usize = if piece.files.is_empty() { 0 } else { 1 };

        for piece_file in &piece.files {
            let files = finder.find_length(piece_file.file_length);

            result = match result.checked_mul(files.len()) {
                Some(new_result) => new_result,
                None => return usize::MAX,
            };
        }

        result
    }

    pub fn scan(
        finder: &LengthFileFinder,
        piece: &Piece,
    ) -> Result<Option<PieceMatchResult>, std::io::Error> {
        let mut paths: Vec<&PathBuf> = Vec::new();
        let mut bytes: Vec<u8> = Vec::new();

        if MultiFilePieceMatcher::scan_internal(&mut paths, &mut bytes, finder, piece)? {
            let paths: Vec<PathBuf> = paths.into_iter().cloned().collect();

            return Ok(Some(PieceMatchResult {
                bytes,
                paths,
            }));
        }

        Ok(None)
    }

    fn scan_internal<'a>(
        paths: &mut Vec<&'a PathBuf>,
        buffer: &mut Vec<u8>,
        finder: &'a LengthFileFinder,
        piece: &Piece,
    ) -> Result<bool, std::io::Error> {
        let piece_file = piece.files.get(paths.len()).unwrap();
        let entries = finder.find_length(piece_file.file_length);

        for entry in entries {
            let read_buffer = MultiFilePieceMatcher::read_bytes(
                entry,
                piece_file.read_length,
                piece_file.read_start_position,
            )?;

            let previous_buffer_length = buffer.len();
            buffer.extend(read_buffer);
            paths.push(entry);

            let valid = if paths.len() == piece.files.len() {
                let hash = Sha1::digest(&buffer);
                piece.hash.as_slice().cmp(&hash).is_eq()
            } else {
                MultiFilePieceMatcher::scan_internal(paths, buffer, finder, piece)?
            };

            if valid {
                return Ok(valid);
            }

            paths.pop();
            buffer.truncate(previous_buffer_length);
        }

        Ok(false)
    }

    fn read_bytes(
        path: &PathBuf,
        read_length: u64,
        read_start_position: u64,
    ) -> Result<Vec<u8>, std::io::Error> {
        let mut read_bytes = vec![0u8; read_length as usize];
        let mut handle = File::open(path)?;

        handle.seek(SeekFrom::Start(read_start_position))?;
        handle.read_exact(&mut read_bytes)?;

        Ok(read_bytes)
    }
}
