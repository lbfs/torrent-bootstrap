use std::path::PathBuf;

use crate::{
    finder::{read_bytes, LengthFileFinder},
    torrent::Piece,
};
use sha1::{Digest, Sha1};


pub struct PieceMatchResult {
    pub bytes: Vec<u8>,
    pub paths: Vec<PathBuf>,
}

pub struct MultiFilePieceMatcher;
impl MultiFilePieceMatcher {
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
            let read_buffer = read_bytes(
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
}
