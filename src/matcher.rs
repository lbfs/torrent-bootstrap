use std::{collections::HashMap, path::PathBuf};

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
        // Check if we have at-minimum 1 match that can be made.
        for file in piece.files.iter() {
            if finder.find_length(file.file_length).len() == 0 {
                return Ok(None);
            }
        }

        // Start scanning
        let mut paths: Vec<&PathBuf> = Vec::with_capacity(piece.files.len());
        let mut bytes: Vec<u8> = Vec::with_capacity(piece.length as usize);
        let loaded = MultiFilePieceMatcher::preload(piece, finder)?;

        if MultiFilePieceMatcher::scan_internal(&mut paths, &mut bytes, &loaded, piece)? {
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
        finder: &HashMap<usize, Vec<(&'a PathBuf, Vec<u8>)>>,
        piece: &Piece
    ) -> Result<bool, std::io::Error> {
        let entries = finder.get(&paths.len()).unwrap();

        for (path, read_buffer) in entries {
            let previous_buffer_length = buffer.len();
            buffer.extend(read_buffer);
            paths.push(*path);

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

    fn preload<'a>(piece: &Piece, finder: &'a LengthFileFinder) -> Result<HashMap<usize, Vec<(&'a PathBuf, Vec<u8>)>>, std::io::Error> {
        let mut loaded = HashMap::new();

        for (file_position, file) in piece.files.iter().enumerate() {

            let mut results: Vec<(&'a PathBuf, Vec<u8>)> = Vec::new();
            let entries = finder.find_length(file.file_length);

            for entry in entries {
                let value = read_bytes(entry, file.read_length, file.read_start_position)?;
                results.push((entry, value));
            }

            loaded.insert(file_position, results);
        }

        Ok(loaded)
    }
}
