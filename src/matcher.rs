use std::{
    fs::File,
    io::{Read, Seek, SeekFrom},
    path::PathBuf, collections::HashSet
};

use crate::{
    finder::LengthFileFinder,
    torrent::Piece,
};
use crypto::{digest::Digest, sha1::Sha1};

pub struct PieceMatchResult {
    pub bytes: Vec<u8>,
    pub paths: Vec<PathBuf>,
}

pub struct MultiFilePieceMatcher;
impl MultiFilePieceMatcher {
    pub fn count_choices(finder: &LengthFileFinder, piece: &Piece) -> usize {
        let mut result: usize = if piece.files.len() == 0 { 0 } else { 1 };

        for piece_file in &piece.files {
            let files = finder.find_length(piece_file.file_length);

            result = match result.checked_mul(files.len()) {
                Some(new_result) => new_result,
                None => return usize::MAX,
            };
        }

        return result;
    }

    pub fn scan(
        finder: &LengthFileFinder,
        piece: &Piece,
    ) -> Result<Option<PieceMatchResult>, std::io::Error> {
        let mut paths: Vec<&PathBuf> = Vec::new();
        let mut bytes: Vec<u8> = Vec::new();

        if MultiFilePieceMatcher::scan_internal(&mut paths, &mut bytes, finder, &piece)? {
            let paths: Vec<PathBuf> = paths.into_iter().map(|entry| entry.clone()).collect();

            return Ok(Some(PieceMatchResult {
                bytes: bytes,
                paths: paths,
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

        // TODO: Accounting. We need to make count_choices return the correct
        // deduplicated count with the optimization of not hashing entries we've seen.
        let mut seen: HashSet<String> = HashSet::new();
        let should_deduplicate = entries.len() > 1;

        for entry in entries {
            let read_buffer = MultiFilePieceMatcher::read_bytes(
                entry,
                piece_file.read_length,
                piece_file.read_start_position,
            )?;

            // TODO: Hoist this check somewhere else.
            // We can filter ahead of time and not have to do this in a loop.
            if should_deduplicate {
                let hash = {
                    let mut hasher = Sha1::new();
                    hasher.input(&read_buffer);
                    hasher.result_str()
                };
                
                if !seen.insert(hash) {
                    continue;
                }
            }

            let previous_buffer_length = buffer.len();
            buffer.extend(read_buffer);
            paths.push(entry);

            let valid = if paths.len() == piece.files.len() {
                let mut hasher = Sha1::new();
                hasher.input(buffer);
                hasher.result_str() == piece.piece_hash
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

        return Ok(read_bytes);
    }
}
