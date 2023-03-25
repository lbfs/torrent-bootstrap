use std::{
    collections::HashMap,
    fs::File,
    io::{Read, Seek, SeekFrom},
    path::PathBuf,
};

use crypto::{digest::Digest, sha1::Sha1};

use crate::{finder::{PathFinder}, torrent::Piece};

pub struct PieceMatchResult {
    pub bytes: Vec<u8>,
    pub paths: Vec<PathBuf>,
}

pub struct PieceMatcher;
impl PieceMatcher {
    pub fn count_choices<T: PathFinder>(finder: &T, piece: &Piece) -> Option<usize> {
        let mut result: usize = if piece.files.len() == 0 { 0 } else { 1 };

        for piece_file in &piece.files {
            let files = finder.find_length(piece_file.file_length);

            result = match result.checked_mul(files.len()) {
                Some(new_result) => new_result,
                None => return None,
            };
        }

        Some(result)
    }

    pub fn scan<T: PathFinder>(
        finder: &T,
        piece: &Piece,
    ) -> Result<Option<PieceMatchResult>, std::io::Error> {
        let count_choices = PieceMatcher::count_choices(finder, piece);
        if count_choices.is_none() || count_choices.unwrap() > 100000 {
            println!("Skipping!");
            return Ok(None);
        }

        let mut paths: Vec<&PathBuf> = Vec::new();
        let mut bytes: Vec<u8> = Vec::new();

        let result = if count_choices.unwrap() >= 5000 {
            use std::time::Instant;
            let now = Instant::now();
            println!("Falling back to cache method!");

            let cache = PieceMatcher::preload(&piece, finder)?;
            let result = PieceMatcher::scan_internal_memory(&mut paths, &mut bytes, &cache, finder, piece);

            println!("Cached method completed in {} seconds.", now.elapsed().as_secs());
            result
        } else {
            PieceMatcher::scan_internal_disk(&mut paths, &mut bytes, finder, piece)?
        };

        if result {
            let paths: Vec<PathBuf> = paths.into_iter().map(|entry| entry.clone()).collect();

            return Ok(Some(PieceMatchResult {
                paths: paths,
                bytes: bytes,
            }));
        }

        Ok(None)
    }

    fn scan_internal_memory<'a, T: PathFinder>(
        paths: &mut Vec<&'a PathBuf>,
        buffer: &mut Vec<u8>,
        cache: &HashMap<String, Vec<u8>>,
        finder: &'a T,
        piece: &Piece,
    ) -> bool {
        let piece_file = piece.files.get(paths.len()).unwrap();
        let entries = finder.find_length(piece_file.file_length);

        for entry in entries {
            let read_buffer = PieceMatcher::read_bytes_cache(
                entry,
                piece_file.read_length,
                piece_file.read_start_position,
                cache
            );

            let previous_buffer_length = buffer.len();
            buffer.extend(read_buffer);
            paths.push(entry);

            let valid = if paths.len() == piece.files.len() {
                let mut hasher = Sha1::new();
                hasher.input(buffer);
                hasher.result_str() == piece.hash
            } else {
                PieceMatcher::scan_internal_memory(paths, buffer, cache, finder, piece)
            };

            if valid {
                return valid;
            }

            paths.pop();
            buffer.truncate(previous_buffer_length);
        }

        false
    }

    fn scan_internal_disk<'a, T: PathFinder>(
        paths: &mut Vec<&'a PathBuf>,
        buffer: &mut Vec<u8>,
        finder: &'a T,
        piece: &Piece,
    ) -> Result<bool, std::io::Error> {
        let piece_file = piece.files.get(paths.len()).unwrap();
        let entries = finder.find_length(piece_file.file_length);

        for entry in entries {
            let read_buffer = PieceMatcher::read_bytes(
                entry,
                piece_file.read_length,
                piece_file.read_start_position,
            )?;

            let previous_buffer_length = buffer.len();
            buffer.extend(read_buffer);
            paths.push(entry);

            let valid = if paths.len() == piece.files.len() {
                let mut hasher = Sha1::new();
                hasher.input(buffer);
                hasher.result_str() == piece.hash
            } else {
                PieceMatcher::scan_internal_disk(paths, buffer, finder, piece)?
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

    fn preload<T: PathFinder>(
        piece: &Piece,
        finder: &T,
    ) -> Result<HashMap<String, Vec<u8>>, std::io::Error> {
        let mut preload: HashMap<String, Vec<u8>> = HashMap::new();

        for piece_file in &piece.files {
            let entries = finder.find_length(piece_file.file_length);

            for entry in entries {
                let value =
                    PieceMatcher::read_bytes(entry, piece_file.read_length, piece_file.read_start_position)?;

                let key = format!("{}:{}:{}", entry.clone().display(), piece_file.read_length, piece_file.read_start_position);
                preload.insert(key.clone(), value);
            }
        }

        Ok(preload)
    }

    fn read_bytes_cache<'a>(
        path: &PathBuf,
        read_length: u64,
        read_start_position: u64,
        cache: &'a HashMap<String, Vec<u8>>,
    ) -> &'a Vec<u8> {
        let key = format!("{}:{}:{}", path.clone().display(), read_length, read_start_position);
        return cache.get(&key).unwrap();
    }
}
