use std::{collections::HashMap, path::PathBuf};

use sha1::{Digest, Sha1};

use crate::{finder::{read_bytes, sort_by_target_absolute_path, LengthFileFinder}, orchestrator::OrchestrationPiece};

pub fn scan(
    finder: &LengthFileFinder,
    mut entry: &mut OrchestrationPiece,
) -> Result<bool, std::io::Error> {
    let loaded = preload(entry, finder)?;
    scan_internal(0, Sha1::new(), &loaded, &mut entry)
}

fn scan_internal<'a>(
    depth: usize,
    hasher: Sha1,
    finder: &HashMap<usize, Vec<(&'a PathBuf, Vec<u8>)>>,
    entry: &mut OrchestrationPiece
) -> Result<bool, std::io::Error> {
    let entries = finder.get(&depth).unwrap();

    for (path, read_buffer) in entries.into_iter() {
        let mut hasher = hasher.clone();
        hasher.update(&read_buffer);

        let valid = if depth + 1 == entry.files.len() {
            let hash = hasher.finalize();
            entry.hash.as_slice().cmp(&hash).is_eq()
        } else {
            scan_internal(depth + 1, hasher, finder, entry)?
        };

        if valid {
            let depth_file = entry.files.get_mut(depth).unwrap();
            depth_file.bytes = Some(read_buffer.clone());
            depth_file.source = Some(path.to_path_buf());
            return Ok(valid);
        }
    }

    Ok(false)
}

fn preload<'a>(entry: &OrchestrationPiece, finder: &'a LengthFileFinder) -> Result<HashMap<usize, Vec<(&'a PathBuf, Vec<u8>)>>, std::io::Error> {
    let mut loaded = HashMap::new();

    for (file_position, file) in entry.files.iter().enumerate() {
        let mut results: Vec<(&'a PathBuf, Vec<u8>)> = Vec::new();
        let search_paths = finder.find_length(file.file_length);
        let search_paths = sort_by_target_absolute_path(&file.file_path, &file.export, search_paths);

        // De-duplicate identical files if the file has already been seen.
        'inner: for search_path in search_paths {
            let value = read_bytes(search_path, file.read_length, file.read_start_position)?;

            for (_, result_bytes) in results.iter() {
                if result_bytes.cmp(&value).is_eq() {
                    continue 'inner;
                }
            }

            results.push((search_path, value));
        }

        loaded.insert(file_position, results);
    }

    Ok(loaded)
}