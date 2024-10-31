use std::path::PathBuf;

use sha1::{Digest, Sha1};

use crate::{finder::{read_bytes, ExportFileFinder}, orchestrator::OrchestrationPiece};

pub fn scan(
    finder: &ExportFileFinder,
    mut entry: &mut OrchestrationPiece,
) -> Result<bool, std::io::Error> {
    let loaded = preload(entry, finder)?;
    scan_internal(0, Sha1::new(), &loaded, &mut entry)
}

fn scan_internal<'a>(
    depth: usize,
    hasher: Sha1,
    finder: &Vec<Vec<(Option<&'a PathBuf>, Vec<u8>)>>,
    entry: &mut OrchestrationPiece
) -> Result<bool, std::io::Error> {
    let entries = &finder[depth];

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
            depth_file.source = if let Some(path) = path { Some(path.to_path_buf()) } else { None };
            return Ok(valid);
        }
    }

    Ok(false)
}

fn preload<'a>(entry: &OrchestrationPiece, finder: &'a ExportFileFinder) -> Result<Vec<Vec<(Option<&'a PathBuf>, Vec<u8>)>>, std::io::Error> {
    let mut loaded = Vec::with_capacity(entry.files.len());

    for file in entry.files.iter() {
        let mut results: Vec<(Option<&'a PathBuf>, Vec<u8>)> = Vec::new();

        if file.is_padding_file { 
            results.push((None, vec![0; file.read_length as usize]));
        } else {
            let search_paths = finder.find_length(file.export_index);
    
            // De-duplicate identical files if the file has already been seen.
            'inner: for search_path in search_paths {
               let value = read_bytes(search_path, file.read_length, file.read_start_position)?;
    
                for (_, result_bytes) in results.iter() {
                    if result_bytes.cmp(&value).is_eq() {
                        continue 'inner;
                    }
                }
    
                results.push((Some(search_path), value));
            }
        }

        loaded.push(results);
    }

    Ok(loaded)
}