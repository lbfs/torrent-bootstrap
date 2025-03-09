use std::path::PathBuf;

use sha1::{Digest, Sha1};

use crate::{finder::read_bytes, orchestrator::OrchestrationPiece};

use super::PieceMatchResult;

pub fn scan<'a>(
    entry: &'a OrchestrationPiece,
) -> Result<Option<PieceMatchResult<'a>>, std::io::Error> {
    let loaded = preload(entry)?;

    let mut check = vec![0; loaded.len()];

    let found = scan_internal(0, &mut check, &loaded, entry);

    Ok(if found { 
        let mut output_buffer = Vec::new();
        let mut output_paths = Vec::new();

        for (depth, index) in check.into_iter().enumerate() {
            let (path, value) = &loaded[depth][index];
            output_buffer.extend(value);
            output_paths.push(*path);
        }

        Some(PieceMatchResult { bytes: output_buffer, source: output_paths })
     } else { 
        None 
    })
}

fn scan_internal(
    depth: usize,
    check: &mut [usize],
    finder: &Vec<Vec<(Option<&PathBuf>, Vec<u8>)>>,
    entry: &OrchestrationPiece
) -> bool {
    let entries = &finder[depth];

    for entry_index in 0..entries.len() {
        check[depth] = entry_index;

        let valid = if depth + 1 == entry.files.len() {
            let mut hasher = Sha1::new();
            
            for (depth, index) in check.iter().enumerate() {
                let index = *index;
                let value = &finder[depth][index].1;
                hasher.update(value);
            }
            
            entry.hash.as_slice().cmp(&hasher.finalize_reset()).is_eq()
        } else {
            scan_internal(depth + 1, check, finder, entry)
        };

        if valid {
            return valid;
        }
    }

    false
}

fn preload<'a>(entry: &'a OrchestrationPiece) -> Result<Vec<Vec<(Option<&'a PathBuf>, Vec<u8>)>>, std::io::Error> {
    let mut loaded = Vec::with_capacity(entry.files.len());

    for file in entry.files.iter() {
        let mut results: Vec<(Option<&'a PathBuf>, Vec<u8>)> = Vec::new();

        if file.is_padding_file { 
            results.push((None, vec![0; file.read_length as usize]));
        } else {
            let search_paths = file.metadata.searches.as_ref().unwrap();
    
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