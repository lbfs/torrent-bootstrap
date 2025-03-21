use std::{path::PathBuf, sync::Arc};

use sha1::{digest::core_api::CoreWrapper, Digest, Sha1, Sha1Core};

use crate::{finder::read_bytes, orchestrator::OrchestrationPiece};

use super::PieceMatchResult;

type Cache = Vec<Vec<(Option<Arc<PathBuf>>, Vec<u8>)>>;

pub fn scan(
    piece: &OrchestrationPiece,
    result: &mut PieceMatchResult
) -> std::io::Result<bool> {
    let loaded = preload(piece)?;

    let mut check = vec![0; loaded.len()];
    let mut hasher = Sha1::new();

    let found = scan_internal(0, &mut check, &loaded, &mut hasher, piece);

    if found { 
        let output_buffer = &mut result.bytes;
        let output_paths = &mut result.source;

        output_buffer.clear();
        output_paths.clear();

        for (depth, index) in check.into_iter().enumerate() {
            let (path, value) = &loaded[depth][index];
            output_buffer.extend(value);
            output_paths.push(path.clone());
        }

        return Ok(true)
    }
    
    Ok(false)
}

fn scan_internal(
    depth: usize,
    check: &mut [usize],
    finder: &Cache,
    hasher: &mut CoreWrapper<Sha1Core>,
    piece: &OrchestrationPiece
) -> bool {
    let entries = &finder[depth];

    for entry_index in 0..entries.len() {
        check[depth] = entry_index;

        let valid = if depth + 1 == piece.files.len() {
            for (depth, index) in check.iter().enumerate() {
                let index = *index;
                let value = &finder[depth][index].1;
                hasher.update(value);
            }
            
            piece.hash.as_slice().cmp(&hasher.finalize_reset()).is_eq()
        } else {
            scan_internal(depth + 1, check, finder, hasher, piece)
        };

        if valid {
            return valid;
        }
    }

    false
}

fn preload(piece: &OrchestrationPiece) -> std::io::Result<Cache> {
    let mut loaded = Vec::with_capacity(piece.files.len());

    for file in piece.files.iter() {
        let mut results: Vec<(Option<Arc<PathBuf>>, Vec<u8>)> = Vec::new();

        if file.metadata.is_padding_file { 
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
    
                results.push((Some(search_path.clone()), value));
            }
        }

        loaded.push(results);
    }

    Ok(loaded)
}