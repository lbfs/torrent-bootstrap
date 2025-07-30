use std::{fs::File, io::{Read, Seek, SeekFrom}, path::PathBuf, sync::Arc};

use hashlru::Cache;
use sha1::{Digest, Sha1};

use crate::orchestrator::OrchestrationPiece;

pub fn scan(
    piece: &OrchestrationPiece,
    output_paths: &mut Vec<Option<Arc<PathBuf>>>,
    output_bytes: &mut Vec<u8>,
    handle_cache: &mut Cache<Arc<PathBuf>, File>
) -> std::io::Result<bool> {
    let first_file = piece.files.first().unwrap();
    let search_paths = first_file.metadata.searches.as_ref().unwrap();
    let mut hasher = Sha1::new();

    for search_path in search_paths {
        // Get handle from cache or create if not present
        // TODO: Hoist this check
        let handle = match handle_cache.get_mut(search_path) {
            Some(handle) => {
                handle
            },
            None => {
                let handle = File::open(search_path.as_ref())?;
                handle_cache.insert(search_path.clone(), handle);
                handle_cache.get_mut(search_path).unwrap()
            },
        };

        output_bytes.clear();
        handle.seek(SeekFrom::Start(first_file.read_start_position))?;
        handle.take(first_file.read_length)
            .read_to_end(output_bytes)?;


        // Validate
        hasher.update(&output_bytes);
        let hash = hasher.finalize_reset();

        if piece.hash.as_slice().cmp(&hash).is_eq() {

            output_paths.clear();
            output_paths.push(Some(search_path.clone()));

            return Ok(true)
        }
    }

    return Ok(false);
}