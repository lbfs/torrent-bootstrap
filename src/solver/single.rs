use sha1::{Digest, Sha1};

use crate::{finder::read_bytes, orchestrator::OrchestrationPiece};

use super::PieceMatchResult;

pub fn scan<'a>(
    entry: &'a OrchestrationPiece,
) -> Result<Option<PieceMatchResult<'a>>, std::io::Error> {

    let first_file = entry.files.first().unwrap();
    let search_paths = first_file.metadata.searches.as_ref().unwrap();

    for search_path in search_paths {
        let bytes = read_bytes(search_path, first_file.read_length, first_file.read_start_position)?;

        let mut hasher = Sha1::new();
        hasher.update(&bytes);
        let hash = hasher.finalize();

        if entry.hash.as_slice().cmp(&hash).is_eq() {
            return Ok(Some(PieceMatchResult { 
                bytes, 
                source: vec![Some(search_path)]
            }));
        }
    }

    Ok(None)
}