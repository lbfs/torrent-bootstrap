use sha1::{Digest, Sha1};

use crate::{finder::read_bytes_reuse_buffer, orchestrator::OrchestrationPiece};

use super::PieceMatchResult;

pub fn scan(
    piece: &OrchestrationPiece,
    result: &mut PieceMatchResult,
) -> std::io::Result<bool> {
    let first_file = piece.files.first().unwrap();
    let search_paths = first_file.metadata.searches.as_ref().unwrap();
    let mut hasher = Sha1::new();

    for search_path in search_paths {
        read_bytes_reuse_buffer(
            search_path,
            first_file.read_length,
            first_file.read_start_position,
            &mut result.bytes,
        )?;

        hasher.update(&result.bytes);
        let hash = hasher.finalize_reset();

        if piece.hash.as_slice().cmp(&hash).is_eq() {
            let output_paths = &mut result.source;

            output_paths.clear();
            output_paths.push(Some(search_path.clone()));

            return Ok(true)
        }
    }

    return Ok(false);
}