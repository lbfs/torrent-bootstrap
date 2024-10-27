use sha1::{Digest, Sha1};

use crate::{finder::{read_bytes, sort_by_target_absolute_path, LengthFileFinder}, orchestrator::OrchestrationPiece};

pub fn scan(
    finder: &LengthFileFinder,
    entry: &mut OrchestrationPiece,
) -> Result<bool, std::io::Error> {

    let first_file = entry.files.first_mut().unwrap();
    let search_paths = finder.find_length(first_file.file_length);
    let search_paths = sort_by_target_absolute_path(&first_file.file_path, &first_file.export, search_paths);

    for search_path in search_paths {
        let bytes = read_bytes(search_path, first_file.read_length, first_file.read_start_position)?;

        let mut hasher = Sha1::new();
        hasher.update(&bytes);
        let hash = hasher.finalize();

        if entry.hash.as_slice().cmp(&hash).is_eq() {
            first_file.bytes = Some(bytes);
            first_file.source = Some(search_path.clone());
            return Ok(true);
        }
    }

    Ok(false)
}