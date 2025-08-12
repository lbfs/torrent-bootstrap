use std::{collections::HashMap, fs::OpenOptions, path::{Path, PathBuf}, sync::Mutex};

use crate::{filesystem::{ExportPathFormatter, FrozenPathInterner, PathCacheEntry, PathInterner}, torrent::{pieces::Pieces, Torrent}};

#[derive(Debug)]
pub struct TorrentProcessState {
    // Pieces that were discovered successfully
    pub success_pieces: usize,

    // Pieces that failed to find a successful on disk match.
    pub failed_pieces: usize,

    // Pieces that encountered a processing exception, like I/O error.
    pub fault_pieces: usize,

    // We may detect successful pieces that don't need to be written, as they 
    // may either be padding files or content is already on-disk.
    // For global state, pieces with partial writes do not count as ignored.
    pub writable_pieces: usize,
    pub ignored_pieces: usize,
    
    // Total pieces that might ever exist.
    pub total_pieces: usize
}

impl TorrentProcessState {
    pub fn new(total_pieces: usize) -> TorrentProcessState {
        TorrentProcessState {
            success_pieces: 0,
            failed_pieces: 0,
            fault_pieces: 0,
            writable_pieces: 0,
            ignored_pieces: 0,
            total_pieces
        }
    }
}

#[derive(Debug)]
pub struct TorrentPieceFileEntry {
    pub read_length: u64,
    pub read_start_position: u64,
    pub file_id: usize
}

#[derive(Debug)]
pub struct TorrentPieceEntry {
    pub piece_id: usize,
    pub hash: Vec<u8>,
    pub torrent_id: usize,
    pub position: usize,
    pub files: Vec<TorrentPieceFileEntry>,
    pub length: u64,
    pub total_choices: Vec<usize>
}

#[derive(Debug)]
pub struct TorrentFileEntry {
    pub file_id: usize,
    pub torrent_id: usize,
    pub file_length: u64,
    pub export_target: usize,
    pub relative_target: usize,
    pub padding: bool,
    pub searches: Option<Vec<usize>>,
    pub processing_state: Mutex<TorrentProcessState>
}

pub fn build_raw_torrent_piece_metadata(
    torrents: &[Torrent]
) -> Vec<TorrentPieceEntry> {
    let mut torrent_piece_entry: Vec<TorrentPieceEntry> = Vec::new();
    let mut base_file_id = 0;

    for (torrent_id, torrent) in torrents.iter().enumerate() {
        let pieces = Pieces::from_torrent(torrent);
        for piece in pieces {
            let piece_id = torrent_piece_entry.len();
            let position = piece.position;
            let length = piece.length;

            let mut files = Vec::new();
            for file_entry in piece.files {
                files.push(TorrentPieceFileEntry {
                    read_length: file_entry.read_length,
                    read_start_position: file_entry.read_start_position,
                    file_id: file_entry.file_index + base_file_id
                });
            }

            torrent_piece_entry.push(TorrentPieceEntry {
                piece_id,
                hash: piece.hash,
                torrent_id,
                position,
                files,
                length,
                total_choices: Vec::with_capacity(0)
            });
        }

        let base_id_increment = if torrent.info.length.is_some() {
            1
        } else if torrent.info.files.is_some() {
            torrent.info.files.as_ref().unwrap().len()
        } else {
            0
        };

        base_file_id += base_id_increment; 
    }

    torrent_piece_entry
}

pub fn build_raw_torrent_file_metadata<E: ExportPathFormatter>(
    torrents: &[Torrent], 
    path_interner: &mut PathInterner, 
    export_root: &Path
) -> Vec<TorrentFileEntry> {
    let mut torrent_file_entry: Vec<TorrentFileEntry> = Vec::new();

    for (torrent_id, torrent) in torrents.iter().enumerate() {
        if torrent.info.length.is_some() {
            let export_target = E::format_single_file(torrent, export_root);
            let relative_target = Path::new(&torrent.info.name).to_path_buf();

            let export_target_handle = path_interner.put(export_target);
            let relative_target_handle = path_interner.put(relative_target);

            torrent_file_entry.push(TorrentFileEntry {
                file_id: torrent_file_entry.len(),
                torrent_id: torrent_id,
                file_length: torrent.info.length.unwrap(),
                export_target: export_target_handle,
                relative_target: relative_target_handle,
                padding: false,
                searches: None,
                processing_state: Mutex::new(TorrentProcessState::new(torrent.info.pieces.len()))
            });
        } else if torrent.info.files.is_some() {
            for file in torrent.info.files.as_ref().unwrap().iter() {
                let export_target = E::format_multiple_files(file, torrent, export_root);
                let relative_target = file.path.iter().collect::<PathBuf>();

                let export_target_handle = path_interner.put(export_target);
                let relative_target_handle = path_interner.put(relative_target);

                torrent_file_entry.push(TorrentFileEntry {
                    file_id: torrent_file_entry.len(),
                    torrent_id: torrent_id,
                    file_length: file.length,
                    export_target: export_target_handle,
                    relative_target: relative_target_handle,
                    padding: file.padding(),
                    searches: None,
                    processing_state: Mutex::new(TorrentProcessState::new(torrent.info.pieces.len()))
                });
            }
        } 
    }

    torrent_file_entry
}

// TODO: NOTE TO SELF
// If the user is not correcting file lengths as part of their config, then we need to fail out any pieces that use those files
// Otherwise, we are potentially damaging existing data, or just exit...?
// We could potentially make this an option?
pub fn validate_export_file_length(entry: &TorrentFileEntry, path_interner: &PathInterner, resize_export_files: bool) -> Result<(), std::io::Error> {
    if entry.padding { return Ok(()); }

    let export_target = path_interner.get_by_id(entry.export_target);
    let handle = OpenOptions::new()
        .write(false)
        .truncate(false)
        .create(false)
        .create_new(false)
        .read(true)
        .open(export_target);

    if let Err(err) = handle {
        if err.kind() == std::io::ErrorKind::NotFound {
            return Ok(());
        }

        return Err(err);
    }

    let handle = handle.unwrap();

    let actual_length = handle.metadata()?.len();
    let expected_length = entry.file_length;

    // We should error out as soon as possible, before we start modifying user files, because something is clearly wrong.
    if !resize_export_files && actual_length < expected_length {
        Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, 
            format!("File {:#?} exists on filesystem, but the length of the file is less than the file length in the piece. Since resize_export_files is disabled, aborting to prevent accidental data loss.", export_target)))?
    }

    if actual_length > expected_length {
        Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, 
            format!("File {:#?} exists on filesystem, but the length of the file is greater than the file length in the piece, aborting to prevent accidental data loss.", export_target)))?
    }

    Ok(())
}

// If the user has not selected to pre-allocate files in their torrent client, the files will be smaller on disk in some circumstances if the pieces
// are not complete. This will ask the filesystem to correct the file length to the expected value, but also allow the rest of the script to properly 
// acknowledge the file exists.
pub fn correct_export_file_length(entry: &TorrentFileEntry, path_interner: &PathInterner) -> Result<(), std::io::Error> {
    if entry.padding { return Ok(()); }

    let export_target = path_interner.get_by_id(entry.export_target);
    let handle = OpenOptions::new()
        .write(true)
        .create(false)
        .create_new(false)
        .read(true)
        .truncate(false)
        .open(export_target);

    if let Err(err) = handle {
        if err.kind() == std::io::ErrorKind::NotFound {
            return Ok(());
        }

        return Err(err);
    }

    let handle = handle.unwrap();

    let actual_length = handle.metadata()?.len();
    let expected_length = entry.file_length;

    if actual_length < expected_length {
        eprintln!("Updating {:#?} from length {} to length {}", export_target, actual_length, expected_length);
        handle.set_len(expected_length)?;
    }
    Ok(())
}

pub fn calculate_total_choices_for_piece(
    torrent_file_metadata: &mut [TorrentFileEntry],
    torrent_piece_metadata: &mut [TorrentPieceEntry],
) {
    for piece_metadata in torrent_piece_metadata.iter_mut() {
        let mut choices = Vec::new();
        for piece_file in piece_metadata.files.iter() {
            let file = &torrent_file_metadata[piece_file.file_id];
            let searches = file.searches.as_ref();
            let padding = file.padding;

            if padding {
                choices.push(1);
            } else if let Some(searches) = searches {
                choices.push(searches.len());
            } else {
                choices.push(0);
            }
        }

        if choices.iter().any(|choice| *choice == 0) {
            choices.clear();
        }

        piece_metadata.total_choices = choices;

    }
}

pub fn discover_and_apply_searches(
    torrent_file_metadata: &mut [TorrentFileEntry], 
    disk_metadata: &HashMap<usize, PathCacheEntry>,
    path_interner: &FrozenPathInterner
) {

    // Aggregate all the files by their file-size so we can clone the searches across each entry
    // and then do post-processing for hard-link detection.
    let mut by_file_length_aggregation: HashMap<u64, Vec<usize>> = HashMap::new();

    for metadata in torrent_file_metadata.iter() {
        if by_file_length_aggregation.contains_key(&metadata.file_length) {
            continue;
        }

        let handles: Vec<usize> = disk_metadata.iter()
            .filter(|(_, entry)| entry.length() == metadata.file_length)
            .map(|(handle, _)| *handle)
            .collect();

        by_file_length_aggregation.insert(metadata.file_length, handles);
    }

    // Now copy across each metadata entry with hard-links removed.
    for metadata in torrent_file_metadata.iter_mut() {
        let handles = by_file_length_aggregation.get(&metadata.file_length).unwrap();
        let mut handles = handles.clone();

        // Sort the files by the ones that have the most-matching file-name to the one in the torrent.
        // This should always put the export path first in the search list so that validation checks
        // happen first during processing.
        let export_target = path_interner.get(metadata.export_target);
        let relative_target = path_interner.get(metadata.relative_target);

        handles.sort_by(|left, right| {
            let left_path = path_interner.get(*left);
            let right_path = path_interner.get(*right);

            let left_sim 
                = find_file_similarity(left_path, relative_target, export_target);
            let right_sim 
                = find_file_similarity(right_path, relative_target, export_target);

            left_sim.cmp(&right_sim)
        });

        // Remove hard-links by keeping the order of the ranking and only keeping the first
        // file that has a specific device and index node, discarding any other duplicates.
        let mut filtered = Vec::new();

        for handle in handles {
            let entry = disk_metadata.get(&handle).unwrap();
            let mut should_add = true;

            for added in filtered.iter() {
                let added_entry = disk_metadata.get(&added).unwrap();
                if entry.eq(added_entry) {
                    should_add = false;
                    break;
                }
            }

            if should_add {
                filtered.push(handle);
            }
        }

        // Search is valid only if there are items.
        if filtered.len() > 0 {
            metadata.searches = Some(filtered);
        }
    }
}

fn find_file_similarity(entry: &Path, relative_target: &Path, export_target: &Path) -> usize {
    if entry.ends_with(export_target) { 
        0
    } else if entry.ends_with(relative_target) {
        1
    } else if entry.file_name().unwrap().eq(relative_target.file_name().unwrap()) {
        2
    } else {
        3
    }
}