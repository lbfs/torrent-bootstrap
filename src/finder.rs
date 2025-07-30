use std::{
    collections::{HashMap, HashSet}, fs::OpenOptions, os::unix::fs::MetadataExt, path::{Path, PathBuf}, sync::{Arc, Mutex}
};
use walkdir::WalkDir;

use crate::{get_sha1_hexdigest, Pieces, Torrent};
use crate::File as TorrentFile;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct FileNodeInfo {
    device: u64,
    inode: u64
}

pub struct FileCache {
    nodes: HashMap<u64, HashMap<PathBuf, FileNodeInfo>>
}

impl FileCache {
    pub fn new() -> FileCache {
        FileCache {
            nodes: HashMap::new()
        }
    }

    pub fn add_by_directory_and_length(&mut self, directory: &Path, lengths: &HashSet<u64>) {
        for result in WalkDir::new(directory) {
            if let Err(e) = result {
                eprintln!("Encountered error while searching directory: {}", e);
                continue;
            }
    
            let result = result.unwrap();
            let metadata = result.metadata();
    
            if let Err(e) = result.metadata() {
                eprintln!("Encountered error while reading metadata: {}", e);
                continue;
            }
    
            let metadata = metadata.unwrap();

            if metadata.is_dir() {
                continue;
            }

            if metadata.is_symlink() {
                continue;
            }

            let file_length = metadata.len();
    
            if !lengths.contains(&file_length) { continue; }

            let file_inode = metadata.ino();
            let file_device = metadata.dev();

            let info = FileNodeInfo {
                device: file_device,
                inode: file_inode
            };

            let length_entry = self.nodes.entry(file_length).or_default();
            length_entry.insert(result.path().to_path_buf(), info);
        }
    }

    pub fn add_by_path(&mut self, path: &Path, length: u64) {
        let handle = OpenOptions::new()
            .write(false)
            .truncate(false)
            .create(false)
            .create_new(false)
            .read(true)
            .open(path);

        if let Err(err) = handle {
            eprintln!("Encountered error while opening file {:#?} with error {}", path, err);
            return;
        }

        let result = handle.unwrap();
        let metadata = result.metadata();
    
        if let Err(e) = result.metadata() {
            eprintln!("Encountered error while reading metadata {}", e);
            return;
        }

        let metadata = metadata.unwrap();

        if metadata.is_dir() {
            return;
        }

        if metadata.is_symlink() {
            return;
        }

        let file_length = metadata.len();
    
        if length != file_length { return; }

        let file_inode = metadata.ino();
        let file_device = metadata.dev();

        let info = FileNodeInfo {
            device: file_device,
            inode: file_inode
        };

        let length_entry = self.nodes.entry(file_length).or_default();
        length_entry.insert(path.to_path_buf(), info);
    }
}

fn find_file_similarity(entry: &Path, partial_target: &Path, full_target: &Path) -> usize {
    if entry.ends_with(full_target) { 
        0
    } else if entry.ends_with(partial_target) {
        1
    } else if entry.file_name().unwrap().eq(partial_target.file_name().unwrap()) {
        2
    } else {
        3
    }
}

fn sort_by_target_absolute_path(
    partial_target: &Path, 
    full_target: &Path, 
    entries: &mut Vec<PathBuf>
) {
    entries.sort_by(|a, b| {
        let left = find_file_similarity(a, partial_target, full_target);
        let right = find_file_similarity(b, partial_target, full_target);

        left.cmp(&right)
    });
}

fn prune_duplicate_hard_links(
    file_cache: &FileCache,
    length: u64,
    entries: Vec<PathBuf>
) -> Vec<PathBuf> {
    let mut seen: HashSet<FileNodeInfo> = HashSet::with_capacity(entries.len());
    let mut new_entries = Vec::new();

    for entry in entries.into_iter() {
        if let Some(entries) = file_cache.nodes.get(&length) {
            if let Some(info) = entries.get(&entry) {
                if !seen.contains(&info) {
                    seen.insert(info.clone());
                    new_entries.push(entry);
                }
            }
        }
    }

    new_entries
}

// If the user has not selected to pre-allocate files in their torrent client, the files will be smaller on disk in some circumstances if the pieces
// are not complete. This will ask the filesystem to correct the file length to the expected value, but also allow the rest of the script to properly 
// acknowledge the file exists.
pub fn fix_export_file_lengths(metadata: &[TorrentMetadataEntry]) -> Result<(), std::io::Error> {
    for entry in metadata {
        if entry.is_padding_file { continue; }

        let handle = OpenOptions::new()
            .write(false)
            .truncate(false)
            .create(false)
            .create_new(false)
            .read(true)
            .open(&entry.full_target);

        let expected_length = entry.file_length;

        if let Err(err) = handle {
            if err.kind() == std::io::ErrorKind::NotFound {
                continue;
            }
    
            return Err(err);
        }

        let handle = handle.unwrap();
        let actual_length = handle.metadata()?.len();

        // We should error out as soon as possible, before we start modifying user files, because something is clearly wrong.
        if actual_length > expected_length {
            Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, 
                format!("File {:#?} exists on filesystem, but the length of the file is greater than the file length in the piece. Aborting to prevent accidental data loss.", &entry.full_target)))?
        }
    }

    for entry in metadata {
        if entry.is_padding_file { continue; }

        let handle = OpenOptions::new()
            .write(true)
            .create(false)
            .create_new(false)
            .read(true)
            .truncate(false)
            .open(&entry.full_target);

        if let Err(err) = handle {
            if err.kind() == std::io::ErrorKind::NotFound {
                continue;
            }
    
            return Err(err);
        }

        let handle = handle.unwrap();
        let expected_length = entry.file_length;
        let actual_length = handle.metadata()?.len();

        if actual_length < expected_length {
            eprintln!("Updating {:#?} from length {} to length {}", entry.full_target, actual_length, expected_length);
            handle.set_len(expected_length)?;
        }
    }

    Ok(())
}

// We only look for file lengths where the file length exists in the metadata.
pub fn get_unique_file_lengths(metadata: &[TorrentMetadataEntry]) -> HashSet<u64> {
    metadata
        .iter()
        .filter(|value| !value.is_padding_file)
        .map(|value| value.file_length)
        .collect::<HashSet<u64>>()
}

pub fn add_export_paths(metadata: &[TorrentMetadataEntry], file_cache: &mut FileCache) {
    for entry in metadata {
        if entry.is_padding_file { continue; }

        file_cache.add_by_path(&entry.full_target, entry.file_length);
    }
}

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

pub fn build_global_torrent_state(torrents: &[Torrent]) -> TorrentProcessState {
    let mut total_pieces = 0;

    for torrent in torrents {
        let pieces = Pieces::from_torrent(torrent);
        total_pieces += pieces.len();
    }

    TorrentProcessState::new(total_pieces)
}

#[derive(Debug)]
pub struct TorrentMetadataEntry {
    pub id: usize,
    pub info_hash: Vec<u8>,
    file_index: usize,
    pub file_length: u64,
    pub full_target: PathBuf,
    partial_target: PathBuf,
    pub is_padding_file: bool,
    pub searches: Option<Box<[Arc<PathBuf>]>>,
    pub processing_state: Mutex<TorrentProcessState>
} 

pub fn build_torrent_metadata_table(torrents: &[Torrent], export_directory: &Path) -> Vec<TorrentMetadataEntry> {
    let mut result = Vec::new();
    let mut internal_id_counter: usize = 0;

    for torrent in torrents {
        let pieces = Pieces::from_torrent(torrent);

        if torrent.info.files.is_some() {
            for (file_index, file) in torrent.info.files.as_ref().unwrap().iter().enumerate() {
                let full_target = format_path_multiple(file, torrent, export_directory);
                let partial_target = file.path.iter().collect::<PathBuf>();
                let is_padding_file = file.path.len() == 2 && file.path[0] == ".pad" && file.path[1].chars().all(char::is_numeric);

                let total_pieces = pieces
                    .iter()
                    .filter(|piece| piece.files.iter().find(|iter_file| iter_file.file_index == file_index).is_some())
                    .count();

                let state = TorrentProcessState {
                    success_pieces: 0,
                    failed_pieces: 0,
                    fault_pieces: 0,
                    writable_pieces: 0,
                    ignored_pieces: 0,
                    total_pieces
                };

                let meta: TorrentMetadataEntry = TorrentMetadataEntry { 
                    id: internal_id_counter, 
                    info_hash: torrent.info_hash.clone(), 
                    file_index,
                    file_length: file.length,
                    full_target,
                    partial_target,
                    is_padding_file,
                    searches: None,
                    processing_state: Mutex::new(state)
                };

                result.push(meta);
                internal_id_counter += 1;
            }
        } else if torrent.info.length.is_some() {
            let full_target = format_path_single(torrent, export_directory);
            let partial_target = Path::new(&torrent.info.name).to_path_buf();

            let total_pieces = pieces.len();
            let state = TorrentProcessState {
                success_pieces: 0,
                failed_pieces: 0,
                fault_pieces: 0,
                writable_pieces: 0,
                ignored_pieces: 0,
                total_pieces
            };

            let meta = TorrentMetadataEntry { 
                id: internal_id_counter, 
                info_hash: torrent.info_hash.clone(), 
                file_index: 0,
                file_length: torrent.info.length.unwrap(),
                full_target,
                partial_target,
                is_padding_file: false,
                searches: None,
                processing_state: Mutex::new(state)
            };

            result.push(meta);
            internal_id_counter += 1;
        }
    }
    result
}

pub fn populate_metadata_searches(metadata: &mut [TorrentMetadataEntry], file_cache: &FileCache) {
    let mut path_to_reference_counted: HashMap<PathBuf, Arc<PathBuf>> = HashMap::new();

    for entry in metadata {
        if entry.is_padding_file { continue; }

        let full_target = &entry.full_target;
        let partial_target = &entry.partial_target;
        let length = entry.file_length;

        if let Some(nodes) = file_cache.nodes.get(&length) {
            // Generate search paths in most similar order to torrent file name and remove any hard links.
            let mut found: Vec<_> = nodes.keys().map(|value| value.to_path_buf()).collect();

            sort_by_target_absolute_path(&partial_target, &full_target, &mut found);

            let deduplicated_entries = prune_duplicate_hard_links(file_cache, length, found);

            // Store de-duplicated file paths
            let search_references = deduplicated_entries
                .into_iter()
                .map(|path| add_path_or_get_if_exists(&mut path_to_reference_counted, path))
                .collect::<Box<_>>();

            entry.searches = Some(search_references);
        }
    }
}

fn add_path_or_get_if_exists(path_to_reference_counted: &mut HashMap<PathBuf, Arc<PathBuf>>, path: PathBuf) -> Arc<PathBuf> {
    if let Some(existing_path) = path_to_reference_counted.get(&path) {
        return existing_path.clone();
    }

    let value = Arc::new(path.clone());
    path_to_reference_counted.insert(path, value.clone());
    value
}

pub fn arcify_metadata(metadata: Vec<TorrentMetadataEntry>) -> Vec<Arc<TorrentMetadataEntry>> {
    metadata
        .into_iter()
        .map(|item| Arc::new(item))
        .collect()
}

pub fn build_info_hash_file_index_lookup_table(metadata: &[Arc<TorrentMetadataEntry>]) -> HashMap<Vec<u8>, HashMap<usize, Arc<TorrentMetadataEntry>>> {
    let mut metadata_id_lookup: HashMap<Vec<u8>, HashMap<usize, Arc<TorrentMetadataEntry>>> = HashMap::new();

    for entry in metadata.iter() {
    
        metadata_id_lookup
            .entry(entry.info_hash.clone())
            .or_default()
            .entry(entry.file_index)
            .or_insert(entry.clone());
    }

    metadata_id_lookup
}

fn format_path_multiple(file: &TorrentFile, torrent: &Torrent, export_directory: &Path) -> PathBuf {
    let data = Path::new("Data");
    let info_hash_as_human = get_sha1_hexdigest(&torrent.info_hash);
    let info_hash_path = Path::new(&info_hash_as_human);
    let torrent_name = Path::new(&torrent.info.name);

    [export_directory, info_hash_path, data, torrent_name, &file.path.iter().collect::<PathBuf>()]
        .iter()
        .collect()
}

fn format_path_single(torrent: &Torrent, export_directory: &Path) -> PathBuf {
    let data = Path::new("Data");
    let info_hash_as_human = get_sha1_hexdigest(&torrent.info_hash);
    let info_hash_path = Path::new(&info_hash_as_human);
    let torrent_name = Path::new(&torrent.info.name);

    [export_directory, info_hash_path, data, torrent_name]
        .iter()
        .collect()
}
