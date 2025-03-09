use std::{
    collections::{HashMap, HashSet}, fs::{File, OpenOptions}, io::{Read, Seek, SeekFrom}, os::unix::fs::MetadataExt, path::{Path, PathBuf}, sync::Arc
};
use walkdir::WalkDir;

use crate::{get_sha1_hexdigest, Torrent};
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
                eprintln!("Encountered error while reading metadata {}", e);
                continue;
            }
    
            let metadata = metadata.unwrap();

            if metadata.is_dir() {
                eprintln!("Skipped {:#?} as it is a directory.", result.path());
                continue;
            }

            if metadata.is_symlink() {
                eprintln!("Skipped {:#?} as it is a symlink.", result.path());
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
            eprintln!("Skipped {:#?} as it is a directory.", path);
            return;
        }

        if metadata.is_symlink() {
            eprintln!("Skipped {:#?} as it is a symlink.", path);
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

pub struct FileFinder {
    search: HashMap<usize, Vec<Arc<PathBuf>>>,
    lengths: HashMap<usize, u64>,
    metadata_id_to_path: HashMap<usize, Arc<PathBuf>>,
    metadata_id_lookup: HashMap<Vec<u8>, HashMap<usize, usize>>
}

impl FileFinder {
    pub fn new(metadata: &[TorrentTableEntry], file_cache: &FileCache) -> FileFinder {
        let mut finder = FileFinder {
            search: HashMap::new(),
            lengths: HashMap::new(),
            metadata_id_lookup: HashMap::new(),
            metadata_id_to_path: HashMap::new()
        };

        FileFinder::initalize(&mut finder, metadata, file_cache);

        finder
    }

    fn initalize(finder: &mut FileFinder,  metadata: &[TorrentTableEntry], file_cache: &FileCache) {
        let mut path_to_reference_counted: HashMap<PathBuf, Arc<PathBuf>> = HashMap::new();

        for entry in metadata {
            let full_target = &entry.full_target;
            let partial_target = &entry.partial_target;
            let length = entry.file_length;
            let metadata_id = entry.id;

            if let Some(nodes) = file_cache.nodes.get(&length) {
                // Generate search paths in most similar order to torrent file name and remove any hard links.
                let mut found: Vec<_> = nodes.keys().map(|value| value.to_path_buf()).collect();

                sort_by_target_absolute_path(&partial_target, &full_target, &mut found);
                let entries = prune_duplicate_hard_links(file_cache, length, found);

                // Store de-duplicated file paths
                let search_references = entries
                    .into_iter()
                    .map(|path| FileFinder::add_path_or_get_if_exists(&mut path_to_reference_counted, path))
                    .collect::<Vec<_>>();

                finder.search.insert(metadata_id, search_references);
            }
        }

        for entry in metadata {
            finder.metadata_id_lookup
                .entry(entry.info_hash.clone())
                .or_default()
                .entry(entry.file_index)
                .or_insert(entry.id);

            finder.lengths.insert(entry.id, entry.file_length);

            let file_path = FileFinder::add_path_or_get_if_exists(&mut path_to_reference_counted, entry.full_target.clone());
            finder.metadata_id_to_path.insert(entry.id, file_path);
        }

    }

    fn add_path_or_get_if_exists(path_to_reference_counted: &mut HashMap<PathBuf, Arc<PathBuf>>, path: PathBuf) -> Arc<PathBuf> {
        if let Some(datum) = path_to_reference_counted.get(&path) {
            return datum.clone();
        }

        let value = Arc::new(path.clone());
        path_to_reference_counted.insert(path, value.clone());
        value
    }

    pub fn find_full_target(&self, id: usize) -> &PathBuf {
        self.metadata_id_to_path.get(&id).as_ref().unwrap()
    }

    pub fn find_id_from_info_hash_file_index(&self, info_hash: &[u8], file_index: usize) -> usize {
        let info_hash_entry = self.metadata_id_lookup.get(info_hash).unwrap();
        *info_hash_entry.get(&file_index).unwrap()
    }

    pub fn find_length(&self, id: usize) -> u64 {
        *self.lengths.get(&id).unwrap()
    }

    pub fn find_searches(&self, id: usize) -> Option<&Vec<Arc<PathBuf>>> {
        self.search.get(&id)
    }

    pub fn find_searches_unsafe(&self, id: usize) -> &Vec<Arc<PathBuf>> {
        self.search.get(&id).unwrap().as_ref()
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
pub fn fix_export_file_lengths(metadata: &[TorrentTableEntry]) -> Result<(), std::io::Error> {
    for entry in metadata {
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

        if actual_length > expected_length {
            Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, 
                format!("File {:#?} exists on filesystem, but the length of the file is greater than the file length in the piece. Aborting to prevent accidental data loss.", &entry.full_target)))?
        }
    }

    for entry in metadata {
        let handle = OpenOptions::new()
            .write(true)
            .create(false)
            .create_new(false)
            .read(true)
            .truncate(false)
            .open(&entry.full_target)?;

        let expected_length = entry.file_length;
        handle.set_len(expected_length)?;
    }

    Ok(())
}

// We only look for file lengths where the file length exists in the metadata.
pub fn get_unique_file_lengths(metadata: &[TorrentTableEntry]) -> HashSet<u64> {
    metadata
        .iter()
        .map(|value| value.file_length)
        .collect::<HashSet<u64>>()
}

pub fn add_export_paths(metadata: &[TorrentTableEntry], file_cache: &mut FileCache) {
    for entry in metadata {
        file_cache.add_by_path(&entry.full_target, entry.file_length);
    }
}

pub struct TorrentTableEntry {
    id: usize,
    info_hash: Vec<u8>,
    file_index: usize,
    file_length: u64,
    full_target: PathBuf,
    partial_target: PathBuf
} 

pub fn build_torrent_metadata_table(torrents: &[Torrent], export_directory: &Path) -> Vec<TorrentTableEntry> {
    let mut result = Vec::new();
    let mut internal_id_counter: usize = 0;

    for torrent in torrents {
        if torrent.info.files.is_some() {
            for (file_index, file) in torrent.info.files.as_ref().unwrap().iter().enumerate() {
                let full_target = format_path_multiple(file, torrent, export_directory);
                let partial_target = file.path.iter().collect::<PathBuf>();

                let meta: TorrentTableEntry = TorrentTableEntry { 
                    id: internal_id_counter, 
                    info_hash: torrent.info_hash.clone(), 
                    file_index,
                    file_length: file.length,
                    full_target,
                    partial_target
                };

                result.push(meta);
                internal_id_counter += 1;
            }
        } else if torrent.info.length.is_some() {
            let full_target = format_path_single(torrent, export_directory);
            let partial_target = Path::new(&torrent.info.name).to_path_buf();

            let meta = TorrentTableEntry { 
                id: internal_id_counter, 
                info_hash: torrent.info_hash.clone(), 
                file_index: 0,
                file_length: torrent.info.length.unwrap(),
                full_target,
                partial_target
            };

            result.push(meta);
            internal_id_counter += 1;
        }
    }
    result
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

pub(crate) fn read_bytes(
    path: &Path,
    read_length: u64,
    read_start_position: u64,
) -> Result<Vec<u8>, std::io::Error> {
    let mut handle = File::open(path)?;
    read_bytes_with_handle(&mut handle, read_length, read_start_position)
}

pub(crate) fn read_bytes_with_handle(
    handle: &mut File,
    read_length: u64,
    read_start_position: u64
) -> Result<Vec<u8>, std::io::Error> {
    let mut read_bytes = vec![0u8; read_length as usize];

    handle.seek(SeekFrom::Start(read_start_position))?;
    handle.read_exact(&mut read_bytes)?;

    Ok(read_bytes)
}
