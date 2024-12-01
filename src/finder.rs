use std::{
    collections::{HashMap, HashSet}, fs::{File, OpenOptions}, io::{Read, Seek, SeekFrom}, path::{Path, PathBuf}, sync::Arc
};
use walkdir::WalkDir;

use crate::{get_sha1_hexdigest, Torrent};
use crate::File as TorrentFile;

fn generate_export_path_to_length(torrents: &[Torrent], export_directory: &Path) -> HashMap<PathBuf, u64> {
    let mut res = HashMap::new();

    for torrent in torrents {
        if torrent.info.files.is_some() {
            for file in torrent.info.files.as_ref().unwrap() {
                let target = format_path_multiple(file, torrent, export_directory);
                let length = file.length;

                res.insert(target, length);
            }
        } else if torrent.info.length.is_some() {
            let target = format_path_single(torrent, export_directory);
            let length = torrent.info.length.unwrap();

            res.insert(target, length);
        }
    }

    res
}

// Validate we aren't corrupting data; or that we haven't missed any files due to pre-allocation not happening.
fn fix_export_path_lengths(export_path_with_length: HashMap<PathBuf, u64>) -> Result<HashMap<PathBuf, u64>, std::io::Error> {
    let mut cache = HashMap::new();

    for (export_path, expected_length) in export_path_with_length.into_iter() {
        let handle = OpenOptions::new().write(true).create(false).open(&export_path);

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
                format!("File {:#?} exists on filesystem, but the length of the file is greater than the file length in the piece. Aborting to prevent accidental data loss.", export_path)))?
        } else if actual_length != expected_length {
            handle.set_len(expected_length)?;
        }

        cache.insert(export_path, expected_length);
    }

    Ok(cache)
}

fn get_unique_file_lengths(torrents: &[Torrent]) -> HashSet<u64> {
    let mut unique_lengths: HashSet<u64> = HashSet::new();

    for torrent in torrents {
        if torrent.info.length.is_some() {
            unique_lengths.insert(torrent.info.length.unwrap());
        } else if torrent.info.files.is_some() {
            for file in torrent.info.files.as_ref().unwrap() {
                unique_lengths.insert(file.length);
            }
        }
    }

    unique_lengths
}

fn add_matching_lengths_in_scan_directory(cache: &mut HashMap<u64, Vec<PathBuf>>, unique_lengths: &HashSet<u64>, scan_directory: &Path) {
    for result in WalkDir::new(scan_directory) {
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
        
        if !metadata.is_file() {
            continue;
        }

        let file_length = metadata.len();

        if unique_lengths.contains(&file_length) {
            let items: &mut _ = cache.entry(file_length).or_default();
            let path = result.path();

            let found = items.iter()
                .find(|value| (*value).eq(path));

            if found.is_none() {
                items.push(path.to_path_buf());
            }
        }
    }
}

pub fn setup_finder_cache(torrents: &[Torrent], export_directory: &Path, scan_directories: &[PathBuf]) -> Result<HashMap<u64, Vec<PathBuf>>, std::io::Error> {
    let mut cache: HashMap<u64, Vec<PathBuf>> = HashMap::new();
    let unique_lengths = get_unique_file_lengths(torrents);

    // Add any export files and treat them as part of the scan path, even if they are not explicitly defined there
    // This will stop additional writes that do not need to occur. 
    let export_path_to_length = generate_export_path_to_length(torrents, export_directory);
    let export_path_to_length = fix_export_path_lengths(export_path_to_length)?;

    for (export_path, length) in export_path_to_length {
        let items: &mut _ = cache.entry(length).or_default();

        let found = items.iter()
            .find(|value| (*value).eq(&export_path));
    
        if found.is_none() {
            println!("Adding {:#?} as it is not defined in the search path.", export_path);
            items.push(export_path);
        }
    }

    // Now, scan the preferred search paths from the user
    for scan_directory in scan_directories {
        add_matching_lengths_in_scan_directory(&mut cache, &unique_lengths, scan_directory);
    }

    Ok(cache)
}

pub fn intern_paths(finder: HashMap<u64, Vec<PathBuf>>) -> HashMap<u64, Box<[Arc<PathBuf>]>> {
    let mut cache: HashMap<u64, Box<[Arc<PathBuf>]>> = HashMap::new();

    for (length, search) in finder {
        cache.insert(length, 
            search.into_iter().map(|value| Arc::new(value)).collect());
    }

    cache
}

pub struct FileFinder {
    search: Vec<Vec<Arc<PathBuf>>>,
    lengths: Vec<u64>,
    index_to_path: Vec<PathBuf>
}

impl FileFinder {
    pub fn new(torrents: &[Torrent], export_directory: &Path, length_finder: HashMap<u64, Box<[Arc<PathBuf>]>>) -> FileFinder {
        let mut export_search: Vec<Vec<Arc<PathBuf>>> = Vec::new();
        let mut index_to_path: Vec<PathBuf> = Vec::new();
        let mut export_lengths: Vec<u64> = Vec::new();

        for torrent in torrents {
            if torrent.info.files.is_some() {
                for file in torrent.info.files.as_ref().unwrap() {
                    
                    let full_target = format_path_multiple(file, torrent, export_directory);
                    let partial_target = file.path.iter().collect::<PathBuf>();

                    let searches = if let Some(entries) = length_finder.get(&file.length) {
                        sort_by_target_absolute_path(&partial_target, &full_target, entries)
                            .into_iter()
                            .map(|value| value.clone())
                            .collect()
                    } else {
                        Vec::new()
                    };

                    export_lengths.push(file.length);
                    index_to_path.push(full_target);
                    export_search.push(searches);

                }
            } else if torrent.info.length.is_some() {
                let full_target = format_path_single(torrent, export_directory);
                let partial_target = Path::new(&torrent.info.name);

                let searches = if let Some(entries) = length_finder.get(&torrent.info.length.unwrap()) {
                    sort_by_target_absolute_path(partial_target, &full_target, entries)
                        .into_iter()
                        .map(|value| value.clone())
                        .collect()
                } else {
                    Vec::new()
                };

                export_lengths.push(torrent.info.length.unwrap());
                index_to_path.push(full_target);
                export_search.push(searches);
            }
        }

        FileFinder {
            search: export_search,
            lengths: export_lengths,
            index_to_path: index_to_path
        }
    }

    pub fn find_path_from_index(&self, index: usize) -> &PathBuf {
        self.index_to_path.get(index).unwrap()
    }

    pub fn find_length(&self, index: usize) -> u64 {
        *self.lengths.get(index).unwrap()
    }

    pub fn find_searches(&self, index: usize) -> &[Arc<PathBuf>] {
        self.search.get(index).unwrap().as_ref()
    }
}

pub(crate) fn sort_by_target_absolute_path<'a>(partial_target: &Path, full_target: &Path, entries: &'a [Arc<PathBuf>]) -> Vec<Arc<PathBuf>> {
    let mut entries: Vec<Arc<PathBuf>> = entries.iter().cloned().collect();

    entries.sort_by(|a, b| {
        let left = find_file_similarity(a, partial_target, full_target);
        let right = find_file_similarity(b, partial_target, full_target);

        left.cmp(&right)
    });

    entries
}

fn find_file_similarity(entry: &Path, partial_target: &Path, full_target: &Path) -> usize {
    if entry.ends_with(&full_target) { 
        0
    } else if entry.ends_with(&partial_target) {
        1
    } else if entry.file_name().unwrap().eq(partial_target.file_name().unwrap()) {
        2
    } else {
        3
    }
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
