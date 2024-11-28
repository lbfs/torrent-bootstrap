use std::{
    collections::{HashMap, HashSet}, fs::File, io::{Read, Seek, SeekFrom}, path::{Path, PathBuf}
};
use walkdir::WalkDir;

use crate::{get_sha1_hexdigest, Torrent};
use crate::File as TorrentFile;

pub struct LengthFileFinder {
    cache: HashMap<u64, Vec<PathBuf>>,
}

impl LengthFileFinder {
    pub fn new(torrents: &[Torrent], scan_directories: &[PathBuf]) -> LengthFileFinder {
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

        let mut cache = HashMap::new();
        for scan_directory in scan_directories {
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
                    let items: &mut Vec<PathBuf> = cache.entry(file_length).or_default();
                    let path = result.into_path();

                    if !items.contains(&path) {
                        items.push(path);
                    }
                }
            }
        }

        LengthFileFinder {
            cache
        }
    }

    pub fn find_length(&self, length: u64) -> &[PathBuf] {
        static EMPTY_RESULT: [PathBuf; 0] = [];
        match self.cache.get(&length) {
            Some(value) => value.as_slice(),
            None => &EMPTY_RESULT
        }
    }
}

pub struct FileFinder {
    pub search: Vec<Vec<PathBuf>>,
    lengths: Vec<u64>,
    index_to_path: Vec<PathBuf>
}

impl FileFinder {
    pub fn new(torrents: &[Torrent], export_directory: &Path, length_finder: LengthFileFinder) -> FileFinder {
        let mut export_search: Vec<Vec<PathBuf>> = Vec::new();
        let mut index_to_path: Vec<PathBuf> = Vec::new();
        let mut export_lengths: Vec<u64> = Vec::new();

        for torrent in torrents {
            if torrent.info.files.is_some() {
                for file in torrent.info.files.as_ref().unwrap() {

                    let entries = length_finder.find_length(file.length);
                    let partial_target = file.path.iter().collect::<PathBuf>();
                    let full_target = format_path_multiple(file, torrent, export_directory);

                    let sorted = sort_by_target_absolute_path(&partial_target, &full_target, entries);

                    let searches = sorted
                        .into_iter()
                        .map(|value| value.to_path_buf())
                        .collect(); 


                    export_lengths.push(file.length);
                    index_to_path.push(full_target);
                    export_search.push(searches);
                }
            } else if torrent.info.length.is_some() {
                let entries = length_finder.find_length(torrent.info.length.unwrap());
                let full_target = format_path_single(torrent, export_directory);
                let partial_target = Path::new(&torrent.info.name);

                let sorted = sort_by_target_absolute_path(partial_target, &full_target, entries);

                let searches = sorted
                    .into_iter()
                    .map(|value| value.to_path_buf())
                    .collect(); 

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

    pub fn get_paths_in_index_order(&self) -> &[PathBuf] {
        &self.index_to_path
    }

    pub fn find_path_from_index(&self, index: usize) -> &PathBuf {
        self.index_to_path.get(index).unwrap()
    }

    pub fn find_length(&self, index: usize) -> u64 {
        *self.lengths.get(index).unwrap()
    }

    pub fn find_searches(&self, index: usize) -> &[PathBuf] {
        self.search.get(index).unwrap().as_ref()
    }
}

pub(crate) fn sort_by_target_absolute_path<'a>(partial_target: &Path, full_target: &Path, entries: &'a [PathBuf]) -> Vec<&'a PathBuf> {
    let mut entries: Vec<&PathBuf> = entries.iter().collect();

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
