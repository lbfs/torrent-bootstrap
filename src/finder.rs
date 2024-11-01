use std::{
    collections::{HashMap, HashSet}, fs::File, io::{Read, Seek, SeekFrom}, path::{Path, PathBuf}
};
use walkdir::WalkDir;

use crate::{get_sha1_hexdigest, PieceFile, Torrent};
use crate::File as TorrentFile;

pub struct LengthFileFinder {
    pub cache: HashMap<u64, Vec<PathBuf>>,
}

impl LengthFileFinder {
    pub fn new(torrents: &[Torrent], scan_directories: &[PathBuf]) -> LengthFileFinder {
        // Unique File Lengths
        let mut lengths: HashSet<u64> = HashSet::new();

        for torrent in torrents {
            if torrent.info.length.is_some() {
                lengths.insert(torrent.info.length.unwrap());
            } else if torrent.info.files.is_some() {
                for file in torrent.info.files.as_ref().unwrap() {
                    lengths.insert(file.length);
                }
            }
        }

        // Scan the disk
        let mut cache = HashMap::new();
        for scan_directory in scan_directories {
            for result in WalkDir::new(scan_directory) {
                if let Ok(result) = result {
                    if result.file_type().is_file() && lengths.contains(&(result.metadata().unwrap().len())) {
                        let length = result.metadata().unwrap().len();
                        cache.entry(length).or_default();
                    
                        let items: &mut Vec<PathBuf> = cache.get_mut(&length).unwrap();
                        let path = result.into_path();
            
                        if !items.contains(&path) {
                            items.push(path);
                        }
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


#[derive(Clone)]
pub struct FileFinder {
    search: Vec<Vec<PathBuf>>,
    lengths: Vec<u64>,
    index_to_path: Vec<PathBuf>,
    pub path_to_index: HashMap<PathBuf, usize>, // TODO: Remove public accessor
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

                    sort_by_target_absolute_path(&partial_target, &full_target, entries);

                    let searches = entries
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

                sort_by_target_absolute_path(partial_target, &full_target, entries);

                let searches = length_finder.find_length(torrent.info.length.unwrap())
                    .into_iter()
                    .map(|value| value.to_path_buf())
                    .collect(); 

                export_lengths.push(torrent.info.length.unwrap());
                index_to_path.push(full_target);
                export_search.push(searches);
            }
        }

        let mut path_to_index = HashMap::with_capacity(index_to_path.len());
        for (index, export_path) in index_to_path.iter().enumerate() {
            path_to_index.insert(export_path.clone(), index);
        }

        FileFinder {
            search: export_search,
            lengths: export_lengths,
            path_to_index: path_to_index,
            index_to_path: index_to_path
        }
    }

    pub fn find_path_from_index(&self, index: usize) -> &PathBuf {
        self.index_to_path.get(index).unwrap()
    }

    pub fn find_index_from_path(&self, path: &Path) -> usize {
        *self.path_to_index.get(path).unwrap()
    }

    pub fn find_length(&self, index: usize) -> u64 {
        *self.lengths.get(index).unwrap()
    }

    pub fn find_searches(&self, index: usize) -> &[PathBuf] {
        self.search.get(index).unwrap().as_ref()
    }
}

pub(crate) fn read_bytes(
    path: &PathBuf,
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

pub(crate) fn sort_by_target_absolute_path<'a>(partial_target: &Path, full_target: &Path, entries: &'a [PathBuf]) -> Vec<&'a PathBuf> {
    let mut entries: Vec<&PathBuf> = entries.iter().collect();

    // Sort by filename so that we check most-matching path first before checking 
    // other random files.
    entries.sort_by(|a, b| {
        let mut left = a.ends_with(partial_target) as usize;
        let mut right = b.ends_with(partial_target) as usize;

        left += (*a).eq(full_target) as usize;
        right += (*b).eq(full_target) as usize;
        
        if let Some(source) = partial_target.file_name() {
            if let Some(left_filename) = a.file_name() {
                left += source.cmp(left_filename).is_eq() as usize;
            }

            if let Some(right_filename) = b.file_name() {
                right += source.cmp(right_filename).is_eq() as usize;
            }
        }

        left.cmp(&right).reverse()
    });

    entries
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

pub fn format_path(file: &PieceFile, torrent: &Torrent, export_directory: &Path) -> PathBuf {
    let data = Path::new("Data");
    let info_hash_as_human = get_sha1_hexdigest(&torrent.info_hash);
    let info_hash_path = Path::new(&info_hash_as_human);
    let torrent_name = Path::new(&torrent.info.name);

    if torrent.info.files.is_some() {
        [
            export_directory,
            info_hash_path,
            data,
            torrent_name,
            file.file_path.as_path(),
        ]
        .iter()
        .collect()
    } else {
        [
            export_directory,
            info_hash_path,
            data,
            file.file_path.as_path(),
        ]
        .iter()
        .collect()
    }
}