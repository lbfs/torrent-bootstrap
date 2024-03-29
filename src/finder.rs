use std::{
    collections::{HashMap, HashSet}, fs::File, io::{Read, Seek, SeekFrom}, path::{Path, PathBuf}
};
use walkdir::WalkDir;

pub struct LengthFileFinder {
    pub cache: HashMap<u64, Vec<PathBuf>>,
}

impl LengthFileFinder {
    pub fn new() -> LengthFileFinder {
        LengthFileFinder {
            cache: HashMap::new(),
        }
    }

    pub fn add(&mut self, lengths: &[u64], scan_directory: &Path) {
        let entries = WalkDir::new(scan_directory)
            .into_iter()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_type().is_file() && lengths.contains(&(e.metadata().unwrap().len())));

        // Create a temporary cache for storing the matches with the matches entries.
        let mut matches: HashMap<u64, HashSet<PathBuf>> = HashMap::new();
        for entry in entries {
            let length = entry.metadata().unwrap().len();

            matches.entry(length).or_default();
            
            let items = matches.get_mut(&length).unwrap();
            items.insert(entry.into_path());
        }

        // Add entries from temporary cache into struct cache
        for (length, mut entries) in matches.into_iter() {
            let previous = match self.cache.remove(&length) {
                Some(value) => value,
                None => Vec::new()
            };

            entries.extend(previous);
            self.cache.insert(length, entries.into_iter().collect());
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

pub(crate) fn read_bytes(
    path: &PathBuf,
    read_length: u64,
    read_start_position: u64,
) -> Result<Vec<u8>, std::io::Error> {
    let mut read_bytes = vec![0u8; read_length as usize];
    let mut handle = File::open(path)?;

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