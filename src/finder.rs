use std::{
    collections::HashMap, fs::File, io::{Read, Seek, SeekFrom}, path::{Path, PathBuf}
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
        for result in WalkDir::new(scan_directory) {
            if let Ok(result) = result {
                if result.file_type().is_file() && lengths.contains(&(result.metadata().unwrap().len())) {
                    let length = result.metadata().unwrap().len();
                    self.cache.entry(length).or_default();
                
                    let items = self.cache.get_mut(&length).unwrap();
                    let path = result.into_path();
        
                    if !items.contains(&path) {
                        items.push(path);
                    }
                }
            }
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