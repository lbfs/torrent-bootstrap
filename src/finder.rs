use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
};
use walkdir::WalkDir;

pub struct CachedPathFinder {
    pub cache: HashMap<u64, Vec<PathBuf>>,
}

impl CachedPathFinder {
    pub fn new() -> CachedPathFinder {
        CachedPathFinder {
            cache: HashMap::new(),
        }
    }

    pub fn add(&mut self, lengths: &[u64], search_directory: &Path) {
        let entries = WalkDir::new(&search_directory)
            .into_iter()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_type().is_file() && lengths.contains(&e.metadata().unwrap().len()));

        // Create a temporary cache for storing the matches with the matches entries.
        let mut matches: HashMap<u64, HashSet<PathBuf>> = HashMap::new();
        for entry in entries {
            let length = entry.metadata().unwrap().len();

            // We could pre-generate these keys based on input lengths, but we do this here to prevent creating entries
            // with no path entries, to prevent unexpected behavior when calling
            // find_length
            if !matches.contains_key(&length) {
                matches.insert(length, HashSet::new());
            }

            // Safety: Unwrapping here is safe since we already have
            // the cache populated with a default entry
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
}

pub trait PathFinder {
    fn find_length<'a>(&'a self, length: u64) -> &'a [PathBuf];
}

impl PathFinder for CachedPathFinder {
    fn find_length<'a>(&'a self, length: u64) -> &'a [PathBuf] {
        static EMPTY_RESULT: [PathBuf; 0] = [];
        match self.cache.get(&length) {
            Some(value) => value.as_slice(),
            None => &EMPTY_RESULT
        }
    }
}
