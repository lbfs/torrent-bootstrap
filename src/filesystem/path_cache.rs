use std::{collections::HashMap, fs::{Metadata, OpenOptions}, os::unix::fs::MetadataExt, path::{Path, PathBuf}};

use walkdir::WalkDir;

use crate::filesystem::path_interner::PathInterner;

pub struct PathCacheEntry {
    file_length: u64,
    device_node: u64,
    index_node: u64
}

impl PathCacheEntry {
    pub fn length(&self) -> u64 {
        self.file_length
    }

    pub fn device_node(&self) -> u64 {
        self.device_node
    }
    
    pub fn index_node(&self) -> u64 {
        self.index_node
    }
}

impl PartialOrd for PathCacheEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PathCacheEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.device_node.cmp(&other.device_node)
            .then(self.index_node.cmp(&other.index_node))
            .then(self.file_length.cmp(&other.file_length))
    }
}

impl PartialEq for PathCacheEntry {
    fn eq(&self, other: &Self) -> bool {
        self.file_length == other.file_length && self.device_node == other.device_node && self.index_node == other.index_node
    }
}

impl Eq for PathCacheEntry {}

pub struct PathCache {
    entries: HashMap<usize, PathCacheEntry>,
    visited_directories: Vec<PathBuf>
}
 
impl PathCache {
    pub fn new() -> PathCache {
        PathCache {
            entries: HashMap::new(),
            visited_directories: Vec::new()
        }
    }

    pub fn add_directory(&mut self, interner: &mut PathInterner, root: &Path) {
        if !root.is_absolute() {
            panic!("Only absolute paths are supported.");
        }

        if !root.is_dir() {
            panic!("Only directories are supported in add_directory.");
        }

        // Skip adding if the root path has been added already.
        for visited_directory in &self.visited_directories {
            if root.starts_with(visited_directory) {
                return;
            }
        }

        for result in WalkDir::new(root) {
            if let Err(e) = result {
                eprintln!("Encountered error while searching directory: {}", e);
                continue;
            }

            let result = result.unwrap();
            let path = result.path();

            // If we've read this file at some time in the past, we do not need to check it again.
            if interner.has_key(path) && self.entries.contains_key(&interner.get(path)) {
                continue;
            }

            if path.is_dir() {
                continue;
            }

            let metadata = Self::to_metadata(path);

            if let Err(e) = metadata {
                eprintln!("Encountered error while reading metadata: {}", e);
                continue;
            }

            let metadata = metadata.unwrap();
            let entry = PathCacheEntry {
                file_length: metadata.len(),
                index_node: metadata.ino(),
                device_node: metadata.dev()
            };

            let id = interner.get_or_put_clone(path);
            self.entries.insert(id, entry);
        }

        self.visited_directories.push(root.to_path_buf());
        
    }

    pub fn add_path(&mut self, interner: &mut PathInterner, path: &Path) {
        if !path.is_absolute() {
            panic!("Only absolute paths are supported.");
        }

        // If we've read this file at some time in the past, we do not need to check it again.
        if interner.has_key(path) && self.entries.contains_key(&interner.get(path)) {
            return;
        }

        let metadata = Self::to_metadata(path);

        if let Err(e) = metadata {
            eprintln!("Encountered error while reading metadata: {}", e);
            return;
        }

        let metadata = metadata.unwrap();
        let entry = PathCacheEntry {
            file_length: metadata.len(),
            index_node: metadata.ino(),
            device_node: metadata.dev()
        };

        let id = interner.get_or_put_clone(path);
        self.entries.insert(id, entry);
    }

    pub fn add_path_by_interner_id(&mut self, interner: &mut PathInterner, id: usize) {
        if self.entries.contains_key(&id) {
            return;
        }

        let path = interner.get_by_id(id);
        let metadata = Self::to_metadata(path);

        if let Err(e) = metadata {
            eprintln!("Encountered error while reading metadata: {}", e);
            return;
        }

        let metadata = metadata.unwrap();
        let entry = PathCacheEntry {
            file_length: metadata.len(),
            index_node: metadata.ino(),
            device_node: metadata.dev()
        };

        self.entries.insert(id, entry);
    }

    fn to_metadata(file_path: &Path) -> std::io::Result<Metadata> {
        let handle = OpenOptions::new()
            .write(false)
            .truncate(false)
            .create(false)
            .create_new(false)
            .read(true)
            .open(file_path)?;

        let metadata = handle.metadata()?;
        Ok(metadata)
    }

    pub fn freeze(self) -> FrozenPathCache {
        FrozenPathCache::from(self)
    }
}

pub struct FrozenPathCache {
    pub entries: HashMap<usize, PathCacheEntry>,
}

impl FrozenPathCache {
    pub fn from(cache: PathCache) -> FrozenPathCache {
        FrozenPathCache {
            entries: cache.entries
        }
    }

    pub fn get(&self, id: usize) -> &PathCacheEntry {
        self.entries.get(&id).unwrap()
    }
}