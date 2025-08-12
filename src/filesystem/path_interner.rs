use std::{collections::HashMap, path::{Path, PathBuf}};

// TODO: HANDLE COLLISIONS

pub struct PathInterner {
    map: HashMap<PathBuf, usize>,
    vec: Vec<PathBuf>
}

impl PathInterner {
    pub fn new() -> PathInterner {
        PathInterner { 
            map: HashMap::new(),
            vec: Vec::new()
        }
    }

    pub fn put(&mut self, path: PathBuf) -> usize {
        if self.map.contains_key(&path) {
            return *self.map.get(&path).unwrap();
        }

        let next_id = self.vec.len();
        self.map.insert(path.clone(), next_id);
        self.vec.push(path);
        next_id
    }

    pub fn get_or_put_clone(&mut self, path: &Path) -> usize {
        if !self.has_key(path) {
            return self.put(path.to_path_buf())
        }
        self.get(path)
    }

    pub fn has_key(&self, path: &Path) -> bool {
        self.map.contains_key(path)
    }
    
    pub fn get(&self, path: &Path) -> usize {
        *&self.map[path]
    }

    pub fn get_by_id(&self, id: usize) -> &Path {
        &self.vec[id]
    }

    pub fn freeze(self) -> FrozenPathInterner {
        FrozenPathInterner::from(self)
    }
}

pub struct FrozenPathInterner {
    vec: Vec<PathBuf>
}

impl FrozenPathInterner {
    fn from(interner: PathInterner) -> FrozenPathInterner {
        FrozenPathInterner { vec: interner.vec }
    }

    pub fn get(&self, id: usize) -> &Path {
        &self.vec[id]
    }
}