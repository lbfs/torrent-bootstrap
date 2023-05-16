use std::{
    collections::{HashMap, HashSet},
    fs::{self, File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    time::Instant, thread::{self, JoinHandle}, sync::{Arc, Mutex},
};

use crypto::{digest::Digest, sha1::Sha1, sha2::Sha256};
use lrumap::LruHashMap;
use rayon::prelude::{IntoParallelIterator, ParallelIterator};

use crate::{
    finder::LengthFileFinder,
    matcher::{MultiFilePieceMatcher, PieceMatchResult},
    torrent::{Piece, Torrent},
};

#[derive(Clone)]
pub struct OrchestratorOptions {
    pub torrents_paths: Vec<PathBuf>,
    pub scan_directories: Vec<PathBuf>,
    pub export_directory: PathBuf,
    pub threads: usize,
}

pub struct OrchestratorPiece {
    pub piece: Piece,
    pub result: Option<PieceMatchResult>,
    pub torrent_hash: String,
}

pub struct Orchestrator;
impl Orchestrator {
    pub fn start(options: &OrchestratorOptions) -> Result<(), std::io::Error> {
        let now = Instant::now();

        // Load all the torrents!
        let torrents = Orchestrator::load_torrents(options);
        println!(
            "All torrents loaded and finished at {} seconds.",
            now.elapsed().as_secs()
        );
        
        
        // Setup the finder
        let finder = Orchestrator::setup_finder(options, &torrents);
        println!(
            "Finder finished caching and finished at {} seconds.",
            now.elapsed().as_secs()
        );

        let finder = Orchestrator::deduplicate(finder)?;
        println!(
            "Finder de-duplication finished at {} seconds.",
            now.elapsed().as_secs()
        );
        
        // "Pre-allocate" all the files on the disk so we have some place to write to
        Orchestrator::setup_export(options, &torrents)?;
        println!(
            "Generating export finished at {} seconds.",
            now.elapsed().as_secs()
        );


        // Start!
        SingleFileOrchestrator::start(options, &torrents, &finder)?;
        println!(
            "Single File Orchestrator finished at {} seconds.",
            now.elapsed().as_secs()
        );

        MultiFileOrchestrator::start(options, &torrents, &finder)?;
        println!(
            "Multi File Orchestrator finished at {} seconds.",
            now.elapsed().as_secs()
        );


        println!(
            "Total time elapsed finished at {} seconds.",
            now.elapsed().as_secs()
        );

        Ok(())
    }

    fn load_torrents(options: &OrchestratorOptions) -> Vec<Torrent> {
        let mut existence: HashSet<String> = HashSet::new();
        let mut torrents = Vec::new();

        for path in &options.torrents_paths {
            match Torrent::from(path.as_path()) {
                Ok(torrent) => {
                    if existence.contains(&torrent.info_hash) {
                        continue;
                    }

                    existence.insert(torrent.info_hash.clone());
                    torrents.push(torrent);
                }
                Err(error) => {
                    println!(
                        "Failed to open torrent file at {} with error {}",
                        path.to_str().unwrap(),
                        error.to_string()
                    );
                }
            }
        }

        return torrents;
    }

    fn setup_export(
        options: &OrchestratorOptions,
        torrents: &[Torrent],
    ) -> Result<(), std::io::Error> {
        // Setup preallocated files
        for torrent in torrents {
            let export_directory: PathBuf = [
                options.export_directory.to_path_buf(),
                Path::new(&torrent.info_hash).to_path_buf(),
                Path::new("Data").to_path_buf(),
            ]
            .iter()
            .collect();

            Orchestrator::preallocate_all(&export_directory, torrent)?;
        }

        // Write out each torrent as a file to disk.
        for torrent in torrents {
            let filename = format!("{}.torrent", &torrent.info_hash);
            let export_path: PathBuf = [
                options.export_directory.to_path_buf(),
                Path::new(&torrent.info_hash).to_path_buf(),
                Path::new(&filename).to_path_buf(),
            ]
            .iter()
            .collect();

            fs::copy(&torrent.path, &export_path)?;
        }

        Ok(())
    }

    fn setup_finder(options: &OrchestratorOptions, torrents: &[Torrent]) -> LengthFileFinder {
        let mut file_lengths: Vec<u64> = Vec::new();
        for torrent in torrents {
            for piece in &torrent.pieces {
                for file in &piece.files {
                    if !file_lengths.contains(&file.file_length) {
                        file_lengths.push(file.file_length);
                    }
                }
            }
        }

        let mut finder = LengthFileFinder::new();
        for scan_directory in &options.scan_directories {
            finder.add(&file_lengths, scan_directory.as_path());
        }

        return finder;
    }

    fn preallocate(path: &Path, length: u64) -> Result<(), std::io::Error> {
        let options = OpenOptions::new().write(true).create(true).open(path)?;
        options.set_len(length)
    }

    fn preallocate_all(export_directory: &Path, torrent: &Torrent) -> Result<(), std::io::Error> {
        for file in &torrent.files {
            let torrent_file_relative = file.path.as_path();
            let torrent_complete_path: PathBuf =
                [export_directory, torrent_file_relative].iter().collect();

            // Safety: This should not fail. A torrent path should *always* be an relative path to a *file*
            // If we fail here, we do have an actual issue and we need to abort since this is not a recoverable
            // situtation, and we have absolutely no idea what directory we're creating.
            let torrent_complete_parent = torrent_complete_path.parent().unwrap();
            let length = file.length as u64;

            std::fs::create_dir_all(&torrent_complete_parent)?;
            Orchestrator::preallocate(&torrent_complete_path, length)?;
        }

        Ok(())
    }

    // This code is the most garbage code in the entire app, clean it up.
    fn deduplicate(finder: LengthFileFinder) -> Result<LengthFileFinder, std::io::Error> {
        let mut existence: HashSet<String> = HashSet::new();
        let mut cache: HashMap<u64, Vec<PathBuf>> = HashMap::new();
        let pathbuf_cache: HashMap<PathBuf, String> = HashMap::new();
        let pathbuf_cache = Mutex::new(pathbuf_cache);
        
        let mut kept = 0;
        let mut removed = 0;

        let mut all_paths = Vec::new();
        for (_, paths) in &finder.cache {
            if paths.len() <= 1 {
                continue;
            }

            for path in paths {
                all_paths.push(path.to_path_buf());
            }
        }
        
        all_paths.into_par_iter().for_each(|path| {
            let mut input = File::open(&path).unwrap();
            let mut buffer = Vec::new();
            input.read_to_end(&mut buffer).unwrap();
            let mut context = Sha256::new();
            context.input(&buffer);
            let hash = context.result_str();
            let mut pathbuf_cache = pathbuf_cache.lock().unwrap();
            pathbuf_cache.insert(path, hash);
        });

        let pathbuf_cache = pathbuf_cache.lock().unwrap();
        for (length, paths) in finder.cache {
            if paths.len() <= 1 {
                for path in paths {
                    if !cache.contains_key(&length) {
                        cache.insert(length, Vec::new());
                    }
        
                    let items = cache.get_mut(&length).unwrap();
                    items.push(path);
                }
            } else {
                for path in paths {
                    let hash = pathbuf_cache.get(&path).unwrap();
                    if existence.insert(hash.to_string()) {
                        kept += 1;
    
                        if !cache.contains_key(&length) {
                            cache.insert(length, Vec::new());
                        }
            
                        let items = cache.get_mut(&length).unwrap();
                        items.push(path);
                    } else {
                        removed += 1;
                    }
                }
            }
        }

        let mut finder = LengthFileFinder::new();
        finder.cache = cache;

        println!("File de-duplication removed {} and kept {} files in the cache.", removed, kept);
        Ok(finder)
    }
}

// Piece Writer
struct PieceWriter {
    fd_cache: LruHashMap<PathBuf, File>,
    options: OrchestratorOptions
}

impl PieceWriter {
    pub fn new(cache_size: usize, options: OrchestratorOptions) -> PieceWriter {
        use std::cmp::max;
        let capacity = max(cache_size, 2);

        return PieceWriter {
            fd_cache: LruHashMap::new(capacity),
            options: options,
        };
    }

    pub fn write(&mut self, orchestrator_piece: OrchestratorPiece) -> Result<(), std::io::Error> {
        let piece = &orchestrator_piece.piece;
        let export_directory: PathBuf = [
            self.options.export_directory.to_path_buf(),
            Path::new(&orchestrator_piece.torrent_hash).to_path_buf(),
            Path::new("Data").to_path_buf(),
        ]
        .iter()
        .collect();

        match &orchestrator_piece.result {
            Some(result) => {
                let mut start_position = 0;
                for piece_file in &piece.files {
                    let output_path: PathBuf =
                        Path::new(&export_directory).join(Path::new(&piece_file.file_path));

                    // Is this correct?
                    let handle = match self.fd_cache.get_mut(&output_path) {
                        Some(handle) => handle,
                        None => {
                            let handle = OpenOptions::new().write(true).open(&output_path)?;
                            self.fd_cache.push(output_path.clone(), handle);
                            self.fd_cache.get_mut(&output_path).unwrap()
                        }
                    };

                    handle
                        .seek(SeekFrom::Start(piece_file.read_start_position))
                        .expect("Unable to seek into file!");

                    let end_position = start_position + piece_file.read_length as usize;

                    handle
                        .write(&result.bytes[start_position..end_position])
                        .expect("Couldn't write to file!");

                    start_position = end_position;
                }
            }
            _ => {}
        };

        Ok(())
    }
}

// WIP
struct SingleFileOrchestrator;
impl SingleFileOrchestrator {
    pub fn start(
        options: &OrchestratorOptions,
        torrents: &[Torrent],
        finder: &LengthFileFinder,
    ) -> Result<(), std::io::Error> {
        use std::cmp::min;

        let pieces = SingleFileOrchestrator::make_piece_list(torrents);
        let piece_map = SingleFileOrchestrator::make_piece_map(pieces);
        let thread_count = min(piece_map.len(), options.threads);

        let work = SingleFileOrchestrator::partition_work_by_thread(thread_count, piece_map);
        let mut handles: Vec<JoinHandle<Result<(), std::io::Error>>> = Vec::new();

        for (_, map) in work {
            let finder = finder.clone();
            let options = options.clone();

            let handle = thread::spawn(move || {
                let map = map;
                let finder = finder;

                for (file_length, pieces) in map {
                    SingleFileOrchestrator::process(file_length, pieces, &finder, &options)?;
                }

                Ok(())
            });

            handles.push(handle);
        }

        for handle in handles {
            // TODO: Something with errors
            let _ = handle.join();
        }

        Ok(())
    }

    fn process(
        file_length: u64,
        mut pieces: Vec<OrchestratorPiece>,
        finder: &LengthFileFinder,
        options: &OrchestratorOptions,
    ) -> Result<(), std::io::Error> {
        let cache_size = SingleFileOrchestrator::count_torrents_in_pieces(&pieces);
        let mut writer = PieceWriter::new(cache_size, options.clone());

        for path in finder.find_length(file_length) {
            // Read file
            let mut input = File::open(path)?;
            let mut buffer = Vec::new();
            input.read_to_end(&mut buffer).unwrap();

            // Get length so we can track where are as we mutate list
            let pieces_length = pieces.len();

            // Now process file for each torrent
            let mut index = 0;
            while index < pieces_length {
                let mut piece = pieces.remove(0);
                let file = piece.piece.files.get(0).unwrap();

                let read_start_position = file.read_start_position as usize;
                let read_final_position = read_start_position + file.read_length as usize;

                let bytes = &buffer[read_start_position..read_final_position];
                let mut hasher = Sha1::new();
                hasher.input(bytes);

                if hasher.result_str() == piece.piece.piece_hash {
                    let bytes = bytes.to_vec();
                    let mut paths = Vec::new();
                    paths.push(path.clone());

                    piece.result = Some(PieceMatchResult {
                        bytes: bytes,
                        paths: paths,
                    });

                    writer.write(piece)?;
                    // Write file!
                } else {
                    pieces.push(piece);
                }

                index += 1;
            }

            // If we've processed everything, there is no reason to load more files.
            if pieces.len() == 0 {
                break;
            }
        }

        Ok(())
    }

    fn make_piece_list(torrents: &[Torrent]) -> Vec<OrchestratorPiece> {
        let mut pieces: Vec<OrchestratorPiece> = Vec::new();

        for torrent in torrents {
            for piece in &torrent.pieces {
                if piece.files.len() == 1 {
                    let matchable = OrchestratorPiece {
                        piece: piece.clone(),
                        result: None,
                        torrent_hash: torrent.info_hash.clone(),
                    };

                    pieces.push(matchable);
                }
            }
        }

        return pieces;
    }

    fn make_piece_map(pieces: Vec<OrchestratorPiece>) -> HashMap<u64, Vec<OrchestratorPiece>> {
        let mut single_files: HashMap<u64, Vec<OrchestratorPiece>> = HashMap::new();

        for orchestrator_piece in pieces {
            let file = orchestrator_piece.piece.files.get(0).unwrap();
            let length = file.file_length;

            if !single_files.contains_key(&length) {
                single_files.insert(length, Vec::new());
            }

            let items = single_files.get_mut(&length).unwrap();
            items.push(orchestrator_piece);
        }

        return single_files;
    }

    fn partition_work_by_thread(
        thread_count: usize,
        piece_map: HashMap<u64, Vec<OrchestratorPiece>>,
    ) -> HashMap<usize, HashMap<u64, Vec<OrchestratorPiece>>> {
        let mut thread_piece_map: HashMap<usize, HashMap<u64, Vec<OrchestratorPiece>>> =
            HashMap::new();

        for index in 0..thread_count {
            thread_piece_map.insert(index, HashMap::new());
        }

        for (index, entry) in piece_map.into_iter().enumerate() {
            let (file_length, pieces) = entry;
            let thread_position = index % thread_count;

            let map = thread_piece_map.get_mut(&thread_position).unwrap();
            map.insert(file_length, pieces);
        }

        return thread_piece_map;
    }

    fn count_torrents_in_pieces(pieces: &[OrchestratorPiece]) -> usize {
        let mut seen: HashSet<String> = HashSet::new();

        for piece in pieces {
            if !seen.contains(&piece.torrent_hash) {
                seen.insert(piece.torrent_hash.clone());
            }
        }

        return seen.len();
    }
}

// MultiPieceMatcher Orchestrator
struct MultiFileOrchestrator;
impl MultiFileOrchestrator {
    pub fn start(
        options: &OrchestratorOptions,
        torrents: &[Torrent],
        finder: &LengthFileFinder,
    ) -> Result<(), std::io::Error> {
        use std::cmp::min;

        let mut pieces = MultiFileOrchestrator::make_piece_list(torrents);
        let thread_count = min(pieces.len(), options.threads); 

        MultiFileOrchestrator::sort_by_combinations(&mut pieces, finder);

        let work = MultiFileOrchestrator::partition_work_by_thread(thread_count, pieces);
        let writer = Arc::new(Mutex::new(PieceWriter::new(1, options.clone())));
        let mut handles: Vec<JoinHandle<Result<(), std::io::Error>>> = Vec::new();

        for (_, pieces) in work {
            let finder = finder.clone();
            let writer = writer.clone();
            
            let handle = thread::spawn(move || {
                let pieces = pieces;
                let mut writer = writer;

                for piece in pieces {
                    MultiFileOrchestrator::process(piece, &mut writer, &finder)?;
                }

                Ok(())
            });

            handles.push(handle);
        }

        for handle in handles {
            let _ = handle.join();
        }

        Ok(())
    }

    fn process(mut piece: OrchestratorPiece, writer: &mut Arc<Mutex<PieceWriter>>, finder: &LengthFileFinder) -> Result<(), std::io::Error> {
        piece.result = MultiFilePieceMatcher::scan(finder, &piece.piece)?;

        let mut writer = writer.lock().unwrap();
        writer.write(piece)
    }

    fn make_piece_list(torrents: &[Torrent]) -> Vec<OrchestratorPiece> {
        let mut pieces: Vec<OrchestratorPiece> = Vec::new();

        for torrent in torrents {
            for piece in &torrent.pieces {
                if piece.files.len() > 1 {
                    let matchable = OrchestratorPiece {
                        piece: piece.clone(),
                        result: None,
                        torrent_hash: torrent.info_hash.clone(),
                    };

                    pieces.push(matchable);
                }
            }
        }

        return pieces;
    }

    fn sort_by_combinations(pieces: &mut [OrchestratorPiece], finder: &LengthFileFinder) {
        let count_fn = |piece: &OrchestratorPiece| {
            let res = MultiFilePieceMatcher::count_choices(finder, &piece.piece);
            let value = if let Some(res) = res { res } else { usize::MAX };
            return value;
        };

        pieces.sort_by(|left, right| {
            let left_count = count_fn(left);
            let right_count = count_fn(right);

            left_count.partial_cmp(&right_count).unwrap()
        });
    }

    fn partition_work_by_thread(
        thread_count: usize,
        pieces: Vec<OrchestratorPiece>,
    ) -> HashMap<usize, Vec<OrchestratorPiece>> {
        let mut thread_piece_map: HashMap<usize, Vec<OrchestratorPiece>> = HashMap::new();

        for index in 0..thread_count {
            thread_piece_map.insert(index, Vec::new());
        }

        for (index, entry) in pieces.into_iter().enumerate() {
            let thread_position = index % thread_count;

            let items = thread_piece_map.get_mut(&thread_position).unwrap();
            items.push(entry);
        }

        return thread_piece_map;
    }    
}
