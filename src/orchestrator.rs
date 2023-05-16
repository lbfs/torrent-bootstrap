use std::{
    collections::{HashMap, HashSet},
    fs::{self, File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    time::Instant,
};

use crypto::{digest::Digest, sha1::Sha1};
use lrumap::LruHashMap;

use crate::{
    finder::LengthFileFinder,
    matcher::{MultiFilePieceMatcher, PieceMatchResult},
    torrent::{Piece, Torrent},
};

pub struct OrchestratorOptions<'a> {
    pub torrents_paths: &'a [PathBuf],
    pub scan_directories: &'a [PathBuf],
    pub export_directory: &'a Path,
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

        for path in options.torrents_paths {
            match Torrent::from(path) {
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
                options.export_directory,
                Path::new(&torrent.info_hash),
                Path::new("Data"),
            ]
            .iter()
            .collect();

            Orchestrator::preallocate_all(&export_directory, torrent)?;
        }

        // Write out each torrent as a file to disk.
        for torrent in torrents {
            let filename = format!("{}.torrent", &torrent.info_hash);
            let export_path: PathBuf = [
                options.export_directory,
                Path::new(&torrent.info_hash),
                Path::new(&filename),
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
        for scan_directory in options.scan_directories {
            finder.add(&file_lengths, scan_directory);
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
}

// Piece Writer
struct PieceWriter<'a> {
    fd_cache: LruHashMap<PathBuf, File>,
    options: &'a OrchestratorOptions<'a>,
}

impl<'a> PieceWriter<'a> {
    pub fn new(cache_size: usize, options: &'a OrchestratorOptions) -> PieceWriter<'a> {
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
            self.options.export_directory,
            Path::new(&orchestrator_piece.torrent_hash),
            Path::new("Data"),
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
        let pieces = SingleFileOrchestrator::make_piece_list(torrents);
        let piece_map = SingleFileOrchestrator::make_piece_map(pieces);
        let work = SingleFileOrchestrator::partition_work_by_thread(options.threads, piece_map);

        for (_, map) in work {
            for (file_length, pieces) in map {
                SingleFileOrchestrator::process(file_length, pieces, finder, options)?;
            }
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
        let mut writer = PieceWriter::new(cache_size, options);

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
        let mut pieces = MultiFileOrchestrator::make_piece_list(torrents);
        MultiFileOrchestrator::sort_by_combinations(&mut pieces, finder);

        let work = MultiFileOrchestrator::partition_work_by_thread(options.threads, pieces);
        let mut writer = PieceWriter::new(16, options);

        for (_, pieces) in work {
            for piece in pieces {
                MultiFileOrchestrator::process(piece, &mut writer, finder)?;
            }
        }

        Ok(())
    }

    fn process(mut piece: OrchestratorPiece, writer: &mut PieceWriter, finder: &LengthFileFinder) -> Result<(), std::io::Error> {
        piece.result = MultiFilePieceMatcher::scan(finder, &piece.piece)?;
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
