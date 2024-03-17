use std::{
    collections::HashMap, fs::File, io::{Read, Seek, SeekFrom}, path::PathBuf, sync::{Arc, Mutex}, thread::{self, JoinHandle}, time::Instant
};

use crypto::{digest::Digest, sha1::Sha1};

use crate::{
    finder::LengthFileFinder,
    matcher::{MultiFilePieceMatcher, PieceMatchResult},
    torrent::{Piece, Pieces, Torrent},
};

pub struct OrchestratorOptions {
    pub torrents: Vec<Torrent>,
    pub scan_directories: Vec<PathBuf>,
    pub export_directory: PathBuf,
    pub threads: usize,
}

pub struct OrchestratorPiece {
    pub piece: Piece,
    pub result: Option<PieceMatchResult>,
    pub torrent_hash: Vec<u8>,
}

pub struct Orchestrator;
impl Orchestrator {
    pub fn start(options: &OrchestratorOptions) -> Result<(), std::io::Error> {
        let now = Instant::now();

        // Setup the finders
        let finder = Orchestrator::setup_finder(options, &options.torrents);
        println!(
            "File finder finished caching and finished at {} seconds.",
            now.elapsed().as_secs()
        );

        let piece_count = options.torrents
            .iter()
            .map(|torrent| torrent.info.pieces.len())
            .sum();

        let writer = Arc::new(
            PieceWriter::new(
                piece_count
            )
        );

        // Start!
        SingleFileOrchestrator::start(options, &options.torrents, &finder, &writer)?;
        println!(
            "Single File Orchestrator finished at {} seconds.",
            now.elapsed().as_secs()
        );

        MultiFileOrchestrator::start(options, &options.torrents, &finder, &writer)?;
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

    fn setup_finder(options: &OrchestratorOptions, torrents: &[Torrent]) -> LengthFileFinder {
        let mut file_lengths: Vec<u64> = Vec::new();

        for torrent in torrents {
            let pieces = Pieces::from_torrent(torrent);

            for piece in &pieces {
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

        finder
    }
}

// Piece Writer
struct PieceState {
    written_pieces: usize,
    total_piece_count: usize
}

struct PieceWriter {
    state: Mutex<PieceState>
}

impl PieceWriter {
    pub fn new(total_piece_count: usize) -> PieceWriter {
        PieceWriter {
            state: Mutex::new(PieceState {
                written_pieces: 0,
                total_piece_count
            })
        }
    }

    pub fn write(&self, orchestrator_piece: OrchestratorPiece) -> Result<(), std::io::Error> {
        let piece = &orchestrator_piece.piece;

        /*
        let export_directory: PathBuf = [
            self.export_directory.clone(),
            Path::new(&orchestrator_piece.torrent_hash).to_path_buf(),
            Path::new("Data").to_path_buf(),
        ]
        .iter()
        .collect();
        */

        if let Some(_) = &orchestrator_piece.result {
            let mut state = self.state.lock().unwrap();
            let mut start_position = 0;

            for piece_file in &piece.files {
                
                /*
                let output_path: PathBuf =
                    Path::new(&export_directory).join(Path::new(&piece_file.file_path));

                // Is this correct?
                let handle = match state.fd_cache.get_mut(&output_path) {
                    Some(handle) => handle,
                    None => {
                        let handle = OpenOptions::new().write(true).open(&output_path)?;
                        state.fd_cache.push(output_path.clone(), handle);
                        state.fd_cache.get_mut(&output_path).unwrap()
                    }
                };

                handle
                    .seek(SeekFrom::Start(piece_file.read_start_position))
                    .expect("Unable to seek into file!");


                handle
                    .write_all(&result.bytes[start_position..end_position])
                    .expect("Couldn't write to file!");
                 */
                let end_position = start_position + piece_file.read_length;


                start_position = end_position;
            }

            state.written_pieces += 1;
            println!("{} of {} total pieces written - {:.02}%", state.written_pieces, state.total_piece_count, (state.written_pieces as f64 / state.total_piece_count as f64) * 100 as f64);
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
        writer: &Arc<PieceWriter>
    ) -> Result<(), std::io::Error> {
        use std::cmp::min;

        let pieces = SingleFileOrchestrator::make_piece_list(torrents);
        let piece_map = SingleFileOrchestrator::make_piece_map(pieces);
        let thread_count = min(piece_map.len(), options.threads);

        let work = SingleFileOrchestrator::partition_work_by_thread(thread_count, piece_map);
        let mut handles: Vec<JoinHandle<Result<(), std::io::Error>>> = Vec::new();

        for (_, map) in work {
            let finder = finder.clone();
            let writer = writer.clone();

            let handle = thread::spawn(move || {

                for (file_length, pieces) in map {
                    SingleFileOrchestrator::process(file_length, pieces, &finder, &writer)?;
                }

                Ok(())
            });

            handles.push(handle);
        }

        for handle in handles {
            let _ = handle.join().expect("We have dropped data, aborting!");
        }

        Ok(())
    }

    fn process(
        file_length: u64,
        mut pieces: Vec<OrchestratorPiece>,
        finder: &LengthFileFinder,
        writer: &PieceWriter
    ) -> Result<(), std::io::Error> {

        for path in finder.find_length(file_length) {
            // Get length so we can track where are as we mutate list
            let pieces_length = pieces.len();

            // Now process file for each torrent
            let mut index = 0;
            while index < pieces_length {
                let mut work = pieces.remove(0);
                let file = work.piece.files.first().unwrap();

                let read_start_position = file.read_start_position;
                let bytes = SingleFileOrchestrator::read_bytes(path, file.read_length, read_start_position)?;
                let mut hasher = Sha1::new();
                hasher.input(&bytes);

                let mut hash = [0u8; 20];
                hasher.result(&mut hash);

                if work.piece.hash.as_slice().cmp(&hash).is_eq() {
                    let bytes = bytes.to_vec();
                    let paths = vec![path.clone()];

                    work.result = Some(PieceMatchResult {
                        bytes,
                        paths,
                    });

                    writer.write(work)?;
                    // Write file!
                } else {
                    pieces.push(work);
                }

                index += 1;
            }

            if pieces.is_empty() {
                break;
            }
        }

        Ok(())
    }

    fn make_piece_list(torrents: &[Torrent]) -> Vec<OrchestratorPiece> {
        let mut output: Vec<OrchestratorPiece> = Vec::new();

        for torrent in torrents {
            let pieces = Pieces::from_torrent(torrent);

            for piece in pieces {
                if piece.files.len() == 1 {
                    let matchable = OrchestratorPiece {
                        piece: piece,
                        result: None,
                        torrent_hash: torrent.info_hash.clone(),
                    };

                    output.push(matchable);
                }
            }
        }

        output
    }

    fn make_piece_map(pieces: Vec<OrchestratorPiece>) -> HashMap<u64, Vec<OrchestratorPiece>> {
        let mut single_files: HashMap<u64, Vec<OrchestratorPiece>> = HashMap::new();

        for orchestrator_piece in pieces {
            let file = orchestrator_piece.piece.files.first().unwrap();
            let length = file.file_length;

            single_files.entry(length).or_default();

            let items = single_files.get_mut(&length).unwrap();
            items.push(orchestrator_piece);
        }

        single_files
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

        thread_piece_map
    }

    // TODO: Cleanup duplicate code
    fn read_bytes(
        path: &PathBuf,
        read_length: u64,
        read_start_position: u64,
    ) -> Result<Vec<u8>, std::io::Error> {
        let mut read_bytes = vec![0u8; read_length as usize];
        let mut handle = File::open(path)?;

        handle.seek(SeekFrom::Start(read_start_position as u64))?;
        handle.read_exact(&mut read_bytes)?;

        Ok(read_bytes)
    }
}

// MultiPieceMatcher Orchestrator
struct MultiFileOrchestrator;
impl MultiFileOrchestrator {
    pub fn start(
        options: &OrchestratorOptions,
        torrents: &[Torrent],
        finder: &LengthFileFinder,
        writer: &Arc<PieceWriter>,
    ) -> Result<(), std::io::Error> {
        use std::cmp::min;

        let mut pieces = MultiFileOrchestrator::make_piece_list(torrents);
        let thread_count = min(pieces.len(), options.threads); 

        MultiFileOrchestrator::sort_by_combinations(&mut pieces, finder);

        let work = MultiFileOrchestrator::partition_work_by_thread(thread_count, pieces);
        let mut handles: Vec<JoinHandle<Result<(), std::io::Error>>> = Vec::new();

        for (_, pieces) in work {
            let finder = finder.clone();
            let writer = writer.clone();
            
            let handle = thread::spawn(move || {
                for piece in pieces {
                    MultiFileOrchestrator::process(piece, &writer, &finder)?;
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

    fn process(mut piece: OrchestratorPiece, writer: &Arc<PieceWriter>, finder: &LengthFileFinder) -> Result<(), std::io::Error> {
        piece.result = MultiFilePieceMatcher::scan(finder, &piece.piece)?;
        writer.write(piece)
    }

    fn make_piece_list(torrents: &[Torrent]) -> Vec<OrchestratorPiece> {
        let mut output: Vec<OrchestratorPiece> = Vec::new();

        for torrent in torrents {
            let pieces = Pieces::from_torrent(torrent);

            for piece in pieces {
                if piece.files.len() > 1 {
                    let matchable = OrchestratorPiece {
                        piece: piece,
                        result: None,
                        torrent_hash: torrent.info_hash.clone(),
                    };

                    output.push(matchable);
                }
            }
        }

        output
    }

    fn sort_by_combinations(pieces: &mut [OrchestratorPiece], finder: &LengthFileFinder) {
        pieces.sort_by(|left, right| {
            let left_count = MultiFilePieceMatcher::count_choices(finder, &left.piece);
            let right_count = MultiFilePieceMatcher::count_choices(finder, &right.piece);

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

        thread_piece_map
    }    
}
