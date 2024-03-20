use std::{
    collections::HashMap, path::PathBuf, sync::{Arc, Mutex}, thread::{self, JoinHandle}, time::Instant
};

use sha1::{Digest, Sha1};

use crate::{
    finder::{read_bytes, LengthFileFinder},
    matcher::{MultiFilePieceMatcher, PieceMatchResult},
    torrent::{Piece, Pieces, Torrent}, writer::PieceWriter,
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
    pub torrent_hash: Arc<Vec<u8>>,
    pub torrent_name: Arc<String>,
}

pub struct Orchestrator;
impl Orchestrator {
    pub fn start(options: &OrchestratorOptions) -> Result<(), std::io::Error> {
        let now = Instant::now();

        // Setup Writer
        let piece_count = options.torrents
            .iter()
            .map(|torrent| torrent.info.pieces.len())
            .sum();

        let writer = Arc::new(
            PieceWriter::new(
                options.export_directory.clone(),
                piece_count
            )
        );

        // Partition Pieces
        let (singles, multiple) = Orchestrator::make_piece_list(&options.torrents);

        // Setup the finder
        let finder = Arc::new(Orchestrator::setup_finder(&options.torrents, &options));

        println!(
            "File finder finished caching and finished at {} seconds.",
            now.elapsed().as_secs()
        );

        // Start!
        SingleFileOrchestrator::start(options, singles, finder.clone(), writer.clone())?;
        println!(
            "Single File Orchestrator finished at {} seconds.",
            now.elapsed().as_secs()
        );

        MultiFileOrchestrator::start(options, multiple, finder.clone(), writer.clone())?;
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

    fn setup_finder(torrents: &[Torrent], options: &OrchestratorOptions) -> LengthFileFinder {
        let mut file_lengths: Vec<u64> = Vec::new();

        for torrent in torrents {
            if torrent.info.length.is_some() {
                if !file_lengths.contains(&torrent.info.length.unwrap()) {
                    file_lengths.push(torrent.info.length.unwrap());
                }
            } else if torrent.info.files.is_some() {
                for file in torrent.info.files.as_ref().unwrap() {
                    if !file_lengths.contains(&file.length) {
                        file_lengths.push(file.length);
                    }
                }
            } else {
                panic!("Neither single-file or multiple-files option is available, unable to count file lengths.");
            }
        }

        let mut finder = LengthFileFinder::new();
        for scan_directory in &options.scan_directories {
            finder.add(&file_lengths, scan_directory.as_path());
        }

        finder
    }

    fn make_piece_list(torrents: &[Torrent]) -> (Vec<OrchestratorPiece>, Vec<OrchestratorPiece>) {
        let mut singles = Vec::new();
        let mut multiple = Vec::new();

        for torrent in torrents {
            let pieces = Pieces::from_torrent(torrent);

            let torrent_hash = Arc::new(torrent.info_hash.clone());
            let torrent_name =  Arc::new(torrent.info.name.clone());

            for piece in pieces {
                let matchable = OrchestratorPiece {
                    piece: piece,
                    result: None,
                    torrent_hash: torrent_hash.clone(),
                    torrent_name: torrent_name.clone()
                };

                if matchable.piece.files.len() == 1 {
                    singles.push(matchable);
                } else {
                    multiple.push(matchable);
                }
            }
        }

        (singles, multiple)
    }
}

struct SingleFileOrchestrator;
impl SingleFileOrchestrator {
    pub fn start(
        options: &OrchestratorOptions,
        pieces: Vec<OrchestratorPiece>,
        finder: Arc<LengthFileFinder>,
        writer: Arc<PieceWriter>
    ) -> Result<(), std::io::Error> {
        use std::cmp::{min, max};

        let piece_map = SingleFileOrchestrator::make_piece_map(pieces);

        let thread_count = max(min(piece_map.len(), options.threads), 1); 
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
            let _ = handle.join().expect("We have dropped data, aborting!")?;
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
            let pieces_length = pieces.len();

            let mut index = 0;
            while index < pieces_length {
                let mut work = pieces.remove(0);
                let file = work.piece.files.first().unwrap();

                let read_start_position = file.read_start_position;
                let bytes = read_bytes(path, file.read_length, read_start_position)?;
                let hash = Sha1::digest(&bytes);

                if work.piece.hash.as_slice().cmp(&hash).is_eq() {
                    let bytes = bytes.to_vec();
                    let paths = vec![path.clone()];

                    work.result = Some(PieceMatchResult {
                        bytes,
                        paths,
                    });

                    writer.write(work)?;
                } else {
                    pieces.push(work);
                }

                index += 1;
            }

            if pieces.is_empty() {
                break;
            }
        }

        // Emit the failed blocks for accounting purposes
        for work in pieces {
            writer.write(work)?;
        }

        Ok(())
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
}

struct MultiFileOrchestrator;
impl MultiFileOrchestrator {
    pub fn start(
        options: &OrchestratorOptions,
        mut pieces: Vec<OrchestratorPiece>,
        finder: Arc<LengthFileFinder>,
        writer: Arc<PieceWriter>,
    ) -> Result<(), std::io::Error> {
        use std::cmp::{min, max};

        let thread_count = max(min(pieces.len(), options.threads), 1); 

        MultiFileOrchestrator::sort_by_file_count(&mut pieces);

        // Create Work Queues
        let work_queues = {
            let min_work_per_worker = pieces.len() / thread_count;
            let mut remainder = pieces.len() % thread_count;

            let mut work_queues: Vec<Vec<OrchestratorPiece>> = Vec::new();
            for _ in 1..thread_count {
                let has_remaining = (remainder > 0) as usize;
                let work_per_worker = min_work_per_worker + has_remaining;
                remainder -= has_remaining;

                work_queues.push(pieces.drain(..work_per_worker).into_iter().collect());
            }
            work_queues.push(pieces);

            let work_queues: Vec<_> = work_queues.into_iter()
                .map(|entry| Some(entry))
                .map(|entry| Arc::new(Mutex::new(entry)))
                .collect();

            work_queues
        };

        let steal_lock = Arc::new(Mutex::new(()));

        // Start up the threads
        let mut handles: Vec<JoinHandle<Result<(), std::io::Error>>> = Vec::new();

        for thread_id in 0..work_queues.len() {
            let finder = finder.clone();
            let writer = writer.clone();
            let work_queues = work_queues.clone();
            let steal_lock = steal_lock.clone();

            let handle = thread::spawn(move || {
                'outer: loop {
                    let found = {
                        let guard = work_queues.get(thread_id)
                            .unwrap()
                            .try_lock();

                        if let Ok(mut guard) = guard {
                            guard.as_mut().unwrap().pop()
                        } else {
                            None
                        }
                    };

                    match found {
                        Some(mut work) => {
                            work.result = MultiFilePieceMatcher::scan(&finder, &work.piece)?;
                            writer.write(work)?;
                        },
                        None => {
                            let steal_lock_guard = steal_lock.lock().unwrap();

                            // We may have multiple waiters here if multiple queues are looking for work, quickly recheck to see if we can quick abort
                            // as we may have just balanced all the threads, no point in doing it again.
                            let mut guard = work_queues.get(thread_id)
                                .unwrap()
                                .lock()
                                .unwrap();

                            let should_abort = match guard.as_ref() {
                                Some(value) => { 
                                    value.len() > 0
                                },
                                None => true
                            };

                            if should_abort {
                                println!("Quickly aborting rebalance on thread {}", thread_id);
                                continue 'outer;
                            }

                            // Lock all the other threads
                            // Only store the active threads.
                            let mut other_guards = Vec::new();
                            for guard_thread_id in (0..thread_id).chain((thread_id + 1)..thread_count) {
                                let guard = work_queues.get(guard_thread_id)
                                    .unwrap()
                                    .lock()
                                    .unwrap();


                                let inner = &*guard;
                                match inner.as_ref() {
                                    Some(_) => other_guards.push(guard),
                                    None => {}
                                }
                            }

                            if other_guards.len() > 0 {
                                // Sort other threads by those that do not have work first.
                                // Give them work first, so that they don't waste time doing their own re-balance.
                                // We know the other threads are most-likely working an item already.
                                other_guards.sort_by(|a, b| {
                                    a.as_ref().unwrap().len().cmp(&b.as_ref().unwrap().len())
                                });

                                // Take the work from the threads
                                let work_to_balance = guard.as_mut().unwrap();
                                for other_guard in other_guards.iter_mut() {
                                    let data = other_guard.as_mut().unwrap();
                                    work_to_balance.extend(data.drain(..));
                                }

                                // Sort and rebalance
                                MultiFileOrchestrator::sort_by_file_count( work_to_balance);

                                let total_work = work_to_balance.len();
                                let active_threads = other_guards.len() + 1;

                                let work_for_other_threads = total_work - ((total_work / active_threads) + ((total_work % active_threads != 0) as usize));
                                
                                let min_work_per_worker = work_for_other_threads / other_guards.len();
                                let mut remainder = work_for_other_threads % other_guards.len();

                                let mut counted_work = 0;

                                for guard in other_guards.iter_mut() {
                                    if work_to_balance.len() == 0 {
                                        break;
                                    }

                                    let has_remaining = (remainder > 0) as usize;
                                    let work_per_worker = min_work_per_worker + has_remaining;
                                    remainder -= has_remaining;

                                    let data = guard.as_mut().unwrap();
                                    data.extend(work_to_balance.drain(..work_per_worker));
                                    counted_work += data.len();
                                }

                                counted_work += work_to_balance.len();

                                println!("Rebalanced {} items across {} workers with at-minimum {} per worker; lost {}", total_work, active_threads, min_work_per_worker, total_work - counted_work);
                            }

                            // Mark thread as dead if there is no more work and exit
                            match guard.as_ref() {
                                Some(value) => { 
                                    if value.len() == 0 { 
                                        guard.take();
                                        break 'outer;
                                    }
                                },
                                None => panic!("Thread {} is already shutdown, yet tried to re-balance. This is impossible.", thread_id)
                            };

                            drop(steal_lock_guard);
                        }
                    }
                }

                println!("Thread {} has exited", thread_id);
                Ok(())
            });

            handles.push(handle);
        }

        for handle in handles {
            let _ = handle.join().expect("We have dropped data, aborting!")?;
        }

        Ok(())
    }

    fn sort_by_file_count(pieces: &mut [OrchestratorPiece]) {
        pieces.sort_by(|left, right| {
            let left_count = left.piece.files.len();
            let right_count = right.piece.files.len();

            left_count.partial_cmp(&right_count).unwrap()
        });
    } 
}
