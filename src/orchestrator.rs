use std::{
    cmp::min, collections::HashMap, path::PathBuf, sync::Arc, time::Instant
};

use sha1::{Digest, Sha1};

use crate::{
    finder::{read_bytes, LengthFileFinder}, matcher::{MultiFilePieceMatcher, PieceMatchResult}, solver::Solver, torrent::{Piece, Pieces, Torrent}, writer::PieceWriter
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

        // Setup the finder
        let finder = Arc::new(Orchestrator::setup_finder(&options.torrents, &options));

        println!(
            "File finder finished caching and finished at {} seconds.",
            now.elapsed().as_secs()
        );

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

        // Start!
        let single_piece_map = Orchestrator::make_single_piece_map(singles);
        let single_piece_map_as_list: Vec<_> = single_piece_map.into_iter().collect();

        let single_solver = SinglePieceSolver { writer: writer.clone(), finder: finder.clone() };
        single_solver.start(single_piece_map_as_list, options.threads)?;

        println!(
            "Single File Orchestrator finished at {} seconds.",
            now.elapsed().as_secs()
        );

        let multiple_solver = MultiplePieceSolver { writer: writer.clone(), finder: finder.clone() };
        multiple_solver.start(multiple, options.threads)?;

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

    // Only use this with single pieces!
    fn make_single_piece_map(pieces: Vec<OrchestratorPiece>) -> HashMap<u64, Vec<OrchestratorPiece>> {
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
}


#[derive(Clone)]
struct SinglePieceSolver {
    writer: Arc<PieceWriter>,
    finder: Arc<LengthFileFinder>
}

impl Solver<(u64, Vec<OrchestratorPiece>), std::io::Error> for SinglePieceSolver {
    fn solve(&self, work: (u64, Vec<OrchestratorPiece>)) -> Result<(), std::io::Error> {
        let (file_length, mut pieces) = work;

        for path in self.finder.find_length(file_length) {
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

                    self.writer.write(work)?;
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
            self.writer.write(work)?;
        }

        Ok(())
    }
}

#[derive(Clone)]
struct MultiplePieceSolver {
    writer: Arc<PieceWriter>,
    finder: Arc<LengthFileFinder>
}

impl Solver<OrchestratorPiece, std::io::Error> for MultiplePieceSolver {
    fn solve(&self, mut work: OrchestratorPiece) -> Result<(), std::io::Error> {
        work.result = MultiFilePieceMatcher::scan(&self.finder, &work.piece)?;
        self.writer.write(work)
    }

    // Custom balance method to enforce that cheaper pieces to evaluate are always evaulated first, regardless
    // of the thread, making it easier to terminate the program if it gets stuck on high cardinality pieces without
    // losing much data.
    fn balance(source: &mut Vec<OrchestratorPiece>, others: &mut Vec<&mut Vec<OrchestratorPiece>>) {
        let mut collected: Vec<OrchestratorPiece> = source.drain(..).collect();

        let total_work = collected.len();
        let active_threads = others.len() + 1;

        // Sort
        collected.sort_by(|left, right| {
            let left_count = left.piece.files.len();
            let right_count = right.piece.files.len();

            left_count.cmp(&right_count)
        });

        // Balance
        'outer: loop {
            if collected.len() == 0 {
                break 'outer;
            }

            source.push(collected.pop().unwrap());

            for other in others.iter_mut() {       
                if collected.len() == 0 {
                    break 'outer;
                }

                other.push(collected.pop().unwrap());
            }
        }

        // Debugging
        let mut counted_work = source.len();
        let mut min_work_per_worker = source.len();
        for other in others.iter_mut() {
            counted_work += other.len();
            min_work_per_worker = min(min_work_per_worker, other.len())
        }

        println!("Rebalanced {} items across {} workers with at-minimum {} per worker; lost {}", total_work, active_threads, min_work_per_worker, total_work - counted_work);
    }
}