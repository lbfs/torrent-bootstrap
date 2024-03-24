use std::{collections::HashMap, path::PathBuf, sync::Arc, time::Instant};

use crate::{finder::LengthFileFinder, solver::{MultiplePieceSolver, PieceMatchResult, SinglePieceSolver, Solver}, torrent::{Piece, Pieces, Torrent}, writer::PieceWriter};

pub struct OrchestratorOptions {
    pub torrents: Vec<Torrent>,
    pub scan_directories: Vec<PathBuf>,
    pub export_directory: PathBuf,
    pub threads: usize,
}

pub struct OrchestratorPiece {
    pub piece: Piece,
    pub result: Option<PieceMatchResult>,
    pub info_hash: Arc<Vec<u8>>
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

        let writer = PieceWriter::new(piece_count, &options.export_directory, &options.torrents);
        let writer = Arc::new(writer);

        // Partition Pieces
        let (singles, multiple) = Orchestrator::make_piece_list(&options.torrents);

        // Start!
        let single_piece_map = Orchestrator::make_single_piece_map(singles);
        let single_piece_map_as_list: Vec<_> = single_piece_map.into_iter().collect();

        let single_solver = SinglePieceSolver::new(writer.clone(), finder.clone());
        single_solver.start(single_piece_map_as_list, options.threads)?;

        println!(
            "Single File Orchestrator finished at {} seconds.",
            now.elapsed().as_secs()
        );

        let multiple_solver = MultiplePieceSolver::new(writer.clone(), finder.clone());
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
            let info_hash = Arc::new(torrent.info_hash.clone());

            for piece in pieces {
                let matchable = OrchestratorPiece {
                    piece: piece,
                    result: None,
                    info_hash: info_hash.clone()
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