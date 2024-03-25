use std::{collections::HashMap, fmt::Write, fs::File, path::{Path, PathBuf}, sync::Arc, time::Instant};

use crate::{finder::LengthFileFinder, solver::{MultiplePieceSolver, PieceMatchResult, SinglePieceSolver, Solver}, torrent::{Piece, PieceFile, Pieces, Torrent}, writer::PieceWriter};

pub struct OrchestratorOptions {
    pub torrents: Vec<Torrent>,
    pub scan_directories: Vec<PathBuf>,
    pub export_directory: PathBuf,
    pub threads: usize,
}

pub struct OrchestratorPiece {
    pub piece: Piece,
    pub result: Option<PieceMatchResult>,
    pub info_hash: Arc<Vec<u8>>,
    pub export_paths: Vec<PathBuf>
}

pub struct Orchestrator;
impl Orchestrator {
    pub fn start(options: &OrchestratorOptions) -> Result<(), std::io::Error> {
        let now = Instant::now();

        // Make sure we don't have duplicate torrents
        let mut hashes = Vec::new();
        for torrent in options.torrents.iter() {
            if hashes.contains(torrent.info_hash.as_ref()) {
                Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, format!("Passed torrent {} more than once. The input list to the orchestrator must be unique.", Orchestrator::get_sha1_hexdigest(&torrent.info_hash))))?
            }

            hashes.push(torrent.info_hash.clone());
        }
        drop(hashes);

        // Setup work
        let work = Orchestrator::make_piece_list(&options.torrents, &options.export_directory);

        // Validate entries
        // Solvers will weigh the identical paths as higher, and writer will skip any parts that have already been written
        for entry in work.iter() {
            for (export_path, file_entry) in entry.export_paths.iter().zip(entry.piece.files.iter()) {
                let expected_file_length = file_entry.file_length;

                if !export_path.exists() {
                    continue;
                }

                let handle = File::open(export_path)?;
                if handle.metadata()?.len() != expected_file_length {
                    Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "File exists on filesystem, but the length of the file does not match the file length in the piece. Aborting to prevent accidental data loss."))?
                }
            }
        }

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

        let writer = PieceWriter::new(piece_count);
        let writer = Arc::new(writer);

        // Partition Pieces
        let (singles, multiple) = Orchestrator::partition_piece_list(work);

        // Start!
        let single_pieces_partitioned_by_hash = Orchestrator::partition_single_pieces_by_path_and_length(singles);
        let single_solver = SinglePieceSolver::new(writer.clone(), finder.clone());
        single_solver.start(single_pieces_partitioned_by_hash, options.threads)?;

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

    fn partition_piece_list(work: Vec<OrchestratorPiece>) -> (Vec<OrchestratorPiece>, Vec<OrchestratorPiece>) {
        let mut singles = Vec::new();
        let mut multiple = Vec::new();

        for entry in work {
            if entry.piece.files.len() == 1 {
                singles.push(entry);
            } else {
                multiple.push(entry);
            }
        }

        (singles, multiple)
    }

    fn make_piece_list(torrents: &[Torrent], export_directory: &Path) -> Vec<OrchestratorPiece> {
        let mut results = Vec::new();

        for torrent in torrents {
            let pieces = Pieces::from_torrent(torrent);
            let info_hash = Arc::new(torrent.info_hash.clone());

            for piece in pieces {
                let mut export_paths: Vec<PathBuf> = Vec::new();
                for file in piece.files.iter() {
                    export_paths.push(Orchestrator::format_path(file, torrent, export_directory));
                }

                let matchable = OrchestratorPiece {
                    piece: piece,
                    result: None,
                    info_hash: info_hash.clone(),
                    export_paths
                };
                
                results.push(matchable);
            }
        }

        results
    }

    // Only use this with single pieces!
    fn partition_single_pieces_by_length(work: Vec<OrchestratorPiece>) -> HashMap<u64, Vec<OrchestratorPiece>> {
        let mut single_files: HashMap<u64, Vec<OrchestratorPiece>> = HashMap::new();

        for orchestrator_piece in work {
            let file = orchestrator_piece.piece.files.first().unwrap();
            let length = file.file_length;

            single_files.entry(length).or_default();

            let items = single_files.get_mut(&length).unwrap();
            items.push(orchestrator_piece);
        }

        single_files
    }

    fn partition_single_pieces_by_path_and_length(work: Vec<OrchestratorPiece>) -> Vec<Vec<OrchestratorPiece>> {
        let mut total = Vec::new();

        for (_, value) in Orchestrator::partition_single_pieces_by_length(work) {
            let mut partitioned: HashMap<PathBuf, Vec<OrchestratorPiece>> = HashMap::new();

            for orchestrator_piece in value {
                partitioned.entry(orchestrator_piece.export_paths.first().unwrap().clone()).or_default();
    
                let items = partitioned.get_mut(orchestrator_piece.export_paths.first().unwrap()).unwrap();
                items.push(orchestrator_piece);
            }
    
            total.extend(partitioned.into_iter().map(|(_, v)| v))
        }
        
        total
    }

    fn format_path(file: &PieceFile, torrent: &Torrent, export_directory: &Path) -> PathBuf {
        let data = Path::new("Data");
        let info_hash_as_human = Orchestrator::get_sha1_hexdigest(&torrent.info_hash);
        let info_hash_path = Path::new(&info_hash_as_human);
        let torrent_name = Path::new(&torrent.info.name);

        if torrent.info.files.is_some() {
            [export_directory, info_hash_path, data, torrent_name, file.file_path.as_path()].iter().collect()
        } else {
            [export_directory, info_hash_path, data, file.file_path.as_path()].iter().collect()
        }
    }

    fn get_sha1_hexdigest(bytes: &[u8]) -> String {
        let mut output = String::new();
        for byte in bytes {
            write!(&mut output, "{:02x?}", byte).expect("Unable to write");
        }
        output
    }
}