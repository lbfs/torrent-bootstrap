use std::{collections::HashMap, fs::File, path::{Path, PathBuf}, sync::Arc, time::Instant};

use crate::{finder::LengthFileFinder, get_sha1_hexdigest, solver::{MultiplePieceSolver, SinglePieceSolver, Solver}, torrent::{PieceFile, Pieces, Torrent}, writer::PieceWriter};

pub struct OrchestratorOptions {
    pub torrents: Vec<Torrent>,
    pub scan_directories: Vec<PathBuf>,
    pub export_directory: PathBuf,
    pub threads: usize,
}

pub(crate) struct OrchestrationPieceFile {
    // Filled out when generating pieces
    pub read_length: u64,
    pub read_start_position: u64,
    pub file_path: Arc<PathBuf>,
    pub file_length: u64,

    // Filled out by orchestration
    pub bytes: Option<Vec<u8>>,
    pub source: Option<PathBuf>,
    pub export: Arc<PathBuf>
}

pub(crate) struct OrchestrationPiece {
    pub files: Vec<OrchestrationPieceFile>,
    pub hash: Arc<Vec<u8>>
}

pub struct Orchestrator;
impl Orchestrator {
    pub fn start(options: &OrchestratorOptions) -> Result<(), std::io::Error> {
        let now = Instant::now();

        // Make sure paths are allowed
        for scan_directory in options.scan_directories.iter() {
            if !(scan_directory.exists() && scan_directory.is_dir()) {
                Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "Scan directory does not exist or is not a directory."))?
            } 

            if !scan_directory.is_absolute() {
                Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "Scan directory must be an absolute path."))?
            }
        }

        // Check export path to make sure it is valid also.
        if !(options.export_directory.exists() && options.export_directory.is_dir()) {
            Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "Export directory does not exist or is not a directory."))?
        } 

        if !options.export_directory.is_absolute() {
            Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "Export directory must be an absolute path."))?
        }

        // Make sure we don't have duplicate torrents
        let mut hashes = Vec::new();
        for torrent in options.torrents.iter() {
            if hashes.contains(torrent.info_hash.as_ref()) {
                Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, format!("Passed torrent {} more than once. The input list to the orchestrator must be unique.", get_sha1_hexdigest(&torrent.info_hash))))?
            }

            hashes.push(torrent.info_hash.clone());
        }
        drop(hashes);

        // Setup work
        let work = Orchestrator::convert_pieces_to_work(&options.torrents, &options.export_directory);

        // Validate entries
        // Solvers will weigh the identical paths as higher, and writer will skip any parts that have already been written
        for entry in work.iter() {
            for file in entry.files.iter() {
                let expected_file_length = file.file_length;

                if !file.export.exists() {
                    continue;
                }

                let handle = File::open(file.export.as_ref())?;
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

        // Immeditately reject any piece without matches
        let mut accepted = Vec::new();

        for entry in work {
            let mut is_rejected = false;

            for file in entry.files.iter() {
                if finder.find_length(file.file_length).len() == 0 {
                    is_rejected = true;
                    break;
                }
            }

            if is_rejected {
                writer.write(None)?;
            } else {
                accepted.push(entry);
            }
        }

        // Partition Pieces
        let (singles, multiple) = Orchestrator::partition_piece_list(accepted);

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

    fn partition_piece_list(work: Vec<OrchestrationPiece>) -> (Vec<OrchestrationPiece>, Vec<OrchestrationPiece>) {
        let mut singles = Vec::new();
        let mut multiple = Vec::new();

        for entry in work {
            if entry.files.len() == 1 {
                singles.push(entry);
            } else {
                multiple.push(entry);
            }
        }

        (singles, multiple)
    }
    
    fn convert_pieces_to_work(torrents: &[Torrent], export_directory: &Path) -> Vec<OrchestrationPiece> {
        let mut results = Vec::new();

        for torrent in torrents {
            let pieces = Pieces::from_torrent(torrent);

            let mut previous_path: Option<Arc<PathBuf>> = None;
            let mut previous_export: Option<Arc<PathBuf>> = None;
            let mut previous_hash: Option<Arc<Vec<u8>>> = None;

            for piece in pieces {
                let mut orchestration_piece_files: Vec<OrchestrationPieceFile> = Vec::new();

                for file in piece.files {
                    // Cleanup this path interning logic
                    // Previous export
                    let mut export = Arc::new(Orchestrator::format_path(&file, torrent, export_directory));
                    if let Some(previous_export_unpacked) = &previous_export {
                        if !export.as_ref().cmp(previous_export_unpacked.as_ref()).is_eq() {
                            previous_export = Some(export.clone());
                        } else {
                            export = previous_export_unpacked.clone();
                        }
                    } else {
                        previous_export = Some(export.clone());
                    }

                    // Previous path
                    let mut file_path = Arc::new(file.file_path);
                    if let Some(previous_file_unpacked) = &previous_path {
                        if !previous_file_unpacked.as_ref().cmp(file_path.as_ref()).is_eq()  {
                            previous_path = Some(file_path.clone());
                        } else {
                            file_path = previous_file_unpacked.clone();
                        }
                    } else {
                        previous_path = Some(file_path.clone());
                    }

                    orchestration_piece_files.push(OrchestrationPieceFile {
                        read_length: file.read_length,
                        read_start_position: file.read_start_position,
                        file_path,
                        file_length: file.file_length,
                        bytes: None,
                        source: None,
                        export
                    });
                }

                // More interning code
                let mut hash = Arc::new(piece.hash);
                if let Some(previous_hash_unpacked) = &previous_hash {
                    if !hash.as_ref().cmp(previous_hash_unpacked.as_ref()).is_eq() {
                        previous_hash = Some(hash.clone());
                    } else {
                        hash = previous_hash_unpacked.clone();
                    }
                } else {
                    previous_hash = Some(hash.clone());
                }

                let matchable = OrchestrationPiece {
                    files: orchestration_piece_files,
                    hash: hash
                };
                
                results.push(matchable);
            }
        }

        results
    }

    // Only use this with single pieces!
    fn partition_single_pieces_by_length(work: Vec<OrchestrationPiece>) -> HashMap<u64, Vec<OrchestrationPiece>> {
        let mut single_files: HashMap<u64, Vec<OrchestrationPiece>> = HashMap::new();

        for entry in work {
            let file = entry.files.first().unwrap();
            let length = file.file_length;

            single_files.entry(length).or_default();

            let items = single_files.get_mut(&length).unwrap();
            items.push(entry);
        }

        single_files
    }

    fn partition_single_pieces_by_path_and_length(work: Vec<OrchestrationPiece>) -> Vec<Vec<OrchestrationPiece>> {
        let mut total = Vec::new();

        for (_, value) in Orchestrator::partition_single_pieces_by_length(work) {
            let mut partitioned: HashMap<PathBuf, Vec<OrchestrationPiece>> = HashMap::new();

            for entry in value {
                partitioned.entry(entry.files.first().unwrap().export.as_ref().clone()).or_default();
    
                let items = partitioned.get_mut(entry.files.first().unwrap().export.as_ref()).unwrap();
                items.push(entry);
            }
    
            total.extend(partitioned.into_iter().map(|(_, v)| v))
        }
        
        total
    }

    fn format_path(file: &PieceFile, torrent: &Torrent, export_directory: &Path) -> PathBuf {
        let data = Path::new("Data");
        let info_hash_as_human = get_sha1_hexdigest(&torrent.info_hash);
        let info_hash_path = Path::new(&info_hash_as_human);
        let torrent_name = Path::new(&torrent.info.name);

        if torrent.info.files.is_some() {
            [export_directory, info_hash_path, data, torrent_name, file.file_path.as_path()].iter().collect()
        } else {
            [export_directory, info_hash_path, data, file.file_path.as_path()].iter().collect()
        }
    }
}