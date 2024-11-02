use std::{
    fs::{self, File},
    path::PathBuf,
    sync::Arc,
    time::Instant,
};

use crate::{
    finder::{FileFinder, LengthFileFinder},
    solver::{run, PieceSolver, PieceSolverContext},
    torrent::{Pieces, Torrent}
};

#[derive(Debug)]
pub struct OrchestrationPieceFile {
    // Filled out when generating pieces
    pub read_length: u64,
    pub read_start_position: u64,
    pub is_padding_file: bool,

    // Filled out by orchestration
    pub bytes: Option<Vec<u8>>,
    pub source: Option<PathBuf>,
    pub export_index: usize,
}

#[derive(Debug)]
pub struct OrchestrationPiece {
    pub files: Vec<OrchestrationPieceFile>,
    pub hash: Vec<u8>,
}

pub struct OrchestratorOptions {
    pub torrents: Vec<Torrent>,
    pub scan_directories: Vec<PathBuf>,
    pub export_directory: PathBuf,
    pub threads: usize,
}

pub fn start(options: &OrchestratorOptions) -> Result<(), std::io::Error> {
    let now = Instant::now();

    validate_input_paths(options)?;

    // Make sure we don't have duplicate torrents
    let initial_torrent_count = options.torrents.len();

    let mut torrents = options.torrents.to_vec();
    torrents.sort_by(|a, b| {
        a.info_hash.cmp(&b.info_hash)
    });
    torrents.dedup_by(|a, b| {
        a.info_hash.cmp(&b.info_hash).is_eq()
    });

    if torrents.len() != initial_torrent_count {
        println!("Removed {} duplicated torrents from the input list.", initial_torrent_count - torrents.len());
    }

    // Setup the finder
    let length_file_finder = LengthFileFinder::new(&torrents, &options.scan_directories);
    let finder = FileFinder::new(&torrents, &options.export_directory, length_file_finder);

    println!(
        "File finder finished caching and finished at {} seconds.",
        now.elapsed().as_secs()
    );

    // Setup work
    let work = convert_pieces_to_work(&torrents);

    // Validate entries
    // Solvers will weigh the identical paths as higher, and writer will skip any parts that have already been written
    for (index, export_path) in finder.get_paths_in_index_order().iter().enumerate() {
        let expected_file_length = finder.find_length(index);

        if !export_path.exists() {
            continue;
        }

        let handle = File::open(export_path)?;
        if handle.metadata()?.len() != expected_file_length {
            Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "File exists on filesystem, but the length of the file does not match the file length in the piece. Aborting to prevent accidental data loss."))?
        }
    }

    // Start processing the work
    println!("Solver started at {} seconds.", now.elapsed().as_secs());

    let context = Arc::new(PieceSolverContext::new(finder, work.len()));
    run::<_, _, PieceSolver>(work, context, options.threads);

    println!("Solver finished at {} seconds.", now.elapsed().as_secs());

    Ok(())
}

fn convert_pieces_to_work(
    torrents: &[Torrent]
) -> Vec<OrchestrationPiece> {
    let mut results = Vec::new();

    let mut base_index = 0;

    for torrent in torrents {
        let pieces = Pieces::from_torrent(torrent);

        for piece in pieces {
            let mut orchestration_piece_files: Vec<OrchestrationPieceFile> = Vec::new();

            for file in piece.files {
                orchestration_piece_files.push(OrchestrationPieceFile {
                    read_length: file.read_length,
                    read_start_position: file.read_start_position,
                    is_padding_file: file.is_padding_file,
                    bytes: None,
                    source: None,
                    export_index: file.file_index + base_index
                });
            }

            let matchable = OrchestrationPiece {
                files: orchestration_piece_files,
                hash: piece.hash,
            };

            results.push(matchable);
        }

        if torrent.info.files.is_some() {
            base_index += torrent.info.files.as_ref().unwrap().len();
        } else if torrent.info.length.is_some() {
            base_index += 1;
        }
    }

    results
}

fn validate_input_paths(options: &OrchestratorOptions) -> Result<(), std::io::Error> {
    // Make sure all input scan paths are absolute and are proper directories
    for scan_directory in options.scan_directories.iter() {
        if !scan_directory.is_absolute() {
            Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Scan directory must be an absolute path.",
            ))?
        }
    
        match fs::metadata(scan_directory) {
            Ok(metadata) => {
                if !metadata.is_dir() {
                    Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "Scan directory is not a directory.",
                    ))?
                }
            },
            Err(e) => {
                Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("Could not open scan directory: {}", e),
                ))?
            },
        }
    }

    // Same thing as above, but for the export path.
    {
        if !options.export_directory.is_absolute() {
            Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Export directory must be an absolute path.",
            ))?
        }
    
        match fs::metadata(&options.export_directory) {
            Ok(metadata) => {
                if !metadata.is_dir() {
                    Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "Export directory is not a directory.",
                    ))?
                }
            },
            Err(e) => {
                Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("Could not open export directory: {}", e),
                ))?
            },
        }
    }

    Ok(())
}