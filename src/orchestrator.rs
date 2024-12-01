use std::{fs::{self}, path::PathBuf, sync::Arc, time::Instant};

use crate::{
    finder::{intern_paths, setup_finder_cache, FileFinder},
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

pub fn start(mut options: OrchestratorOptions) -> Result<(), std::io::Error> {
    let now = Instant::now();
    let options = &mut options;

    validate_input_paths(options)?;

    // Make sure we don't have duplicate torrents
    let initial_torrent_count = options.torrents.len();
    let torrents = &mut options.torrents;
    
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
    println!(
        "File finder started {} seconds.",
        now.elapsed().as_secs()
    );

    let length_finder = setup_finder_cache(torrents, &options.export_directory, &options.scan_directories)?;
    let length_finder = intern_paths(length_finder); 

    let finder = FileFinder::new(&torrents, &options.export_directory, length_finder);

    println!(
        "File finder finished setup at {} seconds.",
        now.elapsed().as_secs()
    );

    // Setup work
    let work = convert_pieces_to_work(&torrents);

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

fn validate_path(path: &PathBuf) -> Result<(), std::io::Error> {
    if !path.is_absolute() {
        Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("{:#?} path must be absolute.", &path),
        ))?
    }

    let metadata = fs::metadata(&path)?;

    if !metadata.is_dir() {
        Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, format!("{:#?} is not a directory.", path)))?
    }

    Ok(())
}

fn validate_input_paths(options: &OrchestratorOptions) -> Result<(), std::io::Error> {
    // Make sure all input scan paths are absolute and are proper directories
    for scan_directory in options.scan_directories.iter() {
        validate_path(scan_directory)?;
    }

    // Same thing as above, but for the export path.
    validate_path(&options.export_directory)
}