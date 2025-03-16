use std::{
    fs::{self},
    path::PathBuf,
    sync::Arc,
    time::Instant,
};

use crate::{
    finder::{
        add_export_paths, build_info_hash_file_index_lookup_table, build_torrent_metadata_table,
        fix_export_file_lengths, get_unique_file_lengths, populate_metadata_searches, FileCache,
        TorrentMetadataEntry,
    },
    solver::{run, PieceSolverContext},
    torrent::{Pieces, Torrent},
    writer::FileWriter,
};

#[derive(Debug)]
pub struct OrchestrationPieceFile {
    // Filled out when generating pieces
    pub read_length: u64,
    pub read_start_position: u64,

    // Filled out by orchestration
    pub metadata: Arc<TorrentMetadataEntry>,
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
    pub resize_export_files: bool
}

pub fn start(mut options: OrchestratorOptions) -> Result<(), std::io::Error> {
    let options = &mut options;

    if options.torrents.len() == 0 {
        return Ok(());
    }

    let now = Instant::now();
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

    let metadata = setup_metadata(torrents, &options.export_directory, &options.scan_directories, options.resize_export_files)?;

    println!(
        "File finder finished setup at {} seconds.",
        now.elapsed().as_secs()
    );

    // Setup Writer
    let writer = FileWriter::new(&metadata);

    // Setup work
    let work = convert_pieces_to_work(torrents, metadata);

    // Start processing the work
    println!("Solver started at {} seconds.", now.elapsed().as_secs());

    let context = Arc::new(PieceSolverContext::new(writer, work.len()));
    run(work, context, options.threads);

    println!("Solver finished at {} seconds.", now.elapsed().as_secs());

    Ok(())
}

fn setup_metadata(torrents: &[Torrent], export_directory: &PathBuf, scan_directories: &[PathBuf], resize_export_files: bool) -> Result<Vec<TorrentMetadataEntry>, std::io::Error> {
    let mut file_cache = FileCache::new();

    let mut metadata = build_torrent_metadata_table(torrents, export_directory);

    if resize_export_files {
        fix_export_file_lengths(&metadata)?;
    }

    let unique_lengths = get_unique_file_lengths(&metadata);

    add_export_paths(&metadata, &mut file_cache);

    for scan_directory in scan_directories {
        file_cache.add_by_directory_and_length(scan_directory, &unique_lengths);
    }

    populate_metadata_searches(&mut metadata, &file_cache);

    Ok(metadata)
}

fn convert_pieces_to_work(
    torrents: &[Torrent],
    metadata: Vec<TorrentMetadataEntry>
) -> Vec<OrchestrationPiece> {
    let lookup = build_info_hash_file_index_lookup_table(metadata);

    let mut results = Vec::new();

    for torrent in torrents {
        let pieces = Pieces::from_torrent(torrent);

        for piece in pieces {
            let mut orchestration_piece_files: Vec<OrchestrationPieceFile> = Vec::new();

            for file in piece.files {
                orchestration_piece_files.push(OrchestrationPieceFile {
                    read_length: file.read_length,
                    read_start_position: file.read_start_position,
                    metadata: lookup.get(&torrent.info_hash).unwrap().get(&file.file_index).unwrap().clone()
                });
            }

            let matchable = OrchestrationPiece {
                files: orchestration_piece_files,
                hash: piece.hash,
            };

            results.push(matchable);
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

    let metadata = fs::metadata(path)?;

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