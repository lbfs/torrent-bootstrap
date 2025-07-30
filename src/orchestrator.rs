use std::{
    fs::{self},
    path::PathBuf,
    sync::Arc,
    time::Instant,
};

use crate::{
    finder::{
        add_export_paths, arcify_metadata, build_global_torrent_state, build_info_hash_file_index_lookup_table, build_torrent_metadata_table, fix_export_file_lengths, get_unique_file_lengths, populate_metadata_searches, FileCache, TorrentMetadataEntry
    }, get_sha1_hexdigest, solver::{run, PieceSolver, PieceUpdate}, torrent::{Pieces, Torrent}, writer::FileWriter
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
    let metadata = arcify_metadata(metadata);

    println!(
        "File finder finished setup at {} seconds.",
        now.elapsed().as_secs()
    );

    // Setup work
    let work = convert_pieces_to_work(torrents, &metadata);

    // Setup Writer
    let mut writer = FileWriter::new(&work, options.threads);
    let mut global_state = build_global_torrent_state(torrents);

    let (sender, receiver) = std::sync::mpsc::sync_channel::<PieceUpdate>(1);
    let writer_thread = std::thread::spawn(move || {
        while let Ok(mut result) = receiver.recv() {

            // Write to disk
            let mut wrote_to_disk = false;

            if result.found && !result.fault && result.output_bytes.is_some() && result.output_paths.is_some() {
                let res = writer.write(
                    &result.piece, 
                    result.output_paths.as_ref().unwrap(), 
                    result.output_bytes.as_ref().unwrap()
                );

                match res {
                    Ok(found) => { wrote_to_disk = found },
                    Err(err) => {
                        eprintln!("Failed to write piece to disk: {:#?}", err);
                        result.fault = true;
                    },
                }
            }

            // Print a message if all pieces for a file are finished processing
            for file in &result.piece.files {
                let processing_state = file.metadata.processing_state
                    .lock()
                    .expect("Should always lock the processing state.");

                if processing_state.writable_pieces + processing_state.ignored_pieces + processing_state.fault_pieces == processing_state.total_pieces {
                    println!(
                        "Finished processing file at {:#?} for torrent {} with {} ignored pieces, {} fault pieces, {} writable pieces of {} total pieces", 
                        file.metadata.full_target, 
                        get_sha1_hexdigest(&file.metadata.info_hash),
                        processing_state.ignored_pieces,
                        processing_state.fault_pieces,
                        processing_state.writable_pieces,
                        processing_state.total_pieces
                    )
                }
            }

            // Print out the global processing status
            global_state.success_pieces += (result.found && !result.fault) as usize;
            global_state.failed_pieces += (!result.found && !result.fault) as usize;
            global_state.fault_pieces += (result.fault) as usize;
            global_state.writable_pieces += (wrote_to_disk) as usize;
            global_state.ignored_pieces += (!wrote_to_disk) as usize;

            let availability = (global_state.success_pieces as f64 / global_state.total_pieces as f64) * 100_f64;
            let processed = global_state.success_pieces + global_state.failed_pieces + global_state.fault_pieces;
            let scanned = (processed as f64 / global_state.total_pieces as f64) * 100_f64;
        
            println!(
                "Availability: {:.03}%, Scanned: {:.03}% - Success: {}, Failed: {}, Faulted: {}, Written: {}, Ignored: {} Total: {} of {}", 
                availability, scanned, global_state.success_pieces, global_state.failed_pieces, global_state.fault_pieces, 
                global_state.writable_pieces, global_state.ignored_pieces, processed, global_state.total_pieces
            );
        }
    });

    // Start processing the work
    println!("Solver threads started at {} seconds.", now.elapsed().as_secs());

    let solver = PieceSolver::new(sender, &metadata, &work);   
    run(work, solver, options.threads);

    println!("Solver threads completed at {} seconds.", now.elapsed().as_secs());

    writer_thread.join().expect("Writer thread should not crash.");

    println!("Writer threads completed at {} seconds.", now.elapsed().as_secs());

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
    metadata: &[Arc<TorrentMetadataEntry>]
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

    let metadata = fs::metadata(path);

    if let Err(error) = &metadata {
        Err(std::io::Error::new(error.kind(), format!("Encountered error while reading metadata for path {:#?}: {}", path, error)))?;
    }

    let metadata = metadata.unwrap();

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