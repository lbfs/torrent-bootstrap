use std::{fs::{self}, path::PathBuf, sync::{Arc, Mutex}, time::Instant};

use crate::{
    filesystem::{DefaultExportPathFormatter, PathCache, PathInterner},
    metadata::{
        build_raw_torrent_file_metadata, build_raw_torrent_piece_metadata, calculate_total_choices_for_piece, correct_export_file_length, discover_and_apply_searches, validate_export_file_length, TorrentProcessState
    },
    solver::{executor, task::{PieceUpdate, SolverMetadata, Task}},
    torrent::Torrent, writer::FileWriter,
};

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

    if options.threads == 0 {
        return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "Thread count cannot be set to 0."));
    }

    options.threads = std::cmp::max(options.threads, 1);

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

    let torrents_len = torrents.len();

    // Setup required metadata for processing
    let mut path_interner = PathInterner::new();

    let mut torrent_file_metadata 
        = build_raw_torrent_file_metadata::<DefaultExportPathFormatter>(torrents, &mut path_interner, &options.export_directory);

    for metadata_file in torrent_file_metadata.iter() {
        validate_export_file_length(metadata_file, &path_interner, options.resize_export_files)?
    }

    if options.resize_export_files {
        for metadata_file in torrent_file_metadata.iter() {
            correct_export_file_length(metadata_file, &path_interner)?;
        }
    }

    // Now that the files have been updated on disk, scan the user-provided scan directories
    // and get cache the metadata related to the export files that were just updated.
    let mut path_cache = PathCache::new();

    for scan_directory in options.scan_directories.iter() {
        path_cache.add_directory(&mut path_interner, &scan_directory);
    }

    for metadata_file in torrent_file_metadata.iter() {
        path_cache.add_path_by_interner_id(&mut path_interner, metadata_file.export_target);
    }

    // Freeze the data as we've stopped making modifications to disk-related content.
    let path_cache = path_cache.freeze();
    let path_interner = path_interner.freeze();

    // Now, setup the search data that will be needed during processing.
    discover_and_apply_searches(&mut torrent_file_metadata, &path_cache.entries, &path_interner);

    // Build the piece metadata used for work-scheduling
    let mut torrent_piece_metadata = build_raw_torrent_piece_metadata(torrents);
    calculate_total_choices_for_piece(&mut torrent_file_metadata, &mut torrent_piece_metadata);

    let mut items: Vec<usize> = Vec::with_capacity(torrent_piece_metadata.len());
    for piece in torrent_piece_metadata.iter() {
        items.push(piece.piece_id);
    }

    items.sort_by(|left, right| {
        let left_piece = &torrent_piece_metadata[*left];
        let right_piece = &torrent_piece_metadata[*right];

        left_piece.files.len().cmp(&right_piece.files.len())
    });

    items.reverse();

    let solver_metadata = SolverMetadata {
        torrent_files: torrent_file_metadata,
        torrent_pieces: torrent_piece_metadata,
        path_interner,
        counter: Mutex::new(TorrentProcessState::new(items.len()))
    };

    let solver_metadata = Arc::new(solver_metadata);
    let tasks: Vec<Task> = items
        .into_iter()
        .map(| piece_id | Task::new(piece_id, solver_metadata.clone(), options.threads))
        .collect();

    // Setup Writer
    let mut writer = FileWriter::new(solver_metadata.clone());

    let (sender, receiver) = std::sync::mpsc::sync_channel::<PieceUpdate>(1);
    let writer_thread = std::thread::spawn(move || {

        let solver_metadata = solver_metadata.clone();
        let global_state = &solver_metadata.counter;

        while let Ok(mut result) = receiver.recv() {
            // Write to disk
            let mut wrote_to_disk = false;

            if result.found && !result.fault && result.output_bytes.is_some() && result.output_paths.is_some() {
                let res = writer.write(
                    result.piece_id, 
                    result.output_paths.as_ref().unwrap(), 
                    result.output_bytes.as_ref().unwrap()
                );

                match res {
                    Ok(found) => { 
                        wrote_to_disk = found 
                    },
                    Err(err) => {
                        eprintln!("Failed to write piece to disk: {:#?}", err);
                        result.fault = true;
                    },
                }
            }

            /*
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
            */

            // Print out the global processing status
            let mut global_state = global_state
                .lock()
                .expect("Process state should always lock.");

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

    executor::run(tasks, options.threads, sender);

    writer_thread.join().expect("Writer thread should not crash.");

    let elapsed = now.elapsed().as_secs();
    println!("Orchestrator took {} seconds for {} torrents.", elapsed, torrents_len);
    Ok(())
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