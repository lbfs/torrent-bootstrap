use std::{
    collections::BTreeSet,
    fs::File,
    path::{Path, PathBuf},
    sync::Arc,
    time::Instant,
};

use crate::{
    finder::{format_path, FileFinder, LengthFileFinder},
    get_sha1_hexdigest,
    solver::{start, PieceSolver, PieceSolverContext},
    torrent::{Pieces, Torrent},
    writer::PieceWriter,
};

pub struct OrchestratorOptions {
    pub torrents: Vec<Torrent>,
    pub scan_directories: Vec<PathBuf>,
    pub export_directory: PathBuf,
    pub threads: usize,
}

#[derive(Debug)]
pub struct OrchestrationPieceFile {
    // Filled out when generating pieces
    pub read_length: u64,
    pub read_start_position: u64,
    // pub file_path: PathBuf,
    pub file_length: u64,
    pub is_padding_file: bool,

    // Filled out by orchestration
    pub bytes: Option<Vec<u8>>,
    pub source: Option<PathBuf>,
    pub export: PathBuf,
    pub export_index: usize,
}

#[derive(Debug)]
pub struct OrchestrationPiece {
    pub files: Vec<OrchestrationPieceFile>,
    pub hash: Vec<u8>,
}

pub struct Orchestrator;
impl Orchestrator {
    pub fn start(options: &OrchestratorOptions) -> Result<(), std::io::Error> {
        let now = Instant::now();

        // Make sure paths are allowed
        for scan_directory in options.scan_directories.iter() {
            if !(scan_directory.exists() && scan_directory.is_dir()) {
                Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Scan directory does not exist or is not a directory.",
                ))?
            }

            if !scan_directory.is_absolute() {
                Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Scan directory must be an absolute path.",
                ))?
            }
        }

        // Check export path to make sure it is valid also.
        if !(options.export_directory.exists() && options.export_directory.is_dir()) {
            Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Export directory does not exist or is not a directory.",
            ))?
        }

        if !options.export_directory.is_absolute() {
            Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Export directory must be an absolute path.",
            ))?
        }

        // Make sure we don't have duplicate torrents
        let mut hashes = Vec::with_capacity(options.torrents.len());
        for torrent in options.torrents.iter() {
            if hashes.contains(torrent.info_hash.as_ref()) {
                Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, format!("Passed torrent {} more than once. The input list to the orchestrator must be unique.", get_sha1_hexdigest(&torrent.info_hash))))?
            }

            hashes.push(torrent.info_hash.clone());
        }
        drop(hashes);

        // Setup the finder
        let finder = Orchestrator::setup_finder(&options.torrents, &options);

        println!(
            "File finder finished caching and finished at {} seconds.",
            now.elapsed().as_secs()
        );

        // Setup work
        let work =
            Orchestrator::convert_pieces_to_work(&options.torrents, &options.export_directory, &finder);

        // Validate entries
        // Solvers will weigh the identical paths as higher, and writer will skip any parts that have already been written
        for entry in work.iter() {
            for file in entry.files.iter() {
                let expected_file_length = file.file_length;

                if !file.export.exists() {
                    continue;
                }

                let handle = File::open(&file.export)?;
                if handle.metadata()?.len() != expected_file_length {
                    Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "File exists on filesystem, but the length of the file does not match the file length in the piece. Aborting to prevent accidental data loss."))?
                }
            }
        }



        // Setup Writer
        let writer = PieceWriter::new(work.len());

        // Start processing the work
        println!("Solver started at {} seconds.", now.elapsed().as_secs());

        let context = Arc::new(PieceSolverContext::new(finder, writer));
        start::<_, _, PieceSolver>(work, context, options.threads);

        println!("Solver finished at {} seconds.", now.elapsed().as_secs());

        Ok(())
    }

    fn setup_finder(
        torrents: &[Torrent],
        options: &OrchestratorOptions,
    ) -> FileFinder {
        // Unique File Lengths
        let mut file_lengths: BTreeSet<u64> = BTreeSet::new();

        for torrent in torrents {
            if torrent.info.length.is_some() {
                file_lengths.insert(torrent.info.length.unwrap());
            } else if torrent.info.files.is_some() {
                for file in torrent.info.files.as_ref().unwrap() {
                    file_lengths.insert(file.length);
                }
            }
        }

        // Length File Finder
        let length_file_finder = LengthFileFinder::new(&file_lengths, &options.scan_directories);
        FileFinder::new(torrents, &options.export_directory, length_file_finder)
    }

    fn convert_pieces_to_work(
        torrents: &[Torrent],
        export_directory: &Path,
        finder: &FileFinder
    ) -> Vec<OrchestrationPiece> {
        let mut results = Vec::new();

        for torrent in torrents {
            let pieces = Pieces::from_torrent(torrent);

            for piece in pieces {
                let mut orchestration_piece_files: Vec<OrchestrationPieceFile> = Vec::new();

                for file in piece.files {
                    let export = format_path(&file, torrent, export_directory);
                    let export_index = finder.find_position(&export);

                    orchestration_piece_files.push(OrchestrationPieceFile {
                        read_length: file.read_length,
                        read_start_position: file.read_start_position,
                        // file_path,
                        file_length: file.file_length,
                        is_padding_file: file.is_padding_file,
                        bytes: None,
                        source: None,
                        export: export,
                        export_index: export_index.unwrap()
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
}
