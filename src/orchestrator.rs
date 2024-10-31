use std::{
    collections::{BTreeSet, HashMap},
    fs::File,
    path::{Path, PathBuf},
    sync::Arc,
    time::Instant,
};

use crate::{
    finder::{sort_by_target_absolute_path, ExportFileFinder, LengthFileFinder},
    get_sha1_hexdigest,
    solver::{start, PieceSolver, PieceSolverContext},
    torrent::{PieceFile, Pieces, Torrent},
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
    pub file_path: PathBuf,
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

        // Setup work
        let mut work =
            Orchestrator::convert_pieces_to_work(&options.torrents, &options.export_directory);

        // Setup the finder
        let finder = Orchestrator::setup_finder(&options.torrents, &mut work, &options);

        println!(
            "File finder finished caching and finished at {} seconds.",
            now.elapsed().as_secs()
        );

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
        pieces: &mut [OrchestrationPiece],
        options: &OrchestratorOptions,
    ) -> ExportFileFinder {
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
        let mut length_file_finder = LengthFileFinder::new();
        for scan_directory in &options.scan_directories {
            length_file_finder.add(&file_lengths, scan_directory.as_path());
        }

        // Build sorted file finder
        let mut file_index: usize = 0;

        let mut export_file_finder: Vec<Box<[PathBuf]>> = Vec::new();
        let mut path_to_position: HashMap<PathBuf, usize> = HashMap::new();
        for piece in pieces {
            for file in piece.files.iter_mut() {
                if path_to_position.contains_key(&file.export) {
                    file.export_index = *path_to_position.get(&file.export).unwrap();
                    continue;
                };

                let entries = length_file_finder.find_length(file.file_length);
                sort_by_target_absolute_path(&file.file_path, &file.export, entries);

                let sorted: Box<[PathBuf]> =
                    entries.into_iter().map(|value| value.clone()).collect();

                path_to_position.insert(file.export.clone(), file_index);
                export_file_finder.push(sorted);
                file.export_index = file_index;
                file_index = export_file_finder.len();
            }
        }

        ExportFileFinder::new(export_file_finder)
    }

    fn convert_pieces_to_work(
        torrents: &[Torrent],
        export_directory: &Path,
    ) -> Vec<OrchestrationPiece> {
        let mut results = Vec::new();

        for torrent in torrents {
            let pieces = Pieces::from_torrent(torrent);

            for piece in pieces {
                let mut orchestration_piece_files: Vec<OrchestrationPieceFile> = Vec::new();

                for file in piece.files {
                    let export = Orchestrator::format_path(&file, torrent, export_directory);
                    let file_path = file.file_path.clone();

                    orchestration_piece_files.push(OrchestrationPieceFile {
                        read_length: file.read_length,
                        read_start_position: file.read_start_position,
                        file_path,
                        file_length: file.file_length,
                        is_padding_file: file.is_padding_file,
                        bytes: None,
                        source: None,
                        export,
                        export_index: usize::MAX
                    });
                }

                let hash = piece.hash.clone();
                let matchable = OrchestrationPiece {
                    files: orchestration_piece_files,
                    hash: hash,
                };

                results.push(matchable);
            }
        }

        results
    }

    fn format_path(file: &PieceFile, torrent: &Torrent, export_directory: &Path) -> PathBuf {
        let data = Path::new("Data");
        let info_hash_as_human = get_sha1_hexdigest(&torrent.info_hash);
        let info_hash_path = Path::new(&info_hash_as_human);
        let torrent_name = Path::new(&torrent.info.name);

        if torrent.info.files.is_some() {
            [
                export_directory,
                info_hash_path,
                data,
                torrent_name,
                file.file_path.as_path(),
            ]
            .iter()
            .collect()
        } else {
            [
                export_directory,
                info_hash_path,
                data,
                file.file_path.as_path(),
            ]
            .iter()
            .collect()
        }
    }
}
