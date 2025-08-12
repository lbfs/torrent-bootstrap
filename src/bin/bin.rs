use std::{fs::{self}, path::{Path, PathBuf}, time::Instant};

use clap::Parser;
use torrent_bootstrap::{orchestrator::OrchestratorOptions, torrent::Torrent};

#[derive(Parser)] // requires `derive` feature
#[command(version, about, long_about = None)]
struct Cli {
    /// Path that should be used to load a torrent.
    #[arg(long, required = true, num_args = 1..)]
    torrents: Vec<PathBuf>,

    /// Paths that should be scanned for matching files.
    #[arg(long, required = true, num_args = 1..)]
    scan: Vec<PathBuf>,

    /// Path where the exported file should be updated or stored. Any matching files under this export path are automatically added to the scan path.
    #[arg(long, required = true)]
    export: PathBuf,

    /// If the export file on disk is smaller than the one in the torrent, then resize to match the torrent. This helps with accuracy during the scanning process.
    #[arg(long, required = false, default_value_t = false)]
    resize_export_files: bool,

    /// Number of read threads for hashing.
    #[arg(long, required = false, default_value_t = 1)]
    threads: usize,
}

fn run() -> std::io::Result<()> {
    let args = Cli::parse();
    let now = Instant::now();

    // Load Torrents
    let mut torrents: Vec<Torrent> = Vec::new();
    for torrent_path in &args.torrents {
        let bytes = fs::read(torrent_path)?;

        match Torrent::from_bytes(&bytes) {
            Ok(torrent) => torrents.push(torrent),
            Err(error) => eprintln!("Unable to load torrent from path {:#?} due to error: {:#?}:{}", torrent_path, error.kind, error.message),
        }
    }

    let torrent_len = torrents.len();

    // Start it up!
    let options = OrchestratorOptions {
        torrents,
        scan_directories: args.scan.iter().map(|value| Path::new(value).to_path_buf()).collect(),
        export_directory: Path::new(&args.export).to_path_buf(),
        threads: args.threads,
        resize_export_files: args.resize_export_files
    };

    let res = torrent_bootstrap::orchestrator::start(options);
    let elapsed = now.elapsed().as_secs();
    println!("Time elapsed took {} seconds for {} torrents.", elapsed, torrent_len);
    res
}

fn main() {
    if let Err(err) = run() {
        eprintln!("Error: {}", err);
    }
}
