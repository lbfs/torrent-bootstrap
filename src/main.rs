use std::{fs::File, io::Read, path::{Path, PathBuf}, time::Instant};

use crate::{orchestrator::{Orchestrator, OrchestratorOptions}, torrent::Torrent};
use clap::Parser;

mod finder;
mod torrent;
mod orchestrator;
mod bencode;
mod writer;
mod solver;

#[derive(Parser)] // requires `derive` feature
#[command(version, about, long_about = None)]
struct Cli {
    // Path that should be used to load a torrent
    #[arg(long, required = true, num_args = 1..)]
    torrents: Vec<String>,

    // Absolute path that should be scanned to find identical pieces
    #[arg(long, required = true, num_args = 1..)]
    scan: Vec<String>,

    // Absolute path where the merged or updated file should be placed.
    #[arg(long, required = true)]
    export: String,

    // Number of threads to perform scanning and hashing.
    #[arg(long, required = true)]
    threads: usize,
}

fn main() -> std::io::Result<()> {
    let now = Instant::now();

    let args = Cli::parse();

    // Load Torrents
    let mut torrents: Vec<Torrent> = Vec::new();
    for torrent_path_as_string in &args.torrents {
        let torrent_as_path = Path::new(torrent_path_as_string);

        if !(torrent_as_path.exists() && torrent_as_path.is_file()) {
            Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "Export directory does not exist or is not a directory."))?
        }

        let mut handle = File::open(torrent_as_path)?;
        let mut buffer = Vec::new();
        handle.read_to_end(&mut buffer)?;

        let torrent = Torrent::from_bytes(&buffer)
            .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err.message))?;

        torrents.push(torrent);
    }
    let torrent_len = torrents.len();


    let mut scan_directories: Vec<PathBuf> = Vec::new();
    for scan_directory in &args.scan {
        scan_directories.push(Path::new(scan_directory).to_path_buf());
    }

    let export_directory = Path::new(&args.export).to_path_buf();

    let options = OrchestratorOptions {
        torrents: torrents,
        scan_directories: scan_directories,
        export_directory: export_directory,
        threads: 16
    };

    if let Err(err) = Orchestrator::start(&options) {
        eprintln!("Error: {}", err);
    }

    let elapsed = now.elapsed().as_secs();
    println!("Time elapsed took {} seconds for {} torrents.", elapsed, torrent_len);

    Ok(())
}
