use std::{path::{Path, PathBuf}, time::Instant};

use crate::orchestrator::{OrchestratorOptions, Orchestrator};

mod finder;
mod torrent;
mod orchestrator;
mod bencode;
mod writer;
mod solver;

fn main() {
    let now = Instant::now();

    // Load Torrents
    let torrents = vec![];
    let torrent_len = torrents.len();

    let scan_directories: Vec<PathBuf> = Vec::new();
    let export_directory = Path::new(r#""#).to_path_buf();

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
}
