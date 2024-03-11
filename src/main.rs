use std::{time::Instant, path::{Path, PathBuf}, collections::HashSet};

use torrent::Torrent;
use walkdir::WalkDir;

use crate::orchestrator::{OrchestratorOptions, Orchestrator};

mod matcher;
mod finder;
mod torrent;
mod orchestrator;
mod bencode;

fn main() {
    let now = Instant::now();

    // Scan Torrents
    let mut torrents_paths: Vec<PathBuf> = Vec::new();

    // Load Torrents
    let torrents = vec![];
    let torrent_len = torrents.len();

    let mut scan_directories: Vec<PathBuf> = Vec::new();
    let export_directory = Path::new(r#"/mnt/storage/Export"#).to_path_buf();

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
