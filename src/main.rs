use std::{time::Instant, path::{Path, PathBuf}, collections::HashSet};

use torrent::Torrent;
use walkdir::WalkDir;

use crate::orchestrator::{OrchestratorOptions, Orchestrator};

mod matcher;
mod finder;
mod torrent;
mod orchestrator;

fn load_torrents(torrent_paths: &Vec<PathBuf>) -> Vec<Torrent> {
    let mut existence: HashSet<String> = HashSet::new();
    let mut torrents = Vec::new();

    for path in torrent_paths {
        match Torrent::from(path.as_path()) {
            Ok(torrent) => {
                if existence.contains(&torrent.info_hash) {
                    continue;
                }

                existence.insert(torrent.info_hash.clone());
                torrents.push(torrent);
            }
            Err(error) => {
                println!(
                    "Failed to open torrent file at {} with error {}",
                    path.to_str().unwrap(),
                    error.to_string()
                );
            }
        }
    }

    return torrents;
}

fn main() {
    let now = Instant::now();

    // Scan Torrents
    let mut torrents_paths: Vec<PathBuf> = Vec::new();
    for e in WalkDir::new("/mnt/stepping/torrent_files").into_iter().filter_map(|e| e.ok()) {
        if e.metadata().unwrap().is_file() {
            if e.path().extension().unwrap() == "torrent" {
                torrents_paths.push(e.path().to_path_buf());
            }
        }
    }

    // Load Torrents
    let torrents = load_torrents(&torrents_paths);
    let torrent_len = torrents.len();

    let mut scan_directories: Vec<PathBuf> = Vec::new();
    scan_directories.push(Path::new(&"/mnt/stepping").to_path_buf());

    let export_directory = Path::new(r#"/mnt/export"#).to_path_buf();

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
