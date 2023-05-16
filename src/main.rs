use std::{time::Instant, path::{Path, PathBuf}};

use walkdir::WalkDir;

use crate::orchestrator::{OrchestratorOptions, Orchestrator};

mod matcher;
mod finder;
mod torrent;
mod orchestrator;

fn main() {
    let now = Instant::now();

    let mut torrents_paths: Vec<PathBuf> = Vec::new();
    for e in WalkDir::new("").into_iter().filter_map(|e| e.ok()) {
        if e.metadata().unwrap().is_file() {
            if e.path().extension().unwrap() == "torrent" {
                torrents_paths.push(e.path().to_path_buf());
            }
        }
    }

    let mut scan_directories: Vec<PathBuf> = Vec::new();
    scan_directories.push(Path::new(r#""#).to_path_buf());

    let export_directory = Path::new(r#""#);

    let options = OrchestratorOptions {
        torrents_paths: &torrents_paths,
        scan_directories: &scan_directories,
        export_directory: export_directory,
        threads: 16
    };

    if let Err(err) = Orchestrator::start(&options) {
        eprintln!("Error: {}", err);
    }

    let elapsed = now.elapsed().as_secs();
    println!("Time elapsed took {} seconds for {} torrents.", elapsed, torrents_paths.len());
}
