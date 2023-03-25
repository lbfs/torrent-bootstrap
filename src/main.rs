use std::{time::Instant, path::Path};

use crate::orchestrator::{OrchestratorOptions, Orchestrator};

mod finder;
mod torrent;
mod orchestrator;
mod matcher;

fn main() {
    let now = Instant::now();

    let torrent_path = Path::new(r#""#);
    let scan_directory = Path::new(r#""#);
    let export_directory = Path::new(r#""#);

    let options = OrchestratorOptions {
        torrent_path: torrent_path,
        scan_directory: scan_directory,
        export_directory: export_directory
    };

    if let Err(err) = Orchestrator::start(&options) {
        eprintln!("Error: {}", err);
    }

    let elapsed = now.elapsed().as_secs();
    println!("Time elapsed took {} seconds for torrent path {}", elapsed, torrent_path.display());
}
