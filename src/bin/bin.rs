use std::{fs::File, io::Read, path::Path, time::Instant};

use clap::Parser;
use torrent_bootstrap::{OrchestratorOptions, Torrent};

#[derive(Parser)] // requires `derive` feature
#[command(version, about, long_about = None)]
struct Cli {
    /// Path that should be used to load a torrent
    #[arg(long, required = true, num_args = 1..)]
    torrents: Vec<String>,

    /// Absolute path that should be scanned to find identical pieces
    #[arg(long, required = true, num_args = 1..)]
    scan: Vec<String>,

    /// Absolute path where the merged or updated file should be placed.
    #[arg(long, required = true)]
    export: String,

    /// Number of threads to perform scanning and hashing.
    #[arg(long, required = true)]
    threads: usize,
}

fn run() -> std::io::Result<()> {
    let args = Cli::parse();
    let now = Instant::now();

    // Load Torrents
    let mut torrents: Vec<Torrent> = Vec::new();
    for torrent_path_as_string in &args.torrents {
        let torrent_as_path = Path::new(torrent_path_as_string);
        
        let mut handle = File::open(torrent_as_path)?;
        let mut bytes: Vec<u8> = Vec::new(); 
        handle.read_to_end(&mut bytes)?;
        let torrent = Torrent::from_bytes(&bytes)
            .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err.message))?;

        torrents.push(torrent);
    }
    let torrent_len = torrents.len();

    // Start it up!
    let options = OrchestratorOptions {
        torrents: torrents,
        scan_directories: args.scan.iter().map(|value| Path::new(value).to_path_buf()).collect(),
        export_directory: Path::new(&args.export).to_path_buf(),
        threads: args.threads
    };

    let res = torrent_bootstrap::start(&options);
    let elapsed = now.elapsed().as_secs();
    println!("Time elapsed took {} seconds for {} torrents.", elapsed, torrent_len);
    res
}

fn main() {
    if let Err(err) = run() {
        eprintln!("Error: {}", err);
    }
}
