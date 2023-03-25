use std::{collections::{HashSet, HashMap}, path::{Path, PathBuf}, io::{BufReader, Read, Seek, Write, SeekFrom}, fs::{File, OpenOptions}, time::Instant};

use crypto::{sha2::Sha256, digest::Digest};

use crate::{finder::{CachedPathFinder, PathFinder}, matcher::PieceMatcher, torrent::{Torrent, Piece}};

pub struct OrchestratorOptions<'a> {
    pub torrent_path: &'a Path,
    pub scan_directory: &'a Path,
    pub export_directory: &'a Path,
}

pub struct Orchestrator;
impl Orchestrator {
    pub fn start(options: &OrchestratorOptions) -> Result<(), std::io::Error> {
        let torrent = Torrent::from(options.torrent_path)?;

        // Setup preallocated files
        let export_directory: PathBuf =
            [options.export_directory, Path::new(&torrent.info_hash), Path::new("Data")]
                .iter()
                .collect();

        Orchestrator::preallocate_all(&export_directory, &torrent)?;

        // Load, deduplicate, and initial rank 
        let finder = Orchestrator::cache_scan_directory(options.scan_directory, &torrent);
        let finder = Orchestrator::deduplicate(finder)?;
        let finder = Orchestrator::rank(&torrent, finder)?; 

        // Start Processing
        Orchestrator::process(&finder, &torrent, &export_directory)?;

        Ok(())
    }

    fn process(finder: &CachedPathFinder, torrent: &Torrent, export_directory: &Path) -> Result<(), std::io::Error> {
        let now = Instant::now();

        // Begin Processing
        let mut success: usize = 0;
        let mut failed: usize = 0;
        let mut skipped: usize = 0;

        for piece in &torrent.pieces {
            let choice_count = PieceMatcher::count_choices(finder, piece);

            let log = {
                move |message: &str, success: usize| {
                    let total = torrent.pieces.len();
                    let success_percentage = (success as f64 / total as f64) * 100 as f64;

                    println!("[{}] Elapsed: {} Choices: {} Position: {} Success Percentage: {:.2}% - {}", torrent.info_hash, now.elapsed().as_secs(), choice_count.unwrap_or(0), piece.position, success_percentage, message);
                }
            };

            if choice_count.is_none() {
                log("Skipped piece due to an unknown amount of choices.", success);
                skipped += 1;
                continue;
            }

            println!("[{}] Elapsed: {} Processing piece {} with {} choices.", torrent.info_hash, now.elapsed().as_secs(), piece.position, choice_count.unwrap_or(0));
            match PieceMatcher::scan(finder, piece)? {
                Some(result) => {
                    let mut start_position = 0;
                    for piece_file in &piece.files {
                     
                        let output_path: PathBuf = Path::new(export_directory)
                            .join(Path::new(&piece_file.file_path));

                        let mut handle = OpenOptions::new().write(true).open(&output_path).unwrap();

                        handle
                            .seek(SeekFrom::Start(piece_file.read_start_position))
                            .expect("Unable to seek into file!");

                        let end_position = start_position + piece_file.read_length as usize;

                        handle
                            .write(&result.bytes[start_position..end_position])
                            .expect("Couldn't write to file!");

                        start_position = end_position;
                    }

                    success += 1;
                    log("Successfully found piece", success);
                }
                None => {
                    failed += 1;
                    log("Failed finding piece", success);
                }
            };
        }

        println!("[{}] Final Statistics: Elapsed: {} Success: {}, Failed: {}, Skipped: {} Total: {}", torrent.info_hash, now.elapsed().as_secs(), success, failed, skipped, &torrent.pieces.len());
        Ok(())
    }

    // TODO: Optimize
    // We shouldn't re-evaluate the pieces here twice...... also needs validation it does what it says it does...
    // We should partition, share the partition with the main thread, and then write the result blocks from here.
    // But also might be a total waste of time with how quick this should be since its heavily constrained.
    fn rank(torrent: &Torrent, mut finder: CachedPathFinder) -> Result<CachedPathFinder, std::io::Error> {
        let mut weights: HashMap<PathBuf, isize> = HashMap::new();
        let mut existence: HashSet<PathBuf> = HashSet::new();
        let samples: Vec<Piece> = torrent.pieces
            .clone()
            .into_iter()
            .filter(|piece| piece.files.len() == 1 && finder.find_length(piece.files[0].file_length).len() > 1)
            .filter(|piece| {
                let piece_file = &piece.files[0];
                existence.insert(piece_file.file_path.clone())
            })
            .collect();

        println!("Ranking file finder using {} samples.", samples.len());
        for sample in samples {
            match PieceMatcher::scan(&finder, &sample)? {
                Some(result) => {
                    for path in &result.paths {
                        if !weights.contains_key(path) {
                            weights.insert(path.clone(), 0);
                        }

                        let count = weights.get_mut(path).unwrap();
                        *count += 1;
                    }
                }
                _ => {}
            }
        }

        for (_, value) in finder.cache.iter_mut() {
            value.sort_by_cached_key(|item| {
                let value = match weights.get(item) {
                    Some(weight) => *weight,
                    None => 0
                };
                value * -1
            });
        }

        Ok(finder)
    }

    fn deduplicate(finder: CachedPathFinder) -> Result<CachedPathFinder, std::io::Error> {
        let mut existence: HashSet<String> = HashSet::new();
        let mut cache: HashMap<u64, Vec<PathBuf>> = HashMap::new();
        
        let mut kept = 0;
        let mut removed = 0;
        for (length, paths) in finder.cache {
            for path in paths {
                let input = File::open(&path)?;
                let reader = BufReader::new(input);
                let hash = Orchestrator::sha256_digest(reader)?;
                if existence.insert(hash) {
                    kept += 1;

                    if !cache.contains_key(&length) {
                        cache.insert(length, Vec::new());
                    }
        
                    let items = cache.get_mut(&length).unwrap();
                    items.push(path);
                } else {
                    removed += 1;
                }
            }
        }
        
        let mut finder = CachedPathFinder::new();
        finder.cache = cache;

        println!("File de-duplication removed {} and kept {} files in the cache.", removed, kept);
        Ok(finder)
    }

    fn sha256_digest<R: Read>(mut reader: R) -> Result<String, std::io::Error> {
        let mut context = Sha256::new();
        let mut buffer = [0; 1024];
    
        loop {
            let count = reader.read(&mut buffer)?;
            if count == 0 {
                break;
            }
            context.input(&buffer[..count]);
        }
    
        Ok(context.result_str())
    }    

    fn cache_scan_directory(scan_directory: &Path, torrent: &Torrent) -> CachedPathFinder {
        let mut finder: CachedPathFinder = CachedPathFinder::new();
        let mut lengths = HashSet::new();

        for file in &torrent.files {
            lengths.insert(file.length);
        }

        let lengths: Vec<u64> = lengths.into_iter().collect();
        finder.add(&lengths, scan_directory);

        return finder;
    }

    fn preallocate(path: &Path, length: u64) -> Result<(), std::io::Error> {
        let options = OpenOptions::new().write(true).create_new(true).open(path)?;
        options.set_len(length)
    }

    fn preallocate_all(export_directory: &Path, torrent: &Torrent) -> Result<(), std::io::Error> {
        for file in &torrent.files {
            let torrent_file_relative = file.path.as_path();
            let torrent_complete_path: PathBuf =
                [export_directory, torrent_file_relative]
                    .iter()
                    .collect();

            // Safety: This should not fail. A torrent path should *always* be an relative path to a *file*
            // If we fail here, we do have an actual issue and we need to abort since this is not a recoverable
            // situtation, and we have absolutely no idea what directory we're creating.
            let torrent_complete_parent = torrent_complete_path.parent().unwrap();
            let length = file.length as u64;

            std::fs::create_dir_all(&torrent_complete_parent)?;
            Orchestrator::preallocate(&torrent_complete_path, length)?;
        }

        Ok(())
    }
}
