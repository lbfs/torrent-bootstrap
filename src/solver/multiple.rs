use std::{cmp::min, collections::HashMap, path::PathBuf, sync::Arc};

use sha1::{Digest, Sha1};

use crate::{finder::{read_bytes, sort_by_target_absolute_path, LengthFileFinder}, orchestrator::OrchestrationPiece, writer::PieceWriter};

use super::Solver;

#[derive(Clone)]
pub struct MultiplePieceSolver {
    writer: Arc<PieceWriter>,
    finder: Arc<LengthFileFinder>
}

impl MultiplePieceSolver {
    pub fn new(writer: Arc<PieceWriter>, finder: Arc<LengthFileFinder>) -> MultiplePieceSolver {
        MultiplePieceSolver {
            writer,
            finder
        }
    }

    fn scan(
        finder: &LengthFileFinder,
        mut entry: &mut OrchestrationPiece,
    ) -> Result<bool, std::io::Error> {
        let loaded = MultiplePieceSolver::preload(entry, finder)?;
        MultiplePieceSolver::scan_internal(0, Sha1::new(), &loaded, &mut entry)
    }

    fn scan_internal<'a>(
        depth: usize,
        hasher: Sha1,
        finder: &HashMap<usize, Vec<(&'a PathBuf, Vec<u8>)>>,
        entry: &mut OrchestrationPiece
    ) -> Result<bool, std::io::Error> {
        let entries = finder.get(&depth).unwrap();

        for (path, read_buffer) in entries.into_iter() {
            let mut hasher = hasher.clone();
            hasher.update(&read_buffer);

            let valid = if depth + 1 == entry.files.len() {
                let hash = hasher.finalize();
                entry.hash.as_slice().cmp(&hash).is_eq()
            } else {
                MultiplePieceSolver::scan_internal(depth + 1, hasher, finder, entry)?
            };

            if valid {
                let depth_file = entry.files.get_mut(depth).unwrap();
                depth_file.bytes = Some(read_buffer.clone());
                depth_file.source = Some(path.to_path_buf());
                return Ok(valid);
            }
        }

        Ok(false)
    }

    fn preload<'a>(entry: &OrchestrationPiece, finder: &'a LengthFileFinder) -> Result<HashMap<usize, Vec<(&'a PathBuf, Vec<u8>)>>, std::io::Error> {
        let mut loaded = HashMap::new();

        for (file_position, file) in entry.files.iter().enumerate() {
            let mut results: Vec<(&'a PathBuf, Vec<u8>)> = Vec::new();
            let search_paths = finder.find_length(file.file_length);
            let search_paths = sort_by_target_absolute_path(&file.file_path, &file.export, search_paths);

            // De-duplicate identical files if the file has already been seen.
            'inner: for search_path in search_paths {
                let value = read_bytes(search_path, file.read_length, file.read_start_position)?;

                for (_, result_bytes) in results.iter() {
                    if result_bytes.cmp(&value).is_eq() {
                        continue 'inner;
                    }
                }

                results.push((search_path, value));
            }

            loaded.insert(file_position, results);
        }

        Ok(loaded)
    }
}

impl Solver<OrchestrationPiece, std::io::Error> for MultiplePieceSolver {
    fn solve(&self, mut entry: OrchestrationPiece) -> Result<(), std::io::Error> {
        let result = MultiplePieceSolver::scan(&self.finder, &mut entry)?;

        if result {
            self.writer.write(Some(entry))
        } else {
            self.writer.write(None)
        }
    }

    // Custom balance method to enforce that cheaper pieces to evaluate are always evaulated first, regardless
    // of the thread, making it easier to terminate the program if it gets stuck on high cardinality pieces without
    // losing much data.
    fn balance(source: &mut Vec<OrchestrationPiece>, others: &mut Vec<&mut Vec<OrchestrationPiece>>) {
        let mut collected: Vec<OrchestrationPiece> = source.drain(..).collect();
        for other in others.iter_mut() {
            collected.extend(other.drain(..));
        }

        let total_work = collected.len();
        let active_threads = others.len() + 1;

        // Sort
        collected.sort_by(|left, right| {
            let left_count = left.files.len();
            let right_count = right.files.len();

            left_count.cmp(&right_count)
        });

        // Balance
        'outer: loop {
            if collected.len() == 0 {
                break 'outer;
            }

            source.push(collected.pop().unwrap());

            for other in others.iter_mut() {       
                if collected.len() == 0 {
                    break 'outer;
                }

                other.push(collected.pop().unwrap());
            }
        }

        // Debugging
        let mut counted_work = source.len();
        let mut min_work_per_worker = source.len();
        for other in others.iter_mut() {
            counted_work += other.len();
            min_work_per_worker = min(min_work_per_worker, other.len())
        }

        println!("Rebalanced {} items across {} workers with at-minimum {} per worker; lost {}", total_work, active_threads, min_work_per_worker, total_work - counted_work);
    }
}