use std::{cmp::min, collections::HashMap, path::PathBuf, sync::Arc};

use sha1::{Digest, Sha1};

use crate::{finder::{read_bytes, LengthFileFinder}, orchestrator::OrchestratorPiece, torrent::Piece, writer::PieceWriter};

use super::{PieceMatchResult, Solver};

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
        piece: &Piece,
    ) -> Result<Option<PieceMatchResult>, std::io::Error> {
        // Check if we have at-minimum 1 match that can be made.
        for file in piece.files.iter() {
            if finder.find_length(file.file_length).len() == 0 {
                return Ok(None);
            }
        }

        // Start scanning
        let mut paths: Vec<&PathBuf> = Vec::with_capacity(piece.files.len());
        let mut bytes: Vec<u8> = Vec::with_capacity(piece.length as usize);
        let loaded = MultiplePieceSolver::preload(piece, finder)?;

        if MultiplePieceSolver::scan_internal(&mut paths, &mut bytes, &loaded, piece)? {
            let paths: Vec<PathBuf> = paths.into_iter().cloned().collect();

            return Ok(Some(PieceMatchResult {
                bytes,
                paths,
            }));
        }

        Ok(None)
    }

    fn scan_internal<'a>(
        paths: &mut Vec<&'a PathBuf>,
        buffer: &mut Vec<u8>,
        finder: &HashMap<usize, Vec<(&'a PathBuf, Vec<u8>)>>,
        piece: &Piece
    ) -> Result<bool, std::io::Error> {
        let entries = finder.get(&paths.len()).unwrap();

        for (path, read_buffer) in entries {
            let previous_buffer_length = buffer.len();
            buffer.extend(read_buffer);
            paths.push(*path);

            let valid = if paths.len() == piece.files.len() {
                let hash = Sha1::digest(&buffer);
                piece.hash.as_slice().cmp(&hash).is_eq()
            } else {
                MultiplePieceSolver::scan_internal(paths, buffer, finder, piece)?
            };

            if valid {
                return Ok(valid);
            }

            paths.pop();
            buffer.truncate(previous_buffer_length);
        }

        Ok(false)
    }

    fn preload<'a>(piece: &Piece, finder: &'a LengthFileFinder) -> Result<HashMap<usize, Vec<(&'a PathBuf, Vec<u8>)>>, std::io::Error> {
        let mut loaded = HashMap::new();

        for (file_position, file) in piece.files.iter().enumerate() {

            let mut results: Vec<(&'a PathBuf, Vec<u8>)> = Vec::new();
            let entries = finder.find_length(file.file_length);

            let mut entries: Vec<&'a PathBuf> = entries.iter().collect();

            // Sort by filename so that we check matching filenames first before checking 
            // other random files.
            entries.sort_by(|a, b| {
                let mut a_1 = a.ends_with(&file.file_path) as usize;
                let mut b_1 = b.ends_with(&file.file_path) as usize;

                if let Some(source) = file.file_path.file_name() {
                    if let Some(a_filename) = a.file_name() {
                        a_1 += source.cmp(a_filename).is_eq() as usize;
                    }
    
                    if let Some(b_filename) = b.file_name() {
                        b_1 += source.cmp(b_filename).is_eq() as usize;
                    }
                }

                a_1.cmp(&b_1).reverse()
            });

            // De-duplicate identical files if the file has already been seen.
            'inner: for entry in entries {
                let value = read_bytes(entry, file.read_length, file.read_start_position)?;

                for (_, result_bytes) in results.iter() {
                    if result_bytes.cmp(&value).is_eq() {
                        continue 'inner;
                    }
                }

                results.push((entry, value));
            }

            loaded.insert(file_position, results);
        }

        Ok(loaded)
    }
}

impl Solver<OrchestratorPiece, std::io::Error> for MultiplePieceSolver {
    fn solve(&self, mut work: OrchestratorPiece) -> Result<(), std::io::Error> {
        work.result = MultiplePieceSolver::scan(&self.finder, &work.piece)?;
        self.writer.write(work)
    }

    // Custom balance method to enforce that cheaper pieces to evaluate are always evaulated first, regardless
    // of the thread, making it easier to terminate the program if it gets stuck on high cardinality pieces without
    // losing much data.
    fn balance(source: &mut Vec<OrchestratorPiece>, others: &mut Vec<&mut Vec<OrchestratorPiece>>) {
        let mut collected: Vec<OrchestratorPiece> = source.drain(..).collect();

        let total_work = collected.len();
        let active_threads = others.len() + 1;

        // Sort
        collected.sort_by(|left, right| {
            let left_count = left.piece.files.len();
            let right_count = right.piece.files.len();

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