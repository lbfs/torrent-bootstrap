use std::sync::Arc;

use sha1::{Digest, Sha1};

use crate::{finder::{read_bytes, sort_by_target_absolute_path, LengthFileFinder}, orchestrator::OrchestrationPiece, writer::PieceWriter};

use super::Solver;

#[derive(Clone)]
pub struct SinglePieceSolver {
    writer: Arc<PieceWriter>,
    finder: Arc<LengthFileFinder>
}

impl SinglePieceSolver {
    pub fn new(writer: Arc<PieceWriter>, finder: Arc<LengthFileFinder>) -> SinglePieceSolver {
        SinglePieceSolver {
            writer,
            finder
        }
    }
}

impl Solver<Vec<OrchestrationPiece>, std::io::Error> for SinglePieceSolver {
    fn solve(&self, mut work: Vec<OrchestrationPiece>) -> Result<(), std::io::Error> {
        if work.len() == 0 {
            return Ok(());
        }

        let first = work.first().unwrap();
        let first_file = first.files.first().unwrap();

        let search_paths = self.finder.find_length(first_file.file_length);
        let search_paths = sort_by_target_absolute_path(&first_file.file_path, &first_file.export, search_paths);

        // Evaluate
        for search_path in search_paths {
            let pieces_length = work.len();

            let mut index = 0;
            while index < pieces_length {
                let mut entry = work.remove(0);
                let entry_first_file = entry.files.first_mut().unwrap();

                let bytes = read_bytes(search_path, entry_first_file.read_length, entry_first_file.read_start_position)?;
                let hash = Sha1::digest(&bytes);

                if entry.hash.as_slice().cmp(&hash).is_eq() {
                    entry_first_file.bytes = Some(bytes.to_vec());
                    entry_first_file.source = Some(search_path.clone());

     
                    self.writer.write(Some(entry))?;
                } else {
                    work.push(entry);
                }

                index += 1;
            }

            if work.is_empty() {
                break;
            }
        }

        // Emit the failed blocks for accounting purposes
        for _ in work {
            self.writer.write(None)?;
        }

        Ok(())
    }
}

