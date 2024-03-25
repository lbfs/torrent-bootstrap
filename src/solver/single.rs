use std::sync::Arc;

use sha1::{Digest, Sha1};

use crate::{finder::{read_bytes, sort_by_target_absolute_path, LengthFileFinder},orchestrator::OrchestratorPiece, writer::PieceWriter};

use super::{PieceMatchResult, Solver};

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

impl Solver<Vec<OrchestratorPiece>, std::io::Error> for SinglePieceSolver {
    // Requires that all files have the same length and same target export path
    // TODO: Add proper validation checks
    fn solve(&self, mut work: Vec<OrchestratorPiece>) -> Result<(), std::io::Error> {
        if work.len() == 0 {
            return Ok(());
        }

        // Get first entry and use it to determine the current file length
        let file_length = work.first().unwrap().piece.files.first().unwrap().file_length;
        let file_path = &work.first().unwrap().piece.files.first().unwrap().file_path;
        let export_path = work.first().unwrap().export_paths.first().unwrap();

        let entries = self.finder.find_length(file_length);
        let entries = sort_by_target_absolute_path(file_path, export_path, entries);

        // Evaluate
        for path in entries {
            let pieces_length = work.len();

            let mut index = 0;
            while index < pieces_length {
                let mut entry = work.remove(0);
                let file = entry.piece.files.first().unwrap();

                let bytes = read_bytes(path, file.read_length, file.read_start_position)?;
                let hash = Sha1::digest(&bytes);

                if entry.piece.hash.as_slice().cmp(&hash).is_eq() {
                    let bytes = bytes.to_vec();
                    let paths = vec![path.clone()];

                    entry.result = Some(PieceMatchResult {
                        bytes,
                        paths,
                    });

                    self.writer.write(entry)?;
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
        for work in work {
            self.writer.write(work)?;
        }

        Ok(())
    }
}

