use std::sync::Arc;

use sha1::{Digest, Sha1};

use crate::{finder::{read_bytes, LengthFileFinder}, matcher::PieceMatchResult, orchestrator::OrchestratorPiece, writer::PieceWriter};

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

impl Solver<(u64, Vec<OrchestratorPiece>), std::io::Error> for SinglePieceSolver {
    fn solve(&self, work: (u64, Vec<OrchestratorPiece>)) -> Result<(), std::io::Error> {
        let (file_length, mut pieces) = work;

        for path in self.finder.find_length(file_length) {
            let pieces_length = pieces.len();

            let mut index = 0;
            while index < pieces_length {
                let mut work = pieces.remove(0);
                let file = work.piece.files.first().unwrap();

                let read_start_position = file.read_start_position;
                let bytes = read_bytes(path, file.read_length, read_start_position)?;
                let hash = Sha1::digest(&bytes);

                if work.piece.hash.as_slice().cmp(&hash).is_eq() {
                    let bytes = bytes.to_vec();
                    let paths = vec![path.clone()];

                    work.result = Some(PieceMatchResult {
                        bytes,
                        paths,
                    });

                    self.writer.write(work)?;
                } else {
                    pieces.push(work);
                }

                index += 1;
            }

            if pieces.is_empty() {
                break;
            }
        }

        // Emit the failed blocks for accounting purposes
        for work in pieces {
            self.writer.write(work)?;
        }

        Ok(())
    }
}

