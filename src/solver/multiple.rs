use std::{cmp::min, sync::Arc};

use crate::{finder::LengthFileFinder, matcher::MultiFilePieceMatcher, orchestrator::OrchestratorPiece, writer::PieceWriter};

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
}

impl Solver<OrchestratorPiece, std::io::Error> for MultiplePieceSolver {
    fn solve(&self, mut work: OrchestratorPiece) -> Result<(), std::io::Error> {
        work.result = MultiFilePieceMatcher::scan(&self.finder, &work.piece)?;
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