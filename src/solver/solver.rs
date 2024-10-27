use crate::{finder::LengthFileFinder, orchestrator::OrchestrationPiece, writer::PieceWriter};

use super::{multiple, single};

pub trait Solver<T, K>
{
    fn solve(item: T, context: &K);
}

pub struct PieceSolverContext {
    pub finder: LengthFileFinder,
    pub writer: PieceWriter
}

impl PieceSolverContext {
    pub fn new(finder: LengthFileFinder, writer: PieceWriter) -> PieceSolverContext {
        PieceSolverContext {
            finder,
            writer
        }
    }
}

pub struct PieceSolver;

impl Solver<OrchestrationPiece, PieceSolverContext> for PieceSolver {
    fn solve(mut item: OrchestrationPiece, context: &PieceSolverContext) { 
        let res: Result<(), std::io::Error> = (|| {
            let mut is_rejected = false;
            for file in item.files.iter() {
                if context.finder.find_length(file.file_length).len() == 0 {
                    is_rejected = true;
                    break;
                }
            }

            let found = if is_rejected {
                false
            } else if item.files.len() == 1 {
                single::scan(&context.finder, &mut item)?
            } else {
                multiple::scan(&context.finder, &mut item)?
            };
    
            if found {
                context.writer.write(Some(item))?
            } else {
                context.writer.write(None)?
            }
            
            Ok(())
        })();

        if let Err(err) = res {
            eprintln!("Unable to solve piece due to following error: {:#?}", err);
        }
    }
}
