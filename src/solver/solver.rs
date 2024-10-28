use std::ops::DerefMut;

use crate::{finder::LengthFileFinder, orchestrator::OrchestrationPiece, writer::PieceWriter};

use super::{multiple, single};

pub trait Solver<T, K>
{
    fn solve(item: T, context: &K);
    fn balance(entries: &mut [impl DerefMut<Target=Vec<T>>]);
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
                if file.is_padding_file { continue; }
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
    
    fn balance(entries: &mut [impl DerefMut<Target=Vec<OrchestrationPiece>>]) {
        // Take items out of each thread
        let capacity = entries
            .iter()
            .map(|value| value.len())
            .sum::<usize>();


        // Split items based on if they have multiple files or only one file.
        let mut multiple = Vec::new();
        let mut singles = Vec::new();

        for entry in entries.iter_mut() {
            while entry.len() > 0 {
                let last = entry.last().unwrap();
                if last.files.len() == 1 {
                    singles.push(entry.pop().unwrap());
                } else if last.files.len() > 1 {
                    multiple.push(entry.pop().unwrap());
                }
            }
        }

        // Sort single files by export name to avoid writing to same file from multiple threads
        singles.sort_by(|left, right| {
            let left_name = &left.files.first().unwrap().export;
            let right_name = &right.files.first().unwrap().export;

            left_name.cmp(&right_name)
        });

        // Sort multiple files by complexity
        multiple.sort_by(|left, right| {
            left.files.len().cmp(&right.files.len())
        });

        // Put worst time complexity pieces last (by putting them first)
        let mut index = 0;
        while multiple.len() > 0 {
            entries[index].push(multiple.pop().unwrap());
    
            index += 1;
            index *= (index < entries.len()) as usize
        }

        // Put files with identical export files onto the same thread.
        let mut index = 0;
        loop {
            if singles.len() == 0 { break; }
            let last = singles.pop().unwrap();

            while singles.len() > 0 {

                let next = singles.last().unwrap();
                let next_file_name = &next.files.first().unwrap().export;

                if !next_file_name.cmp(&last.files.first().unwrap().export).is_eq() {
                    break;
                }

                entries[index].push(singles.pop().unwrap());
            }

            entries[index].push(last);

            index += 1;
            index *= (index < entries.len()) as usize
        }

        let final_capacity = entries
            .iter()
            .map(|value| value.len())
            .sum::<usize>();

        if final_capacity != capacity {
            panic!("Balancing method has a problem. Input and output sizes are not the same!");
        }
    }
}
