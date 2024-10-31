use std::ops::DerefMut;

use crate::{finder::ExportFileFinder, orchestrator::OrchestrationPiece, writer::PieceWriter};

use super::{multiple, single};

pub trait Solver<T, K>
{
    fn solve(item: T, context: &K);
    fn balance(entries: &mut [impl DerefMut<Target=Vec<T>>]);
}

pub struct PieceSolverContext {
    pub finder: ExportFileFinder,
    pub writer: PieceWriter
}

impl PieceSolverContext {
    pub fn new(finder: ExportFileFinder, writer: PieceWriter) -> PieceSolverContext {
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
                if context.finder.find_length(file.export_index).len() == 0 {
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

        let mut pieces_to_place = Vec::with_capacity(capacity);

        for entry in entries.iter_mut() {
            pieces_to_place.extend(entry.drain(..));
        }

        // Sort pieces from least complex to most complex by file count, and within the file count sort by file path
        pieces_to_place.sort_by(|left, right| {
            let left_files = &left.files;
            let right_files = &right.files;

            let same_number_of_files = left_files.len().cmp(&right_files.len());
            if same_number_of_files.is_eq() { 
                left_files.first().unwrap().export_index.cmp(&right_files.first().unwrap().export_index)
            } else {
                same_number_of_files
            }
        });

        // Place items onto worker queue
        while pieces_to_place.len() > 0 {
            let last = pieces_to_place.last().unwrap();
            let last_file_name = last.files.first().unwrap().export_index;

            let mut thread_id = usize::MAX;
            let mut thread_size = usize::MAX;
    
            // Find thread id with lowest size
            for (entry_index, entry) in entries.iter().enumerate() {
                if entry.len() < thread_size {
                    thread_id = entry_index;
                    thread_size = entry.len();
                }
            }

            while pieces_to_place.len() > 0 {
                let next_file_name = &pieces_to_place.last().unwrap()
                    .files.first().unwrap().export_index;

                if !last_file_name.cmp(next_file_name).is_eq() { break; }

                entries[thread_id].push(pieces_to_place.pop().unwrap());
            }
        }
    }
}
