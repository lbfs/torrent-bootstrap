use std::{ops::DerefMut, path::PathBuf, sync::Mutex};

use crate::{finder::FileFinder, orchestrator::OrchestrationPiece, writer::{self}};

use super::{multiple, single};

pub trait Solver<T, K>
{
    fn solve(item: T, context: &K);
    fn balance(entries: &mut [impl DerefMut<Target=Vec<T>>]);
}

pub struct PieceMatchResult<'a> {
    pub source: Vec<Option<&'a PathBuf>>,
    pub bytes: Vec<u8>
}

pub struct PieceState {
    success_pieces: usize,
    failed_pieces: usize,
    total_piece_count: usize
}

pub struct PieceSolverContext {
    pub finder: FileFinder,
    pub state: Mutex<PieceState>
}

impl PieceSolverContext {
    pub fn new(finder: FileFinder, total_piece_count: usize) -> PieceSolverContext {
        PieceSolverContext {
            finder,
            state: Mutex::new(PieceState { success_pieces: 0, failed_pieces: 0, total_piece_count: total_piece_count})
        }
    }
}

pub struct PieceSolver;

fn process(item: &OrchestrationPiece, context: &PieceSolverContext) -> std::io::Result<bool> {
    let mut is_rejected = false;
    for file in item.files.iter() {
        if file.is_padding_file { continue; }
        if context.finder.find_searches(file.export_index).len() == 0 {
            is_rejected = true;
            break;
        }
    }

    let found = if is_rejected {
        None
    } else if item.files.len() == 1 {
        single::scan(&context.finder, &item)?
    } else {
        multiple::scan(&context.finder, &item)?
    };

    let result = if let Some(found) = found {
        let mut state = context.state.lock().unwrap();

        writer::write(&item, &found, &context.finder)?;

        state.success_pieces += 1;

        let availability = (state.success_pieces as f64 / state.total_piece_count as f64) * 100 as f64;
        let scanned = ((state.success_pieces + state.failed_pieces) as f64 / state.total_piece_count as f64) * 100 as f64;

        println!("{} of {} total pieces found - scanned: {:.02}% - availability: {:.02}%", 
            state.success_pieces, 
            state.total_piece_count,
            scanned,
            availability
        );

        true
    } else {
        let mut state = context.state.lock().unwrap();

        state.failed_pieces += 1;

        let availability = (state.success_pieces as f64 / state.total_piece_count as f64) * 100 as f64;
        let scanned = ((state.success_pieces + state.failed_pieces) as f64 / state.total_piece_count as f64) * 100 as f64;

        println!("{} of {} total pieces found - scanned: {:.02}% - availability: {:.02}%", 
            state.success_pieces, 
            state.total_piece_count,
            scanned,
            availability
        );

        false
    };

    Ok(result)
}

impl Solver<OrchestrationPiece, PieceSolverContext> for PieceSolver {
    fn solve(item: OrchestrationPiece, context: &PieceSolverContext) { 
        let res = process(&item, context);
        
        if let Err(err) = res {
            let mut state = context.state.lock().unwrap();

            state.failed_pieces += 1;

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
