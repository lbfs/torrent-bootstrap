use std::{collections::HashMap, ops::DerefMut, path::PathBuf, sync::Mutex};

use crate::{orchestrator::OrchestrationPiece, writer::FileWriter};

use super::{multiple, single};

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
    pub writer: FileWriter,
    pub state: Mutex<PieceState>
}

impl PieceSolverContext {
    pub fn new(writer: FileWriter, total_piece_count: usize) -> PieceSolverContext {
        PieceSolverContext {
            writer,
            state: Mutex::new(PieceState { success_pieces: 0, failed_pieces: 0, total_piece_count})
        }
    }
}

fn solve_internal(item: &OrchestrationPiece, context: &PieceSolverContext) -> std::io::Result<bool> {
    let mut is_rejected = false;
    for file in item.files.iter() {
        if file.is_padding_file { continue; }
        if file.metadata.searches.is_none() {
            is_rejected = true;
            break;
        }
    }

    let found = if is_rejected {
        None
    } else if item.files.len() == 1 {
        single::scan( item)?
    } else {
        multiple::scan(item)?
    };
    
    if let Some(found) = &found {
        context.writer.write(item, found)?;
    }

    let mut state = context.state.lock().unwrap();

    state.success_pieces += found.is_some() as usize;
    state.failed_pieces += found.is_none() as usize;

    let availability = (state.success_pieces as f64 / state.total_piece_count as f64) * 100_f64;
    let scanned = ((state.success_pieces + state.failed_pieces) as f64 / state.total_piece_count as f64) * 100_f64;

    println!("{} of {} total pieces found - scanned: {:.02}% - availability: {:.02}%", 
        state.success_pieces, 
        state.total_piece_count,
        scanned,
        availability
    );

    Ok(found.is_some())
}

pub fn solve(item: OrchestrationPiece, context: &PieceSolverContext) { 
    let res = solve_internal(&item, context);
    
    if let Err(err) = res {
        let mut state = context.state.lock().unwrap();

        state.failed_pieces += 1;

        eprintln!("Unable to solve piece due to following error: {:#?}", err);
    }
}

pub fn balance(thread_entries: &mut [impl DerefMut<Target=Vec<OrchestrationPiece>>]) {
    // Take work out of threads
    let capacity = thread_entries
        .iter()
        .map(|value| value.len())
        .sum::<usize>();

    let mut entries = Vec::with_capacity(capacity);
    for entry in thread_entries.iter_mut() {
        entries.extend(entry.drain(..));
    }

    // Balance the work by | Multiples -> Most Complex to Least Complex | | Singles -> 1,2,3,4,5,1,2,3,4,5 | 
    let mut singles: HashMap<usize, Vec<OrchestrationPiece>> = HashMap::new();
    let mut result = Vec::new();

    for entry in entries.into_iter() {
        if entry.files.len() == 1 {
            let items: &mut _ = singles.entry(entry.files[0].metadata.id).or_default();
            items.push(entry);
        } else {
            result.push(entry);
        }
    }

    result.sort_by(|left, right| left.files.len().cmp(&right.files.len()));
    result.reverse();

    let mut remaining = true;
    while remaining {
        let mut found = false;

        for (_, value) in singles.iter_mut() {
            if !value.is_empty() {
                result.push(value.pop().unwrap());
                found = true;
            }
        }

        remaining = found;
    }

    result.reverse();

    // Place them back on the threads evenly as possible
    let mut thread_index = 0;
    while let Some(element) = result.pop() {
        thread_entries[thread_index].push(element);

        thread_index += 1;
        thread_index *= (thread_index < thread_entries.len()) as usize;
    }
}
