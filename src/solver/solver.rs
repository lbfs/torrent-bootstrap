use std::{collections::HashMap, ops::DerefMut, path::PathBuf, sync::{Arc, Mutex}};
use crate::{get_sha1_hexdigest, orchestrator::OrchestrationPiece, writer::FileWriter};
use super::{multiple, single};

pub struct PieceMatchResult {
    pub source: Vec<Option<Arc<PathBuf>>>,
    pub bytes: Vec<u8>
}

pub struct PieceState {
    success: usize,
    failed: usize,
    fault: usize,
    total: usize
}

pub struct PieceSolverContext {
    pub writer: FileWriter,
    pub state: Mutex<PieceState>
}

impl PieceSolverContext {
    pub fn new(writer: FileWriter, total_piece_count: usize) -> PieceSolverContext {
        PieceSolverContext {
            writer,
            state: Mutex::new(PieceState { success: 0, failed: 0, fault: 0, total: total_piece_count})
        }
    }
}

pub struct PieceSolver {
    match_result: PieceMatchResult
}

impl PieceSolver {
    pub fn new() -> PieceSolver {
        let match_result = PieceMatchResult {
            source: Vec::new(),
            bytes: Vec::new()
        };

        PieceSolver {
            match_result
        }
    }

    pub fn solve(&mut self, piece: OrchestrationPiece, context: &PieceSolverContext) { 
        let result = self.solve_internal(&piece, context);

        let mut state = context.state.lock().unwrap();

        match result {
            Ok(found) => {
                state.success += found as usize;
                state.failed += !found as usize;
            },
            Err(err) => {
                state.fault += 1;
                eprintln!("Unable to solve piece {} due to following error: {:#?}", get_sha1_hexdigest(&piece.hash), err);
            },
        }

        let availability = (state.success as f64 / state.total as f64) * 100_f64;
        let scanned = ((state.success + state.failed + state.fault) as f64 / state.total as f64) * 100_f64;
    
        println!("Availability: {:.03}%, Scanned: {:.03}% - Success: {}, Failed: {}, Faulted: {}, Total: {}", 
            availability, scanned, state.success, state.failed, state.fault, state.total);
    }

    fn solve_internal(&mut self, piece: &OrchestrationPiece, context: &PieceSolverContext) -> std::io::Result<bool> {
        let mut is_rejected = false;
        for file in piece.files.iter() {
            if file.metadata.is_padding_file { continue; }
            if file.metadata.searches.is_none() {
                is_rejected = true;
                break;
            }
        }
    
        let found = if is_rejected {
            false
        } else if piece.files.len() == 1 {
            single::scan(piece, &mut self.match_result)?
        } else {
            multiple::scan(piece, &mut self.match_result)?
        };
        
        if found {
            context.writer.write(piece, &self.match_result)?;
        }

        Ok(found)
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
