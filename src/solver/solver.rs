use std::{collections::HashMap, ops::DerefMut, path::PathBuf, sync::{Arc, Mutex}};
use crate::{get_sha1_hexdigest, orchestrator::OrchestrationPiece, writer::FileWriter};
use super::{multiple, single};

#[derive(Clone)]
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

#[derive(Clone)]
pub struct PieceSolver {
    match_result: PieceMatchResult,
    writer: Arc<FileWriter>,
    state: Arc<Mutex<PieceState>>
}

impl PieceSolver {
    pub fn new(writer: FileWriter, total_pieces: usize, pieces: &[OrchestrationPiece]) -> PieceSolver {
        let max_files = pieces
            .iter()
            .map(|piece| piece.files.len())
            .max()
            .unwrap_or(0);

        let max_piece_length = pieces
            .iter()
            .map(|piece| piece.files.iter().map(|file| file.read_length).sum::<u64>())
            .max()
            .unwrap_or(0);

        let match_result = PieceMatchResult {
            source: Vec::with_capacity(max_files),
            bytes: Vec::with_capacity(max_piece_length as usize)
        };

        PieceSolver {
            match_result,
            writer: Arc::new(writer),
            state: Arc::new(Mutex::new(PieceState { success: 0, failed: 0, fault: 0, total: total_pieces }))
        }
    }

    pub fn solve(&mut self, piece: OrchestrationPiece) { 
        let result = self.solve_internal(&piece);

        let mut state = self.state.lock().unwrap();

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

    fn solve_internal(&mut self, piece: &OrchestrationPiece) -> std::io::Result<bool> {
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
            self.writer.write(piece, &self.match_result)?;
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
