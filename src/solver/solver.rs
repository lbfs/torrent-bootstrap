use std::{collections::HashMap, ops::DerefMut, path::PathBuf, sync::{Arc, Mutex}};
use crate::{finder::TorrentProcessState, get_sha1_hexdigest, orchestrator::OrchestrationPiece, writer::FileWriter};
use super::{multiple, single};

#[derive(Clone)]
pub struct PieceSolver {
    output_paths: Vec<Option<Arc<PathBuf>>>,
    output_bytes: Vec<u8>,
    writer: Arc<FileWriter>,
    global_state: Arc<Mutex<TorrentProcessState>>
}

impl PieceSolver {
    pub fn new(writer: FileWriter, global_state: TorrentProcessState, pieces: &[OrchestrationPiece]) -> PieceSolver {
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

        PieceSolver {
            output_paths: Vec::with_capacity(max_files),
            output_bytes: Vec::with_capacity(max_piece_length as usize),
            writer: Arc::new(writer),
            global_state: Arc::new(Mutex::new(global_state))
        }
    }

    pub fn solve(&mut self, piece: OrchestrationPiece) { 
        let result = self.solve_internal(&piece);

        let mut success: usize = 0;
        let mut failed: usize = 0;
        let mut fault: usize = 0;

        match result {
            Ok(found) => {
                success += found as usize;
                failed += !found as usize;
            },
            Err(err) => {
                fault = 1;
                eprintln!("Unable to solve piece {} due to following error: {:#?}", get_sha1_hexdigest(&piece.hash), err);
            },
        }

        // Update the file local processing state
        for file in &piece.files {
            let mut processing_state = file.metadata.processing_state
                .lock()
                .expect("Should always lock the processing state in solver thread.");

            processing_state.failed_pieces += failed;
            processing_state.success_pieces += success;
            processing_state.fault_pieces += fault;
        }

        // Update the global processing state
        let mut state = self.global_state.lock().unwrap();
        state.success_pieces += success;
        state.failed_pieces += failed;
        state.fault_pieces += fault;

        let availability = (state.success_pieces as f64 / state.total_pieces as f64) * 100_f64;
        let scanned = ((state.success_pieces + state.failed_pieces + state.fault_pieces) as f64 / state.total_pieces as f64) * 100_f64;
    
        println!("Availability: {:.03}%, Scanned: {:.03}% - Success: {}, Failed: {}, Faulted: {}, Total: {}", 
            availability, scanned, state.success_pieces, state.failed_pieces, state.fault_pieces, state.total_pieces);
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
            single::scan(piece, &mut self.output_paths, &mut self.output_bytes)?
        } else {
            multiple::scan(piece, &mut self.output_paths, &mut self.output_bytes)?
        };
        
        if found {
            self.writer.write(piece, self.output_paths.clone(), self.output_bytes.clone())?;
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
