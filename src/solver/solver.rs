use std::{collections::HashMap, ops::DerefMut, path::PathBuf, sync::{mpsc::SyncSender, Arc}};
use crate::{get_sha1_hexdigest, orchestrator::OrchestrationPiece};
use super::{multiple, single};

pub struct PieceUpdate {
    pub piece: OrchestrationPiece,
    pub found: bool,
    pub fault: bool,
    pub output_bytes: Option<Vec<u8>>,
    pub output_paths: Option<Vec<Option<Arc<PathBuf>>>>
}

#[derive(Clone)]
pub struct PieceSolver {
    output_paths: Vec<Option<Arc<PathBuf>>>,
    output_bytes: Vec<u8>,
    sender: SyncSender<PieceUpdate>,
}

impl PieceSolver {
    pub fn new(sender: SyncSender<PieceUpdate>, pieces: &[OrchestrationPiece]) -> PieceSolver {
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
            sender,
        }
    }

    pub fn solve(&mut self, piece: OrchestrationPiece) {
        let result = self.solve_internal(&piece);
        
        let state_update = match result {
            Ok(found) => {
                let output_paths;
                let output_bytes;

                if found {
                    output_paths = Some(self.output_paths.clone());
                    output_bytes = Some(self.output_bytes.clone());
                } else {
                    output_paths = None;
                    output_bytes = None;
                }

                PieceUpdate {
                    piece,
                    found,
                    fault: false,
                    output_paths,
                    output_bytes
                }
            },
            Err(err) => {
                eprintln!("Unable to solve piece {} due to following error: {:#?}", get_sha1_hexdigest(&piece.hash), err);
                PieceUpdate {
                    piece,
                    found: false,
                    fault: true,
                    output_paths: None,
                    output_bytes: None
                }
            }
        };

        self.sender
            .send(state_update)
            .expect("Sender should only be shut-down after executor service has exited.");
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
