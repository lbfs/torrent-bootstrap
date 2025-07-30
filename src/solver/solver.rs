use std::{collections::HashMap, fs::File, ops::DerefMut, path::PathBuf, sync::{mpsc::SyncSender, Arc}};
use hashlru::Cache;

use crate::{finder::TorrentMetadataEntry, get_sha1_hexdigest, orchestrator::OrchestrationPiece};
use super::{multiple, single};

pub struct PieceUpdate {
    pub piece: OrchestrationPiece,
    pub found: bool,
    pub fault: bool,
    pub output_bytes: Option<Vec<u8>>,
    pub output_paths: Option<Vec<Option<Arc<PathBuf>>>>
}

pub struct PieceSolver {
    output_paths: Vec<Option<Arc<PathBuf>>>,
    output_bytes: Vec<u8>,
    sender: SyncSender<PieceUpdate>,
    cache: Cache<Arc<PathBuf>, File>,
    max_handles: usize
}

impl Clone for PieceSolver {
    fn clone(&self) -> Self {
        Self { 
            output_paths: self.output_paths.clone(), 
            output_bytes: self.output_bytes.clone(), 
            sender: self.sender.clone(), 
            cache: Cache::new(self.max_handles),
            max_handles: self.max_handles.clone() 
        }
    }
}

impl PieceSolver {
    pub fn new(sender: SyncSender<PieceUpdate>, metadata: &[Arc<TorrentMetadataEntry>], pieces: &[OrchestrationPiece]) -> PieceSolver {
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

        let max_handles = metadata
            .iter()
            .map(|entry| {
                if let Some(searches) = &entry.searches {
                    searches.len()
                } else {
                    0
                }
            })
            .max()
            .unwrap_or_else(|| 1);
        let max_handles = std::cmp::max(1, max_handles);
        println!("Initializing PieceSolver with maximum {} open file handles.", max_handles);

        PieceSolver {
            output_paths: Vec::with_capacity(max_files),
            output_bytes: Vec::with_capacity(max_piece_length as usize),
            sender,
            cache: Cache::new(max_handles),
            max_handles: max_handles
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
            single::scan(piece, &mut self.output_paths, &mut self.output_bytes, &mut self.cache)?
        } else {
            self.cache.clear();
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

    // Balance the work by | Multiples -> Most Complex to Least Complex |
    let mut singles: HashMap<usize, Vec<OrchestrationPiece>> = HashMap::new();
    let mut multiples = Vec::new();

    for entry in entries.into_iter() {
        if entry.files.len() == 1 {
            let items: &mut _ = singles.entry(entry.files[0].metadata.id).or_default();
            items.push(entry);
        } else {
            multiples.push(entry);
        }
    }

    // Sort the pieces by their start positions
    for (_, pieces) in singles.iter_mut() {
        pieces.sort_by(|left, right| {
            let left_value = left.files.first().unwrap().read_start_position;
            let right_value = right.files.first().unwrap().read_start_position;
            left_value.cmp(&right_value)
        })
    }

    multiples.sort_by(|left, right| left.files.len().cmp(&right.files.len()));
    multiples.reverse();

    // Evenly distribute all of the complex pieces (>1 file)
    let mut thread_index = 0;
    while let Some(element) = multiples.pop() {
        thread_entries[thread_index].push(element);

        thread_index += 1;
        thread_index *= (thread_index < thread_entries.len()) as usize;
    }

    // Distribute the pieces so that each thread works on the same files always
    // This is to maximize hitting the cache for a specific file.
    let mut thread_index = 0;
    for (_, pieces) in singles.into_iter() {
        thread_entries[thread_index].extend(pieces);

        thread_index += 1;
        thread_index *= (thread_index < thread_entries.len()) as usize;
    }
}
