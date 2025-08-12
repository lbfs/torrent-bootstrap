use std::{fs::File, io::{Read, Seek, SeekFrom}, path::Path, sync::{atomic::{AtomicBool, Ordering}, mpsc::SyncSender, Arc, Mutex}};
use sha1::{digest::core_api::CoreWrapper, Digest, Sha1, Sha1Core};

use crate::{filesystem::FrozenPathInterner, metadata::{TorrentFileEntry, TorrentPieceEntry, TorrentProcessState}, solver::choices::{ChoiceConsumer, ChoiceGenerator}};

pub type PreloadCache = Vec<Vec<(Option<usize>, Vec<u8>)>>;

pub struct PieceUpdate {
    pub piece_id: usize,
    pub found: bool,
    pub fault: bool,
    pub output_bytes: Option<Vec<u8>>,
    pub output_paths: Option<Vec<Option<usize>>>
}

pub struct SolverMetadata {
    pub torrent_files: Vec<TorrentFileEntry>,
    pub torrent_pieces: Vec<TorrentPieceEntry>,
    pub path_interner: FrozenPathInterner,
    pub counter: Mutex<TorrentProcessState>
}

pub struct TaskState {
    piece_id: usize,
    solver_metadata: Arc<SolverMetadata>,
    preloaded: Option<Arc<PreloadCache>>,
    completed: AtomicBool
}

pub struct Task {
    solver_metadata: Arc<SolverMetadata>,
    piece_id: usize,
    task_state: Option<Arc<TaskState>>,
    initialized: Option<ChoiceGenerator>,
    target_split: usize
}

impl Task {
    pub fn new(piece_id: usize, solver_metadata: Arc<SolverMetadata>, target_split: usize) -> Task {
        Task {
            solver_metadata: solver_metadata.clone(),
            task_state: None,
            piece_id,
            initialized: None,
            target_split
        }
    }

    pub fn take(&mut self, consumer: &mut ChoiceConsumer) -> Option<Arc<TaskState>> {
        if let None = self.initialized {
            let mut choice_generator = ChoiceGenerator::empty();
            let mut preloaded: Option<Arc<PreloadCache>> = None;

            let piece = &self.solver_metadata
                .torrent_pieces[self.piece_id];

            let mut choices = piece.total_choices.clone();

            if piece.files.len() > 1 {
                let loaded = self.preload().unwrap();
                for index in 0..loaded.len() {
                    choices[index] = loaded[index].len();
                }
                preloaded = Some(Arc::new(loaded));
            }

            choice_generator.reset_from(&choices, self.target_split);

            self.task_state = Some(Arc::new(TaskState {
                solver_metadata: self.solver_metadata.clone(),
                piece_id: self.piece_id,
                preloaded,
                completed: AtomicBool::new(false)
            }));
            self.initialized = Some(choice_generator);
        }

        let generator = self.initialized.as_mut().unwrap();
        let task_state = self.task_state.as_ref().unwrap();

        if generator.ended() {
            return None;
        }

        if task_state.completed.load(Ordering::Relaxed) {
            return None;
        }

        generator.get(consumer);
        generator.next();

        Some(task_state.clone())
    }

    fn preload(&self) -> std::io::Result<PreloadCache> {
        let piece = &self
            .solver_metadata
            .torrent_pieces[self.piece_id];

        let mut loaded = Vec::with_capacity(piece.files.len());

        for piece_file in piece.files.iter() {
            let torrent_file = &self
                .solver_metadata
                .torrent_files[piece_file.file_id];

            let mut results: Vec<(Option<usize>, Vec<u8>)> = Vec::new();

            if torrent_file.padding { 
                results.push((None, vec![0; piece_file.read_length as usize]));
            } else if let Some(searches) = torrent_file.searches.as_ref() {
                // De-duplicate identical files if the file has already been seen.
                'inner: for search_path_handle in searches {
                    let search_path = self.solver_metadata.path_interner.get(*search_path_handle);
                    let value = Self::read_bytes(search_path, piece_file.read_length, piece_file.read_start_position)?;
        
                    for (_, result_bytes) in results.iter() {
                        if result_bytes.cmp(&value).is_eq() {
                            continue 'inner;
                        }
                    }
        
                    results.push((Some(*search_path_handle), value));
                }
            } else {
                return Ok(Vec::new());
            }

            loaded.push(results);
        }

        Ok(loaded)
    }

    fn read_bytes(
        path: &Path,
        read_length: u64,
        read_start_position: u64
    ) -> Result<Vec<u8>, std::io::Error> {
        let mut handle = File::open(path)?;
        let mut read_bytes = Vec::with_capacity(read_length as usize);

        handle.seek(SeekFrom::Start(read_start_position))?;
        handle.take(read_length)
            .read_to_end(&mut read_bytes)?;

        Ok(read_bytes)
    }
}

pub struct Solver {
    output_bytes: Vec<u8>,
    output_paths: Vec<Option<usize>>,
    hasher: CoreWrapper<Sha1Core>
}

impl Solver {
    pub fn new() -> Solver {
        Solver {
            output_bytes: Vec::new(),
            output_paths: Vec::new(),
            hasher: Sha1::new()
        }
    }

    pub fn solve(&mut self, choices: &mut ChoiceConsumer, task_state: &TaskState, writer: &mut SyncSender<PieceUpdate>) {
        let solver_metadata = task_state.solver_metadata.as_ref();
        let torrent_files = &solver_metadata.torrent_files;
        let path_interner = &solver_metadata.path_interner;
        let piece = &solver_metadata.torrent_pieces[task_state.piece_id];

        let piece_hash = piece.hash.as_slice();
        let completed = &task_state.completed;

        'choices: while !choices.ended() {
            self.output_bytes.clear();
            self.output_paths.clear();
            self.hasher.reset();
            
            if completed.load(Ordering::Relaxed) {
                break 'choices;
            }

            for file_index in 0..choices.len() {
                let choice = choices.get(file_index).get();

                let piece_file_entry = &piece.files[file_index];
                let file_entry = &torrent_files[piece_file_entry.file_id];

                if let Some(preloaded) = &task_state.preloaded {  
                    self.output_bytes.extend_from_slice(&preloaded[file_index][choice].1);
                    self.output_paths.push(preloaded[file_index][choice].0);
                } else if file_entry.padding {
                    self.output_bytes.extend(vec![0; file_entry.file_length as usize]); // TODO: OPTIMIZE
                    self.output_paths.push(None);
                } else {
                    let path_id = file_entry.searches.as_ref().unwrap()[choice];
                    let path = path_interner.get(path_id);

                    let mut file_handle = File::open(path).unwrap(); // TODO: FIX ME
                    file_handle.seek(SeekFrom::Start(piece_file_entry.read_start_position)).unwrap();
                    file_handle.take(piece_file_entry.read_length)
                        .read_to_end(&mut self.output_bytes).unwrap();

                    self.output_paths.push(Some(path_id));
                }
            }

            self.hasher.update(&self.output_bytes);
            let hash = self.hasher.finalize_reset();

            if piece_hash.cmp(&hash).is_eq() {
                let swapped = completed.compare_exchange(
                    false, true, Ordering::AcqRel, Ordering::Relaxed
                );

                if let Ok(false) = swapped {

                    let piece_update = PieceUpdate {
                        piece_id: piece.piece_id,
                        found: true,
                        fault: false,
                        output_bytes: Some(self.output_bytes.clone()),
                        output_paths: Some(self.output_paths.clone())
                    };

                    writer
                        .send(piece_update)
                        .expect("Should never fail to write.");

                    break 'choices;
                }
            }

            choices.next();
        }
    }
}
