use std::{sync::{Arc, Mutex, MutexGuard}, thread::{self, JoinHandle}};

use crate::orchestrator::OrchestrationPiece;

use super::{balance, PieceSolver};

struct ExecutionState<T> {
    active_threads: usize,
    locks: Vec<Arc<Mutex<Vec<T>>>>
}

pub fn run(items: Vec<OrchestrationPiece>, solver: PieceSolver, thread_count: usize) {
    if items.is_empty() {
        return;
    }

    let thread_count = std::cmp::max(std::cmp::min(items.len(), thread_count), 1);

    // Setup work for balancing
    let mut entries: Vec<Vec<_>> = Vec::with_capacity(thread_count);
    entries.push(items);
    for _ in 1..thread_count {
        entries.push(Vec::new())
    }

    balance(&mut (entries.iter_mut().collect::<Vec<_>>()));

    // Setup state and start
    let locks: Vec<_> = entries
        .into_iter()
        .map(|value| Arc::new(Mutex::new(value)))
        .collect();

    let execution_state = Arc::new(Mutex::new(ExecutionState {
        active_threads: locks.len(),
        locks: locks.clone()
    }));

    // Start up the workers
    let mut handles: Vec<JoinHandle<()>> = Vec::with_capacity(thread_count);
    for (thread_id, local) in locks.into_iter().enumerate() {
        let execution_state = execution_state.clone();
        let solver = solver.clone();

        let handle = thread::spawn(move || {
            run_internal(solver, thread_id, local, execution_state);
        });

        handles.push(handle);
    }

    for handle in handles.into_iter() {
        handle.join()
            .expect("Encountered panic while joining on thread handle.");
    }
}

fn run_internal(mut solver: PieceSolver, thread_id: usize, local: Arc<Mutex<Vec<OrchestrationPiece>>>, execution_state: Arc<Mutex<ExecutionState<OrchestrationPiece>>>) {
    'outer: loop {
        let found = {
            let guard = local.try_lock();
            
            if let Ok(mut guard) = guard {
                guard.pop()
            } else {
                None
            }
        };

        match found {
            Some(work) => {
                solver.solve(work);
            },
            None => {
                let mut state = execution_state.lock().unwrap();

                // Exit the thread if we are terminated.
                if thread_id >= state.active_threads {
                    break 'outer;
                }

                // If multiple threads were waiting for work, we need to abort the thread from
                // performing a work re-balance, as it was just done.
                let guard = local.lock().unwrap();
                if guard.len() > 0 {
                    continue 'outer;
                }

                // Store all the thread guards in the exact order they are in based on thread id
                let mut thread_guards: Vec<MutexGuard<Vec<_>>> = Vec::with_capacity(state.active_threads);
                for thread_index in 0..thread_id {
                    let guard = state.locks[thread_index]
                        .lock()
                        .unwrap();

                    thread_guards.push(guard);
                }

                thread_guards.push(guard);

                for thread_index in thread_id + 1..state.active_threads {
                    let guard = state.locks[thread_index]
                        .lock()
                        .unwrap();

                    thread_guards.push(guard);
                }

                // Balance the work across all the active threads
                balance(&mut thread_guards[0..state.active_threads]);

                // Remove any threads off the tail from processing if they have no work.
                let mut deactivated_threads = 0;
                for thread_index in (0..thread_guards.len()).rev() {
                    if thread_guards[thread_index].len() > 0 {
                        break;
                    }

                    thread_guards[thread_index].clear();
                    thread_guards[thread_index].shrink_to_fit();
                    deactivated_threads += 1;
                }

                drop(thread_guards);
                state.active_threads -= deactivated_threads;
            }
        }
    }
}
