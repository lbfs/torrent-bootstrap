use std::{sync::{mpsc::SyncSender, Arc, Mutex, MutexGuard}, thread::{self, JoinHandle}};

use crate::solver::{choices::ChoiceConsumer, task::{PieceUpdate, Solver, Task}};

struct ExecutionState {
    pending: Mutex<Vec<Task>>,
    active: Vec<Mutex<Option<Task>>>
}

pub fn run(mut items: Vec<Task>, thread_count: usize, writer: SyncSender<PieceUpdate>) {
    if items.is_empty() {
        return;
    }

    let thread_count = std::cmp::max(std::cmp::min(items.len(), thread_count), 1);

    let mut active_tasks: Vec<Mutex<Option<Task>>> = Vec::new();
    for _ in 0..thread_count {
        match items.pop() {
            Some(item) => {
                active_tasks.push(Mutex::new(Some(item)))
            }
            None => {
                active_tasks.push(Mutex::new(None));
            }
        }     
    }

    let execution_state = Arc::new(ExecutionState {
        active: active_tasks,
        pending: Mutex::new(items)
    });

    // Start up the workers
    let mut handles: Vec<JoinHandle<()>> = Vec::with_capacity(thread_count);
    for thread_id in 0..thread_count {
        let writer = writer.clone();
        let execution_state = execution_state.clone();

        let handle = thread::spawn(move || {
            run_internal(thread_id, execution_state, writer);
        });

        handles.push(handle);
    }

    for handle in handles.into_iter() {
        handle.join()
            .expect("Encountered panic while joining on thread handle.");
    }
}

fn run_internal(thread_id: usize, execution_state: Arc<ExecutionState>, mut writer: SyncSender<PieceUpdate>) {
    let mut current_thread_id = thread_id;
    let mut choice_consumer = ChoiceConsumer::empty();
    let mut solver = Solver::new();

    'outer: loop {
        let found = {
            let mut guard = execution_state
                .active[current_thread_id]
                .lock()
                .unwrap();

            let mut item = None;
            if let Some(generator) = guard.as_mut() {
                item = generator.take(&mut choice_consumer);
                if let None = item {
                    guard.take();
                }
            }

            item            
        };

        match found {
            Some(task_state) => {
                solver.solve(&mut choice_consumer, task_state.as_ref(), &mut writer);
            },
            None => {
                let mut pending = execution_state.pending
                    .lock()
                    .unwrap();

                let mut local = execution_state.active[thread_id]
                    .lock()
                    .unwrap();

                // Another thread has performed a re-balance of work.
                if local.is_some() {
                    continue;
                }

                // If multiple threads were waiting for work, we need to abort the thread from
                // performing a work re-balance, as it was just done.
                if !pending.is_empty() {
                    let _ = local.insert(pending.pop().unwrap());
                    continue;
                }

                // Lock all the threads so we can steal and re-balance the work for optimal round-robin.
                let mut thread_guards: Vec<MutexGuard<_>> = Vec::with_capacity(execution_state.active.len());

                for thread_index in 0..thread_id {
                    let guard = execution_state.active[thread_index]
                        .lock()
                        .unwrap();

                    thread_guards.push(guard);
                }

                thread_guards.push(local);

                for thread_index in thread_id + 1..execution_state.active.len() {
                    let guard = execution_state.active[thread_index]
                        .lock()
                        .unwrap();

                    thread_guards.push(guard);
                }

                // Fetch all the remaining tasks
                let mut remaining: Vec<Task> = Vec::new();
                for thread in thread_guards.iter_mut() {
                    if let Some(item) = thread.take() {
                        remaining.push(item);
                    }
                }

                let remaining_work_len = remaining.len();

                if remaining_work_len == 0 {
                    // Terminate the thread when all work has been exhausted.
                    break 'outer;
                }

                for (assignment, item) in remaining.into_iter().enumerate() {
                    let _ = thread_guards[assignment].insert(item);
                }

                current_thread_id = thread_id % remaining_work_len;
            }
        }
    }
}
