use std::{cmp::{max, min}, collections::HashMap, sync::{Arc, Mutex}, thread::{self, JoinHandle}};

pub trait Solver<T: Sync + Send + 'static, E: Sync + Send + 'static> 
    where Self: Send + Sync + Sized + Clone + 'static {
    fn solve(&self, work: T) -> Result<(), E>;

    fn start(&self, items: Vec<T>, thread_count: usize) -> Result<(), E> {
        // No items means nothing to process; quickly leave.
        if items.len() == 0 {
            return Ok(());
        }

        // Setup thread count
        let thread_count = max(min(items.len(), thread_count), 1);

        // Worker function
        let worker_fn = |solver: Self, thread_id: usize, local_queue: Arc<Mutex<Option<Vec<T>>>>, work_queue: Arc<Mutex<HashMap<usize, Arc<Mutex<Option<Vec<T>>>>>>>| -> Result<(), E> {
            'outer: loop {
                let found = {
                    let guard = local_queue.try_lock();
                    
                    if let Ok(mut guard) = guard {
                        guard.as_mut().unwrap().pop()
                    } else {
                        None
                    }
                };

                match found {
                    Some(work) => {
                        solver.solve(work)?;
                    },
                    None => {
                        let work_queue_lock_guard = work_queue.lock().unwrap();

                        // We may have multiple waiters here if multiple queues are looking for work, quickly recheck to see if we can quick abort
                        // as we may have just balanced all the threads, no point in doing it again.
                        let mut guard = local_queue.lock().unwrap();

                        let should_abort = match guard.as_ref() {
                            Some(value) => { 
                                value.len() > 0
                            },
                            None => true
                        };

                        if should_abort {
                            println!("Quickly aborting rebalance on thread {}", thread_id);
                            continue 'outer;
                        }

                        // Lock all the other threads
                        // Only store the active threads.
                        let mut other_guards = Vec::new();
                        for (other_thread_id, other_lock) in work_queue_lock_guard.iter() {
                            if *other_thread_id == thread_id {
                                continue;
                            }

                            let guard = other_lock
                                .lock()
                                .unwrap();

                            let inner = &*guard;
                            match inner.as_ref() {
                                Some(_) => other_guards.push(guard),
                                None => {}
                            }
                        }

                        if other_guards.len() > 0 {
                            // Sort other threads by those that do not have work first.
                            // Give them work first, so that they don't waste time doing their own re-balance.
                            // We know the other threads are most-likely working an item already.
                            other_guards.sort_by(|a, b| {
                                a.as_ref().unwrap().len().cmp(&b.as_ref().unwrap().len())
                            });

                            // Count available work
                            let mut remaining_work = guard.as_ref().unwrap().len();
                            for other_guard in other_guards.iter() {
                                remaining_work += other_guard.as_ref().unwrap().len();
                            }

                            if remaining_work > 0 {
                                // Sort and rebalance
                                let mut others: Vec<_> = other_guards
                                    .iter_mut()
                                    .map(|value| value.as_mut().unwrap())
                                    .collect();

                                Self::balance(guard.as_mut().unwrap(), &mut others);
                            }
                        }

                        drop(other_guards);
                        drop(work_queue_lock_guard);

                        // Mark thread as dead if there is no more work and exit
                        match guard.as_ref() {
                            Some(value)  => { 
                                if value.len() == 0 { 
                                    guard.take();
                                    break 'outer;
                                }
                            },
                            None => panic!("Thread {} is already shutdown, yet tried to re-balance. This is impossible.", thread_id)
                        };

                    }
                }
            }

            Ok(())
        };

        // Setup work queue
        let mut work_queue: HashMap<usize, Arc<Mutex<Option<Vec<T>>>>> = HashMap::new();
        
        work_queue.insert(0, Arc::new(Mutex::new(Some(items))));
        for thread_id in 1..thread_count {
            work_queue.insert(thread_id, Arc::new(Mutex::new(Some(Vec::new()))));
        }

        let shared_work_queue = Arc::new(Mutex::new(work_queue.clone()));
        
        // Startup workers
        let mut handles: Vec<JoinHandle<Result<(), E>>> = Vec::new();

        for (thread_id, local_queue) in work_queue {
            let shared_work_queue = shared_work_queue.clone();
            let solver = (*self).clone();

            let handle = thread::spawn(move || {
                worker_fn(solver, thread_id, local_queue, shared_work_queue)
            });

            handles.push(handle);
        }

        // Capture results
        let mut results: Vec<_> = Vec::new();
        for handle in handles {
            let res = handle.join().expect("Thread crashed during processing");
            results.push(res);
        }

        for result in results {
            if result.is_err() {
                return result;
            }
        }

        Ok(())
    }

    // Source should be the thread that is performing the rebalance, others is all other locked threads.
    // Source should always have 1 item unless the total work queue is empty, or 
    // worker will shutdown and a different thread will have more work items on queue then necessary.
    fn balance(source: &mut Vec<T>, others: &mut Vec<&mut Vec<T>>) {
        if others.len() > 0 {
            let others_len = others.len();

            let mut total_work = others.first().unwrap().len();
            let mut max_worker = 0;

            for index in 1..others_len {
                let compare_worker = others.get(index).unwrap();

                if compare_worker.len() > total_work {
                    max_worker = index;
                    total_work = compare_worker.len();
                }
            }

            let max_worker = others.get_mut(max_worker).unwrap();

            let take = (max_worker.len() / 2) + ((max_worker.len() % 2 != 0) as usize);
            source.extend(max_worker.drain(..take));

            let counted_work = source.len() + max_worker.len();
            println!("Rebalanced {} items between 2 workers; lost {}", total_work, total_work - counted_work);
        }
    }
}
