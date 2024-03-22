use std::{cmp::{max, min}, collections::HashMap, sync::{Arc, Mutex}, thread::{self, JoinHandle}};

pub struct Processor<T> {
    items: Vec<T>,
    thread_count: usize
}

impl<T: Sync + Send + 'static> Processor<T> {
    pub fn new(items: Vec<T>, thread_count: usize) -> Processor<T> {
        let thread_count = max(min(items.len(), thread_count), 1);

        Processor {
            items,
            thread_count
        }
    }

    pub fn start<K, S>(self, worker: K, sorter: S) -> Result<(), std::io::Error> where
        K: 'static + Send + Clone + Fn(T) -> Result<(), std::io::Error>,
        S: 'static + Send + Clone + Fn(&mut [T]),
    {
        // No items means nothing to process; quickly leave.
        if self.items.len() == 0 {
            return Ok(());
        }

        // Setup work items for threads
        let mut source = self.items;
        sorter(&mut source);

        let mut others = (1..self.thread_count)
            .map(|_| Vec::new())
            .collect::<Vec<Vec<_>>>();

        Processor::balance(&mut source, &mut others.iter_mut().map(|value| value.as_mut()).collect::<Vec<_>>());

        // Setup work queues
        let work_queues: HashMap<usize, _> = std::iter::once(source)
            .chain(others.into_iter())
            .map(|entry| Some(entry))
            .map(|entry| Arc::new(Mutex::new(entry)))
            .enumerate()
            .collect();

        let work_queues_lock = Arc::new(Mutex::new(work_queues.clone()));
        let mut handles: Vec<JoinHandle<Result<(), std::io::Error>>> = Vec::new();

        for (thread_id, local_queue) in work_queues {
            let work_queues_lock = work_queues_lock.clone();
            let worker = worker.clone();
            let sorter = sorter.clone();

            let handle = thread::spawn(move || {
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
                            worker(work)?
                        },
                        None => {
                            let work_queue_lock_guard = work_queues_lock.lock().unwrap();

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

                                // Take the work from the threads
                                let mut source = guard.as_mut().unwrap();
                                for other_guard in other_guards.iter_mut() {
                                    let data = other_guard.as_mut().unwrap();
                                    source.extend(data.drain(..));
                                }

                                if source.len() > 0 {
                                    // Sort and rebalance
                                    let mut others: Vec<_> = other_guards
                                        .iter_mut()
                                        .map(|value| value.as_mut().unwrap())
                                        .collect();

                                    sorter(&mut source);
                                    Processor::balance(&mut source, &mut others);
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
            });

            handles.push(handle);
        }

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

    // Source should be the thread that is performing the rebalance and should have all items from
    // all threads executing, others will be the other threads that will be given a new set of work items.
    fn balance(source: &mut Vec<T>, others: &mut Vec<&mut Vec<T>>) {
        let total_work = source.len();
        let active_threads = others.len() + 1;

        let work_for_other_threads = total_work - ((total_work / active_threads) + ((total_work % active_threads != 0) as usize));

        let min_work_per_worker = work_for_other_threads / others.len();
        let mut remainder = work_for_other_threads % others.len();

        for target in others.iter_mut() {
            let has_remaining = (remainder > 0) as usize;
            let work_for_target = min_work_per_worker + has_remaining;
            remainder -= has_remaining;

            target.extend(source.drain(..work_for_target));
        }

        let counted_work = source.len() + others
            .iter()
            .map(|target| target.len())
            .sum::<usize>();
        
        println!("Rebalanced {} items across {} workers with at-minimum {} per worker; lost {}", total_work, active_threads, min_work_per_worker, total_work - counted_work);
    }
}
