// Phase 54-04 Pass A6b: whole file gated off — references the deleted
// `StateStore`. Pass C re-gates or prunes.
#![cfg(any())]

// CORR-10: busy-racer asserting take_dirty_and_advance_gen() loses no marks.
//
// Pattern: N writer threads call mark_dirty(key) while a snapshotter thread
// calls take_dirty_and_advance_gen() in a tight loop.  Every marked key must
// appear in exactly one generation's dirty set — no key must be lost.

use beava::state::store::StateStore;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use std::thread;
use std::time::Duration;

#[ignore = "54-03 Task 4: legacy StateStore API / engine.push(&store, ...); Wave 4 re-enables after legacy-engine removal"]
#[test]
fn busy_racer_no_lost_keys() {
    let store = Arc::new(StateStore::new());
    const N_WRITERS: usize = 8;
    const N_ITERS: usize = 1000;

    let stop = Arc::new(AtomicBool::new(false));
    // Collect the Arc<DashSet> returned by every take_dirty_and_advance_gen()
    // call — including empty ones, because a writer may insert into a set
    // *after* the snapshotter has swapped it out (the Guard keeps it alive).
    let collected: Arc<Mutex<Vec<std::sync::Arc<dashmap::DashSet<String>>>>> =
        Arc::new(Mutex::new(Vec::new()));

    // Snapshotter thread: repeatedly calls take_dirty_and_advance_gen() until
    // all writers are done, then the main thread stops it.
    let snap_handle = {
        let store_ = store.clone();
        let stop_ = stop.clone();
        let coll_ = collected.clone();
        thread::spawn(move || {
            while !stop_.load(Ordering::Acquire) {
                let frozen = store_.take_dirty_and_advance_gen();
                // Always push — even empty sets — because a writer whose
                // ArcSwap Guard still points to this Arc may insert into it
                // after we swapped, and we must capture those keys.
                coll_.lock().unwrap().push(frozen);
                thread::yield_now();
            }
        })
    };

    // Writer threads: each inserts N_ITERS unique keys.
    let writers: Vec<_> = (0..N_WRITERS)
        .map(|tid| {
            let store_ = store.clone();
            thread::spawn(move || {
                for i in 0..N_ITERS {
                    store_.mark_dirty(&format!("k{}-{}", tid, i));
                }
            })
        })
        .collect();

    // Wait for all writers to finish.
    for w in writers {
        w.join().unwrap();
    }

    // Give the snapshotter a brief moment to observe any final marks that
    // landed in the active set after the last writer finished.
    thread::sleep(Duration::from_millis(50));
    stop.store(true, Ordering::Release);
    snap_handle.join().unwrap();

    // One final take to capture any keys that landed in the active set after
    // the snapshotter's last take but before we stopped it.
    let final_arc = store.take_dirty_and_advance_gen();
    collected.lock().unwrap().push(final_arc);

    // Union all collected sets and verify completeness + no duplicates.
    let sets = collected.lock().unwrap();
    let mut all: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut duplicates: Vec<String> = Vec::new();

    for arc in sets.iter() {
        for r in arc.iter() {
            let key = r.key().clone();
            if !all.insert(key.clone()) {
                duplicates.push(key);
            }
        }
    }

    let expected = N_WRITERS * N_ITERS;
    assert!(
        duplicates.is_empty(),
        "CORR-10: found {} duplicate keys across snapshot cycles: {:?}",
        duplicates.len(),
        &duplicates[..duplicates.len().min(10)]
    );
    assert_eq!(
        all.len(),
        expected,
        "CORR-10: expected {} unique keys, got {} (missing {} keys)",
        expected,
        all.len(),
        expected.saturating_sub(all.len())
    );
}
