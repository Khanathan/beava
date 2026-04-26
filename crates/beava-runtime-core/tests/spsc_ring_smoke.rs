//! Plan 18-13 RED — rtrb SPSC ring smoke test.
//!
//! Pins the contract for the lock-free SPSC ring used between IoPool worker
//! threads and the apply thread:
//!
//! 1. **FIFO ordering**: items pop in the same order they were pushed.
//! 2. **Capacity invariant**: `producer.slots() + consumer.slots()` always
//!    equals the ring's max capacity.
//! 3. **Cross-thread safety**: producer can run on a worker thread while the
//!    consumer drains on another; no data races, no torn reads.
//! 4. **Push-fail on full**: when the ring fills up, `push` returns
//!    `Err(rtrb::PushError::Full(value))` and the value can be retried.
//!
//! The test fails to compile until rtrb is added as a dep — that is the
//! strongest form of RED for a new dependency.

use rtrb::{Consumer, Producer, RingBuffer};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

const RING_CAPACITY: usize = 1024;
const TOTAL_ITEMS: u64 = 100_000;

#[test]
fn spsc_ring_basic_push_pop_fifo() {
    let (mut producer, mut consumer): (Producer<u64>, Consumer<u64>) =
        RingBuffer::new(RING_CAPACITY);

    // Cross-thread: one thread pushes 100k integers, main consumes them.
    let producer_handle = thread::spawn(move || {
        let mut sent: u64 = 0;
        while sent < TOTAL_ITEMS {
            match producer.push(sent) {
                Ok(()) => sent += 1,
                Err(rtrb::PushError::Full(_)) => thread::yield_now(),
            }
        }
    });

    // Drain on main thread; assert FIFO and no drops.
    let mut received: u64 = 0;
    let deadline = Instant::now() + Duration::from_secs(5);
    while received < TOTAL_ITEMS {
        match consumer.pop() {
            Ok(value) => {
                assert_eq!(
                    value, received,
                    "FIFO violation: expected {received}, got {value}"
                );
                received += 1;
            }
            Err(rtrb::PopError::Empty) => {
                if Instant::now() > deadline {
                    panic!("timeout waiting for items: received {received}/{TOTAL_ITEMS}");
                }
                thread::yield_now();
            }
        }
    }

    producer_handle.join().expect("producer thread join");
    assert_eq!(received, TOTAL_ITEMS, "all items must be received");
}

#[test]
fn spsc_ring_capacity_invariant() {
    let (mut producer, mut consumer): (Producer<u64>, Consumer<u64>) = RingBuffer::new(64);

    // Empty: producer has all slots, consumer has none.
    assert_eq!(producer.slots(), 64);
    assert_eq!(consumer.slots(), 0);

    // Push 32 items.
    for i in 0..32 {
        producer.push(i).expect("push");
    }
    // Producer reports 32 free slots, consumer reports 32 occupied slots.
    assert_eq!(producer.slots(), 32);
    assert_eq!(consumer.slots(), 32);

    // Pop 16 items.
    for i in 0..16 {
        let v = consumer.pop().expect("pop");
        assert_eq!(v, i);
    }
    // Producer has 48 free, consumer has 16 occupied.
    assert_eq!(producer.slots(), 48);
    assert_eq!(consumer.slots(), 16);
}

#[test]
fn spsc_ring_push_fails_when_full() {
    let (mut producer, mut consumer): (Producer<u32>, Consumer<u32>) = RingBuffer::new(4);

    // Fill the ring.
    for i in 0..4 {
        producer.push(i).expect("push");
    }
    assert!(producer.is_full());
    assert_eq!(producer.slots(), 0);

    // Next push must fail with Full(value), preserving the value.
    match producer.push(99) {
        Err(rtrb::PushError::Full(v)) => assert_eq!(v, 99, "Full must return the rejected value"),
        Ok(()) => panic!("push should fail on full ring"),
    }

    // After consumer pops one, push succeeds.
    let _ = consumer.pop().expect("pop");
    producer.push(99).expect("push after pop should succeed");
}

/// Stress test: 8 producer/consumer hand-offs, verifying no items lost.
/// Uses a shared atomic to coordinate producer/consumer thread handoff.
#[test]
fn spsc_ring_stress_handoff() {
    const N: u64 = 1_000_000;
    let (mut producer, mut consumer): (Producer<u64>, Consumer<u64>) = RingBuffer::new(8192);
    let consumed = Arc::new(AtomicUsize::new(0));
    let consumed_thread = Arc::clone(&consumed);

    let consumer_handle = thread::spawn(move || {
        let mut received: u64 = 0;
        while received < N {
            match consumer.pop() {
                Ok(v) => {
                    assert_eq!(v, received);
                    received += 1;
                    consumed_thread.fetch_add(1, Ordering::Relaxed);
                }
                Err(rtrb::PopError::Empty) => thread::yield_now(),
            }
        }
    });

    let mut sent: u64 = 0;
    while sent < N {
        match producer.push(sent) {
            Ok(()) => sent += 1,
            Err(rtrb::PushError::Full(_)) => thread::yield_now(),
        }
    }

    consumer_handle.join().expect("consumer join");
    assert_eq!(consumed.load(Ordering::Relaxed) as u64, N);
}
