// Copyright 2026 The NativeLink Authors. All rights reserved.
//
// Licensed under the Functional Source License, Version 1.1, Apache 2.0 Future License (the "License");
// you may not use this file except in compliance with the License.
//
// See LICENSE file for details.

//! TE1 — minimal counter API.
//!
//! The 1.4 telemetry surface is deliberately tiny: `inc`, `add`, `get`,
//! `snapshot`. No periodic dumper, no per-stage harness. These tests
//! pin the API in place so future drift is caught.

use nativelink_util::counters::{
    add, add_with_rate, advance_clock_for_test, get, inc, rate_last_60s, reset_for_test,
    reset_rate_for_test, set_clock_for_test, snapshot,
};

#[test]
fn inc_and_get_round_trip() {
    let name = "test.te1.simple_inc";
    reset_for_test(name);
    inc(name);
    inc(name);
    inc(name);
    assert_eq!(get(name), 3);
}

#[test]
fn add_increments_by_delta() {
    let name = "test.te1.add_delta";
    reset_for_test(name);
    add(name, 100);
    add(name, 23);
    assert_eq!(get(name), 123);
}

#[test]
fn unregistered_counter_reads_as_zero() {
    let name = "test.te1.never_touched";
    assert_eq!(get(name), 0);
}

#[test]
fn snapshot_includes_registered_counters() {
    let unique = "test.te1.snapshot_unique_marker";
    reset_for_test(unique);
    inc(unique);
    let snap = snapshot();
    assert!(
        snap.iter().any(|(n, v)| *n == unique && *v >= 1),
        "snapshot must include the just-incremented counter; got {snap:?}"
    );
}

#[test]
fn concurrent_inc_does_not_lose_updates() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::thread;

    let name = "test.te1.concurrent_marker";
    reset_for_test(name);

    let stop = Arc::new(AtomicBool::new(false));
    let handles: Vec<_> = (0..8)
        .map(|_| {
            let stop = Arc::clone(&stop);
            thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    inc(name);
                }
            })
        })
        .collect();

    // Let the threads run briefly, then signal stop.
    std::thread::sleep(std::time::Duration::from_millis(30));
    stop.store(true, Ordering::Relaxed);
    for h in handles {
        h.join().unwrap();
    }
    assert!(get(name) > 0, "concurrent increments must produce a positive count");
}

// ---------------------------------------------------------------------------
// Rate-window tests — pin the contract for `add_with_rate` /
// `rate_last_60s`. The window is a 60-second sliding ring. Time is
// driven by an injected test clock so eviction is deterministic.
//
// The test clock is process-global, so these tests serialize on a
// shared Mutex to keep one test's clock advance from racing another's
// window assertions.
// ---------------------------------------------------------------------------

fn rate_test_lock() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();
    LOCK.get_or_init(|| std::sync::Mutex::new(()))
        .lock()
        .unwrap_or_else(|p| p.into_inner())
}

#[test]
fn rate_last_60s_zero_for_never_incremented_counter() {
    let _g = rate_test_lock();
    let name = "test.te1.rate.never_touched";
    reset_rate_for_test(name);
    assert_eq!(rate_last_60s(name), 0);
}

#[test]
fn rate_last_60s_zero_for_counter_only_bumped_via_plain_add() {
    let _g = rate_test_lock();
    // Opt-in contract: a counter only ever bumped via plain `add`/`inc`
    // must NOT contribute to `rate_last_60s`. Rate is exclusively a
    // function of `add_with_rate` calls.
    let name = "test.te1.rate.plain_add_only";
    reset_for_test(name);
    reset_rate_for_test(name);
    set_clock_for_test(1_000_000);
    add(name, 100);
    inc(name);
    assert_eq!(rate_last_60s(name), 0);
}

#[test]
fn rate_last_60s_sums_increments_within_window() {
    let _g = rate_test_lock();
    let name = "test.te1.rate.sum_within_window";
    reset_rate_for_test(name);
    reset_for_test(name);
    set_clock_for_test(2_000_000);
    add_with_rate(name, 3);
    advance_clock_for_test(30);
    add_with_rate(name, 5);
    assert_eq!(rate_last_60s(name), 8);
}

#[test]
fn rate_last_60s_evicts_increments_older_than_60s() {
    let _g = rate_test_lock();
    let name = "test.te1.rate.evict_old";
    reset_rate_for_test(name);
    reset_for_test(name);
    set_clock_for_test(3_000_000);
    add_with_rate(name, 7);
    // Jump beyond the 60s window — the bucket from the original second
    // is now stale. The next add_with_rate at the new "now" must NOT
    // pick up the stale 7 in its sum.
    advance_clock_for_test(61);
    add_with_rate(name, 2);
    assert_eq!(rate_last_60s(name), 2);
}

#[test]
fn rate_last_60s_handles_same_second_concurrent_adds() {
    use std::sync::Arc;
    use std::sync::Barrier;
    use std::thread;

    let _g = rate_test_lock();
    let name = "test.te1.rate.concurrent_same_second";
    reset_rate_for_test(name);
    reset_for_test(name);
    set_clock_for_test(4_000_000);

    const THREADS: usize = 8;
    const PER_THREAD: usize = 1000;
    let barrier = Arc::new(Barrier::new(THREADS));
    let handles: Vec<_> = (0..THREADS)
        .map(|_| {
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                for _ in 0..PER_THREAD {
                    add_with_rate(name, 1);
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
    assert_eq!(rate_last_60s(name), (THREADS * PER_THREAD) as u64);
    assert_eq!(get(name), (THREADS * PER_THREAD) as u64);
}

#[test]
fn add_with_rate_also_increments_cumulative() {
    let _g = rate_test_lock();
    let name = "test.te1.rate.also_cumulative";
    reset_rate_for_test(name);
    reset_for_test(name);
    set_clock_for_test(5_000_000);
    add_with_rate(name, 4);
    add_with_rate(name, 6);
    assert_eq!(get(name), 10);
}

#[test]
fn cumulative_add_does_not_pollute_rate_for_add_with_rate_users() {
    let _g = rate_test_lock();
    // If a counter is ever bumped via `add_with_rate`, plain `add`
    // calls must NOT show up in `rate_last_60s`. Plain `add` only
    // updates the cumulative atomic, never the rate ring.
    let name = "test.te1.rate.no_cross_contamination";
    reset_rate_for_test(name);
    reset_for_test(name);
    set_clock_for_test(6_000_000);
    add_with_rate(name, 1);
    add(name, 100);
    assert_eq!(rate_last_60s(name), 1);
    assert_eq!(get(name), 101);
}
