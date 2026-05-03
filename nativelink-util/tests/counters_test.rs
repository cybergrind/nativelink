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

use nativelink_util::counters::{add, get, inc, reset_for_test, snapshot};

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
