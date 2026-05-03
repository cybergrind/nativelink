// Copyright 2026 The NativeLink Authors. All rights reserved.
//
// Licensed under the Functional Source License, Version 1.1, Apache 2.0 Future License (the "License");
// you may not use this file except in compliance with the License.
//
// See LICENSE file for details.

//! Minimal named u64 counters.
//!
//! 1.4 deliberately does NOT ship a periodic timing-dump cron, a per-
//! stage timing harness, or Redis pool counters. The stack is just:
//!   - `inc(name)` increments by 1
//!   - `add(name, delta)` increments by N
//!   - `get(name)` reads the current value
//!   - `snapshot()` returns a (name, value) Vec for scrape endpoints
//!
//! Counters are lazily registered on first use. Storage is a single
//! global RwLock<HashMap<&'static str, AtomicU64>>; reads on the hot
//! path do one HashMap lookup + one Relaxed atomic add. Names are
//! `&'static str` to keep the map allocation-free at runtime.

use std::collections::HashMap;
use std::sync::OnceLock;
use std::sync::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};

static REGISTRY: OnceLock<RwLock<HashMap<&'static str, AtomicU64>>> = OnceLock::new();

fn registry() -> &'static RwLock<HashMap<&'static str, AtomicU64>> {
    REGISTRY.get_or_init(|| RwLock::new(HashMap::new()))
}

/// Increment counter `name` by 1. Lazily registers on first use.
pub fn inc(name: &'static str) {
    add(name, 1);
}

/// Increment counter `name` by `delta`. Lazily registers on first use.
pub fn add(name: &'static str, delta: u64) {
    {
        let guard = registry().read().unwrap();
        if let Some(c) = guard.get(name) {
            c.fetch_add(delta, Ordering::Relaxed);
            return;
        }
    }
    let mut guard = registry().write().unwrap();
    guard
        .entry(name)
        .or_insert_with(|| AtomicU64::new(0))
        .fetch_add(delta, Ordering::Relaxed);
}

/// Current value of `name`, or 0 if never registered.
pub fn get(name: &'static str) -> u64 {
    registry()
        .read()
        .unwrap()
        .get(name)
        .map_or(0, |c| c.load(Ordering::Relaxed))
}

/// Snapshot the registry — `(name, value)` pairs in arbitrary order.
/// Intended for periodic-scrape diagnostic endpoints, not the hot path.
pub fn snapshot() -> Vec<(&'static str, u64)> {
    registry()
        .read()
        .unwrap()
        .iter()
        .map(|(name, c)| (*name, c.load(Ordering::Relaxed)))
        .collect()
}

/// Reset a counter to 0 — test-only.
#[doc(hidden)]
pub fn reset_for_test(name: &'static str) {
    if let Some(c) = registry().read().unwrap().get(name) {
        c.store(0, Ordering::Relaxed);
    }
}
