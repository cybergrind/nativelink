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
//!
//! ## Optional rate-per-minute (`add_with_rate` / `rate_last_60s`)
//!
//! A counter bumped via `add_with_rate(name, delta)` ALSO records
//! deltas into a 60-cell ring of (epoch_second, count) buckets, one
//! cell per second of wall-clock. `rate_last_60s(name)` sums cells
//! whose timestamp is within the trailing 60 seconds — older cells
//! are implicitly evicted by the next write that lands on them.
//!
//! Rate tracking is **opt-in per call site**. Counters bumped via
//! plain `inc`/`add` pay no extra cost and `rate_last_60s` returns 0
//! for them. This keeps the hot per-file counter sites
//! (`worker.plan_k.hit/miss/...`, `cas.find_missing_blobs.*`) at one
//! Relaxed RMW.

use std::collections::HashMap;
use std::sync::OnceLock;
use std::sync::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

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

// ---------------------------------------------------------------------------
// Rate-window facility — opt-in 60-second sliding ring.
// ---------------------------------------------------------------------------

const RATE_WINDOW_SECS: usize = 60;

/// One slot in the rate ring. Packs `(epoch_second_u32, count_u32)`
/// into a single u64 so a CAS atomically rotates the bucket — no
/// torn reads of split fields.
struct RateBucket {
    packed: AtomicU64,
}

impl RateBucket {
    const fn new() -> Self {
        Self {
            packed: AtomicU64::new(0),
        }
    }
}

struct RateRing {
    cells: [RateBucket; RATE_WINDOW_SECS],
}

impl RateRing {
    fn new() -> Self {
        Self {
            cells: std::array::from_fn(|_| RateBucket::new()),
        }
    }
}

#[inline]
fn pack(sec: u32, count: u32) -> u64 {
    ((sec as u64) << 32) | (count as u64)
}

#[inline]
fn unpack(value: u64) -> (u32, u32) {
    ((value >> 32) as u32, (value & 0xFFFF_FFFF) as u32)
}

static RATES: OnceLock<RwLock<HashMap<&'static str, RateRing>>> = OnceLock::new();

fn rates() -> &'static RwLock<HashMap<&'static str, RateRing>> {
    RATES.get_or_init(|| RwLock::new(HashMap::new()))
}

static TEST_CLOCK: OnceLock<AtomicU64> = OnceLock::new();

#[inline]
fn now_secs() -> u64 {
    if let Some(c) = TEST_CLOCK.get() {
        return c.load(Ordering::Relaxed);
    }
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

/// Increment counter `name` by `delta` AND record the delta into the
/// 60-second rate ring. Cumulative reads via `get`/`snapshot` work as
/// usual; rate reads via `rate_last_60s(name)` reflect this delta.
///
/// Counters that are never bumped via `add_with_rate` have no ring
/// and `rate_last_60s` returns 0 for them.
pub fn add_with_rate(name: &'static str, delta: u64) {
    add(name, delta);
    let now = now_secs();
    // Truncate-to-u32 is safe for our purposes: epoch_seconds wraps
    // every ~136 years, and we only compare second values within a
    // 60-second sliding window.
    let now32 = now as u32;
    let delta32 = u32::try_from(delta).unwrap_or(u32::MAX);
    let idx = (now % RATE_WINDOW_SECS as u64) as usize;

    {
        let guard = rates().read().unwrap();
        if let Some(ring) = guard.get(name) {
            bump_cell(&ring.cells[idx], now32, delta32);
            return;
        }
    }
    let mut guard = rates().write().unwrap();
    let ring = guard.entry(name).or_insert_with(RateRing::new);
    bump_cell(&ring.cells[idx], now32, delta32);
}

#[inline]
fn bump_cell(cell: &RateBucket, now: u32, delta: u32) {
    loop {
        let old = cell.packed.load(Ordering::Acquire);
        let (old_sec, old_count) = unpack(old);
        let new = if old_sec == now {
            // Same second: accumulate. Saturate on u32 overflow rather
            // than wrap — a single second carrying >4G increments is
            // already outside the design envelope.
            pack(now, old_count.saturating_add(delta))
        } else {
            // Stale (or never-written) bucket — implicit eviction.
            pack(now, delta)
        };
        if cell
            .packed
            .compare_exchange_weak(old, new, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            return;
        }
    }
}

/// Sum of `add_with_rate` deltas for `name` whose timestamp falls in
/// the trailing 60 seconds. Returns 0 for names with no rate ring.
pub fn rate_last_60s(name: &'static str) -> u64 {
    let guard = rates().read().unwrap();
    let Some(ring) = guard.get(name) else {
        return 0;
    };
    let now = now_secs();
    let now32 = now as u32;
    // A cell is in-window iff its epoch_second is within
    // [now - 59, now]. We avoid underflow when now < 59 (only possible
    // under the test clock) by clamping with saturating_sub.
    let cutoff = now32.saturating_sub((RATE_WINDOW_SECS as u32) - 1);
    let mut sum: u64 = 0;
    for cell in &ring.cells {
        let (sec, count) = unpack(cell.packed.load(Ordering::Acquire));
        if sec >= cutoff && sec <= now32 {
            sum += u64::from(count);
        }
    }
    sum
}

/// Reset the rate ring for `name` to all-zero — test-only.
#[doc(hidden)]
pub fn reset_rate_for_test(name: &'static str) {
    if let Some(ring) = rates().read().unwrap().get(name) {
        for cell in &ring.cells {
            cell.packed.store(0, Ordering::Release);
        }
    }
}

/// Pin the test clock to a specific epoch-second — test-only.
/// Once set, ALL subsequent calls (in any test in the binary) read
/// from the test clock. Tests should set a deterministic per-test
/// value to avoid cross-test interference; the initial bucket layout
/// (now % 60) is what differs between tests.
#[doc(hidden)]
pub fn set_clock_for_test(secs: u64) {
    TEST_CLOCK
        .get_or_init(|| AtomicU64::new(secs))
        .store(secs, Ordering::Relaxed);
}

/// Advance the test clock by `delta` seconds — test-only.
#[doc(hidden)]
pub fn advance_clock_for_test(delta: u64) {
    let cell = TEST_CLOCK.get_or_init(|| AtomicU64::new(0));
    cell.fetch_add(delta, Ordering::Relaxed);
}
