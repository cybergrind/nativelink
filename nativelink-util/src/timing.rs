// Copyright 2024 The NativeLink Authors. All rights reserved.
//
// Licensed under the Functional Source License, Version 1.1, Apache 2.0 Future License (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    See LICENSE file for details
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Lightweight per-stage timing harness for finding performance hot spots.
//!
//! Each `StageStats` is a const-initializable static that accumulates
//! `(count, total_ns, max_ns)` atomically. Two common usage patterns:
//!
//! ```ignore
//! use nativelink_util::timing::{StageStats, StageTimer};
//!
//! // Timed region — RAII guard records elapsed on drop.
//! static PREPARE: StageStats = StageStats::new("prepare_action_inputs");
//! fn run() {
//!     let _t = PREPARE.timer();
//!     // ... work ...
//! }
//!
//! // Plain counter (no timing).
//! static CACHE_HIT: StageStats = StageStats::new("plan_k.hit");
//! CACHE_HIT.incr();
//! ```
//!
//! Call `nativelink_util::timing::dump_to_tracing()` periodically (and
//! once at shutdown) to log all registered stages' cumulative stats.
//!
//! Cumulative `total_ns` exposes stages that consume the most wall time
//! across a run; per-stage `max_ns` surfaces tail-latency outliers that
//! break parallelism. Counter-only stages (increments via `incr`) have
//! `total_ns = 0` and are useful for hit/miss ratios in caches.

use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// A registered timing stage. Construct as a `static` so it can be
/// referenced from any context without lifetime juggling.
#[derive(Debug)]
pub struct StageStats {
    name: &'static str,
    count: AtomicU64,
    total_ns: AtomicU64,
    max_ns: AtomicU64,
    registered: std::sync::Once,
}

impl StageStats {
    #[must_use]
    pub const fn new(name: &'static str) -> Self {
        Self {
            name,
            count: AtomicU64::new(0),
            total_ns: AtomicU64::new(0),
            max_ns: AtomicU64::new(0),
            registered: std::sync::Once::new(),
        }
    }

    fn register_once(&'static self) {
        self.registered.call_once(|| {
            if let Ok(mut v) = REGISTRY.lock() {
                v.push(self);
            }
        });
    }

    /// Record a single timed observation.
    pub fn record(&'static self, d: Duration) {
        self.register_once();
        let ns = u64::try_from(d.as_nanos()).unwrap_or(u64::MAX);
        self.count.fetch_add(1, Ordering::Relaxed);
        self.total_ns.fetch_add(ns, Ordering::Relaxed);
        // Max via compare-exchange loop — monotonic.
        let mut cur = self.max_ns.load(Ordering::Relaxed);
        while ns > cur {
            match self.max_ns.compare_exchange_weak(
                cur,
                ns,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(v) => cur = v,
            }
        }
    }

    /// Increment the count without recording a duration — for counter-only
    /// stages (e.g. cache hit/miss).
    pub fn incr(&'static self) {
        self.register_once();
        self.count.fetch_add(1, Ordering::Relaxed);
    }

    /// Create an RAII timer that records elapsed time when dropped.
    #[must_use]
    pub fn timer(&'static self) -> StageTimer {
        self.register_once();
        StageTimer {
            stats: self,
            start: Instant::now(),
        }
    }

    #[must_use]
    pub fn snapshot(&self) -> StageSnapshot {
        let count = self.count.load(Ordering::Relaxed);
        let total_ns = self.total_ns.load(Ordering::Relaxed);
        let max_ns = self.max_ns.load(Ordering::Relaxed);
        StageSnapshot {
            name: self.name,
            count,
            total_ns,
            max_ns,
            mean_ns: if count > 0 { total_ns / count } else { 0 },
        }
    }

    #[must_use]
    pub const fn name(&self) -> &'static str {
        self.name
    }
}

/// RAII guard returned by [`StageStats::timer`]. On drop, records the
/// elapsed time since construction.
#[derive(Debug)]
pub struct StageTimer {
    stats: &'static StageStats,
    start: Instant,
}

impl Drop for StageTimer {
    fn drop(&mut self) {
        self.stats.record(self.start.elapsed());
    }
}

/// Snapshot of a stage's cumulative counters at one moment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StageSnapshot {
    pub name: &'static str,
    pub count: u64,
    pub total_ns: u64,
    pub mean_ns: u64,
    pub max_ns: u64,
}

static REGISTRY: Mutex<Vec<&'static StageStats>> = Mutex::new(Vec::new());

/// Collect a snapshot of every stage that has been touched at least once.
#[must_use]
pub fn snapshot_all() -> Vec<StageSnapshot> {
    let Ok(v) = REGISTRY.lock() else {
        return Vec::new();
    };
    v.iter().map(|s| s.snapshot()).collect()
}

/// Log a sorted dump of every registered stage's cumulative stats at INFO
/// level. Sorted by `total_ns` descending so the biggest time sinks come
/// first. Counter-only stages (`total_ns == 0`) appear at the bottom.
pub fn dump_to_tracing() {
    let mut snaps = snapshot_all();
    snaps.sort_by_key(|s| std::cmp::Reverse(s.total_ns));
    for s in &snaps {
        if s.total_ns > 0 {
            tracing::info!(
                stage = s.name,
                count = s.count,
                total_ms = s.total_ns / 1_000_000,
                mean_us = s.mean_ns / 1_000,
                max_ms = s.max_ns / 1_000_000,
                "timing:stage"
            );
        } else {
            tracing::info!(
                stage = s.name,
                count = s.count,
                "timing:counter"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    static TEST_RECORD: StageStats = StageStats::new("test.record");

    #[test]
    fn record_updates_count_total_and_max() {
        TEST_RECORD.record(Duration::from_millis(10));
        TEST_RECORD.record(Duration::from_millis(30));
        TEST_RECORD.record(Duration::from_millis(20));

        let s = TEST_RECORD.snapshot();
        assert_eq!(s.name, "test.record");
        assert_eq!(s.count, 3);
        // Allow for either ms resolution or sub-ms jitter; check total is
        // at least 60ms.
        assert!(s.total_ns >= 60 * 1_000_000, "total_ns={}", s.total_ns);
        assert!(s.max_ns >= 30 * 1_000_000, "max_ns={}", s.max_ns);
        assert!(s.mean_ns >= 20 * 1_000_000, "mean_ns={}", s.mean_ns);
    }

    static TEST_TIMER: StageStats = StageStats::new("test.timer");

    #[test]
    fn timer_records_elapsed_on_drop() {
        let before = TEST_TIMER.snapshot().count;
        {
            let _t = TEST_TIMER.timer();
            std::thread::sleep(Duration::from_millis(5));
        }
        let after = TEST_TIMER.snapshot();
        assert_eq!(after.count, before + 1);
        assert!(after.max_ns >= 5 * 1_000_000, "max_ns={}", after.max_ns);
    }

    static TEST_COUNTER: StageStats = StageStats::new("test.counter");

    #[test]
    fn incr_does_not_affect_durations() {
        TEST_COUNTER.incr();
        TEST_COUNTER.incr();
        TEST_COUNTER.incr();
        let s = TEST_COUNTER.snapshot();
        assert_eq!(s.count, 3);
        assert_eq!(s.total_ns, 0);
        assert_eq!(s.max_ns, 0);
        assert_eq!(s.mean_ns, 0);
    }

    static TEST_REGISTRY_A: StageStats = StageStats::new("test.registry.a");
    static TEST_REGISTRY_B: StageStats = StageStats::new("test.registry.b");

    #[test]
    fn registered_stages_appear_in_snapshot_all() {
        // Trigger registration.
        TEST_REGISTRY_A.incr();
        TEST_REGISTRY_B.record(Duration::from_millis(1));
        let names: Vec<&str> = snapshot_all().iter().map(|s| s.name).collect();
        assert!(
            names.contains(&"test.registry.a"),
            "missing registry.a in {names:?}"
        );
        assert!(
            names.contains(&"test.registry.b"),
            "missing registry.b in {names:?}"
        );
    }
}
