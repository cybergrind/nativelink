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

/// Dynamic-name counter registry. Used when the stage name is only known
/// at runtime (e.g. per-worker dispatch counters like
/// `scheduler.dispatch.to.192.168.88.133`). Names leak into a static slab
/// via `Box::leak` so they satisfy the `&'static str` requirement of
/// `StageStats`. Leak is bounded — stages are cardinality-controlled by
/// the caller (one entry per worker machine, typically ≤ 10).
static DYN_COUNTERS: Mutex<Vec<&'static StageStats>> = Mutex::new(Vec::new());

/// Look up or lazily create a `StageStats` for `name` and increment it.
/// First call for a given name leaks the name and an allocated
/// `StageStats` (one-time per distinct name); subsequent calls reuse the
/// leaked static. Intended for per-worker / per-target counters whose
/// names come from runtime data.
pub fn dyn_counter_incr(name: &str) {
    // Fast path: scan the dynamic registry for an existing match.
    if let Ok(list) = DYN_COUNTERS.lock() {
        if let Some(stats) = list.iter().find(|s| s.name() == name) {
            stats.incr();
            return;
        }
    }
    // Slow path: allocate + leak a fresh stage, insert, increment.
    let leaked_name: &'static str = Box::leak(name.to_string().into_boxed_str());
    let leaked: &'static StageStats = Box::leak(Box::new(StageStats::new(leaked_name)));
    if let Ok(mut list) = DYN_COUNTERS.lock() {
        // Recheck in case another caller raced us.
        if let Some(existing) = list.iter().find(|s| s.name() == name) {
            existing.incr();
            return;
        }
        list.push(leaked);
    }
    leaked.incr();
}

/// Collect a snapshot of every stage that has been touched at least once.
#[must_use]
pub fn snapshot_all() -> Vec<StageSnapshot> {
    let Ok(v) = REGISTRY.lock() else {
        return Vec::new();
    };
    v.iter().map(|s| s.snapshot()).collect()
}

/// Process-level user/sys CPU time (Unix only, reads `getrusage`).
/// On non-Unix (Windows), returns zeros — the CPU-split diagnostic is
/// Unix-targeted. Used by [`dump_to_tracing`] to emit a `timing:cpu`
/// line each period showing `user_ms` and `sys_ms` deltas. A high
/// `sys_ms` share relative to `user_ms` points at syscall-heavy work
/// (Redis RTTs, process fork/exec, mutex contention) rather than
/// CPU-bound user code.
#[must_use]
pub fn process_cpu_times() -> (Duration, Duration) {
    #[cfg(unix)]
    {
        // SAFETY: getrusage with RUSAGE_SELF and a valid rusage pointer
        // is always safe on Unix; failure is indicated via return value.
        let mut ru: libc::rusage = unsafe { std::mem::zeroed() };
        let rc = unsafe { libc::getrusage(libc::RUSAGE_SELF, &raw mut ru) };
        if rc != 0 {
            return (Duration::ZERO, Duration::ZERO);
        }
        let user = Duration::from_secs(ru.ru_utime.tv_sec as u64)
            + Duration::from_micros(ru.ru_utime.tv_usec as u64);
        let sys = Duration::from_secs(ru.ru_stime.tv_sec as u64)
            + Duration::from_micros(ru.ru_stime.tv_usec as u64);
        (user, sys)
    }
    #[cfg(not(unix))]
    {
        (Duration::ZERO, Duration::ZERO)
    }
}

static LAST_CPU_SAMPLE: Mutex<Option<(Duration, Duration, Instant)>> =
    Mutex::new(None);

/// Log a sorted dump of every registered stage's cumulative stats at INFO
/// level. Sorted by `total_ns` descending so the biggest time sinks come
/// first. Counter-only stages (`total_ns == 0`) appear at the bottom.
///
/// Also emits a `timing:cpu` line with the `user_ms` / `sys_ms` delta
/// since the last dump plus the derived `sys_pct` — the single clearest
/// signal of whether the process is syscall-bound. If `sys_pct > 50`,
/// the stages with the highest `total_ms` are probably doing blocking
/// I/O / locking / fork-exec rather than CPU-bound work.
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

    let (user, sys) = process_cpu_times();
    let now = Instant::now();
    if let Ok(mut slot) = LAST_CPU_SAMPLE.lock() {
        if let Some((prev_user, prev_sys, prev_t)) = *slot {
            let user_delta = user.saturating_sub(prev_user);
            let sys_delta = sys.saturating_sub(prev_sys);
            let wall_delta = now.saturating_duration_since(prev_t);
            let total_ms = (user_delta + sys_delta).as_millis() as u64;
            let sys_pct = if total_ms > 0 {
                (sys_delta.as_millis() as u64 * 100) / total_ms
            } else {
                0
            };
            tracing::info!(
                user_ms = user_delta.as_millis() as u64,
                sys_ms = sys_delta.as_millis() as u64,
                wall_ms = wall_delta.as_millis() as u64,
                sys_pct,
                "timing:cpu"
            );
        }
        *slot = Some((user, sys, now));
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

    #[cfg(unix)]
    #[test]
    fn process_cpu_times_increases_after_busy_work() {
        let (u0, s0) = process_cpu_times();
        // Burn user CPU in a tight loop.
        let mut acc: u64 = 0;
        for i in 0..5_000_000u64 {
            acc = acc.wrapping_add(i.wrapping_mul(0x9E3779B97F4A7C15));
        }
        std::hint::black_box(acc);
        let (u1, s1) = process_cpu_times();
        assert!(
            u1 >= u0,
            "user time must not decrease; u0={u0:?} u1={u1:?}"
        );
        assert!(s1 >= s0, "sys time must not decrease");
        assert!(
            u1 > u0,
            "busy loop must produce measurable user-time delta; u0={u0:?} u1={u1:?}"
        );
    }

    #[test]
    fn dyn_counter_registers_and_accumulates() {
        dyn_counter_incr("test.dyn.alpha");
        dyn_counter_incr("test.dyn.alpha");
        dyn_counter_incr("test.dyn.beta");
        let snaps = snapshot_all();
        let alpha = snaps
            .iter()
            .find(|s| s.name == "test.dyn.alpha")
            .expect("alpha not registered");
        let beta = snaps
            .iter()
            .find(|s| s.name == "test.dyn.beta")
            .expect("beta not registered");
        assert_eq!(alpha.count, 2, "alpha count");
        assert_eq!(beta.count, 1, "beta count");
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
