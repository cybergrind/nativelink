// Copyright 2026 The NativeLink Authors. All rights reserved.
//
// Licensed under the Functional Source License, Version 1.1, Apache 2.0 Future License.

//! Process-wide single-flight for `download_to_directory`.
//!
//! Without this, N concurrent actions targeting the same `(path,
//! digest)` (the typical shared-tree shape: `InputRootAbsolutePath`
//! shared across every worker on the host) each independently do the
//! recursive walk, fetch every Directory proto, hash every file. Plan L
//! only marks subtrees walked AFTER the walk completes — by then the
//! herd has already duplicated the work.
//!
//! The coalescer collapses N concurrent `(path, digest)` calls into one
//! actual walk + (N-1) cheap `notify`-based waits. Followers re-check
//! Plan L when the leader signals; on success they fast-path out, on
//! failure they fall back to running their own walk so a transient
//! error doesn't permanently mark the path "broken."

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use nativelink_util::common::DigestInfo;
use parking_lot::Mutex;
use tokio::sync::Notify;

/// Per-`(path, digest)` rendezvous point. Created by the leader,
/// cloned (Arc) into every follower's `Wait` outcome. The leader
/// signals completion by dropping the `LeadGuard`, which sets `done`
/// and notifies all waiters at once.
#[derive(Debug)]
pub struct DirWalkSlot {
    notify: Notify,
    done: AtomicBool,
}

impl DirWalkSlot {
    fn new() -> Self {
        Self {
            notify: Notify::new(),
            done: AtomicBool::new(false),
        }
    }

    /// Race-safe wait. Returns when the leader has signalled
    /// completion (either by drop or by explicit complete).
    pub async fn wait(&self) {
        // Standard Notify pattern: register the waiter (`notified()`
        // + `enable()`) BEFORE re-checking `done`. Otherwise a race
        // between the leader's `notify_waiters()` and our load could
        // wedge the follower forever.
        loop {
            if self.done.load(Ordering::Acquire) {
                return;
            }
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.done.load(Ordering::Acquire) {
                return;
            }
            notified.await;
        }
    }
}

/// Outcome of a coalesce attempt. Leaders own a guard whose Drop
/// cleans up the slot; followers own an Arc to the same slot and
/// `await` it.
#[derive(Debug)]
pub enum CoalesceOutcome {
    /// You are the first caller for this `(path, digest)`. Do the
    /// walk; the guard auto-cleans on drop. Other callers will wait
    /// for you.
    Lead(LeadGuard),
    /// Another caller is already walking this `(path, digest)`. Await
    /// the slot and then re-check Plan L; if it hit, you can skip
    /// your own walk.
    Follow(Arc<DirWalkSlot>),
}

/// RAII handle held by the leader. On drop:
///   1. removes the slot from the in-flight map (so future calls can
///      lead instead of joining a stale slot),
///   2. flips `done` to `true` and `notify_waiters()` so every
///      follower wakes up.
///
/// This intentionally fires whether the leader's walk succeeded or
/// failed — followers re-check Plan L to know which case they're in.
/// Plan L is marked only on success, so:
///   - success → followers find Plan L hit → fast-path return.
///   - failure → followers find Plan L miss → fall through to their
///     own walk (no de-dup on the failure path, but no false success
///     either).
#[derive(Debug)]
pub struct LeadGuard {
    key: (PathBuf, DigestInfo),
    coalescer: Arc<DirWalkCoalescer>,
    slot: Arc<DirWalkSlot>,
}

impl Drop for LeadGuard {
    fn drop(&mut self) {
        self.coalescer.in_flight.lock().remove(&self.key);
        self.slot.done.store(true, Ordering::Release);
        self.slot.notify.notify_waiters();
    }
}

/// Process-shared single-flight registry. Cheap: one `Mutex<HashMap>`
/// + one `Arc<Slot>` per in-flight walk. Slots vanish when their
/// leader's guard is dropped, so memory usage tracks active walks
/// (not historical paths).
#[derive(Debug, Default)]
pub struct DirWalkCoalescer {
    in_flight: Mutex<HashMap<(PathBuf, DigestInfo), Arc<DirWalkSlot>>>,
}

impl DirWalkCoalescer {
    pub fn new() -> Self {
        Self::default()
    }

    /// Atomically claim or join the `(path, digest)` slot. If empty,
    /// you're the leader and get a guard. If occupied, you're a
    /// follower and get a slot to await.
    ///
    /// Must be called as `&Arc<Self>` so the leader's guard can hold a
    /// strong ref to the coalescer (needed for slot cleanup on drop).
    pub fn try_lead_or_follow(
        self: &Arc<Self>,
        path: &Path,
        digest: DigestInfo,
    ) -> CoalesceOutcome {
        let key = (path.to_path_buf(), digest);
        let mut g = self.in_flight.lock();
        if let Some(existing) = g.get(&key) {
            return CoalesceOutcome::Follow(existing.clone());
        }
        let slot = Arc::new(DirWalkSlot::new());
        g.insert(key.clone(), slot.clone());
        CoalesceOutcome::Lead(LeadGuard {
            key,
            coalescer: self.clone(),
            slot,
        })
    }

    /// Diagnostic only: how many slots are currently occupied. Used
    /// by tests to assert the coalescer cleaned up after itself.
    #[cfg(test)]
    fn in_flight_count(&self) -> usize {
        self.in_flight.lock().len()
    }
}

impl CoalesceOutcome {
    /// Wait for the leader to finish. Only meaningful on `Follow`.
    pub async fn wait_if_follower(self) -> WaitResult {
        match self {
            CoalesceOutcome::Lead(guard) => WaitResult::Led(guard),
            CoalesceOutcome::Follow(slot) => {
                slot.wait().await;
                WaitResult::Followed
            }
        }
    }
}

/// Result of `wait_if_follower`. The leader gets the guard back to
/// hold for the duration of its own walk; the follower gets a marker
/// so the caller knows to re-check Plan L.
#[derive(Debug)]
pub enum WaitResult {
    Led(LeadGuard),
    Followed,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicU64;
    use std::time::Duration;

    /// Slice 1 of 1.3.2: N concurrent calls with the same `(path,
    /// digest)` must result in exactly ONE inner future running. The
    /// other (N-1) must coalesce and return after the leader finishes.
    /// This is the central correctness property — the whole point of
    /// the coalescer.
    #[tokio::test]
    async fn concurrent_callers_with_same_key_run_inner_future_once() {
        let coalescer = Arc::new(DirWalkCoalescer::new());
        let path = PathBuf::from("/shared/src/some/dep");
        let digest = DigestInfo::new([0xCC; 32], 4096);
        let inner_runs = Arc::new(AtomicU64::new(0));

        const N: usize = 16;
        let mut joins = Vec::with_capacity(N);
        for _ in 0..N {
            let coalescer = coalescer.clone();
            let path = path.clone();
            let inner_runs = inner_runs.clone();
            joins.push(tokio::spawn(async move {
                let outcome = coalescer.try_lead_or_follow(&path, digest);
                match outcome.wait_if_follower().await {
                    WaitResult::Led(_guard) => {
                        // We're the leader: simulate the actual walk
                        // (slow enough that all followers definitely
                        // arrive while we're still working).
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        inner_runs.fetch_add(1, Ordering::Relaxed);
                        // Guard drops here, signalling waiters.
                    }
                    WaitResult::Followed => {
                        // Coalesced — nothing to do, leader did it.
                    }
                }
            }));
        }
        for j in joins {
            j.await.unwrap();
        }
        assert_eq!(
            inner_runs.load(Ordering::Relaxed),
            1,
            "all {N} concurrent callers must collapse to exactly one inner walk",
        );
        // Leader's drop must clean the slot so future calls re-lead.
        assert_eq!(
            coalescer.in_flight_count(),
            0,
            "slot must be cleared after leader finishes",
        );
    }

    /// Distinct keys must NOT coalesce — each gets its own leader.
    /// Otherwise the coalescer would block unrelated walks.
    #[tokio::test]
    async fn distinct_keys_each_get_their_own_leader() {
        let coalescer = Arc::new(DirWalkCoalescer::new());
        let inner_runs = Arc::new(AtomicU64::new(0));

        let mut joins = Vec::new();
        for i in 0..8u64 {
            let coalescer = coalescer.clone();
            let inner_runs = inner_runs.clone();
            joins.push(tokio::spawn(async move {
                let path = PathBuf::from(format!("/shared/src/dep_{i}"));
                let digest = DigestInfo::new([(i as u8); 32], i * 100);
                let outcome = coalescer.try_lead_or_follow(&path, digest);
                if let WaitResult::Led(_guard) = outcome.wait_if_follower().await {
                    inner_runs.fetch_add(1, Ordering::Relaxed);
                }
            }));
        }
        for j in joins {
            j.await.unwrap();
        }
        assert_eq!(
            inner_runs.load(Ordering::Relaxed),
            8,
            "8 distinct keys must produce 8 leaders",
        );
        assert_eq!(coalescer.in_flight_count(), 0);
    }

    /// After the leader finishes, a brand-new call for the same key
    /// must be allowed to lead (slot was cleared). Otherwise we'd
    /// permanently coalesce against a stale slot that nobody will
    /// ever signal.
    #[tokio::test]
    async fn new_caller_after_leader_finishes_becomes_new_leader() {
        let coalescer = Arc::new(DirWalkCoalescer::new());
        let path = PathBuf::from("/some/path");
        let digest = DigestInfo::new([0x11; 32], 1);

        // First leader runs and drops.
        {
            let outcome = coalescer.try_lead_or_follow(&path, digest);
            match outcome {
                CoalesceOutcome::Lead(_) => {}
                _ => panic!("first call must be leader"),
            }
        }

        // Second call — slot should be empty, so we lead again.
        let outcome = coalescer.try_lead_or_follow(&path, digest);
        match outcome {
            CoalesceOutcome::Lead(_) => {}
            CoalesceOutcome::Follow(_) => {
                panic!("second call after leader's drop must lead, not follow")
            }
        }
    }

    /// A late-arriving follower (registers its `notified()` AFTER the
    /// leader has already called `notify_waiters` and dropped) must
    /// still observe `done=true` and return immediately, not wedge
    /// forever. This is the race the `done` AtomicBool guards
    /// against.
    #[tokio::test]
    async fn follower_arriving_after_leader_completion_does_not_wedge() {
        let coalescer = Arc::new(DirWalkCoalescer::new());
        let path = PathBuf::from("/race/path");
        let digest = DigestInfo::new([0x22; 32], 1);

        // Leader claims the slot.
        let CoalesceOutcome::Lead(guard) = coalescer.try_lead_or_follow(&path, digest)
        else {
            panic!("first call must be leader");
        };
        // Save the slot Arc so followers can still see it after the
        // leader's drop removes it from the in-flight map.
        let slot_for_follower = match coalescer.try_lead_or_follow(&path, digest) {
            CoalesceOutcome::Follow(slot) => slot,
            _ => panic!("second call must be follower while leader holds guard"),
        };

        // Leader completes BEFORE the follower starts its `wait()`.
        drop(guard);

        // Follower waits. With the race guard (`done` AtomicBool +
        // re-check inside `wait()`), this returns immediately rather
        // than wedging on a notify that already fired.
        tokio::time::timeout(Duration::from_millis(500), slot_for_follower.wait())
            .await
            .expect("follower must not wedge after leader's already-completed signal");
    }
}
