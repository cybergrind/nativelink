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

//! Digest-keyed LRU cache for small hot CAS blobs.
//!
//! Target use: the ByteStream server on a scheduler-host that serves the
//! same blobs (libc++ headers, generated mojom, toolchain binaries) many
//! times during a cold build. Each cache hit replaces an
//! `open+read+send` disk cycle with an `Arc::clone` of cached `Bytes` —
//! the single largest pressure reducer identified in report #09.
//!
//! Design:
//! - `Mutex<lru::LruCache>`-guarded. A single mutex is acceptable at our
//!   scale (hundreds of entries, fetches are µs with memory already hot).
//! - Per-blob size cap: blobs over the cap are never inserted. Prevents
//!   one 50MB `.pcm` from evicting hundreds of small headers.
//! - Total-bytes budget: `put` evicts LRU entries until `current_bytes +
//!   incoming <= max_total_bytes`. If the incoming blob alone exceeds the
//!   budget (shouldn't, given the per-blob cap), it's rejected.
//! - CAS content is content-addressed: a given digest always maps to the
//!   same bytes forever. No invalidation logic needed.

use std::sync::Mutex;

use bytes::Bytes;
use lru::LruCache;

use crate::common::DigestInfo;

/// LRU cache for content-addressed blobs. Thread-safe via internal mutex.
#[derive(Debug)]
pub struct DigestLruCache {
    inner: Mutex<Inner>,
    max_total_bytes: usize,
    max_blob_bytes: usize,
}

#[derive(Debug)]
struct Inner {
    map: LruCache<DigestInfo, Bytes>,
    current_bytes: usize,
}

impl DigestLruCache {
    /// `max_total_bytes` bounds the aggregate bytes across all cached
    /// entries; `max_blob_bytes` rejects any individual blob larger than
    /// this threshold (so one huge blob can't monopolize the cache).
    /// `max_total_bytes` must be ≥ `max_blob_bytes`; a sane default is
    /// a 200:1 ratio (e.g. 200 MB total, 1 MB per-blob cap).
    #[must_use]
    pub fn new(max_total_bytes: usize, max_blob_bytes: usize) -> Self {
        // Unbounded entry count: we enforce eviction by byte budget, not
        // by entry count. `lru::LruCache::unbounded` creates a cache with
        // no entry-count limit.
        Self {
            inner: Mutex::new(Inner {
                map: LruCache::unbounded(),
                current_bytes: 0,
            }),
            max_total_bytes,
            max_blob_bytes,
        }
    }

    /// Look up a cached blob. Returns `Some(bytes)` on hit (cloning the
    /// `Bytes`, which is `Arc`-backed so this is O(1) refcount increment)
    /// and updates LRU recency. Returns `None` on miss.
    #[must_use]
    pub fn get(&self, digest: &DigestInfo) -> Option<Bytes> {
        let Ok(mut inner) = self.inner.lock() else {
            return None;
        };
        inner.map.get(digest).cloned()
    }

    /// Insert a blob. Rejected (silently) if `bytes.len() >
    /// max_blob_bytes`. Otherwise evicts LRU entries until the incoming
    /// blob fits within the total-bytes budget. If the same digest is
    /// already cached, the entry is replaced (LRU recency refreshed).
    pub fn put(&self, digest: DigestInfo, bytes: Bytes) {
        let incoming = bytes.len();
        if incoming > self.max_blob_bytes {
            return;
        }
        if incoming > self.max_total_bytes {
            return;
        }
        let Ok(mut inner) = self.inner.lock() else {
            return;
        };
        // If the key is already present, account for its current size so
        // replacement doesn't double-count.
        if let Some(existing) = inner.map.peek(&digest) {
            inner.current_bytes = inner.current_bytes.saturating_sub(existing.len());
        }
        // Evict LRU until there's room for the incoming blob.
        while inner.current_bytes + incoming > self.max_total_bytes {
            let Some((_, evicted)) = inner.map.pop_lru() else {
                break;
            };
            inner.current_bytes = inner.current_bytes.saturating_sub(evicted.len());
        }
        inner.current_bytes += incoming;
        inner.map.put(digest, bytes);
    }

    /// Number of cached entries.
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner.lock().map_or(0, |i| i.map.len())
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Aggregate bytes currently held.
    #[must_use]
    pub fn current_bytes(&self) -> usize {
        self.inner.lock().map_or(0, |i| i.current_bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mk(byte: u8, size: usize) -> (DigestInfo, Bytes) {
        let digest = DigestInfo::new([byte; 32], size as u64);
        let bytes = Bytes::from(vec![byte; size]);
        (digest, bytes)
    }

    #[test]
    fn get_miss_returns_none() {
        let cache = DigestLruCache::new(1024, 1024);
        let (d, _) = mk(1, 10);
        assert_eq!(cache.get(&d), None);
        assert_eq!(cache.len(), 0);
        assert_eq!(cache.current_bytes(), 0);
    }

    #[test]
    fn put_then_get_returns_same_bytes() {
        let cache = DigestLruCache::new(1024, 1024);
        let (d, b) = mk(1, 10);
        cache.put(d, b.clone());
        assert_eq!(cache.get(&d), Some(b));
        assert_eq!(cache.len(), 1);
        assert_eq!(cache.current_bytes(), 10);
    }

    #[test]
    fn put_oversized_blob_is_rejected_silently() {
        let cache = DigestLruCache::new(1024, 64);
        let (d, b) = mk(1, 128);
        cache.put(d, b); // 128 > max_blob_bytes=64
        assert_eq!(cache.get(&d), None);
        assert_eq!(cache.len(), 0);
        assert_eq!(cache.current_bytes(), 0);
    }

    #[test]
    fn put_evicts_oldest_to_fit_total_budget() {
        // Budget: 30 bytes total, blob cap 15.
        let cache = DigestLruCache::new(30, 15);
        let (d1, b1) = mk(1, 10);
        let (d2, b2) = mk(2, 10);
        let (d3, b3) = mk(3, 10);
        cache.put(d1, b1);
        cache.put(d2, b2);
        cache.put(d3, b3);
        assert_eq!(cache.len(), 3, "30 = 10*3 fits");
        assert_eq!(cache.current_bytes(), 30);

        // Inserting a 4th forces eviction of d1 (least-recently-used).
        let (d4, b4) = mk(4, 10);
        cache.put(d4, b4);
        assert_eq!(cache.len(), 3);
        assert_eq!(cache.current_bytes(), 30);
        assert_eq!(cache.get(&d1), None, "d1 should have been evicted");
        assert!(cache.get(&d4).is_some(), "d4 should be present");
    }

    #[test]
    fn get_updates_recency_so_accessed_entries_survive_eviction() {
        let cache = DigestLruCache::new(30, 15);
        let (d1, b1) = mk(1, 10);
        let (d2, b2) = mk(2, 10);
        let (d3, b3) = mk(3, 10);
        cache.put(d1, b1);
        cache.put(d2, b2);
        cache.put(d3, b3);
        // Touch d1 so it becomes most-recently-used.
        let _ = cache.get(&d1);
        // Now inserting a 4th should evict d2 (now LRU), not d1.
        let (d4, b4) = mk(4, 10);
        cache.put(d4, b4);
        assert!(cache.get(&d1).is_some(), "d1 was touched, must survive");
        assert_eq!(cache.get(&d2), None, "d2 should have been evicted");
    }

    #[test]
    fn put_same_digest_twice_does_not_double_count_bytes() {
        let cache = DigestLruCache::new(100, 100);
        let (d, b) = mk(1, 40);
        cache.put(d, b.clone());
        cache.put(d, b);
        assert_eq!(cache.len(), 1);
        assert_eq!(
            cache.current_bytes(),
            40,
            "replacing same digest should not accumulate bytes"
        );
    }

    #[test]
    fn blob_too_large_for_total_but_below_per_blob_cap_is_rejected() {
        // Pathological config: max_total < max_blob. A blob that fits
        // max_blob but not max_total must be rejected (not evict
        // everything and still fail).
        let cache = DigestLruCache::new(10, 100);
        let (d, b) = mk(1, 50);
        cache.put(d, b);
        assert_eq!(cache.get(&d), None);
        assert_eq!(cache.current_bytes(), 0);
    }
}
