// Copyright 2024 The NativeLink Authors. All rights reserved.
//
// Licensed under the Functional Source License, Version 1.1, Apache 2.0 Future License.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use nativelink_util::common::DigestInfo;
use parking_lot::Mutex;

/// Trait for the walked-dirs cache backend. The local implementation uses an
/// in-memory HashSet (per-worker). The Redis implementation shares state across
/// workers so that once any worker in the cluster walks a Directory subtree,
/// all other workers can skip it.
pub trait WalkedDirsProvider: Send + Sync + std::fmt::Debug {
    /// Returns true if the subtree rooted at the given Directory digest has
    /// been fully walked by any worker sharing this cache.
    fn dir_walked(&self, digest: &DigestInfo) -> bool;

    /// Records that the subtree rooted at the given Directory digest has been
    /// fully materialized. Called only after all child futures succeed.
    fn mark_dir_walked(&self, digest: DigestInfo);
}

/// In-memory walked-dirs provider. Per-worker, reset on restart.
#[derive(Debug, Default)]
pub struct LocalWalkedDirs {
    set: Mutex<HashSet<DigestInfo>>,
}

impl LocalWalkedDirs {
    pub fn new() -> Self {
        Self::default()
    }
}

impl WalkedDirsProvider for LocalWalkedDirs {
    fn dir_walked(&self, digest: &DigestInfo) -> bool {
        self.set.lock().contains(digest)
    }

    fn mark_dir_walked(&self, digest: DigestInfo) {
        self.set.lock().insert(digest);
    }
}

/// Redis-backed walked-dirs provider with an L1 in-memory cache.
/// Persists walked-dirs state in Redis so that it survives worker restarts.
///
/// The Redis key is namespaced per machine (hostname) so that workers on
/// different machines never trust each other's walks — each machine must
/// verify its own tree at least once. The value of sharing via Redis is
/// that a worker restart on the same machine doesn't lose the walked-dirs
/// state, avoiding the expensive re-walk of ~2000 Directory protobufs.
///
/// On `dir_walked`:
///   1. Check L1 (local HashSet) — sub-microsecond.
///   2. On miss, check Redis SISMEMBER — one network RTT.
///   3. On Redis hit, populate L1 so subsequent checks are free.
///
/// On `mark_dir_walked`:
///   1. Insert into L1.
///   2. Fire-and-forget SADD to Redis (best-effort; if Redis is down,
///      the worker still benefits from L1, just loses persistence).
#[derive(Debug)]
pub struct RedisWalkedDirs {
    l1: Mutex<HashSet<DigestInfo>>,
    redis_client: redis::Client,
    redis_key: String,
}

impl RedisWalkedDirs {
    /// Create a new Redis-backed walked-dirs provider.
    /// `url` is a Redis connection string (e.g. `redis://127.0.0.1:6379`).
    ///
    /// The Redis key is automatically namespaced by `machine_id`:
    /// `nativelink:walked_dirs:{machine_id}`. This ensures that workers on
    /// different machines never skip walks based on another machine's state,
    /// since the pre-staged source trees may differ.
    ///
    /// `machine_id` should be something unique per machine — typically the
    /// machine's IP address (e.g. `"192.168.88.133"`).
    pub fn new(url: &str, machine_id: &str) -> Result<Self, nativelink_error::Error> {
        let redis_client = redis::Client::open(url).map_err(|e| {
            nativelink_error::make_err!(
                nativelink_error::Code::Unavailable,
                "Failed to create Redis client for walked_dirs cache: {e}"
            )
        })?;
        let redis_key = format!("nativelink:walked_dirs:{machine_id}");
        Ok(Self {
            l1: Mutex::new(HashSet::new()),
            redis_client,
            redis_key,
        })
    }

    fn digest_to_member(digest: &DigestInfo) -> String {
        format!("{}-{}", digest.packed_hash(), digest.size_bytes())
    }
}

impl WalkedDirsProvider for RedisWalkedDirs {
    fn dir_walked(&self, digest: &DigestInfo) -> bool {
        // L1 hit — no network.
        if self.l1.lock().contains(digest) {
            return true;
        }

        // L2: synchronous Redis check via a short-lived blocking connection.
        // We use a blocking call here because the WalkedDirsProvider trait is
        // synchronous (called from within an async context but not on the
        // critical async path — the check is at the very top of
        // download_to_directory before any futures are spawned).
        let member = Self::digest_to_member(digest);
        let result: bool = (|| {
            let mut conn = self.redis_client.get_connection().ok()?;
            redis::cmd("SISMEMBER")
                .arg(&self.redis_key)
                .arg(&member)
                .query::<bool>(&mut conn)
                .ok()
        })()
        .unwrap_or(false);

        if result {
            // Populate L1 on Redis hit.
            self.l1.lock().insert(*digest);
        }

        result
    }

    fn mark_dir_walked(&self, digest: DigestInfo) {
        self.l1.lock().insert(digest);

        // Best-effort write to Redis. If Redis is down, only this worker
        // benefits from the walk — other workers re-walk. No correctness issue.
        let member = Self::digest_to_member(&digest);
        if let Ok(mut conn) = self.redis_client.get_connection() {
            drop(
                redis::cmd("SADD")
                    .arg(&self.redis_key)
                    .arg(&member)
                    .query::<i64>(&mut conn),
            );
        }
    }
}

/// Per-worker cache combining path->digest mapping and walked-dirs cache.
///
/// The path->digest map is always local (per-file paths are worker-specific).
/// The walked-dirs cache can be local or Redis-backed depending on config.
#[derive(Debug)]
pub struct PathDigestCache {
    map: Mutex<HashMap<PathBuf, DigestInfo>>,
    walked_dirs: Box<dyn WalkedDirsProvider>,
}

impl PathDigestCache {
    /// Create with local-only walked-dirs (default).
    pub fn new() -> Self {
        Self {
            map: Mutex::new(HashMap::new()),
            walked_dirs: Box::new(LocalWalkedDirs::new()),
        }
    }

    /// Create with a custom walked-dirs provider (e.g. Redis-backed).
    pub fn with_walked_dirs(walked_dirs: Box<dyn WalkedDirsProvider>) -> Self {
        Self {
            map: Mutex::new(HashMap::new()),
            walked_dirs,
        }
    }

    pub fn contains(&self, path: &Path, digest: &DigestInfo) -> bool {
        let map = self.map.lock();
        map.get(path).is_some_and(|d| d == digest)
    }

    pub fn insert(&self, path: PathBuf, digest: DigestInfo) {
        self.map.lock().insert(path, digest);
    }

    pub fn dir_walked(&self, digest: &DigestInfo) -> bool {
        self.walked_dirs.dir_walked(digest)
    }

    pub fn mark_dir_walked(&self, digest: DigestInfo) {
        self.walked_dirs.mark_dir_walked(digest);
    }

    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.map.lock().len()
    }
}

impl Default for PathDigestCache {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_cache_is_empty() {
        let cache = PathDigestCache::new();
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn test_contains_miss_on_empty() {
        let cache = PathDigestCache::new();
        let digest = DigestInfo::new([1u8; 32], 100);
        assert!(!cache.contains(Path::new("/foo/bar.cc"), &digest));
    }

    #[test]
    fn test_insert_then_contains_hit() {
        let cache = PathDigestCache::new();
        let digest = DigestInfo::new([2u8; 32], 200);
        let path = PathBuf::from("/src/third_party/clang");

        cache.insert(path.clone(), digest);
        assert!(cache.contains(&path, &digest));
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn test_contains_wrong_digest_is_miss() {
        let cache = PathDigestCache::new();
        let digest_a = DigestInfo::new([3u8; 32], 300);
        let digest_b = DigestInfo::new([4u8; 32], 400);
        let path = PathBuf::from("/src/foo.o");

        cache.insert(path.clone(), digest_a);
        assert!(!cache.contains(&path, &digest_b));
    }

    #[test]
    fn test_insert_overwrites_previous() {
        let cache = PathDigestCache::new();
        let digest_old = DigestInfo::new([5u8; 32], 500);
        let digest_new = DigestInfo::new([6u8; 32], 600);
        let path = PathBuf::from("/src/gen/foo.h");

        cache.insert(path.clone(), digest_old);
        cache.insert(path.clone(), digest_new);
        assert!(cache.contains(&path, &digest_new));
        assert!(!cache.contains(&path, &digest_old));
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn test_dir_walked_miss_on_empty() {
        let cache = PathDigestCache::new();
        let digest = DigestInfo::new([7u8; 32], 700);
        assert!(!cache.dir_walked(&digest));
    }

    #[test]
    fn test_mark_dir_walked_then_hit() {
        let cache = PathDigestCache::new();
        let digest = DigestInfo::new([8u8; 32], 800);

        cache.mark_dir_walked(digest);
        assert!(cache.dir_walked(&digest));
    }

    #[test]
    fn test_dir_walked_different_digest_is_miss() {
        let cache = PathDigestCache::new();
        let digest_a = DigestInfo::new([9u8; 32], 900);
        let digest_b = DigestInfo::new([10u8; 32], 1000);

        cache.mark_dir_walked(digest_a);
        assert!(!cache.dir_walked(&digest_b));
    }

    #[test]
    fn test_with_custom_walked_dirs_provider() {
        let provider = Box::new(LocalWalkedDirs::new());
        let cache = PathDigestCache::with_walked_dirs(provider);
        let digest = DigestInfo::new([11u8; 32], 1100);

        assert!(!cache.dir_walked(&digest));
        cache.mark_dir_walked(digest);
        assert!(cache.dir_walked(&digest));
    }

    #[test]
    fn test_redis_walked_dirs_digest_to_member() {
        let digest = DigestInfo::new([0xABu8; 32], 42);
        let member = RedisWalkedDirs::digest_to_member(&digest);
        assert!(member.ends_with("-42"));
        assert!(member.len() > 3); // hash + "-42"
    }
}
