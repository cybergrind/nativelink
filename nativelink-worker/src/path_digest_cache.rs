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
    /// been fully walked into `directory_path` on this machine.
    /// Path-aware: walking digest D into path A does NOT imply D was walked
    /// into path B.
    fn dir_walked(&self, directory_path: &str, digest: &DigestInfo) -> bool;

    /// Records that the subtree rooted at the given Directory digest has been
    /// fully materialized at `directory_path`. Called only after all child
    /// futures succeed.
    fn mark_dir_walked(&self, directory_path: &str, digest: DigestInfo);
}

/// In-memory walked-dirs provider. Per-worker, reset on restart.
/// Keys are `(directory_path, digest)` pairs so the same digest walked into
/// different target paths are tracked independently.
#[derive(Debug, Default)]
pub struct LocalWalkedDirs {
    set: Mutex<HashSet<(String, DigestInfo)>>,
}

impl LocalWalkedDirs {
    pub fn new() -> Self {
        Self::default()
    }
}

impl WalkedDirsProvider for LocalWalkedDirs {
    fn dir_walked(&self, directory_path: &str, digest: &DigestInfo) -> bool {
        self.set.lock().contains(&(directory_path.to_string(), *digest))
    }

    fn mark_dir_walked(&self, directory_path: &str, digest: DigestInfo) {
        self.set.lock().insert((directory_path.to_string(), digest));
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
pub struct RedisWalkedDirs {
    l1: Mutex<HashSet<(String, DigestInfo)>>,
    redis_client: redis::Client,
    conn: Mutex<Option<redis::Connection>>,
    redis_key: String,
}

impl std::fmt::Debug for RedisWalkedDirs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisWalkedDirs")
            .field("redis_key", &self.redis_key)
            .finish_non_exhaustive()
    }
}

impl RedisWalkedDirs {
    /// Create a new Redis-backed walked-dirs provider.
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
            conn: Mutex::new(None),
            redis_key,
        })
    }

    fn digest_to_member(digest: &DigestInfo) -> String {
        format!("{}-{}", digest.packed_hash(), digest.size_bytes())
    }
}

impl WalkedDirsProvider for RedisWalkedDirs {
    fn dir_walked(&self, directory_path: &str, digest: &DigestInfo) -> bool {
        let key = (directory_path.to_string(), *digest);
        if self.l1.lock().contains(&key) {
            return true;
        }

        let member = format!("{}|{}", directory_path, Self::digest_to_member(digest));
        let result: bool = {
            let mut slot = self.conn.lock();
            if slot.is_none() {
                *slot = self.redis_client.get_connection().ok();
            }
            match slot.as_mut() {
                Some(conn) => match redis::cmd("SISMEMBER")
                    .arg(&self.redis_key)
                    .arg(&member)
                    .query::<bool>(conn)
                {
                    Ok(v) => v,
                    Err(_) => {
                        *slot = None;
                        false
                    }
                },
                None => false,
            }
        };

        if result {
            self.l1.lock().insert(key);
        }
        result
    }

    fn mark_dir_walked(&self, directory_path: &str, digest: DigestInfo) {
        self.l1.lock().insert((directory_path.to_string(), digest));

        let member = format!("{}|{}", directory_path, Self::digest_to_member(&digest));
        let mut slot = self.conn.lock();
        if slot.is_none() {
            if let Ok(c) = self.redis_client.get_connection() {
                *slot = Some(c);
            } else {
                return;
            }
        }
        if let Some(conn) = slot.as_mut() {
            if redis::cmd("SADD")
                .arg(&self.redis_key)
                .arg(&member)
                .query::<i64>(conn)
                .is_err()
            {
                *slot = None;
            }
        }
    }
}

/// Handles cross-machine output synchronization via Redis.
///
/// After an action completes, the worker records output file paths + digests
/// in per-target-machine Redis LISTs. Before the next action starts on any
/// machine, the worker drains its pending list and the caller fetches missing
/// files from CAS into the local shared tree.
///
/// This ensures outputs produced on machine A become visible on machine B's
/// filesystem BEFORE machine B's next action — solving the split-filesystem
/// problem in shared-tree (Plan J) mode.
pub struct RedisOutputSync {
    redis_client: redis::Client,
    conn: Mutex<Option<redis::Connection>>,
    machine_id: String,
}

impl std::fmt::Debug for RedisOutputSync {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisOutputSync")
            .field("machine_id", &self.machine_id)
            .finish_non_exhaustive()
    }
}

impl RedisOutputSync {
    pub fn new(url: &str, machine_id: &str) -> Result<Self, nativelink_error::Error> {
        let redis_client = redis::Client::open(url).map_err(|e| {
            nativelink_error::make_err!(
                nativelink_error::Code::Unavailable,
                "Failed to create Redis client for output sync: {e}"
            )
        })?;
        let this = Self {
            redis_client,
            conn: Mutex::new(None),
            machine_id: machine_id.to_string(),
        };
        // Register this machine so others can discover it.
        drop(this.with_conn(|conn| {
            redis::cmd("SADD")
                .arg("nativelink:machines")
                .arg(&this.machine_id)
                .query::<i64>(conn)
        }));
        Ok(this)
    }

    /// Runs `op` with a cached Redis connection, reconnecting on error.
    fn with_conn<T, F>(&self, op: F) -> Result<T, redis::RedisError>
    where
        F: FnOnce(&mut redis::Connection) -> Result<T, redis::RedisError>,
    {
        let mut slot = self.conn.lock();
        if slot.is_none() {
            *slot = Some(self.redis_client.get_connection()?);
        }
        let conn = slot.as_mut().expect("just populated");
        match op(conn) {
            Ok(v) => Ok(v),
            Err(e) => {
                *slot = None;
                Err(e)
            }
        }
    }

    /// Record outputs produced by this machine. Pushes entries to
    /// `nativelink:pending_outputs:{target}` for every OTHER registered machine.
    pub fn record_outputs(&self, outputs: &[(String, DigestInfo)]) {
        if outputs.is_empty() {
            return;
        }
        drop(self.with_conn(|conn| {
            // Discover other machines.
            let other_machines: Vec<String> = redis::cmd("SMEMBERS")
                .arg("nativelink:machines")
                .query(conn)
                .unwrap_or_default();
            for target in &other_machines {
                if target == &self.machine_id {
                    continue;
                }
                let key = format!("nativelink:pending_outputs:{target}");
                let mut cmd = redis::cmd("RPUSH");
                cmd.arg(&key);
                for (path, digest) in outputs {
                    let member = format!(
                        "{}|{}-{}",
                        path,
                        digest.packed_hash(),
                        digest.size_bytes()
                    );
                    cmd.arg(member);
                }
                drop(cmd.query::<i64>(conn));
            }
            Ok::<(), redis::RedisError>(())
        }));
    }

    /// Parse a 64-char hex string into [u8; 32]. Returns None on invalid input.
    fn hex_to_32(s: &str) -> Option<[u8; 32]> {
        if s.len() != 64 {
            return None;
        }
        let mut arr = [0u8; 32];
        for i in 0..32 {
            arr[i] = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16).ok()?;
        }
        Some(arr)
    }

    /// Drain pending outputs that OTHER machines produced for THIS machine.
    /// Returns `(relative_path, DigestInfo)` pairs that need to be fetched
    /// from CAS and written to the local shared tree.
    pub fn drain_pending_outputs(&self) -> Vec<(String, DigestInfo)> {
        let key = format!("nativelink:pending_outputs:{}", self.machine_id);
        let entries: Vec<String> = self
            .with_conn(|conn| {
                redis::pipe()
                    .atomic()
                    .cmd("LRANGE")
                    .arg(&key)
                    .arg(0i64)
                    .arg(-1i64)
                    .cmd("DEL")
                    .arg(&key)
                    .ignore()
                    .query(conn)
            })
            .unwrap_or_default();
        let mut result = Vec::with_capacity(entries.len());
        for entry in entries {
            // Format: "relative/path/to/file.o|<hex_hash>-<size>"
            if let Some((path, digest_str)) = entry.rsplit_once('|') {
                if let Some((hash_hex, size_str)) = digest_str.rsplit_once('-') {
                    if let (Some(hash_arr), Ok(size)) =
                        (Self::hex_to_32(hash_hex), size_str.parse::<u64>())
                    {
                        let digest = DigestInfo::new(hash_arr, size);
                        result.push((path.to_string(), digest));
                    }
                }
            }
        }
        result
    }

    /// Format the value stored at `worker_state:{machine_id}[path]`.
    /// Must match the format consumed by the scheduler's dispatch
    /// journaler (see nativelink-scheduler/src/dir_index_resolver.rs).
    pub fn worker_state_value(digest: &DigestInfo) -> String {
        format!("{}-{}", digest.packed_hash(), digest.size_bytes())
    }

    /// Record that this worker now has `path` at the given digest. Writes
    /// `HSET worker_state:{machine_id} path "{digest_hex}-{size}"`.
    /// Best-effort: on Redis failure the update is silently dropped and
    /// the scheduler will re-publish this file on the next action.
    pub fn update_worker_state(&self, path: &str, digest: &DigestInfo) {
        let key = format!("nativelink:worker_state:{}", self.machine_id);
        let value = Self::worker_state_value(digest);
        drop(self.with_conn(|conn| {
            redis::cmd("HSET")
                .arg(&key)
                .arg(path)
                .arg(&value)
                .query::<i64>(conn)
        }));
    }

    /// Batch variant — writes multiple (path, digest) pairs in a single HSET.
    pub fn update_worker_state_bulk(&self, entries: &[(String, DigestInfo)]) {
        if entries.is_empty() {
            return;
        }
        let key = format!("nativelink:worker_state:{}", self.machine_id);
        drop(self.with_conn(|conn| {
            let mut cmd = redis::cmd("HSET");
            cmd.arg(&key);
            for (path, digest) in entries {
                cmd.arg(path).arg(Self::worker_state_value(digest));
            }
            cmd.query::<i64>(conn)
        }));
    }
}

/// Per-worker cache combining path->digest mapping and walked-dirs cache.
///
/// The path->digest map is always local (per-file paths are worker-specific).
/// The walked-dirs cache can be local or Redis-backed depending on config.
/// The output sync (optional) broadcasts action outputs to other machines via
/// Redis so that cross-machine builds see each other's produced files.
#[derive(Debug)]
pub struct PathDigestCache {
    map: Mutex<HashMap<PathBuf, DigestInfo>>,
    walked_dirs: Box<dyn WalkedDirsProvider>,
    output_sync: Option<RedisOutputSync>,
}

impl PathDigestCache {
    /// Create with local-only walked-dirs (default).
    pub fn new() -> Self {
        Self {
            map: Mutex::new(HashMap::new()),
            walked_dirs: Box::new(LocalWalkedDirs::new()),
            output_sync: None,
        }
    }

    /// Create with a custom walked-dirs provider (e.g. Redis-backed).
    pub fn with_walked_dirs(walked_dirs: Box<dyn WalkedDirsProvider>) -> Self {
        Self {
            map: Mutex::new(HashMap::new()),
            walked_dirs,
            output_sync: None,
        }
    }

    /// Create with both walked-dirs provider and output sync.
    pub fn with_walked_dirs_and_output_sync(
        walked_dirs: Box<dyn WalkedDirsProvider>,
        output_sync: RedisOutputSync,
    ) -> Self {
        Self {
            map: Mutex::new(HashMap::new()),
            walked_dirs,
            output_sync: Some(output_sync),
        }
    }

    pub fn contains(&self, path: &Path, digest: &DigestInfo) -> bool {
        let map = self.map.lock();
        map.get(path).is_some_and(|d| d == digest)
    }

    pub fn insert(&self, path: PathBuf, digest: DigestInfo) {
        self.map.lock().insert(path, digest);
    }

    pub fn dir_walked(&self, directory_path: &str, digest: &DigestInfo) -> bool {
        self.walked_dirs.dir_walked(directory_path, digest)
    }

    pub fn mark_dir_walked(&self, directory_path: &str, digest: DigestInfo) {
        self.walked_dirs.mark_dir_walked(directory_path, digest);
    }

    /// Record output files produced by this action. Broadcasts to other
    /// machines via Redis so they can fetch the files before their next action.
    pub fn record_outputs(&self, outputs: &[(String, DigestInfo)]) {
        if let Some(ref sync) = self.output_sync {
            sync.record_outputs(outputs);
        }
    }

    /// Drain pending output files from other machines. The caller must
    /// fetch each returned (relative_path, digest) from CAS and write
    /// to the local shared tree.
    pub fn drain_pending_outputs(&self) -> Vec<(String, DigestInfo)> {
        if let Some(ref sync) = self.output_sync {
            sync.drain_pending_outputs()
        } else {
            vec![]
        }
    }

    /// Record that this worker now has `path` at the given digest in
    /// `worker_state:{machine_id}`. Called after a successful
    /// materialization so the scheduler's next HMGET sees the update and
    /// stops re-publishing the same file.
    pub fn update_worker_state(&self, path: &str, digest: &DigestInfo) {
        if let Some(ref sync) = self.output_sync {
            sync.update_worker_state(path, digest);
        }
    }

    /// Batch variant of `update_worker_state`.
    pub fn update_worker_state_bulk(&self, entries: &[(String, DigestInfo)]) {
        if let Some(ref sync) = self.output_sync {
            sync.update_worker_state_bulk(entries);
        }
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
        assert!(!cache.dir_walked("/some/path", &digest));
    }

    #[test]
    fn test_mark_dir_walked_then_hit() {
        let cache = PathDigestCache::new();
        let digest = DigestInfo::new([8u8; 32], 800);

        cache.mark_dir_walked("/src/third_party", digest);
        assert!(cache.dir_walked("/src/third_party", &digest));
    }

    #[test]
    fn test_dir_walked_different_digest_is_miss() {
        let cache = PathDigestCache::new();
        let digest_a = DigestInfo::new([9u8; 32], 900);
        let digest_b = DigestInfo::new([10u8; 32], 1000);

        cache.mark_dir_walked("/src/sdk", digest_a);
        assert!(!cache.dir_walked("/src/sdk", &digest_b));
    }

    #[test]
    fn test_dir_walked_same_digest_different_path_is_miss() {
        let cache = PathDigestCache::new();
        let digest = DigestInfo::new([12u8; 32], 1200);

        cache.mark_dir_walked("/machine_a/src/out/Mac/obj", digest);
        // Same digest, different path — must be a MISS.
        assert!(!cache.dir_walked("/machine_b/src/out/Mac/obj", &digest));
    }

    #[test]
    fn test_with_custom_walked_dirs_provider() {
        let provider = Box::new(LocalWalkedDirs::new());
        let cache = PathDigestCache::with_walked_dirs(provider);
        let digest = DigestInfo::new([11u8; 32], 1100);

        assert!(!cache.dir_walked("/test/path", &digest));
        cache.mark_dir_walked("/test/path", digest);
        assert!(cache.dir_walked("/test/path", &digest));
    }

    #[test]
    fn test_redis_walked_dirs_digest_to_member() {
        let digest = DigestInfo::new([0xABu8; 32], 42);
        let member = RedisWalkedDirs::digest_to_member(&digest);
        assert!(member.ends_with("-42"));
        assert!(member.len() > 3); // hash + "-42"
    }

    #[test]
    fn test_hex_to_32_valid() {
        let hex = "ab".repeat(32); // 64 hex chars = 32 bytes of 0xAB
        let result = RedisOutputSync::hex_to_32(&hex);
        assert!(result.is_some());
        assert_eq!(result.unwrap(), [0xABu8; 32]);
    }

    #[test]
    fn test_hex_to_32_invalid_length() {
        assert!(RedisOutputSync::hex_to_32("abcd").is_none());
        assert!(RedisOutputSync::hex_to_32("").is_none());
    }

    #[test]
    fn test_hex_to_32_invalid_chars() {
        let hex = "zz".repeat(32);
        assert!(RedisOutputSync::hex_to_32(&hex).is_none());
    }

    #[test]
    fn test_drain_pending_outputs_empty_without_sync() {
        let cache = PathDigestCache::new();
        assert!(cache.drain_pending_outputs().is_empty());
    }

    #[test]
    fn test_record_outputs_noop_without_sync() {
        let cache = PathDigestCache::new();
        let digest = DigestInfo::new([1u8; 32], 100);
        // Should not panic even without output_sync configured.
        cache.record_outputs(&[("foo.o".to_string(), digest)]);
    }

    #[test]
    fn test_worker_state_value_format() {
        let digest = DigestInfo::new([0xABu8; 32], 42);
        let v = RedisOutputSync::worker_state_value(&digest);
        assert!(v.ends_with("-42"));
        // Must match the value format that the scheduler's
        // `DispatchJournaler::publish_for_worker` compares against,
        // which is `"{hash}-{size}"`.
        assert!(v.contains('-'));
        assert_eq!(v.matches('-').count(), 1);
    }

    #[test]
    fn test_update_worker_state_noop_without_sync() {
        let cache = PathDigestCache::new();
        let digest = DigestInfo::new([1u8; 32], 100);
        // Should not panic without output_sync.
        cache.update_worker_state("foo.o", &digest);
        cache.update_worker_state_bulk(&[("a".to_string(), digest)]);
    }

    #[test]
    fn test_update_worker_state_bulk_empty_noop() {
        let cache = PathDigestCache::new();
        // Empty input must not attempt any Redis work.
        cache.update_worker_state_bulk(&[]);
    }
}
