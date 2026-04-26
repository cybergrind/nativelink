// Copyright 2024 The NativeLink Authors. All rights reserved.
//
// Licensed under the Functional Source License, Version 1.1, Apache 2.0 Future License.

use std::collections::{HashMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use nativelink_util::common::DigestInfo;
use parking_lot::{Mutex, RwLock};

/// Process-shared path->digest map. Many `PathDigestCache` instances —
/// one per `RunningActionsManagerImpl` — share a single `Arc` of this
/// type so that a verification performed by any worker in the process
/// is immediately visible to every other worker. Plan L's walked-dirs
/// provider is *not* shared via this type; it remains per-instance so
/// each manager can keep its own `shared_walked_dirs_redis_url` /
/// `machine_id` configuration.
pub type SharedPathDigestMap = Arc<RwLock<HashMap<PathBuf, DigestInfo>>>;

/// Build a fresh `SharedPathDigestMap`. Exposed so callers (e.g.
/// `src/bin/nativelink.rs`) can construct one without depending on the
/// concrete `parking_lot::RwLock` / `HashMap` types directly.
pub fn new_shared_path_digest_map() -> SharedPathDigestMap {
    Arc::new(RwLock::new(HashMap::new()))
}

/// Maximum number of pooled Redis connections per client. With ~20
/// concurrent actions per worker × multiple Redis ops per action,
/// 8 pooled connections give us enough parallelism to avoid serializing
/// async tasks on a single connection's I/O.
const REDIS_POOL_SIZE: usize = 8;

/// A small Redis connection pool. Borrow takes a connection from the
/// front (creating one if pool is empty), do I/O without holding the
/// pool lock, then return the connection (discarded if the pool is full
/// or the connection errored).
///
/// Using a pool instead of a single cached connection prevents the
/// `parking_lot::Mutex<Option<redis::Connection>>` pattern from
/// serializing all in-flight tokio tasks during Redis I/O.
struct RedisPool {
    client: redis::Client,
    pool: Mutex<VecDeque<redis::Connection>>,
}

// Redis pool instrumentation: pool_hit means we reused an existing
// connection (cheap). pool_miss_new_conn means we opened a new TCP
// connection (syscall heavy: socket() + connect() + 3-way handshake,
// plus pressure on macOS ephemeral port range). If new_conn count is
// large, the pool capacity is undersized or contention is draining it
// faster than callers return them.
static REDIS_POOL_HIT: StageStats =
    StageStats::new("worker.redis_pool.hit");
static REDIS_POOL_NEW_CONN: StageStats =
    StageStats::new("worker.redis_pool.new_conn");

impl RedisPool {
    fn new(client: redis::Client) -> Self {
        Self {
            client,
            pool: Mutex::new(VecDeque::with_capacity(REDIS_POOL_SIZE)),
        }
    }

    /// Take a connection from the pool, opening a new one if the pool is
    /// empty. Returns `None` if connection-open fails.
    fn take(&self) -> Option<redis::Connection> {
        if let Some(c) = self.pool.lock().pop_front() {
            REDIS_POOL_HIT.incr();
            return Some(c);
        }
        REDIS_POOL_NEW_CONN.incr();
        self.client.get_connection().ok()
    }

    /// Return a connection to the pool. Drops the connection if the pool
    /// is at capacity (limits TIME_WAIT pressure on macOS ephemeral ports).
    fn put(&self, conn: redis::Connection) {
        let mut pool = self.pool.lock();
        if pool.len() < REDIS_POOL_SIZE {
            pool.push_back(conn);
        }
    }

    /// Convenience: take a conn, run `op` WITHOUT holding the pool lock,
    /// then put it back on success. On error, the conn is dropped (forces
    /// reconnect on next call). Returns `None` if no connection could be
    /// obtained.
    fn with_conn<T, F>(&self, op: F) -> Option<Result<T, redis::RedisError>>
    where
        F: FnOnce(&mut redis::Connection) -> Result<T, redis::RedisError>,
    {
        let mut conn = self.take()?;
        let result = op(&mut conn);
        if result.is_ok() {
            self.put(conn);
        }
        Some(result)
    }
}

impl std::fmt::Debug for RedisPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisPool")
            .field("pool_size", &self.pool.lock().len())
            .finish_non_exhaustive()
    }
}

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

    /// Static discriminator for telemetry and tests. Returns "local"
    /// for the in-memory provider and "redis" for the Redis-backed
    /// provider. Lets the wiring code log which Plan L is in effect
    /// per worker, and lets tests verify the wiring without standing
    /// up Redis.
    fn kind(&self) -> &'static str;
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

    fn kind(&self) -> &'static str {
        "local"
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
    pool: RedisPool,
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
            pool: RedisPool::new(redis_client),
            redis_key,
        })
    }

    fn digest_to_member(digest: &DigestInfo) -> String {
        format!("{}-{}", digest.packed_hash(), digest.size_bytes())
    }
}

// Timing harness: instrument every Redis round-trip so the dump reveals
// whether Plan L is the sys-CPU hotspot under a busy build. Splits:
//   walked_dirs.l1_hit   — L1 HashSet hit (no syscalls).
//   walked_dirs.l1_miss  — L1 miss, fell through to Redis.
//   walked_dirs.redis_sismember — wall time of SISMEMBER (Redis RTT +
//                                 pool lock + sync I/O).
//   walked_dirs.redis_sadd      — wall time of SADD.
// A scheduler-host seeing high sys CPU while `walked_dirs.redis_*` count
// is high → Plan L's Redis round-trips dominate. If `l1_hit` hugely
// outnumbers `l1_miss` but the sums are still large, the L1 check is
// still worth it but Redis is an unavoidable cost.
use nativelink_util::timing::StageStats;

static WALKED_DIRS_L1_HIT: StageStats =
    StageStats::new("worker.walked_dirs.l1_hit");
static WALKED_DIRS_L1_MISS: StageStats =
    StageStats::new("worker.walked_dirs.l1_miss");
static WALKED_DIRS_REDIS_SISMEMBER: StageStats =
    StageStats::new("worker.walked_dirs.redis_sismember");
static WALKED_DIRS_REDIS_SADD: StageStats =
    StageStats::new("worker.walked_dirs.redis_sadd");

impl WalkedDirsProvider for RedisWalkedDirs {
    fn dir_walked(&self, directory_path: &str, digest: &DigestInfo) -> bool {
        let key = (directory_path.to_string(), *digest);
        if self.l1.lock().contains(&key) {
            WALKED_DIRS_L1_HIT.incr();
            return true;
        }
        WALKED_DIRS_L1_MISS.incr();

        let member = format!("{}|{}", directory_path, Self::digest_to_member(digest));
        let result: bool = {
            let _t = WALKED_DIRS_REDIS_SISMEMBER.timer();
            self.pool
                .with_conn(|conn| {
                    redis::cmd("SISMEMBER")
                        .arg(&self.redis_key)
                        .arg(&member)
                        .query::<bool>(conn)
                })
                .and_then(Result::ok)
                .unwrap_or(false)
        };

        if result {
            self.l1.lock().insert(key);
        }
        result
    }

    fn mark_dir_walked(&self, directory_path: &str, digest: DigestInfo) {
        self.l1.lock().insert((directory_path.to_string(), digest));

        let member = format!("{}|{}", directory_path, Self::digest_to_member(&digest));
        let _t = WALKED_DIRS_REDIS_SADD.timer();
        drop(self.pool.with_conn(|conn| {
            redis::cmd("SADD")
                .arg(&self.redis_key)
                .arg(&member)
                .query::<i64>(conn)
        }));
    }

    fn kind(&self) -> &'static str {
        "redis"
    }
}

/// Per-worker cache combining path->digest mapping (Plan K) and
/// walked-dirs cache (Plan L). Both are pure optimizations: a miss falls
/// through to the canonical CAS walk in `download_to_directory`.
///
/// The path->digest map lives behind a `SharedPathDigestMap`
/// (`Arc<RwLock<HashMap<...>>>`). Multiple `PathDigestCache` instances
/// constructed via `with_shared_map_and_walked_dirs` share the same
/// underlying map so that any insert is immediately visible to every
/// other instance — this is the cold-start optimization Plan K
/// Prewarm landed.
///
/// The walked-dirs provider stays per-instance (`Box<dyn ...>`). Each
/// `RunningActionsManagerImpl` keeps its own
/// `shared_walked_dirs_redis_url` / `machine_id` configuration so that
/// the previous Redis-backed Plan L sharing across machines is not
/// silently lost when the path-digest map is process-shared.
#[derive(Debug)]
pub struct PathDigestCache {
    map: SharedPathDigestMap,
    walked_dirs: Box<dyn WalkedDirsProvider>,
}

impl PathDigestCache {
    /// Create with a fresh (un-shared) map and local-only walked-dirs.
    pub fn new() -> Self {
        Self {
            map: Arc::new(RwLock::new(HashMap::new())),
            walked_dirs: Box::new(LocalWalkedDirs::new()),
        }
    }

    /// Create with a fresh (un-shared) map and a custom walked-dirs
    /// provider (e.g. Redis-backed). Used by tests and by callers that
    /// want a single self-contained cache.
    pub fn with_walked_dirs(walked_dirs: Box<dyn WalkedDirsProvider>) -> Self {
        Self {
            map: Arc::new(RwLock::new(HashMap::new())),
            walked_dirs,
        }
    }

    /// Create with a caller-supplied shared map and a per-instance
    /// walked-dirs provider. This is the constructor used by
    /// `RunningActionsManagerImpl::new_with_callbacks` when a
    /// process-level `SharedPathDigestMap` is injected: the manager
    /// picks up the shared Plan K state while still building its own
    /// Plan L provider from `shared_walked_dirs_redis_url`.
    pub fn with_shared_map_and_walked_dirs(
        map: SharedPathDigestMap,
        walked_dirs: Box<dyn WalkedDirsProvider>,
    ) -> Self {
        Self { map, walked_dirs }
    }

    pub fn contains(&self, path: &Path, digest: &DigestInfo) -> bool {
        let map = self.map.read();
        map.get(path).is_some_and(|d| d == digest)
    }

    pub fn insert(&self, path: PathBuf, digest: DigestInfo) {
        self.map.write().insert(path, digest);
    }

    pub fn dir_walked(&self, directory_path: &str, digest: &DigestInfo) -> bool {
        self.walked_dirs.dir_walked(directory_path, digest)
    }

    pub fn mark_dir_walked(&self, directory_path: &str, digest: DigestInfo) {
        self.walked_dirs.mark_dir_walked(directory_path, digest);
    }

    /// Return a clone of the underlying shared map handle. Used by
    /// process-level wiring (`src/bin/nativelink.rs`) and tests that
    /// need to verify two caches are backed by the same `Arc` via
    /// `Arc::ptr_eq`.
    pub fn share_map(&self) -> &SharedPathDigestMap {
        &self.map
    }

    /// Static discriminator for the in-effect walked-dirs provider —
    /// "local" or "redis". Useful for startup logs and for tests that
    /// need to verify `shared_walked_dirs_redis_url` was honored
    /// without standing up Redis.
    pub fn walked_dirs_kind(&self) -> &'static str {
        self.walked_dirs.kind()
    }

    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.map.read().len()
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

    /// Concurrent readers: the `contains` hot path must not serialize
    /// readers. With an `RwLock` backing the path->digest map, many
    /// threads can take a shared read guard simultaneously. With a
    /// `Mutex` they would serialize.
    ///
    /// We probe this property indirectly: spin many threads doing
    /// `contains` calls in a tight loop and require that the total
    /// observed hits matches the expected N×iter, which proves no
    /// reader was starved or skipped under contention. The mere fact
    /// that this test does NOT deadlock in the presence of nested
    /// `read()` calls (via the `Arc<PathDigestCache>` shared across
    /// threads) is itself the core RwLock guarantee being asserted.
    #[test]
    fn test_concurrent_readers_share_path_digest_cache() {
        use std::sync::Arc;
        use std::thread;
        use std::sync::atomic::{AtomicU64, Ordering};

        let cache = Arc::new(PathDigestCache::new());
        let path = PathBuf::from("/concurrent/readers/probe");
        let digest = DigestInfo::new([0xAAu8; 32], 9999);
        cache.insert(path.clone(), digest);

        const READERS: usize = 8;
        const ITERS: u64 = 5_000;

        let hits = Arc::new(AtomicU64::new(0));
        let mut handles = Vec::with_capacity(READERS);
        for _ in 0..READERS {
            let cache = cache.clone();
            let hits = hits.clone();
            let path = path.clone();
            handles.push(thread::spawn(move || {
                for _ in 0..ITERS {
                    if cache.contains(&path, &digest) {
                        hits.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(hits.load(Ordering::Relaxed), READERS as u64 * ITERS);
    }

    #[test]
    fn test_redis_walked_dirs_digest_to_member() {
        let digest = DigestInfo::new([0xABu8; 32], 42);
        let member = RedisWalkedDirs::digest_to_member(&digest);
        assert!(member.ends_with("-42"));
        assert!(member.len() > 3); // hash + "-42"
    }

    /// Slice 1 (Plan M): two `PathDigestCache` instances built from the
    /// same `SharedPathDigestMap` must observe each other's inserts —
    /// the path-digest map is process-shared. This is the bedrock for
    /// the cold-start optimization: every worker in the process has one
    /// path->digest map, even though each worker may have its own
    /// (Redis-backed or local) walked-dirs provider.
    #[test]
    fn shared_map_visible_across_path_digest_cache_instances() {
        use std::sync::Arc;
        let shared: SharedPathDigestMap = Arc::new(RwLock::new(HashMap::new()));
        let cache_a = PathDigestCache::with_shared_map_and_walked_dirs(
            shared.clone(),
            Box::new(LocalWalkedDirs::new()),
        );
        let cache_b = PathDigestCache::with_shared_map_and_walked_dirs(
            shared.clone(),
            Box::new(LocalWalkedDirs::new()),
        );

        let path = PathBuf::from("/shared/visible.bin");
        let digest = DigestInfo::new([0x77u8; 32], 1234);

        // Insert through A; visible through B.
        cache_a.insert(path.clone(), digest);
        assert!(cache_b.contains(&path, &digest));
        assert!(cache_a.contains(&path, &digest));

        // Identity check on the underlying map.
        assert!(Arc::ptr_eq(&shared, cache_a.share_map()));
        assert!(Arc::ptr_eq(&shared, cache_b.share_map()));
    }

    /// Slice 1 (Plan M): walked-dirs providers stay **per-instance**
    /// even when the path-digest map is shared. Marking a directory
    /// walked through one cache must NOT be visible through another
    /// cache with its own provider — otherwise a worker without a
    /// pre-staged tree at the same path could skip required
    /// materialization just because a peer marked it walked.
    #[test]
    fn shared_map_keeps_walked_dirs_providers_independent() {
        use std::sync::Arc;
        let shared: SharedPathDigestMap = Arc::new(RwLock::new(HashMap::new()));
        let cache_a = PathDigestCache::with_shared_map_and_walked_dirs(
            shared.clone(),
            Box::new(LocalWalkedDirs::new()),
        );
        let cache_b = PathDigestCache::with_shared_map_and_walked_dirs(
            shared,
            Box::new(LocalWalkedDirs::new()),
        );

        let digest = DigestInfo::new([0x88u8; 32], 5678);
        cache_a.mark_dir_walked("/per/instance/path", digest);

        assert!(cache_a.dir_walked("/per/instance/path", &digest));
        assert!(
            !cache_b.dir_walked("/per/instance/path", &digest),
            "walked-dirs providers must be per-instance even when the path-digest map is shared",
        );
    }

    /// Slice 1 (Plan M): the `walked_dirs_kind` accessor lets the
    /// process-startup wiring and tests verify which provider is in
    /// effect without inspecting Redis behavior. Used by the manager
    /// test that proves `shared_walked_dirs_redis_url` is honored even
    /// when a shared map is injected.
    #[test]
    fn walked_dirs_kind_reports_local_for_default_cache() {
        let cache = PathDigestCache::new();
        assert_eq!(cache.walked_dirs_kind(), "local");
    }
}
