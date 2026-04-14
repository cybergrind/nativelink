// Copyright 2024 The NativeLink Authors. All rights reserved.
//
// Licensed under the Functional Source License, Version 1.1, Apache 2.0 Future License.

//! Scheduler-side resolver that walks a Directory subtree via the
//! Redis-backed dir_index produced by the CAS upload hook.
//!
//! The walk produces `(full_path, DigestInfo)` pairs for every file in
//! the subtree. Missing dir_index entries (uncached Directory digests)
//! cause the walk to skip that subtree — the scheduler falls back to
//! the existing input-tree walk on the worker.

use std::collections::VecDeque;

use nativelink_util::common::DigestInfo;

/// A single entry in a Directory protobuf, as parsed from the dir_index
/// Redis HASH value format (`"file|{digest_hex}-{size}"`, etc.).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DirEntry {
    File {
        name: String,
        digest_hex: String,
        size: i64,
    },
    Dir {
        name: String,
        digest_hex: String,
        size: i64,
    },
    Symlink {
        name: String,
        target: String,
    },
}

impl DirEntry {
    /// Parse the Redis HASH value (which was produced by
    /// `DirChild::to_redis_value` in nativelink-service).
    pub fn parse(name: &str, value: &str) -> Option<Self> {
        let (tag, rest) = value.split_once('|')?;
        match tag {
            "file" => {
                let (hex, size) = rest.rsplit_once('-')?;
                let size: i64 = size.parse().ok()?;
                Some(Self::File {
                    name: name.to_string(),
                    digest_hex: hex.to_string(),
                    size,
                })
            }
            "dir" => {
                let (hex, size) = rest.rsplit_once('-')?;
                let size: i64 = size.parse().ok()?;
                Some(Self::Dir {
                    name: name.to_string(),
                    digest_hex: hex.to_string(),
                    size,
                })
            }
            "symlink" => Some(Self::Symlink {
                name: name.to_string(),
                target: rest.to_string(),
            }),
            _ => None,
        }
    }
}

/// Pure path walker: given a root directory digest and a lookup closure
/// that returns the children of a Directory by its digest, produce
/// every `(full_path, file_digest)` pair in the subtree.
///
/// The closure returns `None` if the Directory is not in the dir_index
/// (uncached). In that case the walk stops descending into that subtree
/// — the caller is expected to fall back to a live Directory-proto fetch.
///
/// Paths are built relative to the root with `/` separators. Symlinks
/// are not followed or emitted.
/// Like `walk_tree`, but consults a `fallback` lookup whenever the
/// `primary` lookup returns `None`. This handles the case where some
/// Directory protos uploaded by the client never made it into the
/// dir_index (transient REAPI errors, etc.) — the fallback can fetch
/// the Directory live from CAS so the walk doesn't silently drop a
/// subtree.
///
/// On every fallback hit, a `tracing::warn!` is emitted so operators can
/// see when the dir_index is incomplete and how often the fallback path
/// is exercised.
pub fn walk_tree_with_fallback<P, S>(
    root_digest: &DigestInfo,
    mut primary: P,
    mut fallback: S,
) -> Vec<(String, DigestInfo)>
where
    P: FnMut(&DigestInfo) -> Option<Vec<DirEntry>>,
    S: FnMut(&DigestInfo) -> Option<Vec<DirEntry>>,
{
    walk_tree(root_digest, |d| match primary(d) {
        Some(entries) => Some(entries),
        None => match fallback(d) {
            Some(entries) => {
                tracing::warn!(
                    digest_hex = %d.packed_hash(),
                    size = d.size_bytes(),
                    "dir_index walk: primary missed, fallback served"
                );
                Some(entries)
            }
            None => {
                tracing::warn!(
                    digest_hex = %d.packed_hash(),
                    size = d.size_bytes(),
                    "dir_index walk: BOTH primary and fallback missed — subtree dropped"
                );
                None
            }
        },
    })
}

/// SHA-256 of the empty byte string — the canonical REAPI digest used for
/// empty Directory protobufs (a Directory with no files, no subdirs, no
/// symlinks). The hook never indexes this digest because the corresponding
/// Directory has no children to journal, so any walk that probes Redis or
/// the live-CAS fallback for it gets a miss and erroneously logs
/// "subtree dropped". Short-circuit it as a known-empty subtree instead.
const EMPTY_BLOB_HASH: [u8; 32] = [
    0xe3, 0xb0, 0xc4, 0x42, 0x98, 0xfc, 0x1c, 0x14, 0x9a, 0xfb, 0xf4, 0xc8, 0x99, 0x6f, 0xb9, 0x24,
    0x27, 0xae, 0x41, 0xe4, 0x64, 0x9b, 0x93, 0x4c, 0xa4, 0x95, 0x99, 0x1b, 0x78, 0x52, 0xb8, 0x55,
];

fn is_empty_directory_digest(digest: &DigestInfo) -> bool {
    digest.size_bytes() == 0 && digest.packed_hash().as_ref() == EMPTY_BLOB_HASH
}

pub fn walk_tree<F>(root_digest: &DigestInfo, mut lookup: F) -> Vec<(String, DigestInfo)>
where
    F: FnMut(&DigestInfo) -> Option<Vec<DirEntry>>,
{
    let mut out = Vec::new();
    let mut stack: VecDeque<(String, DigestInfo)> = VecDeque::new();
    stack.push_back((String::new(), *root_digest));

    while let Some((prefix, dir_digest)) = stack.pop_front() {
        if is_empty_directory_digest(&dir_digest) {
            continue;
        }
        let Some(children) = lookup(&dir_digest) else {
            continue;
        };
        for entry in children {
            match entry {
                DirEntry::File {
                    name,
                    digest_hex,
                    size,
                } => {
                    let full_path = if prefix.is_empty() {
                        name
                    } else {
                        format!("{prefix}/{name}")
                    };
                    match digest_info(&digest_hex, size) {
                        Some(d) => out.push((full_path, d)),
                        None => continue,
                    }
                }
                DirEntry::Dir {
                    name,
                    digest_hex,
                    size,
                } => {
                    let new_prefix = if prefix.is_empty() {
                        name
                    } else {
                        format!("{prefix}/{name}")
                    };
                    match digest_info(&digest_hex, size) {
                        Some(d) => stack.push_back((new_prefix, d)),
                        None => continue,
                    }
                }
                DirEntry::Symlink { .. } => {
                    // Symlinks are not emitted in the walk.
                }
            }
        }
    }

    out
}

fn digest_info(hex: &str, size: i64) -> Option<DigestInfo> {
    if hex.len() != 64 || size < 0 {
        return None;
    }
    let mut bytes = [0u8; 32];
    for i in 0..32 {
        bytes[i] = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).ok()?;
    }
    Some(DigestInfo::new(bytes, size as u64))
}

/// Maximum pooled Redis connections per scheduler-side client. Sized
/// generously since the scheduler dispatches concurrently across many
/// workers and walks many Directory chains in parallel.
const SCHEDULER_REDIS_POOL_SIZE: usize = 16;

/// Redis-backed dir_index resolver. Walks a Directory subtree by reading
/// `nativelink:dir_index:{digest_hex}-{size}` HASHes populated by the
/// CAS-server upload hook. Uses a connection pool so concurrent walks
/// don't serialize on a single Mutex-guarded connection.
pub struct RedisDirIndexResolver {
    redis_client: redis::Client,
    pool: parking_lot::Mutex<std::collections::VecDeque<redis::Connection>>,
}

impl std::fmt::Debug for RedisDirIndexResolver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisDirIndexResolver")
            .field("redis_client", &self.redis_client)
            .finish_non_exhaustive()
    }
}

impl RedisDirIndexResolver {
    pub fn new(url: &str) -> Result<Self, redis::RedisError> {
        let redis_client = redis::Client::open(url)?;
        Ok(Self {
            redis_client,
            pool: parking_lot::Mutex::new(std::collections::VecDeque::with_capacity(
                SCHEDULER_REDIS_POOL_SIZE,
            )),
        })
    }

    fn key_for(digest: &DigestInfo) -> String {
        format!(
            "nativelink:dir_index:{}-{}",
            digest.packed_hash(),
            digest.size_bytes()
        )
    }

    fn take_conn(&self) -> Option<redis::Connection> {
        if let Some(c) = self.pool.lock().pop_front() {
            return Some(c);
        }
        self.redis_client.get_connection().ok()
    }

    fn return_conn(&self, conn: redis::Connection) {
        let mut pool = self.pool.lock();
        if pool.len() < SCHEDULER_REDIS_POOL_SIZE {
            pool.push_back(conn);
        }
    }

    /// Fetch the dir_index HASH for a single Directory digest from Redis
    /// and parse its entries. Returns `None` if the key doesn't exist or
    /// Redis is unreachable.
    pub fn lookup(&self, digest: &DigestInfo) -> Option<Vec<DirEntry>> {
        let key = Self::key_for(digest);
        let mut conn = self.take_conn()?;
        let pairs: Vec<(String, String)> = match redis::cmd("HGETALL").arg(&key).query(&mut conn) {
            Ok(v) => {
                self.return_conn(conn);
                v
            }
            Err(_) => return None, // drop conn (don't return to pool)
        };
        if pairs.is_empty() {
            return None;
        }
        let entries: Vec<DirEntry> = pairs
            .into_iter()
            .filter_map(|(name, value)| DirEntry::parse(&name, &value))
            .collect();
        Some(entries)
    }

    /// Walk the subtree rooted at `root_digest`, producing every
    /// `(full_path, digest)` pair. Subtrees whose Directory digest is not
    /// cached in Redis trigger a `warn!` log so operators can see when
    /// the dir_index is incomplete (Bug F from cas_journal_failure_report_05).
    pub fn resolve_paths(&self, root_digest: &DigestInfo) -> Vec<(String, DigestInfo)> {
        walk_tree_with_fallback(
            root_digest,
            |d| self.lookup(d),
            // No CAS fallback yet — this just enables the "primary missed"
            // warn! logging in walk_tree_with_fallback. A real CAS-backed
            // fallback would need the CAS store plumbed into the scheduler.
            |_| None,
        )
    }
}

/// Render a `(path, digest, seqnum)` tuple into the
/// `"path|hash-size|seqnum"` format used in
/// `nativelink:pending_outputs:{machine_id}` LIST entries. The seqnum is
/// the publisher's monotonic batch id; the worker drain reports it back
/// via `nativelink:drained_seqnum:{machine_id}` so the scheduler's
/// pre-action barrier can confirm materialization.
pub fn pending_entry_string(path: &str, digest: &DigestInfo, seqnum: i64) -> String {
    format!(
        "{path}|{}-{}|{seqnum}",
        digest.packed_hash(),
        digest.size_bytes()
    )
}

/// Pure diff function: given the resolved paths from a walk and a lookup
/// of worker_state (what the worker already has on disk), return only
/// the entries that need to be pushed to the worker's pending_outputs.
///
/// A path is pushed when:
/// - It is not in worker_state (worker doesn't have it), OR
/// - The worker_state digest differs from the walk's digest (content changed)
pub fn diff_against_worker_state<F>(
    walked: &[(String, DigestInfo)],
    mut worker_state_get: F,
) -> Vec<(String, DigestInfo)>
where
    F: FnMut(&str) -> Option<String>,
{
    walked
        .iter()
        .filter_map(|(path, digest)| {
            let expected = format!("{}-{}", digest.packed_hash(), digest.size_bytes());
            match worker_state_get(path) {
                Some(current) if current == expected => None,
                _ => Some((path.clone(), *digest)),
            }
        })
        .collect()
}

/// Redis-backed journaler that publishes resolved paths to
/// `nativelink:pending_outputs:{machine_id}` LISTs, deduplicating
/// against `nativelink:worker_state:{machine_id}` HASHes. Uses a
/// connection pool so concurrent dispatches don't serialize on a
/// single Mutex-guarded connection.
pub struct DispatchJournaler {
    redis_client: redis::Client,
    pool: parking_lot::Mutex<std::collections::VecDeque<redis::Connection>>,
}

impl std::fmt::Debug for DispatchJournaler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DispatchJournaler")
            .field("redis_client", &self.redis_client)
            .finish_non_exhaustive()
    }
}

impl DispatchJournaler {
    pub fn new(url: &str) -> Result<Self, redis::RedisError> {
        let redis_client = redis::Client::open(url)?;
        Ok(Self {
            redis_client,
            pool: parking_lot::Mutex::new(std::collections::VecDeque::with_capacity(
                SCHEDULER_REDIS_POOL_SIZE,
            )),
        })
    }

    fn take_conn(&self) -> Result<redis::Connection, redis::RedisError> {
        if let Some(c) = self.pool.lock().pop_front() {
            return Ok(c);
        }
        self.redis_client.get_connection()
    }

    fn return_conn(&self, conn: redis::Connection) {
        let mut pool = self.pool.lock();
        if pool.len() < SCHEDULER_REDIS_POOL_SIZE {
            pool.push_back(conn);
        }
    }

    /// Push pending_outputs entries for files the given worker doesn't
    /// already have on disk. Returns `(pushed_count, batch_seqnum)`; the
    /// seqnum is 0 when nothing was pushed (nothing to wait for).
    /// Best-effort: Redis errors are logged at `warn!` level by the caller
    /// and swallowed.
    pub fn publish_for_worker(
        &self,
        machine_id: &str,
        walked: &[(String, DigestInfo)],
    ) -> Result<(usize, i64), redis::RedisError> {
        if walked.is_empty() {
            return Ok((0, 0));
        }
        let mut conn = self.take_conn()?;
        let worker_state_key = format!("nativelink:worker_state:{machine_id}");

        // Fetch all relevant paths from worker_state in one HMGET.
        let paths: Vec<&str> = walked.iter().map(|(p, _)| p.as_str()).collect();
        let current_values: Vec<Option<String>> = if paths.is_empty() {
            Vec::new()
        } else {
            match redis::cmd("HMGET")
                .arg(&worker_state_key)
                .arg(&paths)
                .query(&mut conn)
            {
                Ok(v) => v,
                Err(e) => {
                    // Drop connection (don't return to pool) on error.
                    return Err(e);
                }
            }
        };

        let mut to_push_raw: Vec<(String, DigestInfo)> = Vec::new();
        for (idx, (path, digest)) in walked.iter().enumerate() {
            let expected = format!("{}-{}", digest.packed_hash(), digest.size_bytes());
            let current = current_values.get(idx).cloned().flatten();
            if current.as_deref() != Some(expected.as_str()) {
                to_push_raw.push((path.clone(), *digest));
            }
        }

        if to_push_raw.is_empty() {
            self.return_conn(conn);
            return Ok((0, 0));
        }

        // Atomic publish via Lua: INCR next_seqnum + RPUSH entries with
        // that seqnum baked in, in a single Redis round-trip that nothing
        // else can interleave. This is critical for the heartbeat on the
        // worker side — if the worker observes `LLEN == 0` it can safely
        // advance `drained_seqnum` to `next_seqnum` knowing no publisher
        // is half-way through a non-atomic INCR+RPUSH.
        const PUBLISH_SCRIPT: &str = r#"
            local seqnum = redis.call('INCR', KEYS[1])
            for i=1,#ARGV do
                redis.call('RPUSH', KEYS[2], ARGV[i] .. '|' .. seqnum)
            end
            return seqnum
        "#;
        let seqnum_key = format!("nativelink:next_seqnum:{machine_id}");
        let pending_key = format!("nativelink:pending_outputs:{machine_id}");
        // ARGV entries are the 2-field prefix ("path|hex-size"); Lua
        // appends "|seqnum" so the worker's parser gets the full 3-field.
        let prefixes: Vec<String> = to_push_raw
            .iter()
            .map(|(path, digest)| {
                format!("{path}|{}-{}", digest.packed_hash(), digest.size_bytes())
            })
            .collect();
        let mut script_cmd = redis::cmd("EVAL");
        script_cmd
            .arg(PUBLISH_SCRIPT)
            .arg(2)
            .arg(&seqnum_key)
            .arg(&pending_key);
        for prefix in &prefixes {
            script_cmd.arg(prefix);
        }
        let seqnum: i64 = match script_cmd.query::<i64>(&mut conn) {
            Ok(n) => n,
            Err(e) => return Err(e),
        };
        let to_push = prefixes;
        let rpush_result: i64 = to_push.len() as i64;
        let post_llen: i64 = redis::cmd("LLEN")
            .arg(&pending_key)
            .query(&mut conn)
            .unwrap_or(-1);
        tracing::info!(
            machine_id = %machine_id,
            key = %pending_key,
            pushed_count = to_push.len(),
            seqnum,
            rpush_result,
            post_llen,
            "publish: post-RPUSH Redis state"
        );
        self.return_conn(conn);
        Ok((to_push.len(), seqnum))
    }

    /// Read `nativelink:drained_seqnum:{machine_id}` (0 when unset). This
    /// is the worker's monotonic cursor of batches it has fully
    /// materialized. The pre-action barrier waits until this is >=
    /// `required_txid` before unblocking action dispatch.
    pub fn get_drained_seqnum(&self, machine_id: &str) -> Result<i64, redis::RedisError> {
        let mut conn = self.take_conn()?;
        let key = format!("nativelink:drained_seqnum:{machine_id}");
        let res: Option<i64> = match redis::cmd("GET").arg(&key).query(&mut conn) {
            Ok(v) => v,
            Err(e) => return Err(e),
        };
        self.return_conn(conn);
        Ok(res.unwrap_or(0))
    }
}

/// Pure decision function for the pre-action barrier: given a snapshot of
/// the target worker's `drained_seqnum` cursor and the action's
/// `required_txid`, decide whether to unblock (`Ready`), keep polling
/// (`Wait`), or give up after the deadline (`Timeout`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BarrierStep {
    Ready,
    Wait,
    Timeout,
}

/// Inputs: current `drained_seqnum`, `required_txid`, elapsed time since
/// barrier started, and the barrier's max wait. Kept pure so the
/// scheduler's polling loop is trivially testable without a live Redis.
#[must_use]
pub fn barrier_decision(
    drained_seqnum: i64,
    required_txid: i64,
    elapsed: std::time::Duration,
    max_wait: std::time::Duration,
) -> BarrierStep {
    if required_txid <= 0 {
        // Nothing was published for this action — no barrier required.
        return BarrierStep::Ready;
    }
    if drained_seqnum >= required_txid {
        return BarrierStep::Ready;
    }
    if elapsed >= max_wait {
        return BarrierStep::Timeout;
    }
    BarrierStep::Wait
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    fn mk_digest(byte: u8, size: u64) -> DigestInfo {
        DigestInfo::new([byte; 32], size)
    }

    fn hex_of(d: &DigestInfo) -> String {
        format!("{}", d.packed_hash())
    }

    use std::time::Duration;

    /// Slice D: pre-action barrier decision table. Verifies the pure
    /// decision function used by the scheduler's polling loop.
    #[test]
    fn barrier_ready_when_cursor_reaches_required() {
        let d = barrier_decision(7, 7, Duration::from_millis(100), Duration::from_secs(1));
        assert_eq!(d, BarrierStep::Ready);
    }

    #[test]
    fn barrier_ready_when_cursor_exceeds_required() {
        let d = barrier_decision(9, 7, Duration::from_millis(100), Duration::from_secs(1));
        assert_eq!(d, BarrierStep::Ready);
    }

    #[test]
    fn barrier_wait_when_cursor_behind_required_and_within_budget() {
        let d = barrier_decision(5, 7, Duration::from_millis(100), Duration::from_secs(1));
        assert_eq!(d, BarrierStep::Wait);
    }

    #[test]
    fn barrier_timeout_when_cursor_behind_and_past_deadline() {
        let d = barrier_decision(5, 7, Duration::from_millis(2_000), Duration::from_secs(1));
        assert_eq!(d, BarrierStep::Timeout);
    }

    #[test]
    fn barrier_ready_immediately_when_required_txid_zero() {
        // When publish_for_worker pushes nothing, seqnum=0 — no barrier.
        let d = barrier_decision(0, 0, Duration::from_millis(0), Duration::from_secs(1));
        assert_eq!(d, BarrierStep::Ready);
    }

    #[test]
    fn barrier_ready_immediately_when_required_txid_negative() {
        // Defensive: unspecified / error seqnum must not block.
        let d = barrier_decision(100, -1, Duration::from_millis(0), Duration::from_secs(1));
        assert_eq!(d, BarrierStep::Ready);
    }

    #[test]
    fn parse_file_entry() {
        let e = DirEntry::parse("cmath", "file|abc-42").unwrap();
        assert_eq!(
            e,
            DirEntry::File {
                name: "cmath".to_string(),
                digest_hex: "abc".to_string(),
                size: 42,
            }
        );
    }

    #[test]
    fn parse_dir_entry() {
        let e = DirEntry::parse("subdir", "dir|def-100").unwrap();
        assert_eq!(
            e,
            DirEntry::Dir {
                name: "subdir".to_string(),
                digest_hex: "def".to_string(),
                size: 100,
            }
        );
    }

    #[test]
    fn parse_symlink_entry() {
        let e = DirEntry::parse("link", "symlink|target.h").unwrap();
        assert_eq!(
            e,
            DirEntry::Symlink {
                name: "link".to_string(),
                target: "target.h".to_string(),
            }
        );
    }

    #[test]
    fn parse_invalid_returns_none() {
        assert!(DirEntry::parse("name", "no_separator").is_none());
        assert!(DirEntry::parse("name", "unknown|data").is_none());
        assert!(DirEntry::parse("name", "file|bad").is_none()); // no size
        assert!(DirEntry::parse("name", "file|abc-bad").is_none()); // size not numeric
    }

    #[test]
    fn walk_empty_tree_empty_output() {
        let root = mk_digest(1, 10);
        let out = walk_tree(&root, |_d| None);
        assert!(out.is_empty());
    }

    #[test]
    fn walk_single_file_at_root() {
        let root = mk_digest(1, 10);
        let file_digest = mk_digest(2, 100);
        let root_hex = hex_of(&root);
        let file_hex = hex_of(&file_digest);

        let mut index: HashMap<String, Vec<DirEntry>> = HashMap::new();
        index.insert(
            format!("{root_hex}-{}", root.size_bytes()),
            vec![DirEntry::File {
                name: "foo.o".to_string(),
                digest_hex: file_hex.clone(),
                size: file_digest.size_bytes() as i64,
            }],
        );

        let out = walk_tree(&root, |d| {
            index
                .get(&format!("{}-{}", d.packed_hash(), d.size_bytes()))
                .cloned()
        });

        assert_eq!(out.len(), 1);
        assert_eq!(out[0].0, "foo.o");
        assert_eq!(out[0].1, file_digest);
    }

    #[test]
    fn walk_nested_directories() {
        let root = mk_digest(1, 10);
        let sub = mk_digest(2, 20);
        let file = mk_digest(3, 100);

        let mut index: HashMap<String, Vec<DirEntry>> = HashMap::new();
        index.insert(
            format!("{}-{}", hex_of(&root), root.size_bytes()),
            vec![DirEntry::Dir {
                name: "include".to_string(),
                digest_hex: hex_of(&sub),
                size: sub.size_bytes() as i64,
            }],
        );
        index.insert(
            format!("{}-{}", hex_of(&sub), sub.size_bytes()),
            vec![DirEntry::File {
                name: "cmath".to_string(),
                digest_hex: hex_of(&file),
                size: file.size_bytes() as i64,
            }],
        );

        let out = walk_tree(&root, |d| {
            index
                .get(&format!("{}-{}", d.packed_hash(), d.size_bytes()))
                .cloned()
        });

        assert_eq!(out.len(), 1);
        assert_eq!(out[0].0, "include/cmath");
        assert_eq!(out[0].1, file);
    }

    #[test]
    fn walk_skips_uncached_subdirectory() {
        // root → [cached_file, uncached_subdir]. The subdir lookup returns None,
        // so the walk emits just the cached file and does not descend.
        let root = mk_digest(1, 10);
        let cached_sub = mk_digest(2, 20);
        let uncached_sub = mk_digest(9, 99);
        let file = mk_digest(3, 50);

        let mut index: HashMap<String, Vec<DirEntry>> = HashMap::new();
        index.insert(
            format!("{}-{}", hex_of(&root), root.size_bytes()),
            vec![
                DirEntry::Dir {
                    name: "cached".to_string(),
                    digest_hex: hex_of(&cached_sub),
                    size: cached_sub.size_bytes() as i64,
                },
                DirEntry::Dir {
                    name: "uncached".to_string(),
                    digest_hex: hex_of(&uncached_sub),
                    size: uncached_sub.size_bytes() as i64,
                },
            ],
        );
        index.insert(
            format!("{}-{}", hex_of(&cached_sub), cached_sub.size_bytes()),
            vec![DirEntry::File {
                name: "foo.h".to_string(),
                digest_hex: hex_of(&file),
                size: file.size_bytes() as i64,
            }],
        );
        // Note: no entry for uncached_sub — lookup will return None.

        let out = walk_tree(&root, |d| {
            index
                .get(&format!("{}-{}", d.packed_hash(), d.size_bytes()))
                .cloned()
        });

        assert_eq!(out.len(), 1);
        assert_eq!(out[0].0, "cached/foo.h");
    }

    /// Bug F regression: when a Directory digest is not in the primary
    /// dir_index, the walker must consult the fallback (live CAS fetch)
    /// instead of silently dropping the subtree.
    #[test]
    fn walk_with_fallback_uses_fallback_for_uncached_subdir() {
        let root = mk_digest(1, 10);
        let cached_sub = mk_digest(2, 20);
        let uncached_sub = mk_digest(9, 99);
        let cached_file = mk_digest(3, 50);
        let fallback_file = mk_digest(4, 51);

        // Primary index knows root + cached_sub but NOT uncached_sub.
        let mut primary: HashMap<String, Vec<DirEntry>> = HashMap::new();
        primary.insert(
            format!("{}-{}", hex_of(&root), root.size_bytes()),
            vec![
                DirEntry::Dir {
                    name: "cached".to_string(),
                    digest_hex: hex_of(&cached_sub),
                    size: cached_sub.size_bytes() as i64,
                },
                DirEntry::Dir {
                    name: "uncached".to_string(),
                    digest_hex: hex_of(&uncached_sub),
                    size: uncached_sub.size_bytes() as i64,
                },
            ],
        );
        primary.insert(
            format!("{}-{}", hex_of(&cached_sub), cached_sub.size_bytes()),
            vec![DirEntry::File {
                name: "foo.h".to_string(),
                digest_hex: hex_of(&cached_file),
                size: cached_file.size_bytes() as i64,
            }],
        );

        // Fallback knows uncached_sub.
        let mut fallback: HashMap<String, Vec<DirEntry>> = HashMap::new();
        fallback.insert(
            format!("{}-{}", hex_of(&uncached_sub), uncached_sub.size_bytes()),
            vec![DirEntry::File {
                name: "bar.h".to_string(),
                digest_hex: hex_of(&fallback_file),
                size: fallback_file.size_bytes() as i64,
            }],
        );

        let out = walk_tree_with_fallback(
            &root,
            |d| {
                primary
                    .get(&format!("{}-{}", d.packed_hash(), d.size_bytes()))
                    .cloned()
            },
            |d| {
                fallback
                    .get(&format!("{}-{}", d.packed_hash(), d.size_bytes()))
                    .cloned()
            },
        );

        // We expect BOTH files: cached/foo.h and uncached/bar.h.
        assert_eq!(out.len(), 2, "expected files from cached + fallback subtrees, got {out:?}");
        let paths: Vec<_> = out.iter().map(|(p, _)| p.as_str()).collect();
        assert!(paths.contains(&"cached/foo.h"), "missing cached file in {paths:?}");
        assert!(paths.contains(&"uncached/bar.h"), "missing fallback file in {paths:?}");
    }

    /// Bug G: the canonical empty-Directory digest (sha256 of empty bytes,
    /// size 0) is the well-known REAPI placeholder for "directory with no
    /// entries". The walker must short-circuit this digest as a known-empty
    /// subtree rather than probing Redis (miss) → fallback (miss) → emitting
    /// `BOTH primary and fallback missed — subtree dropped` warnings. In
    /// production we observed 126 such warns in 3 minutes, all for this
    /// one digest, causing real subtrees to be elided alongside the empty
    /// ones (workers never received `module.pcm` etc. via pending_outputs).
    #[test]
    fn walk_short_circuits_empty_directory_digest() {
        let empty_hash: [u8; 32] = [
            0xe3, 0xb0, 0xc4, 0x42, 0x98, 0xfc, 0x1c, 0x14, 0x9a, 0xfb, 0xf4, 0xc8, 0x99, 0x6f,
            0xb9, 0x24, 0x27, 0xae, 0x41, 0xe4, 0x64, 0x9b, 0x93, 0x4c, 0xa4, 0x95, 0x99, 0x1b,
            0x78, 0x52, 0xb8, 0x55,
        ];
        let empty_digest = DigestInfo::new(empty_hash, 0);
        let root = mk_digest(1, 10);
        let cached_sub = mk_digest(2, 20);
        let file = mk_digest(3, 50);

        let mut primary: HashMap<String, Vec<DirEntry>> = HashMap::new();
        primary.insert(
            format!("{}-{}", hex_of(&root), root.size_bytes()),
            vec![
                DirEntry::Dir {
                    name: "cached".to_string(),
                    digest_hex: hex_of(&cached_sub),
                    size: cached_sub.size_bytes() as i64,
                },
                DirEntry::Dir {
                    name: "empty".to_string(),
                    digest_hex: hex_of(&empty_digest),
                    size: 0,
                },
            ],
        );
        primary.insert(
            format!("{}-{}", hex_of(&cached_sub), cached_sub.size_bytes()),
            vec![DirEntry::File {
                name: "foo.h".to_string(),
                digest_hex: hex_of(&file),
                size: file.size_bytes() as i64,
            }],
        );

        let mut fallback_called_for_empty = false;
        let out = walk_tree_with_fallback(
            &root,
            |d| {
                primary
                    .get(&format!("{}-{}", d.packed_hash(), d.size_bytes()))
                    .cloned()
            },
            |d| {
                if d == &empty_digest {
                    fallback_called_for_empty = true;
                }
                None
            },
        );

        assert!(
            !fallback_called_for_empty,
            "fallback must NOT be probed for the canonical empty-directory digest"
        );
        let paths: Vec<_> = out.iter().map(|(p, _)| p.as_str()).collect();
        assert_eq!(out.len(), 1, "expected only cached/foo.h, got {paths:?}");
        assert!(paths.contains(&"cached/foo.h"), "missing {paths:?}");
    }

    /// Walking with a fallback that also returns None must behave exactly
    /// like the primary-only walk (no panic, just skip).
    #[test]
    fn walk_with_fallback_no_fallback_data_skips_subdir() {
        let root = mk_digest(1, 10);
        let uncached_sub = mk_digest(9, 99);

        let mut primary: HashMap<String, Vec<DirEntry>> = HashMap::new();
        primary.insert(
            format!("{}-{}", hex_of(&root), root.size_bytes()),
            vec![DirEntry::Dir {
                name: "uncached".to_string(),
                digest_hex: hex_of(&uncached_sub),
                size: uncached_sub.size_bytes() as i64,
            }],
        );

        let out = walk_tree_with_fallback(
            &root,
            |d| {
                primary
                    .get(&format!("{}-{}", d.packed_hash(), d.size_bytes()))
                    .cloned()
            },
            |_| None, // fallback also empty
        );

        assert!(out.is_empty(), "no fallback data → empty walk");
    }

    #[test]
    fn walk_multiple_files_same_dir() {
        let root = mk_digest(1, 10);
        let f1 = mk_digest(2, 100);
        let f2 = mk_digest(3, 200);

        let mut index: HashMap<String, Vec<DirEntry>> = HashMap::new();
        index.insert(
            format!("{}-{}", hex_of(&root), root.size_bytes()),
            vec![
                DirEntry::File {
                    name: "a.o".to_string(),
                    digest_hex: hex_of(&f1),
                    size: f1.size_bytes() as i64,
                },
                DirEntry::File {
                    name: "b.o".to_string(),
                    digest_hex: hex_of(&f2),
                    size: f2.size_bytes() as i64,
                },
            ],
        );

        let out = walk_tree(&root, |d| {
            index
                .get(&format!("{}-{}", d.packed_hash(), d.size_bytes()))
                .cloned()
        });

        assert_eq!(out.len(), 2);
        let paths: Vec<_> = out.iter().map(|(p, _)| p.as_str()).collect();
        assert!(paths.contains(&"a.o"));
        assert!(paths.contains(&"b.o"));
    }

    #[test]
    fn redis_resolver_key_format_matches_writer_format() {
        // Must match DirIndexWriter::key_for in nativelink-service/src/dir_index.rs.
        let d = mk_digest(0xAB, 42);
        let key = RedisDirIndexResolver::key_for(&d);
        // Key shape: nativelink:dir_index:<hex>-<size>
        assert!(key.starts_with("nativelink:dir_index:"));
        assert!(key.ends_with("-42"));
    }

    #[test]
    fn redis_resolver_can_be_constructed() {
        assert!(RedisDirIndexResolver::new("redis://127.0.0.1:6379").is_ok());
    }

    #[test]
    fn redis_resolver_rejects_invalid_url() {
        assert!(RedisDirIndexResolver::new("not-a-url").is_err());
    }

    #[test]
    fn pending_entry_string_format() {
        let d = mk_digest(0xAB, 42);
        let s = pending_entry_string("foo/bar.o", &d, 7);
        assert!(s.starts_with("foo/bar.o|"), "got {s}");
        assert!(s.contains("-42|"), "got {s}");
        assert!(s.ends_with("|7"), "got {s}");
    }

    /// Slice A: every published entry must carry a monotonic seqnum so the
    /// worker drain can report "materialized up to seqnum N" back to the
    /// scheduler for the pre-action barrier.
    #[test]
    fn pending_entry_string_roundtrips_seqnum() {
        let d = mk_digest(0xCD, 99);
        let s = pending_entry_string("a/b.o", &d, 1234);
        // Expected format: "path|<hex>-<size>|<seqnum>"
        let parts: Vec<&str> = s.split('|').collect();
        assert_eq!(parts.len(), 3, "expected 3 pipe-separated fields, got {s}");
        assert_eq!(parts[0], "a/b.o");
        assert!(parts[1].ends_with("-99"));
        assert_eq!(parts[2], "1234");
    }

    #[test]
    fn diff_returns_all_when_worker_state_empty() {
        let d1 = mk_digest(1, 100);
        let d2 = mk_digest(2, 200);
        let walked = vec![
            ("a.o".to_string(), d1),
            ("sub/b.o".to_string(), d2),
        ];
        let diff = diff_against_worker_state(&walked, |_| None);
        assert_eq!(diff.len(), 2);
    }

    #[test]
    fn diff_skips_matching_entries() {
        let d = mk_digest(1, 100);
        let walked = vec![("a.o".to_string(), d)];
        let expected = format!("{}-{}", d.packed_hash(), d.size_bytes());
        let diff = diff_against_worker_state(&walked, |path| {
            if path == "a.o" {
                Some(expected.clone())
            } else {
                None
            }
        });
        assert!(diff.is_empty(), "matching entry must be skipped");
    }

    #[test]
    fn diff_emits_entries_with_changed_digest() {
        let d_new = mk_digest(1, 100);
        let d_old = mk_digest(2, 200);
        let walked = vec![("a.o".to_string(), d_new)];
        let old_value = format!("{}-{}", d_old.packed_hash(), d_old.size_bytes());
        let diff = diff_against_worker_state(&walked, |path| {
            if path == "a.o" {
                Some(old_value.clone())
            } else {
                None
            }
        });
        assert_eq!(diff.len(), 1);
        assert_eq!(diff[0].1, d_new);
    }

    #[test]
    fn diff_mixed_hits_and_misses() {
        let d_same = mk_digest(1, 10);
        let d_changed = mk_digest(2, 20);
        let d_new = mk_digest(3, 30);
        let walked = vec![
            ("same.o".to_string(), d_same),
            ("changed.o".to_string(), d_changed),
            ("new.o".to_string(), d_new),
        ];
        let same_value = format!("{}-{}", d_same.packed_hash(), d_same.size_bytes());
        let old_value = format!("{}-{}", mk_digest(99, 99).packed_hash(), 99);
        let diff = diff_against_worker_state(&walked, |path| match path {
            "same.o" => Some(same_value.clone()),
            "changed.o" => Some(old_value.clone()),
            _ => None,
        });
        assert_eq!(diff.len(), 2);
        let paths: Vec<_> = diff.iter().map(|(p, _)| p.as_str()).collect();
        assert!(paths.contains(&"changed.o"));
        assert!(paths.contains(&"new.o"));
        assert!(!paths.contains(&"same.o"));
    }

    #[test]
    fn dispatch_journaler_constructible() {
        assert!(DispatchJournaler::new("redis://127.0.0.1:6379").is_ok());
        assert!(DispatchJournaler::new("not-a-url").is_err());
    }

    #[test]
    fn walk_does_not_emit_symlinks() {
        let root = mk_digest(1, 10);
        let mut index: HashMap<String, Vec<DirEntry>> = HashMap::new();
        index.insert(
            format!("{}-{}", hex_of(&root), root.size_bytes()),
            vec![DirEntry::Symlink {
                name: "link".to_string(),
                target: "real".to_string(),
            }],
        );

        let out = walk_tree(&root, |d| {
            index
                .get(&format!("{}-{}", d.packed_hash(), d.size_bytes()))
                .cloned()
        });

        assert!(out.is_empty());
    }
}
