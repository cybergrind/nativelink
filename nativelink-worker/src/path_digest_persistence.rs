// Copyright 2026 The NativeLink Authors. All rights reserved.
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

//! On-disk persistence for the worker's path-digest cache (Plan K).
//!
//! Plan K lives in `Arc<RwLock<HashMap<PathBuf, DigestInfo>>>` in process
//! memory and is wiped on every NL restart. Empirically this costs ~3h
//! of file-hash CPU and a 5–15 min cold window per worker on the
//! inverted-topology chromium harness. This module adds an opt-in
//! disk snapshot: encode the map, atomically write it on a background
//! cadence, load it back at startup with a per-entry stat-gate +
//! size-equality check.
//!
//! Trust contract: entries are only inserted into Plan K after a
//! successful Plan I digest verification (`file_matches_digest`) or a
//! CAS-fetch materialization, so the persisted `(path, digest)` pairs
//! carry a real verified-content invariant. The load-time gate covers
//! the "file changed while NL was offline" window — entries with a
//! missing or wrong-size on-disk file are dropped before the map is
//! ever consulted.
//!
//! Snapshot format (v1):
//! ```text
//!   bytes 0..4   magic "NLPK"
//!   bytes 4..8   format_version: u32 LE  (= 1)
//!   bytes 8..    bincode (standard config) of Vec<(PathBuf, DigestInfo)>
//! ```
//! Decode rejects mismatched magic or version: the caller drops the
//! snapshot and starts cold. Layout changes bump `FORMAT_VERSION`.

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tokio::sync::Notify;
use tracing::{info, warn};

use nativelink_util::common::DigestInfo;
use nativelink_util::timing::StageStats;

use crate::path_digest_cache::SharedPathDigestMap;

const MAGIC: &[u8; 4] = b"NLPK";
const FORMAT_VERSION: u32 = 1;
const HEADER_LEN: usize = MAGIC.len() + size_of::<u32>();

#[derive(Debug, PartialEq, Eq)]
pub enum SnapshotError {
    /// First 4 bytes were not "NLPK". File is foreign or corrupt.
    BadMagic,
    /// Magic matched but version word did not match `FORMAT_VERSION`.
    /// Bumps happen on layout changes; older snapshots are dropped.
    UnsupportedVersion { found: u32, expected: u32 },
    /// Fewer than `HEADER_LEN` bytes — too small to even check the
    /// header. The body's truncation is reported as `Decode`.
    Truncated,
    /// Bincode failed to decode the body.
    Decode(String),
    /// Bincode failed to encode the entries (e.g. a non-UTF-8 PathBuf).
    Encode(String),
}

impl core::fmt::Display for SnapshotError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::BadMagic => write!(f, "snapshot: bad magic (not NLPK)"),
            Self::UnsupportedVersion { found, expected } => write!(
                f,
                "snapshot: unsupported format_version {found} (expected {expected})"
            ),
            Self::Truncated => write!(f, "snapshot: truncated header"),
            Self::Decode(e) => write!(f, "snapshot: decode error: {e}"),
            Self::Encode(e) => write!(f, "snapshot: encode error: {e}"),
        }
    }
}

impl std::error::Error for SnapshotError {}

/// Encode entries into a self-describing snapshot blob.
pub fn encode_snapshot(
    entries: &[(PathBuf, DigestInfo)],
) -> Result<Vec<u8>, SnapshotError> {
    let body = bincode::serde::encode_to_vec(entries, bincode::config::standard())
        .map_err(|e| SnapshotError::Encode(e.to_string()))?;
    let mut out = Vec::with_capacity(HEADER_LEN + body.len());
    out.extend_from_slice(MAGIC);
    out.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
    out.extend_from_slice(&body);
    Ok(out)
}

/// Decode a snapshot blob. Validates the header before touching bincode.
pub fn decode_snapshot(
    bytes: &[u8],
) -> Result<Vec<(PathBuf, DigestInfo)>, SnapshotError> {
    if bytes.len() < HEADER_LEN {
        return Err(SnapshotError::Truncated);
    }
    if &bytes[0..MAGIC.len()] != MAGIC {
        return Err(SnapshotError::BadMagic);
    }
    let version_bytes: [u8; 4] = bytes[MAGIC.len()..HEADER_LEN]
        .try_into()
        .expect("HEADER_LEN guarantees 4 bytes");
    let version = u32::from_le_bytes(version_bytes);
    if version != FORMAT_VERSION {
        return Err(SnapshotError::UnsupportedVersion {
            found: version,
            expected: FORMAT_VERSION,
        });
    }
    let (entries, _) = bincode::serde::decode_from_slice::<Vec<(PathBuf, DigestInfo)>, _>(
        &bytes[HEADER_LEN..],
        bincode::config::standard(),
    )
    .map_err(|e| SnapshotError::Decode(e.to_string()))?;
    Ok(entries)
}

/// Process-shared "is the map dirty since the last flush?" flag.
/// Cloned alongside the `SharedPathDigestMap` so every `PathDigestCache`
/// instance can mark dirty without taking a lock, and the background
/// flush task can clear-and-claim atomically via `take_dirty`.
pub type DirtyBit = Arc<AtomicBool>;

/// Build a fresh dirty bit (initially clean).
#[must_use]
pub fn new_dirty_bit() -> DirtyBit {
    Arc::new(AtomicBool::new(false))
}

/// Mark the map as having un-flushed changes.
pub fn mark_dirty(bit: &DirtyBit) {
    bit.store(true, Ordering::Release);
}

/// Atomically clear the bit and return its previous value. The flush
/// task uses this to decide whether to flush *and* to claim the work
/// in one operation, so a concurrent insert during flush re-marks the
/// bit and gets picked up on the next tick.
pub fn take_dirty(bit: &DirtyBit) -> bool {
    bit.swap(false, Ordering::AcqRel)
}

/// Read the current dirty state without modifying it. Tests and
/// observability only — the flush path uses `take_dirty`.
pub fn is_dirty(bit: &DirtyBit) -> bool {
    bit.load(Ordering::Acquire)
}

/// Outcome of a snapshot-load attempt. Surfaced through counters and the
/// startup log so operators can see how many entries survived the
/// stat-gate.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct LoadStats {
    /// Entries that passed both the existence and size-equality checks
    /// and were inserted into the shared map.
    pub loaded: usize,
    /// Entries whose on-disk file was missing or had a different size
    /// than the persisted `DigestInfo`. These are dropped silently —
    /// the file was edited, removed, or replaced while NL was offline.
    pub dropped_stale: usize,
    /// Counts wholesale snapshot rejection: file unreadable, header
    /// invalid, or bincode body un-decodable. At most `1` per call —
    /// one bad file means we start cold, no partial loads.
    pub dropped_corrupt: usize,
}

/// Read a snapshot at `path` into the shared map. Each entry must (a)
/// resolve to a file on disk and (b) have its file size equal the
/// persisted `DigestInfo::size_bytes()` — otherwise the entry is
/// dropped. Following symlinks (`fs::metadata`, not `symlink_metadata`)
/// is intentional: Plan I hashes the target's content, so the size
/// the snapshot recorded is the target's size.
///
/// A missing snapshot file is a clean no-op (returns zeros). A corrupt
/// snapshot increments `dropped_corrupt` and inserts nothing — the
/// caller starts cold.
pub fn load_into(map: &SharedPathDigestMap, path: &Path) -> LoadStats {
    let bytes = match fs::read(path) {
        Ok(b) => b,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return LoadStats::default();
        }
        // Any other I/O error (permission, EIO, …) is treated as a
        // corrupt snapshot. Better to start cold than to silently
        // ignore a real failure.
        Err(_) => {
            return LoadStats {
                dropped_corrupt: 1,
                ..LoadStats::default()
            };
        }
    };
    let entries = match decode_snapshot(&bytes) {
        Ok(e) => e,
        Err(_) => {
            return LoadStats {
                dropped_corrupt: 1,
                ..LoadStats::default()
            };
        }
    };
    let mut stats = LoadStats::default();
    let mut writer = map.write();
    for (entry_path, digest) in entries {
        match fs::metadata(&entry_path) {
            Ok(md) if md.is_file() && md.len() == digest.size_bytes() => {
                writer.insert(entry_path, digest);
                stats.loaded += 1;
                LOADED.incr();
            }
            _ => {
                stats.dropped_stale += 1;
                DROPPED_STALE.incr();
            }
        }
    }
    stats
}

/// Reasons `save_to` can fail. The flush task logs and continues; a
/// failed save just leaves the previous snapshot in place.
#[derive(Debug)]
pub enum SaveError {
    Encode(SnapshotError),
    Io(std::io::Error),
}

impl core::fmt::Display for SaveError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Encode(e) => write!(f, "save: encode failed: {e}"),
            Self::Io(e) => write!(f, "save: io error: {e}"),
        }
    }
}

impl std::error::Error for SaveError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Encode(e) => Some(e),
            Self::Io(e) => Some(e),
        }
    }
}

/// Atomically write the shared map to disk. Strategy: encode under a
/// short read lock, write a sibling temp file, fsync the file, rename
/// to `path`, then best-effort fsync the parent directory so the
/// directory-entry update is durable (`man 2 fsync`).
///
/// The temp filename includes the pid to make a panicked predecessor's
/// stale temp file harmless — a fresh save uses a new name. Multiple
/// NL processes pointing at the same snapshot path is unsupported by
/// design (per `MAC_DATA_DIR` precedent), so two-writer races aren't
/// in scope.
pub fn save_to(map: &SharedPathDigestMap, path: &Path) -> Result<(), SaveError> {
    // Hold the read lock just long enough to clone entries out — encoding
    // and I/O run unlocked so insert-side latency is unaffected.
    let entries: Vec<(PathBuf, DigestInfo)> = {
        let r = map.read();
        r.iter().map(|(p, d)| (p.clone(), *d)).collect()
    };
    let bytes = encode_snapshot(&entries).map_err(SaveError::Encode)?;

    let parent = path.parent().ok_or_else(|| {
        SaveError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "snapshot path has no parent directory",
        ))
    })?;
    let file_name = path.file_name().ok_or_else(|| {
        SaveError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "snapshot path has no file name",
        ))
    })?;
    let tmp_path = parent.join(format!(
        "{}.tmp.{}",
        file_name.to_string_lossy(),
        std::process::id(),
    ));

    {
        let mut f = fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&tmp_path)
            .map_err(SaveError::Io)?;
        f.write_all(&bytes).map_err(SaveError::Io)?;
        f.sync_all().map_err(SaveError::Io)?;
    }

    fs::rename(&tmp_path, path).map_err(SaveError::Io)?;

    // Best-effort: any error fsync'ing the parent dir is non-fatal —
    // the file content is already on disk and the rename has happened.
    // Worst case after a power loss is the rename rolls back and we
    // load the previous snapshot at next start, which is still safe.
    if let Ok(dir) = fs::File::open(parent) {
        drop(dir.sync_all());
    }

    Ok(())
}

/// Spawn the background flush task. Wakes every `interval`; on each
/// tick, atomically clears the dirty bit and — if it had been set —
/// snapshots the map to disk via `save_to`. On shutdown (the caller
/// calls `shutdown.notify_one()`), runs one final flush if dirty and
/// returns.
///
/// Errors during a flush are logged at `warn!` and the loop continues:
/// a transient I/O failure must not take down the worker. The next
/// successful flush carries the latest map state — there is no
/// per-flush retention.
pub fn spawn_flush_task(
    map: SharedPathDigestMap,
    dirty: DirtyBit,
    path: PathBuf,
    interval: Duration,
    shutdown: Arc<Notify>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                () = shutdown.notified() => {
                    if take_dirty(&dirty) {
                        match save_to(&map, &path) {
                            Ok(()) => {
                                FLUSH.incr();
                                info!(
                                    target: "nativelink",
                                    path = %path.display(),
                                    "plan_k.persistence: final flush on shutdown",
                                );
                            }
                            Err(e) => warn!(
                                target: "nativelink",
                                path = %path.display(),
                                error = %e,
                                "plan_k.persistence: final flush failed",
                            ),
                        }
                    }
                    return;
                }
                () = tokio::time::sleep(interval) => {
                    if take_dirty(&dirty) {
                        match save_to(&map, &path) {
                            Ok(()) => FLUSH.incr(),
                            Err(e) => warn!(
                                target: "nativelink",
                                path = %path.display(),
                                error = %e,
                                "plan_k.persistence: periodic flush failed; retrying next tick",
                            ),
                        }
                    }
                }
            }
        }
    })
}

/// Per-entry: the file existed and its size matched
/// `DigestInfo::size_bytes()`. Compare to the in-memory
/// `worker.plan_k.{hit,miss}` counters during a steady-state run to see
/// what fraction of cold-start traffic the persistence is absorbing.
static LOADED: StageStats =
    StageStats::new("worker.plan_k.persistence.loaded");

/// Per-entry: snapshot referenced a file that was missing or whose
/// size disagreed with the persisted digest. A spike here after an
/// upgrade signals that input-tree contents drifted while NL was
/// offline — useful to validate that the stat-gate is doing real work.
static DROPPED_STALE: StageStats =
    StageStats::new("worker.plan_k.persistence.dropped_stale");

/// Per-snapshot: a successful background or shutdown flush. Steady
/// state on a busy worker is roughly one flush per `flush_interval`
/// while inserts are happening; idle workers should see zero.
static FLUSH: StageStats =
    StageStats::new("worker.plan_k.persistence.flush");

#[cfg(test)]
mod tests {
    use super::*;
    use crate::path_digest_cache::new_shared_path_digest_map;

    fn digest(seed: u8, size: u64) -> DigestInfo {
        DigestInfo::new([seed; 32], size)
    }

    #[test]
    fn snapshot_round_trip_empty() {
        let entries: Vec<(PathBuf, DigestInfo)> = Vec::new();
        let encoded = encode_snapshot(&entries).expect("encode");
        let decoded = decode_snapshot(&encoded).expect("decode");
        assert_eq!(decoded, entries);
    }

    #[test]
    fn snapshot_round_trip_one_entry() {
        let entries = vec![(PathBuf::from("/a/b"), digest(0, 42))];
        let encoded = encode_snapshot(&entries).expect("encode");
        let decoded = decode_snapshot(&encoded).expect("decode");
        assert_eq!(decoded, entries);
    }

    #[test]
    fn snapshot_round_trip_many() {
        let mut entries = Vec::with_capacity(1000);
        for i in 0u32..1000 {
            // Realistic chromium-shaped path (~150 chars) so the size
            // assertion below is meaningful for the production workload.
            let path = PathBuf::from(format!(
                "/Users/macworker/data/work/chromium/src/third_party/blink/renderer/modules/some_submodule/generated_file_{i:04}.cc"
            ));
            entries.push((path, digest((i % 251) as u8, u64::from(i) * 17)));
        }
        let encoded = encode_snapshot(&entries).expect("encode");
        assert!(
            encoded.len() < 1 << 20,
            "1000 entries encoded to {} B, expected < 1 MiB",
            encoded.len(),
        );
        let decoded = decode_snapshot(&encoded).expect("decode");
        assert_eq!(decoded, entries);
    }

    #[test]
    fn snapshot_decode_rejects_wrong_magic() {
        let entries = vec![(PathBuf::from("/x"), digest(7, 7))];
        let mut encoded = encode_snapshot(&entries).expect("encode");
        encoded[0] = b'X';
        assert_eq!(decode_snapshot(&encoded), Err(SnapshotError::BadMagic));
    }

    #[test]
    fn snapshot_decode_rejects_wrong_version() {
        let entries = vec![(PathBuf::from("/x"), digest(7, 7))];
        let mut encoded = encode_snapshot(&entries).expect("encode");
        encoded[4] = 99;
        assert_eq!(
            decode_snapshot(&encoded),
            Err(SnapshotError::UnsupportedVersion {
                found: 99,
                expected: FORMAT_VERSION,
            })
        );
    }

    #[test]
    fn snapshot_decode_rejects_truncated_header() {
        let bytes = [b'N', b'L', b'P', b'K', 1, 0];
        assert_eq!(decode_snapshot(&bytes), Err(SnapshotError::Truncated));
    }

    #[test]
    fn snapshot_decode_returns_decode_error_on_truncated_body() {
        let entries = vec![
            (PathBuf::from("/some/path"), digest(1, 100)),
            (PathBuf::from("/another/path"), digest(2, 200)),
        ];
        let encoded = encode_snapshot(&entries).expect("encode");
        // Lop off everything past the header — header passes, body fails.
        let truncated = &encoded[..HEADER_LEN + 1];
        match decode_snapshot(truncated) {
            Err(SnapshotError::Decode(_)) => {}
            other => panic!("expected Decode error, got {other:?}"),
        }
    }

    /// Drop a known-size file at `dir/name` and return the path. Test
    /// helper for the load_into test fixtures.
    fn write_file(dir: &Path, name: &str, size: usize) -> PathBuf {
        let path = dir.join(name);
        let bytes = vec![0u8; size];
        fs::write(&path, bytes).expect("test file write");
        path
    }

    #[test]
    fn load_from_missing_path_is_noop() {
        let map = new_shared_path_digest_map();
        let stats = load_into(&map, Path::new("/nonexistent/path/plan_k.bin"));
        assert_eq!(stats, LoadStats::default());
        assert_eq!(map.read().len(), 0);
    }

    #[test]
    fn load_inserts_entries_when_files_exist_with_correct_size() {
        let dir = tempfile::tempdir().expect("tempdir");
        let p1 = write_file(dir.path(), "a", 100);
        let p2 = write_file(dir.path(), "b", 200);
        let p3 = write_file(dir.path(), "c", 300);
        let entries = vec![
            (p1.clone(), digest(1, 100)),
            (p2.clone(), digest(2, 200)),
            (p3.clone(), digest(3, 300)),
        ];
        let snapshot_path = dir.path().join("plan_k.bin");
        fs::write(&snapshot_path, encode_snapshot(&entries).expect("encode"))
            .expect("write snapshot");

        let map = new_shared_path_digest_map();
        let stats = load_into(&map, &snapshot_path);
        assert_eq!(
            stats,
            LoadStats {
                loaded: 3,
                dropped_stale: 0,
                dropped_corrupt: 0
            }
        );
        let m = map.read();
        assert_eq!(m.len(), 3);
        assert_eq!(m.get(&p1), Some(&digest(1, 100)));
        assert_eq!(m.get(&p2), Some(&digest(2, 200)));
        assert_eq!(m.get(&p3), Some(&digest(3, 300)));
    }

    #[test]
    fn load_drops_entry_when_file_missing() {
        let dir = tempfile::tempdir().expect("tempdir");
        let p1 = write_file(dir.path(), "a", 100);
        let p2 = write_file(dir.path(), "b", 200);
        // p3 was in the snapshot but the file no longer exists on disk.
        let p3 = dir.path().join("c");
        let entries = vec![
            (p1.clone(), digest(1, 100)),
            (p2.clone(), digest(2, 200)),
            (p3.clone(), digest(3, 300)),
        ];
        let snapshot_path = dir.path().join("plan_k.bin");
        fs::write(&snapshot_path, encode_snapshot(&entries).expect("encode"))
            .expect("write snapshot");

        let map = new_shared_path_digest_map();
        let stats = load_into(&map, &snapshot_path);
        assert_eq!(stats.loaded, 2);
        assert_eq!(stats.dropped_stale, 1);
        assert_eq!(stats.dropped_corrupt, 0);
        let m = map.read();
        assert!(m.contains_key(&p1));
        assert!(m.contains_key(&p2));
        assert!(!m.contains_key(&p3));
    }

    #[test]
    fn load_drops_entry_when_file_size_mismatches() {
        let dir = tempfile::tempdir().expect("tempdir");
        // On-disk file is 50 bytes, snapshot claims 100. Stat-gate must
        // reject — closes the same-size class beyond what the in-memory
        // gate catches via existence-only.
        let p = write_file(dir.path(), "swapped", 50);
        let entries = vec![(p.clone(), digest(1, 100))];
        let snapshot_path = dir.path().join("plan_k.bin");
        fs::write(&snapshot_path, encode_snapshot(&entries).expect("encode"))
            .expect("write snapshot");

        let map = new_shared_path_digest_map();
        let stats = load_into(&map, &snapshot_path);
        assert_eq!(stats.loaded, 0);
        assert_eq!(stats.dropped_stale, 1);
        assert_eq!(map.read().len(), 0);
    }

    #[test]
    fn load_from_corrupt_snapshot_drops_all() {
        let dir = tempfile::tempdir().expect("tempdir");
        let snapshot_path = dir.path().join("plan_k.bin");
        fs::write(&snapshot_path, b"this is not a snapshot")
            .expect("write garbage");

        let map = new_shared_path_digest_map();
        let stats = load_into(&map, &snapshot_path);
        assert_eq!(stats.loaded, 0);
        assert_eq!(stats.dropped_stale, 0);
        assert_eq!(stats.dropped_corrupt, 1);
        assert_eq!(map.read().len(), 0);
    }

    #[test]
    fn fresh_dirty_flag_is_false() {
        let d = new_dirty_bit();
        assert!(!is_dirty(&d));
    }

    #[test]
    fn mark_then_is_dirty() {
        let d = new_dirty_bit();
        mark_dirty(&d);
        assert!(is_dirty(&d));
    }

    #[test]
    fn take_resets_dirty() {
        let d = new_dirty_bit();
        mark_dirty(&d);
        let prev = take_dirty(&d);
        assert!(prev);
        assert!(!is_dirty(&d));
    }

    #[test]
    fn take_returns_prior_value_when_clean() {
        let d = new_dirty_bit();
        let prev = take_dirty(&d);
        assert!(!prev);
        assert!(!is_dirty(&d));
    }

    #[test]
    fn mark_after_take_re_arms_dirty() {
        // The flush task pattern: take, write, then a concurrent insert
        // re-marks. The next tick must see the new dirty state.
        let d = new_dirty_bit();
        mark_dirty(&d);
        assert!(take_dirty(&d));
        mark_dirty(&d);
        assert!(is_dirty(&d));
        assert!(take_dirty(&d));
    }

    #[test]
    fn save_writes_decodable_snapshot() {
        let dir = tempfile::tempdir().expect("tempdir");
        let map = new_shared_path_digest_map();
        {
            let mut w = map.write();
            w.insert(PathBuf::from("/x/a"), digest(1, 11));
            w.insert(PathBuf::from("/x/b"), digest(2, 22));
            w.insert(PathBuf::from("/x/c"), digest(3, 33));
            w.insert(PathBuf::from("/x/d"), digest(4, 44));
            w.insert(PathBuf::from("/x/e"), digest(5, 55));
        }

        let snapshot_path = dir.path().join("plan_k.bin");
        save_to(&map, &snapshot_path).expect("save");
        assert!(snapshot_path.exists());

        let bytes = fs::read(&snapshot_path).expect("read snapshot");
        let mut decoded = decode_snapshot(&bytes).expect("decode");
        decoded.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(
            decoded,
            vec![
                (PathBuf::from("/x/a"), digest(1, 11)),
                (PathBuf::from("/x/b"), digest(2, 22)),
                (PathBuf::from("/x/c"), digest(3, 33)),
                (PathBuf::from("/x/d"), digest(4, 44)),
                (PathBuf::from("/x/e"), digest(5, 55)),
            ],
        );
    }

    #[test]
    fn save_overwrites_existing_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let snapshot_path = dir.path().join("plan_k.bin");
        // Pre-existing different-content file at the target path.
        fs::write(&snapshot_path, b"old garbage that should be replaced")
            .expect("seed file");

        let map = new_shared_path_digest_map();
        map.write().insert(PathBuf::from("/only"), digest(9, 9));
        save_to(&map, &snapshot_path).expect("save");

        let bytes = fs::read(&snapshot_path).expect("read");
        let decoded = decode_snapshot(&bytes).expect("decode");
        assert_eq!(decoded, vec![(PathBuf::from("/only"), digest(9, 9))]);
    }

    #[tokio::test]
    async fn flush_task_writes_when_dirty() {
        let dir = tempfile::tempdir().expect("tempdir");
        let snapshot_path = dir.path().join("plan_k.bin");
        let map = new_shared_path_digest_map();
        let dirty = new_dirty_bit();
        let shutdown = Arc::new(Notify::new());

        // Populate before the task even runs to avoid a race where the
        // first tick fires before the insert.
        {
            let mut w = map.write();
            w.insert(PathBuf::from("/x"), digest(1, 11));
        }
        mark_dirty(&dirty);

        let handle = spawn_flush_task(
            map.clone(),
            dirty.clone(),
            snapshot_path.clone(),
            Duration::from_millis(20),
            shutdown.clone(),
        );

        // Wait long enough for at least one tick to fire.
        tokio::time::sleep(Duration::from_millis(120)).await;
        assert!(snapshot_path.exists(), "flush task should have written the snapshot");
        assert!(!is_dirty(&dirty), "dirty bit should be cleared after flush");

        let bytes = fs::read(&snapshot_path).expect("read snapshot");
        let decoded = decode_snapshot(&bytes).expect("decode");
        assert_eq!(decoded, vec![(PathBuf::from("/x"), digest(1, 11))]);

        shutdown.notify_one();
        handle.await.expect("task join");
    }

    #[tokio::test]
    async fn flush_task_skips_when_clean() {
        let dir = tempfile::tempdir().expect("tempdir");
        let snapshot_path = dir.path().join("plan_k.bin");
        let map = new_shared_path_digest_map();
        let dirty = new_dirty_bit();
        let shutdown = Arc::new(Notify::new());

        let handle = spawn_flush_task(
            map.clone(),
            dirty.clone(),
            snapshot_path.clone(),
            Duration::from_millis(20),
            shutdown.clone(),
        );
        // Idle through several ticks. No insert, no mark_dirty → no
        // file should ever appear.
        tokio::time::sleep(Duration::from_millis(120)).await;
        assert!(
            !snapshot_path.exists(),
            "no flush should occur when the map is clean",
        );

        shutdown.notify_one();
        handle.await.expect("task join");
    }

    #[tokio::test]
    async fn flush_task_final_flush_on_shutdown() {
        let dir = tempfile::tempdir().expect("tempdir");
        let snapshot_path = dir.path().join("plan_k.bin");
        let map = new_shared_path_digest_map();
        let dirty = new_dirty_bit();
        let shutdown = Arc::new(Notify::new());

        // Long interval so a periodic tick cannot fire — the only way
        // the file appears is via the shutdown branch.
        let handle = spawn_flush_task(
            map.clone(),
            dirty.clone(),
            snapshot_path.clone(),
            Duration::from_secs(3600),
            shutdown.clone(),
        );

        // Mutate after the task has started; the periodic timer can't
        // catch it, so only the shutdown final-flush will.
        tokio::time::sleep(Duration::from_millis(20)).await;
        {
            let mut w = map.write();
            w.insert(PathBuf::from("/late"), digest(7, 77));
        }
        mark_dirty(&dirty);

        shutdown.notify_one();
        handle.await.expect("task join");

        assert!(snapshot_path.exists(), "shutdown should trigger a final flush");
        let bytes = fs::read(&snapshot_path).expect("read snapshot");
        let decoded = decode_snapshot(&bytes).expect("decode");
        assert_eq!(decoded, vec![(PathBuf::from("/late"), digest(7, 77))]);
    }

    #[tokio::test]
    async fn flush_task_shutdown_when_clean_writes_nothing() {
        let dir = tempfile::tempdir().expect("tempdir");
        let snapshot_path = dir.path().join("plan_k.bin");
        let map = new_shared_path_digest_map();
        let dirty = new_dirty_bit();
        let shutdown = Arc::new(Notify::new());

        let handle = spawn_flush_task(
            map.clone(),
            dirty.clone(),
            snapshot_path.clone(),
            Duration::from_secs(3600),
            shutdown.clone(),
        );
        shutdown.notify_one();
        handle.await.expect("task join");

        assert!(
            !snapshot_path.exists(),
            "clean shutdown must not produce a file",
        );
    }

    #[test]
    fn save_then_load_round_trip_with_real_files() {
        let dir = tempfile::tempdir().expect("tempdir");
        let p1 = write_file(dir.path(), "x", 17);
        let p2 = write_file(dir.path(), "y", 999);

        let src_map = new_shared_path_digest_map();
        {
            let mut w = src_map.write();
            w.insert(p1.clone(), digest(1, 17));
            w.insert(p2.clone(), digest(2, 999));
        }
        let snapshot_path = dir.path().join("plan_k.bin");
        save_to(&src_map, &snapshot_path).expect("save");

        let dst_map = new_shared_path_digest_map();
        let stats = load_into(&dst_map, &snapshot_path);
        assert_eq!(stats.loaded, 2);
        assert_eq!(stats.dropped_stale, 0);
        let m = dst_map.read();
        assert_eq!(m.get(&p1), Some(&digest(1, 17)));
        assert_eq!(m.get(&p2), Some(&digest(2, 999)));
    }

    #[test]
    fn load_from_wrong_version_drops_all() {
        let dir = tempfile::tempdir().expect("tempdir");
        let entries = vec![(PathBuf::from("/x"), digest(1, 1))];
        let mut bytes = encode_snapshot(&entries).expect("encode");
        bytes[4] = 99; // bump version to something we don't speak
        let snapshot_path = dir.path().join("plan_k.bin");
        fs::write(&snapshot_path, &bytes).expect("write snapshot");

        let map = new_shared_path_digest_map();
        let stats = load_into(&map, &snapshot_path);
        assert_eq!(stats.dropped_corrupt, 1);
        assert_eq!(map.read().len(), 0);
    }
}
