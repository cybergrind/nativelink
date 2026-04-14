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
pub fn walk_tree<F>(root_digest: &DigestInfo, mut lookup: F) -> Vec<(String, DigestInfo)>
where
    F: FnMut(&DigestInfo) -> Option<Vec<DirEntry>>,
{
    let mut out = Vec::new();
    let mut stack: VecDeque<(String, DigestInfo)> = VecDeque::new();
    stack.push_back((String::new(), *root_digest));

    while let Some((prefix, dir_digest)) = stack.pop_front() {
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

/// Redis-backed dir_index resolver. Walks a Directory subtree by reading
/// `nativelink:dir_index:{digest_hex}-{size}` HASHes populated by the
/// CAS-server upload hook.
pub struct RedisDirIndexResolver {
    redis_client: redis::Client,
    conn: parking_lot::Mutex<Option<redis::Connection>>,
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
            conn: parking_lot::Mutex::new(None),
        })
    }

    fn key_for(digest: &DigestInfo) -> String {
        format!(
            "nativelink:dir_index:{}-{}",
            digest.packed_hash(),
            digest.size_bytes()
        )
    }

    /// Fetch the dir_index HASH for a single Directory digest from Redis
    /// and parse its entries. Returns `None` if the key doesn't exist or
    /// Redis is unreachable.
    pub fn lookup(&self, digest: &DigestInfo) -> Option<Vec<DirEntry>> {
        let key = Self::key_for(digest);
        let pairs: Vec<(String, String)> = {
            let mut slot = self.conn.lock();
            if slot.is_none() {
                *slot = self.redis_client.get_connection().ok();
            }
            let conn = slot.as_mut()?;
            match redis::cmd("HGETALL").arg(&key).query(conn) {
                Ok(v) => v,
                Err(_) => {
                    *slot = None;
                    return None;
                }
            }
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
    /// cached in Redis are skipped silently — the caller is expected to
    /// fall back to the live Directory-proto walk on the worker.
    pub fn resolve_paths(&self, root_digest: &DigestInfo) -> Vec<(String, DigestInfo)> {
        walk_tree(root_digest, |d| self.lookup(d))
    }
}

/// Render a (path, digest) pair into the "path|hash-size" format used
/// in `nativelink:pending_outputs:{machine_id}` LIST entries. This must
/// match the format consumed by the worker's drain path.
pub fn pending_entry_string(path: &str, digest: &DigestInfo) -> String {
    format!("{path}|{}-{}", digest.packed_hash(), digest.size_bytes())
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
/// against `nativelink:worker_state:{machine_id}` HASHes.
pub struct DispatchJournaler {
    redis_client: redis::Client,
    conn: parking_lot::Mutex<Option<redis::Connection>>,
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
            conn: parking_lot::Mutex::new(None),
        })
    }

    /// Push pending_outputs entries for files the given worker doesn't
    /// already have on disk. Best-effort: Redis errors are logged at
    /// `warn!` level and swallowed.
    pub fn publish_for_worker(
        &self,
        machine_id: &str,
        walked: &[(String, DigestInfo)],
    ) -> Result<usize, redis::RedisError> {
        if walked.is_empty() {
            return Ok(0);
        }
        let mut slot = self.conn.lock();
        if slot.is_none() {
            *slot = Some(self.redis_client.get_connection()?);
        }
        let conn = slot.as_mut().expect("just populated");
        let worker_state_key = format!("nativelink:worker_state:{machine_id}");

        // Fetch all relevant paths from worker_state in one HMGET.
        let paths: Vec<&str> = walked.iter().map(|(p, _)| p.as_str()).collect();
        let current_values: Vec<Option<String>> = if paths.is_empty() {
            Vec::new()
        } else {
            match redis::cmd("HMGET")
                .arg(&worker_state_key)
                .arg(&paths)
                .query(conn)
            {
                Ok(v) => v,
                Err(e) => {
                    // Drop connection so next call reconnects.
                    *slot = None;
                    return Err(e);
                }
            }
        };

        let mut to_push = Vec::new();
        for (idx, (path, digest)) in walked.iter().enumerate() {
            let expected = format!("{}-{}", digest.packed_hash(), digest.size_bytes());
            let current = current_values.get(idx).cloned().flatten();
            if current.as_deref() != Some(expected.as_str()) {
                to_push.push(pending_entry_string(path, digest));
            }
        }

        if to_push.is_empty() {
            return Ok(0);
        }
        let conn = slot.as_mut().expect("still populated");
        let pending_key = format!("nativelink:pending_outputs:{machine_id}");
        let mut cmd = redis::cmd("RPUSH");
        cmd.arg(&pending_key);
        for entry in &to_push {
            cmd.arg(entry);
        }
        if let Err(e) = cmd.query::<i64>(conn) {
            *slot = None;
            return Err(e);
        }
        Ok(to_push.len())
    }
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
        let s = pending_entry_string("foo/bar.o", &d);
        assert!(s.starts_with("foo/bar.o|"));
        assert!(s.ends_with("-42"));
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
