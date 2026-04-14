// Copyright 2024 The NativeLink Authors. All rights reserved.
//
// Licensed under the Functional Source License, Version 1.1, Apache 2.0 Future License.

//! Directory-index: when a Directory protobuf is uploaded to CAS, we cache
//! its children in Redis so the scheduler can walk Directory chains without
//! re-fetching protos from CAS.
//!
//! Data model (Redis):
//!   nativelink:dir_index:{dir_digest_hex}-{size}
//!     HASH: child_name -> "file|{digest_hex}-{size}"
//!                      |  "dir|{digest_hex}-{size}"
//!                      |  "symlink|{target}"

use nativelink_proto::build::bazel::remote::execution::v2::Directory as ProtoDirectory;
use prost::Message;

/// Maximum blob size we will attempt to decode as a Directory protobuf.
/// Most Directory protos are small (a few KB), but Chromium-scale builds
/// can produce Directories with thousands of file entries that exceed
/// 1 MiB. Bug F (cas_journal_failure_report_05) traced a missing-file
/// failure to a Directory proto that wasn't indexed because it exceeded
/// the previous 1 MiB cap. 32 MiB is generous headroom; non-Directory
/// blobs above that size (compiled binaries, etc.) skip decoding cheaply
/// since the proto parser fails fast on non-proto bytes.
pub const MAX_DIR_DECODE_BYTES: usize = 32 * 1024 * 1024; // 32 MiB

/// A single child entry in a Directory protobuf, flattened for indexing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DirChild {
    /// A file entry. `digest_hex` is the child's content hash. `size` is
    /// the file size in bytes.
    File {
        name: String,
        digest_hex: String,
        size: i64,
    },
    /// A subdirectory entry. `digest_hex` is the child directory's digest.
    Dir {
        name: String,
        digest_hex: String,
        size: i64,
    },
    /// A symlink entry pointing at `target`.
    Symlink { name: String, target: String },
}

impl DirChild {
    /// Render the Redis HASH value for this child.
    pub fn to_redis_value(&self) -> String {
        match self {
            DirChild::File {
                digest_hex, size, ..
            } => format!("file|{digest_hex}-{size}"),
            DirChild::Dir {
                digest_hex, size, ..
            } => format!("dir|{digest_hex}-{size}"),
            DirChild::Symlink { target, .. } => format!("symlink|{target}"),
        }
    }

    pub fn name(&self) -> &str {
        match self {
            DirChild::File { name, .. }
            | DirChild::Dir { name, .. }
            | DirChild::Symlink { name, .. } => name,
        }
    }
}

/// Attempt to decode `blob` as a Directory protobuf. Returns `Some(children)`
/// only if decoding succeeds AND the blob looks plausibly like a Directory
/// (has at least one populated field).
///
/// Returns `None` if:
/// - The blob is too large to bother decoding
/// - Decoding fails (not a valid protobuf, or not Directory-shaped)
/// - The decoded proto has no children (empty — not useful to index, and
///   likely a false positive on random bytes that happen to decode)
pub fn try_decode_directory(blob: &[u8]) -> Option<Vec<DirChild>> {
    if blob.len() > MAX_DIR_DECODE_BYTES {
        return None;
    }

    let dir = ProtoDirectory::decode(blob).ok()?;

    // Heuristic: skip truly empty decodes (they're almost always random
    // bytes that happened to parse). A real empty Directory proto exists
    // but is rare and cheap to re-fetch when needed.
    if dir.files.is_empty() && dir.directories.is_empty() && dir.symlinks.is_empty() {
        return None;
    }

    let mut children = Vec::with_capacity(
        dir.files.len() + dir.directories.len() + dir.symlinks.len(),
    );

    for f in dir.files {
        let Some(d) = f.digest else { continue };
        children.push(DirChild::File {
            name: f.name,
            digest_hex: d.hash,
            size: d.size_bytes,
        });
    }
    for d in dir.directories {
        let Some(dig) = d.digest else { continue };
        children.push(DirChild::Dir {
            name: d.name,
            digest_hex: dig.hash,
            size: dig.size_bytes,
        });
    }
    for s in dir.symlinks {
        children.push(DirChild::Symlink {
            name: s.name,
            target: s.target,
        });
    }

    Some(children)
}

/// Redis-backed writer that records Directory proto children so the
/// scheduler can walk dir chains without re-decoding blobs from CAS.
///
/// The writer is best-effort: if Redis is unavailable, the record is
/// silently dropped — the walk will fall back to decoding Directory
/// protos from CAS directly.
///
/// Connection reuse: holds a single persistent `redis::Connection` behind
/// a `parking_lot::Mutex`. Opening a new TCP socket per call would
/// exhaust macOS ephemeral ports (~16k) within seconds on a large build
/// (~5k+ Directory protos + more).
pub struct DirIndexWriter {
    redis_client: redis::Client,
    conn: parking_lot::Mutex<Option<redis::Connection>>,
}

impl std::fmt::Debug for DirIndexWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DirIndexWriter")
            .field("redis_client", &self.redis_client)
            .finish_non_exhaustive()
    }
}

impl DirIndexWriter {
    /// `url` is a Redis connection string (e.g. `redis://127.0.0.1:6379`).
    pub fn new(url: &str) -> Result<Self, redis::RedisError> {
        let redis_client = redis::Client::open(url)?;
        Ok(Self {
            redis_client,
            conn: parking_lot::Mutex::new(None),
        })
    }

    /// Runs `op` with a cached Redis connection, lazily opening one on
    /// first use and reconnecting on error. Keeps the connection alive
    /// across many calls to avoid TCP ephemeral port exhaustion.
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
                // Drop the connection so the next call reconnects.
                *slot = None;
                Err(e)
            }
        }
    }

    /// Build the Redis HASH key for a directory with the given digest hash
    /// and size. Keys are namespaced `nativelink:dir_index:` so they don't
    /// collide with other uses.
    pub fn key_for(digest_hex: &str, size: i64) -> String {
        format!("nativelink:dir_index:{digest_hex}-{size}")
    }

    /// Build the (field, value) pairs to write for a given set of
    /// children. Pure function — no I/O — used both for real writes and
    /// for testing the encoding.
    pub fn build_field_values(children: &[DirChild]) -> Vec<(String, String)> {
        children
            .iter()
            .map(|c| (c.name().to_string(), c.to_redis_value()))
            .collect()
    }

    /// Record a directory's children in Redis. Best-effort: on any Redis
    /// error we return `Ok(())` after logging, so a transient Redis
    /// outage does not break the CAS upload path.
    pub fn record_directory(&self, digest_hex: &str, size: i64, children: &[DirChild]) {
        if children.is_empty() {
            return;
        }
        let key = Self::key_for(digest_hex, size);
        let pairs = Self::build_field_values(children);

        drop(self.with_conn(|conn| {
            let mut cmd = redis::cmd("HSET");
            cmd.arg(&key);
            for (field, value) in &pairs {
                cmd.arg(field).arg(value);
            }
            cmd.query::<i64>(conn)
        }));
    }

    /// Hook to be called after every successful CAS write. Attempts to
    /// detect if the blob is a Directory protobuf, and if so, records its
    /// children in Redis. Safe to call on any blob — non-Directory blobs
    /// are cheaply rejected.
    pub fn maybe_record_from_blob(&self, digest_hex: &str, size: i64, blob: &[u8]) {
        if let Some(children) = try_decode_directory(blob) {
            self.record_directory(digest_hex, size, &children);
        }
    }
}

/// Side-effect-free result of the after-upload hook. Returned for testing
/// so we can assert "hook would have recorded these children" without
/// needing a live Redis. The real hook passes this through to Redis.
#[derive(Debug, PartialEq, Eq)]
pub enum HookResult {
    /// Blob was identified as a Directory proto with these children.
    Indexed {
        digest_hex: String,
        size: i64,
        children: Vec<DirChild>,
    },
    /// Blob is not a Directory proto; no indexing needed.
    Skipped,
}

/// Pure hook logic — decodes the blob and reports what would be recorded.
/// Testable without any Redis or I/O.
pub fn after_upload_hook(digest_hex: &str, size: i64, blob: &[u8]) -> HookResult {
    match try_decode_directory(blob) {
        Some(children) => HookResult::Indexed {
            digest_hex: digest_hex.to_string(),
            size,
            children,
        },
        None => HookResult::Skipped,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nativelink_proto::build::bazel::remote::execution::v2::{
        Digest, DirectoryNode, FileNode, SymlinkNode,
    };

    fn digest(hex: &str, size: i64) -> Digest {
        Digest {
            hash: hex.to_string(),
            size_bytes: size,
        }
    }

    #[test]
    fn decode_empty_blob_returns_none() {
        assert!(try_decode_directory(&[]).is_none());
    }

    #[test]
    fn decode_random_bytes_returns_none() {
        // Random bytes that are unlikely to form a valid non-empty Directory.
        let blob = b"hello world this is not a proto";
        assert!(try_decode_directory(blob).is_none());
    }

    #[test]
    fn decode_directory_with_files() {
        let dir = ProtoDirectory {
            files: vec![FileNode {
                name: "cmath".to_string(),
                digest: Some(digest("abc123", 42)),
                is_executable: false,
                node_properties: None,
            }],
            ..Default::default()
        };
        let blob = dir.encode_to_vec();
        let children = try_decode_directory(&blob).expect("should decode");
        assert_eq!(children.len(), 1);
        assert_eq!(
            children[0],
            DirChild::File {
                name: "cmath".to_string(),
                digest_hex: "abc123".to_string(),
                size: 42,
            }
        );
    }

    #[test]
    fn decode_directory_with_subdirs() {
        let dir = ProtoDirectory {
            directories: vec![DirectoryNode {
                name: "include".to_string(),
                digest: Some(digest("def456", 100)),
            }],
            ..Default::default()
        };
        let blob = dir.encode_to_vec();
        let children = try_decode_directory(&blob).expect("should decode");
        assert_eq!(children.len(), 1);
        assert_eq!(
            children[0],
            DirChild::Dir {
                name: "include".to_string(),
                digest_hex: "def456".to_string(),
                size: 100,
            }
        );
    }

    #[test]
    fn decode_directory_with_symlinks() {
        let dir = ProtoDirectory {
            symlinks: vec![SymlinkNode {
                name: "current".to_string(),
                target: "A".to_string(),
                node_properties: None,
            }],
            ..Default::default()
        };
        let blob = dir.encode_to_vec();
        let children = try_decode_directory(&blob).expect("should decode");
        assert_eq!(
            children[0],
            DirChild::Symlink {
                name: "current".to_string(),
                target: "A".to_string(),
            }
        );
    }

    #[test]
    fn decode_mixed_directory() {
        let dir = ProtoDirectory {
            files: vec![FileNode {
                name: "foo.h".to_string(),
                digest: Some(digest("aaa", 1)),
                is_executable: false,
                node_properties: None,
            }],
            directories: vec![DirectoryNode {
                name: "subdir".to_string(),
                digest: Some(digest("bbb", 2)),
            }],
            symlinks: vec![SymlinkNode {
                name: "link".to_string(),
                target: "foo.h".to_string(),
                node_properties: None,
            }],
            ..Default::default()
        };
        let blob = dir.encode_to_vec();
        let children = try_decode_directory(&blob).expect("should decode");
        assert_eq!(children.len(), 3);
    }

    #[test]
    fn decode_empty_directory_returns_none() {
        let dir = ProtoDirectory::default();
        let blob = dir.encode_to_vec();
        // By design, empty protos (all-default fields) are not indexed —
        // they're indistinguishable from a zero-byte blob.
        assert!(try_decode_directory(&blob).is_none());
    }

    #[test]
    fn decode_oversized_blob_returns_none() {
        let blob = vec![0u8; MAX_DIR_DECODE_BYTES + 1];
        assert!(try_decode_directory(&blob).is_none());
    }

    /// Bug F regression (cas_journal_failure_report_05): the limit must be
    /// large enough to handle real-world Chromium Directory protos. A
    /// Directory listing thousands of files in deep subdirectories can
    /// exceed 1 MiB. Lock the limit at >= 16 MiB so we don't silently
    /// drop indexing for large directories.
    #[test]
    fn max_dir_decode_bytes_is_at_least_16_mib() {
        assert!(
            MAX_DIR_DECODE_BYTES >= 16 * 1024 * 1024,
            "MAX_DIR_DECODE_BYTES={} must be >= 16 MiB to handle Chromium-scale Directory protos",
            MAX_DIR_DECODE_BYTES
        );
    }

    /// Decoding a Directory proto in the 1-2 MiB range must succeed.
    /// (Constructed by stuffing many file entries to force a large proto.)
    #[test]
    fn decode_directory_above_one_mib() {
        // Build a Directory with enough file entries to exceed 1 MiB.
        // Each FileNode at the bytes level: name (string) + digest (struct
        // with hash string + size). Use ~96-byte names to amplify.
        let long_name: String = "a".repeat(96);
        let files: Vec<FileNode> = (0..20_000)
            .map(|i| FileNode {
                name: format!("{long_name}_{i}"),
                digest: Some(digest("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", 100)),
                is_executable: false,
                node_properties: None,
            })
            .collect();
        let dir = ProtoDirectory {
            files,
            ..Default::default()
        };
        let blob = dir.encode_to_vec();
        assert!(
            blob.len() > 1 * 1024 * 1024,
            "test setup: expected >1 MiB, got {} bytes",
            blob.len()
        );
        let children = try_decode_directory(&blob).expect("should decode large Directory");
        assert_eq!(children.len(), 20_000);
    }

    #[test]
    fn to_redis_value_file() {
        let c = DirChild::File {
            name: "cmath".to_string(),
            digest_hex: "abc".to_string(),
            size: 42,
        };
        assert_eq!(c.to_redis_value(), "file|abc-42");
    }

    #[test]
    fn to_redis_value_dir() {
        let c = DirChild::Dir {
            name: "include".to_string(),
            digest_hex: "def".to_string(),
            size: 100,
        };
        assert_eq!(c.to_redis_value(), "dir|def-100");
    }

    #[test]
    fn to_redis_value_symlink() {
        let c = DirChild::Symlink {
            name: "current".to_string(),
            target: "A".to_string(),
        };
        assert_eq!(c.to_redis_value(), "symlink|A");
    }

    #[test]
    fn dir_index_writer_key_format() {
        assert_eq!(
            DirIndexWriter::key_for("abc123", 42),
            "nativelink:dir_index:abc123-42"
        );
    }

    #[test]
    fn dir_index_writer_build_field_values_mixed() {
        let children = vec![
            DirChild::File {
                name: "cmath".to_string(),
                digest_hex: "aaa".to_string(),
                size: 10,
            },
            DirChild::Dir {
                name: "subdir".to_string(),
                digest_hex: "bbb".to_string(),
                size: 20,
            },
            DirChild::Symlink {
                name: "link".to_string(),
                target: "cmath".to_string(),
            },
        ];
        let pairs = DirIndexWriter::build_field_values(&children);
        assert_eq!(pairs.len(), 3);
        assert_eq!(pairs[0], ("cmath".to_string(), "file|aaa-10".to_string()));
        assert_eq!(pairs[1], ("subdir".to_string(), "dir|bbb-20".to_string()));
        assert_eq!(pairs[2], ("link".to_string(), "symlink|cmath".to_string()));
    }

    #[test]
    fn dir_index_writer_build_empty() {
        let pairs = DirIndexWriter::build_field_values(&[]);
        assert!(pairs.is_empty());
    }

    #[test]
    fn dir_index_writer_can_be_created_with_valid_url() {
        assert!(DirIndexWriter::new("redis://127.0.0.1:6379").is_ok());
    }

    #[test]
    fn dir_index_writer_rejects_invalid_url() {
        assert!(DirIndexWriter::new("not-a-url").is_err());
    }

    #[test]
    fn after_upload_hook_indexes_directory_proto() {
        let dir = ProtoDirectory {
            files: vec![FileNode {
                name: "cmath".to_string(),
                digest: Some(digest("abc", 42)),
                is_executable: false,
                node_properties: None,
            }],
            ..Default::default()
        };
        let blob = dir.encode_to_vec();
        let blob_digest = "rootdigest";
        let blob_size = blob.len() as i64;

        let result = after_upload_hook(blob_digest, blob_size, &blob);
        match result {
            HookResult::Indexed {
                digest_hex,
                size,
                children,
            } => {
                assert_eq!(digest_hex, "rootdigest");
                assert_eq!(size, blob_size);
                assert_eq!(children.len(), 1);
                assert_eq!(children[0].name(), "cmath");
            }
            HookResult::Skipped => panic!("expected Indexed, got Skipped"),
        }
    }

    #[test]
    fn after_upload_hook_skips_non_directory_blob() {
        // Random bytes that are not a valid non-empty Directory proto.
        let blob = b"this is just a file content, not a Directory proto";
        let result = after_upload_hook("somedigest", blob.len() as i64, blob);
        assert_eq!(result, HookResult::Skipped);
    }

    #[test]
    fn after_upload_hook_skips_empty_blob() {
        let result = after_upload_hook("somedigest", 0, &[]);
        assert_eq!(result, HookResult::Skipped);
    }

    #[test]
    fn after_upload_hook_skips_large_blob() {
        let blob = vec![0u8; MAX_DIR_DECODE_BYTES + 1];
        let result = after_upload_hook("somedigest", blob.len() as i64, &blob);
        assert_eq!(result, HookResult::Skipped);
    }
}
