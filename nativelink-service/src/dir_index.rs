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
/// Directory protos are typically small (a few KB). Decoding huge blobs as
/// protos wastes CPU for no benefit.
pub const MAX_DIR_DECODE_BYTES: usize = 1 * 1024 * 1024; // 1 MiB

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
#[derive(Debug)]
pub struct DirIndexWriter {
    redis_client: redis::Client,
}

impl DirIndexWriter {
    /// `url` is a Redis connection string (e.g. `redis://127.0.0.1:6379`).
    pub fn new(url: &str) -> Result<Self, redis::RedisError> {
        let redis_client = redis::Client::open(url)?;
        Ok(Self { redis_client })
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

        let Ok(mut conn) = self.redis_client.get_connection() else {
            return;
        };
        let mut cmd = redis::cmd("HSET");
        cmd.arg(&key);
        for (field, value) in &pairs {
            cmd.arg(field).arg(value);
        }
        drop(cmd.query::<i64>(&mut conn));
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
}
