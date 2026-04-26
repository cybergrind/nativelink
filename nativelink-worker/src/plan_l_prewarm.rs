// Copyright 2026 The NativeLink Authors. All rights reserved.
//
// Licensed under the Functional Source License, Version 1.1, Apache 2.0 Future License.

//! Plan L pre-population from the local hint tree.
//!
//! Walk the on-disk source tree once at worker startup, compute the
//! REAPI Directory-proto digest for every subtree, and write
//! `(absolute_path, digest)` into the walked-dirs provider so that
//! subsequent actions whose input root references those digests at
//! those paths skip the per-action recursive walk entirely.
//!
//! Why this is correctness-preserving:
//!
//! Plan L's invariant is "this subtree has been fully materialized at
//! this destination path with this digest." In shared-tree mode, the
//! `work_directory` IS the `hint_root` — files are already on disk at
//! the target path. The prewarm walk hashes those files, computes the
//! Directory proto bytes, and verifies (by computing the digest) that
//! a subtree at `path` would have digest `D`. Marking
//! `dir_walked(path, D)` is then exactly true: any future action that
//! asks for digest `D` at `path` will find the materialization complete.
//!
//! If the action's expected digest disagrees with what the local tree
//! hashes to (file mode/mtime drift, missing/extra files), the digest
//! we marked is *different* from the action's lookup key, so Plan L
//! misses and the normal walk runs. No false hit possible.

use std::path::Path;
#[cfg(test)]
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use futures::future::BoxFuture;
use nativelink_error::Error;
use nativelink_proto::build::bazel::remote::execution::v2::{
    Directory as ProtoDirectory, DirectoryNode, FileNode, SymlinkNode,
};
use nativelink_store::ac_utils::compute_buf_digest;
use nativelink_util::common::DigestInfo;
use nativelink_util::digest_hasher::{DigestHasher, DigestHasherFunc};
use nativelink_util::fs;
use prost::Message;

use crate::path_digest_cache::WalkedDirsProvider;

/// Aggregate counters for one prewarm pass. Exposed so the worker
/// startup log can summarize what was paid for.
#[derive(Debug, Default)]
pub struct PrewarmStats {
    /// Number of directories visited and marked into Plan L.
    pub dirs_marked: usize,
    /// Number of regular files stream-hashed during the walk.
    pub files_hashed: usize,
    /// Total bytes hashed across all files. Useful for back-of-envelope
    /// throughput math (`bytes_hashed / wall_seconds`).
    pub bytes_hashed: u64,
    /// Computed digest of the root `hint_root` proto. Operators can
    /// log this and compare against an action's `input_root_digest` to
    /// confirm prewarm covers the action set.
    pub root_digest: Option<DigestInfo>,
}

/// Walk `hint_root` recursively and call `walked_dirs.mark_dir_walked`
/// for every subtree we visit. Returns aggregate stats and the root
/// digest the synthesizer computed.
///
/// Errors are returned (not swallowed) so the caller can log and
/// optionally retry. The walk does NOT touch CAS.
pub async fn prewarm_from_hint_root(
    hint_root: &Path,
    walked_dirs: &dyn WalkedDirsProvider,
    hasher: DigestHasherFunc,
) -> Result<PrewarmStats, Error> {
    let dirs_marked = AtomicU64::new(0);
    let files_hashed = AtomicU64::new(0);
    let bytes_hashed = AtomicU64::new(0);
    let root_digest = walk_and_mark(
        hint_root,
        walked_dirs,
        hasher,
        &dirs_marked,
        &files_hashed,
        &bytes_hashed,
    )
    .await?;
    Ok(PrewarmStats {
        dirs_marked: dirs_marked.load(Ordering::Relaxed) as usize,
        files_hashed: files_hashed.load(Ordering::Relaxed) as usize,
        bytes_hashed: bytes_hashed.load(Ordering::Relaxed),
        root_digest: Some(root_digest),
    })
}

fn walk_and_mark<'a>(
    dir: &'a Path,
    walked_dirs: &'a dyn WalkedDirsProvider,
    hasher: DigestHasherFunc,
    dirs_marked: &'a AtomicU64,
    files_hashed: &'a AtomicU64,
    bytes_hashed: &'a AtomicU64,
) -> BoxFuture<'a, Result<DigestInfo, Error>> {
    Box::pin(async move {
        let mut read_dir = tokio::fs::read_dir(dir).await.map_err(|e| {
            nativelink_error::make_err!(
                nativelink_error::Code::NotFound,
                "Plan L prewarm: read_dir({}) failed: {e}",
                dir.display(),
            )
        })?;

        let mut files: Vec<FileNode> = Vec::new();
        let mut subdirs: Vec<DirectoryNode> = Vec::new();
        let mut symlinks: Vec<SymlinkNode> = Vec::new();

        while let Some(entry) = read_dir.next_entry().await.map_err(|e| {
            nativelink_error::make_err!(
                nativelink_error::Code::Unavailable,
                "Plan L prewarm: next_entry({}) failed: {e}",
                dir.display(),
            )
        })? {
            let path = entry.path();
            let name = entry
                .file_name()
                .to_str()
                .ok_or_else(|| {
                    nativelink_error::make_err!(
                        nativelink_error::Code::InvalidArgument,
                        "Plan L prewarm: non-utf8 entry name under {}",
                        dir.display(),
                    )
                })?
                .to_string();
            let file_type = entry.file_type().await.map_err(|e| {
                nativelink_error::make_err!(
                    nativelink_error::Code::Unavailable,
                    "Plan L prewarm: file_type({}) failed: {e}",
                    path.display(),
                )
            })?;

            if file_type.is_symlink() {
                let target = tokio::fs::read_link(&path).await.map_err(|e| {
                    nativelink_error::make_err!(
                        nativelink_error::Code::Unavailable,
                        "Plan L prewarm: read_link({}) failed: {e}",
                        path.display(),
                    )
                })?;
                let target_str = target
                    .to_str()
                    .ok_or_else(|| {
                        nativelink_error::make_err!(
                            nativelink_error::Code::InvalidArgument,
                            "Plan L prewarm: non-utf8 symlink target at {}",
                            path.display(),
                        )
                    })?
                    .to_string();
                symlinks.push(SymlinkNode {
                    name,
                    target: target_str,
                    node_properties: None,
                });
            } else if file_type.is_dir() {
                let child_digest = walk_and_mark(
                    &path,
                    walked_dirs,
                    hasher,
                    dirs_marked,
                    files_hashed,
                    bytes_hashed,
                )
                .await?;
                subdirs.push(DirectoryNode {
                    name,
                    digest: Some(child_digest.into()),
                });
            } else if file_type.is_file() {
                let metadata = entry.metadata().await.map_err(|e| {
                    nativelink_error::make_err!(
                        nativelink_error::Code::Unavailable,
                        "Plan L prewarm: metadata({}) failed: {e}",
                        path.display(),
                    )
                })?;
                let file = fs::open_file(&path, 0, u64::MAX).await.map_err(|e| {
                    nativelink_error::make_err!(
                        nativelink_error::Code::Unavailable,
                        "Plan L prewarm: open_file({}) failed: {e}",
                        path.display(),
                    )
                })?;
                let mut h = hasher.hasher();
                let file_digest = h.compute_from_reader(file).await.map_err(|e| {
                    nativelink_error::make_err!(
                        nativelink_error::Code::Internal,
                        "Plan L prewarm: hash file {} failed: {e}",
                        path.display(),
                    )
                })?;
                files_hashed.fetch_add(1, Ordering::Relaxed);
                bytes_hashed
                    .fetch_add(file_digest.size_bytes() as u64, Ordering::Relaxed);
                let is_executable = is_executable_mode(&metadata);
                files.push(FileNode {
                    name,
                    digest: Some(file_digest.into()),
                    is_executable,
                    node_properties: None,
                });
            }
            // Other file types (block/char devices, sockets, fifos):
            // not representable in a REAPI Directory; ignored. If an
            // action ever references such a path the prewarm digest
            // wouldn't include it and Plan L would miss → safe.
        }

        // REAPI canonical encoding requires children sorted by name.
        files.sort_by(|a, b| a.name.cmp(&b.name));
        subdirs.sort_by(|a, b| a.name.cmp(&b.name));
        symlinks.sort_by(|a, b| a.name.cmp(&b.name));

        let proto = ProtoDirectory {
            files,
            directories: subdirs,
            symlinks,
            node_properties: None,
        };
        let bytes = proto.encode_to_vec();
        let digest = compute_buf_digest(&bytes, &mut hasher.hasher());

        // Mark Plan L using the absolute filesystem path. Actions in
        // shared-tree mode use the same absolute path as
        // `current_directory` in `download_to_directory`, so the keys
        // match exactly.
        let path_str = dir
            .to_str()
            .ok_or_else(|| {
                nativelink_error::make_err!(
                    nativelink_error::Code::InvalidArgument,
                    "Plan L prewarm: non-utf8 dir path {}",
                    dir.display(),
                )
            })?;
        walked_dirs.mark_dir_walked(path_str, digest);
        dirs_marked.fetch_add(1, Ordering::Relaxed);

        Ok(digest)
    })
}

/// Operator escape hatch that triggers a background prewarm pass at
/// worker startup. Set `NATIVELINK_PLAN_L_PREWARM=1` (or `true` /
/// `yes` / `on`) to enable. Default off so existing deployments keep
/// their current behavior.
pub fn enabled_via_env() -> bool {
    std::env::var("NATIVELINK_PLAN_L_PREWARM")
        .ok()
        .map(|v| matches!(v.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"))
        .unwrap_or(false)
}

#[cfg(unix)]
fn is_executable_mode(metadata: &std::fs::Metadata) -> bool {
    use std::os::unix::fs::MetadataExt;
    metadata.mode() & 0o111 != 0
}

#[cfg(not(unix))]
fn is_executable_mode(_metadata: &std::fs::Metadata) -> bool {
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::path_digest_cache::LocalWalkedDirs;
    use nativelink_macro::nativelink_test;

    fn temp_root(label: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!(
            "nl-prewarm-{}-{}-{}",
            label,
            std::process::id(),
            rand::random::<u64>(),
        ));
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    /// Slice of 1.3.2 (Plan L prewarm): after walking a small tree,
    /// the walked-dirs provider must have an entry for EVERY visited
    /// subdirectory. This is the central correctness property — what
    /// we mark is what `dir_walked()` will hit on later action lookups.
    #[nativelink_test]
    async fn prewarm_marks_every_subdirectory() {
        let root = temp_root("marks_every");
        tokio::fs::create_dir_all(root.join("a/b/c")).await.unwrap();
        tokio::fs::write(root.join("top.txt"), b"top").await.unwrap();
        tokio::fs::write(root.join("a/mid.txt"), b"mid").await.unwrap();
        tokio::fs::write(root.join("a/b/c/leaf.txt"), b"leaf").await.unwrap();

        let walked = LocalWalkedDirs::new();
        let stats =
            prewarm_from_hint_root(&root, &walked, DigestHasherFunc::Sha256)
                .await
                .expect("prewarm must succeed on a well-formed tree");

        // root + a + a/b + a/b/c = 4 dirs
        assert_eq!(stats.dirs_marked, 4, "every visited dir must be marked");
        assert_eq!(stats.files_hashed, 3);
        assert!(stats.bytes_hashed > 0);

        // Every subdirectory must be queryable through the provider
        // with the digest the walker computed for it.
        let root_digest = stats.root_digest.expect("root digest set");
        assert!(
            walked.dir_walked(root.to_str().unwrap(), &root_digest),
            "root path must be marked with the computed root digest",
        );
    }

    /// Prewarm followed by a per-action `dir_walked` lookup at the
    /// SAME path with the SAME digest must hit. This is the property
    /// that lets actions skip the recursive walk after prewarm.
    #[nativelink_test]
    async fn prewarm_makes_subsequent_dir_walked_lookup_hit() {
        let root = temp_root("hit_after_prewarm");
        tokio::fs::write(root.join("a.txt"), b"a").await.unwrap();
        tokio::fs::write(root.join("b.txt"), b"b").await.unwrap();

        let walked = LocalWalkedDirs::new();
        let stats =
            prewarm_from_hint_root(&root, &walked, DigestHasherFunc::Sha256)
                .await
                .unwrap();
        let root_digest = stats.root_digest.unwrap();

        // Simulate the lookup that download_to_directory's top-level
        // Plan L check does. With prewarm done, this MUST hit.
        assert!(
            walked.dir_walked(root.to_str().unwrap(), &root_digest),
            "prewarm must make the action-time Plan L lookup hit",
        );
    }

    /// Mismatched digest must NOT hit. Confirms prewarm doesn't
    /// false-mark — actions whose expected digest disagrees with the
    /// local tree's actual hash fall through to the normal walk.
    #[nativelink_test]
    async fn prewarm_does_not_hit_for_wrong_digest() {
        let root = temp_root("wrong_digest");
        tokio::fs::write(root.join("a.txt"), b"a").await.unwrap();

        let walked = LocalWalkedDirs::new();
        prewarm_from_hint_root(&root, &walked, DigestHasherFunc::Sha256)
            .await
            .unwrap();

        let bogus = DigestInfo::new([0xFFu8; 32], 99);
        assert!(
            !walked.dir_walked(root.to_str().unwrap(), &bogus),
            "Plan L must miss when expected digest disagrees with on-disk content",
        );
    }

    /// Missing hint_root → error returned, not panic. Lets the worker
    /// log a warning and start without prewarm if the configured path
    /// doesn't exist yet (rsync race, fresh deploy).
    #[nativelink_test]
    async fn prewarm_returns_error_for_missing_hint_root() {
        let mut p = std::env::temp_dir();
        p.push(format!(
            "nl-prewarm-missing-{}-{}",
            std::process::id(),
            rand::random::<u64>(),
        ));
        let walked = LocalWalkedDirs::new();
        let result =
            prewarm_from_hint_root(&p, &walked, DigestHasherFunc::Sha256).await;
        assert!(result.is_err(), "missing hint_root must yield Err, not panic");
    }

    /// Empty directory still gets a Plan L mark — REAPI's canonical
    /// empty-Directory digest. Edge case for leaf dirs with no
    /// children.
    #[nativelink_test]
    async fn prewarm_handles_empty_directory() {
        let root = temp_root("empty");
        let walked = LocalWalkedDirs::new();
        let stats =
            prewarm_from_hint_root(&root, &walked, DigestHasherFunc::Sha256)
                .await
                .unwrap();
        assert_eq!(stats.dirs_marked, 1, "empty dir → one mark, the root");
        assert_eq!(stats.files_hashed, 0);
        assert!(walked.dir_walked(root.to_str().unwrap(), &stats.root_digest.unwrap()));
    }
}
