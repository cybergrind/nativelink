// Copyright 2026 The NativeLink Authors. All rights reserved.
//
// Licensed under the Functional Source License, Version 1.1, Apache 2.0 Future License.

//! Plan M: locally synthesize REAPI `Directory` protos by walking the
//! pre-staged source tree on disk. On match, the worker can hand the
//! synthesized proto to `download_to_directory` instead of fetching it
//! from CAS — eliminating network round-trips on cold start when the
//! local tree is intact.
//!
//! Correctness-preserving by construction: a `Hit` is only reported
//! when the synthesized bytes hash to the caller-supplied expected
//! digest. That is the same trust signal `get_and_decode_digest`
//! provides for CAS-fetched protos, so the synthesis path produces a
//! byte-identical `Directory` or returns a miss reason that callers
//! must treat as "fall through to CAS unchanged".

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use nativelink_error::Error;
use nativelink_proto::build::bazel::remote::execution::v2::{
    Directory as ProtoDirectory, DirectoryNode, FileNode, SymlinkNode,
};
use nativelink_store::ac_utils::compute_buf_digest;
use nativelink_util::common::DigestInfo;
use nativelink_util::digest_hasher::{DigestHasher, DigestHasherFunc};
use nativelink_util::fs;
use prost::Message;

/// Result of a local synthesis attempt. The miss variants carry the
/// reason so call sites can split metric buckets and emit informative
/// trace logs without a side channel.
#[derive(Debug)]
pub enum SynthResult {
    /// Local tree's bytes hashed to the caller-supplied expected
    /// digest. Bundle contains every Directory proto produced during
    /// the walk plus every file we verified along the way.
    Hit(SynthBundle),
    /// Walk completed but the root digest did not match. `computed`
    /// is the digest the local tree actually hashed to (useful for
    /// debugging mtime/mode drift).
    MissDigestMismatch { computed: DigestInfo },
    /// I/O error during the walk (missing path, permission denied,
    /// read failure). Carries the underlying error for trace logs.
    MissIoError(Error),
}

/// Side-effects of a successful synthesis: every Directory proto
/// produced and every file whose contents were verified during the
/// walk. `download_to_directory` consults `protos` to skip CAS
/// fetches at every recursive level, Plan I consults `verified_files`
/// to skip a redundant `file_matches_digest` call before hardlinking,
/// and `prepare_action_inputs` consults `dir_paths` to bulk-mark
/// Plan L for every visited subtree (so subsequent actions hit Plan L
/// at the root and skip the whole walk).
#[derive(Debug, Default)]
pub struct SynthBundle {
    /// Every Directory proto produced during the recursive walk,
    /// keyed by its content digest. The root proto's digest is the
    /// one the caller asked for.
    pub protos: HashMap<DigestInfo, ProtoDirectory>,
    /// Map from absolute hint path (e.g. `hint_root/src/foo.cc`) to
    /// the digest the file's contents hashed to. Plan I uses this to
    /// elide its own re-hash pass when about to hardlink.
    pub verified_files: HashMap<PathBuf, DigestInfo>,
    /// Every visited subdirectory's absolute hint path paired with
    /// the digest of its synthesized Directory proto. In shared-tree
    /// mode (hint_root == work_directory), these are exactly the
    /// keys `download_to_directory` will look up in Plan L on
    /// subsequent actions — bulk-marking on synthesis Hit makes
    /// subsequent actions short-circuit at the Plan L check, never
    /// running the walk again.
    pub dir_paths: Vec<(PathBuf, DigestInfo)>,
}

/// Walk `hint_dir` once, synthesize a `Directory` proto for the root
/// (and every recursive subdirectory), and verify the root's serialized
/// bytes hash to `expected_root_digest`. See `SynthResult`.
pub async fn synthesize_directory_tree(
    hint_dir: &Path,
    expected_root_digest: &DigestInfo,
    hasher: DigestHasherFunc,
) -> SynthResult {
    let mut bundle = SynthBundle::default();
    match build_subtree(hint_dir, hasher, &mut bundle).await {
        Ok(root_digest) => {
            if root_digest == *expected_root_digest {
                SynthResult::Hit(bundle)
            } else {
                SynthResult::MissDigestMismatch {
                    computed: root_digest,
                }
            }
        }
        Err(e) => SynthResult::MissIoError(e),
    }
}

/// Recursive worker. Returns the digest of the synthesized Directory
/// proto for `dir` and inserts every Directory proto + verified file
/// into `bundle` along the way.
fn build_subtree<'a>(
    dir: &'a Path,
    hasher: DigestHasherFunc,
    bundle: &'a mut SynthBundle,
) -> futures::future::BoxFuture<'a, Result<DigestInfo, Error>> {
    Box::pin(async move {
        let mut read_dir = tokio::fs::read_dir(dir).await.map_err(|e| {
            nativelink_error::make_err!(
                nativelink_error::Code::NotFound,
                "Plan M: read_dir({}) failed: {e}",
                dir.display(),
            )
        })?;

        let mut files: Vec<FileNode> = Vec::new();
        let mut subdirs: Vec<DirectoryNode> = Vec::new();
        let mut symlinks: Vec<SymlinkNode> = Vec::new();

        while let Some(entry) = read_dir.next_entry().await.map_err(|e| {
            nativelink_error::make_err!(
                nativelink_error::Code::Unavailable,
                "Plan M: next_entry({}) failed: {e}",
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
                        "Plan M: non-utf8 entry name under {}",
                        dir.display(),
                    )
                })?
                .to_string();

            // file_type() is the cheap non-traversing classifier:
            // is_symlink() returns true for the link itself rather
            // than the target. metadata() would resolve the link.
            let file_type = entry.file_type().await.map_err(|e| {
                nativelink_error::make_err!(
                    nativelink_error::Code::Unavailable,
                    "Plan M: file_type({}) failed: {e}",
                    path.display(),
                )
            })?;

            if file_type.is_symlink() {
                let target = tokio::fs::read_link(&path).await.map_err(|e| {
                    nativelink_error::make_err!(
                        nativelink_error::Code::Unavailable,
                        "Plan M: read_link({}) failed: {e}",
                        path.display(),
                    )
                })?;
                let target_str = target.to_str().ok_or_else(|| {
                    nativelink_error::make_err!(
                        nativelink_error::Code::InvalidArgument,
                        "Plan M: non-utf8 symlink target at {}",
                        path.display(),
                    )
                })?;
                symlinks.push(SymlinkNode {
                    name,
                    target: target_str.to_string(),
                    node_properties: None,
                });
            } else if file_type.is_dir() {
                let child_digest = build_subtree(&path, hasher, bundle).await?;
                subdirs.push(DirectoryNode {
                    name,
                    digest: Some(child_digest.into()),
                });
            } else if file_type.is_file() {
                let metadata = entry.metadata().await.map_err(|e| {
                    nativelink_error::make_err!(
                        nativelink_error::Code::Unavailable,
                        "Plan M: metadata({}) failed: {e}",
                        path.display(),
                    )
                })?;
                let file = fs::open_file(&path, 0, u64::MAX).await.map_err(|e| {
                    nativelink_error::make_err!(
                        nativelink_error::Code::Unavailable,
                        "Plan M: open_file({}) failed: {e}",
                        path.display(),
                    )
                })?;
                let mut h = hasher.hasher();
                let file_digest = h.compute_from_reader(file).await.map_err(|e| {
                    nativelink_error::make_err!(
                        nativelink_error::Code::Internal,
                        "Plan M: hash file {} failed: {e}",
                        path.display(),
                    )
                })?;
                bundle
                    .verified_files
                    .insert(path.clone(), file_digest);
                let is_executable = is_executable_mode(&metadata);
                files.push(FileNode {
                    name,
                    digest: Some(file_digest.into()),
                    is_executable,
                    node_properties: None,
                });
            }
            // Other file types (block/char devices, sockets, fifos)
            // are not representable in REAPI Directory; ignore them.
            // If the original proto somehow referenced one, the
            // resulting bytes will mismatch and the caller falls
            // through to CAS.
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
        bundle.protos.insert(digest, proto);
        // Record (path, digest) for this directory so callers can
        // bulk-mark Plan L on synthesis Hit. Used by
        // `prepare_action_inputs` to short-circuit subsequent actions
        // at the Plan L check.
        bundle.dir_paths.push((dir.to_path_buf(), digest));
        Ok(digest)
    })
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
    use nativelink_macro::nativelink_test;

    fn temp_root(label: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!(
            "nl-plan-m-{}-{}-{}",
            label,
            std::process::id(),
            rand::random::<u64>(),
        ));
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    /// Helper: synthesize once with a deliberately-wrong expected
    /// digest to extract the computed root digest, then call again
    /// with the actual digest to assert `Hit`. Returns the bundle.
    async fn synth_to_hit(root: &Path) -> SynthBundle {
        let probe = synthesize_directory_tree(
            root,
            &DigestInfo::new([0u8; 32], 0),
            DigestHasherFunc::Sha256,
        )
        .await;
        let computed = match probe {
            SynthResult::MissDigestMismatch { computed } => computed,
            SynthResult::Hit(_) => panic!(
                "first call against zero digest unexpectedly Hit — empty tree?",
            ),
            SynthResult::MissIoError(e) => panic!("unexpected MissIoError: {e:?}"),
        };
        let result =
            synthesize_directory_tree(root, &computed, DigestHasherFunc::Sha256).await;
        match result {
            SynthResult::Hit(bundle) => bundle,
            other => panic!("expected Hit on second call, got {other:?}"),
        }
    }

    /// Slice 2 (Plan M): walking a single-file tree must produce a
    /// `Hit` on a self-consistent re-walk and the file's hint path
    /// must show up in `verified_files` so Plan I can elide its own
    /// re-hash.
    #[nativelink_test]
    async fn synthesis_returns_hit_for_matching_local_tree() {
        let root = temp_root("hit_simple");
        tokio::fs::write(root.join("a.txt"), b"hello plan m").await.unwrap();

        let bundle = synth_to_hit(&root).await;
        assert!(
            !bundle.protos.is_empty(),
            "bundle must contain at least the root proto",
        );
        let file_path = root.join("a.txt");
        assert!(
            bundle.verified_files.contains_key(&file_path),
            "verified_files must include every file Plan I might want to hardlink",
        );
    }

    /// Slice 2 (Plan M): mismatched expected digest must surface as
    /// `MissDigestMismatch { computed }` so callers can route the
    /// metric to the digest-drift bucket and have the actual computed
    /// digest available for trace logs.
    #[nativelink_test]
    async fn synthesis_returns_miss_digest_mismatch_when_expected_wrong() {
        let root = temp_root("mismatch");
        tokio::fs::write(root.join("b.txt"), b"some content").await.unwrap();

        let result = synthesize_directory_tree(
            &root,
            &DigestInfo::new([0xFFu8; 32], 1),
            DigestHasherFunc::Sha256,
        )
        .await;
        match result {
            SynthResult::MissDigestMismatch { computed } => {
                assert_ne!(
                    computed,
                    DigestInfo::new([0xFFu8; 32], 1),
                    "computed must reflect the actual local-tree digest, not echo expected",
                );
            }
            other => panic!("expected MissDigestMismatch, got {other:?}"),
        }
    }

    /// Slice 2 (Plan M): a missing hint directory is the most common
    /// real-world miss reason (worker without a pre-staged tree).
    /// Must surface as `MissIoError`, NOT panic.
    #[nativelink_test]
    async fn synthesis_returns_miss_io_error_when_hint_dir_missing() {
        let mut p = std::env::temp_dir();
        p.push(format!(
            "nl-plan-m-missing-{}-{}",
            std::process::id(),
            rand::random::<u64>(),
        ));
        // Intentionally not created.
        let result = synthesize_directory_tree(
            &p,
            &DigestInfo::new([0u8; 32], 0),
            DigestHasherFunc::Sha256,
        )
        .await;
        match result {
            SynthResult::MissIoError(_) => {}
            other => panic!("expected MissIoError, got {other:?}"),
        }
    }

    /// Slice 2 (Plan M): bundle.protos must contain a proto for
    /// every visited subdirectory, not just the root. This is the
    /// invariant `download_to_directory` relies on to skip CAS at
    /// every recursive level.
    #[nativelink_test]
    async fn synthesis_collects_protos_for_every_subdirectory() {
        let root = temp_root("recursive");
        tokio::fs::create_dir_all(root.join("sub1/sub2")).await.unwrap();
        tokio::fs::write(root.join("top.txt"), b"top").await.unwrap();
        tokio::fs::write(root.join("sub1/mid.txt"), b"mid").await.unwrap();
        tokio::fs::write(root.join("sub1/sub2/leaf.txt"), b"leaf")
            .await
            .unwrap();

        let bundle = synth_to_hit(&root).await;
        // root + sub1 + sub2 = 3 distinct Directory protos.
        assert_eq!(
            bundle.protos.len(),
            3,
            "bundle.protos must include every visited directory",
        );
        assert_eq!(
            bundle.verified_files.len(),
            3,
            "verified_files must include every regular file",
        );
    }

    /// Slice 2 (Plan M): an empty directory still yields a Directory
    /// proto with deterministic encoding (the canonical empty-Directory
    /// digest). Confirms the synth path doesn't choke on leaf dirs
    /// with no children.
    #[nativelink_test]
    async fn synthesis_handles_empty_directory() {
        let root = temp_root("empty");
        let bundle = synth_to_hit(&root).await;
        assert_eq!(bundle.protos.len(), 1, "empty dir → one proto, the root");
        assert!(bundle.verified_files.is_empty());
    }

    /// 1.3.2 slice (Plan L bulk-mark): SynthBundle.dir_paths must
    /// contain a `(absolute_path, computed_digest)` entry for every
    /// directory the walker visited, including the root. This is the
    /// bridge that lets `prepare_action_inputs` mark Plan L for every
    /// visited subtree on synthesis Hit, so subsequent actions hit
    /// Plan L at the root and never re-walk.
    #[nativelink_test]
    async fn synthesis_bundle_records_dir_paths_for_plan_l_marking() {
        let root = temp_root("dir_paths");
        tokio::fs::create_dir_all(root.join("alpha/beta")).await.unwrap();
        tokio::fs::write(root.join("top.txt"), b"top").await.unwrap();
        tokio::fs::write(root.join("alpha/mid.txt"), b"mid").await.unwrap();
        tokio::fs::write(root.join("alpha/beta/leaf.txt"), b"leaf").await.unwrap();

        let bundle = synth_to_hit(&root).await;
        assert_eq!(
            bundle.dir_paths.len(),
            3,
            "dir_paths must include root + alpha + alpha/beta",
        );
        // Every entry's path must exist on disk and its digest must
        // be present in bundle.protos (otherwise we'd be marking
        // Plan L for a digest we never produced).
        for (path, digest) in &bundle.dir_paths {
            assert!(path.exists(), "dir_paths entry must reference a real dir: {path:?}");
            assert!(
                bundle.protos.contains_key(digest),
                "dir_paths digest must also appear in protos map: {digest:?}",
            );
        }
        // The root path itself must be among them (the action-time
        // Plan L lookup at the top of `download_to_directory` uses
        // exactly this path).
        let root_canonical = root.canonicalize().unwrap_or_else(|_| root.clone());
        assert!(
            bundle.dir_paths.iter().any(|(p, _)| p.canonicalize()
                .ok()
                .as_ref() == Some(&root_canonical)),
            "root path must appear in dir_paths so action's Plan L lookup hits",
        );
    }
}
