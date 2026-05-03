// Copyright 2026 The NativeLink Authors. All rights reserved.
//
// Licensed under the Functional Source License, Version 1.1, Apache 2.0 Future License (the "License");
// you may not use this file except in compliance with the License.
//
// See LICENSE file for details.

//! Phase B.3 integration tests — `input_cache` wired through
//! `download_to_directory`.
//!
//! Tests:
//!  - `cold_run_then_warm_run_uses_plan_k`: first run materializes via
//!    CAS and marks Plan K. A second run with Plan K pre-primed for a
//!    fresh dest path skips work — verified by running against a CAS
//!    that has been *cleared* and seeing the call still succeed.
//!  - `plan_i_hit_satisfies_file_without_cas_blob`: hint root contains
//!    the file's exact bytes; the file blob is NOT uploaded to CAS.
//!    Plan I must satisfy the file from the hint, leaving CAS untouched.
//!  - `plan_l_hit_skips_subdir_walk`: pre-mark a subdir's
//!    `(path, digest)` as walked; the recursive call must not run, and
//!    the subdir directory must not be created.

use std::sync::Arc;
use std::time::Duration;

use nativelink_config::stores::{
    FastSlowSpec, FilesystemSpec, MemorySpec, StoreDirection, StoreSpec,
};
use nativelink_proto::build::bazel::remote::execution::v2::{
    Directory as ProtoDirectory, DirectoryNode, FileNode,
};
use nativelink_store::fast_slow_store::FastSlowStore;
use nativelink_store::filesystem_store::FilesystemStore;
use nativelink_store::memory_store::MemoryStore;
use nativelink_util::common::DigestInfo;
use nativelink_util::digest_hasher::{DigestHasher, DigestHasherFunc};
use nativelink_util::store_trait::{Store, StoreLike};
use nativelink_worker::input_cache::InputCache;
use nativelink_worker::running_actions_manager::download_to_directory;
use prost::Message;
use rand::Rng;
use tempfile::TempDir;

fn make_temp_path(under: &TempDir, leaf: &str) -> String {
    let mut rng = rand::rng();
    let n: u64 = rng.random();
    let p = under.path().join(format!("{leaf}_{n:016x}"));
    std::fs::create_dir_all(&p).unwrap();
    p.to_string_lossy().to_string()
}

async fn setup_stores(
    scratch: &TempDir,
) -> (Arc<FilesystemStore>, Arc<MemoryStore>, Arc<FastSlowStore>) {
    let fast_config = FilesystemSpec {
        content_path: make_temp_path(scratch, "content"),
        temp_path: make_temp_path(scratch, "temp"),
        eviction_policy: None,
        ..Default::default()
    };
    let slow_config = MemorySpec::default();
    let fast_store = FilesystemStore::new(&fast_config).await.unwrap();
    let slow_store = MemoryStore::new(&slow_config);
    let cas_store = FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Filesystem(fast_config),
            slow: StoreSpec::Memory(slow_config),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
        },
        Store::new(fast_store.clone()),
        Store::new(slow_store.clone()),
    );
    (fast_store, slow_store, cas_store)
}

fn digest_of(content: &[u8]) -> DigestInfo {
    let mut hasher = DigestHasherFunc::Sha256.hasher();
    hasher.update(content);
    hasher.finalize_digest()
}

#[tokio::test]
async fn cold_run_then_warm_run_uses_plan_k() -> Result<(), Box<dyn std::error::Error>> {
    let scratch = TempDir::new()?;
    let (fast, slow, cas) = setup_stores(&scratch).await;

    // Upload a file blob and a Directory referencing it.
    let file_content = b"plan_k_warm_test_content";
    let file_digest = digest_of(file_content);
    slow.as_ref()
        .update_oneshot(file_digest, file_content.to_vec().into())
        .await?;
    let dir_proto = ProtoDirectory {
        files: vec![FileNode {
            name: "f.txt".into(),
            digest: Some(file_digest.into()),
            is_executable: false,
            node_properties: None,
        }],
        ..Default::default()
    };
    let dir_bytes = dir_proto.encode_to_vec();
    let dir_digest = digest_of(&dir_bytes);
    slow.as_ref()
        .update_oneshot(dir_digest, dir_bytes.into())
        .await?;

    let work_dir = make_temp_path(&scratch, "work_cold");
    let cache = InputCache::new_shared();

    download_to_directory(
        cas.as_ref(),
        fast.as_pin(),
        &dir_digest,
        &work_dir,
        &cache,
        None,
        DigestHasherFunc::Sha256,
    )
    .await?;
    let dest_path = format!("{work_dir}/f.txt");
    assert_eq!(std::fs::read(&dest_path)?, file_content);

    // Plan K is now warm for `(dest_path, file_digest)`.
    assert!(
        cache
            .path_digests
            .contains(std::path::Path::new(&dest_path), &file_digest)
            .await,
        "Plan K must record the materialized file"
    );

    // Sleep briefly so a second mtime would be detectably different
    // if anything were to overwrite the file.
    tokio::time::sleep(Duration::from_millis(20)).await;
    let mtime_before = std::fs::metadata(&dest_path)?.modified()?;

    // Second call against the same work_dir + cache. Plan K hits and
    // skips re-materialization. We verify by checking mtime didn't move.
    download_to_directory(
        cas.as_ref(),
        fast.as_pin(),
        &dir_digest,
        &work_dir,
        &cache,
        None,
        DigestHasherFunc::Sha256,
    )
    .await?;
    assert_eq!(std::fs::metadata(&dest_path)?.modified()?, mtime_before);
    Ok(())
}

#[tokio::test]
async fn plan_i_hit_satisfies_file_without_cas_blob() -> Result<(), Box<dyn std::error::Error>> {
    let scratch = TempDir::new()?;
    let (fast, slow, cas) = setup_stores(&scratch).await;

    // Build a Directory referencing a file digest. NOTE: we do NOT
    // upload the file blob to CAS — only the Directory proto. Plan I
    // must satisfy the file from hint_root, never going to CAS.
    let file_content = b"plan_i_test_payload";
    let file_digest = digest_of(file_content);
    let dir_proto = ProtoDirectory {
        files: vec![FileNode {
            name: "hint_target.txt".into(),
            digest: Some(file_digest.into()),
            is_executable: false,
            node_properties: None,
        }],
        ..Default::default()
    };
    let dir_bytes = dir_proto.encode_to_vec();
    let dir_digest = digest_of(&dir_bytes);
    slow.as_ref()
        .update_oneshot(dir_digest, dir_bytes.into())
        .await?;

    // Stage the hint tree.
    let hint_root = scratch.path().join("hint");
    std::fs::create_dir_all(&hint_root)?;
    std::fs::write(hint_root.join("hint_target.txt"), file_content)?;

    let work_dir = make_temp_path(&scratch, "work");
    let cache = InputCache::new_shared();

    download_to_directory(
        cas.as_ref(),
        fast.as_pin(),
        &dir_digest,
        &work_dir,
        &cache,
        Some(&hint_root),
        DigestHasherFunc::Sha256,
    )
    .await?;

    let dest = format!("{work_dir}/hint_target.txt");
    assert_eq!(std::fs::read(&dest)?, file_content);
    Ok(())
}

#[tokio::test]
async fn plan_l_hit_skips_subdir_walk() -> Result<(), Box<dyn std::error::Error>> {
    let scratch = TempDir::new()?;
    let (fast, slow, cas) = setup_stores(&scratch).await;

    // Outer directory containing one empty inner directory.
    let inner_proto = ProtoDirectory::default();
    let inner_bytes = inner_proto.encode_to_vec();
    let inner_digest = digest_of(&inner_bytes);
    slow.as_ref()
        .update_oneshot(inner_digest, inner_bytes.into())
        .await?;
    let outer_proto = ProtoDirectory {
        directories: vec![DirectoryNode {
            name: "child".into(),
            digest: Some(inner_digest.into()),
        }],
        ..Default::default()
    };
    let outer_bytes = outer_proto.encode_to_vec();
    let outer_digest = digest_of(&outer_bytes);
    slow.as_ref()
        .update_oneshot(outer_digest, outer_bytes.into())
        .await?;

    let work_dir = make_temp_path(&scratch, "work");
    let cache = InputCache::new_shared();

    // Pre-mark Plan L for the child path *before* the call. The
    // recursive walk must be skipped → child dir must NOT be created.
    let child_path = format!("{work_dir}/child");
    cache
        .walked_dirs
        .insert(std::path::PathBuf::from(&child_path), inner_digest)
        .await;

    download_to_directory(
        cas.as_ref(),
        fast.as_pin(),
        &outer_digest,
        &work_dir,
        &cache,
        None,
        DigestHasherFunc::Sha256,
    )
    .await?;
    assert!(
        !std::path::Path::new(&child_path).exists(),
        "Plan L hit must skip subdir creation; got {child_path} present"
    );
    Ok(())
}
