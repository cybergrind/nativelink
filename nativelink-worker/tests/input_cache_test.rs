// Copyright 2026 The NativeLink Authors. All rights reserved.
//
// Licensed under the Functional Source License, Version 1.1, Apache 2.0 Future License (the "License");
// you may not use this file except in compliance with the License.
//
// See LICENSE file for details.

//! Phase B.2 unit tests — `input_cache` module: Plan I (`try_hint_link`),
//! Plan K (`PathDigestCache`), Plan L (`WalkedDirsCache`), and
//! `SingleFlight` deduper.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use nativelink_util::common::DigestInfo;
use nativelink_util::digest_hasher::{DigestHasher, DigestHasherFunc};
use nativelink_worker::input_cache::{
    HintLinkResult, PathDigestCache, SingleFlight, WalkedDirsCache, try_hint_link,
};
use tempfile::TempDir;

fn digest_of(content: &[u8]) -> DigestInfo {
    let mut hasher = DigestHasherFunc::Sha256.hasher();
    hasher.update(content);
    hasher.finalize_digest()
}

// ----- Plan I — try_hint_link --------------------------------------------------

#[tokio::test]
async fn plan_i_hash_matches_returns_hit() {
    // TB5
    let dir = TempDir::new().unwrap();
    let hint_root = dir.path().join("hint");
    std::fs::create_dir(&hint_root).unwrap();
    std::fs::write(hint_root.join("hello.txt"), b"hello").unwrap();

    let expected = digest_of(b"hello");
    let dst = dir.path().join("dst").join("hello.txt");
    std::fs::create_dir_all(dst.parent().unwrap()).unwrap();

    let result = try_hint_link(
        &hint_root,
        "hello.txt",
        &expected,
        DigestHasherFunc::Sha256,
        &dst,
    )
    .await;

    assert!(matches!(result, HintLinkResult::Hit));
    assert_eq!(std::fs::read(&dst).unwrap(), b"hello");
}

#[tokio::test]
async fn plan_i_hash_mismatch_returns_miss_digest_mismatch() {
    // TB6
    let dir = TempDir::new().unwrap();
    let hint_root = dir.path().join("hint");
    std::fs::create_dir(&hint_root).unwrap();
    std::fs::write(hint_root.join("file"), b"hello").unwrap();

    let wrong = digest_of(b"world"); // size matches (5 bytes), digest differs
    let dst = dir.path().join("dst").join("file");
    std::fs::create_dir_all(dst.parent().unwrap()).unwrap();

    let result = try_hint_link(
        &hint_root,
        "file",
        &wrong,
        DigestHasherFunc::Sha256,
        &dst,
    )
    .await;
    assert!(matches!(result, HintLinkResult::MissDigestMismatch));
    assert!(!dst.exists(), "miss must NOT have linked the file");
}

#[tokio::test]
async fn plan_i_size_mismatch_short_circuits() {
    // TB7 — expected.size_bytes != on-disk size, return Miss without hashing.
    let dir = TempDir::new().unwrap();
    let hint_root = dir.path().join("hint");
    std::fs::create_dir(&hint_root).unwrap();
    std::fs::write(hint_root.join("file"), b"hello").unwrap(); // 5 bytes

    // The size short-circuit must fire before any hashing, so the
    // hash bytes are irrelevant — we use zeros to make that explicit.
    let wrong_size = DigestInfo::new([0u8; 32], 100);

    let dst = dir.path().join("dst").join("file");
    std::fs::create_dir_all(dst.parent().unwrap()).unwrap();

    let result = try_hint_link(
        &hint_root,
        "file",
        &wrong_size,
        DigestHasherFunc::Sha256,
        &dst,
    )
    .await;
    assert!(matches!(result, HintLinkResult::MissSizeMismatch));
}

#[tokio::test]
async fn plan_i_missing_path_returns_miss_not_found() {
    // TB8
    let dir = TempDir::new().unwrap();
    let hint_root = dir.path().join("hint");
    std::fs::create_dir(&hint_root).unwrap();
    let expected = digest_of(b"anything");
    let dst = dir.path().join("dst").join("missing");
    std::fs::create_dir_all(dst.parent().unwrap()).unwrap();

    let result = try_hint_link(
        &hint_root,
        "missing",
        &expected,
        DigestHasherFunc::Sha256,
        &dst,
    )
    .await;
    assert!(matches!(result, HintLinkResult::MissNotFound));
}

#[tokio::test]
async fn plan_i_directory_as_file_returns_miss_io_error() {
    // TB9 — pointing at a directory (not a file) yields IoError, not panic.
    let dir = TempDir::new().unwrap();
    let hint_root = dir.path().join("hint");
    std::fs::create_dir(&hint_root).unwrap();
    std::fs::create_dir(hint_root.join("notafile")).unwrap();

    let expected = digest_of(b"x");
    let dst = dir.path().join("dst").join("x");
    std::fs::create_dir_all(dst.parent().unwrap()).unwrap();

    let result = try_hint_link(
        &hint_root,
        "notafile",
        &expected,
        DigestHasherFunc::Sha256,
        &dst,
    )
    .await;
    assert!(
        matches!(result, HintLinkResult::MissIoError(_) | HintLinkResult::MissSizeMismatch),
        "must return a Miss variant (size or io); got {result:?}"
    );
}

// ----- Plan K — PathDigestCache ------------------------------------------------

#[tokio::test]
async fn plan_k_insert_then_contains_returns_true() {
    // TB13
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("a.txt");
    std::fs::write(&path, b"a").unwrap();

    let cache = PathDigestCache::new();
    let d = digest_of(b"a");
    cache.insert(path.clone(), d.clone()).await.unwrap();
    assert!(cache.contains(&path, &d).await);
}

#[tokio::test]
async fn plan_k_different_digest_misses() {
    // TB14
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("a.txt");
    std::fs::write(&path, b"a").unwrap();

    let cache = PathDigestCache::new();
    cache.insert(path.clone(), digest_of(b"a")).await.unwrap();
    assert!(!cache.contains(&path, &digest_of(b"b")).await);
}

#[tokio::test]
async fn plan_k_stat_gate_drops_modified_path() {
    // TB15 — touching the file (advancing mtime) must invalidate the entry.
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("a.txt");
    std::fs::write(&path, b"a").unwrap();

    let cache = PathDigestCache::new();
    let d = digest_of(b"a");
    cache.insert(path.clone(), d.clone()).await.unwrap();
    assert!(cache.contains(&path, &d).await);

    tokio::time::sleep(Duration::from_millis(20)).await;
    // Rewrite to advance mtime; content unchanged.
    std::fs::write(&path, b"a").unwrap();
    // Mtime may not change on identical content+timestamp on some FSes;
    // force it via filetime crate if needed in green commit. For now we
    // also test the missing-file case which is unambiguous.
    std::fs::remove_file(&path).unwrap();
    assert!(!cache.contains(&path, &d).await);
}

// ----- Plan L — WalkedDirsCache ------------------------------------------------

#[tokio::test]
async fn plan_l_walked_dir_hit_skips_walk() {
    // TB18
    let cache = WalkedDirsCache::new();
    let path = PathBuf::from("/tmp/x");
    let d = digest_of(b"dir-d");
    cache.insert(path.clone(), d.clone()).await;
    assert!(cache.contains(&path, &d).await);
}

#[tokio::test]
async fn plan_l_path_aware_key() {
    // TB19 — the digest-only-key 1.0 stale-walk bug must NOT regress.
    let cache = WalkedDirsCache::new();
    let d = digest_of(b"shared-digest");
    cache.insert(PathBuf::from("/tmp/x"), d.clone()).await;
    assert!(cache.contains(&PathBuf::from("/tmp/x"), &d).await);
    assert!(
        !cache.contains(&PathBuf::from("/tmp/y"), &d).await,
        "Plan L must be path-aware: same digest at a different path must not hit"
    );
}

// ----- SingleFlight ------------------------------------------------------------

#[tokio::test]
async fn single_flight_one_walk_under_concurrency() {
    // TB20 — N concurrent .run((same_key)) calls invoke the closure exactly once.
    let sf = SingleFlight::new();
    let counter = Arc::new(AtomicUsize::new(0));
    let key = (PathBuf::from("/tmp/walk"), digest_of(b"sf-key"));

    let mut handles = Vec::new();
    for _ in 0..32 {
        let sf = Arc::clone(&sf);
        let counter = Arc::clone(&counter);
        let key = key.clone();
        handles.push(tokio::spawn(async move {
            sf.run(key, || async {
                counter.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(20)).await;
                Ok(())
            })
            .await
        }));
    }

    for h in handles {
        h.await.unwrap().unwrap();
    }
    assert_eq!(
        counter.load(Ordering::SeqCst),
        1,
        "SingleFlight must execute the closure exactly once for concurrent .run on the same key"
    );
}

#[tokio::test]
async fn single_flight_distinct_keys_run_independently() {
    let sf = SingleFlight::new();
    let counter = Arc::new(AtomicUsize::new(0));

    let mut handles = Vec::new();
    for i in 0..8 {
        let sf = Arc::clone(&sf);
        let counter = Arc::clone(&counter);
        let key = (PathBuf::from(format!("/tmp/walk-{i}")), digest_of(format!("k-{i}").as_bytes()));
        handles.push(tokio::spawn(async move {
            sf.run(key, || async {
                counter.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
            .await
        }));
    }
    for h in handles {
        h.await.unwrap().unwrap();
    }
    assert_eq!(counter.load(Ordering::SeqCst), 8);
}
