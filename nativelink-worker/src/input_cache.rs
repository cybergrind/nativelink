// Copyright 2026 The NativeLink Authors. All rights reserved.
//
// Licensed under the Functional Source License, Version 1.1, Apache 2.0 Future License (the "License");
// you may not use this file except in compliance with the License.
//
// See LICENSE file for details.

//! In-memory caches for input materialization: Plan I (digest-checked
//! hint link), Plan K (path→digest cache), Plan L (walked-dir cache),
//! and a single-flight wrapper for deduping concurrent walks of the
//! same `(path, digest)` subtree.
//!
//! Skeleton — all bodies are `unimplemented!()` until the green commit.

use core::future::Future;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::SystemTime;

use nativelink_error::Error;
use nativelink_util::common::DigestInfo;
use nativelink_util::digest_hasher::DigestHasherFunc;
use parking_lot::Mutex;
use tokio::sync::{RwLock, broadcast};

// ---------------------------------------------------------------------------
// Plan K — path → digest cache, in-memory, stat-gated.
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
pub struct PathDigestCache {
    inner: RwLock<HashMap<PathBuf, (DigestInfo, SystemTime)>>,
}

impl PathDigestCache {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Stat-gated membership: returns true only when the cached
    /// `(path → (digest, mtime))` entry exists, the on-disk mtime
    /// matches the recorded one, and the recorded digest equals
    /// `digest`. If the on-disk file is missing or its mtime has
    /// drifted, the stale entry is evicted and `false` is returned.
    pub async fn contains(&self, _path: &Path, _digest: &DigestInfo) -> bool {
        unimplemented!("Phase B.2 green: PathDigestCache::contains")
    }

    /// Record `path → digest` and remember the on-disk mtime at the
    /// moment of insertion. Returns `Err` if the file cannot be stat'd.
    pub async fn insert(
        &self,
        _path: PathBuf,
        _digest: DigestInfo,
    ) -> Result<(), Error> {
        unimplemented!("Phase B.2 green: PathDigestCache::insert")
    }

    /// Test-only: number of entries currently held.
    #[doc(hidden)]
    pub fn len_for_test(&self) -> usize {
        // Always returns 0 in the red skeleton; green flips it.
        0
    }
}

// ---------------------------------------------------------------------------
// Plan L — walked-directory cache, in-memory, path-aware.
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
pub struct WalkedDirsCache {
    inner: RwLock<HashSet<(PathBuf, DigestInfo)>>,
}

impl WalkedDirsCache {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub async fn contains(&self, _path: &Path, _digest: &DigestInfo) -> bool {
        unimplemented!("Phase B.2 green: WalkedDirsCache::contains")
    }

    pub async fn insert(&self, _path: PathBuf, _digest: DigestInfo) {
        unimplemented!("Phase B.2 green: WalkedDirsCache::insert")
    }
}

// ---------------------------------------------------------------------------
// SingleFlight — exactly one execution of `f` per in-flight key.
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
pub struct SingleFlight {
    inner: Mutex<HashMap<(PathBuf, DigestInfo), broadcast::Sender<bool>>>,
}

impl SingleFlight {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Run `f` for `key` if no other caller is currently running it;
    /// otherwise wait for the in-flight caller to finish. On success
    /// (leader returned `Ok(())`) followers also return `Ok(())`. On
    /// leader failure followers retry by calling `f()` themselves —
    /// the cache in `f` is expected to short-circuit a successful
    /// retry.
    pub async fn run<F, Fut>(
        &self,
        _key: (PathBuf, DigestInfo),
        _f: F,
    ) -> Result<(), Error>
    where
        F: FnOnce() -> Fut + Send,
        Fut: Future<Output = Result<(), Error>> + Send,
    {
        unimplemented!("Phase B.2 green: SingleFlight::run")
    }
}

// ---------------------------------------------------------------------------
// Plan I — digest-checked hint-link.
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub enum HintLinkResult {
    Hit,
    MissNotFound,
    MissSizeMismatch,
    MissDigestMismatch,
    MissIoError(Error),
}

/// Try to satisfy `expected_digest` for `dst` by hashing
/// `hint_root/<file_name>` and, on match, hardlink-or-clonefile from
/// hint_root into `dst`. Caller is responsible for marking Plan K
/// after a `Hit`. The size short-circuit avoids hashing files that
/// can't possibly match.
pub async fn try_hint_link(
    _hint_root: &Path,
    _file_name: &str,
    _expected_digest: &DigestInfo,
    _hasher_func: DigestHasherFunc,
    _dst: &Path,
) -> HintLinkResult {
    unimplemented!("Phase B.2 green: try_hint_link")
}

// ---------------------------------------------------------------------------
// InputCache — bundle handed to `download_to_directory` by Phase B.3.
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct InputCache {
    pub path_digests: Arc<PathDigestCache>,
    pub walked_dirs: Arc<WalkedDirsCache>,
    pub walk_singleflight: Arc<SingleFlight>,
    pub hint_root: Option<PathBuf>,
    pub plan_i_enabled: bool,
}

impl InputCache {
    /// Process-shared default constructor. All workers in the same
    /// `nativelink` process share these maps; this is intentional —
    /// warming one worker warms its siblings.
    pub fn new_shared() -> Self {
        Self {
            path_digests: PathDigestCache::new(),
            walked_dirs: WalkedDirsCache::new(),
            walk_singleflight: SingleFlight::new(),
            hint_root: None,
            plan_i_enabled: true,
        }
    }
}
