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

use core::future::Future;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::SystemTime;

use nativelink_error::{Code, Error};
use nativelink_util::common::DigestInfo;
use nativelink_util::digest_hasher::{DigestHasher, DigestHasherFunc};
use nativelink_util::fs;
use parking_lot::Mutex as SyncMutex;
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

    /// Stat-gated: returns `true` only when an entry exists, the on-disk
    /// mtime equals the recorded mtime, and the recorded digest equals
    /// `digest`. On stale or missing files, the entry is evicted.
    pub async fn contains(&self, path: &Path, digest: &DigestInfo) -> bool {
        let recorded = {
            let guard = self.inner.read().await;
            guard.get(path).cloned()
        };
        let Some((rec_digest, rec_mtime)) = recorded else {
            return false;
        };
        if rec_digest != *digest {
            return false;
        }
        let on_disk_mtime = match tokio::fs::metadata(path).await {
            Ok(m) => m.modified().ok(),
            Err(_) => None,
        };
        match on_disk_mtime {
            Some(m) if m == rec_mtime => true,
            _ => {
                self.inner.write().await.remove(path);
                false
            }
        }
    }

    pub async fn insert(&self, path: PathBuf, digest: DigestInfo) -> Result<(), Error> {
        let mtime = tokio::fs::metadata(&path)
            .await
            .map_err(Error::from)?
            .modified()
            .map_err(Error::from)?;
        self.inner.write().await.insert(path, (digest, mtime));
        Ok(())
    }

    /// Test-only count of entries.
    #[doc(hidden)]
    pub async fn len_for_test(&self) -> usize {
        self.inner.read().await.len()
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

    pub async fn contains(&self, path: &Path, digest: &DigestInfo) -> bool {
        self.inner
            .read()
            .await
            .contains(&(path.to_path_buf(), digest.clone()))
    }

    pub async fn insert(&self, path: PathBuf, digest: DigestInfo) {
        self.inner.write().await.insert((path, digest));
    }
}

// ---------------------------------------------------------------------------
// SingleFlight — exactly one execution of `f` per in-flight key.
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
pub struct SingleFlight {
    inner: SyncMutex<HashMap<(PathBuf, DigestInfo), broadcast::Sender<bool>>>,
}

impl SingleFlight {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Run `f` for `key` if no other caller is currently running it;
    /// otherwise wait for the in-flight caller to finish. On leader
    /// success followers also return `Ok(())`. On leader failure
    /// followers retry by calling `f()` themselves — the cache inside
    /// `f` is expected to short-circuit the retry cheaply on a
    /// successful walk by some other path.
    pub async fn run<F, Fut>(&self, key: (PathBuf, DigestInfo), f: F) -> Result<(), Error>
    where
        F: FnOnce() -> Fut + Send,
        Fut: Future<Output = Result<(), Error>> + Send,
    {
        // Acquire/insert the entry under the sync mutex; decide leader
        // vs follower; release the mutex before awaiting anything.
        let role = {
            let mut guard = self.inner.lock();
            match guard.get(&key) {
                Some(tx) => Role::Follower(tx.subscribe()),
                None => {
                    let (tx, _rx) = broadcast::channel::<bool>(1);
                    guard.insert(key.clone(), tx);
                    Role::Leader
                }
            }
        };

        match role {
            Role::Leader => {
                let result = f().await;
                let success = result.is_ok();
                let tx_opt = self.inner.lock().remove(&key);
                if let Some(tx) = tx_opt {
                    let _ = tx.send(success);
                }
                result
            }
            Role::Follower(mut rx) => {
                // If the leader's broadcast channel closes (sender dropped
                // before sending), recv returns Err — treat as failure and
                // retry via `f`.
                match rx.recv().await {
                    Ok(true) => Ok(()),
                    Ok(false) | Err(_) => f().await,
                }
            }
        }
    }
}

enum Role {
    Leader,
    Follower(broadcast::Receiver<bool>),
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
/// `hint_root/<file_name>`. On match, hardlink-or-clonefile from the
/// hint into `dst`. The size short-circuit avoids hashing files that
/// can't possibly match. Caller is responsible for marking Plan K
/// after a `Hit`.
///
/// CALLER CONTRACT: `hint_root` MUST NOT resolve so that
/// `hint_root/<file_name>` equals `dst`. If it does, `fs::hard_link`
/// returns EEXIST, this function returns `MissIoError`, and the caller
/// falls through to the CAS path — which then ALSO returns EEXIST when
/// it tries to hardlink onto the existing dest. For Plan J's
/// "shared tree as canonical destination" use case, callers must pass
/// `hint_root: None` and rely on `try_existing_dest_link` for the
/// stat-and-hash short-circuit. See CLAUDE.md hard rule #4.
pub async fn try_hint_link(
    hint_root: &Path,
    file_name: &str,
    expected_digest: &DigestInfo,
    hasher_func: DigestHasherFunc,
    dst: &Path,
) -> HintLinkResult {
    let hint_path = hint_root.join(file_name);

    // Stat for existence + size; short-circuit on mismatch.
    let meta = match tokio::fs::metadata(&hint_path).await {
        Ok(m) => m,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return HintLinkResult::MissNotFound;
        }
        Err(e) => return HintLinkResult::MissIoError(Error::from(e)),
    };
    if !meta.is_file() {
        // Directory or special file at the hint path.
        return HintLinkResult::MissIoError(nativelink_error::make_err!(
            nativelink_error::Code::InvalidArgument,
            "hint path is not a regular file: {}",
            hint_path.display()
        ));
    }
    if meta.len() != expected_digest.size_bytes() {
        return HintLinkResult::MissSizeMismatch;
    }

    // Hash the file. compute_from_reader pulls the whole file through.
    let mut hasher = hasher_func.hasher();
    let mut file = match tokio::fs::File::open(&hint_path).await {
        Ok(f) => f,
        Err(e) => return HintLinkResult::MissIoError(Error::from(e)),
    };
    let computed = match hasher.compute_from_reader(&mut file).await {
        Ok(d) => d,
        Err(e) => return HintLinkResult::MissIoError(e),
    };
    if computed != *expected_digest {
        return HintLinkResult::MissDigestMismatch;
    }

    // Verified match — hardlink (or clonefile on macOS via fs::hard_link).
    if let Err(e) = fs::hard_link(&hint_path, dst).await {
        return HintLinkResult::MissIoError(e);
    }
    HintLinkResult::Hit
}

/// Cold-cache short-circuit for shared-tree mode: returns `Hit` when
/// `dst` itself already exists with size + digest matching
/// `expected_digest` — no link is performed because `dst` is already
/// the right file. Caller is responsible for replacing `dst` on Miss
/// variants (typical pattern: unlink then hardlink from the filesystem
/// store entry).
///
/// Distinct from `try_hint_link` which dereferences a separate hint
/// tree; here the hint and destination are the same path. Used by the
/// shared-tree input walk where `prepare_action_inputs` is called with
/// `hint_root: None` and the canonical input destinations live inside
/// the shared `src/` checkout.
pub async fn try_existing_dest_link(
    dst: &Path,
    expected_digest: &DigestInfo,
    hasher_func: DigestHasherFunc,
) -> HintLinkResult {
    let meta = match tokio::fs::metadata(dst).await {
        Ok(m) => m,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return HintLinkResult::MissNotFound;
        }
        Err(e) => return HintLinkResult::MissIoError(Error::from(e)),
    };
    if !meta.is_file() {
        return HintLinkResult::MissIoError(nativelink_error::make_err!(
            nativelink_error::Code::InvalidArgument,
            "dest path is not a regular file: {}",
            dst.display()
        ));
    }
    if meta.len() != expected_digest.size_bytes() {
        return HintLinkResult::MissSizeMismatch;
    }

    let mut hasher = hasher_func.hasher();
    let mut file = match tokio::fs::File::open(dst).await {
        Ok(f) => f,
        Err(e) => return HintLinkResult::MissIoError(Error::from(e)),
    };
    let computed = match hasher.compute_from_reader(&mut file).await {
        Ok(d) => d,
        Err(e) => return HintLinkResult::MissIoError(e),
    };
    if computed != *expected_digest {
        return HintLinkResult::MissDigestMismatch;
    }

    HintLinkResult::Hit
}

/// Idempotent hardlink: link `src` → `dst`. The shared-tree-as-cache
/// model means the canonical destination may already hold matching
/// content (a concurrent walk-future arriving at the same on-disk inode
/// via a symlink-resolved duplicate path) or stale content (a prior
/// action), and the older "remove-then-link" pattern fails EEXIST when:
///   1. Two walk-futures within one action race on the same inode via
///      symlink chains (common in macOS framework input trees).
///   2. `dst` is a directory: `remove_file` returns EISDIR (silenced
///      via `.ok()`), then `hard_link` returns EEXIST.
///   3. `dst`'s removal is denied (immutable flag, EACCES); silenced.
///
/// This helper:
///   - Tries `hard_link` first.
///   - On `Ok`, returns.
///   - On `AlreadyExists`, verifies `dst`'s digest via
///     `try_existing_dest_link`. A `Hit` is accepted (race winner has
///     the right content). Anything else triggers a single
///     unlink+retry attempt; `remove_dir_all` covers the EISDIR case.
///   - Any other error from the initial link is returned unchanged so
///     the caller can attribute Code::NotFound to filesystem-store
///     eviction (existing call site behavior).
pub async fn idempotent_hard_link(
    src: &Path,
    dst: &Path,
    expected_digest: &DigestInfo,
    hasher_func: DigestHasherFunc,
) -> Result<(), Error> {
    match fs::hard_link(src, dst).await {
        Ok(()) => Ok(()),
        Err(e) if e.code == Code::AlreadyExists => {
            match try_existing_dest_link(dst, expected_digest, hasher_func).await {
                HintLinkResult::Hit => Ok(()),
                _ => {
                    fs::remove_file(dst).await.ok();
                    tokio::fs::remove_dir_all(dst).await.ok();
                    fs::hard_link(src, dst).await
                }
            }
        }
        Err(e) => Err(e),
    }
}

// ---------------------------------------------------------------------------
// InputCache — process-shared state handed to `download_to_directory`.
// Per-action data (hint_root, hasher_func) flows as separate parameters.
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct InputCache {
    pub path_digests: Arc<PathDigestCache>,
    pub walked_dirs: Arc<WalkedDirsCache>,
    pub walk_singleflight: Arc<SingleFlight>,
    pub plan_i_enabled: bool,
}

impl InputCache {
    pub fn new_shared() -> Arc<Self> {
        Arc::new(Self {
            path_digests: PathDigestCache::new(),
            walked_dirs: WalkedDirsCache::new(),
            walk_singleflight: SingleFlight::new(),
            plan_i_enabled: true,
        })
    }

    pub fn with_plan_i_enabled(plan_i_enabled: bool) -> Arc<Self> {
        Arc::new(Self {
            path_digests: PathDigestCache::new(),
            walked_dirs: WalkedDirsCache::new(),
            walk_singleflight: SingleFlight::new(),
            plan_i_enabled,
        })
    }
}
