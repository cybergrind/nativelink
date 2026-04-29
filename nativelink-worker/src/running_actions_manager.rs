// Copyright 2024 The NativeLink Authors. All rights reserved.
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

use core::cmp::min;
use core::convert::Into;
use core::fmt::Debug;
use core::pin::Pin;
use core::sync::atomic::{AtomicBool, Ordering};
use core::time::Duration;
use std::borrow::Cow;
use std::collections::vec_deque::VecDeque;
use std::collections::{HashMap, HashSet};
use std::env;
use std::ffi::{OsStr, OsString};
#[cfg(target_family = "unix")]
use std::fs::Permissions;
#[cfg(target_family = "unix")]
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, Weak};
use std::time::SystemTime;

use bytes::{Bytes, BytesMut};
use filetime::{FileTime, set_file_mtime};
use formatx::Template;
use futures::future::{
    BoxFuture, Future, FutureExt, TryFutureExt, try_join, try_join_all, try_join3,
};
use futures::stream::{FuturesUnordered, StreamExt, TryStreamExt};
use nativelink_config::cas_server::{
    EnvironmentSource, UploadActionResultConfig, UploadCacheResultsStrategy,
};
use nativelink_error::{Code, Error, ResultExt, make_err, make_input_err};
use nativelink_metric::MetricsComponent;
use nativelink_proto::build::bazel::remote::execution::v2::{
    Action, ActionResult as ProtoActionResult, Command as ProtoCommand,
    Directory as ProtoDirectory, Directory, DirectoryNode, ExecuteResponse, FileNode, SymlinkNode,
    Tree as ProtoTree, UpdateActionResultRequest,
};
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{
    HistoricalExecuteResponse, StartExecute,
};
use nativelink_store::ac_utils::{
    ESTIMATED_DIGEST_SIZE, compute_buf_digest, get_and_decode_digest, serialize_and_upload_message,
};
use nativelink_store::cas_utils::is_zero_digest;
use nativelink_store::fast_slow_store::FastSlowStore;
use nativelink_store::filesystem_store::{FileEntry, FilesystemStore};
use nativelink_store::grpc_store::GrpcStore;
use nativelink_util::action_messages::{
    ActionInfo, ActionResult, DirectoryInfo, ExecutionMetadata, FileInfo, NameOrPath, OperationId,
    SymlinkInfo, to_execute_response,
};
use nativelink_util::common::{DigestInfo, fs};
use nativelink_util::digest_hasher::{DigestHasher, DigestHasherFunc};
use nativelink_util::metrics_utils::{AsyncCounterWrapper, CounterWithTime};
use nativelink_util::store_trait::{Store, StoreLike, UploadSizeInfo};
use nativelink_util::timing::StageStats;
use nativelink_util::{background_spawn, spawn, spawn_blocking};

// Timing harness: stages exposed via `nativelink_util::timing::dump_to_tracing()`.
// Instrumented from the hottest paths in `prepare_action_inputs` →
// `download_to_directory` so the dump surfaces which part of input
// materialization dominates per-action latency (Directory proto fetch
// vs. CAS content fetch vs. hardlink vs. cache lookup).
static PREPARE_INPUTS: StageStats = StageStats::new("worker.prepare_action_inputs");
static DIR_WALK: StageStats = StageStats::new("worker.download_to_directory.subtree");
static DIR_PROTO_FETCH: StageStats =
    StageStats::new("worker.download_to_directory.directory_proto_fetch");
static CAS_POPULATE: StageStats = StageStats::new("worker.cas.populate_fast_store");
// Despite the name: on macOS this is APFS clonefile() via
// nativelink_util::fs::hard_link, which tries clonefile first and only
// falls back to hard_link on cross-volume / unsupported errors.
static HARD_LINK: StageStats = StageStats::new("worker.fs.clonefile_or_link");
static PLAN_K_HIT: StageStats = StageStats::new("worker.plan_k.hit");
static PLAN_K_MISS: StageStats = StageStats::new("worker.plan_k.miss");
static PLAN_L_HIT: StageStats = StageStats::new("worker.plan_l.hit");
static PLAN_L_MISS: StageStats = StageStats::new("worker.plan_l.miss");
static PLAN_I_HIT: StageStats = StageStats::new("worker.plan_i.hit");
static PLAN_I_MISS: StageStats = StageStats::new("worker.plan_i.miss");
// Plan M counters retired in 1.3.4: the upper-level whole-tree synth
// they instrumented was removed entirely. Plan M's synth function
// stays in the tree as a primitive (see `local_dir_synthesis`) but
// it's no longer invoked from the action path.
/// Wall-clock cost of one Plan I `file_matches_digest` call (one
/// per-file streaming SHA-256/Blake3 of an on-disk hint file). On a
/// cold Plan K cache with many concurrent actions sharing the same
/// hint tree, this is where the worker actually spends its time —
/// without this counter, the storm is invisible in the timing dump.
static PLAN_I_FILE_HASH: StageStats =
    StageStats::new("worker.plan_i.file_hash");
/// Single-flight `download_to_directory` coalescer counters. `lead`
/// fires when this caller is the first for a `(path, digest)` and
/// will do the actual walk. `follow` fires when another caller is
/// already in-flight; we await them. `follow_late_miss` fires when
/// after waiting we find Plan L still empty (leader failed) and fall
/// through to running our own walk — should be near zero, dominates
/// only when the underlying walk is consistently failing.
static COALESCE_LEAD: StageStats = StageStats::new("worker.coalesce.lead");
static COALESCE_FOLLOW: StageStats = StageStats::new("worker.coalesce.follow");
static COALESCE_FOLLOW_LATE_MISS: StageStats =
    StageStats::new("worker.coalesce.follow_late_miss");
/// Wall-clock that followers spend awaiting their leader — the time
/// they would otherwise have spent re-walking the same subtree. Pair
/// with `follow` count to compute average savings per coalesce.
static COALESCE_FOLLOW_WAIT: StageStats =
    StageStats::new("worker.coalesce.follow_wait");
static ACTION_EXECUTE: StageStats = StageStats::new("worker.action_execute");
static UPLOAD_RESULTS: StageStats = StageStats::new("worker.upload_results");
use parking_lot::Mutex;
use prost::Message;
use relative_path::RelativePath;
use scopeguard::{ScopeGuard, guard};
use serde::Deserialize;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::process;
use tokio::sync::{Notify, oneshot, watch};
use tokio::time::Instant;
use tokio_stream::wrappers::ReadDirStream;
use tonic::Request;
use tracing::{debug, error, info, trace, warn};
use uuid::Uuid;

/// For simplicity we use a fixed exit code for cases when our program is terminated
/// due to a signal.
const EXIT_CODE_FOR_SIGNAL: i32 = 9;

/// Default strategy for uploading historical results.
/// Note: If this value changes the config documentation
/// should reflect it.
const DEFAULT_HISTORICAL_RESULTS_STRATEGY: UploadCacheResultsStrategy =
    UploadCacheResultsStrategy::FailuresOnly;

/// Valid string reasons for a failure.
/// Note: If these change, the documentation should be updated.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum SideChannelFailureReason {
    /// Task should be considered timed out.
    Timeout,
}

/// This represents the json data that can be passed from the running process
/// to the parent via the `SideChannelFile`. See:
/// `config::EnvironmentSource::sidechannelfile` for more details.
/// Note: Any fields added here must be added to the documentation.
#[derive(Debug, Deserialize, Default)]
struct SideChannelInfo {
    /// If the task should be considered a failure and why.
    failure: Option<SideChannelFailureReason>,
}

/// Apply unix mode to `dest`, downgrading PermissionDenied to a non-fatal
/// warning. macOS SDK files staged under `xcode_links/MacOSX*.sdk` carry
/// uchg / SIP-derived flags that block any chmod for the file's owner;
/// the file content is already in place via hardlink, which is what the
/// action actually consumes. Other error classes still propagate so a
/// genuinely broken filesystem still surfaces as a hard error.
#[cfg(target_family = "unix")]
async fn apply_chmod_or_warn(dest: &str, unix_mode: u32, context: &str) -> Result<(), Error> {
    match fs::set_permissions(dest, Permissions::from_mode(unix_mode)).await {
        Ok(()) => Ok(()),
        Err(e) if e.code == Code::PermissionDenied => {
            warn!(
                dest = %dest,
                unix_mode = format!("{unix_mode:o}"),
                error = ?e,
                context,
                "set_permissions denied (immutable / SDK file flag / read-only mount); \
                 continuing — file content already present via hardlink",
            );
            Ok(())
        }
        Err(e) => Err(e.append(format!("Could not set unix mode in {context} {dest}"))),
    }
}

/// Apply mtime to `dest`, downgrading PermissionDenied to a non-fatal
/// warning. Same rationale as `apply_chmod_or_warn`: utimes() also
/// fails on uchg-flagged Apple SDK files, and the action does not
/// depend on perfect mtime mirroring.
async fn apply_mtime_or_warn(
    dest: &str,
    mtime_seconds: i64,
    mtime_nanos: i32,
    context: &str,
) -> Result<(), Error> {
    let dest_owned = dest.to_string();
    let context_owned = context.to_string();
    let result: Result<(), Error> = spawn_blocking!("apply_mtime_or_warn", move || {
        set_file_mtime(
            &dest_owned,
            FileTime::from_unix_time(mtime_seconds, mtime_nanos as u32),
        )
        .map_err(|e| {
            let err: Error = e.into();
            err.append(format!("Failed to set mtime in {context_owned} {dest_owned}"))
        })
    })
    .await
    .err_tip(|| format!("Failed to launch spawn_blocking for mtime in {context}"))?;
    match result {
        Ok(()) => Ok(()),
        Err(e) if e.code == Code::PermissionDenied => {
            warn!(
                dest = %dest,
                error = ?e,
                context,
                "set_file_mtime denied (immutable / SDK file flag / read-only mount); \
                 continuing — file content already present via hardlink",
            );
            Ok(())
        }
        Err(e) => Err(e),
    }
}

/// Aggressively download the digests of files and make a local folder from it. This function
/// will spawn unbounded number of futures to try and get these downloaded. The store itself
/// should be rate limited if spawning too many requests at once is an issue.
/// We require the `FilesystemStore` to be the `fast` store of `FastSlowStore`. This is for
/// efficiency reasons. We will request the `FastSlowStore` to populate the entry then we will
/// assume the `FilesystemStore` has the file available immediately after and hardlink the file
/// to a new location.
// Sadly we cannot use `async fn` here because the rust compiler cannot determine the auto traits
// of the future. So we need to force this function to return a dynamic future instead.
// see: https://github.com/rust-lang/rust/issues/78649
pub fn download_to_directory<'a>(
    cas_store: &'a FastSlowStore,
    filesystem_store: Pin<&'a FilesystemStore>,
    digest: &'a DigestInfo,
    current_directory: &'a str,
    hint_root: Option<PathBuf>,
    path_digest_cache: &'a crate::path_digest_cache::PathDigestCache,
    digest_checked_hint_link: bool,
) -> BoxFuture<'a, Result<(), Error>> {
    async move {
        let _walk_timer = DIR_WALK.timer();
        // Plan L (path-aware): records that this Directory subtree was
        // fully walked into THIS specific target path. We retain the
        // marker for telemetry and for the per-file Plan K fast path
        // it warms, but we DO NOT short-circuit the walk on a hit:
        // a "walked once" stamp is not a "still on disk" guarantee
        // (FS-store eviction, sibling cleanup, gclient re-sync, etc.
        // can remove files between mark and re-hit). The walk re-runs
        // and Plan K (with its own per-file stat gate, see below) is
        // the trust boundary for whether each file is actually present.
        if path_digest_cache.dir_walked(current_directory, digest) {
            PLAN_L_HIT.incr();
            trace!(
                ?digest,
                current_directory,
                "Plan L: prior-walk stamp present; re-walking to verify on-disk state"
            );
        } else {
            PLAN_L_MISS.incr();
        }

        // Single-flight: if another concurrent action is already
        // walking this exact (path, digest), wait for it instead of
        // duplicating the work. This collapses the cold-start
        // thundering herd (N actions × full recursive walk) into one
        // walk + (N-1) cheap awaits. After the leader finishes we
        // re-check Plan L — on success it hits and we fast-path out;
        // on failure it stays empty and we fall through to do our own
        // walk (so a transient leader error doesn't block followers).
        let coalesce_path = std::path::PathBuf::from(current_directory);
        let _lead_guard = match path_digest_cache
            .coalescer()
            .try_lead_or_follow(&coalesce_path, *digest)
        {
            crate::dir_walk_coalescer::CoalesceOutcome::Lead(g) => {
                COALESCE_LEAD.incr();
                Some(g)
            }
            crate::dir_walk_coalescer::CoalesceOutcome::Follow(slot) => {
                COALESCE_FOLLOW.incr();
                {
                    let _t = COALESCE_FOLLOW_WAIT.timer();
                    slot.wait().await;
                }
                if path_digest_cache.dir_walked(current_directory, digest) {
                    trace!(
                        ?digest,
                        current_directory,
                        "coalesce: leader succeeded, fast-path out"
                    );
                    return Ok(());
                }
                COALESCE_FOLLOW_LATE_MISS.incr();
                trace!(
                    ?digest,
                    current_directory,
                    "coalesce: leader failed; running our own walk"
                );
                // Try once more to claim the lead so a *third* caller
                // arriving here can also coalesce against us. If we
                // can't lead (someone else became the new leader
                // first), just proceed without coalescing.
                match path_digest_cache
                    .coalescer()
                    .try_lead_or_follow(&coalesce_path, *digest)
                {
                    crate::dir_walk_coalescer::CoalesceOutcome::Lead(g) => Some(g),
                    crate::dir_walk_coalescer::CoalesceOutcome::Follow(_) => None,
                }
            }
        };

        let directory = {
            let _t = DIR_PROTO_FETCH.timer();
            get_and_decode_digest::<ProtoDirectory>(cas_store, digest.into())
                .await
                .err_tip(|| "Converting digest to Directory")?
        };
        let mut futures = FuturesUnordered::new();

        for file in directory.files {
            let digest: DigestInfo = file
                .digest
                .err_tip(|| "Expected Digest to exist in Directory::file::digest")?
                .try_into()
                .err_tip(|| "In Directory::file::digest")?;
            let dest = format!("{}/{}", current_directory, file.name);
            let (mtime, mut unix_mode) = match file.node_properties {
                Some(properties) => (properties.mtime, properties.unix_mode),
                None => (None, None),
            };
            #[cfg_attr(target_family = "windows", allow(unused_assignments))]
            if file.is_executable {
                unix_mode = Some(unix_mode.unwrap_or(0o444) | 0o111);
            }
            let dest_path_for_cache = PathBuf::from(&dest);
            let plan_i_hint_path: Option<PathBuf> = if digest_checked_hint_link {
                hint_root.as_ref().map(|root| root.join(&file.name))
            } else {
                None
            };
            futures.push(
                async move {
                    // Plan K: cache-first, BUT verify the file still exists
                    // on disk before trusting the (path, digest) entry. A
                    // stale entry (FS-store evicted the only hardlink, a
                    // sibling action cleaned up, gclient re-synced, etc.)
                    // would otherwise let us return Ok(()) for a missing
                    // file — the action then runs against an input root
                    // with a hole in it. Cheap symlink_metadata stat is
                    // the trust boundary; on miss we evict the stale
                    // entry and fall through to Plan I / CAS.
                    if path_digest_cache.contains(&dest_path_for_cache, &digest) {
                        if tokio::fs::symlink_metadata(&dest_path_for_cache).await.is_ok() {
                            PLAN_K_HIT.incr();
                            return Ok::<(), Error>(());
                        }
                        path_digest_cache.evict(&dest_path_for_cache);
                        trace!(
                            dest = %dest,
                            ?digest,
                            "Plan K stale: cached (path, digest) but file missing on disk; re-materializing"
                        );
                    }
                    PLAN_K_MISS.incr();
                    trace!(
                        dest = %dest,
                        ?digest,
                        "Plan K miss — checking disk / fetching from CAS"
                    );

                    // Plan I (digest-checked hint link, opt-in): if a
                    // pre-staged file lives at hint_root and its content
                    // hashes to the expected digest, hardlink it to dest
                    // and skip CAS. The legacy Plan I checked only size,
                    // which silently accepted same-size/different-content
                    // collisions; this variant uses the action's digest
                    // function and is correctness-preserving — any miss
                    // (size mismatch, content mismatch, missing file, I/O
                    // error) falls through to the existing CAS path below.
                    let plan_i_hit = if let Some(ref hint_path) = plan_i_hint_path {
                        let hasher_func = opentelemetry::Context::current()
                            .get::<DigestHasherFunc>()
                            .map_or_else(
                                nativelink_util::digest_hasher::default_digest_hasher_func,
                                |v| *v,
                            );
                        // Timer-wrap so the Plan I storm is visible
                        // in the periodic timing dump. Without this
                        // worker.plan_i.file_hash stat, a worker
                        // hashing thousands of files across many
                        // concurrent actions looks idle in metrics.
                        let _t = PLAN_I_FILE_HASH.timer();
                        let verified = crate::file_digest_check::file_matches_digest(
                            hint_path,
                            &digest,
                            hasher_func,
                        )
                        .await;
                        if verified {
                            // Hint matches. If hint_path == dest the file is
                            // already where the action needs it; otherwise
                            // hardlink hint → dest.
                            let dest_pb = PathBuf::from(&dest);
                            let link_ok = if hint_path == &dest_pb {
                                true
                            } else {
                                let _t = HARD_LINK.timer();
                                match fs::hard_link(hint_path, &dest).await {
                                    Ok(()) => true,
                                    Err(e)
                                        if e.code == Code::AlreadyExists
                                            || format!("{e:?}").contains("os error 17") =>
                                    {
                                        // Same digest already at dest — trust it.
                                        true
                                    }
                                    Err(_) => {
                                        // Cross-device / perms / etc. Never
                                        // let Plan I block the action; fall
                                        // through to CAS below.
                                        false
                                    }
                                }
                            };
                            if link_ok {
                                PLAN_I_HIT.incr();
                                trace!(
                                    dest = %dest,
                                    hint = %hint_path.display(),
                                    ?digest,
                                    "Plan I: hint hit, hardlink-from-disk"
                                );
                                true
                            } else {
                                PLAN_I_MISS.incr();
                                false
                            }
                        } else {
                            PLAN_I_MISS.incr();
                            false
                        }
                    } else {
                        false
                    };

                    if plan_i_hit {
                        // Apply perms/mtime same as the CAS path so the
                        // file presented to the action is identical either
                        // way, then record in Plan K and return. Both
                        // chmod and utimes are downgraded to non-fatal
                        // warnings on PermissionDenied: macOS SDK files
                        // pre-staged under `xcode_links/` carry uchg
                        // (immutable) flags that block any mode/mtime
                        // change for the file's owner, and the file
                        // content is already in place via hardlink — the
                        // action only needs the bytes, not perfect
                        // metadata mirroring. Other error classes still
                        // propagate.
                        #[cfg(target_family = "unix")]
                        if let Some(unix_mode) = unix_mode {
                            apply_chmod_or_warn(&dest, unix_mode, "Plan I").await?;
                        }
                        if let Some(mtime) = mtime {
                            apply_mtime_or_warn(&dest, mtime.seconds, mtime.nanos, "Plan I")
                                .await?;
                        }
                        path_digest_cache.insert(dest_path_for_cache, digest);
                        return Ok::<(), Error>(());
                    }

                    // Plan J stat-hit is DISABLED. It checked only file
                    // size, not content hash. Same-size/different-content
                    // files (common for generated headers that change
                    // between builds) were silently accepted, causing
                    // build failures. Plan K is the only trusted fast
                    // path — it checks full (path, digest).
                    //
                    // On Plan K miss without a Plan I hit, always go to CAS.
                    {
                        info!(dest = %dest, ?digest, "CAS fetch — Plan K miss, downloading from CAS");
                        {
                            let _t = CAS_POPULATE.timer();
                            cas_store
                                .populate_fast_store(digest.into())
                                .await
                                .map_err(|e| e.append(format!("populate_fast_store for digest {digest}")))?;
                        }
                        if is_zero_digest(digest) {
                            let mut file_slot = fs::create_file(&dest).await?;
                            file_slot.write_all(&[]).await?;
                        } else {
                            let file_entry = filesystem_store
                                .get_file_entry_for_digest(&digest)
                                .await
                                .err_tip(|| "During hard link")?;
                            let src_path = file_entry
                                .get_file_path_locked(|src| async move { Ok(PathBuf::from(src)) })
                                .await?;
                            let _t = HARD_LINK.timer();
                            match fs::hard_link(&src_path, &dest).await {
                                Ok(()) => {}
                                Err(e)
                                    if e.code == Code::AlreadyExists
                                        || format!("{e:?}").contains("os error 17") =>
                                {
                                    // File already exists — trust it. It's either
                                    // the same content from rsync or a previous
                                    // CAS fetch. Either way, it's on disk and
                                    // usable. Plan K will record it below.
                                    trace!(
                                        dest = %dest,
                                        "hard_link EEXIST — file already on disk, trusting it"
                                    );
                                }
                                Err(e) if e.code == Code::NotFound => {
                                    return Err(make_err!(
                                        Code::Internal,
                                        "Could not make hardlink, file was likely evicted from cache. {e:?} : {dest}"
                                    ));
                                }
                                Err(e) => {
                                    return Err(make_err!(
                                        Code::Internal,
                                        "Could not make hardlink, {e:?} : {dest}"
                                    ));
                                }
                            }
                        }
                        // Verify the file actually landed on disk.
                        if tokio::fs::metadata(&dest).await.is_err() {
                            warn!(
                                dest = %dest,
                                ?digest,
                                "BUG: CAS fetch completed but file does NOT exist on disk"
                            );
                        }
                    }

                    // Always apply perms and mtime — we always fetch from CAS
                    // on Plan K miss. Same PermissionDenied degradation as
                    // Plan I above: SDK files / immutable filesystems can
                    // refuse chmod/utimes on otherwise-owned content.
                    {
                        #[cfg(target_family = "unix")]
                        if let Some(unix_mode) = unix_mode {
                            apply_chmod_or_warn(&dest, unix_mode, "download_to_directory").await?;
                        }
                        if let Some(mtime) = mtime {
                            apply_mtime_or_warn(
                                &dest,
                                mtime.seconds,
                                mtime.nanos,
                                "download_to_directory",
                            )
                            .await?;
                        }
                    }

                    // Plan K: record the verified (path, digest) pair.
                    path_digest_cache.insert(dest_path_for_cache, digest);

                    Ok::<(), Error>(())
                }
                .map_err(move |e: Error| e.append(format!("for digest {digest}")))
                .boxed(),
            );
        }

        for directory in directory.directories {
            let digest: DigestInfo = directory
                .digest
                .err_tip(|| "Expected Digest to exist in Directory::directories::digest")?
                .try_into()
                .err_tip(|| "In Directory::file::digest")?;
            let new_directory_path = format!("{}/{}", current_directory, directory.name);
            let child_hint: Option<PathBuf> =
                hint_root.as_ref().map(|root| root.join(&directory.name));
            futures.push(
                async move {
                    // Plan J: create_dir_all is idempotent — shared-tree subdirs
                    // already exist from the pre-staged tree or a prior action.
                    fs::create_dir_all(&new_directory_path)
                        .await
                        .err_tip(|| format!("Could not create directory {new_directory_path}"))?;
                    download_to_directory(
                        cas_store,
                        filesystem_store,
                        &digest,
                        &new_directory_path,
                        child_hint,
                        path_digest_cache,
                        digest_checked_hint_link,
                    )
                    .await
                    .err_tip(|| format!("in download_to_directory : {new_directory_path}"))?;
                    Ok(())
                }
                .boxed(),
            );
        }

        #[cfg(target_family = "unix")]
        for symlink_node in directory.symlinks {
            let dest = format!("{}/{}", current_directory, symlink_node.name);
            futures.push(
                async move {
                    // Plan J: idempotent symlink — skip if already present.
                    match tokio::fs::symlink_metadata(&dest).await {
                        Ok(_) => {
                            // Symlink already present; leave it.
                        }
                        Err(_) => {
                            fs::symlink(&symlink_node.target, &dest).await.err_tip(|| {
                                format!(
                                    "Could not create symlink {} -> {}",
                                    symlink_node.target, dest
                                )
                            })?;
                        }
                    }
                    Ok(())
                }
                .boxed(),
            );
        }

        while futures.try_next().await?.is_some() {}
        // Plan L (path-aware): mark this subtree as walked at this path.
        path_digest_cache.mark_dir_walked(current_directory, *digest);
        Ok(())
    }
    .boxed()
}

/// Back-compat no-op (1.3.4): the env var still parses so workers
/// started with `NATIVELINK_PLAN_M_DISABLE=1` don't error out, but
/// Plan M's full-tree synth is unconditionally removed in this
/// version — the function the variable used to disable no longer
/// exists in the action path. Kept so operators upgrading from 1.3.x
/// don't have to rip the env var out of their launch scripts.
#[allow(dead_code)]
fn plan_m_disabled_via_env() -> bool {
    std::env::var("NATIVELINK_PLAN_M_DISABLE")
        .ok()
        .map(|v| matches!(v.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"))
        .unwrap_or(false)
}

/// Prepares action inputs by first trying the directory cache (if available),
/// then falling back to traditional `download_to_directory`.
///
/// This provides a significant performance improvement for repeated builds
/// with the same input directories.
pub async fn prepare_action_inputs(
    directory_cache: &Option<Arc<crate::directory_cache::DirectoryCache>>,
    cas_store: &FastSlowStore,
    filesystem_store: Pin<&FilesystemStore>,
    digest: &DigestInfo,
    work_directory: &str,
    hint_root: Option<PathBuf>,
    path_digest_cache: &crate::path_digest_cache::PathDigestCache,
    digest_checked_hint_link: bool,
) -> Result<(), Error> {
    let _t = PREPARE_INPUTS.timer();
    // Architectural contract: every call walks the input tree. Missing
    // files are fetched from CAS; Plan K (path_digest_cache) and Plan L
    // (walked_dirs) handle the fast path so the walk is cheap on rebuilds.
    // The previous `skip_input_tree_walk` fast path required a separate
    // cross-machine publish/barrier/drain mechanism to pre-stage outputs,
    // which proved unreliable in practice — every failure report #8-#15
    // traced back to a race in that external sync. Keeping the walk
    // unconditional makes correctness local to each action.

    // Try cache first if available
    if let Some(cache) = directory_cache {
        match cache
            .get_or_create(*digest, Path::new(work_directory))
            .await
        {
            Ok(cache_hit) => {
                trace!(
                    ?digest,
                    work_directory, cache_hit, "Successfully prepared inputs via directory cache"
                );
                return Ok(());
            }
            Err(e) => {
                warn!(
                    ?digest,
                    ?e,
                    "Directory cache failed, falling back to traditional download"
                );
                // Fall through to traditional path
            }
        }
    }

    // 1.3.4: Plan M's top-level whole-hint-tree synthesis is REMOVED.
    //
    // Why: in the production shared-tree topology (Chromium with
    // `InputRootAbsolutePath` set to `src/out/Mac`), the local hint
    // tree is much larger than what any single action's input root
    // references. Synthesizing the WHOLE local tree's Directory
    // proto and comparing to the action's expected `digest` produced
    // a guaranteed Miss while still paying minutes-to-hours of file
    // hashing — pure waste storming sys CPU. Coalescing N concurrent
    // synth walks to 1 (1.3.3) only delayed the inevitable; the one
    // walk that did run was still a full-tree walk for nothing.
    //
    // The right shape is `download_to_directory`'s natural recursion:
    // it only walks subtrees the action's Directory proto references,
    // it only hashes (via Plan I) files the action declares, and it
    // marks Plan L bottom-up so subsequent actions hit the cache.
    // That IS "only what we need." Plan M's upper-level synth was
    // adding a wasteful pre-walk on top.
    //
    // The synthesis function itself stays in
    // `nativelink_worker::local_dir_synthesis` as a primitive — it
    // may be useful later for explicit operator-triggered prewarm or
    // a per-subtree variant — but the action path no longer invokes
    // it. `NATIVELINK_PLAN_M_DISABLE` env var is now a no-op (the
    // feature it disabled is gone) but kept for back-compat: a
    // worker started with the flag set still works.

    download_to_directory(
        cas_store,
        filesystem_store,
        digest,
        work_directory,
        hint_root,
        path_digest_cache,
        digest_checked_hint_link,
    )
    .await
}

#[cfg(target_family = "windows")]
fn is_executable(_metadata: &std::fs::Metadata, full_path: &impl AsRef<Path>) -> bool {
    static EXECUTABLE_EXTENSIONS: &[&str] = &["exe", "bat", "com"];
    EXECUTABLE_EXTENSIONS
        .iter()
        .any(|ext| full_path.as_ref().extension().map_or(false, |v| v == *ext))
}

#[cfg(target_family = "unix")]
fn is_executable(metadata: &std::fs::Metadata, _full_path: &impl AsRef<Path>) -> bool {
    (metadata.mode() & 0o111) != 0
}

type DigestUploader = Arc<tokio::sync::OnceCell<()>>;

/// Walk a Tree protobuf (returned by `upload_directory` for action output
/// directories) and produce `(full_path, DigestInfo)` pairs for every file
/// in the subtree, including transitively-nested subdirectories. Symlinks
/// are not emitted.
///
/// `base_path` is prepended to each emitted path. Caller typically passes
/// the output directory's path relative to the work_directory so the
/// resulting paths can be journaled into `pending_outputs` for other
/// workers to materialize.
pub fn walk_output_tree_for_paths(
    tree: &ProtoTree,
    base_path: &str,
) -> Vec<(String, DigestInfo)> {
    use std::collections::HashMap;
    let Some(root) = &tree.root else {
        return Vec::new();
    };

    // Build a digest -> Directory map for the children so we can walk
    // DirectoryNode references without re-scanning the children list.
    let mut by_digest: HashMap<DigestInfo, &ProtoDirectory> = HashMap::new();
    for child in &tree.children {
        let bytes = child.encode_to_vec();
        let digest = compute_buf_digest(&bytes, &mut DigestHasherFunc::Sha256.hasher());
        by_digest.insert(digest, child);
    }

    let mut out = Vec::new();
    let mut stack: VecDeque<(String, &ProtoDirectory)> = VecDeque::new();
    stack.push_back((base_path.to_string(), root));

    while let Some((prefix, dir)) = stack.pop_front() {
        for f in &dir.files {
            let Some(d) = &f.digest else { continue };
            let Ok(digest_info) = DigestInfo::try_from(d.clone()) else {
                continue;
            };
            let full_path = if prefix.is_empty() {
                f.name.clone()
            } else {
                format!("{prefix}/{}", f.name)
            };
            out.push((full_path, digest_info));
        }
        for d in &dir.directories {
            let Some(dig) = &d.digest else { continue };
            let Ok(digest_info) = DigestInfo::try_from(dig.clone()) else {
                continue;
            };
            let Some(child) = by_digest.get(&digest_info) else {
                continue;
            };
            let new_prefix = if prefix.is_empty() {
                d.name.clone()
            } else {
                format!("{prefix}/{}", d.name)
            };
            stack.push_back((new_prefix, child));
        }
    }
    out
}

async fn upload_file(
    cas_store: Pin<&impl StoreLike>,
    full_path: impl AsRef<Path> + Debug + Send + Sync,
    hasher: DigestHasherFunc,
    metadata: std::fs::Metadata,
    digest_uploaders: Arc<Mutex<HashMap<DigestInfo, DigestUploader>>>,
) -> Result<FileInfo, Error> {
    let is_executable = is_executable(&metadata, &full_path);
    let file_size = metadata.len();
    let file = fs::open_file(&full_path, 0, u64::MAX)
        .await
        .err_tip(|| format!("Could not open file {full_path:?}"))?;

    let (digest, mut file) = hasher
        .hasher()
        .digest_for_file(&full_path, file.into_inner(), Some(file_size))
        .await
        .err_tip(|| format!("Failed to hash file in digest_for_file failed for {full_path:?}"))?;

    let digest_uploader = match digest_uploaders.lock().entry(digest) {
        std::collections::hash_map::Entry::Occupied(occupied_entry) => occupied_entry.get().clone(),
        std::collections::hash_map::Entry::Vacant(vacant_entry) => vacant_entry
            .insert(Arc::new(tokio::sync::OnceCell::new()))
            .clone(),
    };

    // Only upload a file with a given hash once.  The file may exist multiple
    // times in the output with different names.
    digest_uploader
        .get_or_try_init(async || {
            // Only upload if the digest doesn't already exist, this should be
            // a much cheaper operation than an upload.
            let cas_store = cas_store.as_store_driver_pin();
            let store_key: nativelink_util::store_trait::StoreKey<'_> = digest.into();
            let has_start = std::time::Instant::now();
            if cas_store
                .has(store_key.borrow())
                .await
                .is_ok_and(|result| result.is_some())
            {
                trace!(
                    ?digest,
                    has_elapsed_ms = has_start.elapsed().as_millis(),
                    "upload_file: digest already exists in CAS, skipping upload",
                );
                return Ok(());
            }
            trace!(
                ?digest,
                has_elapsed_ms = has_start.elapsed().as_millis(),
                file_size = digest.size_bytes(),
                "upload_file: digest not in CAS, starting upload",
            );

            file.rewind().await.err_tip(|| "Could not rewind file")?;

            // Note: For unknown reasons we appear to be hitting:
            // https://github.com/rust-lang/rust/issues/92096
            // or a similar issue if we try to use the non-store driver function, so we
            // are using the store driver function here.
            let store_key_for_upload = store_key.clone();
            let file_upload_start = std::time::Instant::now();
            let upload_result = cas_store
                .update_with_whole_file(
                    store_key_for_upload,
                    full_path.as_ref().into(),
                    file,
                    UploadSizeInfo::ExactSize(digest.size_bytes()),
                )
                .await
                .map(|_slot| ());
            trace!(
                ?digest,
                upload_elapsed_ms = file_upload_start.elapsed().as_millis(),
                success = upload_result.is_ok(),
                "upload_file: update_with_whole_file completed",
            );

            match upload_result {
                Ok(()) => Ok(()),
                Err(err) => {
                    // Output uploads run concurrently and may overlap (e.g. a file is listed
                    // both as an output file and inside an output directory). When another
                    // upload has already moved the file into CAS, this update can fail with
                    // NotFound even though the digest is now present. Per the RE spec, missing
                    // outputs should be ignored, so treat this as success if the digest exists.
                    if err.code == Code::NotFound
                        && cas_store
                            .has(store_key.borrow())
                            .await
                            .is_ok_and(|result| result.is_some())
                    {
                        Ok(())
                    } else {
                        Err(err)
                    }
                }
            }
        })
        .await
        .err_tip(|| format!("for {full_path:?}"))?;

    let name = full_path
        .as_ref()
        .file_name()
        .err_tip(|| format!("Expected file_name to exist on {full_path:?}"))?
        .to_str()
        .err_tip(|| {
            make_err!(
                Code::Internal,
                "Could not convert {:?} to string",
                full_path
            )
        })?
        .to_string();

    Ok(FileInfo {
        name_or_path: NameOrPath::Name(name),
        digest,
        is_executable,
    })
}

async fn upload_symlink(
    full_path: impl AsRef<Path> + Debug,
    full_work_directory_path: impl AsRef<Path>,
) -> Result<SymlinkInfo, Error> {
    let full_target_path = fs::read_link(full_path.as_ref())
        .await
        .err_tip(|| format!("Could not get read_link path of {full_path:?}"))?;

    // Detect if our symlink is inside our work directory, if it is find the
    // relative path otherwise use the absolute path.
    let target = if full_target_path.starts_with(full_work_directory_path.as_ref()) {
        let full_target_path = RelativePath::from_path(&full_target_path)
            .map_err(|v| make_err!(Code::Internal, "Could not convert {} to RelativePath", v))?;
        RelativePath::from_path(full_work_directory_path.as_ref())
            .map_err(|v| make_err!(Code::Internal, "Could not convert {} to RelativePath", v))?
            .relative(full_target_path)
            .normalize()
            .into_string()
    } else {
        full_target_path
            .to_str()
            .err_tip(|| {
                make_err!(
                    Code::Internal,
                    "Could not convert '{:?}' to string",
                    full_target_path
                )
            })?
            .to_string()
    };

    let name = full_path
        .as_ref()
        .file_name()
        .err_tip(|| format!("Expected file_name to exist on {full_path:?}"))?
        .to_str()
        .err_tip(|| {
            make_err!(
                Code::Internal,
                "Could not convert {:?} to string",
                full_path
            )
        })?
        .to_string();

    Ok(SymlinkInfo {
        name_or_path: NameOrPath::Name(name),
        target,
    })
}

fn upload_directory<'a, P: AsRef<Path> + Debug + Send + Sync + Clone + 'a>(
    cas_store: Pin<&'a impl StoreLike>,
    full_dir_path: P,
    full_work_directory: &'a str,
    hasher: DigestHasherFunc,
    digest_uploaders: Arc<Mutex<HashMap<DigestInfo, DigestUploader>>>,
) -> BoxFuture<'a, Result<(Directory, VecDeque<ProtoDirectory>), Error>> {
    Box::pin(async move {
        let file_futures = FuturesUnordered::new();
        let dir_futures = FuturesUnordered::new();
        let symlink_futures = FuturesUnordered::new();
        {
            let (_permit, dir_handle) = fs::read_dir(&full_dir_path)
                .await
                .err_tip(|| format!("Error reading dir for reading {full_dir_path:?}"))?
                .into_inner();
            let mut dir_stream = ReadDirStream::new(dir_handle);
            // Note: Try very hard to not leave file descriptors open. Try to keep them as short
            // lived as possible. This is why we iterate the directory and then build a bunch of
            // futures with all the work we are wanting to do then execute it. It allows us to
            // close the directory iterator file descriptor, then open the child files/folders.
            while let Some(entry_result) = dir_stream.next().await {
                let entry = entry_result.err_tip(|| "Error while iterating directory")?;
                let file_type = entry
                    .file_type()
                    .await
                    .err_tip(|| format!("Error running file_type() on {entry:?}"))?;
                let full_path = full_dir_path.as_ref().join(entry.path());
                if file_type.is_dir() {
                    let full_dir_path = full_dir_path.clone();
                    dir_futures.push(
                        upload_directory(
                            cas_store,
                            full_path.clone(),
                            full_work_directory,
                            hasher,
                            digest_uploaders.clone(),
                        )
                        .and_then(|(dir, all_dirs)| async move {
                            let directory_name = full_path
                                .file_name()
                                .err_tip(|| {
                                    format!("Expected file_name to exist on {full_dir_path:?}")
                                })?
                                .to_str()
                                .err_tip(|| {
                                    make_err!(
                                        Code::Internal,
                                        "Could not convert {:?} to string",
                                        full_dir_path
                                    )
                                })?
                                .to_string();

                            let digest =
                                serialize_and_upload_message(&dir, cas_store, &mut hasher.hasher())
                                    .await
                                    .err_tip(|| format!("for {}", full_path.display()))?;

                            Result::<(DirectoryNode, VecDeque<Directory>), Error>::Ok((
                                DirectoryNode {
                                    name: directory_name,
                                    digest: Some(digest.into()),
                                },
                                all_dirs,
                            ))
                        })
                        .boxed(),
                    );
                } else if file_type.is_file() {
                    let digest_uploaders = digest_uploaders.clone();
                    file_futures.push(async move {
                        let metadata = fs::metadata(&full_path)
                            .await
                            .err_tip(|| format!("Could not open file {}", full_path.display()))?;
                        upload_file(cas_store, &full_path, hasher, metadata, digest_uploaders)
                            .map_ok(TryInto::try_into)
                            .await?
                    });
                } else if file_type.is_symlink() {
                    symlink_futures.push(
                        upload_symlink(full_path, &full_work_directory)
                            .map(|symlink| symlink?.try_into()),
                    );
                }
            }
        }

        let (mut file_nodes, dir_entries, mut symlinks) = try_join3(
            file_futures.try_collect::<Vec<FileNode>>(),
            dir_futures.try_collect::<Vec<(DirectoryNode, VecDeque<Directory>)>>(),
            symlink_futures.try_collect::<Vec<SymlinkNode>>(),
        )
        .await?;

        let mut directory_nodes = Vec::with_capacity(dir_entries.len());
        // For efficiency we use a deque because it allows cheap concat of Vecs.
        // We make the assumption here that when performance is important it is because
        // our directory is quite large. This allows us to cheaply merge large amounts of
        // directories into one VecDeque. Then after we are done we need to collapse it
        // down into a single Vec.
        let mut all_child_directories = VecDeque::with_capacity(dir_entries.len());
        for (directory_node, mut recursive_child_directories) in dir_entries {
            directory_nodes.push(directory_node);
            all_child_directories.append(&mut recursive_child_directories);
        }

        file_nodes.sort_unstable_by(|a, b| a.name.cmp(&b.name));
        directory_nodes.sort_unstable_by(|a, b| a.name.cmp(&b.name));
        symlinks.sort_unstable_by(|a, b| a.name.cmp(&b.name));

        let directory = Directory {
            files: file_nodes,
            directories: directory_nodes,
            symlinks,
            node_properties: None, // We don't support file properties.
        };
        all_child_directories.push_back(directory.clone());

        Ok((directory, all_child_directories))
    })
}

async fn process_side_channel_file(
    side_channel_file: Cow<'_, OsStr>,
    args: &[&OsStr],
    timeout: Duration,
) -> Result<Option<Error>, Error> {
    let mut json_contents = String::new();
    {
        // Note: Scoping `file_slot` allows the file_slot semaphore to be released faster.
        let mut file_slot = match fs::open_file(side_channel_file, 0, u64::MAX).await {
            Ok(file_slot) => file_slot,
            Err(e) => {
                if e.code != Code::NotFound {
                    return Err(e).err_tip(|| "Error opening side channel file");
                }
                // Note: If file does not exist, it's ok. Users are not required to create this file.
                return Ok(None);
            }
        };
        file_slot
            .read_to_string(&mut json_contents)
            .await
            .err_tip(|| "Error reading side channel file")?;
    }

    let side_channel_info: SideChannelInfo =
        serde_json5::from_str(&json_contents).map_err(|e| {
            make_input_err!(
                "Could not convert contents of side channel file (json) to SideChannelInfo : {e:?}"
            )
        })?;
    Ok(side_channel_info.failure.map(|failure| match failure {
        SideChannelFailureReason::Timeout => Error::new(
            Code::DeadlineExceeded,
            format!(
                "Command '{}' timed out after {} seconds",
                args.join(OsStr::new(" ")).to_string_lossy(),
                timeout.as_secs_f32()
            ),
        ),
    }))
}

async fn do_cleanup(
    running_actions_manager: &Arc<RunningActionsManagerImpl>,
    operation_id: &OperationId,
    action_directory: &str,
) -> Result<(), Error> {
    // Mark this operation as being cleaned up
    let Some(_cleaning_guard) = running_actions_manager.perform_cleanup(operation_id.clone())
    else {
        // Cleanup is already happening elsewhere.
        return Ok(());
    };

    debug!("Worker cleaning up");
    // Note: We need to be careful to keep trying to cleanup even if one of the steps fails.
    let remove_dir_result = fs::remove_dir_all(action_directory)
        .await
        .err_tip(|| format!("Could not remove working directory {action_directory}"));

    if let Err(err) = running_actions_manager.cleanup_action(operation_id) {
        error!(%operation_id, ?err, "Error cleaning up action");
        Result::<(), Error>::Err(err).merge(remove_dir_result)
    } else if let Err(err) = remove_dir_result {
        error!(%operation_id, ?err, "Error removing working directory");
        Err(err)
    } else {
        Ok(())
    }
}

pub trait RunningAction: Sync + Send + Sized + Unpin + 'static {
    /// Returns the action id of the action.
    fn get_operation_id(&self) -> &OperationId;

    /// Anything that needs to execute before the actions is actually executed should happen here.
    fn prepare_action(self: Arc<Self>) -> impl Future<Output = Result<Arc<Self>, Error>> + Send;

    /// Actually perform the execution of the action.
    fn execute(self: Arc<Self>) -> impl Future<Output = Result<Arc<Self>, Error>> + Send;

    /// Any uploading, processing or analyzing of the results should happen here.
    fn upload_results(self: Arc<Self>) -> impl Future<Output = Result<Arc<Self>, Error>> + Send;

    /// Cleanup any residual files, handles or other junk resulting from running the action.
    fn cleanup(self: Arc<Self>) -> impl Future<Output = Result<Arc<Self>, Error>> + Send;

    /// Returns the final result. As a general rule this action should be thought of as
    /// a consumption of `self`, meaning once a return happens here the lifetime of `Self`
    /// is over and any action performed on it after this call is undefined behavior.
    fn get_finished_result(
        self: Arc<Self>,
    ) -> impl Future<Output = Result<ActionResult, Error>> + Send;

    /// Returns the work directory of the action.
    fn get_work_directory(&self) -> &String;
}

#[derive(Debug)]
struct RunningActionImplExecutionResult {
    stdout: Bytes,
    stderr: Bytes,
    exit_code: i32,
}

#[derive(Debug)]
struct RunningActionImplState {
    command_proto: Option<ProtoCommand>,
    // TODO(palfrey) Kill is not implemented yet, but is instrumented.
    // However, it is used if the worker disconnects to destroy current jobs.
    kill_channel_tx: Option<oneshot::Sender<()>>,
    kill_channel_rx: Option<oneshot::Receiver<()>>,
    execution_result: Option<RunningActionImplExecutionResult>,
    action_result: Option<ActionResult>,
    execution_metadata: ExecutionMetadata,
    // If there was an internal error, this will be set.
    // This should NOT be set if everything was fine, but the process had a
    // non-zero exit code. Instead this should be used for internal errors
    // that prevented the action from running, upload failures, timeouts, exc...
    // but we have (or could have) the action results (like stderr/stdout).
    error: Option<Error>,
}

#[derive(Debug)]
pub struct RunningActionImpl {
    operation_id: OperationId,
    action_directory: String,
    work_directory: String,
    // Derived once at construction from the action's
    // `InputRootAbsolutePath` platform property (if any), run through
    // the worker's `project_root` remap. Consumed by Plan I inside
    // `inner_prepare_action` — stored here so tests can observe it and
    // so `::new` owns the sole call to `translate_input_root_path`.
    hint_root: Option<PathBuf>,
    action_info: ActionInfo,
    timeout: Duration,
    running_actions_manager: Arc<RunningActionsManagerImpl>,
    state: Mutex<RunningActionImplState>,
    has_manager_entry: AtomicBool,
    did_cleanup: AtomicBool,
}

impl RunningActionImpl {
    pub fn new(
        execution_metadata: ExecutionMetadata,
        operation_id: OperationId,
        action_directory: String,
        action_info: ActionInfo,
        timeout: Duration,
        running_actions_manager: Arc<RunningActionsManagerImpl>,
    ) -> Self {
        // Plan J: if the action carries an InputRootAbsolutePath platform
        // property, translate it through the worker's `project_root` (if
        // configured) and use that as the work directory. Plan I reuses
        // the same translated path as its hardlink-hint root. When the
        // property is absent or empty the Plan J fallback of
        // `action_directory/work` kicks in.
        let translated_input_root: Option<String> = action_info
            .platform_properties
            .get("InputRootAbsolutePath")
            .filter(|v| !v.is_empty())
            .map(|v| {
                translate_input_root_path(
                    v,
                    running_actions_manager.project_root.as_ref(),
                )
            });
        let work_directory = translated_input_root
            .clone()
            .unwrap_or_else(|| format!("{}/{}", action_directory, "work"));
        let hint_root: Option<PathBuf> = translated_input_root.map(PathBuf::from);
        let (kill_channel_tx, kill_channel_rx) = oneshot::channel();
        Self {
            operation_id,
            action_directory,
            work_directory,
            hint_root,
            action_info,
            timeout,
            running_actions_manager,
            state: Mutex::new(RunningActionImplState {
                command_proto: None,
                kill_channel_rx: Some(kill_channel_rx),
                kill_channel_tx: Some(kill_channel_tx),
                execution_result: None,
                action_result: None,
                execution_metadata,
                error: None,
            }),
            // Always need to ensure that we're removed from the manager on Drop.
            has_manager_entry: AtomicBool::new(true),
            // Only needs to be cleaned up after a prepare_action call, set there.
            did_cleanup: AtomicBool::new(true),
        }
    }

    /// The Plan I hardlink-hint root, as derived from the action's
    /// `InputRootAbsolutePath` platform property and rewritten through
    /// the worker's `project_root` remap. `None` when the property is
    /// absent or empty. Exposed so tests can assert that Plan I's path
    /// derivation went through the remap; production code reads
    /// `self.hint_root` directly.
    pub fn hint_root(&self) -> Option<&Path> {
        self.hint_root.as_deref()
    }

    #[allow(
        clippy::missing_const_for_fn,
        reason = "False positive on stable, but not on nightly"
    )]
    fn metrics(&self) -> &Arc<Metrics> {
        &self.running_actions_manager.metrics
    }

    /// Prepares any actions needed to execute this action. This action will do the following:
    ///
    /// * Download any files needed to execute the action
    /// * Build a folder with all files needed to execute the action.
    ///
    /// This function will aggressively download and spawn potentially thousands of futures. It is
    /// up to the stores to rate limit if needed.
    async fn inner_prepare_action(self: Arc<Self>) -> Result<Arc<Self>, Error> {
        {
            let mut state = self.state.lock();
            state.execution_metadata.input_fetch_start_timestamp =
                (self.running_actions_manager.callbacks.now_fn)();
        }
        let command = {
            // Download and build out our input files/folders. Also fetch and decode our Command.
            let command_fut = self.metrics().get_proto_command_from_store.wrap(async {
                get_and_decode_digest::<ProtoCommand>(
                    self.running_actions_manager.cas_store.as_ref(),
                    self.action_info.command_digest.into(),
                )
                .await
                .err_tip(|| "Converting command_digest to Command")
            });
            let filesystem_store_pin =
                Pin::new(self.running_actions_manager.filesystem_store.as_ref());

            let (command, ()) = try_join(command_fut, async {
                fs::create_dir_all(&self.work_directory)
                    .await
                    .err_tip(|| format!("Error creating work directory {}", self.work_directory))?;
                self.did_cleanup.store(false, Ordering::Release);
                self.metrics()
                    .download_to_directory
                    .wrap(prepare_action_inputs(
                        &self.running_actions_manager.directory_cache,
                        &self.running_actions_manager.cas_store,
                        filesystem_store_pin,
                        &self.action_info.input_root_digest,
                        &self.work_directory,
                        self.hint_root.clone(),
                        &self.running_actions_manager.path_digest_cache,
                        self.running_actions_manager.digest_checked_hint_link,
                    ))
                    .await
            })
            .await?;
            command
        };
        {
            // Create all directories needed for our output paths. This is required by the bazel spec.
            let prepare_output_directories = |output_file| {
                let full_output_path = if command.working_directory.is_empty() {
                    format!("{}/{}", self.work_directory, output_file)
                } else {
                    format!(
                        "{}/{}/{}",
                        self.work_directory, command.working_directory, output_file
                    )
                };
                async move {
                    let full_parent_path = Path::new(&full_output_path)
                        .parent()
                        .err_tip(|| format!("Parent path for {full_output_path} has no parent"))?;
                    fs::create_dir_all(full_parent_path).await.err_tip(|| {
                        format!(
                            "Error creating output directory {} (file)",
                            full_parent_path.display()
                        )
                    })?;
                    Result::<(), Error>::Ok(())
                }
            };
            self.metrics()
                .prepare_output_files
                .wrap(try_join_all(
                    command.output_files.iter().map(prepare_output_directories),
                ))
                .await?;
            self.metrics()
                .prepare_output_paths
                .wrap(try_join_all(
                    command.output_paths.iter().map(prepare_output_directories),
                ))
                .await?;
        }
        debug!(?command, "Worker received command");
        {
            let mut state = self.state.lock();
            state.command_proto = Some(command);
            state.execution_metadata.input_fetch_completed_timestamp =
                (self.running_actions_manager.callbacks.now_fn)();
        }
        Ok(self)
    }

    async fn inner_execute(self: Arc<Self>) -> Result<Arc<Self>, Error> {
        let _t = ACTION_EXECUTE.timer();
        let (command_proto, mut kill_channel_rx) = {
            let mut state = self.state.lock();
            state.execution_metadata.execution_start_timestamp =
                (self.running_actions_manager.callbacks.now_fn)();
            (
                state
                    .command_proto
                    .take()
                    .err_tip(|| "Expected state to have command_proto in execute()")?,
                state
                    .kill_channel_rx
                    .take()
                    .err_tip(|| "Expected state to have kill_channel_rx in execute()")?
                    // This is important as we may be killed at any point.
                    .fuse(),
            )
        };
        if command_proto.arguments.is_empty() {
            return Err(make_input_err!("No arguments provided in Command proto"));
        }
        let args: Vec<&OsStr> = if let Some(entrypoint) = &self
            .running_actions_manager
            .execution_configuration
            .entrypoint
        {
            core::iter::once(entrypoint.as_ref())
                .chain(command_proto.arguments.iter().map(AsRef::as_ref))
                .collect()
        } else {
            command_proto.arguments.iter().map(AsRef::as_ref).collect()
        };
        // TODO(palfrey): This should probably be in debug, but currently
        //                    that's too busy and we often rely on this to
        //                    figure out toolchain misconfiguration issues.
        //                    De-bloat the `debug` level by using the `trace`
        //                    level more effectively and adjust this.
        info!(?args, "Executing command",);
        let mut command_builder = process::Command::new(args[0]);
        command_builder
            .args(&args[1..])
            .kill_on_drop(true)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .current_dir(format!(
                "{}/{}",
                self.work_directory, command_proto.working_directory
            ))
            .env_clear();

        let requested_timeout = if self.action_info.timeout.is_zero() {
            self.running_actions_manager.max_action_timeout
        } else {
            self.action_info.timeout
        };

        let mut maybe_side_channel_file: Option<Cow<'_, OsStr>> = None;
        if let Some(additional_environment) = &self
            .running_actions_manager
            .execution_configuration
            .additional_environment
        {
            for (name, source) in additional_environment {
                let value = match source {
                    EnvironmentSource::Property(property) => self
                        .action_info
                        .platform_properties
                        .get(property)
                        .map_or_else(|| Cow::Borrowed(""), |v| Cow::Borrowed(v.as_str())),
                    EnvironmentSource::Value(value) => Cow::Borrowed(value.as_str()),
                    EnvironmentSource::FromEnvironment => {
                        Cow::Owned(env::var(name).unwrap_or_default())
                    }
                    EnvironmentSource::TimeoutMillis => {
                        Cow::Owned(requested_timeout.as_millis().to_string())
                    }
                    EnvironmentSource::SideChannelFile => {
                        let file_cow =
                            format!("{}/{}", self.action_directory, Uuid::new_v4().simple());
                        maybe_side_channel_file = Some(Cow::Owned(file_cow.clone().into()));
                        Cow::Owned(file_cow)
                    }
                    EnvironmentSource::ActionDirectory => {
                        Cow::Borrowed(self.action_directory.as_str())
                    }
                };
                command_builder.env(name, value.as_ref());
            }
        }

        #[cfg(target_family = "unix")]
        let envs = &command_proto.environment_variables;
        // If SystemRoot is not set on windows we set it to default. Failing to do
        // this causes all commands to fail.
        #[cfg(target_family = "windows")]
        let envs = {
            let mut envs = command_proto.environment_variables.clone();
            if !envs.iter().any(|v| v.name.to_uppercase() == "SYSTEMROOT") {
                envs.push(
                    nativelink_proto::build::bazel::remote::execution::v2::command::EnvironmentVariable {
                        name: "SystemRoot".to_string(),
                        value: "C:\\Windows".to_string(),
                    },
                );
            }
            if !envs.iter().any(|v| v.name.to_uppercase() == "PATH") {
                envs.push(
                    nativelink_proto::build::bazel::remote::execution::v2::command::EnvironmentVariable {
                        name: "PATH".to_string(),
                        value: "C:\\Windows\\System32".to_string(),
                    },
                );
            }
            envs
        };
        for environment_variable in envs {
            command_builder.env(&environment_variable.name, &environment_variable.value);
        }

        let mut child_process = command_builder
            .spawn()
            .err_tip(|| format!("Could not execute command {args:?}"))?;
        let mut stdout_reader = child_process
            .stdout
            .take()
            .err_tip(|| "Expected stdout to exist on command this should never happen")?;
        let mut stderr_reader = child_process
            .stderr
            .take()
            .err_tip(|| "Expected stderr to exist on command this should never happen")?;

        let mut child_process_guard = guard(child_process, |mut child_process| {
            let result: Result<Option<std::process::ExitStatus>, std::io::Error> =
                child_process.try_wait();
            match result {
                Ok(res) if res.is_some() => {
                    // The child already exited, probably a timeout or kill operation
                }
                result => {
                    error!(
                        ?result,
                        "Child process was not cleaned up before dropping the call to execute(), killing in background spawn."
                    );
                    background_spawn!("running_actions_manager_kill_child_process", async move {
                        child_process.kill().await
                    });
                }
            }
        });

        let all_stdout_fut = spawn!("stdout_reader", async move {
            let mut all_stdout = BytesMut::new();
            loop {
                let sz = stdout_reader
                    .read_buf(&mut all_stdout)
                    .await
                    .err_tip(|| "Error reading stdout stream")?;
                if sz == 0 {
                    break; // EOF.
                }
            }
            Result::<Bytes, Error>::Ok(all_stdout.freeze())
        });
        let all_stderr_fut = spawn!("stderr_reader", async move {
            let mut all_stderr = BytesMut::new();
            loop {
                let sz = stderr_reader
                    .read_buf(&mut all_stderr)
                    .await
                    .err_tip(|| "Error reading stderr stream")?;
                if sz == 0 {
                    break; // EOF.
                }
            }
            Result::<Bytes, Error>::Ok(all_stderr.freeze())
        });
        let mut killed_action = false;

        let timer = self.metrics().child_process.begin_timer();
        let mut sleep_fut = (self.running_actions_manager.callbacks.sleep_fn)(self.timeout).fuse();
        loop {
            tokio::select! {
                () = &mut sleep_fut => {
                    self.running_actions_manager.metrics.task_timeouts.inc();
                    killed_action = true;
                    if let Err(err) = child_process_guard.kill().await {
                        error!(
                            ?err,
                            "Could not kill process in RunningActionsManager for action timeout",
                        );
                    }
                    {
                        let joined_command = args.join(OsStr::new(" "));
                        let command = joined_command.to_string_lossy();
                        info!(
                            seconds = self.action_info.timeout.as_secs_f32(),
                            %command,
                            "Command timed out"
                        );
                        let mut state = self.state.lock();
                        state.error = Error::merge_option(state.error.take(), Some(Error::new(
                            Code::DeadlineExceeded,
                            format!(
                                "Command '{}' timed out after {} seconds",
                                command,
                                self.action_info.timeout.as_secs_f32()
                            )
                        )));
                    }
                },
                maybe_exit_status = child_process_guard.wait() => {
                    // Defuse our guard so it does not try to cleanup and make senseless logs.
                    drop(ScopeGuard::<_, _>::into_inner(child_process_guard));
                    let exit_status = maybe_exit_status.err_tip(|| "Failed to collect exit code of process")?;
                    // TODO(palfrey) We should implement stderr/stdout streaming to client here.
                    // If we get killed before the stream is started, then these will lock up.
                    // TODO(palfrey) There is a significant bug here. If we kill the action and the action creates
                    // child processes, it can create zombies. See: https://github.com/tracemachina/nativelink/issues/225
                    let (stdout, stderr) = if killed_action {
                        drop(timer);
                        (Bytes::new(), Bytes::new())
                    } else {
                        timer.measure();
                        let (maybe_all_stdout, maybe_all_stderr) = tokio::join!(all_stdout_fut, all_stderr_fut);
                        (
                            maybe_all_stdout.err_tip(|| "Internal error reading from stdout of worker task")??,
                            maybe_all_stderr.err_tip(|| "Internal error reading from stderr of worker task")??
                        )
                    };

                    let exit_code = exit_status.code().map_or(EXIT_CODE_FOR_SIGNAL, |exit_code| {
                        if exit_code == 0 {
                            self.metrics().child_process_success_error_code.inc();
                        } else {
                            self.metrics().child_process_failure_error_code.inc();
                        }
                        exit_code
                    });

                    info!(?args, "Command complete");

                    let maybe_error_override = if let Some(side_channel_file) = maybe_side_channel_file {
                        process_side_channel_file(side_channel_file.clone(), &args, requested_timeout).await
                        .err_tip(|| format!("Error processing side channel file: {}", side_channel_file.display()))?
                    } else {
                        None
                    };
                    {
                        let mut state = self.state.lock();
                        state.error = Error::merge_option(state.error.take(), maybe_error_override);

                        state.command_proto = Some(command_proto);
                        state.execution_result = Some(RunningActionImplExecutionResult{
                            stdout,
                            stderr,
                            exit_code,
                        });
                        state.execution_metadata.execution_completed_timestamp = (self.running_actions_manager.callbacks.now_fn)();
                    }
                    return Ok(self);
                },
                _ = &mut kill_channel_rx => {
                    killed_action = true;
                    if let Err(err) = child_process_guard.kill().await {
                        error!(
                            operation_id = ?self.operation_id,
                            ?err,
                            "Could not kill process",
                        );
                    }
                    {
                        let mut state = self.state.lock();
                        state.error = Error::merge_option(state.error.take(), Some(Error::new(
                            Code::Aborted,
                            format!(
                                "Command '{}' was killed by scheduler",
                                args.join(OsStr::new(" ")).to_string_lossy()
                            )
                        )));
                    }
                },
            }
        }
        // Unreachable.
    }

    async fn inner_upload_results(self: Arc<Self>) -> Result<Arc<Self>, Error> {
        let _t = UPLOAD_RESULTS.timer();
        enum OutputType {
            None,
            File(FileInfo),
            Directory(DirectoryInfo),
            FileSymlink(SymlinkInfo),
            DirectorySymlink(SymlinkInfo),
        }

        let upload_start = std::time::Instant::now();
        debug!(
            operation_id = ?self.operation_id,
            "Worker uploading results - starting",
        );
        let (mut command_proto, execution_result, mut execution_metadata) = {
            let mut state = self.state.lock();
            state.execution_metadata.output_upload_start_timestamp =
                (self.running_actions_manager.callbacks.now_fn)();
            (
                state
                    .command_proto
                    .take()
                    .err_tip(|| "Expected state to have command_proto in execute()")?,
                state
                    .execution_result
                    .take()
                    .err_tip(|| "Execution result does not exist at upload_results stage")?,
                state.execution_metadata.clone(),
            )
        };
        let cas_store = self.running_actions_manager.cas_store.as_ref();
        let hasher = self.action_info.unique_qualifier.digest_function();

        let mut output_path_futures = FuturesUnordered::new();
        let mut output_paths = command_proto.output_paths;
        if output_paths.is_empty() {
            output_paths
                .reserve(command_proto.output_files.len() + command_proto.output_directories.len());
            output_paths.append(&mut command_proto.output_files);
            output_paths.append(&mut command_proto.output_directories);
        }
        let digest_uploaders = Arc::new(Mutex::new(HashMap::new()));
        for entry in output_paths {
            let full_path = OsString::from(if command_proto.working_directory.is_empty() {
                format!("{}/{}", self.work_directory, entry)
            } else {
                format!(
                    "{}/{}/{}",
                    self.work_directory, command_proto.working_directory, entry
                )
            });
            let work_directory = &self.work_directory;
            let digest_uploaders = digest_uploaders.clone();
            output_path_futures.push(async move {
                let metadata = {
                    let metadata = match fs::symlink_metadata(&full_path).await {
                        Ok(file) => file,
                        Err(e) => {
                            if e.code == Code::NotFound {
                                // In the event our output does not exist, according to the bazel remote
                                // execution spec, we simply ignore it continue.
                                return Result::<OutputType, Error>::Ok(OutputType::None);
                            }
                            return Err(e).err_tip(|| {
                                format!("Could not open file {}", full_path.display())
                            });
                        }
                    };

                    if metadata.is_file() {
                        return Ok(OutputType::File(
                            upload_file(
                                cas_store.as_pin(),
                                &full_path,
                                hasher,
                                metadata,
                                digest_uploaders,
                            )
                            .await
                            .map(|mut file_info| {
                                file_info.name_or_path = NameOrPath::Path(entry);
                                file_info
                            })
                            .err_tip(|| format!("Uploading file {}", full_path.display()))?,
                        ));
                    }
                    metadata
                };
                if metadata.is_dir() {
                    Ok(OutputType::Directory(
                        upload_directory(
                            cas_store.as_pin(),
                            &full_path,
                            work_directory,
                            hasher,
                            digest_uploaders,
                        )
                        .and_then(|(root_dir, children)| async move {
                            let tree = ProtoTree {
                                root: Some(root_dir),
                                children: children.into(),
                            };
                            let tree_digest = serialize_and_upload_message(
                                &tree,
                                cas_store.as_pin(),
                                &mut hasher.hasher(),
                            )
                            .await
                            .err_tip(|| format!("While processing {entry}"))?;
                            Ok(DirectoryInfo {
                                path: entry,
                                tree_digest,
                            })
                        })
                        .await
                        .err_tip(|| format!("Uploading directory {}", full_path.display()))?,
                    ))
                } else if metadata.is_symlink() {
                    let output_symlink = upload_symlink(&full_path, work_directory)
                        .await
                        .map(|mut symlink_info| {
                            symlink_info.name_or_path = NameOrPath::Path(entry);
                            symlink_info
                        })
                        .err_tip(|| format!("Uploading symlink {}", full_path.display()))?;
                    match fs::metadata(&full_path).await {
                        Ok(metadata) => {
                            if metadata.is_dir() {
                                Ok(OutputType::DirectorySymlink(output_symlink))
                            } else {
                                // Note: If it's anything but directory we put it as a file symlink.
                                Ok(OutputType::FileSymlink(output_symlink))
                            }
                        }
                        Err(e) => {
                            if e.code != Code::NotFound {
                                return Err(e).err_tip(|| {
                                    format!(
                                        "While querying target symlink metadata for {}",
                                        full_path.display()
                                    )
                                });
                            }
                            // If the file doesn't exist, we consider it a file. Even though the
                            // file doesn't exist we still need to populate an entry.
                            Ok(OutputType::FileSymlink(output_symlink))
                        }
                    }
                } else {
                    Err(make_err!(
                        Code::Internal,
                        "{full_path:?} was not a file, folder or symlink. Must be one.",
                    ))
                }
            });
        }
        let mut output_files = vec![];
        let mut output_folders = vec![];
        let mut output_directory_symlinks = vec![];
        let mut output_file_symlinks = vec![];

        if execution_result.exit_code != 0 {
            let stdout = core::str::from_utf8(&execution_result.stdout).unwrap_or("<no-utf8>");
            let stderr = core::str::from_utf8(&execution_result.stderr).unwrap_or("<no-utf8>");
            error!(
                exit_code = ?execution_result.exit_code,
                stdout = ?stdout[..min(stdout.len(), 1000)],
                stderr = ?stderr[..min(stderr.len(), 1000)],
                "Command returned non-zero exit code",
            );
        }

        let stdout_digest_fut = self.metrics().upload_stdout.wrap(async {
            let start = std::time::Instant::now();
            let data = execution_result.stdout;
            let data_len = data.len();
            let digest = compute_buf_digest(&data, &mut hasher.hasher());
            cas_store
                .update_oneshot(digest, data)
                .await
                .err_tip(|| "Uploading stdout")?;
            debug!(
                ?digest,
                data_len,
                elapsed_ms = start.elapsed().as_millis(),
                "upload_results: stdout upload completed",
            );
            Result::<DigestInfo, Error>::Ok(digest)
        });
        let stderr_digest_fut = self.metrics().upload_stderr.wrap(async {
            let start = std::time::Instant::now();
            let data = execution_result.stderr;
            let data_len = data.len();
            let digest = compute_buf_digest(&data, &mut hasher.hasher());
            cas_store
                .update_oneshot(digest, data)
                .await
                .err_tip(|| "Uploading  stderr")?;
            debug!(
                ?digest,
                data_len,
                elapsed_ms = start.elapsed().as_millis(),
                "upload_results: stderr upload completed",
            );
            Result::<DigestInfo, Error>::Ok(digest)
        });

        debug!(
            operation_id = ?self.operation_id,
            num_output_paths = output_path_futures.len(),
            "upload_results: starting stdout/stderr/output_paths uploads",
        );
        let join_start = std::time::Instant::now();
        let upload_result = futures::try_join!(stdout_digest_fut, stderr_digest_fut, async {
            while let Some(output_type) = output_path_futures.try_next().await? {
                match output_type {
                    OutputType::File(output_file) => output_files.push(output_file),
                    OutputType::Directory(output_folder) => output_folders.push(output_folder),
                    OutputType::FileSymlink(output_symlink) => {
                        output_file_symlinks.push(output_symlink);
                    }
                    OutputType::DirectorySymlink(output_symlink) => {
                        output_directory_symlinks.push(output_symlink);
                    }
                    OutputType::None => { /* Safe to ignore */ }
                }
            }
            Ok(())
        });
        drop(output_path_futures);
        info!(
            operation_id = ?self.operation_id,
            elapsed_ms = join_start.elapsed().as_millis(),
            success = upload_result.is_ok(),
            "upload_results: all uploads completed",
        );
        let (stdout_digest, stderr_digest) = match upload_result {
            Ok((stdout_digest, stderr_digest, ())) => (stdout_digest, stderr_digest),
            Err(e) => return Err(e).err_tip(|| "Error while uploading results"),
        };

        execution_metadata.output_upload_completed_timestamp =
            (self.running_actions_manager.callbacks.now_fn)();
        output_files.sort_unstable_by(|a, b| a.name_or_path.cmp(&b.name_or_path));
        output_folders.sort_unstable_by(|a, b| a.path.cmp(&b.path));
        output_file_symlinks.sort_unstable_by(|a, b| a.name_or_path.cmp(&b.name_or_path));
        output_directory_symlinks.sort_unstable_by(|a, b| a.name_or_path.cmp(&b.name_or_path));

        let num_output_files = output_files.len();
        let num_output_folders = output_folders.len();
        let first_output_file_summary = output_files
            .first()
            .map(|f| format!("{:?} digest={:?}", f.name_or_path, f.digest));
        let last_output_file_summary = output_files
            .last()
            .map(|f| format!("{:?} digest={:?}", f.name_or_path, f.digest));
        {
            let mut state = self.state.lock();
            execution_metadata.worker_completed_timestamp =
                (self.running_actions_manager.callbacks.now_fn)();
            state.action_result = Some(ActionResult {
                output_files,
                output_folders,
                output_directory_symlinks,
                output_file_symlinks,
                exit_code: execution_result.exit_code,
                stdout_digest,
                stderr_digest,
                execution_metadata,
                server_logs: HashMap::default(), // TODO(palfrey) Not implemented.
                error: state.error.clone(),
                message: String::new(), // Will be filled in on cache_action_result if needed.
            });
        }
        info!(
            operation_id = ?self.operation_id,
            total_elapsed_ms = upload_start.elapsed().as_millis(),
            num_output_files,
            num_output_folders,
            first_output_file = first_output_file_summary,
            last_output_file = last_output_file_summary,
            "upload_results: inner_upload_results completed successfully",
        );
        Ok(self)
    }

    async fn inner_get_finished_result(self: Arc<Self>) -> Result<ActionResult, Error> {
        let mut state = self.state.lock();
        state
            .action_result
            .take()
            .err_tip(|| "Expected action_result to exist in get_finished_result")
    }
}

impl Drop for RunningActionImpl {
    fn drop(&mut self) {
        if self.did_cleanup.load(Ordering::Acquire) {
            if self.has_manager_entry.load(Ordering::Acquire) {
                drop(
                    self.running_actions_manager
                        .cleanup_action(&self.operation_id),
                );
            }
            return;
        }
        let operation_id = self.operation_id.clone();
        error!(
            %operation_id,
            "RunningActionImpl did not cleanup. This is a violation of the requirements, will attempt to do it in the background."
        );
        let running_actions_manager = self.running_actions_manager.clone();
        let action_directory = self.action_directory.clone();
        background_spawn!("running_action_impl_drop", async move {
            let Err(err) =
                do_cleanup(&running_actions_manager, &operation_id, &action_directory).await
            else {
                return;
            };
            error!(
                %operation_id,
                ?action_directory,
                ?err,
                "Error cleaning up action"
            );
        });
    }
}

impl RunningAction for RunningActionImpl {
    fn get_operation_id(&self) -> &OperationId {
        &self.operation_id
    }

    async fn prepare_action(self: Arc<Self>) -> Result<Arc<Self>, Error> {
        let res = self
            .metrics()
            .clone()
            .prepare_action
            .wrap(Self::inner_prepare_action(self))
            .await;
        if let Err(ref e) = res {
            warn!(?e, "Error during prepare_action");
        }
        res
    }

    async fn execute(self: Arc<Self>) -> Result<Arc<Self>, Error> {
        let res = self
            .metrics()
            .clone()
            .execute
            .wrap(Self::inner_execute(self))
            .await;
        if let Err(ref e) = res {
            warn!(?e, "Error during prepare_action");
        }
        res
    }

    async fn upload_results(self: Arc<Self>) -> Result<Arc<Self>, Error> {
        let upload_timeout = self.running_actions_manager.max_upload_timeout;
        let operation_id = self.operation_id.clone();
        info!(
            ?operation_id,
            upload_timeout_s = upload_timeout.as_secs(),
            "upload_results: starting with timeout",
        );
        let metrics = self.metrics().clone();
        let upload_fut = metrics
            .upload_results
            .wrap(Self::inner_upload_results(self));

        let stall_warn_fut = async {
            let mut elapsed_secs = 0u64;
            loop {
                tokio::time::sleep(Duration::from_secs(60)).await;
                elapsed_secs += 60;
                warn!(
                    ?operation_id,
                    elapsed_s = elapsed_secs,
                    timeout_s = upload_timeout.as_secs(),
                    "upload_results: still in progress — possible stall",
                );
            }
        };

        let res = tokio::time::timeout(upload_timeout, async {
            tokio::pin!(upload_fut);
            tokio::pin!(stall_warn_fut);
            tokio::select! {
                result = &mut upload_fut => result,
                () = &mut stall_warn_fut => unreachable!(),
            }
        })
        .await
        .map_err(|_| {
            make_err!(
                Code::DeadlineExceeded,
                "Upload results timed out after {}s for operation {:?}",
                upload_timeout.as_secs(),
                operation_id,
            )
        })?;
        if let Err(ref e) = res {
            warn!(?operation_id, ?e, "Error during upload_results");
        }
        res
    }

    async fn cleanup(self: Arc<Self>) -> Result<Arc<Self>, Error> {
        let res = self
            .metrics()
            .clone()
            .cleanup
            .wrap(async move {
                let result = do_cleanup(
                    &self.running_actions_manager,
                    &self.operation_id,
                    &self.action_directory,
                )
                .await;
                self.has_manager_entry.store(false, Ordering::Release);
                self.did_cleanup.store(true, Ordering::Release);
                result.map(move |()| self)
            })
            .await;
        if let Err(ref e) = res {
            warn!(?e, "Error during cleanup");
        }
        res
    }

    async fn get_finished_result(self: Arc<Self>) -> Result<ActionResult, Error> {
        self.metrics()
            .clone()
            .get_finished_result
            .wrap(Self::inner_get_finished_result(self))
            .await
    }

    fn get_work_directory(&self) -> &String {
        &self.work_directory
    }
}

pub trait RunningActionsManager: Sync + Send + Sized + Unpin + 'static {
    type RunningAction: RunningAction;

    fn create_and_add_action(
        self: &Arc<Self>,
        worker_id: String,
        start_execute: StartExecute,
    ) -> impl Future<Output = Result<Arc<Self::RunningAction>, Error>> + Send;

    fn cache_action_result(
        &self,
        action_digest: DigestInfo,
        action_result: &mut ActionResult,
        hasher: DigestHasherFunc,
    ) -> impl Future<Output = Result<(), Error>> + Send;

    fn kill_all(&self) -> impl Future<Output = ()> + Send;

    fn kill_operation(
        &self,
        operation_id: &OperationId,
    ) -> impl Future<Output = Result<(), Error>> + Send;

    fn metrics(&self) -> &Arc<Metrics>;
}

/// A function to get the current system time, used to allow mocking for tests
type NowFn = fn() -> SystemTime;
type SleepFn = fn(Duration) -> BoxFuture<'static, ()>;

/// Functions that may be injected for testing purposes, during standard control
/// flows these are specified by the new function.
#[derive(Clone, Copy)]
pub struct Callbacks {
    /// A function that gets the current time.
    pub now_fn: NowFn,
    /// A function that sleeps for a given Duration.
    pub sleep_fn: SleepFn,
}

impl Debug for Callbacks {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Callbacks").finish_non_exhaustive()
    }
}

/// The set of additional information for executing an action over and above
/// those given in the `ActionInfo` passed to the worker.  This allows
/// modification of the action for execution on this particular worker.  This
/// may be used to run the action with a particular set of additional
/// environment variables, or perhaps configure it to execute within a
/// container.
#[derive(Debug, Default)]
pub struct ExecutionConfiguration {
    /// If set, will be executed instead of the first argument passed in the
    /// `ActionInfo` with all of the arguments in the `ActionInfo` passed as
    /// arguments to this command.
    pub entrypoint: Option<String>,
    /// The only environment variables that will be specified when the command
    /// executes other than those in the `ActionInfo`.  On Windows, `SystemRoot`
    /// and PATH are also assigned (see `inner_execute`).
    pub additional_environment: Option<HashMap<String, EnvironmentSource>>,
}

#[derive(Debug)]
struct UploadActionResults {
    upload_ac_results_strategy: UploadCacheResultsStrategy,
    upload_historical_results_strategy: UploadCacheResultsStrategy,
    ac_store: Option<Store>,
    historical_store: Store,
    success_message_template: Template,
    failure_message_template: Template,
}

impl UploadActionResults {
    fn new(
        config: &UploadActionResultConfig,
        ac_store: Option<Store>,
        historical_store: Store,
    ) -> Result<Self, Error> {
        let upload_historical_results_strategy = config
            .upload_historical_results_strategy
            .unwrap_or(DEFAULT_HISTORICAL_RESULTS_STRATEGY);
        if !matches!(
            config.upload_ac_results_strategy,
            UploadCacheResultsStrategy::Never
        ) && ac_store.is_none()
        {
            return Err(make_input_err!(
                "upload_ac_results_strategy is set, but no ac_store is configured"
            ));
        }
        Ok(Self {
            upload_ac_results_strategy: config.upload_ac_results_strategy,
            upload_historical_results_strategy,
            ac_store,
            historical_store,
            success_message_template: Template::new(&config.success_message_template).map_err(
                |e| {
                    make_input_err!(
                        "Could not convert success_message_template to rust template: {} : {e:?}",
                        config.success_message_template
                    )
                },
            )?,
            failure_message_template: Template::new(&config.failure_message_template).map_err(
                |e| {
                    make_input_err!(
                        "Could not convert failure_message_template to rust template: {} : {e:?}",
                        config.success_message_template
                    )
                },
            )?,
        })
    }

    const fn should_cache_result(
        strategy: UploadCacheResultsStrategy,
        action_result: &ActionResult,
        treat_infra_error_as_failure: bool,
    ) -> bool {
        let did_fail = action_result.exit_code != 0
            || (treat_infra_error_as_failure && action_result.error.is_some());
        match strategy {
            UploadCacheResultsStrategy::SuccessOnly => !did_fail,
            UploadCacheResultsStrategy::Never => false,
            // Never cache internal errors or timeouts.
            UploadCacheResultsStrategy::Everything => {
                treat_infra_error_as_failure || action_result.error.is_none()
            }
            UploadCacheResultsStrategy::FailuresOnly => did_fail,
        }
    }

    /// Formats the message field in `ExecuteResponse` from the `success_message_template`
    /// or `failure_message_template` config templates.
    fn format_execute_response_message(
        mut template_str: Template,
        action_digest_info: DigestInfo,
        maybe_historical_digest_info: Option<DigestInfo>,
        hasher: DigestHasherFunc,
    ) -> Result<String, Error> {
        template_str.replace(
            "digest_function",
            hasher.proto_digest_func().as_str_name().to_lowercase(),
        );
        template_str.replace(
            "action_digest_hash",
            action_digest_info.packed_hash().to_string(),
        );
        template_str.replace("action_digest_size", action_digest_info.size_bytes());
        if let Some(historical_digest_info) = maybe_historical_digest_info {
            template_str.replace(
                "historical_results_hash",
                format!("{}", historical_digest_info.packed_hash()),
            );
            template_str.replace(
                "historical_results_size",
                historical_digest_info.size_bytes(),
            );
        } else {
            template_str.replace("historical_results_hash", "");
            template_str.replace("historical_results_size", "");
        }
        template_str
            .text()
            .map_err(|e| make_input_err!("Could not convert template to text: {e:?}"))
    }

    async fn upload_ac_results(
        &self,
        action_digest: DigestInfo,
        action_result: ProtoActionResult,
        hasher: DigestHasherFunc,
    ) -> Result<(), Error> {
        let Some(ac_store) = self.ac_store.as_ref() else {
            return Ok(());
        };
        // If we are a GrpcStore we shortcut here, as this is a special store.
        if let Some(grpc_store) = ac_store.downcast_ref::<GrpcStore>(Some(action_digest.into())) {
            let update_action_request = UpdateActionResultRequest {
                // This is populated by `update_action_result`.
                instance_name: String::new(),
                action_digest: Some(action_digest.into()),
                action_result: Some(action_result),
                results_cache_policy: None,
                digest_function: hasher.proto_digest_func().into(),
            };
            return grpc_store
                .update_action_result(Request::new(update_action_request))
                .await
                .map(|_| ())
                .err_tip(|| "Caching ActionResult");
        }

        let mut store_data = BytesMut::with_capacity(ESTIMATED_DIGEST_SIZE);
        action_result
            .encode(&mut store_data)
            .err_tip(|| "Encoding ActionResult for caching")?;

        ac_store
            .update_oneshot(action_digest, store_data.split().freeze())
            .await
            .err_tip(|| "Caching ActionResult")
    }

    async fn upload_historical_results_with_message(
        &self,
        action_digest: DigestInfo,
        execute_response: ExecuteResponse,
        message_template: Template,
        hasher: DigestHasherFunc,
    ) -> Result<String, Error> {
        let historical_digest_info = serialize_and_upload_message(
            &HistoricalExecuteResponse {
                action_digest: Some(action_digest.into()),
                execute_response: Some(execute_response.clone()),
            },
            self.historical_store.as_pin(),
            &mut hasher.hasher(),
        )
        .await
        .err_tip(|| format!("Caching HistoricalExecuteResponse for digest: {action_digest}"))?;

        Self::format_execute_response_message(
            message_template,
            action_digest,
            Some(historical_digest_info),
            hasher,
        )
        .err_tip(|| "Could not format message in upload_historical_results_with_message")
    }

    async fn cache_action_result(
        &self,
        action_info: DigestInfo,
        action_result: &mut ActionResult,
        hasher: DigestHasherFunc,
    ) -> Result<(), Error> {
        let should_upload_historical_results =
            Self::should_cache_result(self.upload_historical_results_strategy, action_result, true);
        let should_upload_ac_results =
            Self::should_cache_result(self.upload_ac_results_strategy, action_result, false);
        // Shortcut so we don't need to convert to proto if not needed.
        if !should_upload_ac_results && !should_upload_historical_results {
            return Ok(());
        }

        let mut execute_response = to_execute_response(action_result.clone());

        // In theory exit code should always be != 0 if there's an error, but for safety we
        // catch both.
        let message_template = if action_result.exit_code == 0 && action_result.error.is_none() {
            self.success_message_template.clone()
        } else {
            self.failure_message_template.clone()
        };

        let upload_historical_results_with_message_result = if should_upload_historical_results {
            let maybe_message = self
                .upload_historical_results_with_message(
                    action_info,
                    execute_response.clone(),
                    message_template,
                    hasher,
                )
                .await;
            match maybe_message {
                Ok(message) => {
                    action_result.message.clone_from(&message);
                    execute_response.message = message;
                    Ok(())
                }
                Err(e) => Result::<(), Error>::Err(e),
            }
        } else {
            match Self::format_execute_response_message(message_template, action_info, None, hasher)
            {
                Ok(message) => {
                    action_result.message.clone_from(&message);
                    execute_response.message = message;
                    Ok(())
                }
                Err(e) => Err(e).err_tip(|| "Could not format message in cache_action_result"),
            }
        };

        // Note: Done in this order because we assume most results will succeed and most configs will
        // either always upload upload historical results or only upload on filure. In which case
        // we can avoid an extra clone of the protos by doing this last with the above assumption.
        let ac_upload_results = if should_upload_ac_results {
            self.upload_ac_results(
                action_info,
                execute_response
                    .result
                    .err_tip(|| "No result set in cache_action_result")?,
                hasher,
            )
            .await
        } else {
            Ok(())
        };
        upload_historical_results_with_message_result.merge(ac_upload_results)
    }
}

pub use nativelink_config::cas_server::ProjectRoot;

/// Translate an absolute path carried by an action's
/// `InputRootAbsolutePath` platform property onto the local filesystem,
/// according to `project_root`. Returns the path to be used by the
/// worker for `work_directory` / `hint_root`.
///
/// Contract (documented exhaustively by unit tests in this module):
///
/// * `project_root = None` → identity (legacy behavior).
/// * `raw.is_empty()` → identity (preserves the Plan J
///   `action_directory/work` fallback at the call site).
/// * `raw.starts_with(in_action)` **at a path boundary** → returns
///   `on_disk + raw[in_action.len()..]`. "At a path boundary" means
///   `raw == in_action` or `raw[in_action.len()] == '/'`; it prevents
///   false-matches like `/Users/octoOther` matching `/Users/octo`.
/// * `in_action` equal to `on_disk` → identity (symmetric config for
///   workers that share the origin's filesystem layout, e.g. `.132`).
/// * `raw` does not start with `in_action` (after trailing-slash
///   normalization) → identity; unrelated paths are left untouched.
///
/// The helper is a pure string operation; it performs no I/O and does
/// not canonicalize (the inputs are expected to be already-absolute
/// paths as advertised by siso).
#[must_use]
pub fn translate_input_root_path(raw: &str, project_root: Option<&ProjectRoot>) -> String {
    let Some(pr) = project_root else {
        return raw.to_string();
    };
    if raw.is_empty() {
        return raw.to_string();
    }
    // Normalize trailing slash on `in_action` so operators can enter
    // the path in either form. `on_disk` keeps its form — whatever
    // trailing shape it has will appear at the start of the result.
    let in_action = pr.in_action.strip_suffix('/').unwrap_or(&pr.in_action);
    // Match at a path boundary: `raw == in_action`, or `raw[len] == '/'`.
    // This rejects `/Users/octoOther` matching `/Users/octo`.
    if let Some(suffix) = raw.strip_prefix(in_action) {
        if suffix.is_empty() || suffix.starts_with('/') {
            let on_disk = pr.on_disk.strip_suffix('/').unwrap_or(&pr.on_disk);
            return format!("{on_disk}{suffix}");
        }
    }
    raw.to_string()
}

#[derive(Debug)]
pub struct RunningActionsManagerArgs<'a> {
    pub root_action_directory: String,
    pub execution_configuration: ExecutionConfiguration,
    pub cas_store: Arc<FastSlowStore>,
    pub ac_store: Option<Store>,
    pub historical_store: Store,
    pub upload_action_result_config: &'a UploadActionResultConfig,
    pub max_action_timeout: Duration,
    pub max_upload_timeout: Duration,
    pub timeout_handled_externally: bool,
    pub directory_cache: Option<Arc<crate::directory_cache::DirectoryCache>>,
    /// Optional Redis URL for persistent walked-dirs cache.
    pub shared_walked_dirs_redis_url: Option<String>,
    /// Machine identifier for Redis key namespacing (e.g. IP address).
    pub machine_id: String,
    /// Optional shared-tree root (typically matches the
    /// `InputRootAbsolutePath` platform property). When set together with
    /// `shared_walked_dirs_redis_url`, the worker spawns a background task
    /// that drains `pending_outputs` on a 1s timer and materializes files
    /// into this directory. This ensures outputs from other workers land
    /// on local disk even when no remote action is dispatched here —
    /// required for siso-local link/SOLINK steps on the scheduler machine.
    pub shared_tree_path: Option<String>,
    /// Per-worker remap of action-borne `InputRootAbsolutePath` onto the
    /// local filesystem. `None` (the default) preserves legacy behavior
    /// — the raw absolute path from the platform property is used
    /// directly for `work_directory` and `hint_root`.
    pub project_root: Option<ProjectRoot>,
    /// Experimental: when true, `download_to_directory` checks the
    /// pre-staged on-disk file at `hint_root/<name>` against the
    /// expected digest before hitting CAS. On match → hardlink-from-disk
    /// (Plan I). On miss → falls through to CAS unchanged.
    pub digest_checked_hint_link: bool,
    /// Optional process-level shared path->digest map (Plan K). When
    /// `Some`, every `RunningActionsManagerImpl` constructed with the
    /// same `SharedPathDigestMap` shares the in-memory Plan K state so
    /// the first worker to verify a (path, digest) pair primes the
    /// cache for every other worker in the same nativelink process.
    /// When `None`, each manager constructs its own per-instance map.
    ///
    /// The walked-dirs provider (Plan L) is **not** carried by this
    /// field — each manager builds its own provider from
    /// `shared_walked_dirs_redis_url` / `machine_id`, so injecting a
    /// shared map does not silently override per-worker Redis-backed
    /// Plan L sharing.
    pub path_digest_cache: Option<crate::path_digest_cache::SharedPathDigestMap>,
    /// Optional process-level shared single-flight registry. When
    /// `Some`, concurrent `download_to_directory(path, digest)` calls
    /// across every manager in the process collapse to one underlying
    /// walk. Without this, N concurrent actions on a cold cache each
    /// independently re-walk and re-hash the entire input tree
    /// (the `cold_walk × N` problem we hit in 1.3.0/1.3.1). When
    /// `None`, each manager builds its own coalescer (deduplicates
    /// only within that manager).
    pub dir_walk_coalescer: Option<crate::path_digest_cache::SharedDirWalkCoalescer>,
}

struct CleanupGuard {
    manager: Weak<RunningActionsManagerImpl>,
    operation_id: OperationId,
}

impl Drop for CleanupGuard {
    fn drop(&mut self) {
        let Some(manager) = self.manager.upgrade() else {
            return;
        };
        let mut cleaning = manager.cleaning_up_operations.lock();
        cleaning.remove(&self.operation_id);
        manager.cleanup_complete_notify.notify_waiters();
    }
}

/// Holds state info about what is being executed and the interface for interacting
/// with actions while they are running.
#[derive(Debug)]
pub struct RunningActionsManagerImpl {
    root_action_directory: String,
    execution_configuration: ExecutionConfiguration,
    cas_store: Arc<FastSlowStore>,
    filesystem_store: Arc<FilesystemStore>,
    upload_action_results: UploadActionResults,
    max_action_timeout: Duration,
    max_upload_timeout: Duration,
    timeout_handled_externally: bool,
    running_actions: Mutex<HashMap<OperationId, Weak<RunningActionImpl>>>,
    // Note: We don't use Notify because we need to support a .wait_for()-like function, which
    // Notify does not support.
    action_done_tx: watch::Sender<()>,
    callbacks: Callbacks,
    metrics: Arc<Metrics>,
    /// Track operations being cleaned up to avoid directory collisions during action retries.
    /// When an action fails and is retried on the same worker, we need to ensure the previous
    /// attempt's directory is fully cleaned up before creating a new one.
    /// See: <https://github.com/TraceMachina/nativelink/issues/1859>
    cleaning_up_operations: Mutex<HashSet<OperationId>>,
    /// Notify waiters when a cleanup operation completes. This is used in conjunction with
    /// `cleaning_up_operations` to coordinate directory cleanup and creation.
    cleanup_complete_notify: Arc<Notify>,
    /// Optional directory cache for improving performance by caching reconstructed
    /// input directories and using hardlinks.
    directory_cache: Option<Arc<crate::directory_cache::DirectoryCache>>,
    /// Per-worker path->digest cache. Shared across all concurrent actions so
    /// that verified (path, digest) pairs and walked Directory subtrees are
    /// remembered for subsequent actions.
    path_digest_cache: Arc<crate::path_digest_cache::PathDigestCache>,
    /// Optional remap of action-borne `InputRootAbsolutePath` onto the
    /// local filesystem. Consulted at `work_directory` / `hint_root`
    /// derivation sites (Plan J / Plan I). `None` → identity.
    project_root: Option<ProjectRoot>,
    /// Experimental: gate for Plan I (digest-checked hint-path
    /// short-circuit). See `LocalWorkerConfig::experimental_digest_checked_hint_link`.
    pub(crate) digest_checked_hint_link: bool,
}

impl RunningActionsManagerImpl {
    /// Maximum time to wait for a cleanup operation to complete before timing out.
    /// TODO(marcussorealheis): Consider making cleanup wait timeout configurable in the future
    const MAX_WAIT: Duration = Duration::from_secs(30);
    /// Maximum backoff duration for exponential backoff when waiting for cleanup.
    const MAX_BACKOFF: Duration = Duration::from_millis(500);
    pub fn new_with_callbacks(
        args: RunningActionsManagerArgs<'_>,
        callbacks: Callbacks,
    ) -> Result<Self, Error> {
        // Sadly because of some limitations of how Any works we need to clone more times than optimal.
        let filesystem_store = args
            .cas_store
            .fast_store()
            .downcast_ref::<FilesystemStore>(None)
            .err_tip(
                || "Expected FilesystemStore store for .fast_store() in RunningActionsManagerImpl",
            )?
            .get_arc()
            .err_tip(|| "FilesystemStore's internal Arc was lost")?;
        let (action_done_tx, _) = watch::channel(());
        let this = Self {
            root_action_directory: args.root_action_directory,
            execution_configuration: args.execution_configuration,
            cas_store: args.cas_store,
            filesystem_store,
            upload_action_results: UploadActionResults::new(
                args.upload_action_result_config,
                args.ac_store,
                args.historical_store,
            )
            .err_tip(|| "During RunningActionsManagerImpl construction")?,
            max_action_timeout: args.max_action_timeout,
            max_upload_timeout: args.max_upload_timeout,
            timeout_handled_externally: args.timeout_handled_externally,
            running_actions: Mutex::new(HashMap::new()),
            action_done_tx,
            callbacks,
            metrics: Arc::new(Metrics::default()),
            cleaning_up_operations: Mutex::new(HashSet::new()),
            cleanup_complete_notify: Arc::new(Notify::new()),
            directory_cache: args.directory_cache,
            project_root: args.project_root,
            digest_checked_hint_link: args.digest_checked_hint_link,
            path_digest_cache: {
                // Plan L provider is always per-manager: each worker honors
                // its own `shared_walked_dirs_redis_url`/`machine_id` even
                // when the Plan K map is process-shared.
                let walked_dirs: Box<dyn crate::path_digest_cache::WalkedDirsProvider> =
                    if let Some(ref redis_url) = args.shared_walked_dirs_redis_url {
                        let machine_id = if args.machine_id.is_empty() {
                            "default"
                        } else {
                            &args.machine_id
                        };
                        let redis_walked_dirs = crate::path_digest_cache::RedisWalkedDirs::new(
                            redis_url, machine_id,
                        )?;
                        info!("Per-machine walked-dirs cache enabled via Redis: {redis_url}");
                        Box::new(redis_walked_dirs)
                    } else {
                        Box::new(crate::path_digest_cache::LocalWalkedDirs::new())
                    };
                let cache = match (args.path_digest_cache, args.dir_walk_coalescer) {
                    (Some(shared_map), Some(shared_coalescer)) => {
                        info!(
                            walked_dirs_kind = walked_dirs.kind(),
                            "Process-shared Plan K map + dir-walk coalescer injected",
                        );
                        crate::path_digest_cache::PathDigestCache::with_shared_state(
                            shared_map,
                            shared_coalescer,
                            walked_dirs,
                        )
                    }
                    (Some(shared_map), None) => {
                        info!(
                            walked_dirs_kind = walked_dirs.kind(),
                            "Process-shared Plan K map injected; coalescer is per-manager",
                        );
                        crate::path_digest_cache::PathDigestCache::with_shared_map_and_walked_dirs(
                            shared_map,
                            walked_dirs,
                        )
                    }
                    (None, _) => {
                        crate::path_digest_cache::PathDigestCache::with_walked_dirs(walked_dirs)
                    }
                };
                Arc::new(cache)
            },
        };

        // Periodic timing dump: log a sorted summary of every registered
        // stage's cumulative stats (count / total_ms / mean_us / max_ms),
        // a process user/sys CPU split, and per-dispatch counters. Sorted
        // by total_ms descending so the biggest time sinks are immediately
        // visible. Grep for "timing:stage" / "timing:counter" / "timing:cpu"
        // in the worker log. Interval defaults to 5s so that short
        // benchmark runs (e.g. a 30s hot rebuild) produce multiple
        // samples; override via `NATIVELINK_TIMING_DUMP_INTERVAL_SECS`
        // (clamped to [1, 3600]). Lives for the whole process lifetime.
        let dump_secs = std::env::var("NATIVELINK_TIMING_DUMP_INTERVAL_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .map(|v| v.clamp(1, 3600))
            .unwrap_or(5);
        info!(
            dump_interval_secs = dump_secs,
            "timing harness: periodic dump scheduled"
        );
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(dump_secs));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            // Skip the first immediate tick so the first dump isn't empty.
            interval.tick().await;
            loop {
                interval.tick().await;
                nativelink_util::timing::dump_to_tracing();
            }
        });

        Ok(this)
    }

    pub fn new(args: RunningActionsManagerArgs<'_>) -> Result<Self, Error> {
        Self::new_with_callbacks(
            args,
            Callbacks {
                now_fn: SystemTime::now,
                sleep_fn: |duration| Box::pin(tokio::time::sleep(duration)),
            },
        )
    }

    /// Returns the `PathDigestCache` (Plan K + Plan L) this manager is using.
    /// When a single `Arc<PathDigestCache>` is injected via
    /// `RunningActionsManagerArgs::path_digest_cache`, every manager built
    /// from that args returns an identity-equal `Arc` here, which lets a
    /// process-level owner verify that all `workers[]` entries truly share
    /// one in-memory cache.
    pub fn path_digest_cache(&self) -> &Arc<crate::path_digest_cache::PathDigestCache> {
        &self.path_digest_cache
    }

    /// Fixes a race condition that occurs when an action fails to execute on a worker, and the same worker
    /// attempts to re-execute the same action before the physical cleanup (file is removed) completes.
    /// See this issue for additional details: <https://github.com/TraceMachina/nativelink/issues/1859>
    async fn wait_for_cleanup_if_needed(&self, operation_id: &OperationId) -> Result<(), Error> {
        let start = Instant::now();
        let mut backoff = Duration::from_millis(10);
        let mut has_waited = false;

        loop {
            let should_wait = {
                let cleaning = self.cleaning_up_operations.lock();
                cleaning.contains(operation_id)
            };

            if !should_wait {
                let dir_path =
                    PathBuf::from(&self.root_action_directory).join(operation_id.to_string());

                if !dir_path.exists() {
                    return Ok(());
                }

                // Safety check: ensure we're only removing directories under root_action_directory
                let root_path = Path::new(&self.root_action_directory);
                let canonical_root = root_path.canonicalize().err_tip(|| {
                    format!(
                        "Failed to canonicalize root directory: {}",
                        self.root_action_directory
                    )
                })?;
                let canonical_dir = dir_path.canonicalize().err_tip(|| {
                    format!("Failed to canonicalize directory: {}", dir_path.display())
                })?;

                if !canonical_dir.starts_with(&canonical_root) {
                    return Err(make_err!(
                        Code::Internal,
                        "Attempted to remove directory outside of root_action_directory: {}",
                        dir_path.display()
                    ));
                }

                // Directory exists but not being cleaned - remove it
                warn!(
                    "Removing stale directory for {}: {}",
                    operation_id,
                    dir_path.display()
                );
                self.metrics.stale_removals.inc();

                // Try to remove the directory, with one retry on failure
                let remove_result = fs::remove_dir_all(&dir_path).await;
                if let Err(e) = remove_result {
                    // Retry once after a short delay in case the directory is temporarily locked
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    fs::remove_dir_all(&dir_path).await.err_tip(|| {
                        format!(
                            "Failed to remove stale directory {} for retry of {} after retry (original error: {})",
                            dir_path.display(),
                            operation_id,
                            e
                        )
                    })?;
                }
                return Ok(());
            }

            if start.elapsed() > Self::MAX_WAIT {
                self.metrics.cleanup_wait_timeouts.inc();
                return Err(make_err!(
                    Code::DeadlineExceeded,
                    "Timeout waiting for previous operation cleanup: {} (waited {:?})",
                    operation_id,
                    start.elapsed()
                ));
            }

            if !has_waited {
                self.metrics.cleanup_waits.inc();
                has_waited = true;
            }

            trace!(
                "Waiting for cleanup of {} (elapsed: {:?}, backoff: {:?})",
                operation_id,
                start.elapsed(),
                backoff
            );

            tokio::select! {
                () = self.cleanup_complete_notify.notified() => {},
                () = tokio::time::sleep(backoff) => {
                    // Exponential backoff
                    backoff = (backoff * 2).min(Self::MAX_BACKOFF);
                },
            }
        }
    }

    fn make_action_directory<'a>(
        &'a self,
        operation_id: &'a OperationId,
    ) -> impl Future<Output = Result<String, Error>> + 'a {
        self.metrics.make_action_directory.wrap(async move {
            let action_directory = format!("{}/{}", self.root_action_directory, operation_id);
            fs::create_dir(&action_directory)
                .await
                .err_tip(|| format!("Error creating action directory {action_directory}"))?;
            Ok(action_directory)
        })
    }

    fn create_action_info(
        &self,
        start_execute: StartExecute,
        queued_timestamp: SystemTime,
    ) -> impl Future<Output = Result<ActionInfo, Error>> + '_ {
        self.metrics.create_action_info.wrap(async move {
            let execute_request = start_execute
                .execute_request
                .err_tip(|| "Expected execute_request to exist in StartExecute")?;
            let action_digest: DigestInfo = execute_request
                .action_digest
                .clone()
                .err_tip(|| "Expected action_digest to exist on StartExecute")?
                .try_into()?;
            let load_start_timestamp = (self.callbacks.now_fn)();
            let action =
                get_and_decode_digest::<Action>(self.cas_store.as_ref(), action_digest.into())
                    .await
                    .err_tip(|| "During start_action")?;
            let action_info = ActionInfo::try_from_action_and_execute_request(
                execute_request,
                action,
                load_start_timestamp,
                queued_timestamp,
            )
            .err_tip(|| "Could not create ActionInfo in create_and_add_action()")?;
            Ok(action_info)
        })
    }

    fn cleanup_action(&self, operation_id: &OperationId) -> Result<(), Error> {
        let mut running_actions = self.running_actions.lock();
        let result = running_actions.remove(operation_id).err_tip(|| {
            format!("Expected operation id '{operation_id}' to exist in RunningActionsManagerImpl")
        });
        // No need to copy anything, we just are telling the receivers an event happened.
        self.action_done_tx.send_modify(|()| {});
        result.map(|_| ())
    }

    // Note: We do not capture metrics on this call, only `.kill_all()`.
    // Important: When the future returns the process may still be running.
    async fn kill_operation(action: Arc<RunningActionImpl>) {
        warn!(
            operation_id = ?action.operation_id,
            "Sending kill to running operation",
        );
        let kill_channel_tx = {
            let mut action_state = action.state.lock();
            action_state.kill_channel_tx.take()
        };
        if let Some(kill_channel_tx) = kill_channel_tx {
            if kill_channel_tx.send(()).is_err() {
                error!(
                    operation_id = ?action.operation_id,
                    "Error sending kill to running operation",
                );
            }
        }
    }

    fn perform_cleanup(self: &Arc<Self>, operation_id: OperationId) -> Option<CleanupGuard> {
        let mut cleaning = self.cleaning_up_operations.lock();
        cleaning
            .insert(operation_id.clone())
            .then_some(CleanupGuard {
                manager: Arc::downgrade(self),
                operation_id,
            })
    }
}

impl RunningActionsManager for RunningActionsManagerImpl {
    type RunningAction = RunningActionImpl;

    async fn create_and_add_action(
        self: &Arc<Self>,
        worker_id: String,
        start_execute: StartExecute,
    ) -> Result<Arc<RunningActionImpl>, Error> {
        self.metrics
            .create_and_add_action
            .wrap(async move {
                let queued_timestamp = start_execute
                    .queued_timestamp
                    .and_then(|time| time.try_into().ok())
                    .unwrap_or(SystemTime::UNIX_EPOCH);
                let operation_id = start_execute
                    .operation_id.as_str().into();
                let action_info = self.create_action_info(start_execute, queued_timestamp).await?;
                debug!(
                    ?action_info,
                    "Worker received action",
                );
                // Wait for any previous cleanup to complete before creating directory
                self.wait_for_cleanup_if_needed(&operation_id).await?;
                let action_directory = self.make_action_directory(&operation_id).await?;
                let execution_metadata = ExecutionMetadata {
                    worker: worker_id,
                    queued_timestamp: action_info.insert_timestamp,
                    worker_start_timestamp: action_info.load_timestamp,
                    worker_completed_timestamp: SystemTime::UNIX_EPOCH,
                    input_fetch_start_timestamp: SystemTime::UNIX_EPOCH,
                    input_fetch_completed_timestamp: SystemTime::UNIX_EPOCH,
                    execution_start_timestamp: SystemTime::UNIX_EPOCH,
                    execution_completed_timestamp: SystemTime::UNIX_EPOCH,
                    output_upload_start_timestamp: SystemTime::UNIX_EPOCH,
                    output_upload_completed_timestamp: SystemTime::UNIX_EPOCH,
                };
                let timeout = if action_info.timeout.is_zero() || self.timeout_handled_externally {
                    self.max_action_timeout
                } else {
                    action_info.timeout
                };
                if timeout > self.max_action_timeout {
                    return Err(make_err!(
                        Code::InvalidArgument,
                        "Action timeout of {} seconds is greater than the maximum allowed timeout of {} seconds",
                        timeout.as_secs_f32(),
                        self.max_action_timeout.as_secs_f32()
                    ));
                }
                let running_action = Arc::new(RunningActionImpl::new(
                    execution_metadata,
                    operation_id.clone(),
                    action_directory,
                    action_info,
                    timeout,
                    self.clone(),
                ));
                {
                    let mut running_actions = self.running_actions.lock();
                    // Check if action already exists and is still alive
                    if let Some(existing_weak) = running_actions.get(&operation_id) {
                        if let Some(_existing_action) = existing_weak.upgrade() {
                            return Err(make_err!(
                                Code::AlreadyExists,
                                "Action with operation_id {} is already running",
                                operation_id
                            ));
                        }
                    }
                    running_actions.insert(operation_id, Arc::downgrade(&running_action));
                }
                Ok(running_action)
            })
            .await
    }

    async fn cache_action_result(
        &self,
        action_info: DigestInfo,
        action_result: &mut ActionResult,
        hasher: DigestHasherFunc,
    ) -> Result<(), Error> {
        self.metrics
            .cache_action_result
            .wrap(self.upload_action_results.cache_action_result(
                action_info,
                action_result,
                hasher,
            ))
            .await
    }

    async fn kill_operation(&self, operation_id: &OperationId) -> Result<(), Error> {
        let running_action = {
            let running_actions = self.running_actions.lock();
            running_actions
                .get(operation_id)
                .and_then(Weak::upgrade)
                .ok_or_else(|| make_input_err!("Failed to get running action {operation_id}"))?
        };
        Self::kill_operation(running_action).await;
        Ok(())
    }

    // Note: When the future returns the process should be fully killed and cleaned up.
    async fn kill_all(&self) {
        self.metrics
            .kill_all
            .wrap_no_capture_result(async move {
                let kill_operations: Vec<Arc<RunningActionImpl>> = {
                    let running_actions = self.running_actions.lock();
                    running_actions
                        .iter()
                        .filter_map(|(_operation_id, action)| action.upgrade())
                        .collect()
                };
                let mut kill_futures: FuturesUnordered<_> = kill_operations
                    .into_iter()
                    .map(Self::kill_operation)
                    .collect();
                while kill_futures.next().await.is_some() {}
            })
            .await;
        // Ignore error. If error happens it means there's no sender, which is not a problem.
        // Note: Sanity check this API will always check current value then future values:
        // https://play.rust-lang.org/?version=stable&edition=2021&gist=23103652cc1276a97e5f9938da87fdb2
        drop(
            self.action_done_tx
                .subscribe()
                .wait_for(|()| self.running_actions.lock().is_empty())
                .await,
        );
    }

    #[inline]
    fn metrics(&self) -> &Arc<Metrics> {
        &self.metrics
    }
}

#[derive(Debug, Default, MetricsComponent)]
pub struct Metrics {
    #[metric(help = "Stats about the create_and_add_action command.")]
    create_and_add_action: AsyncCounterWrapper,
    #[metric(help = "Stats about the cache_action_result command.")]
    cache_action_result: AsyncCounterWrapper,
    #[metric(help = "Stats about the kill_all command.")]
    kill_all: AsyncCounterWrapper,
    #[metric(help = "Stats about the create_action_info command.")]
    create_action_info: AsyncCounterWrapper,
    #[metric(help = "Stats about the make_work_directory command.")]
    make_action_directory: AsyncCounterWrapper,
    #[metric(help = "Stats about the prepare_action command.")]
    prepare_action: AsyncCounterWrapper,
    #[metric(help = "Stats about the execute command.")]
    execute: AsyncCounterWrapper,
    #[metric(help = "Stats about the upload_results command.")]
    upload_results: AsyncCounterWrapper,
    #[metric(help = "Stats about the cleanup command.")]
    cleanup: AsyncCounterWrapper,
    #[metric(help = "Stats about the get_finished_result command.")]
    get_finished_result: AsyncCounterWrapper,
    #[metric(help = "Number of times an action waited for cleanup to complete.")]
    cleanup_waits: CounterWithTime,
    #[metric(help = "Number of stale directories removed during action retries.")]
    stale_removals: CounterWithTime,
    #[metric(help = "Number of timeouts while waiting for cleanup to complete.")]
    cleanup_wait_timeouts: CounterWithTime,
    #[metric(help = "Stats about the get_proto_command_from_store command.")]
    get_proto_command_from_store: AsyncCounterWrapper,
    #[metric(help = "Stats about the download_to_directory command.")]
    download_to_directory: AsyncCounterWrapper,
    #[metric(help = "Stats about the prepare_output_files command.")]
    prepare_output_files: AsyncCounterWrapper,
    #[metric(help = "Stats about the prepare_output_paths command.")]
    prepare_output_paths: AsyncCounterWrapper,
    #[metric(help = "Stats about the child_process command.")]
    child_process: AsyncCounterWrapper,
    #[metric(help = "Stats about the child_process_success_error_code command.")]
    child_process_success_error_code: CounterWithTime,
    #[metric(help = "Stats about the child_process_failure_error_code command.")]
    child_process_failure_error_code: CounterWithTime,
    #[metric(help = "Total time spent uploading stdout.")]
    upload_stdout: AsyncCounterWrapper,
    #[metric(help = "Total time spent uploading stderr.")]
    upload_stderr: AsyncCounterWrapper,
    #[metric(help = "Total number of task timeouts.")]
    task_timeouts: CounterWithTime,
}

// Unit tests for `translate_input_root_path`. These nail the contract
// down before the helper does anything — they run against the identity
// stub and most will fail until the green-phase rewrite lands.
#[cfg(test)]
mod translate_input_root_tests {
    use super::{ProjectRoot, translate_input_root_path};

    fn pr(in_action: &str, on_disk: &str) -> ProjectRoot {
        ProjectRoot {
            in_action: in_action.to_string(),
            on_disk: on_disk.to_string(),
        }
    }

    #[test]
    fn project_root_none_is_identity() {
        let got = translate_input_root_path("/Users/octo/devel/proj/src", None);
        assert_eq!(got, "/Users/octo/devel/proj/src");
    }

    // Plan J's call site treats an empty platform-property value as
    // "no hint" and falls back to `action_directory/work`. The helper
    // must preserve that by returning empty unchanged.
    #[test]
    fn empty_raw_is_identity() {
        let pr = pr("/Users/octo/devel/proj", "/Users/general/devel/proj");
        assert_eq!(translate_input_root_path("", Some(&pr)), "");
    }

    #[test]
    fn matching_prefix_is_rewritten() {
        let pr = pr("/Users/octo/devel/proj", "/Users/general/devel/proj");
        let got = translate_input_root_path("/Users/octo/devel/proj/src/out/Mac", Some(&pr));
        assert_eq!(got, "/Users/general/devel/proj/src/out/Mac");
    }

    // `.132`/`.133` case: worker declares in_action == on_disk.
    // Rewrite must be observationally identity.
    #[test]
    fn identity_config_is_noop() {
        let pr = pr("/Users/octo/devel/proj", "/Users/octo/devel/proj");
        let got = translate_input_root_path("/Users/octo/devel/proj/src", Some(&pr));
        assert_eq!(got, "/Users/octo/devel/proj/src");
    }

    #[test]
    fn non_matching_path_is_identity() {
        let pr = pr("/Users/octo/devel/proj", "/Users/general/devel/proj");
        let got = translate_input_root_path("/tmp/unrelated/path", Some(&pr));
        assert_eq!(got, "/tmp/unrelated/path");
    }

    #[test]
    fn exact_prefix_match_yields_on_disk() {
        let pr = pr("/Users/octo/devel/proj", "/Users/general/devel/proj");
        let got = translate_input_root_path("/Users/octo/devel/proj", Some(&pr));
        assert_eq!(got, "/Users/general/devel/proj");
    }

    // Prefix must match at a path boundary — `/Users/octo` must NOT
    // match a path starting with `/Users/octoOther/...`.
    #[test]
    fn prefix_boundary_prevents_false_match() {
        let pr = pr("/Users/octo", "/Users/general");
        let got = translate_input_root_path("/Users/octoOther/file", Some(&pr));
        assert_eq!(got, "/Users/octoOther/file");
    }

    // Config fields may be entered with or without a trailing slash;
    // normalize so both forms produce the same translation.
    #[test]
    fn trailing_slash_in_in_action_is_normalized() {
        let pr_slash = pr("/Users/octo/devel/proj/", "/Users/general/devel/proj");
        let pr_noslash = pr("/Users/octo/devel/proj", "/Users/general/devel/proj");
        let raw = "/Users/octo/devel/proj/src";
        assert_eq!(
            translate_input_root_path(raw, Some(&pr_slash)),
            translate_input_root_path(raw, Some(&pr_noslash)),
        );
        assert_eq!(
            translate_input_root_path(raw, Some(&pr_slash)),
            "/Users/general/devel/proj/src",
        );
    }
}
