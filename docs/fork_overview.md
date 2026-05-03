# NativeLink 1.4 Fork Overview

This document describes the macOS-focused fork of NativeLink whose
purpose is to run the 3-Mac Chromium distributed-build cluster.

## What this fork is

A lean superset of upstream `v1.0.0` with exactly the features the
3-Mac LAN cluster needs. No Plan M, no Redis, no disk persistence
caches, no per-stage timing harness — those are all explicit non-goals
documented in `roadmap_1.4.md`.

## What it adds over upstream

| # | Feature | Where |
|---|---|---|
| F1 | macOS APFS clonefile + idempotent hardlink-tree + exec-bit-preserving readonly | `nativelink-util/src/{fs,fs_util}.rs` |
| F2 | `perm_warn::classify_perm_result` (PermissionDenied non-fatal helper for chmod/utimes on uchg-flagged Apple SDK files) | `nativelink-util/src/perm_warn.rs` |
| F3 | Per-worker `project_root` remap for differing `$USER` paths across cluster nodes | `nativelink-config/src/cas_server.rs` |
| F4 | Plan I (digest-checked hint link) + Plan K (path-digest cache) + Plan L (walked-dir cache, in-memory only) + SingleFlight (stand-alone, available for future use) | `nativelink-worker/src/input_cache.rs` |
| F5 | Worker integration: `download_to_directory` consults Plan K, then Plan I, then CAS; subdir recursion consults Plan L | `nativelink-worker/src/running_actions_manager.rs` |
| F6 | `local_materialization_root` — in-process worker mirrors declared outputs to a build-tree root after upload, idempotent | `running_actions_manager.rs::inner_upload_results` |
| F7 | AC self-check on read (`get_self_check_store`) — invalidates AC entries whose first output blob is missing from CAS | `nativelink-service/src/ac_server.rs` |
| F8 | Slim counters (`worker.plan_{i,k,l}.{hit,miss}`, `worker.cas.populate_fast_store`) | `nativelink-util/src/counters.rs` |
| F9 | macOS arm64 release workflow on semver tags from `v1.4_branch` | `.github/workflows/macos-artifact.yaml` |

Diff vs upstream `v1.0.0`: ~1.5 k production LOC + ~1 k test LOC.

## Configuration

### Per-worker (`workers[].local`)

```json5
{
  // Optional: remap action-borne InputRootAbsolutePath onto local FS.
  // When the cluster has differing $USER paths across nodes, set this
  // on each worker so actions originating elsewhere find their tree.
  project_root: {
    in_action: "/Users/octo",
    on_disk:   "/Users/kpi",
  },

  // Default: true on this fork. Set to false if the worker has no
  // reliable local hint tree.
  experimental_digest_checked_hint_link: true,

  // Combined-mode in-process worker only: mirrors declared outputs
  // from sandbox to <root>/<declared_path>, idempotent on matching
  // size. Closes the siso local-fallback bug verified in pre-1.4
  // investigation. Leave unset on remote-only workers.
  local_materialization_root: "/Users/kpi/chromium-distributed-compile/src/out/Mac",
}
```

### AC server (`ac[].config`)

```json5
{
  ac_store: "main_ac",
  read_only: false,

  // Optional: AC GET checks that the first output blob exists in
  // the named CAS; if not, returns NotFound to invalidate the stale
  // AC entry. Default: None.
  get_self_check_store: "main_cas",
}
```

## How input materialization works

```
prepare_action_inputs(action)
  ├─ if action carries platform_property["InputRootAbsolutePath"]:
  │    hint_root = translate_input_root_path(in_action, project_root)
  │  else:
  │    hint_root = None  (Plan I disabled for this action)
  │
  └─ download_to_directory(...)
       │
       ├─ for each file in Directory:
       │    Plan K: cache.path_digests.contains((dest, digest))?
       │      hit  → counter("worker.plan_k.hit"), skip
       │      miss → counter("worker.plan_k.miss"), proceed
       │
       │    Plan I: try_hint_link(hint_root, name, digest)?
       │      hit  → counter("worker.plan_i.hit"), link from hint
       │      miss → counter("worker.plan_i.miss"), fall to CAS
       │
       │    CAS: populate_fast_store + hard_link
       │
       │    mark Plan K
       │
       └─ for each subdir:
            Plan L: cache.walked_dirs.contains((subdir_path, digest))?
              hit  → counter("worker.plan_l.hit"), skip subdir
              miss → counter("worker.plan_l.miss"), recurse
            mark Plan L
```

## How output materialization works (combined-mode workers)

```
inner_upload_results(action_result)
  ├─ upload all output_files to CAS (existing)
  ├─ if local_materialization_root is configured:
  │    for each output_file in action_result.output_files:
  │      dst = <local_root>/<declared_path>
  │      if dst exists with matching size: skip   (path-share-lucky)
  │      else: rm dst (if any) + mkdir parents + hardlink/clonefile
  │      on failure: warn, continue (CAS has the bytes)
  └─ commit AC entry
```

## How AC self-check works

```
inner_get_action_result(req)
  ├─ get AC entry from store
  ├─ if get_self_check_store is configured:
  │    digest = first_output_digest(action_result)
  │    if cas.has(digest) is None:
  │      log warn + return NotFound  (caller re-runs)
  └─ return action_result
```

## Telemetry

Counters are scraped via `nativelink_util::counters::snapshot()`. The
worker's input path emits:

- `worker.plan_k.hit` / `worker.plan_k.miss`
- `worker.plan_i.hit` / `worker.plan_i.miss`
- `worker.plan_l.hit` / `worker.plan_l.miss`
- `worker.cas.populate_fast_store`

There is no periodic dump task. Read counters via the metrics endpoint
or by adding a one-shot `SIGUSR1` handler in operator scripts.

## Branch and release

- Working branch: `v1.4_branch` (cut from upstream `v1.0.0`).
- Tags: `1.4.0`, `1.4.1`, … on `v1.4_branch`.
- macOS arm64 binary is built and published as a GitHub Release on
  every `MAJOR.MINOR.PATCH` tag.

## Operator runbook

See `docs/operator_runbook.md` for cluster-bring-up and the cold/warm
build playbook.
