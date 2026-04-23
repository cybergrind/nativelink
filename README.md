# NativeLink macOS Shared-Tree Worker

A specialized fork of [NativeLink](https://github.com/TraceMachina/nativelink)
optimized for macOS distributed builds using a shared directory execution model
with a Redis-backed CAS journal.

## What is this?

This is NativeLink v1.0.0 with targeted patches that:

1. **Eliminate per-action input tree materialization** on macOS APFS workers —
   actions run directly in a pre-staged shared source tree, the same execution
   model as local `ninja -j20`.
2. **Cross-machine output sync via Redis** — action outputs produced on one
   worker are materialized on other workers before their next action, so siso
   can read depfiles on the controller machine regardless of where the action ran.
3. **Scheduler-side CAS journal** — the scheduler, at action dispatch time,
   tells each worker what files it needs to fetch, using a Redis-backed
   path→digest index populated as siso uploads blobs. Workers skip the full
   input-tree walk entirely and only materialize the delta.

The result: **450+ compile steps/min** on a 2-Mac-Mini cluster (up from 5-6/min
with unpatched NativeLink), approaching local-solo throughput with the benefits
of distributed execution.

## Design

The worker's per-action flow, with the CAS journal enabled:

```
Scheduler sends Execute notification to worker W
        |
        v
[Pre-action] drain nativelink:pending_outputs:{machine_id}
        |
        v
  For each entry: fetch blob from CAS → hardlink to shared tree
        |
        v
  HSET nativelink:worker_state:{machine_id} path digest
        |
        v
[Execute] clang runs in the shared tree — every file is present
        |
        v
[Post-action] upload outputs to CAS, record_outputs → broadcast to peers
```

The scheduler-side flow, on every action dispatch:

```
Scheduler picks worker W for action A
        |
        v
  Walk A.input_root_digest via Redis nativelink:dir_index
  (HGETALL chain of Directory→children; no CAS reads)
        |
        v
  HMGET nativelink:worker_state:{W.machine_id} for every resolved path
        |
        v
  For each missing/mismatched entry:
    RPUSH nativelink:pending_outputs:{W.machine_id} "path|digest-size"
        |
        v
  Send Execute notification to worker W
```

The CAS server hook, on every blob upload:

```
Client (siso) uploads blob via ByteStream.Write
        |
        v
  store.update_oneshot(digest, bytes)
        |
        v
  Try decode bytes as a Directory protobuf
        |
        v (if Directory)
  HSET nativelink:dir_index:{digest}-{size} <child_name> "file|digest-size" ...
```

### Key architectural decisions

- **Shared-tree execution**: `work_directory` is set to `InputRootAbsolutePath`
  from the action's platform properties. All actions share one filesystem view.
  Hermetic isolation is traded for throughput. A per-worker `project_root`
  remap is available for workers whose local on-disk layout differs from the
  path advertised by the action origin (e.g. off-tree Macs running under a
  different user account) — see the config reference below.

- **APFS clonefile**: `fs::hard_link` calls `clonefile(2)` first on macOS,
  eliminating shared-inode lock contention that serialized parallel
  materialization.

- **CAS-journal flow (Redis-backed)**: the scheduler computes the delta
  between an action's input tree and the target worker's disk state, and only
  tells the worker about missing files. Workers do not walk input trees
  themselves — they just drain the journal.

- **Per-worker caches (Plan K/L)**: in-memory caches retained as a safety net
  for the cases where the journal isn't used (config missing, Redis
  unreachable).

- **Trust-EEXIST on hard_link**: if a file already exists on disk, we trust it.
  Combined with the scheduler-side dedup, this means the journal only lists
  files the worker actually needs to fetch.

## Target use case

- Chromium (or similar large C++ project) builds on macOS via
  [siso](https://chromium.googlesource.com/infra/infra/+/refs/heads/main/go/src/infra/build/siso/)
  remote execution
- Workers are trusted Mac Minis on a local network
- Each worker has its own pre-staged source tree on local disk (synced via rsync)
- The scheduler can run on Linux; workers must be macOS (Apple Silicon)

## Getting started

For complete step-by-step deployment instructions see
**[DEPLOYMENT.md](./DEPLOYMENT.md)**.

Quick summary:

1. Run Redis on the scheduler machine, reachable from every worker
2. Add `dir_index_redis_url` to the `byte_stream` config (CAS server)
3. Add `dir_index_redis_url` to the `simple` scheduler config
4. On each worker: set `name` AND `machine_id` to the same stable string
   (typically the machine's IP), plus `shared_walked_dirs_redis_url`
5. Rsync the source tree to every worker at the path configured in
   `InputRootAbsolutePath`
6. Build + launch the patched NativeLink binary on every Mac

## Prerequisites

Each worker machine needs:

| Requirement | Details |
|---|---|
| macOS | 14 (Sonoma) or later |
| Xcode CLI tools | `xcode-select --install` |
| Rust toolchain | `rustup-init --profile minimal` (not Homebrew) |
| Pre-staged source tree | Byte-identical copy of the build source at a fixed path |
| Network | Gigabit Ethernet to the scheduler/CAS |
| Disk | 50+ GB free for CAS cache |
| Spotlight disabled on CAS dir | `touch <mac-data>/.metadata_never_index` + `sudo mdutil -i off -d /` |

The scheduler machine additionally needs Redis (any version ≥ 6).

## Building

```bash
# On a macOS (aarch64) worker:
cargo build --release --bin nativelink

# The binary:
target/release/nativelink
```

Cross-compilation from Linux to macOS is not supported (Mach-O linker + SDK
required). Develop and test on Linux, build the final binary on macOS.

## Running tests (Linux or macOS)

```bash
# All unit tests (platform-agnostic logic):
cargo test -p nativelink-util -p nativelink-worker -p nativelink-service -p nativelink-scheduler

# The clonefile-specific test runs only on macOS:
cargo test -p nativelink-util --test fs_test
```

## Configuration reference

See [DEPLOYMENT.md](./DEPLOYMENT.md) for complete examples. Summary of each
new config field:

| Location | Field | Required? | Purpose |
|---|---|---|---|
| `byte_stream[].dir_index_redis_url` | CAS server | For CAS journal | Redis URL for Directory proto index writes |
| `schedulers.simple.dir_index_redis_url` | Scheduler | For CAS journal | Redis URL for path resolution + dispatch journal writes |
| `workers[].local.shared_walked_dirs_redis_url` | Worker | For CAS journal | Redis URL for drain + worker_state updates |
| `workers[].local.machine_id` | Worker | With Redis | Machine identifier — **must equal `name`** and must match the value used across Redis keys for this worker |
| `workers[].local.name` | Worker | With Redis | Worker name prefix — **must equal `machine_id`** (scheduler strips a 36-char UUID suffix to recover `machine_id`) |
| `workers[].local.project_root` | Worker | Optional | `{ in_action, on_disk }` path remap for workers whose local tree path differs from the action-borne `InputRootAbsolutePath`. Unset → identity (same path on worker and in action). |

**All three `*_redis_url` values must point to the same Redis instance.**

### Minimal config (no Redis, fallback mode)

If you omit all Redis URLs, the worker falls back to walking the input tree
via `download_to_directory` as before. In-memory caches (Plan K/L) still
speed up repeated actions within a single worker process.

### Pre-staging the source tree

Each worker must have the source tree on disk. By default the on-disk path
must match `InputRootAbsolutePath` exactly:

```bash
rsync -av --delete \
  controller:/path/to/chromium/src/ \
  /Users/octo/devel/chromium-distributed-compile/src/
```

Re-sync after any `gclient sync` or source change on the controller.

Workers that can't use the advertised path (e.g. a worker running under a
different user whose home directory differs) set `project_root` in their
worker config to remap the prefix — the tree on disk can live anywhere,
as long as `on_disk` points at it:

```json5
project_root: {
  in_action: "/Users/octo/devel/chromium-distributed-compile",
  on_disk:   "/Users/general/devel/chromium-distributed-compile",
},
```

The remap is applied at action entry; the action's digest and any Redis
keys are unaffected, so workers with different `project_root` values can
safely share a cluster.

### macOS-specific setup

```bash
# Disable Spotlight on CAS data directory (required):
touch /path/to/mac-data/.metadata_never_index
sudo mdutil -i off -d /

# Clean stale work dirs after aborted builds:
rm -rf /path/to/mac-data/worker/work/*

# Create a dereferenced SDK copy (avoids symlink issues in CAS):
mkdir -p /Users/octo/chromium-sdk-clean
rsync -aL \
  --exclude='System/iOSSupport' \
  --exclude='System/Library/PrivateFrameworks' \
  /Applications/Xcode.app/.../SDKs/MacOSX.sdk/ \
  /Users/octo/chromium-sdk-clean/MacOSX.sdk/
```

## Upstream

Based on [TraceMachina/nativelink v1.0.0](https://github.com/TraceMachina/nativelink/tree/v1_branch).

The patches in this fork correspond to the optimization iterations documented in
the [macOS distributed build optimization](https://github.com/nicholasgasior/chromium-distributed-compile/blob/main/macos_distributed_build_optimize.md)
project.

## License

Same as upstream: Functional Source License, Version 1.1, Apache 2.0 Future
License. See [LICENSE](LICENSE).
