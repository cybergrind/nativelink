# NativeLink macOS Shared-Tree Worker

A specialized fork of [NativeLink](https://github.com/TraceMachina/nativelink)
optimized for macOS distributed builds using a shared directory execution model.

## What is this?

This is NativeLink v1.0.0 with targeted patches that eliminate per-action input
tree materialization overhead on macOS APFS workers. Instead of copying tens of
thousands of files into a fresh sandbox for every build action, this fork runs
actions directly in a pre-staged shared source tree — the same execution model
that local `ninja -j20` uses.

The result: **450+ compile steps/min** on a 2-Mac-Mini cluster (up from 5-6/min
with unpatched NativeLink), approaching local-solo throughput with the benefits
of distributed execution.

## Design

The patches implement a layered caching strategy on the worker:

```
Action dispatched by scheduler
        |
        v
[Plan L] walked_dirs Merkle cache — O(1) HashSet lookup
        |  hit: skip entire subtree walk (zero syscalls)
        |  miss: v
[Plan K] path->digest cache — per-file O(1) HashMap lookup
        |  hit: skip stat/chmod/utime (zero syscalls)
        |  miss: v
[Plan J] shared-tree stat check — file exists with expected size?
        |  yes: skip materialization
        |  no: v
[Plan I] hint-path hardlink — clonefile from pre-staged tree
        |  hit: O(1) APFS COW clone
        |  miss: v
[CAS fetch] traditional download from content-addressable store
```

### Key architectural decisions

- **Shared-tree execution (Plan J)**: `work_directory` is set to
  `InputRootAbsolutePath` from the action's platform properties. All actions
  share one filesystem view. Hermetic isolation is traded for throughput.

- **APFS clonefile (iter 6)**: `fs::hard_link` calls `clonefile(2)` first on
  macOS, eliminating shared-inode lock contention that serialized parallel
  materialization.

- **Per-worker caches (Plans K + L)**: In-memory caches that exploit siso's
  action-graph invariant (no parallel mutations on inputs). The walked-dirs
  Merkle cache collapses entire subtree walks to a single hash lookup after the
  first action.

- **Redis-persistent walked-dirs (optional)**: The walked-dirs Merkle cache can
  be backed by Redis so it survives worker restarts. Each machine has its own
  namespaced Redis key (`nativelink:walked_dirs:{machine_id}`), so machines
  never trust each other's walks.

- **Cross-machine output sync (optional)**: When Redis is configured, action
  outputs (`.o`, `.d`, `.pcm` files) produced on one worker are broadcast to
  other machines via Redis pending lists. Before each action starts, the worker
  drains its pending list and fetches any missing output files from CAS into
  the local shared tree. This ensures siso can read depfiles on the controller
  machine even when the compilation ran on a different worker.

## Target use case

- Chromium (or similar large C++ project) builds on macOS via
  [siso](https://chromium.googlesource.com/infra/infra/+/refs/heads/main/go/src/infra/build/siso/)
  remote execution
- Workers are trusted Mac Minis on a local network
- Each worker has its own pre-staged source tree on local disk (synced via rsync)
- The scheduler can run on Linux; workers must be macOS (Apple Silicon)

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
| Redis (optional) | Any Redis instance reachable from the workers |

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
cargo test -p nativelink-util -p nativelink-worker

# The clonefile-specific test runs only on macOS:
cargo test -p nativelink-util --test fs_test
```

## Configuration

### Worker config (json5)

The worker config must set the following fields to enable shared-tree execution:

```json5
{
  workers: [{
    local: {
      // ... standard NativeLink worker fields ...

      // REQUIRED: the pre-staged source tree path.
      // Must match what siso sends in the action's InputRootAbsolutePath.
      // Each worker machine must have this tree on local disk.
      platform_properties: {
        OSFamily: { values: ["darwin"] },
        ISA: { values: ["aarch64"] },
        InputRootAbsolutePath: {
          values: ["/Users/octo/devel/chromium-distributed-compile/src"],
        },
      },

      // OPTIONAL: Redis URL for persistent walked-dirs cache.
      // Survives worker restarts — avoids re-walking ~2000 Directory
      // protobufs on the first action after restart.
      // If omitted, the walked-dirs cache is in-memory only (lost on restart).
      shared_walked_dirs_redis_url: "redis://192.168.88.132:6379",

      // REQUIRED when shared_walked_dirs_redis_url is set.
      // Unique identifier for this machine — typically its IP address.
      // The Redis key becomes: nativelink:walked_dirs:{machine_id}
      // Each machine only reads/writes its own key.
      machine_id: "192.168.88.133",
    },
  }],
}
```

### What each field does

| Field | Required | Purpose |
|---|---|---|
| `InputRootAbsolutePath` | Yes | Activates shared-tree mode (Plan J). The worker runs actions directly in this directory instead of copying files into per-action sandboxes. |
| `shared_walked_dirs_redis_url` | No | Redis connection for persistent walked-dirs Merkle cache AND cross-machine output sync. Without it, caches are in-memory only (lost on restart) and outputs from other workers are not synced. |
| `machine_id` | Only with Redis | Namespaces Redis keys per machine. Used for both walked-dirs (`nativelink:walked_dirs:{machine_id}`) and output sync (`nativelink:pending_outputs:{machine_id}`). Use the machine's IP address. |

### Minimal config (no Redis)

```json5
platform_properties: {
  OSFamily: { values: ["darwin"] },
  ISA: { values: ["aarch64"] },
  InputRootAbsolutePath: {
    values: ["/Users/octo/devel/chromium-distributed-compile/src"],
  },
},
```

This gives you Plans I + J + K + L with in-memory caches. First action after
worker restart pays the full walk cost; subsequent actions are fast.

### Full config (with Redis persistence)

```json5
platform_properties: {
  OSFamily: { values: ["darwin"] },
  ISA: { values: ["aarch64"] },
  InputRootAbsolutePath: {
    values: ["/Users/octo/devel/chromium-distributed-compile/src"],
  },
},
shared_walked_dirs_redis_url: "redis://192.168.88.132:6379",
machine_id: "192.168.88.133",
```

First action after worker restart is also fast (walks are persisted in Redis).

### Pre-staging the source tree

Each worker must have the source tree at the exact path specified in
`InputRootAbsolutePath`. Sync from the build controller before starting:

```bash
rsync -av --delete \
  controller:/path/to/chromium/src/ \
  /Users/octo/devel/chromium-distributed-compile/src/
```

Re-sync after any `gclient sync` or source change on the controller.

### macOS-specific setup

**Disable Spotlight on CAS data directory** (required):
```bash
touch /path/to/mac-data/.metadata_never_index
sudo mdutil -i off -d /
```

**Clean stale work directories after aborted builds**:
```bash
rm -rf /path/to/mac-data/worker/work/*
```

**Create a dereferenced SDK copy** (avoids symlink issues in CAS):
```bash
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
