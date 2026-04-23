# NativeLink CAS-Journal Deployment Guide

Step-by-step instructions to bring up a multi-machine NativeLink cluster with
the full CAS-journal flow (dir_index + pending_outputs + worker_state).

**Target topology** in these examples:
- `192.168.88.132` — scheduler + CAS server + worker (combined) + Redis
- `192.168.88.133` — worker only
- Siso build controller runs on `192.168.88.132`

Replace IPs with your own.

---

## Prerequisites

On every machine:

| Requirement | Install |
|---|---|
| macOS 14 (Sonoma)+ | — |
| Xcode CLI tools | `xcode-select --install` |
| Rust toolchain | `rustup-init --profile minimal` (avoid Homebrew's rust) |
| Pre-staged source tree | via `rsync -av --delete ...` before the build |
| Gigabit Ethernet between machines | — |
| 50+ GB free for CAS cache | — |

On the scheduler machine only (`.132` here):

| Requirement | Install |
|---|---|
| Redis ≥ 6 | `brew install redis` |

---

## Step 1 — Start Redis on the scheduler machine

```bash
# On .132:
redis-server --bind 0.0.0.0 --daemonize yes
redis-cli config set protected-mode no    # required for .133 to reach .132
redis-cli ping                             # → PONG
```

Verify from `.133`:
```bash
# On .133:
redis-cli -h 192.168.88.132 ping           # → PONG
```

If the remote ping fails, check `bind`/`protected-mode`/firewall.

---

## Step 2 — The `config.name` must equal `machine_id` rule

**CRITICAL**: on every worker, `config.name` and `machine_id` must be the same
string. The scheduler appends a UUID suffix to `config.name` to form the full
`WorkerId`; the scheduler's dispatch hook strips those last 36 chars to recover
the `machine_id` that the worker uses for its Redis keys. If the two fields
differ, Redis keys don't align and the journal flow silently does nothing.

Recommended value: the machine's IP address as a plain string.

---

## Step 3 — CAS server config (`.132`)

In the combined scheduler + worker config file (e.g. `mac-combined.json5`),
add `dir_index_redis_url` to **both** the `byte_stream` entries AND the `cas`
entries. Siso uploads small blobs (including Directory protos) via
`ContentAddressableStorage.BatchUpdateBlobs`, NOT `ByteStream.Write` — so
both hook points must be configured or the dir_index stays empty.

```json5
{
  byte_stream: [{
    instance_name: "main",
    cas_store: "CAS_STORE",                              // existing
    max_bytes_per_stream: 16777216,                      // existing
    persist_stream_on_disconnect_timeout: 10,            // existing
    dir_index_redis_url: "redis://127.0.0.1:6379",       // ← NEW
  }],

  cas: [{
    instance_name: "main",
    cas_store: "CAS_STORE",                              // existing
    dir_index_redis_url: "redis://127.0.0.1:6379",       // ← NEW
  }],
}
```

**Effect**: every blob the client uploads via `ByteStream.Write` **or**
`ContentAddressableStorage.BatchUpdateBlobs` is probed as a Directory
protobuf; successful decodes are recorded in Redis as
`nativelink:dir_index:{digest}-{size}` HASHes.

**IMPORTANT**: if only the `byte_stream` hook is configured (or only the
`cas` hook), most Directory protos won't be indexed because siso picks the
batch API for small blobs. Configure both.

---

## Step 4 — Scheduler config (`.132`)

In the `simple` scheduler block, add `dir_index_redis_url` pointing to the
same Redis instance as step 3:

```json5
{
  schedulers: {
    MAIN_SCHEDULER: {
      simple: {
        supported_platform_properties: { /* existing */ },
        worker_timeout_s: 30,                                // existing
        client_action_timeout_s: 60,                         // existing
        max_job_retries: 3,                                  // existing
        allocation_strategy: "least_recently_used",          // existing
        dir_index_redis_url: "redis://127.0.0.1:6379",       // ← NEW
      }
    }
  }
}
```

**Effect**: on every action dispatch, the scheduler walks the action's input
tree via the Redis `dir_index`, HMGETs the target worker's `worker_state`, and
RPUSHes missing `(path, digest)` entries to `pending_outputs:{machine_id}`.

---

## Step 5 — Worker config on `.132` (combined scheduler + worker)

```json5
{
  workers: [{
    local: {
      name: "192.168.88.132",                             // ← must match machine_id
      worker_api_endpoint: { uri: "grpc://127.0.0.1:50061" },
      max_action_timeout: 7200,
      cas_fast_slow_store: "CAS_STORE",
      work_directory: "/Users/octo/devel/chromium-distributed-compile/remote/mac-data/work",
      platform_properties: {
        OSFamily: { values: ["darwin"] },
        ISA: { values: ["aarch64"] },
        InputRootAbsolutePath: {
          values: ["/Users/octo/devel/chromium-distributed-compile/src"],
        },
      },

      // CAS-journal flow:
      shared_walked_dirs_redis_url: "redis://127.0.0.1:6379",   // ← NEW/existing
      machine_id: "192.168.88.132",                              // ← must match `name` above
    }
  }]
}
```

---

## Step 6 — Worker config on `.133` (pure worker)

```json5
{
  workers: [{
    local: {
      name: "192.168.88.133",                             // ← must match machine_id
      worker_api_endpoint: {
        uri: "grpc://192.168.88.132:50061",              // points at scheduler's worker API
      },
      max_action_timeout: 7200,
      cas_fast_slow_store: "CAS_STORE",
      work_directory: "/Users/octo/devel/chromium-distributed-compile/remote/mac-data/work",
      platform_properties: {
        OSFamily: { values: ["darwin"] },
        ISA: { values: ["aarch64"] },
        InputRootAbsolutePath: {
          values: ["/Users/octo/devel/chromium-distributed-compile/src"],
        },
      },

      shared_walked_dirs_redis_url: "redis://192.168.88.132:6379",   // .132's Redis
      machine_id: "192.168.88.133",                                   // ← must match `name`
    }
  }]
}
```

### Step 6a (optional) — Off-tree worker with `project_root` remap

If a worker runs under a different user account or otherwise cannot place
the source tree at the exact path siso advertises on actions, set
`project_root` to remap the prefix. The on-disk tree can then live
anywhere, and the worker itself does the translation at action entry.

```json5
{
  workers: [{
    local: {
      name: "192.168.88.166",
      worker_api_endpoint: { uri: "grpc://127.0.0.1:50061" },    // tunneled
      max_action_timeout: 7200,
      cas_fast_slow_store: "CAS_STORE",
      work_directory: "/Users/general/devel/chromium-distributed-compile/remote/mac-data/work",
      platform_properties: {
        OSFamily: { values: ["darwin"] },
        ISA: { values: ["aarch64"] },
        // Advertise "" to keep the worker assignable regardless of the
        // action's InputRootAbsolutePath value; the remap below handles
        // the path translation on the worker side.
        InputRootAbsolutePath: { values: [""] },
      },

      shared_walked_dirs_redis_url: "redis://127.0.0.1:6379",   // tunneled
      machine_id: "192.168.88.166",

      // Off-tree remap: actions advertise /Users/octo/... paths; this
      // worker lives under /Users/general/... instead.
      project_root: {
        in_action: "/Users/octo/devel/chromium-distributed-compile",
        on_disk:   "/Users/general/devel/chromium-distributed-compile",
      },
    }
  }]
}
```

The remap applies only to this worker's `work_directory` and Plan I
hardlink-hint root. The action digest, Redis keys, and output-sync
broadcasts are unaffected, so `.132`/`.133` (which leave `project_root`
unset) do not need to be restarted or reconfigured.

---

## Step 7 — Pre-stage the source tree on each worker

Before any build, every worker must have a byte-identical copy of the source
tree at `InputRootAbsolutePath` — or at `project_root.on_disk` if the
worker sets a remap (see Step 6a):

```bash
# From the controller (.132):
rsync -av --delete \
  /Users/octo/devel/chromium-distributed-compile/src/ \
  octo@192.168.88.133:/Users/octo/devel/chromium-distributed-compile/src/
```

Re-run after any `gclient sync` or source change.

---

## Step 8 — macOS-specific setup (every worker)

```bash
# Disable Spotlight on the CAS data directory (prevents vnode storm):
touch /Users/octo/devel/chromium-distributed-compile/remote/mac-data/.metadata_never_index
sudo mdutil -i off -d /

# Clean any stale work dirs from previous crashed builds:
rm -rf /Users/octo/devel/chromium-distributed-compile/remote/mac-data/worker/work/*

# (If using Xcode SDK) create a dereferenced copy to avoid symlink issues:
mkdir -p /Users/octo/chromium-sdk-clean
rsync -aL \
  --exclude='System/iOSSupport' \
  --exclude='System/Library/PrivateFrameworks' \
  /Applications/Xcode.app/.../SDKs/MacOSX.sdk/ \
  /Users/octo/chromium-sdk-clean/MacOSX.sdk/
```

---

## Step 9 — Build and deploy the binary

```bash
# On each Mac worker (binaries can't be cross-compiled from Linux):
cd /path/to/nativelink
cargo build --release --bin nativelink
cp target/release/nativelink /Users/octo/nativelink-patched
```

Launch scheduler + worker with the patched binary:
```bash
NATIVELINK=/Users/octo/nativelink-patched make remote-scheduler-mac   # on .132
NATIVELINK=/Users/octo/nativelink-patched make remote-worker-mac      # on .133
```

---

## Verification

After starting the cluster and running one action, check Redis state:

```bash
# On .132:
redis-cli KEYS 'nativelink:dir_index:*' | head
redis-cli HGETALL nativelink:dir_index:<some-digest>-<size>
# → expect "file|...", "dir|...", "symlink|..." values

redis-cli HLEN nativelink:worker_state:192.168.88.133
# → expect a number > 0 after .133 runs its first action

redis-cli LLEN nativelink:pending_outputs:192.168.88.133
# → expect 0 between actions (drained)

redis-cli SMEMBERS nativelink:machines
# → 192.168.88.132, 192.168.88.133
```

In the scheduler log, expect lines like:
```
dir_index: published pending_outputs for worker machine_id=192.168.88.133 pushed=4 walked=30612
```

In the worker log on `.133`:
```
synced pending outputs from other workers count=4 materialized=4
```

---

## Redis key reference

| Key | Producer | Consumer | Shape |
|---|---|---|---|
| `nativelink:dir_index:{digest_hex}-{size}` | CAS ByteStream hook | Scheduler dispatch | HASH: child_name → `file\|digest-size` / `dir\|digest-size` / `symlink\|target` |
| `nativelink:worker_state:{machine_id}` | Worker post-drain | Scheduler dispatch | HASH: full_path → `digest_hex-size` |
| `nativelink:pending_outputs:{machine_id}` | Scheduler dispatch | Worker pre-action | LIST of `"path\|digest_hex-size"` |
| `nativelink:walked_dirs:{machine_id}` | Worker | Worker | SET of `(path, digest)` (existing Plan L) |
| `nativelink:machines` | Worker on start | Worker `record_outputs` | SET of registered machines |

---

## Operating notes

- **Worker restart**: the in-memory Plan K cache is lost. `worker_state` in Redis
  is retained, so the scheduler continues to dedup — but the *worker* itself
  will re-fetch files (Plan K miss). This is fine; pending_outputs will be
  empty after the first action.

- **Rsync out of sync**: if a worker's disk doesn't actually match its
  `worker_state` entries (e.g. operator deleted files), the scheduler will skip
  re-publishing them. The worker's hard_link will EEXIST, and since we trust
  EEXIST, the action runs against whatever content is actually on disk. The
  result may be wrong. Re-run `rsync` + `FLUSHDB` in Redis to reset.

- **Redis outage**: writes to `dir_index`, `pending_outputs`, and `worker_state`
  are all best-effort. On Redis failure, the scheduler's dispatch and worker's
  drain become no-ops; the system falls back to the worker's existing
  `download_to_directory` flow. Builds still succeed, they just slow down.

- **FLUSHDB rebuilds from scratch**: safe to do between builds. The next
  dispatch will rebuild `dir_index` on every CAS upload, and workers will
  re-materialize files on their next action. Do not `FLUSHDB` during an
  in-flight build.

---

## Rollback

To disable the CAS-journal flow without changing code:

1. Remove `dir_index_redis_url` from `byte_stream` config
2. Remove `dir_index_redis_url` from `simple` scheduler config
3. Remove `shared_walked_dirs_redis_url` and `machine_id` from worker configs
4. Restart all processes

Behavior reverts to the pre-journal flow: workers walk each action's input tree
via `download_to_directory` as before.
