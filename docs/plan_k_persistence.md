# Plan K disk persistence (`path_digest_cache_persistence`)

NL 1.3.11 adds opt-in disk persistence to the worker's path-digest cache
(Plan K). Without it, every NL restart wipes the in-memory map and
forces every worker through a 5–15 minute "cold" window during which
~30k declared inputs are re-stream-hashed by Plan I. With it, the map
is reloaded from a snapshot in ~50 ms, each entry stat-gated against
its on-disk file, and warm steady-state is reached immediately.

## What problem it solves

Plan K is the per-file `(on_disk_path, expected_digest)` map that lets
the worker hardlink-from-disk on hit and skip the Plan I stream-hash
on the second occurrence of the same input. It lives only in
`Arc<RwLock<HashMap<...>>>` in process memory.

On the chromium-distributed-compile inverted topology (workers reach a
remote scheduler+CAS over a high-RTT link), measured cold-start cost on
2026-04-30 was:

| Counter (cold worker, 7 min window) | Value |
|---|---|
| `worker.plan_k.miss` | 31,282 |
| `worker.plan_i.file_hash` count | 31,282 |
| `worker.plan_i.file_hash` total_ms | 12,135,018 (≈3 h 22 m of CPU across parallel actions) |
| `worker.plan_i.file_hash` mean | 387 ms |
| `worker.action_execute` after 7 min | 30 |

After the same map had warmed naturally, the next build on the same
hardware completed actions ~10× faster. Every restart — binary upgrade,
supervisor reconnect, host reboot — paid the full re-hash cost again.

Persistence converts that recurring tax into `~30 s` (the load + first
flush) and a one-time stat per restored entry.

## Configuration

Add a top-level config block to the root `CasConfig`:

```jsonc
{
  // ...
  path_digest_cache_persistence: {
    path: "${MAC_DATA_DIR}/plan_k_snapshot.bin",
    flush_interval_seconds: 30,
  },
  // ...
}
```

Fields:

- `path` (required) — absolute path of the snapshot file. The parent
  directory must exist and be writable; the file itself is created on
  the first flush. `${VAR}` shellexpand is supported, mirroring
  conventions used elsewhere in the config.
- `flush_interval_seconds` (optional, default `30`) — how often the
  background task wakes to check the dirty bit. Flushes only happen
  when the map has changed since the last flush, so quiet workers
  produce zero disk I/O. Tighten only if you want to bound crash-loss
  to <30 s of warm entries.

When the block is absent, behaviour is byte-identical to upstream
(in-memory only). Default is **off**, opt-in.

## Trust contract

A persisted entry is only ever inserted into Plan K after the in-memory
map has accepted it, and the in-memory map only accepts entries from
two sources:

- Plan I (`file_matches_digest` succeeded) — the file at this path was
  fully stream-hashed and the hash equals the action's expected digest.
- CAS materialisation — the file was just downloaded from CAS and is
  known-good by construction.

Persistence inherits this invariant. The on-disk snapshot represents
"these `(path, digest)` pairs were content-verified at some point
during this NL's lifetime."

The window persistence newly opens is "what if the file changed while
NL was offline?" — closed by a load-time stat-gate that drops any
entry whose on-disk file is missing, is not a regular file, or whose
size does not equal the persisted `DigestInfo::size_bytes()`.
Same-size content swaps would still be caught by Plan I's full
re-hash on the next materialisation; same-path size mismatches are
caught at load.

## Snapshot format (v1)

```text
  bytes 0..4   magic "NLPK"
  bytes 4..8   format_version: u32 LE  (= 1)
  bytes 8..    bincode (standard config) of Vec<(PathBuf, DigestInfo)>
```

A snapshot with the wrong magic, wrong version, truncated header, or
bincode-decode failure is dropped wholesale (`dropped_corrupt = 1`),
and the worker starts cold. Layout changes bump `FORMAT_VERSION`.

Atomic write: encode under a short read lock, write to a sibling
`<file>.tmp.<pid>`, `fsync` the file, atomic-rename to target,
best-effort `fsync` the parent directory.

## Counters

Three new `StageStats` (visible in the worker's periodic
`timing:counter` log dump):

- `worker.plan_k.persistence.loaded` — entries restored at startup
  that passed the stat-gate.
- `worker.plan_k.persistence.dropped_stale` — entries whose on-disk
  file was missing or had a different size than the persisted
  digest. A non-zero value confirms the gate is doing real work
  (e.g. files were moved or rewritten while NL was offline).
- `worker.plan_k.persistence.flush` — successful background or
  shutdown flushes. Steady-state on a busy worker is roughly one per
  `flush_interval_seconds` while inserts are happening; idle workers
  see zero.

Compare these to the in-memory `worker.plan_k.{hit,miss}` counters to
see how much of the cold-start traffic the persistence is absorbing.

## Logs

At startup, when persistence is configured:

```
plan_k.persistence: snapshot load complete
  path=/var/nl/plan_k_snapshot.bin  loaded=31277  dropped_stale=0  dropped_corrupt=0
```

On each periodic flush failure (transient I/O error, full disk, etc.):

```
plan_k.persistence: periodic flush failed; retrying next tick
  path=...  error=...
```

On graceful shutdown:

```
plan_k.persistence: final flush on shutdown
  path=...
```

A failed flush never aborts the worker — the map stays in memory and
the next successful flush carries the full state.

## Cost

- Snapshot size: ~50–80 bytes per entry encoded; 70k entries
  (typical chromium hot-cache) is 5–15 MiB on disk.
- Load-time stat-gates: one `metadata` syscall per entry. 70k stats
  at ~10 µs each ≈ 700 ms — invisible compared to the ~3-5 s NL
  bring-up budget.
- Flush cost: bincode encode of the map under a short read lock + one
  atomic write. At 70k entries the encode is sub-100 ms; the lock is
  released before any I/O.
- Memory: the dirty bit is one `AtomicBool`. Encoding briefly clones
  the map's keys and digests into a `Vec`; freed once the write
  completes.

## When to enable

- Inverted-topology / off-LAN workers where the cold Plan I
  stream-hash storm dominates 5–15 min of post-restart latency.
- Frequent NL redeploys (binary upgrades, config flips, supervisor
  reconnects). Every restart is otherwise a full re-warm.
- Build harnesses with per-action deadlines (siso 120 s, etc.) where
  cold workers can't finish their first batch in time.

## When not to enable

- Single-cycle CI runners that build once and shut down — no warm
  state to preserve.
- Deployments where input-tree contents change drastically between
  restarts (most entries would `dropped_stale` anyway).
- Hosts with very tight disk space where even a ~20 MiB snapshot is
  unwelcome.

## Concurrent-NL safety

One NL process per snapshot path. Two NL processes pointing at the
same `path_digest_cache_persistence.path` will race on rename and
either lose flushes or corrupt each other's view. This matches the
existing `MAC_DATA_DIR` convention (concurrent NLs already conflict on
`cas/` and `ac/` directories) and is not enforced by NL itself.

## Test coverage

- `nativelink-worker/src/path_digest_persistence.rs`
  — 24 unit tests: codec round-trip (empty / 1 / 1000 entries), bad
  magic, wrong version, truncated header, truncated body, load
  no-op on missing path, load with all valid files, load drops
  missing file, load drops size mismatch, load drops corrupt
  snapshot, load drops wrong version, save round-trip, save
  overwrites existing, dirty bit lifecycle, flush task writes
  when dirty, skips when clean, final flush on shutdown,
  shutdown without dirty.
- `nativelink-worker/src/path_digest_cache.rs`
  — 4 unit tests for the `with_dirty_bit` builder: insert marks
  dirty, evict marks dirty, contains does not, no-bit case.
- `nativelink-worker/tests/path_digest_persistence_test.rs`
  — 2 end-to-end tests:
  `persistence_survives_simulated_restart` (full save → drop
  everything → fresh map → load → entries restored) and
  `persistence_drops_externally_modified_files_on_load`
  (truncate one file between save and load, confirm only the
  modified entry is dropped).
- `nativelink-config/src/cas_server.rs`
  — 4 unit tests: default `None`, parse minimal, parse full,
  reject unknown field via `deny_unknown_fields`.

## Implementation pointers

- New module:
  `nativelink-worker/src/path_digest_persistence.rs` — codec,
  `LoadStats`, `load_into`, `SaveError`, `save_to`, `DirtyBit`,
  `mark_dirty` / `take_dirty` / `is_dirty`, `spawn_flush_task`,
  three `StageStats` counters.
- Cache hook:
  `nativelink-worker/src/path_digest_cache.rs` — optional
  `dirty: Option<DirtyBit>` field, `with_dirty_bit` builder,
  `insert`/`evict` mark dirty when present.
- Manager wiring:
  `nativelink-worker/src/running_actions_manager.rs` — new
  `path_digest_cache_dirty: Option<DirtyBit>` field on
  `RunningActionsManagerArgs`, applied via
  `PathDigestCache::with_dirty_bit`.
- Local worker plumbing:
  `nativelink-worker/src/local_worker.rs` —
  `new_local_worker` takes a 7th argument
  `path_digest_cache_dirty: Option<DirtyBit>`.
- Config:
  `nativelink-config/src/cas_server.rs` — top-level
  `path_digest_cache_persistence: Option<PathDigestCachePersistenceConfig>`.
- Bring-up:
  `src/bin/nativelink.rs` — when configured, `load_into` after
  constructing the shared map, `spawn_flush_task`, bridge from
  the broadcast `ShutdownGuard` channel to the persistence
  module's `Arc<Notify>`.

## Verification on the chromium harness

1. Bring up the inverted-topology cluster with the 1.3.11 binary on
   `.166`/`.132`/`.133`. Add the config block above to `.132` only;
   leave `.133` as the unpersisted control.
2. Cold-start both workers; run a chrome cold build to populate
   Plan K (~30 min). Confirm `worker.plan_k.persistence.flush > 0`
   on `.132` only.
3. `pkill -9 -f nativelink-patched` + supervisor relaunch on both
   workers.
4. Re-run the build. On `.132`:
   - `worker.plan_k.persistence.loaded` ≈ 30k–70k
   - `worker.plan_k.persistence.dropped_stale` = 0 (or low)
   - First `worker.action_execute` within ~1–5 s of worker connect
     (vs ~30–40 s today)
   - First 100 actions inside siso's 120 s deadline without
     `--prewarm-target-actions`
5. On `.133` (control), expect today's behaviour (~3 h cumulative
   `worker.plan_i.file_hash`).
6. To exercise the gate, mutate one input file on `.132` between
   restart and run, e.g. `truncate -s 0
   ${MAC_DATA_DIR}/.../some_input.h`. Confirm
   `worker.plan_k.persistence.dropped_stale > 0` on the next load.

**Success criterion**: cold-restart-to-steady-state on `.132` drops
from ~7 min (control) to ~30 s.
