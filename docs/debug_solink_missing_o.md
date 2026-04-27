# Debug Plan: SOLINK "no such file or directory: …/foo.o"

## Symptom

A remote compile action lands on a NativeLink worker, NL reports the
action complete, but on a downstream SOLINK action the linker fails:

```
clang++: error: no such file or directory:
  'obj/third_party/blink/renderer/core/core/line_info.o'
```

In the build log the missing `.o` was traced through `S → f → F` in
siso (started → fallback → finished). siso believes the action
succeeded; the file is not on disk.

## Empirical baseline (established 2026-04-27)

A 2-pass experiment on **NL 1.2.0**, no restart between passes, same
binary, same Redis, same `src/out/Mac`, same siso. Captured in
`/home/kpi/devel/opensource/chromium/document_h_build_report.md`.

| NL process age | Wall | local | remote | fallback | Outcome |
|---|---|---|---|---|---|
| ~2 min        | 15m08s | 9    | 2142 | **1**   | clean ✅ |
| ~21 min       | 25m11s | 99   | 2052 | **91**  | succeeded slow ✅ |
| hours (1.3.4) | 33m32s aborted | 130 | 1967 | **129** | SOLINK fail ❌ |

Conclusions from the report:

1. The bug is present in **1.2.0**. It is *not* a regression introduced
   by any of the seven 1.2.0→1.3.4 commits (Plan K prewarm / Plan M
   synth lineage / coalesce / kill-switch / Plan M removal).
2. Fallback rate rises **monotonically with NL process uptime**, on
   the same binary, same external state. The SOLINK failure is the
   same `f CXX` pattern at a higher count, with the additional bad
   luck of the missing `.o` being one SOLINK needs.
3. All 91 pass-2 fallbacks are `f CXX` (none are `f SOLINK`). They
   cluster in `third_party/blink/renderer/core/core/` — the same
   neighborhood as `line_info.o`.
4. The bug needs **both** real action dispatches *and* aged NL process
   state to surface (touch-only rebuilds don't trigger it).
5. Operationally, restarting NL between hot rebuilds is the only
   known reliable workaround.

The 1.3.x commits may modulate the magnitude — worth measuring — but
they did not introduce the failure mode.

## Where missing-files can come from in NL — verified call paths

`download_to_directory` in
`nativelink-worker/src/running_actions_manager.rs:175` is the function
that materializes an action's input tree on a worker. Two
short-circuit branches return without writing files:

1. **Plan L hit** — `running_actions_manager.rs:190-197`:
   ```rust
   if path_digest_cache.dir_walked(current_directory, digest) {
       PLAN_L_HIT.incr();
       return Ok(());
   }
   ```
2. **Coalescer follower hit** — `running_actions_manager.rs:218-231`.

These exist only in 1.3.x. In 1.2.0 there is no Plan L / coalescer /
process-shared cache, so neither branch can be the failure mode there.
The fact that the same symptom occurs on 1.2.0 means the failure
originates **outside the input-prep cache layer**: at action
execution, output capture, output upload to CAS, or output retrieval
on the driver side.

## What grows in NL's process state over uptime — candidates

These are the NL-side structures/resources that accumulate while the
process runs and could plausibly degrade the
fallback-output-materialization path. Each is a hypothesis bucket for
the bisection plan below.

### G1 — Filesystem store eviction churn

`nativelink-store`'s `FilesystemStore` enforces a `max_bytes` cap by
LRU-evicting old blobs. After many actions:

- The store has cycled through more bytes than the cap several times.
- `BatchUpdateBlobs`/`Read` requests may race with eviction.
- An output blob just written by an action could be evicted before
  the driver fetches it via `BatchReadBlobs` — siso sees a missing
  CAS entry, falls back to local, and the local fallback writes the
  `.o` to a path that may or may not stick depending on its own state.

This is the strongest age-correlated suspect that exists in 1.2.0.

### G2 — Inflight-action / scheduler bookkeeping leak

The scheduler tracks active actions, worker registrations, lease
state. If any of these maps grow without proper cleanup (orphaned
leases, stale connection objects, retry slots), at high uptime the
scheduler may take longer to dispatch, time out on output collection,
or return ambiguous results that siso treats as fallback-required.

### G3 — File descriptor / OS-resource exhaustion on the worker

Long-running worker accumulates open fds (CAS streams, log files,
spawn pipes). At high count, action `spawn`/`exec` can fail in
non-obvious ways (`EMFILE` partial reads, truncated outputs, hangs
that cross siso's deadline). Outputs may be partially produced or
not collected.

### G4 — Tokio runtime backlog / executor saturation

Background tasks (FS-store eviction, Redis sync, metric flushes) that
accumulate across uptime can starve action-execution tasks. An action
runs slowly or its output-upload future is dropped/cancelled, leading
to a "completed but no output" state.

### G5 — CAS upload deduplication / pending-set lingering

If NL has a structure tracking "blob X is currently being uploaded by
some action" to avoid duplicate work, and that structure does not
fully clean up entries when an action ends, a later action's
`BatchUpdateBlobs` can early-return ("someone is uploading this") but
the original uploader has long since gone, leaving the blob
unavailable.

### G6 — Worker→scheduler heartbeat / re-registration churn

If at high uptime the worker briefly disconnects and re-registers,
in-flight action results may be lost in transit, scheduler returns
ambiguous status, siso falls back. The fallback runs locally and may
race with NL state.

## Hypotheses, ranked by evidence fit

H1 is no longer the dominant hypothesis. The new ranking, conditioned
on the 1.2.0 evidence:

### NEW-H1 — Filesystem-store eviction race (G1)

After `O(many)` blob writes the FS store is cycling its LRU. An
output blob written at end of action N is evicted during action
N+M's input prep (which downloads and pins many files), so by the
time siso fetches the `.o` from CAS the blob is gone. Strongest
age-correlated candidate that exists in 1.2.0.

### NEW-H2 — Worker output-collection / upload race (G2/G5)

NL marks the action complete before its output blobs have been
durably written to CAS, or the upload futures are cancelled at a
boundary. siso fetches and gets nothing.

### NEW-H3 — Resource exhaustion on the worker host (G3/G4)

OS-level fd/memory/scheduler pressure on a long-running worker
process makes some actions silently truncate or drop outputs.

H1/H2/H3 from the previous version of this document (Plan L/I/K
caches) remain candidates only as **amplifiers** for the 1.3.4 case;
they cannot explain the 1.2.0 observation.

## Bisection plan

The new top priority is identifying **what NL state is age-driving the
fallback rate**, not which commit introduced it.

### Step 0 — confirm age curve on 1.3.4

Repeat the document_h 2-pass experiment on 1.3.4 with the same
preserved-state procedure. Goal: a row in the same table format as
the report. If 1.3.4's pass-2 fallback count is similar to 1.2.0's
~91, the 1.3.x commits do not amplify; if substantially higher
(say >150), one of them does. This is mostly to size the optimization
target, not to gate the rest of the plan.

Capture metrics dumps every 5 minutes during the run:
`worker.fs_store.bytes`, `worker.fs_store.evict.*` if exposed,
process RSS, fd count (`lsof -p $NL_PID | wc -l`), `worker.execute.*`,
`worker.upload.*`, `worker.plan_l.*` (1.3.x only).

### Step 1 — capture the failing-action signature

On a warm reproduce, identify one specific `f CXX` event whose `.o`
does not appear on disk afterward. Record:

- Action digest.
- Worker that took it (scheduler log).
- That worker's `worker.execute.*` for the digest:
  was the action observed as completed? With what exit code? With
  what `output_files` count?
- The matching `BatchUpdateBlobs` call: was the output blob
  uploaded? With what byte count?
- A `BatchReadBlobs` for that digest from the driver: does NL serve
  it? Or does it 404?
- Whether `FilesystemStore`'s underlying file for that digest exists
  on the worker host filesystem at the moment of the failure.

This single trace will tell us which of {action ran but didn't write
output, output written but never uploaded, uploaded but evicted,
served but not received} is happening. Without it the rest of the
plan is guesswork.

#### Step 1 — results (NL 1.2.0 pass 2, 91 fallbacks, captured 2026-04-27)

**Per-worker fallback distribution** (cross-referencing the 91 fallback
`.o` paths from `chrome-hot2.log` against `Executing command` lines in
each NL log, time-filtered to the pass-2 window 07:15:59 – 07:41:11):

| Worker | Pass 2 dispatches | Unique fallback `.o`s seen | Fallback rate |
|---|---:|---:|---:|
| `.132` (combined: scheduler + worker, central CAS) | 672 | **70** | **10.4%** |
| `.133` (worker-only, LAN, separate per-worker FS store) | 1059 | 1 | 0.09% |
| `.166` (worker-only, tunnel-routed, separate per-worker FS store) | 430 | 26 | 6.0% |

(70+1+26 = 97 ≠ 91; the 6 overlap entries are actions that appear in
two workers' logs — most plausibly retried after the first attempt's
output failed to materialize. That is itself a hint.)

The shape that matters is not "all three workers do this evenly" but
the **two-orders-of-magnitude gap between `.132` and `.133`** —
they're on the same LAN running the same binary against the same
scheduler. The only architectural difference is whether the worker
shares its FilesystemStore with the scheduler.

**Single-action trace — `view_transition_supplement.o` (one of the
`.166` cases, operation_id `aaeb86e3-7160-4ec0-80f8-ddff359785ee`):**

| Time UTC | Event | Source |
|---|---|---|
| 07:15:59 | siso build start (pass 2) | siso |
| 07:16:05.6 | siso `S CXX` (action accepted by scheduler) | siso log |
| 07:21:43.463 | scheduler dispatches → `.166` (5m38s queue wait) | `scheduler-mac.log` line 62162 |
| 07:22:14.387 | `.166` `Executing command` (input prep ~31s) | `.166` worker log |
| 07:22:32.265 | `.166` `Command complete` (clang++ ran 17.9s, exit 0 implied — no error path entry) | `.166` worker log |
| 07:22:32.266 | `.166` `upload_results: starting with timeout=600s` | `.166` worker log |
| (none) | no `upload_results: still in progress` warn (would fire at 60s) | implies upload finished < 60s |
| (none) | no `Error during upload_results` | implies upload succeeded |
| 07:23:42.49 | siso fires `f CXX` (gives up on the remote) | siso log |
| ~07:24:55 | siso local fallback `F CXX` (writes the `.o` itself on .132) | siso log |

The remote action **completed and uploaded**; siso fell back ~70s
later anyway. Whatever propagation step exists between
"`upload_results` returned Ok" and "siso receives the ActionResult and
moves on" did not happen in time. siso's per-action remote-wait
threshold was reached and it ran the compile locally on `.132`.

**Configuration finding — actually rules G1 *out*, not in:**

`mac-combined.json5` has two separate stores on the combined node:

```
AC_MAIN_STORE (action-cache metadata only):  max_bytes = 20 GB
SHARED_CAS    (the actual blob store):       fast_slow {
                                               fast: filesystem max_bytes = 100 GB
                                               slow: noop
                                             }
```

`AC_MAIN_STORE` only holds `ActionResult` protos (sub-KB each); 20 GB
is irrelevant at our action volumes. The actual `.o` blobs live in
`SHARED_CAS`, which is a single 100 GB filesystem tier — *not* a
20 GB-tiered cache. A pass-2 hot rebuild generates roughly
~3,100 actions × ~2 MB avg outputs ≈ 6 GB of new blobs, plus a few
GB of newly-touched inputs — well under the 100 GB cap. Eviction
under that cap is implausible inside a single pass.

So **G1 is not actually well-supported once we look at the real
config.** What *is* still distinctive about `.132` is the architectural
fact the per-worker breakdown highlights: the combined node runs
scheduler + worker in the **same process / same Tokio runtime**,
sharing executor, fds, and disk I/O against the central store. Remote
workers (`.133`, `.166`) don't share a runtime with the scheduler.

**Revised hypothesis ranking after Step 1:**

1. **NEW-H2 (output-collection / scheduler-side delivery race) — now
   the dominant suspect.** The trace shows `upload_results` returned
   without a stall warning and without an error, but siso never
   received the result before its per-action wait threshold. On the
   combined node, the path "worker finishes upload → scheduler
   forwards `ExecuteResponse` to siso over the streaming gRPC" runs
   on the same Tokio executor that's also handling 35× concurrent
   action lifecycle traffic. Plausible failure modes:
   - `ExecuteResponse` future is dropped/cancelled at a `select!`
     boundary when the scheduler is pulling many things at once.
   - The streaming gRPC sink to siso buffers but the scheduler-side
     send task is starved long enough that siso's per-action wait
     elapses first.
   - The action transitions to `Completed` in the scheduler, but the
     subscription that should hand it to the originating client is
     awoken too late.

2. **G3/G4 (resource exhaustion / Tokio runtime backlog).** Same
   underlying mechanism as NEW-H2 viewed from the other side — if
   the Tokio runtime is overloaded, every step of NEW-H2 gets
   slower; combined-node load amplifies this because the same
   runtime carries scheduler + worker. fd / RSS / task-count
   sampling during pass 2 would test this directly.

3. NEW-H1 (FS-store eviction race) — **deprioritized.** With
   `SHARED_CAS = 100 GB` and ~6 GB of churn per pass, eviction
   shouldn't fire. Worth re-checking only if Steps 4/5 produce
   a counter that contradicts this (e.g. an unexpectedly high
   eviction rate at info-level metric dumps).

**Better Step 2 (replaces the FS-store cap experiment):**

Test the "combined-node executor sharing" hypothesis directly by
splitting `.132` into two processes:
- A scheduler-only process (`mac-scheduler.json5` already exists in
  the configs; currently unused since combined is the default).
- A worker-only process (use `mac-worker.json5` against
  `127.0.0.1:50061`).

Run the same hot1/hot2 procedure. If pass-2 fallback rate on `.132`
drops from ~10% toward `.133`'s ~0.1%, NEW-H2/G4 confirmed and the
fix is to ensure the scheduler's client-result-delivery path is not
on the same executor as worker action-execution (or more
operationally: never run `.132` in combined mode for builds where
fallback matters).

If the rate stays high even after splitting, the issue isn't
executor-sharing per se — likely a scheduler-internal subscription
delivery bug.

Step 0 (1.3.4 age curve) and the original Step 2 (FS-store cap)
both move from "next" to "later, only if needed".

#### Step 2 — results (NL 1.2.0 split-mode pass 1, captured 2026-04-27)

**NEW-H2 falsified.** Full report at
`/home/kpi/devel/opensource/chromium/document_h_split_test_report.md`.

Splitting `.132` into separate scheduler + worker processes (each
with its own `MAC_DATA_DIR`) made the failure dramatically worse, not
better. Pass 1 of split mode reproduced the original SOLINK
missing-`.o` failure at fresh NL state (~2 min uptime), where pass 1
of combined mode runs clean. The bug therefore is **not
age-correlated** as the prior 1.2.0 evidence suggested — the
"monotonic with NL uptime" pattern was an artifact of how the worker
distribution shifts as a build progresses.

| Metric | 1.2.0 combined pass 1 | 1.2.0 split pass 1 |
|---|---:|---:|
| Wall | 15m08s ✅ | **31m01s** ❌ |
| `local` | 9 | 87 |
| `remote` | 2142 | 2022 |
| **`fallback`** | **1** | **86** (~86×) |
| `^FAILED` SOLINK | 0 | **1** |
| Build | Succeeded | Failure (no Chromium binary) |

**Root cause: gRPC blob path leaks 24 bytes onto disk.**

The action that broke SOLINK (`obj/.../core/core/local_frame.o`) was
*not* a fallback. It completed remotely on `.166` (`F CXX` at
10m41s, no `f` event), siso received the result, and siso wrote the
bytes to local disk. SOLINK 21 minutes later failed reading it:

```
ld64.lld: error: obj/third_party/blink/renderer/core/core/local_frame.o:
            unhandled file type
```

`xxd` of that file:

```
00000000: 0100 0001 0000 0000 00c8 fa0b 0000 0000  ................
00000010: 0000 fd53 0000 a2cf faed fe0c 0000 0100  ...S............
```

Mach-O 64 magic `cf fa ed fe` is at on-disk offset `0x18`, not
`0x00`. There are **24 bytes of NL framing prepended** to the actual
Mach-O content. SOLINK reads `data` (not Mach-O) and rejects.

**Why combined mode is immune** (and why the prior `.132 = 10.4%`
fallback rate vs `.133 = 0.09%` was the *opposite* signal it looked
like):

In combined mode, scheduler and worker share `SHARED_CAS = fast_slow {
fast: filesystem, slow: noop }`. When the worker writes an output
blob, it goes through the in-process `fast_slow.update()` path
straight into `${MAC_DATA_DIR}/cas/content/`. siso's later
`BatchReadBlobs` reads from the same in-process store via the same
handle. Zero gRPC framing in the path.

In split mode (and on `.166` even in baseline, because `.166`'s
worker → scheduler hop *is* gRPC over the tailscale tunnel), the
worker's outputs traverse:

```
worker fast tier (filesystem) → worker slow tier (GRPC_CAS_STORE)
   → scheduler-side BatchUpdateBlobs/ByteStream.Write
   → scheduler CAS (filesystem)
   → siso BatchReadBlobs against scheduler
```

Somewhere on that path, 24 bytes of internal framing are leaking
into the on-disk content. Combined mode skips it entirely; that's
why combined had 0–1 fallbacks per pass and split had 86 in pass 1
alone. The prior baseline's `.132 = 10.4%` was inflated because
*shared `.166` overflow* was being attributed to `.132` — actually
re-reading the per-worker breakdown with this lens: those `.132`
fallbacks were `.132`-driver-host's local fallback compiles, not
`.132`-worker-emitted-bad-output. The driver-host writes go through
the in-process store too, so they're fine.

#### Step 2.1 — header bytes decoded (verified against source)

The 22-byte prefix is **a bincode-serialized `CompressionStore::Header`
+ the first `CHUNK` frame's 5-byte sub-header**. Field-by-field:

| Offset | Bytes | Field | Value |
|---:|---|---|---|
| 0 | `01` | `Header.version` (u8) | 1 = `CURRENT_STREAM_FORMAT_VERSION` (`compression_store.rs:42`) |
| 1–4 | `00 00 01 00` | `Header.config.block_size` (u32 LE) | 65,536 (default lz4 block size) |
| 5–8 | `00 00 00 00` | `Header.upload_size` enum tag (u32 LE, bincode legacy/Fixint) | 0 = `UploadSizeInfo::ExactSize` |
| 9–16 | `c8 fa 0b 00 00 00 00 00` | `Header.upload_size` payload (u64 LE) | **785,096** (raw uncompressed size) |
| 17 | `00` | first frame `frame_type` (u8) | 0 = `CHUNK_FRAME_TYPE` (`compression_store.rs:111`) |
| 18–21 | `fd 53 00 00` | first chunk's compressed length (u32 LE) | 21,501 |
| 22 | `a2` | LZ4 token byte (start of compressed block) | — |

The Mach-O magic `cf fa ed fe` at byte 23 onward is **not raw Mach-O**;
it's the LZ4 literal copy of the first bytes of the original `.o` (LZ4
literals at the start of a stream show up unaltered after the token
byte, so the file looks like Mach-O magic was just shifted by 23
bytes).

This is a **legacy CompressionStore stream**, not a new framing layer.

#### Step 2.2 — stale-CAS-blob root cause (smoking gun)

`remote/mac-combined.json5` had `CAS_MAIN_STORE = compression { lz4 }`
**from 2026-04-06 (`6678af7`) until 2026-04-08 17:28
(`42dd157` "macos: use directory cache")**, after which it was
reverted to `fast_slow { fast: filesystem, slow: noop }`. During those
~2 days the central CAS at `${MAC_DATA_DIR}/cas/content/` was written
in CompressionStore format — every blob = 17B Header + (CHUNK frames)
+ FOOTER frame, named by digest of the original (uncompressed) bytes.

After the config revert, the FilesystemStore on disk still holds those
legacy compressed blobs. The 100 GB cap and 3 weeks of churn evicted
most, but any digest LRU-touched recently survives.

**Why the legacy blobs are self-perpetuating** —
`nativelink-worker/src/running_actions_manager.rs:800-811`
(`upload_file`) does:

```rust
if cas_store.has(store_key.borrow()).await
    .is_ok_and(|result| result.is_some())
{
    trace!("upload_file: digest already exists in CAS, skipping upload");
    return Ok(());
}
```

When a deterministic Chromium build re-produces a `.o` whose raw bytes
hash to the same digest as a surviving April-6/7/8 compressed blob,
the worker calls `has(digest)` against the central CAS, gets a hit,
and **silently skips the upload**. The stale compressed blob is never
overwritten, and every subsequent siso `BatchReadBlobs(digest)` hands
back the CompressionStore stream raw. siso writes those bytes to
`out/Mac/.../foo.o`, SOLINK reads it, sees `data` not Mach-O, and
errors with `unhandled file type` (or, in earlier 1.3.4 incident,
`no such file or directory` because the linker's later codepath
treats the malformed file as missing).

**Why combined mode looked clean and split looked catastrophic** —
both modes share the same on-disk `${MAC_DATA_DIR}/cas/content/`. The
prevalence of corrupt outputs depends only on which digests siso ends
up needing on a given pass. Pass-2 and split-mode dispatched more
`.166`-routed CXX actions; `.166`'s output uploads necessarily go via
gRPC `update_with_whole_file` to the central CAS (gated by the same
`has(digest)`-skip), so they hit the same stale-blob trap that
combined mode happens to avoid for in-process-completed actions.
The earlier "monotonic with NL uptime" pattern was an artifact of
which `.o`s siso fetched — not real age correlation.

**Why this is a NativeLink design issue, not just a config-history
issue** — the dedup-on-write check trusts that a digest's existence
implies the bytes-on-disk are correct. There is no on-disk-format tag
distinguishing "legacy compressed" from "current raw"; once the FS
contains a lying blob, it can't self-heal. Any operator who removes a
wrapping store (compression / dedup) without wiping the FS underneath
will get this exact failure mode silently.

**Independent finding from the same run — `mac-scheduler.json5` was
broken too.**

The first split-mode launch attempt failed before any blob corruption
could surface, with every `.132` worker action erroring at
`prepare_action`:

```
status: Internal, message: "Failed to deserialize header :
    OtherString(\"invalid value: integer `1949249070`,
    expected variant index 0 <= i < 2\")"
... in GrpcStore::get_part() ... Converting command_digest to Command
```

`mac-scheduler.json5` wrapped its `CAS_MAIN_STORE` in
`compression { compression_algorithm: { lz4 } } { backend: filesystem }`,
but the on-disk blobs at `${MAC_DATA_DIR}/cas/content/` were written
by combined mode's uncompressed `SHARED_CAS = fast_slow { fast:
filesystem }`. The compression layer tried to decode the raw bytes
as an LZ4 frame, the variant tag was out of range, and every read
returned `Internal` to the worker. The fix was to drop the
compression wrapper from `mac-scheduler.json5` so it matches the
on-disk format combined writes — see the chromium checkout's
`remote/mac-scheduler.json5` for the patch.

This is a **separate, pre-existing bug** that would block any
scheduler-only deployment regardless of the framing-leak issue
above. Worth treating it as its own bug to file/fix.

**Net hypothesis update after Step 2:**

1. **NEW-H2 (executor sharing) — falsified.** Combined mode is
   *better*, not worse. Removing executor sharing made everything
   worse by forcing outputs through the buggy gRPC path.

2. **NEW-H4 (gRPC blob path leaks header bytes onto on-disk content)
   — promoted to dominant suspect.** The on-disk
   `cf fa ed fe` at offset `0x18` is the smoking gun. This explains:
   - The original 1.3.4 SOLINK failure (`line_info.o: no such file or
     directory`) — different specific symptom but same family;
     possibly the prefix shifted the file into a state where
     parsing/seek failed differently.
   - The prior baseline's `.166`-correlated fallbacks (gRPC over
     tunnel always traverses the buggy framing path).
   - Why combined mode succeeds — it bypasses the gRPC path on
     same-host worker outputs.

3. NEW-H1 (FS-store eviction) — remains deprioritized; the 100 GB
   `SHARED_CAS` cap is not the issue.

4. G3/G4 (resource exhaustion) — moot once NEW-H4 explains the
   distribution.

**Recommended next steps (verification + fix):**

The fix is not a code patch — it's a one-time CAS wipe to evict the
legacy compressed blobs. See `docs/test_clean_cas_combined.md` for
the runbook.

#### Step 2.3 — clean-CAS test results (NL 1.2.0, pass 1, captured 2026-04-27)

**Hypothesis falsified for the original 1.3.4 / Step 10 missing-`.o` symptom.** Full report at `/home/kpi/devel/opensource/chromium/document_h_clean_cas_report.md`. Pass 1 alone was conclusive — passes 2 and 3 not run.

| Metric | Clean-CAS pass 1 | 1.2.0 baseline pass 1 | 1.3.4 SOLINK incident |
|---|---:|---:|---:|
| Wall | **45m51.75s** ❌ | 15m08s ✅ | aborted 33m32s ❌ |
| `local` | 229 | 9 | 130 |
| `remote` | 1820 | 2142 | 1967 |
| **`fallback`** | **228** | **1** | **129** |
| `f CXX` events | 323 | n/a | n/a |
| `^FAILED` | 1 (SOLINK) | 0 | 1 (SOLINK) |
| Build | Failure | Succeeded | Failure |

The failing SOLINK call lists `clang++: error: no such file or directory: 'obj/third_party/blink/renderer/core/core/style_recalc_context.o'` and ~9 other missing `.o`s, all in `core/core/`. **Verified on disk**: those `.o`s genuinely do not exist after the run.

The three missing files traversed siso's `S → f → F` sequence — siso fired a fallback at minute 17–28, ran a local `clang++` compile, and recorded `F CXX` (Finished). But the local fallback's output never landed at the expected on-disk path. SOLINK 17 minutes later read nothing.

This is **the original 1.3.4 / Step 10 incident's exact failure mode**, reproduced on a freshly-wiped CAS. The two failure families are now clearly distinct:

| Symptom | Where the bytes are | Root cause |
|---|---|---|
| `unhandled file type` (split test) | `.o` exists with 24-byte CompressionStore header prepended | NEW-H4 — stale legacy-compressed blob served raw via gRPC path. Wiping CAS fixes this. |
| `no such file or directory` (1.3.4 + clean-CAS) | `.o` is missing entirely | **Local-fallback output materialization** — siso records `F CXX` after a fallback but the `.o` is never on disk. **Not a CAS-content bug.** |

Wiping CAS is **not** the fix for the missing-`.o` family. The clean-CAS run produces it more, not less, because cold CAS forces more remote-action timeouts → more fallbacks → more incidents of the underlying bug.

**Net hypothesis update after Step 2.3:**

1. **NEW-H4 (gRPC blob path leaks 24-byte CompressionStore header)** — confirmed root cause for the split-test `unhandled file type` symptom. Operational fix: keep `mac-combined.json5` (no compression wrapper); the surviving April-6/7/8 compressed blobs are slowly LRU-evicting. A full CAS wipe also evicts them in one shot but at the cost of a 45+ min cold-rebuild and exposure to NEW-H5 (see below).

2. **NEW-H5 (siso local-fallback output materialization race) — promoted to dominant suspect for the 1.3.4 / Step 10 / canonical missing-`.o` failure.** The `S → f → F` with no `.o` on disk is the single most diagnostic observation. Specific places to look:
   - `siso`'s fallback codepath: when an action transitions `started_remote → fallback → local`, where does siso write the output and how does it move it to the build-expected path? Is there a tmp dir that gets cleaned out on certain conditions?
   - NL worker `running_actions_manager.rs`'s `output_files` collection: when an action falls back, are partial outputs being half-collected into CAS in a state that the next action's `has(digest)` query confuses with a successful upload?
   - Concurrent-write race on `out/Mac/obj/.../foo.o`: a remote action partially produces output, gets cancelled, the local fallback writes the same path, NL's later cleanup of the remote action's work dir touches the on-disk output.
   - `work/` aborted-action cleanup: the feedback memory `feedback_stale_work_dir_race` says stale UUIDs in `worker/work/` cause hardlink-race 100% fallback. The clean-CAS test did **not** wipe `work/` (runbook said preserve). Worth re-running with `worker/work/*` also wiped.

3. NEW-H1 (FS-store eviction) and NEW-H2 (executor sharing) — both falsified earlier; no change.

4. The "monotonic with NL uptime" signal from the 1.2.0 2-pass baseline is now reinterpreted as: cold CAS / partially-warmed fast tier → more remote-action timeouts → higher fallback rate → more occurrences of the dormant siso/NL local-fallback bug. Process age is incidental; what matters is fallback rate.

**Recommended next steps (revised):**

1. **strace one `S → f → F` event end-to-end on `.132`** to localize where the local-fallback output goes missing. Compile `clang++ ... -o obj/.../foo.o` should produce the file; if it does, then something else deletes/moves it before SOLINK. If it doesn't, the local-fallback's `-o` argument is being remapped wrong.

2. **Test wiping `worker/work/` instead of `cas/content/`** — runs much faster, and tests a cleaner hypothesis (stale-UUID hardlink race). If that fixes the bug, it confirms NEW-H5 lives in the worker's work-dir reuse logic.

3. **Read siso's local-fallback handler** in `chromium/src/build/siso/...` to find where the local compile's `-o` path is materialized.

4. **Continue to keep `mac-combined.json5` as production config** (no compression wrapper). Don't wipe `cas/content/` as a routine fix — the cost is high and it doesn't solve the canonical SOLINK bug.

#### Step 2.4 — pending: original "wipe + 3-pass" sub-plan (no longer applicable)

The original plan in this section ("wipe + restart + hot1/2/3, expect ≤5 fallbacks") was structured around the stale-CAS hypothesis and is moot now. Kept here for archaeological reference only:

1. ~~Wipe `${MAC_DATA_DIR}/cas/` on `.132` and per-worker fast tiers.~~
2. ~~Restart NL in combined mode.~~
3. ~~Run hot1 / hot2 / hot3, expect all clean.~~
4. ~~If clean → confirmed.~~ → **Confirmed *false*. See § Step 2.3.**
5. ~~If still failing → some other store layer has stale framing.~~ → No, the on-disk `.o`s are missing entirely; this is not a framing bug at all.

**Code-level hardening (separate PR, lower priority):**

- In `running_actions_manager.rs:800-811`, gate the `has(digest)`-
  skip on a `--strict-cas-format` flag (or remove it for output
  uploads); the optimization is unsound when a CAS dir has been
  reformatted in place.
- Add a startup self-check that samples a few CAS files and verifies
  they are *not* CompressionStore-framed when the configured store
  chain has no `compression` wrapper. Refuse to start if a
  contaminated blob is found, with a "wipe `cas/content/` or restore
  the wrapper" error.

**Independent items still on the table:**

- `mac-scheduler.json5`'s `compression { lz4 }` wrapper (already
  removed in the chromium checkout). Keep it removed; it would
  re-corrupt freshly-cleaned CAS dirs.
- `feedback_pkill_before_launch.md` is no longer the right framing
  for SOLINK avoidance; it stays useful for unrelated Plan I cache
  hygiene but should be re-described.
- The runbook entry "Step 10 fallback bug" attributes the failure to
  NL process age — re-attribute to "stale legacy-compressed CAS
  blobs from the April-6/7/8 `mac-combined.json5` compression
  window".

### Step 2 — vary FS-store cap to test G1 (eviction)

On a warm-fail-prone NL 1.2.0:

- Increase the `FilesystemStore` `max_bytes` cap by 5–10× via config
  on every worker. Restart and run hot1/hot2/hot3 without restart
  between iterations.
- If pass-2 fallback count drops materially → eviction-driven (G1
  confirmed). Long-term fix: pin output blobs until the driver has
  acked, or change eviction policy to favor recently-uploaded
  outputs.
- If pass-2 fallback count is unchanged → eviction is not the
  dominant driver.

### Step 3 — vary worker uptime axis directly

Same 1.2.0 binary. Do five passes back-to-back on one warm daemon,
no restart, no marker-only rebuild. Plot fallback count per pass.

- Linear growth → cumulative-state hypothesis (G1/G2/G5 family).
- Threshold → resource-exhaustion hypothesis (G3/G4 family — once
  you cross some line, behavior degrades sharply).

This is cheap and discriminating; ~80 minutes of wall.

### Step 4 — measure worker-host resources (G3/G4)

During pass 2 of the warm reproduce, sample on each NL worker host:

- `ps -o pid,rss,vsz,nlwp,etime $NL_PID` every 30s
- `lsof -p $NL_PID | wc -l` every 30s
- `vm_stat` (mac), `iostat -d 5` for disk pressure
- worker log level temporarily bumped to `debug` for the
  scheduler→worker dispatch path

If any one of these crosses an OS limit (fd ≈ rlimit, RSS pressure)
right around when fallbacks start firing, G3/G4 confirmed.

### Step 5 — output-upload trace on the worker (G2/G5)

Add a trace at the worker site that calls `BatchUpdateBlobs` for an
action's outputs. For every action, log digest, output count,
upload-elapsed, upload-result. On a warm-fail run, find the
`f CXX` actions in this log:

- If the action does not appear → its outputs were never uploaded
  (action-completion bookkeeping fault).
- If it appears with success → check whether a later
  `BatchReadBlobs` from the driver returns 404 for that digest
  (eviction or store inconsistency).
- If it appears with failure → upload itself fails on warm state
  (which structure?).

### Step 6 — only after steps 0–5: 1.3.x commit bisection

If step 0 shows 1.3.4 pass-2 fallback ≫ 1.2.0 pass-2 fallback,
bisect across `2965bc2`, `1317ee9`, `4a24bd3`, `6b4fc6a`, `e30f7bc`,
`b56fe24`, `cbfba4d`. Each pair-of-passes is ~40 min wall.
Otherwise skip — the 1.3.x commits are not the failure mode.

## What "fixed" means for each hypothesis

- NEW-H1 (FS-store eviction): pin output blobs until driver-ack, or
  cap-aware admission control on input-prep so prep never evicts a
  freshly-written output.
- NEW-H2 (output-collection race): ensure
  `action_complete_observable_to_driver` is sequenced strictly after
  output blobs are durably uploaded; never cancel an upload future
  before its blob is ack'd.
- NEW-H3 (resource exhaustion): bound the long-lived structures,
  reset background tasks at action boundary, raise worker-host fd
  limits, or run a periodic worker-process recycle on uptime
  (operational mitigation, not a fix).

## Real env-var kill switches in 1.3.4 (for reference)

| Var | File:line | Effect |
|---|---|---|
| `NATIVELINK_PLAN_I_DISABLE` | `local_worker.rs:516` | Disables hint-hardlink (Plan I) globally |
| `NATIVELINK_PLAN_M_DISABLE` | `running_actions_manager.rs:593` | No-op in 1.3.4 (Plan M removed by `cbfba4d4`) |
| `NATIVELINK_PLAN_L_PREWARM` | `plan_l_prewarm.rs:259` | Enables startup prewarm pass; default off |

These do not exist in 1.2.0 and are not on the critical path for the
1.2.0 reproduction. They are useful only if step 0 shows 1.3.4
amplifies the rate and step 6's bisection points at the Plan I/L
layer.

## Done criteria

- Step 1's trace on a single failing action localizes the failure to
  one of {execute, upload, evict, fetch}.
- Steps 2–5 confirm which of NEW-H1/H2/H3 is the dominant driver.
- A fix or operational mitigation lands and is validated by running
  hot1/hot2/hot3/hot4/hot5 back-to-back on one warm daemon without a
  SOLINK missing-`.o` failure and with fallback count flat across
  passes.
- The runbook entry for "Step 10 fallback bug" is updated to
  attribute the failure mode to NL process age (not siso), and the
  long-term mitigation replaces the manual-retry workaround.

## Open questions

- Does the same age-correlated pattern hold on a single-worker
  cluster, or only on multi-worker? (Tests whether the issue is
  worker-local or scheduler-coordinated.)
- Does forcing `--remote_jobs=10` (instead of 35) flatten the curve?
  Lower concurrency means slower accumulation of whatever resource
  is driving the rate.
- Does the curve restart after a worker briefly idles (no actions
  for, say, 10 min)? Tests whether the driver is uptime or
  cumulative-action-count.

---

## Step 2.4 — clang-shim trace evidence (2026-04-27, run #3, NL 1.3.4)

After CAS-wipe (Step 2.3) failed to fix the canonical `no such file`
failure, we deployed a clang-shim wrapper that records every
clang invocation's argv/cwd/RC/post-exec output presence to
`/tmp/nl-clang-trace/` on each Mac. Tooling lives in the chromium
checkout:

- Shim: `chromium/scripts/clang_trace_wrapper.sh`
- Makefile targets: `chromium/infra/clang-trace.mk`
  (`make clang-trace-{install,uninstall,status,clean,collect,grep}`)

### Reproduction recipe (10 min cycle, no full build needed)

The bug fires reliably when caches are warm AND siso has been
through one prior invalidation cycle. Shortest path:

1. NL up on all 3 Macs (do not restart between iterations).
2. `make clang-trace-install` (one-time).
3. `make clang-trace-clean` between runs.
4. Edit `document.h` with a fresh epoch marker.
5. `rm Chromium.app/Contents/MacOS/Chromium`.
6. `autoninja chrome --remote_jobs=35` from `.132`.
7. **Critically: kill `siso/autoninja` mid-build, edit `document.h`
   again, re-run.** The second run (with NL still up, with
   `worker/work/` artifacts from the killed run) is the high-repro
   window per user observation. Confirmed in run #3 below.

### Run #3 raw evidence

Build succeeded in 13m07s with `local:10 remote:2141 cache:1
fallback:2`. The two `f CXX` (lowercase = real fallback) events:

- Step 1820 / 11m01s: `core/core/html_media_element.o`
- Step 1909 / 11m33s: `core/exported/exported/web_view_impl.o`

Cross-referencing the shim traces for those paths shows each `.o`
was compiled successfully MULTIPLE times by clang, on different
hosts, all with `RC=0` and `EXISTS_AFTER_CLANG=yes`:

`html_media_element.o` — **3 successful clang invocations** (size
780888 bytes every time, different inodes):

| # | Host | clang start | clang done | Δ | inode |
|---|------|-------------|------------|---|-------|
| 1 | .166 | T+9m15s     | T+9m35s    | 20s | 535724496 |
| 2 | .166 | T+10m49s    | T+11m08s   | 19s | 535725102 |
| 3 | .132 | T+11m01s (local fallback) | T+11m16s | 15s | 179381949 |

`web_view_impl.o` — **2 successful clang invocations** (size 342848
bytes, different inodes):

| # | Host | clang start | clang done | Δ | inode |
|---|------|-------------|------------|---|-------|
| 1 | .166 | T+11m10s    | T+11m32s   | 22s | 535725467 |
| 2 | .132 | T+11m33s (local fallback) | T+11m56s | 23s | 179382754 |

For `html_media_element.o`, **siso re-dispatched the action twice
to .166** before falling back local. siso's local fallback fired
at T+11m01s — only 12s after .166 started attempt 2, and 26s after
.166 finished attempt 1. .166's first attempt finished correctly
on disk at T+9m35s and was simply dropped on the floor by NL/siso.

For `web_view_impl.o`, the local fallback fired **1 second** after
.166 finished — siso could not have processed .166's result before
deciding to abandon it. This is a **race or premature timeout in
the worker→scheduler→siso ActionResult delivery path**.

### What this rules out

The observed bug is **not**:
- Clang failing to write its output (every RC=0, EXISTS=yes).
- Output written to wrong path (canonical `obj/.../foo.o` every
  time).
- File unlinked after write (size + inode persist).
- `worker/work/<UUID>/` materialization race (CWD is
  `src/out/Mac` on every host, including `.166` — Plan I /
  `InputRootAbsolutePath` exact-match means clang executes
  directly in the shared input tree, not in a per-action sandbox).
- Path remap on `.166` (`/Users/general/...` → `/Users/octo/...`
  works correctly: `.166`'s clang runs in
  `/Users/general/devel/chromium-distributed-compile/src/out/Mac`
  and writes the correct relative `obj/...` path).

NEW-H5 (siso/NL local-fallback output materialization race) — the
prior dominant hypothesis — is **also falsified**. Local fallback
clang on `.132` writes its `.o` correctly every time we've
captured. The race is upstream of the local fallback.

### What this points at

**NEW-H6 (new dominant hypothesis): premature ActionResult
acknowledgement causes phantom-success dispatches.**

The pattern strongly suggests the worker (or NL between worker
and siso) is acking action completion before the output digest is
durably visible to siso's fetch path. siso then either:
- Times out waiting for the result (visible as fallback-after-1s),
  OR
- Decides the worker is unresponsive and re-dispatches (visible
  as 2× dispatch to `.166` before falling back local).

In the observed run, siso recovered (the local fallback wrote a
fresh `.o`, build succeeded). The canonical Step-10 `no such file`
failure is the same race except siso records `F CXX` (Finished)
based on a phantom-success ack — never re-dispatching, never
falling back — and SOLINK then reads a missing `.o`.

### Why `.166` is over-represented in fallbacks

`.166` runs through tailscale_punch tunnels with measurably higher
RTT than `.132`/`.133` (memory § Mac cluster maturity). siso's
per-action deadline is one global value; `.166`'s tunnel latency
makes it disproportionately likely to trip the timeout that
triggers re-dispatch / fallback. This matches the runbook's noted
"`.166` saturation" behaviour — under-feeding `.166` is one
symptom of the same timing pressure.

### Implications for code change in NL

For a 1.3.5 candidate:

1. **Worker should ack action complete only after the output blob
   is queryable** — not before. Today's fast-ack lets the
   scheduler return ActionResult to siso while the upload is
   still in flight (or never started). Look at
   `running_actions_manager.rs` `execute()` → `upload_outputs()`
   ordering.
2. **Scheduler should refuse to return ActionResult to siso
   before the output digests are present in CAS** (a
   `BatchReadBlobs` self-check, or a worker-side
   confirmation that the upload future has resolved successfully).
3. **Idempotent re-dispatch.** When siso re-dispatches an
   already-running action digest, NL should join the in-flight
   execution rather than spawning a duplicate. The shim trace
   shows duplicate `.166` invocations of the same digest — wasted
   work on top of a correctness issue.
4. **Tunnel-aware deadlines (longer-term).** Per-worker action
   deadlines tuned to `.166`'s tunnel latency would close the
   `.166` premature-fallback gap. Lower priority than 1-3.

### Forensic raw data

Trace bundles for run #3 (2150 logs across 3 hosts) live in:
`/home/kpi/devel/opensource/chromium/logs/clang-trace/192_168_88_*/`

The same shim is left installed on all 3 Macs; future iterations
should `make clang-trace-clean` between runs and
`make clang-trace-collect` after. `make clang-trace-uninstall`
restores the original `clang++` symlink when the investigation is
done.
