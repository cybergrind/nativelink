# SOLINK missing-`.o` — operational runbook

This is the **forward-looking** operational guide for investigating
the SOLINK missing-`.o` failure on the macOS NativeLink fork.

For the historical investigation log (what we tried that didn't
work, falsified hypotheses, raw evidence dumps), see the sibling
`debug_solink_missing_o.md`. This runbook is for picking up the
investigation in a future session **without** having to re-derive
context from the historical doc.

## What the bug looks like

The Chromium remote build on the 3-Mac NativeLink cluster
intermittently fails at the SOLINK step with:

```
clang++: error: no such file or directory: 'obj/third_party/blink/renderer/core/.../foo.o'
ninja: build stopped: subcommand failed.
```

The missing `.o` was reported by siso as `F CXX` (Finished
successfully). siso's local-fallback path (`f CXX`) sometimes also
appears in the log just before the failure.

## Status as of 2026-04-30 (NL 1.3.10, AC-read self-check shipped)

**A new failure shape surfaced in cycle-3 (CAS on `.166`, workers on
`.132`/`.133` over 6-tunnel tailscale): siso reports `F CXX foo.o` within
1–3 s, file not on disk, downstream SOLINK / AR fails on a different `.o`
each run. 1.3.6's self-check fires cleanly throughout — and is structurally
bypassed because it only covers `Execute → Completed`, not
`ActionCache.GetActionResult`. 1.3.10 ships the AC-read counterpart
(opt-in `AcStoreConfig.get_self_check_store`); see the
"1.3.10 — AC-read CAS self-check shipped" section near the end of this
runbook and `docs/ac_read_self_check.md` for the operator writeup. Patch
not yet redeployed to the cluster — operator must add
`get_self_check_store: "SHARED_CAS"` to the `ac` block in
`mac-combined-studio1.json5` on `.166` and redeploy.**

**Earlier defensible checkpoint (2026-04-28, NL 1.3.6 + `.132`
`max_inflight_tasks: 8`, iter 9):** scheduler Completed-arm self-check
shipped, mechanism armed, NEW-H6 has not fired in 5042 broadcasts (iters
7+8+9). Forced-repro attempted and failed (cluster death, not bug
surfacing). The gap-closing patch is in place; whether it closes a real
gap remains undetermined. The cycle-3 failure shape is **distinct from
NEW-H6** — different code path (AC read vs Completed broadcast) and
different mechanism (stale AC entry vs upload-durability race).

- **Operational fix in place**: `.132 max_inflight_tasks: 8` in
  `chromium/remote/mac-combined.json5:93` (also pushed to `.132`
  via `dist_rpc.cli put`). Iter 9 confirmed this is genuinely
  load-bearing — bumping to 16 with cold central CAS killed
  `.132` (network-offline, physical power-cycle) within ~13min.
- **NL 1.3.6 deployed** on all three Macs (`.132/.133/.166`,
  `~/nativelink-patched`). 1.3.6 adds the opt-in scheduler CAS
  self-check (commit `2546325b`).
- **Self-check enabled on `.132`** via
  `completed_cas_self_check_store: "SHARED_CAS"` in
  `mac-combined.json5`.
- **Self-check has fired 5042 times across iters 7+8+9, 0 FAILED.**
  Every `output_files.digest` of every Completed ActionResult
  verified durable in CAS before broadcast.
- The CAS-residue / lz4-framing-window hypothesis was falsified
  (clean-CAS test 2026-04-27).
- siso/NL local-fallback output materialization race (NEW-H5)
  was falsified by clang-shim trace evidence (run #3,
  2026-04-27).
- **Current state of the dominant suspect (NEW-H6)**: mechanism
  was that worker reports Completed → scheduler broadcasts to
  siso → output blob not yet durable in CAS. 1.3.6's self-check
  is engineered to catch exactly this. After 5042 broadcasts
  including one forced-repro under stress, **0 caught faults.**
  Either (a) NEW-H6 doesn't open under any condition we can
  produce on this cluster, or (b) NEW-H6 doesn't exist as
  hypothesized and the historical P2 events came from a different
  failure mode that 1.3.5→1.3.6 didn't directly address.

The worker's upload path is awaited via `try_join!` before the
ActionResult is constructed; source read on 2026-04-28 of
`fast_slow_store.rs`, `bytestream_server.rs`, `cas_server.rs`
confirms the upload chain is synchronous end-to-end. This is
consistent with hypothesis (b).

It remains possible that some past P2s came from siso's
per-action deadline firing on tunnel-bound `.166` results. That
class of failure shows as P1 (`f CXX` lowercase = "fell back",
local fallback succeeded) and is explicitly NOT a NL-side bug.

**Next P2 in the wild remains the diagnostic event we need.** With
1.3.6's logging in place, an actual fault would generate a
`CAS self-check FAILED` warn that pinpoints the digest, the
worker, and the operation_id — sufficient to root-cause without
guesswork.

## Reproduction recipe (10-15 min cycle)

The bug reproduces under the **kill-and-edit cycle** with NL
staying up across iterations. NL caches matter; do NOT restart
NL between iterations once the cluster is up.

### Pre-flight — cluster on 1.3.5 + correct `.132` config

```bash
# 1. Confirm NL version on each Mac (must be 1.3.5+)
for W in 192.168.88.132 192.168.88.133 192.168.88.166; do
  uv run python -m dist_rpc.cli run --worker "$W" --timeout 10 \
    '"$HOME/nativelink-patched" --version'
done

# 2. Confirm .132's mac-combined.json5 has max_inflight_tasks: 8
uv run python -m dist_rpc.cli run --worker 192.168.88.132 --timeout 10 \
  'grep max_inflight_tasks /Users/octo/devel/chromium-distributed-compile/remote/mac-combined.json5'
# Expect: max_inflight_tasks: 8
# If it shows 20 (the historical value before 2026-04-28),
# re-deploy the corrected config:
#   uv run python -m dist_rpc.cli put --worker 192.168.88.132 \
#     /home/kpi/devel/opensource/chromium/remote/mac-combined.json5 \
#     /Users/octo/devel/chromium-distributed-compile/remote/mac-combined.json5
# then pkill+restart .132's NL (see "NL restart on .132" below).
```

### NL restart on `.132` (when picking up a config change)

```bash
# Kill scheduler+worker on .132 only
uv run python -m dist_rpc.cli run --worker 192.168.88.132 --timeout 15 \
  "pkill -9 -f nativelink-patched; sleep 3; pgrep -f nativelink-patched | wc -l"
# Expect: 0

# Restart with combined config
uv run python -m dist_rpc.cli run --worker 192.168.88.132 --timeout 30 \
  "cd /Users/octo/devel/chromium-distributed-compile && \
   : > remote/scheduler-mac.log && \
   MAC_DATA_DIR=/Users/octo/devel/chromium-distributed-compile/remote/mac-data \
   RUST_LOG=info nohup /Users/octo/nativelink-patched \
     /Users/octo/devel/chromium-distributed-compile/remote/mac-combined.json5 \
     > remote/scheduler-mac.log 2>&1 & echo LAUNCHED"
# Expect: command times out (nohup keeps pty alive) — that's fine.

# Verify .132 listening + .133/.166 reconnected
uv run python -m dist_rpc.cli run --worker 192.168.88.132 --timeout 10 \
  "grep -aE 'listening|Worker registered|Client connected.*50061' \
     /Users/octo/devel/chromium-distributed-compile/remote/scheduler-mac.log | tail -8"
# Expect: 2× "Ready, listening on 0.0.0.0:{50051,50061}",
#         1× combined-mode local worker registered,
#         1× Client connected from 192.168.88.133,
#         1× Client connected from 127.0.0.1 (= .166 via tunnel)
# Workers re-register within ~250ms of scheduler coming up.
```

`.133` and `.166` do NOT need restart for a `.132` config
change. Their configs reference `SCHEDULER_ENDPOINT=192.168.88.132`
(or `127.0.0.1` for `.166` over tunnel) and will reconnect
automatically.

If any host is on a stale version, follow the published-release
deployment in `chromium/docs/macos/runbooks/nativelink-test-cycle.md`
Step 1, **but note**: `gh release download` on the Macs requires
`gh` to be installed via Homebrew. If `gh` is missing, fall back
to fetch-locally-then-`dist_rpc.cli put` (this is how 1.3.5 was
deployed 2026-04-28 — search for `gh release download 1.3.5
--repo cybergrind/nativelink` in commit history for the full
flow).

### Pre-flight — install the clang-shim once per cluster

```bash
cd ~/devel/opensource/chromium
make clang-trace-status   # are wrappers already installed?
make clang-trace-install  # if not, install (idempotent)
```

The shim survives across NL restarts and reboots. Leave installed
across investigation sessions; uninstall only when the
investigation is closed.

### One reproduction iteration

```bash
cd ~/devel/opensource/chromium

# 1. Wipe traces from the previous iter
make clang-trace-clean

# 2. Force SOLINK to re-run
uv run python -m dist_rpc.cli run --worker 192.168.88.132 --timeout 10 \
  "rm -f /Users/octo/devel/chromium-distributed-compile/src/out/Mac/Chromium.app/Contents/MacOS/Chromium && \
   ls /Users/octo/devel/chromium-distributed-compile/src/out/Mac/Chromium.app/Contents/MacOS/Chromium 2>&1 || echo CONFIRMED_DELETED"

# 3. Invalidate the document.h fan-out (~2000 actions)
uv run python -m dist_rpc.cli run --worker 192.168.88.132 --timeout 15 \
  "sed -i '' \"1s|^|// hot-rebuild marker iter\$(date +%s): \$(date +%s)\\\\n|\" \
   /Users/octo/devel/chromium-distributed-compile/src/third_party/blink/renderer/core/dom/document.h && \
   head -1 /Users/octo/devel/chromium-distributed-compile/src/third_party/blink/renderer/core/dom/document.h"

# 4. Kick off the build
uv run python -m dist_rpc.cli run --worker 192.168.88.132 --timeout 30 \
  "cd /Users/octo/devel/chromium-distributed-compile/src && \
   : > /tmp/chrome-hot.log && date +%s > /tmp/chrome-hot-start.txt && \
   nohup bash -c 'RBE_service_no_security=true autoninja -C out/Mac --remote_jobs=35 chrome > /tmp/chrome-hot.log 2>&1; \
     echo EXIT_CODE=\$? >> /tmp/chrome-hot.log; \
     date +%s > /tmp/chrome-hot-end.txt; \
     echo BUILD_DONE >> /tmp/chrome-hot.log' &>/dev/null & echo launched"
# Expected: dist_rpc.cli timeout (nohup keeps pty alive). That's fine.

# 5. Wait for completion
uv run python -m dist_rpc.cli run --worker 192.168.88.132 --timeout 5400 \
  "until grep -qE 'BUILD_DONE' /tmp/chrome-hot.log; do sleep 30; done; \
   echo '=== TAIL ==='; tail -30 /tmp/chrome-hot.log; \
   echo '=== FAILED ==='; grep -E 'FAILED:|no such file|unhandled file type|Build Failure' /tmp/chrome-hot.log | head -50; \
   echo '=== FALLBACKS ==='; grep -E '^\\[[0-9]+/[0-9]+\\] [0-9.]+m?[0-9.]+s f (CXX|LINK|SOLINK)' /tmp/chrome-hot.log; \
   echo '=== STATS ==='; grep -E '^local:|Build Succeeded|Build Failure|EXIT_CODE' /tmp/chrome-hot.log"
```

### The high-repro window — kill-and-edit

If iteration #1 completes with `Build Succeeded` and 0 fallbacks,
**do not stop.** The bug fires reliably on iteration #2 of a
kill-and-edit cycle. Procedure:

```bash
# Kill iter 1's siso/autoninja mid-build (or after it finishes —
# either way works). NL must stay up.
uv run python -m dist_rpc.cli run --worker 192.168.88.132 --timeout 20 \
  "pkill -9 -f autoninja; pkill -9 -f siso; pkill -9 -f 'clang.*-MMD'; sleep 5; \
   echo autoninja=\$(pgrep -f autoninja | wc -l); \
   echo siso=\$(pgrep -f siso | wc -l); \
   echo NL=\$(pgrep -f nativelink-patched | wc -l)"
# Expect: autoninja=0, siso=0, NL=3 (NL stays up — that's critical)

# Then re-do steps 1-5 with a fresh marker.
```

Run #3 captured 2 fallbacks on the kill-and-edit iteration that
the first iteration didn't produce. Repeat up to ~5 times — bug
becomes very likely after the first kill.

## Forensic capture after a failure

```bash
cd ~/devel/opensource/chromium

# 1. Pull all clang-shim traces from all 3 Macs
make clang-trace-collect
# → ~chromium/logs/clang-trace/192_168_88_{132,133,166}/

# 2. Pull the NL logs from all 3 Macs (separate dist_rpc.cli fetch each)
mkdir -p logs/nl-mac
uv run python -m dist_rpc.cli fetch --worker 192.168.88.132 \
  /Users/octo/devel/chromium-distributed-compile/remote/scheduler-mac.log \
  logs/nl-mac/132-scheduler-mac.log
uv run python -m dist_rpc.cli fetch --worker 192.168.88.133 \
  /Users/octo/devel/chromium-distributed-compile/remote/worker-mac.log \
  logs/nl-mac/133-worker-mac.log
uv run python -m dist_rpc.cli fetch --worker 192.168.88.166 \
  /Users/general/devel/chromium-distributed-compile/remote/worker-mac.log \
  logs/nl-mac/166-worker-mac.log

# 3. For each failing .o, do a digest cross-reference.
#    Pattern: filename → siso log step → clang-shim trace → NL info!
TARGET=style_recalc_context.o   # or whatever SOLINK said is missing
make clang-trace-grep TARGET=$TARGET
# Note OUT_TARGET, WALL, WALL_DONE, EXISTS_AFTER_CLANG, RC, inode, size

# 4. Cross-reference against scheduler & worker logs.
grep -aF "$TARGET" logs/nl-mac/132-scheduler-mac.log | head
grep -aF "$TARGET" logs/nl-mac/133-worker-mac.log | head
grep -aF "$TARGET" logs/nl-mac/166-worker-mac.log | head

# 5. The 1.3.5 info! lines to look for:
#
# Worker side (each worker's worker-mac.log, or .132's scheduler log
# for combined-mode):
#   "upload_results: all uploads completed"           operation_id, elapsed_ms, success
#   "upload_results: inner_upload_results completed"  num_output_files, first/last digest
#
# Scheduler side (.132 scheduler-mac.log only):
#   "scheduler: storing Completed ActionResult"       operation_id, worker_id, num_outputs,
#                                                     first/last digest, stdout digest
#
# Cross-reference these digests to the clang-shim trace's digest
# field (if logged) or to file size — sizes will match across
# duplicate compilations of the same digest.
```

## Decision tree from a captured failure

For each failing `.o` reported by SOLINK:

| Clang-shim trace shows | NL info! logs show | Diagnosis |
|---|---|---|
| **No trace match** for OUT_TARGET=*foo.o | "scheduler: storing Completed ActionResult" with the digest, but no upload_results matching it | siso accepted a phantom-success ActionResult; the action never actually ran. NL bug — likely cached ActionResult delivered without verification. |
| **Single trace** with RC=0 EXISTS=yes | "upload_results: ... completed" once, "scheduler: storing Completed" once, output digest match | Action ran, NL recorded success, but `.o` is gone at SOLINK time. Look for unlink/eviction in the FS-store between scheduler-store-Completed time and SOLINK-read time. |
| **2-3 traces** for same OUT_TARGET (run #3 shape, all RC=0 EXISTS=yes) | One or more "upload_results" finished, but a later "scheduler: storing Completed" comes from a DIFFERENT worker | Re-dispatch race. siso received the first ActionResult, found its digest unfetchable, re-dispatched. Either: NL acked before durable, OR siso's deadline is too short for `.166`'s tunnel latency. |
| **Trace with RC != 0** | upload_results "success=false" or absent | clang failed; NL or siso treated it as success. Exit-handling bug. Check the trace's stderr in the scheduler log. |

The 2-3-traces-same-OUT_TARGET pattern is what run #3 captured.
The single-trace-then-gone pattern is what the canonical Step-10
failure looks like. These are different rows in the table; both
need to be investigated when caught.

## Smallest-blast-radius fixes to consider, by row of the table

- **Phantom-success row:** add a CAS self-check in
  `nativelink-scheduler/src/simple_scheduler_state_manager.rs`
  `inner_update_operation`'s `Completed` arm before storing the
  Completed state. Walk `action_result.output_files`, call
  `cas_store.has_with_results` for each, and if any are missing
  delay+retry briefly or transition to `Queued` for re-dispatch.
  Requires plumbing a store handle through
  `SimpleSchedulerStateManager` (architectural).
- **Single-trace-gone row:** instrument FS-store eviction to log
  on every `unref()` / file delete. The bug is then either
  premature eviction (raise the cap or the `LRU` heuristic) or a
  `download_to_directory` race on the input materialization side
  consuming the file as input prep for another action.
- **Re-dispatch race row:** the same scheduler self-check helps;
  additionally consider idempotent re-dispatch in the scheduler
  (if siso re-requests an in-flight digest, join the existing
  execution). Code: see `api_worker_scheduler.rs:280-289` for
  where the scheduler currently rejects the duplicate via
  `Code::Aborted`. The siso side is already doing
  `assign_operation` correctly; the issue is **which actions
  siso decides to redispatch**, and that's a function of how
  fast NL signals success vs. when siso's deadline fires.
- **RC≠0 row:** trace the result-handling path
  (`local_worker.rs:319-329`,
  `running_actions_manager.rs:2012-2059`) for any branch that
  unconditionally treats the action as Completed instead of
  Failed.

## Tooling — files & where they live

### Chromium repo (driver / observability)

- `chromium/scripts/clang_trace_wrapper.sh` — the shim binary;
  drop-in clang++ wrapper that records argv/cwd/RC/EXISTS_AFTER
  to `/tmp/nl-clang-trace/<pid>-<ts>.log` on the Mac it runs on.
  Defensive — never breaks the build, exec's real clang
  unconditionally.
- `chromium/infra/clang-trace.mk` — Makefile targets:
  - `make clang-trace-install` — deploy + symlink-swap.
  - `make clang-trace-uninstall` — restore original.
  - `make clang-trace-status` — per-host install state.
  - `make clang-trace-clean` — wipe `/tmp/nl-clang-trace/*`.
  - `make clang-trace-collect` — tar→fetch→untar to
    `chromium/logs/clang-trace/<host>/`.
  - `make clang-trace-grep TARGET=foo.o` — print all log blocks
    whose `OUT_TARGET` matches.
- `chromium/docs/macos/runbooks/document_h_build_runbook.md` —
  the standard hot-rebuild runbook (what you'd follow in a normal
  test cycle, including pre-flight + diagnostics).
- `chromium/docs/macos/runbooks/nativelink-test-cycle.md` — the
  full cluster-cycle runbook, with the published-release
  deployment for NL upgrades.

### NL repo (the code we're investigating)

- `nativelink/docs/debug_solink_missing_o.md` — historical log
  of every hypothesis tested, raw evidence per iteration. Heavy
  context, archive material. Read for "what's been ruled out and
  why."
- `nativelink/docs/solink_missing_o_runbook.md` — this document.
  Forward-looking. What to do next time the bug fires.
- `nativelink/nativelink-worker/src/running_actions_manager.rs` —
  worker action execution + upload. The `inner_upload_results`
  function (line ~1690-1936) is where outputs are uploaded and
  the ActionResult is constructed. 1.3.5 added `info!` at:
  - `~1873`: "upload_results: all uploads completed"
  - `~1928`: "upload_results: inner_upload_results completed
    successfully" (with first/last output digest)
- `nativelink/nativelink-worker/src/local_worker.rs:319-329` —
  per-action runner that calls execute, upload_results,
  get_finished_result, then sends `UpdateWithActionStage(Completed)`
  to the scheduler.
- `nativelink/nativelink-scheduler/src/simple_scheduler_state_manager.rs` —
  state manager. `inner_update_operation` (line ~619) handles the
  worker's "complete" signal. 1.3.5 added `info!` at the
  Completed arm, with output digest summary + an explicit note
  about the missing CAS self-check.
- `nativelink/nativelink-store/src/filesystem_store.rs:796-873` —
  `emplace_file`. Synchronous via awaited `JoinHandle`. The agent
  report from 2026-04-27 hypothesized a race here between insert
  and rename, but reading the code shows the read lock is held
  through the entire `get_file_path_locked` handler. The race may
  exist somewhere subtler; deferred for now.

### Memory (Claude self-reference)

- `MEMORY.md` index entry for the SOLINK investigation —
  pointer to the project memory file.
- `project_solink_missing_o.md` — the persistent investigation
  context. Updated 2026-04-28 with NEW-H6 + 1.3.5 deployment.
- `reference_clang_trace_shim.md` — clang-shim tooling
  reference, including the decision tree for trace patterns.
- `feedback_solink_missing_o_not_cas_wipe.md` (chromium-side
  memory) — the lesson that CAS-wipe doesn't fix this; don't
  propose it again.

## What the next investigation should produce

1. A captured `no such file` failure with **all four** of:
   - the SOLINK ninja error line
   - the clang-shim traces for the failing `.o` (one or many,
     RC=0 EXISTS=yes vs other patterns)
   - `worker-mac.log` lines from each worker that touched the
     digest
   - `.132/scheduler-mac.log`'s `"scheduler: storing Completed
     ActionResult"` line for the digest
2. The ms-level timeline reconstructed from those four sources.
3. A concrete diagnosis row from the decision-tree table.
4. The smallest possible code change that addresses the
   diagnosed row.

## Hands-off checklist when starting a new investigation session

- [ ] `dist_rpc.cli list` — all 3 workers reachable
- [ ] All 3 NL on the latest tag (`1.3.5` or later)
- [ ] All 3 NL processes registered with scheduler (combined
      worker on `.132` + `.133` LAN + `.166` tunnel)
- [ ] `clang_trace_wrapper.sh` installed on all 3 (`make
      clang-trace-status`)
- [ ] `make clang-trace-clean` between iterations
- [ ] `pkill autoninja siso` between kill-and-edit iterations
- [ ] Don't restart NL between iterations (caches matter)
- [ ] Don't wipe `mac-data/cas/` (stale-blob hypothesis is
      already falsified; CAS wipe makes things worse — see
      memory `feedback_solink_missing_o_not_cas_wipe.md`)

## Live evidence — 1.3.5 first-run findings (2026-04-28, iter 1, KILLED at step 1173/5001)

First post-deployment build under the kill-and-edit cycle. NL
1.3.5 deployed and registered on all 3 hosts (`.132` combined +
`.133` LAN + `.166` tunnel), shim installed, traces wiped,
`document.h` marker'd, binary deleted, autoninja launched with
`--remote_jobs=35`.

**Killed at step 1173/5001 / 24m09s** — never reached SOLINK.
`.132` was thrashing (G3 death spiral, see below). After kill
the build state was: 148 fallbacks, 0 FAILED lines, 0 SOLINK
attempts. Forensic value: confirmed scheduler-side info!
logging works (1094 Completed broadcasts logged, 471
inner_upload_results on `.133`); did not capture P2.

**Earlier state at step 1084/5001 (~10 min in):**

- **35 fallbacks already** (`f CXX`), all in `core/`. Targets
  cluster around `core/core/*.o`, `core/exported/exported/*.o`,
  `core/animation/animation/*.o`, `core/core_hot/*.o`. Same
  subdirectory shape as runs #1-3 — `blink/renderer/core/` is
  consistently where the bug lands.
- Scheduler log already contains **1094 `storing Completed
  ActionResult`** info! lines (the new 1.3.5 logging working as
  designed). One observed worker_id is
  `192.168.88.1331f142839-5e79-6dcc-b253-0831303e0c1d`; outputs
  per Completed = 2 (`.o` + `.o.d`); `exit_code: 0`; first/last
  output digests captured.
- Worker `.133`'s log has **471 `inner_upload_results completed
  successfully`** info! lines with matching `operation_id` and
  output digests.

The pattern: worker uploads succeed (471 logged on `.133`,
others on other hosts), scheduler stores Completed (1094
logged), and yet 35 actions trigger an `f CXX` local fallback.
**That delta is the bug.** A successful worker upload + a
successful scheduler ActionResult store does not guarantee siso
can fetch the output blobs in time.

**Smoking-gun finding from `.133`'s worker startup log:**

```
nativelink_worker::local_worker: Starting worker '192.168.88.133'.
  IMPORTANT: If running multiple workers, all workers must share
  the same CAS storage path to avoid 'Object not found' errors
```

NL itself warns at startup about exactly this failure class.
Tracing the warning shows two emission sites:
- `nativelink-worker/src/local_worker.rs:543` — startup banner.
- `nativelink-store/src/fast_slow_store.rs:192` — **the actual
  error returned to clients** when a fetch hits a fast_slow
  store with no slow-path.

### Architecture analysis (2026-04-28)

Configuration map:

- **`.132` (combined)** runs scheduler + worker AND hosts the
  central CAS. `CAS_MAIN_STORE = fast_slow { fast: filesystem,
  slow: noop }`. siso fetches blobs from `.132:50051`. **If a
  digest is not in `.132`'s local `cas/content/`, the slow=noop
  path is hit and the client receives `NotFound` with the
  startup-warning text** (literally: "Object N/M not found in
  either fast or slow store. If using multiple workers, ensure
  all workers share the same CAS storage path.").
- **`.133`/`.166` (worker-only)** run with
  `WORKER_FAST_SLOW_STORE = fast_slow { fast: filesystem(local),
  slow: ref_store(GRPC_CAS_STORE→.132), fast_direction: "get" }`.
  - `fast_direction: "get"` means **uploads skip the local fast
    store** and go straight to slow=GRPC→.132. So in principle
    every blob a worker produces ends up on `.132`.
  - Reads populate the local fast on miss.

Worker upload chain code path (`fast_slow_store.rs:498-545`):

1. Worker's `update_with_whole_file(digest, file)` is called.
2. `fast.optimized_for(FileUpdates)` is true → enters the first
   block at line 514.
3. Slow store (GRPC→.132) is uploaded to via
   `slow_update_store_with_file(...).await` (line 521-534). The
   await is real — the future does not resolve until the gRPC
   put completes.
4. `fast_direction == Get` so we return **without writing to
   the local fast store** (line 536-540).

So the worker's `update_with_whole_file` returns Ok only after
`.132` has acknowledged the gRPC put.

`.132`'s gRPC server side accepts the put and routes it through
its `CAS_MAIN_STORE` (the `fast_slow{filesystem,noop}`).
`update_with_whole_file` on that store:
- Slow=noop is `NoopUpdates`-optimized → slow upload skipped.
- `fast_direction` defaults to ReadWrite → falls through to
  `fast.update_with_whole_file(...)` (line 541-544), which
  hits `FilesystemStore::update_with_whole_file →
  emplace_file`. `emplace_file` awaits its
  `background_spawn!(...).await` — the rename to final path is
  done before update_with_whole_file returns.

**By the time `.133`'s worker sees `Ok` from
`update_with_whole_file`, `.132`'s `cas/content/<digest>` file
should be at its final path.** The chain is end-to-end
synchronous. So why are we seeing 35 fallbacks?

### gRPC server-side ack timing (verified 2026-04-28)

Both write paths confirmed synchronous server-side:

- **`ByteStream.Write`** (`nativelink-service/src/bytestream_server.rs:828-837`):
  uses `try_join!(process_client_stream(...), &mut store_update_fut)`.
  The handler returns `WriteResponse` only after BOTH the client
  byte stream consumption AND `store.update(...)` have resolved.
- **`BatchUpdateBlobs`** (`cas_server.rs:159-198`): `inner_batch_update_blobs`
  awaits `store_ref.update_oneshot(...)` for each request. No
  detached spawn.

So the chain `worker → gRPC → .132 fast_slow → filesystem
emplace_file (awaited JoinHandle)` is genuinely synchronous
end-to-end. When `.133`'s worker sees `Ok` from
`update_with_whole_file`, `.132`'s `cas/content/<digest>` is at
its final path.

### Revised theory after closing Gap A

If the upload chain is fully synchronous, then siso fallbacks
are NOT triggered by "blob not yet durable." Two distinct
patterns must be separated:

**Pattern P1 — `f CXX` fallback fires, fallback recovers, build
succeeds.** Likely cause: **siso's per-action deadline expires
before the worker (especially `.166`-via-tunnel) can finish
compile + upload + Completed-broadcast.** When siso's deadline
fires, it abandons the remote dispatch and runs the action
locally. The remote dispatch may continue and complete normally;
siso ignores it. NL is not buggy in this scenario — siso is
simply impatient, especially for tunnel-bound `.166` work.

This is consistent with run #3's `web_view_impl.o` falling back
**1 second** after `.166` finished — if siso's deadline had
already fired, the 1-second timing is incidental.

This is also consistent with iter 1's 35 fallbacks all
clustered in `core/core/` — the heaviest CXX actions where
remote dispatch + tunnel latency is most likely to exceed the
deadline.

**Pattern P2 — siso records `F CXX` (uppercase, "Finished
remote") but the `.o` is missing at SOLINK time.** This is the
*canonical* Step-10 bug. Different from P1: siso is saying the
remote action SUCCEEDED, but the output isn't on disk. We have
not yet captured P2 with shim traces; run #3 was P1.

The runbook's failure-mode table should be updated: the row
labeled "siso re-dispatches to a different worker, all clangs
RC=0" was P1 — not the canonical bug. **The canonical bug
requires siso to record `F CXX` for the failing `.o`, NOT
`f CXX`.**

### Investigation pivot (next session)

1. **Confirm P1 is benign.** Verify in iter 1's build log that
   all `f CXX` fallbacks resulted in successful `F CXX` recovery
   AND the `.o` is on disk after build. If yes, P1 is just
   wasted work — not a correctness bug.
2. **Catch a P2.** Hot-rebuild + kill-and-edit cycle until SOLINK
   actually fails with `no such file`. Do NOT count `f CXX` as
   "the bug fired." Look for build log lines like:
   ```
   FAILED: Chromium clang++ ... -o ... -Lobj/...
   clang++: error: no such file or directory: 'obj/.../foo.o'
   ```
   That is P2. The shim traces for `foo.o` and the new info!
   logs in scheduler-mac.log/worker-mac.log will tell us where
   the result was supposed to come from and why siso couldn't
   read it.
3. **Tune `--remote_jobs` lower (=20)** if iter 1 is just P1.
   With a less aggressive queue depth, `.166` won't be saturated
   and siso's deadline won't fire as often. If P2 also goes
   away, the bug we've been chasing IS partially siso-deadline
   amplified by `.166` tunnel latency, and the fix is per-worker
   action deadlines (or `.166` cluster wiring with lower
   latency, e.g. faster tunnel).

### G3 — `.132` resource-exhaustion death spiral (observed iter 1 2026-04-28)

A separate, NEW failure mode surfaced when iter 1 ran on a
**cold-cache cluster** (NL had just been restarted to pick up
1.3.5; all in-process caches empty). Build at step 1173/5001
after 24m09s with **148 fallbacks accumulating at ~8/min**.
Cluster diagnostics on `.132`:

```
Load Avg: 38.48, 32.74, 23.46
CPU usage: 2.15% user, 35.17% sys, 62.66% idle
PhysMem: 23G used (2094M wired, 20G compressor), 58M unused
swapins: 1077B  swapouts: 1087B
115 clang procs (concurrent local fallbacks)
38 running, 58 stuck threads
```

This is a **memory-pressure death spiral**:

1. NL restart wiped Plan L hot tier and Plan I dir-cache.
2. First post-restart build → all input materialization goes
   through CAS gRPC → `.132`'s CAS handles huge load from
   `.133`/`.166` workers + local actions.
3. Some remote dispatches miss siso's deadline → siso falls
   back local on `.132`.
4. Each fallback spawns another local clang on `.132` (the
   already-loaded combined-mode host).
5. RAM pressure → macOS compressor activates → swapouts → I/O
   thrashing → less CPU available for NL/clang → more
   dispatches miss deadlines → more fallbacks → loop.
6. At 115 concurrent clangs, `.132` is unrecoverable until
   build is killed.

**After `pkill -9` on autoninja+siso+clang**, `.132` recovers
in seconds: 23G→6.3G PhysMem, 20G→859M compressor, 17G freed.
NL keeps running cleanly.

**This is NOT the SOLINK no-such-file bug.** The 148 fallbacks
were all P1 (deadline-driven), and most likely all would have
recovered if the build hadn't collapsed under thrashing first.
But it IS a real operational failure mode that prevents reliable
investigation of P2 (the canonical bug) on a cold cluster.

**Mitigations to test for the next investigation iteration:**

- Drop `--remote_jobs=35` → `=20` to cap concurrent fallbacks.
- Skip the cold-cache phase: run **two** back-to-back builds
  without restarting NL between them. The second build will
  have warm Plan L / Plan I and won't thrash.
- Consider standalone scheduler on `.132` (separate from the
  combined-mode local worker) to isolate the scheduler's
  dispatch path from worker thrash. Pair with `make
  remote-scheduler-standalone-mac`.
- If `.132` is the bottleneck operationally, consider moving
  the scheduler to a less-loaded host. The cluster's central
  CAS plus scheduler plus a local worker is too much for one
  M2 Pro under cold-cache + tunnel-fed CAS load.

Iter 1's lesson: **do not run a cold-cache reproduction at
`--remote_jobs=35`** under the current 3-worker topology.
Either warm caches first (one preliminary build), or lower
the concurrency.

## Iter 2 evidence (2026-04-28, after `max_inflight_tasks: 8`)

After the `.132` config change + restart, iter 2 ran cleanly:

- **Build Succeeded in 12m46s, EXIT_CODE=0**
- `local:29 remote:2035 cache:14 fallback:21 retry:0`
- `.132` peak at ~16 clang procs (vs iter 1's 115); RAM
  compressor stayed at ~600M (vs iter 1's 20G)
- 21 `f CXX` fallbacks (lowercase, P1) — all in `core/*` or
  `modules/*`, all recovered. **0 FAILED**, no SOLINK
  no-such-file failure. Same shape as run #3 from 2026-04-27.

The `max_inflight_tasks: 8` operational fix prevented the G3
death spiral. Iter 2 confirms the cluster is now stable enough
to run the kill-and-edit cycle without `.132` collapse —
investigation can proceed.

**P2 (canonical Step-10 bug) was not captured in iter 2.**
Iter 3 should be the kill-and-edit follow-up that historically
has highest P2 hit rate.

## Iter 3 — kill-and-edit, clean (2026-04-28)

After dist_rpc reconnect, iter 3 ran:

- **Build Succeeded in 10m23s, EXIT_CODE=0**
- `local:8 remote:2143 cache:1 fallback:0 retry:0`
- **0 fallbacks** (vs iter 2's 21) — *cleanest run all day*. Warm
  NL caches + `max_inflight_tasks: 8` + warm clang traces from
  iter 2.
- No P2.

## Iter 4 — second kill-and-edit, P1 fallbacks captured (2026-04-28)

- **Build Succeeded in 12m56s, EXIT_CODE=0**
- `local:24 remote:2127 cache:1 fallback:16 retry:0`
- 16 P1 fallbacks (lowercase `f CXX`), all in `core/*`, all
  recovered. **0 FAILED**, no SOLINK no-such-file.
- Forensic capture done — all 3 NL logs (~390 MB total) at
  `chromium/logs/nl-mac-iter4/`, clang-shim traces at
  `chromium/logs/clang-trace/192_168_88_*/`.

### Iter 4 ms-level timeline for `paint_layer_scrollable_area.o`

First fallback at step 1262 / 7m01s. Full chain reconstructed:

| Time UTC | Event | Source |
|---|---|---|
| 06:00:13.811 | scheduler dispatches op `5766a4f3` → `.133` | scheduler log |
| 06:00:13.881 | `.133` clang starts (shim trace) | shim |
| ~06:02:13 | **siso deadline fires (~120s after dispatch)** | inferred |
| 06:02:13.875 | `.132` local-fallback clang starts | shim |
| 06:02:27.974 | `.132` local-fallback done, RC=0, size 366120 | shim |
| 06:02:36.026 | `.133` clang done (142s remote, RC=0, same size) | shim |
| 06:02:36.124 | `.133` `inner_upload_results completed successfully` (52ms) | worker log |
| **06:02:36.183** | **scheduler `storing Completed ActionResult` — 23s after siso gave up** | scheduler log |

Same digest `e02a8b2b...366120` was already in CAS from iter 3
(uploaded 05:48:51), so the upload at 06:02:36.124 was a 52ms
no-op via `running_actions_manager.rs:800-811`'s
`has(digest)`-skip. siso correctly recorded `f CXX` (lowercase)
because it had already accepted the local fallback's result.

**Key 1.3.5 logging confirmed working as designed:** the
scheduler's `storing Completed ActionResult` line includes the
`(no CAS self-check; siso may fetch before blobs durable)` text.
The mechanism for NEW-H6 is observable; this iter just didn't
have a P2-firing siso-state.

### Why P1 fallbacks fire on `.133` — root cause

`.133`'s remote compile of `paint_layer_scrollable_area.o`
took **142 seconds** vs the local fallback's 14 seconds. Root
cause is queue saturation:

- `--remote_jobs=35` floods the cluster.
- `.166`'s tunnel latency makes its slots slower.
- Scheduler dispatches more to the LAN-fast `.133` to compensate.
- `.133` is a Mac mini (M2 Pro, 8 perf cores) — it can't keep
  up with 20+ concurrent heavy CXX compiles, so wall time per
  action balloons to >120s.
- siso's per-action deadline (~120s default) fires first → P1.

The local fallback then runs cleanly on `.132` (which is also
combined-mode but has fewer concurrent actions because
`max_inflight_tasks: 8`). 14s is the "real" compile time.

### Why iter 4 had no P2

For P2 (canonical no-such-file) the conditions seem to be:
1. Worker reports complete with output digest (we get this).
2. Scheduler stores Completed and broadcasts (we get this).
3. siso accepts the Completed as `F CXX` (uppercase).
4. siso queries CAS for the blob.
5. Blob is unfetchable (NEW-H6 firing).

In iter 4, every Completed broadcast happened **after** siso's
deadline already fired and a local fallback was in flight. siso
ignored the late broadcasts (recorded `f CXX`). For step 3 to
happen, the Completed broadcast needs to arrive *before* siso's
deadline, AND the blob needs to be unfetchable.

### Plausible mechanism for P2 (still hypothesis)

The dedup-on-write `has(digest)`-skip in
`running_actions_manager.rs:800-811` is the leading suspect: if
a stale digest existed in CAS (from a prior aborted action that
got mid-write, or from the LZ4-residue NEW-H4 era), the worker
sees `has=true` and **skips the upload entirely**. The Completed
broadcast then references a digest that may not actually have
correct content — siso fetches and gets bytes that don't parse
(`unhandled file type`) or 0-length. That would be a P2 in the
*malformed-output* class, not the *missing-file* class.

For the *missing-file* class, the most plausible mechanism is:
worker Completes with a digest that the FilesystemStore evicted
between worker-store-Ok and scheduler-broadcast-Completed.
Eviction in a 100 GB pool over 6 GB/build is unlikely (NEW-H1
falsified earlier), but cumulative across multiple builds is
plausible. Worth instrumenting eviction next.

## Iters 5/6 — clean baselines (2026-04-28)

- iter 5 (1.3.5, kill-and-edit): 10m21s, 0 fallbacks, 0 FAILED.
- iter 6 (1.3.5, kill-and-edit): 10m24s, 0 fallbacks, 0 FAILED.

Pattern across iters 2-6 under `max_inflight_tasks: 8`:
21 / 0 / 16 / 0 / 0 fallbacks. Iter 4 looks like an outlier
caused by a transient `.166` queue spike, not a systematic
condition. P2 not reproduced in any iter under these conditions.

## 1.3.6 — scheduler CAS self-check shipped (2026-04-28)

Code change: `nativelink-scheduler/src/simple_scheduler_state_manager.rs`
`inner_update_operation` Completed arm now (when configured)
calls `cas_self_check_store.has_many(output_digests)` before
storing+broadcasting Completed. On any miss, action is
re-queued (worker_id cleared, attempts++, stage=Queued)
instead of broadcasting a phantom-success ActionResult to siso.
Plumbing: new `SimpleSpec.completed_cas_self_check_store:
Option<String>` field; threaded through
`default_scheduler_factory` → `SimpleScheduler::new` →
`SimpleSchedulerStateManager::new`. Behavior is opt-in;
unconfigured deployments are byte-identical to 1.3.5.

Chromium-side enablement (`mac-combined.json5`):
`completed_cas_self_check_store: "SHARED_CAS"` on the `.132`
combined-mode scheduler. `.133`/`.166` remain on 1.3.5 binaries
(they're worker-only; the self-check is scheduler-side).

### Iter 7 — 1.3.6 baseline (cold-cache after restart)

- **Build Succeeded in 10m31s, EXIT_CODE=0**
- `local:8 remote:2143 cache:1 fallback:0 retry:0`
- **2143 self-check OK broadcasts. 0 self-check FAILED.**

Every Completed ActionResult had its output digests verified
durable in CAS before siso saw it. Same shape as iter 3/5/6
on 1.3.5.

### Iter 8 — 1.3.6 kill-and-edit

- **Build Succeeded in 10m58s, EXIT_CODE=0**
- `local:11 remote:2140 cache:1 fallback:3 retry:0`
- **3 P1 fallbacks**, all on `.166` (`internal_popup_menu.o`,
  `document_loader.o`, `layout_box_model_object.o`) with `.166`
  remote compiles 116-138s vs `.132` local fallback 12-15s.
- **4286 cumulative self-check OK broadcasts (iters 7+8).
  0 self-check FAILED.**

### Iter 9 — forced-repro attempt: death-spiral reproduced, NEW-H6 did NOT (2026-04-28)

Attempted the recipe in the next subsection (`max_inflight_tasks: 16`,
cold central CAS, `--remote_jobs=50`) to verify whether 1.3.6's
self-check actually catches a real fault. Outcome:

- **Build did NOT complete.** `.132` swap-thrashed itself into
  network-unresponsive state at T+~13min and required physical
  power-cycle. Build process group (autoninja+siso+NL) survived as
  zombies but made no progress; cluster went idle.
- **756 Completed broadcasts before death, 0 self-check FAILED.**
  Self-check fired on every Completed, every digest verified
  durable.
- **0 P2 events** before death (no `FAILED:` / `no such file or
  directory` in build log). Build was still in CXX phase; SOLINK
  actions never dispatched.
- **1 `ENHANCE_YOUR_CALM`** HTTP/2 GoAway in scheduler log,
  self-recovered in-place.
- **Death-spiral signature** (worker logs):
  - T+13:36 — first `http2 error` reading from `.132`'s slow store
  - T+13:46 — first `http2 error` uploading `.o.d` to `.132`
  - T+14:06–14:47 — cascading "Sender dropped before sending
    EOF", "transport error" on `download_to_directory`
  - T+15:23 — `.133` `UpdateForWorker stream closed early`
  - T+15:28 — `.133` `connect_worker()` cancelled (Timeout)
  - T+~17 — `.132` no longer pingable, dist_rpc disconnected

**What this proves:**

1. **The forced-repro recipe is not viable.** Combined-mode `.132`
   under cold central CAS + 16 inflight + 50 remote jobs dies of
   memory/swap exhaustion (gRPC upload firehose) before any
   SOLINK can dispatch and surface NEW-H6.
2. **Self-check is robust under stress** — 756 broadcasts, 0
   FAILED, even as the host is collapsing around it. The patch is
   not the failure mode.
3. **NEW-H6 hypothesis status: undetermined.** Self-check has now
   fired on 5042 Completed broadcasts (iters 7+8+9) without ever
   catching a fault. Either NEW-H6 doesn't fire under the
   conditions we can produce, or it doesn't exist as hypothesized.
4. **Operational fix `max_inflight_tasks: 8` is genuinely
   load-bearing**, not just "a precaution" — bumping to 16 with
   cold central CAS is catastrophic in ~13min.

### Conclusions from the 1.3.6 A/B (post iter 9)

1. **Self-check fires on every Completed**, verifying every
   `output_files.digest` via `has_many()` — confirmed by 5042+
   `self_check="CAS self-check OK"` info! lines (iters 7+8+9)
   and 0 FAILED warnings.
2. **NEW-H6 mechanism does not fire — even under stress.** Iter
   9 deliberately tried to open the race window (cold central
   CAS + `max_inflight_tasks: 16` + `--remote_jobs=50`) and
   produced 756 self-checks before `.132` died of swap
   exhaustion — none caught a fault. Source-side analysis on
   2026-04-28 of `fast_slow_store.rs`, `bytestream_server.rs`,
   `cas_server.rs` confirms the upload chain is synchronous
   end-to-end. The cumulative evidence is consistent with the
   gap not existing in the form NEW-H6 hypothesized.
3. **P1 fallbacks remain `.166`-tunnel-latency-driven.** The
   self-check is the wrong layer for this — siso's per-action
   deadline fires upstream of NL's broadcast. To reduce P1, the
   options are: longer siso deadline; don't dispatch heavy CXX
   to `.166`; or run `.166` on faster networking. None of these
   are NL-side fixes.
4. **Patch is correct, harmless, and ready as defense-in-depth.**
   Opt-in, compilation clean across workspace, all 72 scheduler
   unit tests still pass. We tried to demonstrate it *closes* a
   real bug and failed (iter 9). The patch costs nothing when
   the gap doesn't fire and gives a bright-line "FAILED" log
   line if it ever does — making it a useful instrument going
   forward, even absent confirmed fault-catching.

### How to trigger NEW-H6 — recipe failed; rethink required

**Attempted 2026-04-28 (iter 9) — failed.** The recipe below was
the leading hypothesis for forcing NEW-H6 to surface. Execution
killed `.132` (swap-thrash, network-offline, physical
power-cycle required) at T+~13min before any SOLINK dispatched.
Recorded here for completeness; **do not run again as-is**.

Failed recipe (for reference only):
1. `--remote_jobs=35 → 50` (more pressure on workers).
2. `.132 max_inflight_tasks: 8 → 16` (re-enable concurrent churn).
3. Wipe `mac-data/cas/` on `.132` (cold central CAS).
4. Hot-rebuild — cold central CAS forces every blob through the
   gRPC upload path.

Why it doesn't work: combined-mode `.132` runs scheduler + CAS +
local worker in one process. Cold central CAS forces all worker
blobs (~80% of build I/O) up through `.132`'s gRPC server while
the same process is hosting 16 concurrent local clangs. Memory
compressor + swap saturate within ~13min and the kernel becomes
network-unresponsive. NL stays alive but cannot serve gRPC; the
build deadlocks on uploads/downloads to `.132`. SOLINK actions
(near build-end) never dispatch — there is no NEW-H6 race window
to exercise.

**Implication for next attempt:** the cluster topology means we
cannot produce "cold central CAS + heavy concurrency" without
killing `.132` first. To meaningfully exercise NEW-H6 we'd need
either:
- **Split scheduler from CAS host** so the upload firehose hits a
  different process. Requires reworking `mac-combined.json5` into
  separate scheduler-only and worker-only configs and standing up
  a dedicated CAS host. Larger op-cost.
- **Force the race directly** without cold CAS. Requires reading
  the upload code path and identifying a deterministic window
  (e.g. fault-injection between worker Completed-send and CAS
  durability fence). Source-side instrumentation work.
- **Accept that NEW-H6 may not exist as hypothesized.** 5042
  Completed broadcasts across iters 7+8+9 without a single
  FAILED is consistent with the upload chain being synchronous
  end-to-end (which the source read on 2026-04-28 supports).
  Self-check then becomes defense-in-depth, not a bug fix.

The decision-tree above (case A/B/C) still stands: the next P2
sighting in the wild would discriminate between these
hypotheses. Until one occurs, default operational mode is
`max_inflight_tasks: 8` + 1.3.6 self-check both in place.

### Operational workaround (not yet validated)

Change `.132`'s `CAS_MAIN_STORE` slow path from `noop` to a
ref/grpc store that points at `.133` and `.166`. Then if
`.132`'s fast doesn't have a digest, fast_slow falls through to
ask `.133`/`.166`. Adds tunnel-latency on cache miss; closes
the symptom even if Gap A/B is still open underneath. Test
with care — the workers' fast stores have `fast_direction:
"get"` so they only contain blobs they've fetched (not their
own outputs), which is exactly the wrong direction for this
fix. Better operational path: change worker `fast_direction` to
`ReadWrite` so worker fast contains all outputs the worker
produced, then make `.132`'s slow query workers.

**Defer the operational workaround until after the post-build
forensic dump confirms which gap is firing.**

**Concrete next step on the new-info log lines:** for any
fallback `.o` after this build finishes, the correlation chain
is:

1. siso log line `[N/M] Tm.SSs f CXX obj/.../foo.o` → take
   `.o` filename
2. clang-shim trace: `make clang-trace-grep TARGET=foo.o` →
   yields one or more traces with `OUT_TARGET`, `WALL`, `RC=0`,
   `EXISTS_AFTER_CLANG=yes` lines, with file size and
   timestamps
3. From the trace's hosts/timestamps, find in
   `worker-mac.log` (or `scheduler-mac.log` for combined-mode)
   the `inner_upload_results completed successfully` line
   nearest in time. Note the `first_output_file` digest.
4. Find that digest in `scheduler-mac.log`'s `storing Completed
   ActionResult` line. Note the timestamp and worker_id.
5. siso saw the Completed but couldn't fetch the blob. The
   timestamp delta between (3) — worker says upload done — and
   (4) — scheduler stores Completed — is the durability window
   we've been hunting. If the delta is ms-small, the bug is
   that scheduler exposes the result to siso before the worker
   has actually flushed the blob to a place siso can read.

The warning above suggests the architectural fix may not be
just "gate scheduler on CAS self-check" — it may need to be
"workers must share CAS storage path." That implies the
operational fix is **also** an option: configure the cluster
with a shared CAS path (e.g. `.133` and `.166` writing to
`.132`'s `mac-data/cas/` over a network mount), eliminating
the "object on worker only" race.

Update this section with the post-build-finish forensic dump.

## 1.3.10 — AC-read CAS self-check shipped (2026-04-30)

Triggered by `bug_report.md` (cycle-3 chromium topology: siso + scheduler +
CAS on `.166`, workers on `.132`/`.133` over 6-tunnel tailscale, NL 1.3.9
with `completed_cas_self_check_store: SHARED_CAS` enabled). siso reports
`F CXX foo.o` within 1–3 s, file not on disk, downstream SOLINK / AR errors
with `no such file or directory` on a *different* `.o` each run. 1.3.6's
self-check fires cleanly throughout — and is structurally bypassed.

### Why 1.3.6's self-check doesn't catch it

The Completed-arm self-check only runs on
`Execute → Completed` (`simple_scheduler_state_manager.rs`
`inner_update_operation`). It does **not** run on
`ActionCache.GetActionResult` (`nativelink-service/src/ac_server.rs`
`inner_get_action_result`). Code read 2026-04-30:

- `ac_server.rs:178–200` — gRPC `get_action_result` entry.
- `ac_server.rs:82–119` — `inner_get_action_result`, store lookup.
- `nativelink-store/src/ac_utils.rs:47–54` — `get_and_decode_digest`,
  protobuf decode.
- → returns the decoded `ActionResult` directly. Zero validation that
  output digests are still present in CAS.

`F CXX` in 1–3 s for a non-trivial source file is the AC-hit timing
signature: real remote CXX on this cluster is 12–150 s. A stale AC entry
from cycle-2 (when each Mac had its own CAS) survives into cycle-3 (CAS
moved to `.166` and pre-staged), names output blobs that aren't in
`.166`'s `mac-data/cas/`, siso AC-hits, REAPI fetch silently materializes
nothing, SOLINK fails on the missing `.o`. Different `.o` each run because
the AC contains many stale entries; whichever is the next link bottleneck
in the build graph surfaces first.

### Patch

`nativelink-service/src/ac_server.rs::inner_get_action_result`: when
`AcStoreConfig.get_self_check_store` is configured, after decoding the
cached `ActionResult` collect every `output_files[].digest`, run
`cas.has_many(...)`, and on **any** miss return `Code::NotFound`. Client
falls back to `Execute`; 1.3.6's Completed-arm then guarantees the
freshly-produced result is durable before broadcast.

Logs mirror the Completed-arm self-check exactly:
- `ac_server: AC self-check OK` info per cache hit.
- `ac_server: AC self-check FAILED` warn per missing-digest hit, with
  `action_digest`, `instance_name`, `num_missing`, `first_missing`.
- `has_many` errors are logged as transient and the cached entry is
  served (matches scheduler behaviour — preserves availability).

Plumbing:
- New `AcStoreConfig.get_self_check_store: Option<StoreRefName>` in
  `nativelink-config/src/cas_server.rs`.
- Resolved in `AcServer::new` via `StoreManager::get_store`; stored on
  `AcStoreInfo` next to `store` and `read_only`.
- Opt-in; unset = byte-identical to upstream.

Tests: `nativelink-service/tests/ac_server_test.rs` adds
`self_check_returns_not_found_when_output_blob_missing_from_cas`,
`self_check_returns_ok_when_output_blob_present_in_cas`, and
`self_check_disabled_serves_stale_action_result`. All 7 ac_server tests
pass; full workspace builds clean.

Feature reference: see `docs/ac_read_self_check.md` for the
config-and-operate writeup (when to enable, log shapes, cost analysis).

### Activation on the chromium cluster

Add the config field to the `ac` block in `mac-combined-studio1.json5` on
`.166` and redeploy NL 1.3.10:

```json5
ac: [
  {
    ac_store: "AC_MAIN_STORE",
    get_self_check_store: "SHARED_CAS",
  },
],
```

`.132`/`.133` run worker-only and don't host the `ac` service — their
configs don't need to change.

### Validating the patch in the wild

Two cheap experiments to confirm the diagnosis (run *before* relying on
1.3.10 as the fix; the patch itself is correct as defense-in-depth either
way):

1. **AC-disabled siso run.** Configure siso to skip AC reads (force every
   action through `Execute`). If the missing-output race disappears →
   stale-AC hypothesis confirmed; 1.3.10 closes the gap.
2. **Cold-AC run.** Wipe the AC store on `.166` (only the AC, leave CAS
   intact); rerun cycle 1 to populate AC fresh; rerun cycle 2. If the
   race vanishes on cold AC and reappears once the AC has been "polluted"
   across cycles → stale-AC hypothesis confirmed.

If 1.3.10 is enabled, the diagnostic event is the
`AC self-check FAILED` warn line. Capture the `action_digest` and
`first_missing` digest from that line; cross-reference against
`.166`'s `cas/content/<sha>-<size>` to confirm the blob is genuinely
absent (not just metadata-stale in the probe path).

### What 1.3.10 does NOT address

- **Race between worker `Completed` and CAS-side blob durability over a
  remote tunnel.** Iter 9 (2026-04-28) tried to surface this and failed;
  1.3.6's self-check covers it on the Execute path. Cycle-3's CAS-on-`.166`
  topology has a larger timing window than iter-9's combined-mode `.132`,
  so this remains plausible-but-unproven. If `AC self-check FAILED` does
  *not* fire and missing-output races persist after 1.3.10 deployment, this
  is the next hypothesis to chase.
- **Worker `running_actions_manager.rs:800–811` `has(digest)`-skip
  unsoundness.** Worker skips redundant uploads when the digest is already
  in CAS; if the in-CAS blob has wrong content (left over from a torn
  prior write) the skip masks the corruption. Separate operator-safety
  concern; not on this patch's path.
- **Path-encoding silent failure in
  `nativelink-util/src/action_messages.rs:431–438` / `:866`.** `TryFrom`
  rejects `NameOrPath::Path(_)` and the conversion error is swallowed
  by `.ok()`. May be a long-standing latent dead branch with no
  observable effect, but the silent `.ok()` is a bad smell. Flagged for
  future investigation; not addressed here.

---

This document is intended to be self-contained. If anything
above is no longer accurate (e.g. tag version, code line
numbers, runbook paths), update it in place rather than
maintaining a delta. The companion `debug_solink_missing_o.md`
is the archive for "things we tried that didn't work" — keep
that section growing; keep this document evergreen.
