# Hand-off message for the next session

## Where we are right now

NL 1.3.6 is shipped + deployed (`max_inflight_tasks: 8`,
self-check on every Completed). Iter 9 attempted the documented
forced-repro recipe (cold central CAS + `max_inflight_tasks: 16`
+ `--remote_jobs=50`) and it killed `.132` — swap-thrash to
network-offline at T+~13min, requiring physical power-cycle —
without ever surfacing P2 or a self-check FAILED. Cumulative:
5042 Completed broadcasts across iters 7+8+9, zero FAILED. The
runbook now reflects this; the recipe is documented as not
viable. NEW-H6 likely doesn't exist as hypothesized; 1.3.6's
self-check is being kept as defense-in-depth instrumentation.

## Cluster state at end of this session (2026-04-28 ~11:35 local)

- `.132` was network-offline (post swap-thrash, needs physical
  power-cycle). When it returns:
  - Push the locally-edited `chromium/remote/mac-combined.json5`
    (already reverted to `max_inflight_tasks: 8` with comment
    recording the iter 9 outcome).
  - `pkill -9 -f autoninja; pkill -9 -f "siso ninja"; pkill -9
    -f nativelink-patched` to clear the stuck build process
    group from iter 9.
  - Wipe `mac-data/cas/` again (it's mid-rebuild from iter 9 —
    inconsistent state) and restart NL with the reverted config.
  - Verify scheduler listening on `:50051` + `:50061`, then
    confirm `.133`/`.166` re-register.
- `.133` and `.166` are alive (NL still running) but idle.
- Pre-iter-9 scheduler log preserved on `.132` at
  `~/devel/chromium-distributed-compile/remote/scheduler-mac.log.preforced-1777363645`
  (~57MB). Don't delete — it's the iter 7+8 evidence base.

## Goals

**Immediate (start here once `.132` is back):** rebuild the
operational baseline.

1. `dist_rpc.cli list` — all 3 reachable, `.132` back.
2. Push reverted `mac-combined.json5` to `.132`.
3. Clear iter 9's stuck procs + wipe inconsistent `mac-data/cas/`.
4. Restart NL on `.132`; verify workers reconnect.
5. `dist_rpc.cli run --worker 192.168.88.132 --timeout 10
   '"$HOME/nativelink-patched" --version'` returns `1.3.6`.
6. (Optional) hot-rebuild smoke test — should complete cleanly
   in ~15min, like iters 7+8. If yes, cluster is back to its
   "stable + monitored" state.

**Then choose one:**

- **Stay observe-only.** Wait for a P2 to fire in the wild on
  1.3.6. If/when it does, the `CAS self-check FAILED` warn
  pinpoints the digest, the worker_id, and the operation_id —
  which is sufficient for root-cause without forced repro. This
  is the parsimonious path.
- **Rethink NEW-H6 reproduction.** Two paths in the runbook's
  "How to trigger NEW-H6 — recipe failed; rethink required"
  section: (a) split scheduler from CAS host (op-cost: rework
  configs, dedicated CAS host) so cold CAS doesn't kill `.132`;
  (b) source-side fault injection between worker Completed-send
  and CAS durability fence. Both are real work; neither
  guarantees a fault to catch.
- **Move on to other items.** The worker-side
  `running_actions_manager.rs:800-811` `has(digest)`-skip
  unsoundness flag is open as an operator-safety concern.
  Tunnel-aware per-worker deadlines for `.166` would reduce
  P1 churn but doesn't affect correctness. See runbook's "Open
  questions" section.

**Overall:** ship a NativeLink fork that handles the 3-Mac
Chromium cluster reliably without operator intervention. We're
mostly there — `max_inflight_tasks: 8` + 1.3.6 self-check is
operationally stable (validated across iters 7+8 and 9's first
13min). The open question of NEW-H6's reality is now
load-bearing only on whether a future P2 sighting reveals a
real gap; without one, the patch is harmless instrumentation.

## Context files (read in this order)

**Authoritative state of the investigation:**
- `/home/kpi/devel/opensource/nativelink/docs/solink_missing_o_runbook.md`
  — evergreen runbook, includes iter 1–9 evidence, decision
  tree, post-iter-9 conclusions, "recipe failed; rethink"
  section.
- `/home/kpi/devel/opensource/nativelink/docs/debug_solink_missing_o.md`
  — historical archive of falsified hypotheses (NEW-H1/H2/H4/H5).
- `/home/kpi/.claude/projects/-home-kpi-devel-opensource-nativelink/memory/project_solink_missing_o.md`
  — auto-memory snapshot of current state.

**1.3.6 patch (source of truth on what the self-check does):**
- `/home/kpi/devel/opensource/nativelink/nativelink-config/src/schedulers.rs` — `SimpleSpec.completed_cas_self_check_store` field.
- `/home/kpi/devel/opensource/nativelink/nativelink-scheduler/src/default_scheduler_factory.rs` — store lookup + plumbing.
- `/home/kpi/devel/opensource/nativelink/nativelink-scheduler/src/simple_scheduler.rs` — `new`/`new_with_callback` constructor args.
- `/home/kpi/devel/opensource/nativelink/nativelink-scheduler/src/simple_scheduler_state_manager.rs` — `inner_update_operation` Completed arm with `has_many` self-check + re-queue path.
- Commit `2546325b`, tag `1.3.6`. View with `git log -1 -p 2546325b`.

**Cluster config (what's actually running):**
- `/home/kpi/devel/opensource/chromium/remote/mac-combined.json5` — `.132` combined-mode config; `max_inflight_tasks: 8`, `completed_cas_self_check_store: "SHARED_CAS"`. Local was reverted from iter 9's 16; push to `.132` once back online.
- `/home/kpi/devel/opensource/chromium/remote/mac-scheduler.json5` and `mac-worker.json5` — split-mode alternatives (not currently used; would be relevant if pursuing the "split scheduler from CAS host" rethink).

**Reproduction tooling (useful for next P2 in the wild):**
- `/home/kpi/devel/opensource/chromium/scripts/clang_trace_wrapper.sh` — clang-shim binary, records argv/RC/EXISTS to `/tmp/nl-clang-trace/`.
- `/home/kpi/devel/opensource/chromium/infra/clang-trace.mk` — `make clang-trace-{install,uninstall,status,clean,collect,grep}`.
- `/home/kpi/devel/opensource/chromium/docs/macos/runbooks/document_h_build_runbook.md` — standard hot-rebuild procedure.
- `/home/kpi/devel/opensource/chromium/docs/macos/runbooks/nativelink-test-cycle.md` — full cluster-cycle runbook including published-release deployment.

**Forensic evidence (iter 4 is the load-bearing example):**
- `/home/kpi/devel/opensource/chromium/logs/nl-mac-iter4/132-scheduler-mac.log` (~123 MB)
- `/home/kpi/devel/opensource/chromium/logs/nl-mac-iter4/133-worker-mac.log` (~128 MB)
- `/home/kpi/devel/opensource/chromium/logs/nl-mac-iter4/166-worker-mac.log` (~140 MB)
- `/home/kpi/devel/opensource/chromium/logs/clang-trace/192_168_88_{132,133,166}/` — 600-900 shim traces per host.

**Cluster invariants (don't change unprompted):**
- NL caches matter; do NOT restart NL between iterations once the cluster is up. (Iter 9 restart was deliberate.)
- Don't wipe `mac-data/cas/` as routine — the stale-blob hypothesis (NEW-H4) was falsified for the missing-`.o` family and CAS wipe makes things worse (cold-cache amplifies fallbacks). Iter 9 wiped it deliberately for the forced-repro experiment.
- `.132 max_inflight_tasks: 8` is the operational fix; iter 9 confirmed it is genuinely load-bearing — bumping back to 16 with cold central CAS killed the host within ~13min.

## Suggested first commands

```bash
cd /home/kpi/devel/opensource/chromium
ping -c 2 -W 2 192.168.88.132                                # is .132 back?
uv run python -m dist_rpc.cli list                           # all 3 reachable?
# If .132 reachable, push reverted config + clean restart:
uv run python -m dist_rpc.cli put --worker 192.168.88.132 \
  remote/mac-combined.json5 \
  /Users/octo/devel/chromium-distributed-compile/remote/mac-combined.json5
# Then follow the cluster-bring-up steps in nativelink-test-cycle.md
# (skip the gclient sync / gn gen — only NL needs reset).
```

Read the runbook (post-iter-9 conclusions section) and align
with the user on whether to proceed with rethink-NEW-H6 work,
move on to other items, or just stay observe-only.
