# GetTree input-tree prewarm — 1.3.9 adoption guide

## What it is

A worker-side optimization that drives a single REAPI `GetTree` RPC against
the slow CAS to populate the local fast store with every transitive
`Directory` proto reachable from an action's input root **before** the
recursive `download_to_directory` walk runs. After prewarm, every level of
the recursion hits the local fast store — no slow-CAS round trip for any
Directory proto.

For high-latency CAS connections (this fork's typical case: SSH-tunneled
gRPC, ~14.9 s per Directory `Read` observed at depth 17), this collapses
**D × per-Directory-RTT** on the critical path into approximately one
GetTree streaming call.

## When it helps

- **Helps:** workers connected to a remote scheduler/CAS over a high-RTT
  link, deep input trees (Chromium ~17 levels), where the depth-D
  `get_and_decode_digest::<Directory>` chain dominates `prepare_action_inputs`
  wall-clock.
- **Doesn't help:** co-located workers (sub-ms RTT), shallow trees, small
  actions. In those cases prewarm is cheap but pointless.
- **Cannot help:** when the slow CAS isn't a direct `GrpcStore` (e.g.
  wrapped in `CompressionStore` or `VerifyStore`). Prewarm detects this at
  runtime via `slow_store().downcast_ref::<GrpcStore>()` and falls through
  to legacy behavior. A `worker.input_tree_prewarm.skip` counter ticks each
  time this happens.

## How to enable

Add to your `LocalWorkerConfig`:

```jsonc
{
  "experimental_input_tree_prewarm": true
}
```

Default is `false`. The flag is honored on next worker process start.

## Code surface (where the changes live)

| File | Change |
|------|--------|
| `nativelink-store/src/ac_utils.rs` | New `cache_directory_protos` (kernel: stream of Directory protos → write to fast store under content-addressed digest) and `prewarm_input_tree` (composes `GrpcStore::get_tree` paging + `cache_directory_protos`). |
| `nativelink-store/tests/ac_utils_test.rs` | Unit tests for the kernel and the `SlowStoreNotGrpc` fallback. |
| `nativelink-config/src/cas_server.rs` | `LocalWorkerConfig::experimental_input_tree_prewarm: bool` (default `false`). |
| `nativelink-worker/src/local_worker.rs` | Plumbs `config.experimental_input_tree_prewarm` into `RunningActionsManagerArgs`. |
| `nativelink-worker/src/running_actions_manager.rs` | New `run_input_tree_prewarm` helper (free fn), `input_tree_prewarm` field on `RunningActionsManagerArgs` and `RunningActionsManagerImpl`, call site in the running-action driver immediately before the metrics-wrapped `prepare_action_inputs`. Four new `StageStats` counters under `worker.input_tree_prewarm[.ok|.skip|.err]`. |

## Invocation site

The prewarm runs **once per action**, at the running-action driver, before
`prepare_action_inputs`. It is intentionally NOT inside
`prepare_action_inputs` — adding a second `.await` chain into that hot
function bloats its async state machine and overflows the test runner stack
(observed during 1.3.9 development; that was the deciding constraint).

The prewarm takes one position in the action timeline:

```
action arrives → operation_id assigned → work_dir created
              ↓
              (if experimental_input_tree_prewarm)
              GetTree streams full Directory subtree → fast store
              ↓
              metrics().download_to_directory.wrap(prepare_action_inputs)
              ↓
              recursive walk now hits fast store at every level
              ↓
              command runs
```

## Failure modes (all non-fatal)

| Outcome | Counter | Behavior |
|---------|---------|----------|
| Slow store isn't a direct `GrpcStore` | `worker.input_tree_prewarm.skip` | trace! log; fall through to legacy walk. Expected on workers fronting compressed/verified stores. |
| `GrpcStore::get_tree` RPC error | `worker.input_tree_prewarm.err` | warn! log; fall through. Worst case: one wasted RTT before the per-Read recursion. |
| Stream tear-down mid-page | `worker.input_tree_prewarm.err` | warn! log; fall through. |
| Successful prewarm | `worker.input_tree_prewarm.ok` | trace! log; recursion proceeds with fast-store hits. |

The prewarm wall-clock is also captured in
`worker.input_tree_prewarm` (timer). Pair `ok` count with this timer to
compute average prewarm latency per action.

## Verification on a live cluster

1. Deploy 1.3.9 to one worker in the cluster (start with the slowest-link
   one). Leave others on 1.3.8.
2. Set `experimental_input_tree_prewarm: true` in that worker's config and
   restart it.
3. Watch the metrics dump for:
   - `worker.input_tree_prewarm.ok` should equal action count on that worker.
   - `worker.input_tree_prewarm.skip` should be zero (assuming a direct
     `GrpcStore` slow side; if not, fix that or accept the no-op).
   - `worker.input_tree_prewarm.err` should be zero in steady state.
   - `worker.prepare_action_inputs` mean wall-clock should drop noticeably.
   - `worker.download_to_directory.directory_proto_fetch` (if instrumented)
     should also drop — most fetches now serve from the fast store.
4. If clean for an hour of production traffic, roll to remaining workers.

## Risks worth flagging before broad rollout

- **Server-side CPU concentration.** Today's behavior amortizes Directory
  fetches across the action's wall-clock. Prewarm concentrates them into
  one server handler call (`cas_server.rs::inner_get_tree` walks recursively
  in one request). For a single worker this is fine; for several concurrent
  workers all firing prewarm at action start, peak scheduler CPU climbs.
  Watch the scheduler's CPU at rollout.
- **GetTree pagination.** The server splits responses at its
  `MAX_TREE_PAGE_SIZE`. The client driver in `prewarm_input_tree` follows
  `next_page_token` to completion — multi-page is supported but adds RTTs
  for very large trees. For Chromium-scale (~thousands of Directory protos)
  this is typically 1–2 pages.
- **Server returns Directory protos without their digests.** The prewarm
  re-encodes each proto and computes its digest under the action's hasher.
  This is REAPI-correct (Directory digests are content-addressed over the
  encoded form) and matches what `download_to_directory` expects — but only
  works because the action declares its digest function via OpenTelemetry
  context, which we read at the prewarm site.
- **No de-dup across actions.** If two actions arrive back-to-back with
  overlapping subtrees, prewarm fetches each independently. Plan L
  (path-keyed walked-stamps) handles file-level reuse but not Directory
  prewarm. A future improvement: maintain a `DashSet<DigestInfo>` of
  prewarmed root digests in `RunningActionsManagerImpl` and skip on hit.

## Rollback

Set `experimental_input_tree_prewarm: false` (or remove the key — it's the
default) and restart the worker. No data migration; the fast store entries
written by prewarm are content-addressed and indistinguishable from those
written by `download_to_directory`'s own fetches.
