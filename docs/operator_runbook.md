# Operator runbook

## Cluster topology

3 Macs, near-identical directory layouts:

| Host | Role | NL config |
|---|---|---|
| `.132` | combined-mode (scheduler + in-process worker) | `mac-combined.json5` |
| `.133` | worker | `mac-worker.json5` |
| `.166` | worker (often used as siso's local-fallback target via combined-mode) | `mac-combined.json5` |

`$USER` may differ across hosts (e.g. `octo`, `kpi`, `general`); `project_root` handles the translation.

## First-time setup

1. Checkout `v1.4_branch`, `cargo build --release --bin nativelink`.
2. On each Mac, place the binary at `~/nativelink-patched`.
3. Push the appropriate config (`mac-combined.json5` or `mac-worker.json5`).
4. Start NL on each host. It runs as a foreground process; supervise with `tmux` or `launchd`.
5. Verify all workers register against the scheduler's `WorkerApiService` endpoint.

## Per-worker config — required fields

```json5
{
  workers: [{
    local: {
      // ... existing v1.0.0 fields ...

      // 3-Mac cluster: differing $USER prefixes.
      project_root: {
        in_action: "/Users/octo",
        on_disk:   "/Users/<this-host's-user>",
      },

      // Default true. Leave on.
      experimental_digest_checked_hint_link: true,

      // ONLY on combined-mode (.132 and .166). Leave unset on .133.
      local_materialization_root:
        "/Users/<user>/chromium-distributed-compile/src/out/Mac",
    },
  }],
}
```

## AC self-check (recommended)

Add to each AC config block:

```json5
ac: [{
  instance_name: "main",
  ac_store: "main_ac",
  get_self_check_store: "main_cas",  // <-- added in 1.4
}]
```

This catches the AC-says-yes / CAS-says-no class of bugs in the field.

## Standard build cycle

1. `make cluster-prepare TAG=…` — full prep, ~10 min.
2. `make cluster-build TAG=…` — runs the build.
3. After build: `dist_rpc/verify_outputs.py` should report 0 missing.
4. If a SOLINK fails with `clang++: error: no such file or directory`:
   that's the in-process-worker materialization bug; check
   `local_materialization_root` is set on the failing host.

## Telemetry — what to watch

- `worker.plan_k.hit / .miss`: warm-build steady state. Hit rate should
  approach 100% by the second action.
- `worker.plan_i.hit / .miss`: cold-build win. Hit rate as high as the
  hint tree's coverage.
- `worker.plan_l.hit / .miss`: warm-build subtree skipping.
- `worker.cas.populate_fast_store`: should drop to near-zero on warm
  builds. A high warm value means Plan K isn't catching.

## Troubleshooting

### "AC self-check FAILED" warns

The AC entry is stale. The action will be re-run; this is the
defense-in-depth path doing its job. If you see a flood of these:

- Check the AC and CAS stores point at the same backing FS partition.
- Check that no out-of-band process is wiping CAS while AC stays.

### Plan K never warms

- Verify `experimental_digest_checked_hint_link: true`.
- Check that actions carry `InputRootAbsolutePath`.
- Confirm `project_root` translation produces an existing directory.

### Output missing on disk after a successful build

- This is the combined-mode bug closed in 1.4 by
  `local_materialization_root`. If you still see it:
  - Confirm the field is set in the failing host's config.
  - Re-deploy `nativelink-patched` 1.4 (older binaries don't have the fix).
