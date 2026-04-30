# AC-read CAS self-check (`get_self_check_store`)

NL 1.3.10 adds an opt-in CAS self-check on the `ActionCache.GetActionResult`
read path. It is the AC-hit counterpart of 1.3.6's
`completed_cas_self_check_store`, which only fires on the
`Execute → Completed` path.

## What problem it solves

`GetActionResult` returns a previously cached `ActionResult` whose
`output_files[].digest` entries name CAS blobs. Upstream NL serves the cached
entry without verifying those blobs are still present in CAS. If they aren't
— e.g. the entry was written by a previous build cycle whose worker uploaded
to a different CAS, or the blob was evicted — the client (siso/Bazel)
receives a "successful" cached `ActionResult` and then fails to materialize
the `.o` files. The next link / archive step (SOLINK / AR) errors out with
`no such file or directory`.

The 1.3.6 `completed_cas_self_check_store` does **not** cover this path: it
only validates blobs at the moment a worker reports `Completed`, which is
upstream of any AC entry being written. A stale AC entry from a prior cycle
or topology change bypasses 1.3.6 entirely.

When `get_self_check_store` is configured, every `GetActionResult` that
would otherwise return a cached entry first runs `cas.has_many(...)` on
every `output_files[].digest`. If any digest is missing, the AC read is
converted to `Code::NotFound`, forcing the client to re-Execute. The
re-Execute then runs the action fresh, and 1.3.6's Completed-arm self-check
covers correctness from there.

## Configuration

Add a single field to the `ac` block in your service config:

```json5
services: {
  ac: [
    {
      ac_store: "AC_MAIN_STORE",
      // NEW: when set, every AC hit is validated against this CAS store
      //      before being returned to the client.
      get_self_check_store: "SHARED_CAS",
    },
  ],
  // ...
}
```

The store name must reference a store entry in the top-level `stores` map.
In typical deployments the same CAS store referenced by the rest of the
service config is the right choice.

When the field is absent or empty, behaviour is byte-identical to upstream
(no validation).

## Logs

Each successful AC hit emits one of:

```
ac_server: AC self-check OK
  action_digest=...  instance_name=...  num_output_files=N
ac_server: AC self-check FAILED — cached ActionResult references output digests not present in CAS;
                                   converting hit to NotFound to force re-Execute
  action_digest=...  instance_name=...  num_missing=K  first_missing=...
```

`AC self-check FAILED` is the diagnostic event for stale-AC problems —
it pinpoints both the stale `action_digest` and the first missing output
digest. Persistent FAILED-warns indicate either an AC populated by a CAS
that no longer holds the referenced blobs (cycle/topology change), or CAS
eviction below the AC's retention horizon.

If the `has_many` call itself errors (network blip, store backend down),
the cached entry is still served and a `transient` warning is logged. This
matches the scheduler's Completed-arm behaviour and avoids degrading
availability on store hiccups.

## Cost

`has_many` is a metadata-only probe in every standard NL store implementation
(filesystem `metadata` call, gRPC `find_missing_blobs`, etc.) — no blob bytes
are read. The added per-AC-read latency on a healthy CAS is dominated by a
single round-trip to the configured store. On a remote-CAS deployment that
amortises to one extra RTT per AC hit; on a colocated CAS it's effectively
free.

## When to enable

Enable when **any** of the following is true:

- Your AC store may contain entries written by builds that uploaded to a
  different CAS than the one the AC is currently served against (cluster
  topology rearrangements, CAS migrations, multi-host setups where the AC
  is shared but the CAS effectively isn't).
- Your CAS has aggressive eviction (LRU under heavy churn) and the AC's
  retention is longer than the CAS's hot window for the same blobs.
- You are diagnosing a "F CXX in 1–3 s but `.o` not on disk" failure and
  want a bright-line log line that pinpoints the offending AC entry.

## When not to enable

You can leave it off for typical single-host or shared-storage deployments
where the AC and CAS retention are coupled. The patch is opt-in precisely so
that low-churn deployments don't pay even the small probe cost.

## Test coverage

`nativelink-service/tests/ac_server_test.rs` — three new cases:

- `self_check_returns_not_found_when_output_blob_missing_from_cas`
- `self_check_returns_ok_when_output_blob_present_in_cas`
- `self_check_disabled_serves_stale_action_result`

## Implementation pointers

- Config field: `nativelink-config/src/cas_server.rs`,
  `AcStoreConfig.get_self_check_store: Option<StoreRefName>`.
- Store resolution: `nativelink-service/src/ac_server.rs`,
  `AcServer::new` (looked up via `StoreManager::get_store`).
- Self-check logic: `nativelink-service/src/ac_server.rs`,
  `AcServer::inner_get_action_result`.
- Reference (Completed-arm self-check, mirror):
  `nativelink-scheduler/src/simple_scheduler_state_manager.rs`,
  `inner_update_operation`.
