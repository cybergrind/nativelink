# Bug: local-fallback output never written to declared output path

**Affected version**: NL 1.3.11 (current `v1_branch` head: `c237afbc`).
With both `completed_cas_self_check_store` and `get_self_check_store`
enabled (1.3.10's AC-read self-check + 1.3.4's completion self-check).

**Status**: open. **Different failure mode** from the one 1.3.10
addressed; the existing fix doesn't cover this.

## Symptom

Same surface as the canonical SOLINK bug:

```
clang++: error: no such file or directory:
  'obj/third_party/swiftshader/third_party/llvm-10.0/swiftshader_llvm_most/CloneFunction.o'
```

Build aborts on first SOLINK that needs an affected `.o`.

## Why this is *not* the AC-says-yes/CAS-says-no class

`debug_solink_missing_o.md` and `solink_missing_o_runbook.md` describe
the failure where the worker fast-acks an ActionResult to the
scheduler before its CAS upload future resolves; the AC entry then
points at digests CAS doesn't have, and a downstream `BatchReadBlobs`
returns NotFound. The 1.3.10 `get_self_check_store` patch closes that
window by gating AC reads on a CAS presence check.

Today's failure (2026-05-01) flips a different invariant:

| Source of truth | State |
|---|---|
| AC store entry for action_digest `e785b591…/268` | **present** |
| CAS blob for the recorded output digest `3ef52cc5…/56784` | **present, 56784 bytes, content-addressable** |
| `out/Mac/obj/third_party/swiftshader/third_party/llvm-10.0/swiftshader_llvm_most/CloneFunction.o` on the build host | **missing** |
| `mac-data/worker/work/<sandbox>/.../CloneFunction.o` (any sandbox) | **missing** (cleaned post-action) |

Both AC and CAS are internally consistent. The bug is that the bytes
never arrived at the **local filesystem path** that siso's downstream
SOLINK action expects. With `output_local = True` patched into
`clang_unix.star`, **remote** actions on `.132` / `.133` get downloaded
back to the build host's `out/Mac/obj/...` automatically — that's how
631 of the 632 sibling `.o` files in this exact directory are present.
The one that's missing is the one that was *not* remote.

## The action-class that breaks

siso classifies the failing action as:

```
is_local = True
fallback = True       # i.e., remote dispatch failed → siso retried locally
```

(Sibling actions in the same target are `is_remote = True` with
`output_upload = 0.0`; those land on disk.)

In our session 82 actions had `LocalFallback: 82` in siso's stats line.
A single SOLINK action that depends on any one of them is enough to
abort the whole build. Nothing visibly distinguishes which one will be
the unlucky SOLINK input on a given run, so a retry is not safe.

**Important: the bug is probabilistic, not 100%**. Running the
`verify_outputs.py` script (sister chromium repo) over `siso_metrics.json`
post-failure shows only **3 of the 82 local-fallback actions** ended
up with no on-disk output:

```
=== siso_metrics.json output verification ===
  actions_with_output               12570
  fallback                             82
  local                              5221
  remote                             7309
  missing_disk                          4
  missing_local_fallback                3   ← the bug
  unclassified                         40   ← phase / no-exec records

The missing outputs (all is_local=1, fallback=1):
  obj/base/base/values.o
  obj/third_party/swiftshader/third_party/llvm-10.0/swiftshader_llvm_support/Valgrind.o
  obj/third_party/swiftshader/third_party/llvm-10.0/swiftshader_llvm_most/CloneFunction.o
  libvk_swiftshader.dylib    ← the SOLINK that aborted (downstream casualty)
```

So the local-fallback path **mostly** materializes correctly; it loses
~3.7% of outputs on this hardware/load profile. That's enough to make
every chrome build a coin flip on whether SOLINK lands on a victim.

## Hypothesis on the failing path

NL's combined-mode local worker (the in-process worker on `.166` that
siso uses when the remote dispatch fails) appears to:

1. Run `clang++` in a sandbox under `mac-data/worker/work/<uuid>/` —
   confirmed by `siso_metrics.json`: `is_local=True`, `exec=2.27 s`,
   `max_rss=318 MB`. clang+++ definitely ran.
2. Upload the output to CAS — confirmed by the digest match between
   the AC entry and the on-disk CAS blob.
3. Register an ActionResult in AC — confirmed by the AC entry.
4. **Skip writing the output to the action's declared output path on
   the local filesystem** — contradiction.
5. Tear down the sandbox — confirmed by absence of the sandbox dir
   post-build.

Step 4 is the leak. For a real remote action, siso pulls the output
from CAS via `output_local=True`. For an `is_local=True fallback=True`
action, siso treats it as already-local and does no CAS pull, while NL
treats it as already-uploaded and does no local write. Output ends up
in CAS only.

## Reproducer

A combined-mode build with non-trivial remote-dispatch failure rate
(in our case, .166 routes through tailscale tunnels with measurably
higher RTT, which drives the local-fallback rate up monotonically with
NL process uptime, per the existing runbook). Specifically:

- `mac-combined.json5` with `dir_index_redis_url` and
  `completed_cas_self_check_store` set as in this repo
- siso build (`autoninja chrome` from chromium TAG `146.0.7680.208`),
  `--remote_jobs=45`
- 22-min wallclock window with 7,309 remote, 5,221 local, 82
  local-fallback actions; first SOLINK to need any of the 82 missing
  `.o` outputs aborts the build.

Forensic verification recipe (post-failure):

```bash
# action_digest from siso_metrics.json for the failing CXX
ACTION=e785b591…
OUT=3ef52cc5…
ROOT=/Users/general/devel/chromium-distributed-compile
ls -la $ROOT/remote/mac-data/ac/content/d/$ACTION-*    # AC entry → present
ls -la $ROOT/remote/mac-data/cas/content/d/$OUT-*      # CAS blob → present
ls $ROOT/src/out/Mac/<output_path_from_metrics>        # filesystem  → MISSING
```

A repo-side verifier walks `siso_metrics.json` and reports every
declared output that's not on local disk:
[`dist_rpc/verify_outputs.py`](https://github.com/cybergrind/chromium-distributed-compile/…/dist_rpc/verify_outputs.py)
in the chromium-distributed-compile harness.

## Proposed fix (for `nativelink-patched`)

Two angles, either is sufficient:

### A. NL local-worker writes outputs to declared paths

In `nativelink-worker/src/running_actions_manager.rs` (the part
covering `LocalAction::execute`), after the CAS upload future has
resolved, also copy each declared output from the sandbox (or from
CAS) to the action's `output_files[i].path` on the local filesystem
**before** marking the action complete and tearing the sandbox down.
This restores the invariant that "an action with success in AC has
its outputs on disk on the host that ran it".

Tradeoff: this re-introduces a per-action filesystem write on the
build host. For combined-mode that's the host that's already going to
read those outputs in the next siso step, so the cost is moot — siso
would have to materialize them anyway.

### B. Scheduler tags ActionResult with `local_fallback = True` and siso treats it like remote

Less invasive on NL — set a metadata flag in the ActionResult message
when local-worker is used. Patch siso (`build/config/siso/clang_unix.star`
or its outer wrapper) to honor `output_local=True` for any action whose
ActionResult has that tag, not only true-remote actions.

This loses some of the local-fast-path benefit (siso re-fetches a blob
that was just written 50 ms ago) but is much smaller than touching
NL's worker path.

## Forensic data from the 2026-05-01 failure

- siso_metrics.json line for `CloneFunction.o`:
  - step_id=82b08230-e5ef-42d7-afd6-d03e3179edee
  - rule=clang/cxx, action=cxx
  - output=`obj/third_party/swiftshader/third_party/llvm-10.0/swiftshader_llvm_most/CloneFunction.o`
  - digest=`e785b59101a36c6197632b750c0ddd23e99d944e6b48542fc8344ea32cb10840/268`
  - is_local=True, fallback=True
  - exec=2.27 s, run=2.27, max_rss=317 865 984
- AC entry hex-decoded:
  - output[0] path = `obj/.../CloneFunction.o`, digest = `3ef52cc5…/56784`
  - output[1] path = `obj/.../CloneFunction.o.d`, digest = `c1a897cb…`
  - worker_id = `192.168.88.166-1f14572e-…` (the `.166` local worker)
- CAS file: `mac-data/cas/content/d/3ef52cc5…-56784`, 56 784 bytes,
  mtime `May 1 23:27`.
- Sibling `OptBisect.o` in the same dir: `is_remote=True`,
  `output_upload=0.0`, present on disk.
- Build stats line:
  `local:5221 remote:7309 cache:40 cache-write:0(err:0) fallback:82
   retry:0 skip:21693`. The 82 local-fallback class is the affected
  set.

Action-by-action survey from `dist_rpc/verify_outputs.py` (run
locally on `.166`, post-failure) gives the full count of missing
declared outputs and the breakdown by `is_local` / `is_remote` /
`fallback`.

## Repro environment summary (for testing the fix)

- Build host `.166` (M2 Ultra, 20 cores, 64 GB).
- Workers `.132` / `.133` reaching `.166` over tailscale_punch
  multi-tunnel `TAILSCALE_N=6`.
- Each host runs local Redis on `127.0.0.1:6379`.
- NL 1.3.11 binary downloaded from this repo's GitHub release.
- Mac-cluster start: `make cluster-prepare TAG=146.0.7680.208 &&
  make cluster-build TAG=146.0.7680.208 MIN_SHARE_EACH=1
  CHECK_DELAY_SECONDS=1800` from
  `/home/kpi/devel/opensource/chromium`.

The chromium harness's `HANDOFF.md` (commit referenced in the same
repo) contains the operator playbook to resume the cluster after this
NL fix lands.
