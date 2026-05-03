# Report 2 — local-fallback output not materialized to disk (verified with 1.3.12 logs)

**Affected version**: NL 1.3.12 (current `v1_branch` head; binary
deployed on Mac cluster at `2026-05-01 21:55Z`).

**Status**: 1.3.12's added logs *confirm the diagnosis* in
`local_fallback_output_not_materialized.md` from earlier today. The
patch that 1.3.10 added (`get_self_check_store`) reports `self_check:
"CAS self-check OK"` for every action that loses an output here — i.e.,
1.3.10 closes the AC-says-yes/CAS-says-no class but **not** this class.

The hot path for the failure is now visible end-to-end and there's a
clean place to drop the fix.

## Run summary (build started `2026-05-01 20:57:50` local, NL 1.3.12)

| stat | value |
|---|---|
| wall to abort | 14m 45s |
| siso steps done before abort | ~5 200 / 45 233 |
| `actions_with_output` recorded in `siso_metrics.json` | 5 362 |
| siso classification of completed actions | local 1 126 · remote 4 207 · fallback 84 |
| missing on local disk | **12** (11 of 84 fallbacks ≈ 13 %, plus the SOLINK that aborted) |

(Compared to the 1.3.11 run earlier today which had `local 5 221 ·
remote 7 309 · fallback 82` and `missing_local_fallback: 3`. The 1.3.12
run has a higher per-fallback loss rate, but the same failure
mechanism — see below; the loss rate is noisy because it depends on
how many sandboxes happen to be torn down before siso queries the
expected output path.)

The 11 missing `.o` files plus the SOLINK that aborted, from the
chromium-distributed-compile harness's `verify_outputs.py`:

```
1 0 1 clang/cxx  obj/third_party/flatbuffers/compiler_files/file_name_manager.o
1 0 1 clang/cxx  obj/third_party/flatbuffers/compiler_files/idl_gen_swift.o
1 0 1 clang/cxx  obj/third_party/flatbuffers/compiler_files/idl_gen_python.o
1 0 1 clang/cxx  obj/third_party/libyuv/libyuv_internal/scale_common.o
1 0 1 clang/cxx  obj/third_party/flatbuffers/compiler_files/idl_gen_go.o
1 0 1 clang/cxx  obj/gpu/ipc/common/interfaces_shared_cpp_sources/gpu_peak_memory.mojom-shared.o
1 0 1 clang/cxx  obj/gpu/ipc/common/interfaces_shared_cpp_sources/shared_image_metadata.mojom-shared.o
1 0 1 clang/cxx  obj/gpu/ipc/common/surface_handle_shared_cpp_sources/surface_handle.mojom-shared.o
1 0 1 clang/cxx  obj/gpu/command_buffer/common/common_sources/discardable_handle.o
1 0 1 clang/cc   obj/third_party/libvpx/libvpx_intrinsics_neon/vp9_iht4x4_add_neon.o
1 0 1 clang/cc   obj/third_party/libvpx/libvpx_intrinsics_neon/highbd_subpel_variance_neon.o
1 0 0 clang/solink libthird_party_webrtc_overrides_webrtc_component.dylib       (downstream casualty)
```

All 11 victims are `is_local=True fallback=True` C/C++ compiles. None
ran on `.132` or `.133`; all ran on the in-process worker on `.166`.

## End-to-end NL 1.3.12 trace for one victim

Picked `vp9_iht4x4_add_neon.o`, action_digest
`55122c041f2237465b6103e688e07602461cf819b543effc7bfbec793077c3bf-268`.
All log lines come from `remote/scheduler-mac.log` on `.166`
(combined-mode, so worker activity logs to scheduler log).

```
18:05:49.629751Z  api_worker_scheduler.rs:550
                  scheduler: dispatching action to worker
                  machine_id     = 192.168.88.166
                  operation_id   = a6afc8cf-2c37-4239-9ce1-cbd913c8d285

[ … running_actions_manager.rs:1537 / :1740 — sandbox + clang++ exec — ]

18:05:49.919154Z  running_actions_manager.rs:2165
                  upload_results: starting with timeout
                  upload_timeout_s = 600

18:05:49.923551Z  running_actions_manager.rs:2018
                  upload_results: all uploads completed
                  elapsed_ms = 4
                  success    = true

18:05:49.923618Z  running_actions_manager.rs:2042
                  upload_results: declared output uploaded
                  declared_path = "obj/third_party/libvpx/libvpx_intrinsics_neon/vp9_iht4x4_add_neon.o"
                  digest        = "765e383b…/1504"   ← the .o

18:05:49.923637Z  running_actions_manager.rs:2042
                  upload_results: declared output uploaded
                  declared_path = "obj/third_party/libvpx/libvpx_intrinsics_neon/vp9_iht4x4_add_neon.o.d"
                  digest        = "8f363c54…/1356"   ← the depfile

18:05:49.923652Z  upload_results: inner_upload_results completed successfully
                  total_elapsed_ms   = 4
                  num_output_files   = 2
                  num_output_folders = 0

18:05:49.950169Z  simple_scheduler_state_manager
                  scheduler: storing Completed ActionResult
                  worker_id          = 192.168.88.166-1f145872-be93-…
                  num_output_files   = 2
                  exit_code          = 0
                  first_output_digest = Some(DigestInfo("765e383b…/1504"))
                  self_check          = "CAS self-check OK"      ← 1.3.10's invariant
```

Total dispatch → AC-store latency: **320 ms**. Upload to CAS: **4 ms**.
Both AC and CAS are internally consistent; `get_self_check_store`
verifies CAS has the digest before AC commits.

But after this sequence completes:

```
$ ls /Users/general/devel/chromium-distributed-compile/src/out/Mac/\
    obj/third_party/libvpx/libvpx_intrinsics_neon/vp9_iht4x4_add_neon.o
ls: … No such file or directory

$ ls remote/mac-data/cas/content/d/765e383b…-1504
765e383b…-1504    (1 504 bytes — present)

$ ls remote/mac-data/ac/content/d/55122c041f…-268
55122c041f…-268   (612 bytes — present, references 765e383b…/1504)
```

CAS has the bytes. AC points at them. The local filesystem at
`out/Mac/obj/...` does not. 19 minutes later, siso runs the SOLINK
action that takes this path as input and gets
`clang++: error: no such file or directory`.

## What's missing from the trace

There is no log line that says "writing output `<digest>` to
`<declared_path>` on local FS". The visible flow is:

```
dispatch ──▶ sandbox + clang++ ──▶ upload_results(→CAS) ──▶ store_action_result(→AC)
                                                                │
                                          (no leg here writes to declared_path locally)
                                                                ▼
                                                            sandbox cleanup
```

For an action dispatched to a *remote* worker (`.132` / `.133`), siso
pulls the output from CAS via the `output_local = True` rule patched
into `chromium-distributed-compile/remote/clang_unix_patched.star`.
That's how 4 207 of the 4 207 remote actions in this run landed on
disk correctly. For an action dispatched to the *in-process* worker
on `.166` (combined-mode, used as siso's local-fallback target), siso
treats the action as already-local and does **not** issue that pull;
NL also does not push the output anywhere outside CAS. The bytes
remain only in CAS, and the sandbox path that briefly held them is
gone.

## Why the loss is intermittent (not 100 %)

Across the two runs in this session:

| run | NL | local | fallback | missing_local_fallback | rate |
|---|---|---|---|---|---|
| 1 | 1.3.11 | 5 221 | 82 | 3 | 3.7 % |
| 2 | 1.3.12 | 1 126 | 84 | 11 | 13.1 % |

Hypothesis: the per-action sandbox path collides with the action's
declared output path under `out/Mac/obj/...` *only some of the time*,
depending on whether the sandbox shares an inode with the local
working tree (Plan I path) or is a fresh dir. Worth checking
`running_actions_manager.rs:1537` (the action setup site shown in
the span trace above) to see whether sandbox paths are
hardlink-shared with the action's declared output dir or whether
output materialization explicitly happens after the upload.

Either way: the right fix is to make the post-upload step
unconditional.

## Proposed fix shape

Add a `materialize_outputs_locally(...)` step in
`running_actions_manager.rs` between `upload_results` and the
`store_action_result` call (~line 2018–2050 region in 1.3.12). For
each entry in the action's `output_files`, copy the bytes from the
sandbox (or hardlink-from-CAS) to `<work_dir>/<declared_path>`. This
preserves the invariant "an action with success in AC has its outputs
on disk on the host that ran it" — which is the invariant siso assumes
for any action it didn't dispatch over gRPC.

The fix is bounded to the in-process worker; remote workers running
on a separate node still have their outputs go through CAS as today.

## Repro recipe

In `chromium-distributed-compile`, branch `android_spoofing` (commit
`5184e15`):

```bash
make cluster-prepare TAG=146.0.7680.208     # full 9-step prep, ~10 min
make cluster-build   TAG=146.0.7680.208 \   # 22 ± 4 min to first failure
                     MIN_SHARE_EACH=1 \
                     CHECK_DELAY_SECONDS=1800
```

Watch for `clang++: error: no such file or directory: 'obj/...'` in
`out/Mac/siso_output`. The action_digest of any victim will appear in
`scheduler-mac.log` framed by `dispatching action to worker → upload_results: declared output uploaded → storing Completed ActionResult … self_check: "CAS self-check OK"`,
**without** any matching write to the declared path.

Drop the proposed fix in `running_actions_manager.rs`, build, ship as
`1.3.13`, and re-run the same recipe. `dist_rpc/verify_outputs.py`
should report `missing_disk: 0` post-build, and SOLINK should
complete cleanly. The `restore_outputs.py` script (sister harness)
can be used as a one-shot verifier on any future runs.

## Useful artifacts on the build host

```
.166:/Users/general/devel/chromium-distributed-compile/
├── src/out/Mac/
│   ├── siso_metrics.json          ← per-action ground truth (~9 MB)
│   ├── siso_output                ← shows clang++'s no-such-file
│   ├── siso_failed_commands.sh    ← reproducer for the SOLINK
│   └── obj/...                    ← 12 files reportedly missing
└── remote/
    ├── scheduler-mac.log          ← 76 MB; full NL 1.3.12 trace
    ├── worker-mac.log             ← 0 B (combined-mode logs to scheduler-mac.log)
    └── mac-data/
        ├── ac/content/d/55122c…-268   ← the AC entry
        └── cas/content/d/765e38…-1504 ← the CAS blob
```

## Companion artifacts in chromium-distributed-compile

- `dist_rpc/verify_outputs.py` — walks `siso_metrics.json`, reports
  every declared output that's not on local disk.
- `dist_rpc/restore_outputs.py` — fixes the symptom by copying from
  CAS to disk for each missing output. Lets the build resume past the
  SOLINK barrier even before the proper fix lands.
- `HANDOFF.md` — operator playbook to resume a Mac-cluster build after
  this NL fix ships.
