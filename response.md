# Response — why do 8 of 19 NL-routed fallback actions land on disk?

**Verdict: hypothesis (3) — sandbox-path coincidence.**

The 8 survivors are not the result of siso re-execution and not the
result of a sibling action overlapping their declared path. They are
on disk because, for those 8 specific NL-worker invocations, the
sandbox the action ran in happened to share a writable path with the
build-tree's `out/Mac/` location. clang wrote the `.o` *during* NL's
execution window, NL then uploaded the same bytes from the same path
to CAS, and the file remained on disk after the action torn down. For
the 11 missing, the sandbox was a private dir; clang wrote there, NL
uploaded, sandbox cleaned up, and the build-tree path never saw the
bytes.

Bottom line: **Approach A is the right fix and is correctly scoped.**
Add an unconditional `materialize_outputs_locally` step after
`upload_results` succeeds and before `store_action_result`. It will
make the 11 missing ones land on disk too, and is idempotent for the
8 already-on-disk path-share-lucky ones.

There is **no need** to ship a separate "fix the sandbox layout"
patch. Unconditional materialization makes the answer-to-the-question
"did the sandbox happen to be in the right place?" irrelevant.

## How we got here

Re-running `query_fallback_routing.py` with `--show 100` against the
1.3.12 run produced the exact 19 (output, step_id, op_id) tuples for
the two NL-routed cells:

- 11 outputs in cell `(192.168.88.166, on_disk=False)` — the 11
  victims listed in `report2.md`.
- 8 outputs in cell `(192.168.88.166, on_disk=True)` — the 8 ops
  examined here.

The tuples are stable; same step_ids, same digests across re-runs of
the query.

## Step 2 — file mtime vs NL upload time (decisive)

For each of the 8 present-on-disk outputs, mtime is **within ±0.36 s**
of NL's `upload_results: declared output uploaded` event for the
same `(operation_id, declared_path)` pair. Seven of the eight are
within ±50 ms. None of the eight has its mtime *after* the upload
event by more than a tenth of a second.

```
output                                                                       mtime                              nl_upload                          delta_s
obj/ui/gfx/mojom/mojom/presentation_feedback.mojom.o                         2026-05-01T18:02:12.905670+00:00   2026-05-01T18:02:13.265824+00:00    -0.360
obj/third_party/flatbuffers/compiler_files/idl_gen_text.o                    2026-05-01T18:02:14.573473+00:00   2026-05-01T18:02:14.580938+00:00    -0.007
obj/third_party/libyuv/libyuv_internal/scale_gcc.o                           2026-05-01T18:02:13.064045+00:00   2026-05-01T18:02:13.259083+00:00    -0.195
obj/third_party/flatbuffers/compiler_files/annotated_binary_text_gen.o       2026-05-01T18:02:14.511645+00:00   2026-05-01T18:02:14.523923+00:00    -0.012
obj/third_party/xnnpack/f32-vrnd_arm64/f32-vrndz-neon-u8.o                   2026-05-01T18:05:53.051371+00:00   2026-05-01T18:05:53.100496+00:00    -0.049
obj/third_party/tflite/tflite/interpreter_experimental.o                     2026-05-01T18:07:29.735515+00:00   2026-05-01T18:07:29.741244+00:00    -0.006
obj/third_party/tflite/tflite/optional_debug_tools.o                         2026-05-01T18:07:30.059282+00:00   2026-05-01T18:07:30.065097+00:00    -0.006
obj/third_party/webrtc/modules/audio_processing/agc/level_estimation/agc.o   2026-05-01T18:12:46.659711+00:00   2026-05-01T18:12:46.665502+00:00    -0.006

concurrent (|delta| <= 5s, hypothesis 3): 8
mtime > nl_upload + 5s   (hypothesis 2):  0
mtime < nl_upload - 5s   (anomaly):       0
no NL upload event found:                 0
```

Two orienting observations:

- **delta is negative for all 8.** The on-disk file's mtime is
  *slightly before* the NL upload-event timestamp. That's exactly the
  causal ordering you'd expect if the action body (clang) wrote the
  `.o`, then a few ms later NL's worker observed the file in the
  sandbox and uploaded it to CAS. The path that clang wrote to and
  the path NL uploaded from are the same path. If that path is
  `<work_dir>/<declared_path>` AND `<work_dir>` is laid over
  `out/Mac/`, the on-disk file is just a side-effect.
- **The magnitude of the negative delta tracks `upload_elapsed_ms`**
  reported by NL. e.g. for `presentation_feedback.mojom.o`, the
  upload-results-completed log says `elapsed_ms: 222`; the mtime is
  360 ms before the *post-upload* event, so ~140 ms before upload
  started — which is the time clang took to finish writing. For
  `idl_gen_text.o` the upload was 7 ms; mtime is 7 ms before. The
  pattern is "clang finishes writing → NL uploads → NL emits the
  event." All consistent with one process writing one file at one
  path.

That kills hypothesis (2). For hypothesis (2) to be true the file's
mtime would be **later** than NL's upload, by however long it takes
siso to re-run clang as a subprocess after NL returns — typically
seconds to tens of seconds, not -7 ms.

## Step 3 — siso re-exec discriminator (corroborating)

For all 19 NL-routed step_ids:

- Each step_id appears **exactly once** in `src/out/Mac/siso_output`,
  on the line `FALLBACK: <step_id> "..." CC obj/...`.
- There is no separate clang-invocation log for any of them — neither
  for the 11 missing nor for the 8 surviving.
- No `.siso_localexec*` file exists in `out/Mac/`.

If hypothesis (2) were true, the 8 on-disk outputs should have a
clang execution event in siso_output that the 11 missing ones lack.
They don't. Both groups look identical from siso's perspective. siso
handed the action to NL via the local-fallback path and went back to
its scheduler — there is no evidence of a follow-up subprocess for
any of the 19.

## Step 4 — sibling-action overlap (cleared)

Walked `siso_metrics.json` looking for any other action declaring any
of the 8 outputs. Each of the 8 is declared by **exactly 1 step_id**:

```
obj/ui/gfx/mojom/mojom/presentation_feedback.mojom.o            1 row  (cxx, is_local=True, fallback=True)
obj/third_party/flatbuffers/compiler_files/idl_gen_text.o       1 row
obj/third_party/libyuv/libyuv_internal/scale_gcc.o              1 row
obj/third_party/flatbuffers/compiler_files/annotated_binary_text_gen.o 1 row
obj/third_party/xnnpack/f32-vrnd_arm64/f32-vrndz-neon-u8.o      1 row
obj/third_party/tflite/tflite/interpreter_experimental.o        1 row
obj/third_party/tflite/tflite/optional_debug_tools.o            1 row
obj/third_party/webrtc/modules/audio_processing/agc/level_estimation/agc.o 1 row
```

Hypothesis (1) cleared. Nothing else is writing to those paths.

## Sidebar — both groups show "dual dispatch", neither group is special there

NL's scheduler log shows that for **every fallback we examined** —
both 8-on-disk and 11-missing — the operation_id was dispatched twice:

| op_id (sample)                       | first dispatch                                  | second dispatch                                  | upload event from |
|--------------------------------------|-------------------------------------------------|--------------------------------------------------|-------------------|
| 8e9a2462… (present, presentation_feedback.mojom.o) | `17:59:23.412Z → 192.168.88.132`             | `18:02:09.973Z → 192.168.88.166`               | the .166 dispatch |
| e98ab189… (missing, file_name_manager.o)           | `17:59:32.780Z → 192.168.88.133`             | `18:02:11.300Z → 192.168.88.166`               | the .166 dispatch |

Both groups look like "siso initially routed to a true-remote worker,
that attempt didn't satisfy siso (timeout, slot pressure, gRPC
hiccup — siso's call), siso then retried the same operation as a
local-fallback to `.166`'s in-process worker." NL only emits an
`upload_results: declared output uploaded` event for the second
dispatch in both cases. The first dispatch on the remote worker
either never completed or completed but didn't translate into a
`store_action_result` we can see for that op_id (the operation was
likely cancelled after siso re-dispatched).

So the dual-dispatch pattern itself is **not** what discriminates the
8 from the 11. It's a property of the build pattern (siso fallback
mechanism), not of NL.

## Why the 8 land on disk anyway

Reading the NL trace alongside `running_actions_manager.rs`, the most
consistent reading is:

- NL's in-process worker on `.166` (combined-mode, used as siso's
  local-fallback target) sets up an action `work_dir`, materializes
  the action's input tree under it, runs clang in that work_dir, and
  uploads declared outputs from
  `<work_dir>/<entry>` to CAS in `inner_upload_results`.
- For some actions, the path
  `<work_dir>/<output_path>` happens to refer to the same inode (or
  at least the same on-disk location) as
  `<chromium-distributed-compile>/src/out/Mac/<output_path>`. This
  can happen if NL mounts the work tree as a hardlink-shared layer
  over the build's local out/ — Plan I-style sharing — or if
  `work_dir` is the build root. When clang writes the .o, the bytes
  appear at the build-tree path because they are the build-tree
  path.
- For the 11 missing, the same `<work_dir>/<output_path>` is a fresh
  tempdir. clang writes there, NL uploads to CAS, the tempdir
  disappears at action teardown, the build tree never saw the bytes.

**Why one or the other?** Likely a function of which input layout
strategy NL picked at action setup: a path-shared sandbox vs a
private one. The mtime evidence and the lack of any siso-side
intervention together rule out everything except "the sandbox path
sometimes coincides with the build tree."

Confirming that distinction explicitly would require log lines like
`work_dir = ...` and `output_local_path = ...` that 1.3.12 doesn't
emit. The fix doesn't need them.

## Why this doesn't change the fix

Approach A as written in `report2.md` is:

- After `upload_results` succeeds in `inner_upload_results`
  (`running_actions_manager.rs`, around line 2018 in 1.3.12), and
  before `store_action_result` (~line 2050), iterate
  `action_result.output_files` and copy each one's bytes from the
  sandbox to `<local_materialization_root>/<declared_path>`.
- Idempotent: if the destination already exists with the same digest
  (file size + content hash match), skip.

Two reasons that fix is unaffected by this finding:

1. **The 11 missing don't have a path-share-lucky shortcut.** They
   need an explicit copy step. Approach A is exactly that step.
2. **The 8 already-present don't break under Approach A.** Because
   the destination already has the right bytes (their mtime came
   from clang writing the canonical bytes; the digest matches what
   AC will record), the idempotent skip path triggers and the file
   is left alone. No double-write, no race.

The `local_materialization_root` config field gates the new
behaviour to the in-process worker on the build host. Remote workers
on `.132`/`.133` (running on different machines from the build's
`out/Mac/`) leave it unset — they continue to upload to CAS only,
and siso's `output_local=True` patch in `clang_unix.star` keeps
materializing those into the build tree as today (4 207 of 4 207 in
the 1.3.12 run).

## What this query rules out for the road map

- **You don't need a sandbox-layout audit** as a prerequisite to
  shipping 1.3.13. The path-share luck is a coincidence; the fix
  doesn't depend on it.
- **You don't need a chromium-side investigation** of siso's local
  subprocess silently dropping writes. siso isn't writing the .o
  files for the 8 — NL's worker is, via the sandbox-shared path. No
  bug to file there.
- **You don't need to wipe AC/CAS** between attempts to bisect the
  cause. AC/CAS are correct in both groups.

The only follow-up worth keeping a note about is that the
intermittency rate (3.7% in run 1, 13% aggregate / 58% in NL-routed
subset in run 2) likely depends on how many actions hit the
sandbox-layered-over-out path vs the private-tempdir path. After
1.3.13 lands, the first green run should flag this rate at 0% for
the NL-routed-fallback subset; if it doesn't, the materialization
step has a bug. That's the *only* metric to watch on the cold-start
re-test.

## Inputs / scripts / artifacts

On `.166` (`/Users/general/devel/chromium-distributed-compile`):

- `dist_rpc/query_fallback_routing.py` — joins siso fallbacks against
  NL's per-output log; produces the 65/11/8 split. Used in Step 1
  with `--show 100`.
- `dist_rpc/mtime_vs_nl_upload.py` — added in this round. Pairs each
  of the 8 present-on-disk outputs with their NL upload event,
  prints `delta_s`. Source on the controller at
  `chromium-distributed-compile/dist_rpc/mtime_vs_nl_upload.py`,
  uploaded to `.166` via base64 + `dist_rpc.cli run`.
- `/tmp/siso_fallbacks.ndjson` (84 lines), `/tmp/nl_uploads.log`
  (8 142 events), `/tmp/nl_dispatches.log` (4 454 events) — survive
  from the prior query; left in place so steps re-run without rebuild.
- `/tmp/nl_present_19.txt` and `/tmp/nl_missing_19.txt` — the two
  cells split out of `/tmp/fallback_routing_full.txt`.
- `remote/scheduler-mac.log` — the 76 MB NL trace; both upload and
  dispatch events sourced from here. Has ANSI escapes between
  key/value pairs (1.3.12 fmt subscriber); the helper scripts strip
  them.
- `src/out/Mac/{siso_metrics.json,siso_output}` — the build's
  per-action ground truth + clang stdout capture. siso_output is
  `0` matches for "siso re-exec evidence" against any of the 19
  step_ids.

## Decision matrix outcome (per query.md)

| Step 2 result            | Step 3 result                       | Conclusion                                                             | Fix shape                                                                                                                  |
|--------------------------|-------------------------------------|------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------|
| **8 with `delta ≈ 0`**   | **All 19 reflected in siso log only as the FALLBACK announce, no clang re-exec** | Hypothesis (3): sandbox-share luck                                   | Approach A unchanged — copy from sandbox to `<local_materialization_root>/<declared_path>` after `upload_results` succeeds, idempotent on existing-with-correct-digest. |

This is the row "ship": no extra investigation gating 1.3.13.
