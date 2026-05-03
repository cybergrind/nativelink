# 1.4 Build Hand-off

**Date**: 2026-05-03
**Branch**: `v1.4_branch` (cut from `v1.0.0` at `42212c51`)
**Status**: Phase A + B.1 + B.2 complete and green. Phase B.3 onward not started.

## What's done

| Phase | Status | What it ships |
|---|---|---|
| A | green | macOS-arm64 release workflow (drop upstream workflows); APFS clonefile in `fs.rs::hard_link`; idempotent dst in `fs_util::hardlink_directory_tree`; exec-bit-preserving readonly; new `nativelink-util/src/perm_warn.rs` policy helper. Tests TA1–TA3 in `nativelink-util/tests/fs_*_test.rs`. |
| B.1 | green | `LocalWorkerConfig::{ project_root, experimental_digest_checked_hint_link (default true), local_materialization_root }`; `ProjectRoot { in_action, on_disk }`; free function `translate_input_root_path`. Tests TB1–TB4 in `nativelink-config/tests/project_root_test.rs`. |
| B.2 | green | New `nativelink-worker/src/input_cache.rs`: `PathDigestCache` (Plan K, stat-gated), `WalkedDirsCache` (Plan L, path-aware), `SingleFlight` (broadcast-based), `try_hint_link` (Plan I, size-short-circuit then hash). Tests TB5–TB9, TB13–TB15, TB18–TB20 in `nativelink-worker/tests/input_cache_test.rs` (12/12 green). |

**Diff vs `v1.0.0` so far**:
- Code: ~600 LOC added across `nativelink-util/`, `nativelink-config/`, `nativelink-worker/`.
- Tests: ~700 LOC.
- Workflows: −1.2k (drops) +107 (macOS).
- One drive-by baseline fix: `pub(crate)` on two `RequestComponents` structs in `nativelink-store/` so v1.0.0 compiles on rustc 1.96.

**Test status**:
- `cargo test -p nativelink-util` → 143 passed (22 suites)
- `cargo test -p nativelink-config` → 27 passed (5 suites)
- `cargo test -p nativelink-worker` → 56 passed, 1 ignored (7 suites)

## What's not done

### Phase B.3 — worker integration (the real win)

Without this, `input_cache.rs` is dead code. The integration is the actual LAN-fast win.

**Files to touch**:
- `nativelink-worker/src/running_actions_manager.rs` (3 452 LOC) — patch `download_to_directory` (lines ~124–268) and `prepare_action_inputs` (lines ~275–308). Expect ~150 added LOC.
- `nativelink-worker/src/local_worker.rs` (line ~598) — construct `Arc<InputCache>` once at manager init, plumb `project_root` from config to `RunningActionsManagerArgs`.
- `nativelink-worker/tests/running_actions_manager_test.rs` (3 887 LOC) — 3 direct call sites for `download_to_directory` (lines 228, 333, 407) need the new param.

**Concrete patch shape** (per `roadmap_1.4.md` §3.3):

```rust
// Modified signature
pub fn download_to_directory<'a>(
    cas_store: &'a FastSlowStore,
    filesystem_store: Pin<&'a FilesystemStore>,
    digest: &'a DigestInfo,
    current_directory: &'a str,
    cache: &'a InputCache,
    current_hint_dir: Option<&'a Path>,
    hasher_func: DigestHasherFunc,
) -> BoxFuture<'a, Result<(), Error>>;
```

Per-file branch (replaces lines 151–218):
1. `if cache.path_digests.contains(&dest, &digest).await { return Ok(()); }` — Plan K
2. `if cache.plan_i_enabled && let Some(hint_dir) = current_hint_dir { match try_hint_link(hint_dir, &file.name, &digest, hasher_func, dst) { Hit => mark Plan K + return, _ => fall through } }` — Plan I
3. existing `populate_fast_store + hard_link` chain
4. on success: `cache.path_digests.insert(dest_buf, digest).await?` — mark Plan K
5. existing chmod/utimes calls → wrap with `apply_chmod_or_warn` / `apply_mtime_or_warn` (Phase A helpers)

Per-subdir branch (replaces lines 221–244):
1. compute `child_hint_dir = current_hint_dir.map(|h| h.join(&directory.name))`
2. `if cache.walked_dirs.contains(&new_directory_path, &digest).await { return Ok(()); }` — Plan L
3. `cache.walk_singleflight.run((path_buf, digest), || download_to_directory(... child_hint_dir, ...)).await?` — single-flight wrap
4. on success: `cache.walked_dirs.insert(...)` — mark Plan L

`prepare_action_inputs` changes:
- Take `cache: Arc<InputCache>`, `project_root: Option<&ProjectRoot>`, `input_root_abs_path: Option<&str>` (from action's `InputRootAbsolutePath` platform property), `hasher_func: DigestHasherFunc`.
- If `input_root_abs_path` is set → translate via `translate_input_root_path(in_action, project_root)` → use as `work_directory` AND `current_hint_dir`.
- Else → `current_hint_dir = None`, work_directory unchanged.

`RunningActionsManagerImpl` field additions:
```rust
input_cache: Arc<InputCache>,            // process-shared
project_root: Option<ProjectRoot>,        // per-worker config
```

The `local_materialization_root: Option<String>` field is Phase C, not B.3.

**TDD test for B.3** (red→green):
- Add `running_actions_manager_test::download_to_directory_with_warm_plan_k_skips_cas_call` — primes Plan K with a (path, digest), then runs `download_to_directory` against a Directory proto referencing that file with a CAS store that errors on read. Assert: completes successfully → confirms PK skipped CAS.
- Add `download_to_directory_with_hint_root_uses_plan_i` — sets up hint tree, runs against CAS store that errors on read, asserts success.

### Phase C — `local_materialization_root` (~50 LOC)

Roughly 50 LOC in `inner_upload_results` after the upload-success branch. Per-output: clonefile/hardlink from sandbox → `<root>/<declared_path>`, idempotent on matching digest.

### Phase D — scheduler self-checks (~80 LOC)

Adds `SimpleSpec::completed_cas_self_check_store` and AC GET equivalent. Independent of B.3/C — could be done in parallel.

### Phase E — slim counters + cleanup (~80 LOC)

New `nativelink-util/src/counters.rs` module (named u64 atomics, no periodic dumper). Wire counters from B.3/C into call sites. Cross-phase negative invariant tests.

### Phase F — cluster bench

Requires the user's 3-Mac cluster (`.132/.133/.166`). I cannot do this from here.

### Phase G — docs cleanup, version bump, tag

Trivial once everything else is green:
- Trim `docs/` to just settings + how-it-works + a runbook.
- `docs/local_fallback_output_not_materialized.md`, `nl_context.md`, `report2.md`, `response.md` are pre-1.4 working notes — archive or delete.
- `Cargo.toml`: bump every `version = "1.0.0"` to `"1.4.0"`.
- `git tag 1.4.0 && git push origin 1.4.0` (push needs explicit user approval).

## Estimated remaining work

| Phase | Estimate |
|---|---|
| B.3 | 2–3 hours focused |
| C | 30–60 minutes |
| D | 1–2 hours (need to study scheduler code) |
| E | 30–60 minutes |
| F | depends on cluster availability |
| G | 30 minutes |

Total: half a day to a full day, plus whatever Phase F takes.

## Operating notes

- The `roadmap_1.4.md` v2 in repo root is the canonical plan. This HANDOFF tracks deltas against it.
- `pub(crate) RequestComponents` baseline-fix in `nativelink-store/` is a one-time cost — keep it.
- The macOS-arm64 release workflow on `v1_branch` will reject tags not on that branch; current work is on `v1.4_branch`. Either point the workflow at `v1.4_branch` (rename) or merge the work into `v1_branch` before tagging. **Recommended**: keep `v1.4_branch` as the new shipping branch, retire `v1_branch`.
- All new tests are red→green TDD style. The discipline has been worth it: each phase has a recorded "RED state" line in its commit message.

## Resume command

```bash
cd /home/kpi/devel/opensource/nativelink
git checkout v1.4_branch
git log --oneline | head -10   # see what landed
cat roadmap_1.4.md             # canonical plan
cat HANDOFF_1.4.md              # this file
```

Resume with Phase B.3 — see "Concrete patch shape" above.
