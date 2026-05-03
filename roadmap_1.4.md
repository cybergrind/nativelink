# Roadmap 1.4 — Lean Fork from `v1.0.0`

**One sentence:** rebuild a macOS-focused, LAN-fast NativeLink fork on top of `v1.0.0` by adding only the few features the 3-Mac Chromium cluster actually needs, with red→green TDD and a hard diff cap of 4 000 added LOC.

**No commits from `1.0..1.3.13` are reused.** Concepts are taken; code is rewritten freshly against `v1.0.0` shapes.

## 1. End state

| # | Feature | Owner module | Lines (est.) |
|---|---|---|---|
| F1 | macOS APFS clonefile + idempotent hardlink-tree + exec-bit-preserving readonly | `nativelink-util/src/{fs,fs_util}.rs` | ~60 (Phase A done) |
| F2 | `perm_warn::classify_perm_result` policy helper | `nativelink-util/src/perm_warn.rs` | ~50 (Phase A done) |
| F3 | Per-worker `project_root` remap | `nativelink-config/src/cas_server.rs` + helper | ~120 |
| F4 | Plan I + Plan K + Plan L + single-flight | `nativelink-worker/src/input_cache.rs` (NEW) | ~400 |
| F5 | Worker integration (download_to_directory, prepare_action_inputs) | `nativelink-worker/src/running_actions_manager.rs` | ~150 |
| F6 | `local_materialization_root` (the 1.3.13 in-process-worker fix) | `running_actions_manager.rs::inner_upload_results` | ~50 |
| F7 | Scheduler `completed_cas_self_check_store` + AC `get_self_check_store` | scheduler-state-manager + ac_server | ~80 |
| F8 | Slim counters; no periodic dump | `nativelink-util/src/counters.rs` (NEW) | ~80 |
| F9 | macOS arm64 release workflow | `.github/workflows/macos-artifact.yaml` | (Phase A done) |

Total target: **~990 LOC** of production code + ~1 000 LOC of tests. Hard cap: 4 000 LOC vs `v1.0.0` (excluding `docs/` and `.github/`).

## 2. Non-goals (rejected on sight)

- Plan M (local Directory proto synthesis)
- Plan K disk persistence
- Plan L Redis-backed provider, machine_id namespacing, GetTree prewarm
- `dir_index`, `dir_index_resolver`, push-based `pending_outputs`, barrier/drain/heartbeat
- `dir_walk_coalescer` (single-flight in `input_cache` covers it)
- `digest_lru` (OS page cache covers it)
- Per-stage timing harness, 5s timing-dump cron, parallelized scheduler dispatch
- Per-machine dispatch counters, 100ms slow-cycle warns

## 3. Module designs

### 3.1 `input_cache.rs` (~400 LOC, the heart of Phase B)

```rust
// Plan K: in-memory only, process-shared, stat-gated.
pub struct PathDigestCache(RwLock<HashMap<PathBuf, (DigestInfo, SystemTime)>>);
impl PathDigestCache {
    pub fn new() -> Arc<Self>;
    /// Stat-gated: returns true only if (path, digest) match AND mtime
    /// matches what we recorded at insert time.
    pub async fn contains(&self, path: &Path, digest: &DigestInfo) -> bool;
    pub async fn insert(&self, path: PathBuf, digest: DigestInfo) -> Result<(), Error>;
}

// Plan L: in-memory only, path-aware key.
pub struct WalkedDirsCache(RwLock<HashSet<(PathBuf, DigestInfo)>>);
impl WalkedDirsCache {
    pub fn new() -> Arc<Self>;
    pub fn contains(&self, path: &Path, digest: &DigestInfo) -> bool;
    pub fn insert(&self, path: PathBuf, digest: DigestInfo);
}

// Single-flight: dedupe in-flight walks of the same (path, digest).
pub struct SingleFlight(Mutex<HashMap<(PathBuf, DigestInfo), broadcast::Sender<()>>>);
impl SingleFlight {
    pub fn new() -> Arc<Self>;
    pub async fn run<F, Fut>(&self, key: (PathBuf, DigestInfo), f: F) -> Result<(), Error>
    where F: FnOnce() -> Fut, Fut: Future<Output = Result<(), Error>>;
}

// Plan I: digest-checked hint-link.
pub enum HintLinkResult { Hit, MissNotFound, MissSizeMismatch, MissDigestMismatch, MissIoError(Error) }
pub async fn try_hint_link(
    hint_root: &Path, file_name: &str, expected_digest: &DigestInfo, dst: &Path,
) -> HintLinkResult;

// Bundle handed to download_to_directory.
pub struct InputCache {
    pub path_digests: Arc<PathDigestCache>,
    pub walked_dirs: Arc<WalkedDirsCache>,
    pub walk_singleflight: Arc<SingleFlight>,
    pub hint_root: Option<PathBuf>,
    pub plan_i_enabled: bool,
}
```

### 3.2 Config additions

`nativelink-config/src/cas_server.rs`:
```rust
pub struct LocalWorkerConfig {
    /* ...existing... */
    #[serde(default)] pub project_root: Option<ProjectRoot>,
    #[serde(default = "default_true")]
    pub experimental_digest_checked_hint_link: bool,
    #[serde(default)] pub local_materialization_root: Option<String>,
}
pub struct ProjectRoot { pub in_action: String, pub on_disk: String }
```

`nativelink-config/src/schedulers.rs`:
```rust
pub struct SimpleSpec {
    /* ...existing... */
    #[serde(default)] pub completed_cas_self_check_store: Option<String>,
}
```

`nativelink-config/src/stores.rs` (or wherever AC store lives):
```rust
#[serde(default)] pub get_self_check_store: Option<String>,
```

### 3.3 Worker integration patch points (against v1.0.0 line numbers)

`running_actions_manager.rs::download_to_directory` (currently 124–268, ~145 LOC):
1. Add `cache: &InputCache` param threaded through recursion.
2. Per-file (line ~151): wrap `populate_fast_store + hard_link` chain in:
   - `if cache.path_digests.contains(&dest, &digest).await { return Ok(()); }` (Plan K)
   - `if cache.plan_i_enabled { match try_hint_link(...) { Hit => mark Plan K + return, _ => fall through } }` (Plan I)
   - existing CAS path
   - on success: `cache.path_digests.insert(...).await`
   - replace `fs::set_permissions` and mtime calls with `apply_chmod_or_warn` / `apply_mtime_or_warn` (Phase A helpers)
3. Per-subdir (line ~221): wrap recursion in:
   - `if cache.walked_dirs.contains(&new_directory_path, &digest) { return Ok(()); }` (Plan L)
   - `cache.walk_singleflight.run((path, digest), || download_to_directory_inner(...)).await?`
   - on success: `cache.walked_dirs.insert(...)`

`running_actions_manager.rs::prepare_action_inputs` (currently 275–~325):
- Take `cache: &InputCache` param.
- If platform property `InputRootAbsolutePath` is set, derive `hint_root` and (with `project_root`) compute the on-disk path; use as `work_directory` *and* `cache.hint_root`.
- Otherwise set `cache.hint_root = None` (Plan I disabled for this action).

`running_actions_manager.rs::inner_upload_results` (Phase C, ~50 LOC):
- After all uploads succeed, before `store_action_result`:
- `if let Some(root) = &local_materialization_root { for each output: idempotent_hardlink(sandbox_path, root.join(declared_path), digest)? }`

### 3.4 Scheduler self-checks (Phase D)

`simple_scheduler_state_manager.rs::inner_update_operation` Completed-arm:
```rust
if let Some(store_name) = &spec.completed_cas_self_check_store {
    let store = ctx.store_lookup(store_name)?;
    let digest = action_result.first_output_file_digest();
    if !store.has_many(&[digest]).await?[0].is_some() {
        warn!(/* CAS self-check FAILED */);
        // re-queue
    }
}
```

`ac_server.rs::inner_get_action_result`:
```rust
if let Some(store_name) = &spec.get_self_check_store {
    let cas = ctx.store_lookup(store_name)?;
    if !cas.has_many(&[ac_result.first_output_digest()]).await?[0].is_some() {
        return Err(NotFound);  // invalidate stale AC entry
    }
}
```

## 4. Execution order

Each phase = one red commit + one green commit. CI must be green at every commit.

| # | Phase | Subject | Cumulative LOC |
|---|---|---|---:|
| done | A | macOS workflow + fs/fs_util/perm_warn | 330 |
| 1 | B.1 red | ProjectRoot config + translate test (failing: type/fn missing) | 380 |
| 2 | B.1 green | ProjectRoot struct + `translate_input_root_path` helper | 460 |
| 3 | B.2 red | input_cache module skeleton (compiles, all impls return `unimplemented!()`) + 12 unit tests | 1 060 |
| 4 | B.2 green | input_cache real implementations | 1 260 |
| 5 | B.3 red | running_actions_manager integration tests (failing: cache param missing) | 1 460 |
| 6 | B.3 green | download_to_directory + prepare_action_inputs threaded with cache | 1 610 |
| 7 | C red | local_materialization_root tests (failing: field missing) | 1 760 |
| 8 | C green | post-upload materialization loop | 1 810 |
| 9 | D red | scheduler self-check tests (failing) | 1 910 |
| 10 | D green | impl in state_manager + ac_server | 1 990 |
| 11 | E | slim counters module + revert thresholds + invariant tests | 2 070 |
| 12 | F | (cluster bench — user runs) | 2 070 |
| 13 | G | docs cleanup, version bump 1.4.0, tag | 2 100 |

## 5. Tests (~30 total)

| ID range | File | Asserts |
|---|---|---|
| TA1–3 | `nativelink-util/tests/fs_*_test.rs` | clonefile, idempotent dst, perm-warn classifier (done) |
| TB1–4 | `nativelink-config/tests/project_root_test.rs` | identity/remap/no-match/serde-round-trip |
| TB5–9 | `nativelink-worker/tests/input_cache_test.rs` § hint-link | hash match/mismatch, size short-circuit, missing path, IO error |
| TB13–15 | same file § path-digest-cache | insert→contains, digest-mismatch miss, stat-gate eviction |
| TB18–21 | same file § walked-dirs + single-flight | hit-skip, path-aware, single-flight under N=100, bulk-mark |
| TB23–25 | `nativelink-worker/tests/running_actions_manager_test.rs` | cold complete-tree → 0 CAS file reads, warm → 0 per-file work, stale → fallback correct |
| TC1–6 | same file | unset no-op, creates missing, idempotent on match, replace on mismatch, nested dirs, failure non-blocking |
| TD1–3 | `nativelink-scheduler/tests/state_manager_test.rs` | self-check pass/fail/off |
| TD4–6 | `nativelink-service/tests/ac_server_test.rs` | get-self-check pass/invalidate/off |
| TE1–5 | `nativelink-util/tests/counters_test.rs` | counter API + cross-phase negative invariants (modules-not-resolves) |

## 6. Risks & mitigations

| Risk | Mitigation |
|---|---|
| Phase B.3 integration is bigger than estimated | Sub-split: B.3a thread the cache parameter (mechanical, no behavior change, existing tests stay green) → B.3b add Plan K/I/L logic per patch-point |
| Plan L key vs project_root remap | Always key by *post-translation* on-disk path. Documented test covers it. |
| `directory_cache.rs` (530 LOC, already in v1.0.0) overlaps with WalkedDirsCache | They're complementary: directory_cache is whole-tree-exact; WalkedDirsCache is per-subdir. Top of `prepare_action_inputs` checks directory_cache first; on miss, recurses into download_to_directory which uses WalkedDirsCache. |
| Phase F bench requires user hardware | Hard dependency — user runs it. Bench scripts checked in to `integration_tests/`. |

## 7. Acceptance gates

- C1 cold cluster wall ≤ `1.3.2` baseline ± 5%
- C2 warm wall ≤ 1.5× C1
- C3 0 missing on `verify_outputs.py`
- C4 0 `self-check FAILED` lines
- C5 diff vs v1.0.0 ≤ 4 000 LOC (ex docs/.github)
- C6 grep-clean for the §2 non-goals (no `redis::`, `local_dir_synthesis`, `dir_index`, `path_digest_persistence`, `plan_l_prewarm`, `dir_walk_coalescer`, `digest_lru`)
- C7 single-binary deploy

## 8. Workflow rule (red/green TDD)

For every phase ≥ B:

1. **Red commit**: tests added; if production code is needed for the test file to compile, lands as `unimplemented!()` stubs. Run `cargo test`; record which tests fail. Commit message lists the failing test names.
2. **Green commit**: implementation lands; tests pass un-ignored. Run `cargo test --workspace`; must be all green. Diff-budget checkpoint.

If the diff-budget checkpoint shows we are over §1's per-phase target by >25%, stop and re-scope before continuing.

---

**Status**: Phase A complete (~330 LOC, 143 tests green). Resuming at Phase B.1.
