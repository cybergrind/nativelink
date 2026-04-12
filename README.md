# NativeLink macOS Shared-Tree Worker

A specialized fork of [NativeLink](https://github.com/TraceMachina/nativelink)
optimized for macOS distributed builds using a shared directory execution model.

## What is this?

This is NativeLink v1.0.0 with targeted patches that eliminate per-action input
tree materialization overhead on macOS APFS workers. Instead of copying tens of
thousands of files into a fresh sandbox for every build action, this fork runs
actions directly in a pre-staged shared source tree — the same execution model
that local `ninja -j20` uses.

The result: **450+ compile steps/min** on a 2-Mac-Mini cluster (up from 5-6/min
with unpatched NativeLink), approaching local-solo throughput with the benefits
of distributed execution.

## Design

The patches implement a layered caching strategy on the worker:

```
Action dispatched by scheduler
        |
        v
[Plan L] walked_dirs Merkle cache — O(1) HashSet lookup
        |  hit: skip entire subtree walk (zero syscalls)
        |  miss: v
[Plan K] path->digest cache — per-file O(1) HashMap lookup
        |  hit: skip stat/chmod/utime (zero syscalls)
        |  miss: v
[Plan J] shared-tree stat check — file exists with expected size?
        |  yes: skip materialization
        |  no: v
[Plan I] hint-path hardlink — clonefile from pre-staged tree
        |  hit: O(1) APFS COW clone
        |  miss: v
[CAS fetch] traditional download from content-addressable store
```

### Key architectural decisions

- **Shared-tree execution (Plan J)**: `work_directory` is set to
  `InputRootAbsolutePath` from the action's platform properties. All actions
  share one filesystem view. Hermetic isolation is traded for throughput.

- **APFS clonefile (iter 6)**: `fs::hard_link` calls `clonefile(2)` first on
  macOS, eliminating shared-inode lock contention that serialized parallel
  materialization.

- **Per-worker caches (Plans K + L)**: In-memory caches that exploit siso's
  action-graph invariant (no parallel mutations on inputs). The walked-dirs
  Merkle cache collapses entire subtree walks to a single hash lookup after the
  first action.

## Target use case

- Chromium (or similar large C++ project) builds on macOS via
  [siso](https://chromium.googlesource.com/infra/infra/+/refs/heads/main/go/src/infra/build/siso/)
  remote execution
- Workers are trusted Mac Minis on a local network
- All workers share a byte-identical pre-staged source tree (via rsync)
- The scheduler can run on Linux; workers must be macOS (Apple Silicon)

## Building

```bash
# On a macOS (aarch64) worker:
cargo build --release --bin nativelink

# The binary:
target/release/nativelink
```

Cross-compilation from Linux to macOS is not supported (Mach-O linker + SDK
required). Develop and test on Linux, build the final binary on macOS.

## Running tests (Linux or macOS)

```bash
# All unit tests (platform-agnostic logic):
cargo test -p nativelink-util -p nativelink-worker

# The clonefile-specific test runs only on macOS:
cargo test -p nativelink-util --test fs_test
```

## Configuration

Worker config must include the `InputRootAbsolutePath` platform property
pointing to the pre-staged source tree:

```json5
platform_properties: {
  OSFamily: { values: ["darwin"] },
  ISA: { values: ["aarch64"] },
  InputRootAbsolutePath: {
    values: ["/path/to/chromium/src"],
  },
},
```

The shared-tree execution mode activates automatically when
`InputRootAbsolutePath` is set and non-empty.

## Upstream

Based on [TraceMachina/nativelink v1.0.0](https://github.com/TraceMachina/nativelink/tree/v1_branch).

The patches in this fork correspond to the optimization iterations documented in
the [macOS distributed build optimization](https://github.com/nicholasgasior/chromium-distributed-compile/blob/main/macos_distributed_build_optimize.md)
project.

## License

Same as upstream: Functional Source License, Version 1.1, Apache 2.0 Future
License. See [LICENSE](LICENSE).
