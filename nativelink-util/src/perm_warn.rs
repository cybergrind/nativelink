// Copyright 2026 The NativeLink Authors. All rights reserved.
//
// Licensed under the Functional Source License, Version 1.1, Apache 2.0 Future License (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    See LICENSE file for details
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Permission-denied non-fatal helpers for chmod/utimes during input
//! materialization.
//!
//! macOS SDK files staged under `xcode_links/MacOSX*.sdk` carry
//! uchg / SIP-derived flags that block `chmod` and `utimes` for the
//! file's owner. The file content is already in place via hardlink or
//! clonefile, which is what the action actually consumes — the metadata
//! mismatch is harmless. Other error classes still propagate so a
//! genuinely broken filesystem still surfaces as a hard error.

use nativelink_error::{Code, Error};

/// Classification of a chmod/utimes call's result against the
/// "non-fatal on PermissionDenied" policy.
#[derive(Debug)]
pub enum PermPolicy {
    /// The metadata was applied successfully.
    Applied,
    /// PermissionDenied; logged as a warning by the caller, action
    /// continues. The original error is carried for the warn payload.
    WarnNonFatal(Error),
    /// Any other error class; caller must propagate.
    Fatal(Error),
}

/// Decide what to do with the result of a chmod/utimes call.
///
/// Pure logic, separated from I/O so it is testable without running
/// the underlying syscall. Callers wrap it with the actual `tracing`
/// emit + `?` propagation.
pub fn classify_perm_result(result: Result<(), Error>) -> PermPolicy {
    match result {
        Ok(()) => PermPolicy::Applied,
        Err(e) if e.code == Code::PermissionDenied => PermPolicy::WarnNonFatal(e),
        Err(e) => PermPolicy::Fatal(e),
    }
}
