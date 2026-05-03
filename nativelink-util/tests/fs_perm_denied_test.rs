// Copyright 2026 The NativeLink Authors. All rights reserved.
//
// Licensed under the Functional Source License, Version 1.1, Apache 2.0 Future License (the "License");
// you may not use this file except in compliance with the License.
//
// See LICENSE file for details.

//! TA3 — `perm_warn::classify_perm_result` must downgrade
//! `PermissionDenied` to a non-fatal warning policy and propagate
//! every other error class as fatal.
//!
//! Red state: there is no policy helper at v1.0.0. Plan I integration
//! (Phase B) needs this so that `chmod`/`utimes` against immutable Apple
//! SDK files don't abort the action. Green state: a pure-logic
//! classifier exists and is unit-testable without privilege escalation.

use nativelink_error::{Code, Error, make_err};
use nativelink_util::perm_warn::{PermPolicy, classify_perm_result};

#[test]
fn ok_classified_as_applied() {
    assert!(matches!(classify_perm_result(Ok(())), PermPolicy::Applied));
}

#[test]
fn permission_denied_classified_as_warn_non_fatal() {
    let e: Error = make_err!(Code::PermissionDenied, "uchg-flagged SDK file");
    assert!(matches!(
        classify_perm_result(Err(e)),
        PermPolicy::WarnNonFatal(_)
    ));
}

#[test]
fn other_error_classified_as_fatal() {
    let e: Error = make_err!(Code::Internal, "disk failure");
    assert!(matches!(
        classify_perm_result(Err(e)),
        PermPolicy::Fatal(_)
    ));
}

#[test]
fn invalid_argument_is_fatal_not_silenced() {
    // A class of error that must NOT be silenced — guards against
    // someone accidentally widening the non-fatal predicate.
    let e: Error = make_err!(Code::InvalidArgument, "bad mode");
    assert!(matches!(
        classify_perm_result(Err(e)),
        PermPolicy::Fatal(_)
    ));
}
