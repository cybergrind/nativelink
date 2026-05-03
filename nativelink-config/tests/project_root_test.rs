// Copyright 2026 The NativeLink Authors. All rights reserved.
//
// Licensed under the Functional Source License, Version 1.1, Apache 2.0 Future License (the "License");
// you may not use this file except in compliance with the License.
//
// See LICENSE file for details.

//! TB1–TB4 — `ProjectRoot` per-worker path remap.
//!
//! Red state: `ProjectRoot` struct and `translate_input_root_path`
//! function do not exist at v1.0.0. Green state: the struct deserializes
//! `{ in_action, on_disk }`, and the translator substitutes the prefix
//! at a path-boundary, leaving non-matching paths unchanged.

use nativelink_config::cas_server::{ProjectRoot, translate_input_root_path};

#[test]
fn translate_input_root_identity_when_no_project_root() {
    // TB1: with project_root = None, the path is returned unchanged.
    let in_action = "/Users/octo/src/chromium";
    let out = translate_input_root_path(in_action, None);
    assert_eq!(out, in_action);
}

#[test]
fn translate_input_root_remap_at_path_boundary() {
    // TB2: when prefix matches at a path boundary, on_disk replaces in_action.
    let pr = ProjectRoot {
        in_action: "/Users/octo".into(),
        on_disk: "/Users/kpi".into(),
    };
    let out = translate_input_root_path("/Users/octo/src/chromium", Some(&pr));
    assert_eq!(out, "/Users/kpi/src/chromium");
}

#[test]
fn translate_input_root_remap_exact_match() {
    // TB2b: exact-prefix-match (path equals in_action) returns on_disk.
    let pr = ProjectRoot {
        in_action: "/Users/octo".into(),
        on_disk: "/Users/kpi".into(),
    };
    let out = translate_input_root_path("/Users/octo", Some(&pr));
    assert_eq!(out, "/Users/kpi");
}

#[test]
fn translate_input_root_no_match_returned_unchanged() {
    // TB3: a path that does not share the in_action prefix is not touched.
    let pr = ProjectRoot {
        in_action: "/Users/octo".into(),
        on_disk: "/Users/kpi".into(),
    };
    let out = translate_input_root_path("/var/lib/something", Some(&pr));
    assert_eq!(out, "/var/lib/something");
}

#[test]
fn translate_input_root_no_match_when_prefix_is_substring_only() {
    // TB3b: substring (not boundary) match must NOT trigger remap.
    // "/Users/octocat" should not match "/Users/octo".
    let pr = ProjectRoot {
        in_action: "/Users/octo".into(),
        on_disk: "/Users/kpi".into(),
    };
    let out = translate_input_root_path("/Users/octocat/src", Some(&pr));
    assert_eq!(out, "/Users/octocat/src");
}

#[test]
fn project_root_deserializes_from_json() {
    // TB4: serde round-trip. ProjectRoot is parseable from JSON5.
    let raw = r#"{ "in_action": "/Users/octo", "on_disk": "/Users/kpi" }"#;
    let pr: ProjectRoot = serde_json::from_str(raw).expect("must deserialize");
    assert_eq!(pr.in_action, "/Users/octo");
    assert_eq!(pr.on_disk, "/Users/kpi");
}
