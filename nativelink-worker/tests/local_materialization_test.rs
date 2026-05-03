// Copyright 2026 The NativeLink Authors. All rights reserved.
//
// Licensed under the Functional Source License, Version 1.1, Apache 2.0 Future License (the "License");
// you may not use this file except in compliance with the License.
//
// See LICENSE file for details.

//! Phase C — `materialize_one_output_locally` tests.
//!
//! TC1: caller-skip when `local_materialization_root` is None — covered
//!      structurally (the helper isn't called). Not a unit test.
//! TC2: dst missing → file appears at dst with the same bytes as src.
//! TC3: dst already exists with matching size → no rewrite (idempotent;
//!      verifies the path-share-lucky case from `response.md`).
//! TC4: dst exists but wrong size → replaced with the sandbox's bytes.
//! TC5: nested `obj/.../foo.o` → parent dirs created.
//! TC6: failure path — src missing causes Err but the helper returns it
//!      so the caller can downgrade to a warning.

use nativelink_worker::running_actions_manager::materialize_one_output_locally;
use std::time::Duration;
use tempfile::TempDir;

#[tokio::test]
async fn tc2_dst_missing_creates_file_with_src_bytes() -> Result<(), Box<dyn std::error::Error>> {
    let scratch = TempDir::new()?;
    let work_dir = scratch.path().join("work");
    let local_root = scratch.path().join("local_root");
    std::fs::create_dir_all(&work_dir)?;
    std::fs::create_dir_all(&local_root)?;

    let bytes = b"hello-from-clang";
    std::fs::write(work_dir.join("foo.o"), bytes)?;

    materialize_one_output_locally(
        work_dir.to_str().unwrap(),
        local_root.to_str().unwrap(),
        "foo.o",
        bytes.len() as u64,
    )
    .await?;

    let dst = local_root.join("foo.o");
    assert_eq!(std::fs::read(&dst)?, bytes);
    Ok(())
}

#[tokio::test]
async fn tc3_dst_present_with_matching_size_left_alone()
-> Result<(), Box<dyn std::error::Error>> {
    let scratch = TempDir::new()?;
    let work_dir = scratch.path().join("work");
    let local_root = scratch.path().join("local_root");
    std::fs::create_dir_all(&work_dir)?;
    std::fs::create_dir_all(&local_root)?;

    let bytes = b"already-here-from-clang-write";
    std::fs::write(work_dir.join("foo.o"), bytes)?;
    std::fs::write(local_root.join("foo.o"), bytes)?;
    let mtime_before = std::fs::metadata(local_root.join("foo.o"))?.modified()?;

    // Allow the next syscall's mtime to potentially differ if we did
    // touch the file. We're checking that we don't.
    tokio::time::sleep(Duration::from_millis(20)).await;

    materialize_one_output_locally(
        work_dir.to_str().unwrap(),
        local_root.to_str().unwrap(),
        "foo.o",
        bytes.len() as u64,
    )
    .await?;

    let mtime_after = std::fs::metadata(local_root.join("foo.o"))?.modified()?;
    assert_eq!(
        mtime_before, mtime_after,
        "matching-size dst must not be touched (idempotent skip)"
    );
    Ok(())
}

#[tokio::test]
async fn tc4_dst_present_with_wrong_size_is_replaced()
-> Result<(), Box<dyn std::error::Error>> {
    let scratch = TempDir::new()?;
    let work_dir = scratch.path().join("work");
    let local_root = scratch.path().join("local_root");
    std::fs::create_dir_all(&work_dir)?;
    std::fs::create_dir_all(&local_root)?;

    let new_bytes = b"replacement-from-sandbox";
    std::fs::write(work_dir.join("foo.o"), new_bytes)?;
    std::fs::write(local_root.join("foo.o"), b"stale-shorter-content")?;

    materialize_one_output_locally(
        work_dir.to_str().unwrap(),
        local_root.to_str().unwrap(),
        "foo.o",
        new_bytes.len() as u64,
    )
    .await?;

    assert_eq!(std::fs::read(local_root.join("foo.o"))?, new_bytes);
    Ok(())
}

#[tokio::test]
async fn tc5_nested_path_creates_parent_dirs() -> Result<(), Box<dyn std::error::Error>> {
    let scratch = TempDir::new()?;
    let work_dir = scratch.path().join("work");
    let local_root = scratch.path().join("local_root");
    std::fs::create_dir_all(work_dir.join("obj/third_party/libvpx"))?;
    std::fs::create_dir_all(&local_root)?;

    let bytes = b"nested-output";
    std::fs::write(
        work_dir.join("obj/third_party/libvpx/scale_neon.o"),
        bytes,
    )?;

    materialize_one_output_locally(
        work_dir.to_str().unwrap(),
        local_root.to_str().unwrap(),
        "obj/third_party/libvpx/scale_neon.o",
        bytes.len() as u64,
    )
    .await?;

    assert_eq!(
        std::fs::read(local_root.join("obj/third_party/libvpx/scale_neon.o"))?,
        bytes
    );
    Ok(())
}

#[tokio::test]
async fn tc6_src_missing_returns_err() -> Result<(), Box<dyn std::error::Error>> {
    let scratch = TempDir::new()?;
    let work_dir = scratch.path().join("work");
    let local_root = scratch.path().join("local_root");
    std::fs::create_dir_all(&work_dir)?;
    std::fs::create_dir_all(&local_root)?;

    // No src file at work_dir/foo.o.
    let result = materialize_one_output_locally(
        work_dir.to_str().unwrap(),
        local_root.to_str().unwrap(),
        "foo.o",
        10,
    )
    .await;
    assert!(
        result.is_err(),
        "missing src must return Err so the caller can warn-and-continue"
    );
    Ok(())
}
