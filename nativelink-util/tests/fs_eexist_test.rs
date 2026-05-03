// Copyright 2026 The NativeLink Authors. All rights reserved.
//
// Licensed under the Functional Source License, Version 1.1, Apache 2.0 Future License (the "License");
// you may not use this file except in compliance with the License.
//
// See LICENSE file for details.

//! TA2 — `hardlink_directory_tree` must be idempotent on a pre-existing
//! destination directory.
//!
//! Red state: at v1.0.0, `hardlink_directory_tree` errors with
//! "Destination directory already exists" when `dst_dir` exists. The
//! input-materialization caller (Plan I / directory_cache) creates the
//! work directory before calling this function, so the v1.0.0 error
//! makes any cache-hit path fatal. Green state: pre-existing `dst_dir`
//! is accepted; per-entry conflicts still surface as errors via the
//! recursive walker.

use nativelink_util::fs_util::hardlink_directory_tree;
use std::io::Write;
use tempfile::TempDir;

#[tokio::test]
async fn hardlink_directory_tree_accepts_existing_dst() -> Result<(), Box<dyn std::error::Error>> {
    let scratch = TempDir::new()?;
    let src = scratch.path().join("src");
    let dst = scratch.path().join("dst");

    std::fs::create_dir(&src)?;
    {
        let mut f = std::fs::File::create(src.join("a.txt"))?;
        f.write_all(b"contents of a")?;
    }
    std::fs::create_dir(&dst)?; // pre-existing — must NOT cause failure

    hardlink_directory_tree(&src, &dst).await?;

    let dst_a = std::fs::read(dst.join("a.txt"))?;
    assert_eq!(dst_a, b"contents of a");
    Ok(())
}

#[tokio::test]
async fn hardlink_directory_tree_creates_dst_if_missing() -> Result<(), Box<dyn std::error::Error>>
{
    let scratch = TempDir::new()?;
    let src = scratch.path().join("src");
    let dst = scratch.path().join("dst");

    std::fs::create_dir(&src)?;
    {
        let mut f = std::fs::File::create(src.join("b.txt"))?;
        f.write_all(b"contents of b")?;
    }

    hardlink_directory_tree(&src, &dst).await?;

    let dst_b = std::fs::read(dst.join("b.txt"))?;
    assert_eq!(dst_b, b"contents of b");
    Ok(())
}
