// Copyright 2026 The NativeLink Authors. All rights reserved.
//
// Licensed under the Functional Source License, Version 1.1, Apache 2.0 Future License (the "License");
// you may not use this file except in compliance with the License.
//
// See LICENSE file for details.

//! TA1 — `fs::hard_link` should prefer APFS clonefile() on macOS,
//! falling back to `std::fs::hard_link` on other platforms or when
//! clonefile is unsupported by the filesystem.
//!
//! Red state: at v1.0.0, `hard_link` always calls `std::fs::hard_link`,
//! which produces a shared-inode hardlink. On macOS this serializes
//! under parallel materialization. Green state: on macOS APFS the
//! resulting destination is a *clone* (different inode, COW-shared
//! blocks); on Linux nothing observable changes.

use nativelink_util::fs::hard_link;
use std::io::Write;
use tempfile::TempDir;

#[tokio::test]
async fn hard_link_preserves_content() -> Result<(), Box<dyn std::error::Error>> {
    let dir = TempDir::new()?;
    let src = dir.path().join("src.bin");
    let dst = dir.path().join("dst.bin");

    {
        let mut f = std::fs::File::create(&src)?;
        f.write_all(b"hello clonefile")?;
    }

    hard_link(&src, &dst).await?;

    let src_content = std::fs::read(&src)?;
    let dst_content = std::fs::read(&dst)?;
    assert_eq!(
        src_content, dst_content,
        "hard_link/clonefile must preserve content byte-for-byte"
    );
    Ok(())
}

#[cfg(target_os = "macos")]
#[tokio::test]
async fn hard_link_on_macos_uses_clonefile_distinct_inode() -> Result<(), Box<dyn std::error::Error>>
{
    use std::os::unix::fs::MetadataExt;

    let dir = TempDir::new()?;
    let src = dir.path().join("src.bin");
    let dst = dir.path().join("dst.bin");

    {
        let mut f = std::fs::File::create(&src)?;
        f.write_all(b"clonefile distinct inode test")?;
    }

    hard_link(&src, &dst).await?;

    let src_meta = std::fs::metadata(&src)?;
    let dst_meta = std::fs::metadata(&dst)?;

    // APFS clonefile produces a NEW inode that COW-shares blocks with
    // src. std::fs::hard_link produces the SAME inode (st_nlink > 1).
    // We assert the clonefile shape: distinct inodes.
    assert_ne!(
        src_meta.ino(),
        dst_meta.ino(),
        "macOS hard_link must use clonefile (distinct inodes); got shared inode {}",
        src_meta.ino()
    );

    Ok(())
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn hard_link_on_linux_remains_shared_inode() -> Result<(), Box<dyn std::error::Error>> {
    use std::os::unix::fs::MetadataExt;

    let dir = TempDir::new()?;
    let src = dir.path().join("src.bin");
    let dst = dir.path().join("dst.bin");

    {
        let mut f = std::fs::File::create(&src)?;
        f.write_all(b"linux hard_link path")?;
    }

    hard_link(&src, &dst).await?;

    let src_meta = std::fs::metadata(&src)?;
    let dst_meta = std::fs::metadata(&dst)?;

    // Guard test: clonefile is macOS-only; on Linux the function must
    // continue to produce a shared inode. If this regresses, someone
    // has accidentally widened the clonefile path.
    assert_eq!(
        src_meta.ino(),
        dst_meta.ino(),
        "linux hard_link must preserve std::fs::hard_link shared-inode semantics"
    );
    Ok(())
}
