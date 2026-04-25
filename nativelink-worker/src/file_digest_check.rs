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

//! Plan I support: streaming-hash an on-disk file and compare against an
//! expected `DigestInfo`. The helper is the safety check that gates
//! hardlink-from-pre-staged-tree against silent same-size/different-content
//! substitution — the bug that originally got Plan I disabled.

use std::path::Path;

use nativelink_util::common::DigestInfo;
use nativelink_util::digest_hasher::{DigestHasher, DigestHasherFunc};
use nativelink_util::fs;

/// Verify that the file at `path` hashes to `expected` under `hasher_func`.
///
/// Returns `true` iff the file exists, its size matches `expected.size_bytes`,
/// and its content streamed through `hasher_func` produces a digest equal to
/// `expected`. Any other outcome — file missing, size mismatch, content
/// mismatch, I/O error — returns `false`. Errors are deliberately swallowed:
/// a verification failure for any reason should fall through to the existing
/// CAS path at the call site, never abort the action.
///
/// The size check is the cheap rejection: if the on-disk file is the wrong
/// length we never read the contents. The content hash is the safety check
/// that rules out the same-size/different-content collisions that bit the
/// original size-only Plan I.
pub async fn file_matches_digest(
    path: &Path,
    expected: &DigestInfo,
    hasher_func: DigestHasherFunc,
) -> bool {
    let metadata = match tokio::fs::metadata(path).await {
        Ok(m) => m,
        Err(_) => return false,
    };
    if !metadata.is_file() {
        return false;
    }
    if metadata.len() != expected.size_bytes() {
        return false;
    }
    let file = match fs::open_file(path, 0, u64::MAX).await {
        Ok(f) => f,
        Err(_) => return false,
    };
    let mut hasher = hasher_func.hasher();
    let computed = match hasher.compute_from_reader(file).await {
        Ok(d) => d,
        Err(_) => return false,
    };
    computed == *expected
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use nativelink_util::common::DigestInfo;
    use nativelink_util::digest_hasher::{DigestHasher, DigestHasherFunc};
    use nativelink_macro::nativelink_test;

    use super::file_matches_digest;

    fn digest_of(bytes: &[u8], hasher_func: DigestHasherFunc) -> DigestInfo {
        let mut h = hasher_func.hasher();
        DigestHasher::update(&mut h, bytes);
        DigestHasher::finalize_digest(&mut h)
    }

    fn temp_path(name: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!(
            "nl-fdc-{}-{}",
            std::process::id(),
            rand::random::<u64>(),
        ));
        std::fs::create_dir_all(&p).unwrap();
        p.push(name);
        p
    }

    #[nativelink_test]
    async fn matches_when_content_hashes_to_expected_digest() {
        const CONTENT: &[u8] = b"the quick brown fox jumps over the lazy dog";
        let expected = digest_of(CONTENT, DigestHasherFunc::Sha256);
        let path = temp_path("hit.txt");
        tokio::fs::write(&path, CONTENT).await.unwrap();

        let result = file_matches_digest(&path, &expected, DigestHasherFunc::Sha256).await;
        assert!(result, "matching content must verify");
    }

    #[nativelink_test]
    async fn rejects_when_size_matches_but_content_differs() {
        const STALE: &[u8] = b"AAAAAAAAAAAA"; // 12 bytes
        const FRESH: &[u8] = b"BBBBBBBBBBBB"; // 12 bytes — same size, different content
        let expected_for_fresh = digest_of(FRESH, DigestHasherFunc::Sha256);
        let path = temp_path("size_collision.txt");
        tokio::fs::write(&path, STALE).await.unwrap();

        let result =
            file_matches_digest(&path, &expected_for_fresh, DigestHasherFunc::Sha256).await;
        assert!(
            !result,
            "same-size different-content MUST NOT verify — this is the original Plan I bug"
        );
    }

    #[nativelink_test]
    async fn rejects_when_size_differs_without_full_hash() {
        const SHORTER: &[u8] = b"abc";
        const LONGER: &[u8] = b"abcdef";
        let expected_for_longer = digest_of(LONGER, DigestHasherFunc::Sha256);
        let path = temp_path("short.txt");
        tokio::fs::write(&path, SHORTER).await.unwrap();

        let result =
            file_matches_digest(&path, &expected_for_longer, DigestHasherFunc::Sha256).await;
        assert!(!result, "size mismatch must reject");
    }

    #[nativelink_test]
    async fn rejects_when_file_is_missing() {
        let expected = digest_of(b"anything", DigestHasherFunc::Sha256);
        let path = temp_path("does-not-exist.txt");
        // intentionally not created

        let result = file_matches_digest(&path, &expected, DigestHasherFunc::Sha256).await;
        assert!(!result, "missing file must reject (and not error out)");
    }

    #[nativelink_test]
    async fn matches_zero_byte_file_against_zero_digest() {
        const EMPTY: &[u8] = &[];
        let expected = digest_of(EMPTY, DigestHasherFunc::Sha256);
        let path = temp_path("empty.txt");
        tokio::fs::write(&path, EMPTY).await.unwrap();

        let result = file_matches_digest(&path, &expected, DigestHasherFunc::Sha256).await;
        assert!(result, "empty file must verify against zero-content digest");
    }

    #[nativelink_test]
    async fn matches_blake3_hasher() {
        const CONTENT: &[u8] = b"blake3 path must work too";
        let expected = digest_of(CONTENT, DigestHasherFunc::Blake3);
        let path = temp_path("blake3.txt");
        tokio::fs::write(&path, CONTENT).await.unwrap();

        let result = file_matches_digest(&path, &expected, DigestHasherFunc::Blake3).await;
        assert!(result, "blake3 hasher must verify too");
    }

    #[nativelink_test]
    async fn matches_large_file_streamed_in_chunks() {
        // 256 KiB — bigger than DEFAULT_READ_BUFF_SIZE so the hasher
        // streams in multiple chunks rather than buffering whole.
        let content: Vec<u8> = (0..(256 * 1024)).map(|i| (i % 251) as u8).collect();
        let expected = digest_of(&content, DigestHasherFunc::Sha256);
        let path = temp_path("large.bin");
        tokio::fs::write(&path, &content).await.unwrap();

        let result = file_matches_digest(&path, &expected, DigestHasherFunc::Sha256).await;
        assert!(result, "large file must verify when chunked through hasher");
    }
}
