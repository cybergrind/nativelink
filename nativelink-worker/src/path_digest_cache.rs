// Copyright 2024 The NativeLink Authors. All rights reserved.
//
// Licensed under the Functional Source License, Version 1.1, Apache 2.0 Future License.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use nativelink_util::common::DigestInfo;
use parking_lot::Mutex;

/// Per-worker cache that maps absolute filesystem paths to the content digest
/// last known to be at that path.
///
/// Used by `download_to_directory` in shared-tree (Plan J) mode to skip
/// `stat` + `set_permissions` + `set_file_mtime` syscalls on files whose
/// `(path, digest)` pair has already been verified by a prior action.
///
/// The cache never evicts. Memory footprint: ~150 bytes per entry x file count.
/// Reset on worker restart.
#[derive(Debug, Default)]
pub struct PathDigestCache {
    map: Mutex<HashMap<PathBuf, DigestInfo>>,
    /// Merkle cache of Directory digests whose entire subtree has been walked.
    /// A hit means every file under this Directory is already on disk and
    /// recorded in `map`. Used by Plan L to skip the entire tree walk.
    walked_dirs: Mutex<HashSet<DigestInfo>>,
}

impl PathDigestCache {
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns true if the cache has an entry for `path` whose recorded
    /// digest equals `digest`.
    pub fn contains(&self, path: &Path, digest: &DigestInfo) -> bool {
        let map = self.map.lock();
        map.get(path).is_some_and(|d| d == digest)
    }

    /// Records that `path` now holds the content for `digest`.
    pub fn insert(&self, path: PathBuf, digest: DigestInfo) {
        self.map.lock().insert(path, digest);
    }

    /// Returns true if the subtree rooted at the given Directory digest has
    /// been fully walked by a prior action on this worker.
    pub fn dir_walked(&self, digest: &DigestInfo) -> bool {
        self.walked_dirs.lock().contains(digest)
    }

    /// Records that the subtree rooted at the given Directory digest has been
    /// fully materialized. Only called after all child futures succeed.
    pub fn mark_dir_walked(&self, digest: DigestInfo) {
        self.walked_dirs.lock().insert(digest);
    }

    /// Number of cached file entries.
    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.map.lock().len()
    }

    /// Number of Directory subtrees marked walked.
    #[allow(dead_code)]
    pub fn walked_dirs_len(&self) -> usize {
        self.walked_dirs.lock().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_cache_is_empty() {
        let cache = PathDigestCache::new();
        assert_eq!(cache.len(), 0);
        assert_eq!(cache.walked_dirs_len(), 0);
    }

    #[test]
    fn test_contains_miss_on_empty() {
        let cache = PathDigestCache::new();
        let digest = DigestInfo::new([1u8; 32], 100);
        assert!(!cache.contains(Path::new("/foo/bar.cc"), &digest));
    }

    #[test]
    fn test_insert_then_contains_hit() {
        let cache = PathDigestCache::new();
        let digest = DigestInfo::new([2u8; 32], 200);
        let path = PathBuf::from("/src/third_party/clang");

        cache.insert(path.clone(), digest);
        assert!(cache.contains(&path, &digest));
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn test_contains_wrong_digest_is_miss() {
        let cache = PathDigestCache::new();
        let digest_a = DigestInfo::new([3u8; 32], 300);
        let digest_b = DigestInfo::new([4u8; 32], 400);
        let path = PathBuf::from("/src/foo.o");

        cache.insert(path.clone(), digest_a);
        assert!(!cache.contains(&path, &digest_b));
    }

    #[test]
    fn test_insert_overwrites_previous() {
        let cache = PathDigestCache::new();
        let digest_old = DigestInfo::new([5u8; 32], 500);
        let digest_new = DigestInfo::new([6u8; 32], 600);
        let path = PathBuf::from("/src/gen/foo.h");

        cache.insert(path.clone(), digest_old);
        cache.insert(path.clone(), digest_new);
        assert!(cache.contains(&path, &digest_new));
        assert!(!cache.contains(&path, &digest_old));
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn test_dir_walked_miss_on_empty() {
        let cache = PathDigestCache::new();
        let digest = DigestInfo::new([7u8; 32], 700);
        assert!(!cache.dir_walked(&digest));
    }

    #[test]
    fn test_mark_dir_walked_then_hit() {
        let cache = PathDigestCache::new();
        let digest = DigestInfo::new([8u8; 32], 800);

        cache.mark_dir_walked(digest);
        assert!(cache.dir_walked(&digest));
        assert_eq!(cache.walked_dirs_len(), 1);
    }

    #[test]
    fn test_dir_walked_different_digest_is_miss() {
        let cache = PathDigestCache::new();
        let digest_a = DigestInfo::new([9u8; 32], 900);
        let digest_b = DigestInfo::new([10u8; 32], 1000);

        cache.mark_dir_walked(digest_a);
        assert!(!cache.dir_walked(&digest_b));
    }
}
