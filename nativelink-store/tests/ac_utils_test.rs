// Copyright 2024 The NativeLink Authors. All rights reserved.
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

use std::env;
use std::ffi::OsString;

use nativelink_config::stores::{FastSlowSpec, MemorySpec, StoreSpec};
use nativelink_error::{Error, ResultExt};
use nativelink_macro::nativelink_test;
use nativelink_proto::build::bazel::remote::execution::v2::{
    Directory as ProtoDirectory, DirectoryNode,
};
use nativelink_store::ac_utils::{
    cache_directory_protos, compute_buf_digest, get_and_decode_digest, prewarm_input_tree,
    PrewarmOutcome,
};
use nativelink_store::fast_slow_store::FastSlowStore;
use nativelink_store::memory_store::MemoryStore;
use nativelink_util::common::{DigestInfo, fs};
use nativelink_util::digest_hasher::DigestHasherFunc;
use nativelink_util::store_trait::{Store, StoreLike, UploadSizeInfo};
use pretty_assertions::assert_eq;
use prost::Message;
use rand::Rng;
use tokio::io::AsyncWriteExt;

/// Get temporary path from either `TEST_TMPDIR` or best effort temp directory if
/// not set.
async fn make_temp_path(data: &str) -> OsString {
    let dir = format!(
        "{}/{}",
        env::var("TEST_TMPDIR").unwrap_or_else(|_| env::temp_dir().to_str().unwrap().to_string()),
        rand::rng().random::<u64>(),
    );
    fs::create_dir_all(&dir).await.unwrap();
    OsString::from(format!("{dir}/{data}"))
}

const HASH1: &str = "0123456789abcdef000000000000000000000000000000000123456789abcdef";
const HASH1_SIZE: i64 = 147;

// Regression test for bug created when implementing FileSlot
// where the timeout() success condition was breaking out of the outer
// loop resulting in the file always being created with <= 4096 bytes.
#[nativelink_test]
async fn upload_file_to_store_with_large_file() -> Result<(), Error> {
    let filepath = make_temp_path("test.txt").await;
    let expected_data = vec![0x88; 1024 * 1024]; // 1MB.
    let store = MemoryStore::new(&MemorySpec::default());
    let digest = DigestInfo::try_new(HASH1, HASH1_SIZE)?; // Dummy hash data.
    {
        // Write 1MB of 0x88s to the file.
        let mut file = tokio::fs::File::create(&filepath)
            .await
            .err_tip(|| "Could not open file")?;
        file.write_all(&expected_data)
            .await
            .err_tip(|| "Could not write to file")?;
        file.flush().await.err_tip(|| "Could not flush file")?;
        file.sync_all().await.err_tip(|| "Could not sync file")?;
    }
    {
        // Upload our file.
        let file = fs::open_file(&filepath, 0, u64::MAX)
            .await
            .unwrap()
            .into_inner();
        store
            .update_with_whole_file(
                digest,
                filepath,
                file,
                UploadSizeInfo::ExactSize(expected_data.len() as u64),
            )
            .await?;
    }
    {
        // Check to make sure the file was saved correctly to the store.
        let store_data = store.get_part_unchunked(digest, 0, None).await?;
        assert_eq!(store_data.len(), expected_data.len());
        assert_eq!(store_data, expected_data);
    }
    Ok(())
}

fn digest_for(message: &impl Message, hasher_func: DigestHasherFunc) -> DigestInfo {
    let bytes = message.encode_to_vec();
    let mut hasher = hasher_func.hasher();
    compute_buf_digest(&bytes, &mut hasher)
}

#[nativelink_test]
async fn cache_directory_protos_writes_each_proto_under_its_content_digest() -> Result<(), Error> {
    // Goal: prove that streaming a list of REAPI Directory protos through
    // `cache_directory_protos` populates the fast (local) store with each
    // proto keyed by its content-addressed digest. This is the kernel of the
    // GetTree-prewarm fast path: after we've called this, the recursive
    // download_to_directory walk hits the fast store at every level and
    // skips the depth-D sequential Read RPCs against the slow CAS.
    let hasher_func = DigestHasherFunc::Sha256;

    // Build a small 3-deep tree: grandchild (empty) <- child <- root.
    let grandchild = ProtoDirectory::default();
    let grandchild_digest = digest_for(&grandchild, hasher_func);

    let child = ProtoDirectory {
        directories: vec![DirectoryNode {
            name: "g".into(),
            digest: Some(grandchild_digest.into()),
        }],
        ..Default::default()
    };
    let child_digest = digest_for(&child, hasher_func);

    let root = ProtoDirectory {
        directories: vec![DirectoryNode {
            name: "c".into(),
            digest: Some(child_digest.into()),
        }],
        ..Default::default()
    };
    let root_digest = digest_for(&root, hasher_func);

    let fast = Store::new(MemoryStore::new(&MemorySpec::default()));

    let dirs = futures::stream::iter(vec![
        Ok::<_, Error>(root.clone()),
        Ok::<_, Error>(child.clone()),
        Ok::<_, Error>(grandchild.clone()),
    ]);

    let count = cache_directory_protos(&fast, dirs, hasher_func).await?;
    assert_eq!(count, 3);

    // Every proto must be retrievable from the fast store under the digest
    // we computed independently — this is the contract `download_to_directory`
    // will rely on.
    let stored_root: ProtoDirectory = get_and_decode_digest(&fast, root_digest.into()).await?;
    assert_eq!(stored_root, root);
    let stored_child: ProtoDirectory =
        get_and_decode_digest(&fast, child_digest.into()).await?;
    assert_eq!(stored_child, child);
    let stored_gc: ProtoDirectory =
        get_and_decode_digest(&fast, grandchild_digest.into()).await?;
    assert_eq!(stored_gc, grandchild);

    Ok(())
}

#[nativelink_test]
async fn cache_directory_protos_handles_empty_stream() -> Result<(), Error> {
    let fast = Store::new(MemoryStore::new(&MemorySpec::default()));
    let count = cache_directory_protos(
        &fast,
        futures::stream::iter(Vec::<Result<ProtoDirectory, Error>>::new()),
        DigestHasherFunc::Sha256,
    )
    .await?;
    assert_eq!(count, 0);
    Ok(())
}

#[nativelink_test]
async fn prewarm_input_tree_falls_back_when_slow_store_is_not_grpc() -> Result<(), Error> {
    // Defensive contract: if the operator wraps the slow CAS in compression,
    // verify, etc., the GrpcStore downcast fails. We must NOT error — the
    // caller will fall through to the existing per-Read recursive walk.
    let fast = Store::new(MemoryStore::new(&MemorySpec::default()));
    let slow = Store::new(MemoryStore::new(&MemorySpec::default()));
    let cas = FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Memory(MemorySpec::default()),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: Default::default(),
            slow_direction: Default::default(),
        },
        fast,
        slow,
    );
    let dummy_digest = DigestInfo::try_new(HASH1, HASH1_SIZE)?;
    let outcome =
        prewarm_input_tree(&cas, dummy_digest, DigestHasherFunc::Sha256, "main").await?;
    assert_eq!(outcome, PrewarmOutcome::SlowStoreNotGrpc);
    assert_eq!(outcome.count(), 0);
    Ok(())
}

#[nativelink_test]
async fn cache_directory_protos_propagates_stream_error() -> Result<(), Error> {
    // A failing fetch (e.g., gRPC stream tear-down mid-page) must surface as
    // an error, not be silently dropped — otherwise we'd mark the prewarm as
    // successful and the subsequent recursive walk would silently fall back
    // to slow-store reads with no signal.
    let fast = Store::new(MemoryStore::new(&MemorySpec::default()));
    let err = nativelink_error::make_err!(
        nativelink_error::Code::Unavailable,
        "simulated transport failure"
    );
    let dirs = futures::stream::iter(vec![
        Ok::<_, Error>(ProtoDirectory::default()),
        Err(err),
    ]);
    let result = cache_directory_protos(&fast, dirs, DigestHasherFunc::Sha256).await;
    assert!(result.is_err(), "expected propagated error, got {result:?}");
    Ok(())
}
