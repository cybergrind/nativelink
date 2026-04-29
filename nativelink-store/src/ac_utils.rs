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

// @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
// TODO(palfrey): IMPORTANT TODO: IMPORTING THIS SOMETIMES BREAKS
//                    THREADSAFETY. FIGURE OUT WHY AND MOVE IT TO UTILS.
// @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@

use core::pin::Pin;

use bytes::BytesMut;
use futures::stream::{Stream, StreamExt};
use futures::TryFutureExt;
use nativelink_error::{make_err, Code, Error, ResultExt};
use nativelink_proto::build::bazel::remote::execution::v2::{
    Directory as ProtoDirectory, GetTreeRequest,
};
use nativelink_util::common::DigestInfo;
use nativelink_util::digest_hasher::{DigestHasher, DigestHasherFunc};
use nativelink_util::store_trait::{StoreKey, StoreLike};
use prost::Message;

use crate::fast_slow_store::FastSlowStore;
use crate::grpc_store::GrpcStore;

// NOTE(aaronmondal) From some local testing it looks like action cache items are rarely greater than
// 1.2k. Giving a bit more just in case to reduce allocs.
pub const ESTIMATED_DIGEST_SIZE: usize = 2048;

/// This is more of a safety check. We are going to collect this entire message
/// into memory. If we don't bound the max size of the object we enable users
/// to use up all the memory on this machine.
const MAX_ACTION_MSG_SIZE: usize = 10 << 20; // 10mb.

/// Attempts to fetch the digest contents from a store into the associated proto.
pub async fn get_and_decode_digest<T: Message + Default + 'static>(
    store: &impl StoreLike,
    key: StoreKey<'_>,
) -> Result<T, Error> {
    get_size_and_decode_digest(store, key)
        .map_ok(|(v, _)| v)
        .await
}

/// Attempts to fetch the digest contents from a store into the associated proto.
pub async fn get_size_and_decode_digest<T: Message + Default + 'static>(
    store: &impl StoreLike,
    key: impl Into<StoreKey<'_>>,
) -> Result<(T, u64), Error> {
    let key = key.into();
    // Note: For unknown reasons we appear to be hitting:
    // https://github.com/rust-lang/rust/issues/92096
    // or a smiliar issue if we try to use the non-store driver function, so we
    // are using the store driver function here.
    let mut store_data_resp = store
        .as_store_driver_pin()
        .get_part_unchunked(key.borrow(), 0, Some(MAX_ACTION_MSG_SIZE as u64))
        .await;
    if let Err(err) = &mut store_data_resp {
        if err.code == Code::NotFound {
            // Trim the error code. Not Found is quite common and we don't want to send a large
            // error (debug) message for something that is common. We resize to just the last
            // message as it will be the most relevant.
            err.messages.resize_with(1, String::new);
        }
    }
    let store_data = store_data_resp?;
    let store_data_len =
        u64::try_from(store_data.len()).err_tip(|| "Could not convert store_data.len() to u64")?;

    T::decode(store_data)
        .err_tip_with_code(|e| {
            (
                Code::NotFound,
                format!("Stored value appears to be corrupt: {e} - {key:?}"),
            )
        })
        .map(|v| (v, store_data_len))
}

/// Computes the digest of a message.
pub fn message_to_digest(
    message: &impl Message,
    mut buf: &mut BytesMut,
    hasher: &mut impl DigestHasher,
) -> Result<DigestInfo, Error> {
    message
        .encode(&mut buf)
        .err_tip(|| "Could not encode directory proto")?;
    hasher.update(buf);
    Ok(hasher.finalize_digest())
}

/// Takes a proto message and will serialize it and upload it to the provided store.
pub async fn serialize_and_upload_message<'a, T: Message>(
    message: &'a T,
    cas_store: Pin<&'a impl StoreLike>,
    hasher: &mut impl DigestHasher,
) -> Result<DigestInfo, Error> {
    let mut buffer = BytesMut::with_capacity(message.encoded_len());
    let digest = message_to_digest(message, &mut buffer, hasher)
        .err_tip(|| "In serialize_and_upload_message")?;
    // Note: For unknown reasons we appear to be hitting:
    // https://github.com/rust-lang/rust/issues/92096
    // or a smiliar issue if we try to use the non-store driver function, so we
    // are using the store driver function here.
    cas_store
        .as_store_driver_pin()
        .update_oneshot(digest.into(), buffer.freeze())
        .await
        .err_tip(|| "In serialize_and_upload_message")?;
    Ok(digest)
}

/// Computes a digest of a given buffer.
pub fn compute_buf_digest(buf: &[u8], hasher: &mut impl DigestHasher) -> DigestInfo {
    hasher.update(buf);
    hasher.finalize_digest()
}

/// Encodes each `Directory` proto from `directories`, computes its digest under
/// `hasher_func`, and writes it to `fast_store`. Returns the count of protos
/// written.
///
/// This is the kernel of the GetTree-prewarm fast path: source the full
/// transitive `Directory` subtree of an action's input root via REAPI
/// `GetTree` (one streamed RPC, depth-independent), then call this to populate
/// the worker's local FS-store. After prewarm, the recursive
/// `download_to_directory` walk hits the fast store at every level — the
/// depth-D sequential `Read` RPCs against the slow CAS collapse into ~1 RTT.
///
/// The caller is responsible for sourcing the `Directory` protos (e.g., via
/// `GrpcStore::get_tree` paginated streaming). Errors from the input stream
/// propagate immediately; partial writes already made are NOT rolled back —
/// the fast store is content-addressed so duplicate writes from a retry are
/// idempotent.
pub async fn cache_directory_protos<S>(
    fast_store: &impl StoreLike,
    directories: S,
    hasher_func: DigestHasherFunc,
) -> Result<usize, Error>
where
    S: Stream<Item = Result<ProtoDirectory, Error>>,
{
    futures::pin_mut!(directories);
    let mut count = 0usize;
    while let Some(dir) = directories.next().await {
        let dir = dir?;
        let mut buf = BytesMut::with_capacity(dir.encoded_len());
        let mut hasher = hasher_func.hasher();
        let digest = message_to_digest(&dir, &mut buf, &mut hasher)
            .err_tip(|| "In cache_directory_protos")?;
        fast_store
            .as_store_driver_pin()
            .update_oneshot(digest.into(), buf.freeze())
            .await
            .err_tip(|| format!("cache_directory_protos: writing digest {digest}"))?;
        count += 1;
    }
    Ok(count)
}

/// Outcome of a `prewarm_input_tree` call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrewarmOutcome {
    /// The slow store is a direct `GrpcStore`; we drove `GetTree` to the end
    /// and populated the fast store with `count` Directory protos.
    Prewarmed { count: usize },
    /// The slow store isn't a direct `GrpcStore` (e.g., wrapped by a
    /// compression / verify store), so prewarm is unavailable. Caller falls
    /// back to the recursive depth-D `Read` walk.
    SlowStoreNotGrpc,
}
impl PrewarmOutcome {
    #[must_use]
    pub fn count(self) -> usize {
        match self {
            Self::Prewarmed { count } => count,
            Self::SlowStoreNotGrpc => 0,
        }
    }
}

/// Drives REAPI `GetTree` against `cas_store`'s slow side to populate the
/// fast (local) store with every `Directory` proto transitively reachable
/// from `root`. Returns `Prewarmed { count }` on success, or
/// `SlowStoreNotGrpc` if the slow store isn't a direct `GrpcStore` and the
/// caller must fall back.
///
/// After this returns successfully, the recursive `download_to_directory`
/// walk hits the fast store at every level — the depth-D sequential `Read`
/// RPCs against the slow CAS collapse into ~1 RTT of `GetTree`.
///
/// This loops over `next_page_token` so large trees split by the server
/// pagination still complete in one logical call.
pub async fn prewarm_input_tree(
    cas_store: &FastSlowStore,
    root: DigestInfo,
    hasher_func: DigestHasherFunc,
    instance_name: &str,
) -> Result<PrewarmOutcome, Error> {
    let Some(grpc) = cas_store.slow_store().downcast_ref::<GrpcStore>(None) else {
        return Ok(PrewarmOutcome::SlowStoreNotGrpc);
    };
    let mut all_dirs: Vec<ProtoDirectory> = Vec::new();
    let mut page_token = String::new();
    loop {
        let req = GetTreeRequest {
            instance_name: instance_name.to_string(),
            root_digest: Some(root.into()),
            page_size: 0,
            page_token: page_token.clone(),
            digest_function: hasher_func.proto_digest_func() as i32,
        };
        let response = grpc
            .get_tree(tonic::Request::new(req))
            .await
            .err_tip(|| "prewarm_input_tree: GrpcStore::get_tree")?;
        let mut stream = response.into_inner();
        let mut next_token = String::new();
        loop {
            match stream.message().await {
                Ok(Some(resp)) => {
                    all_dirs.extend(resp.directories);
                    next_token = resp.next_page_token;
                }
                Ok(None) => break,
                Err(status) => {
                    return Err(make_err!(
                        Code::Internal,
                        "prewarm_input_tree: GetTree stream error: {status:?}"
                    ));
                }
            }
        }
        if next_token.is_empty() {
            break;
        }
        page_token = next_token;
    }
    let dir_stream = futures::stream::iter(all_dirs.into_iter().map(Ok::<_, Error>));
    let count = cache_directory_protos(cas_store.fast_store(), dir_stream, hasher_func).await?;
    Ok(PrewarmOutcome::Prewarmed { count })
}
