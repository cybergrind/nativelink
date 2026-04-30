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

use core::convert::Into;
use core::fmt::Debug;
use std::collections::HashMap;

use bytes::BytesMut;
use nativelink_config::cas_server::{AcStoreConfig, WithInstanceName};
use nativelink_error::{Code, Error, ResultExt, make_err, make_input_err};
use nativelink_proto::build::bazel::remote::execution::v2::action_cache_server::{
    ActionCache, ActionCacheServer as Server,
};
use nativelink_proto::build::bazel::remote::execution::v2::{
    ActionResult, GetActionResultRequest, UpdateActionResultRequest,
};
use nativelink_store::ac_utils::{ESTIMATED_DIGEST_SIZE, get_and_decode_digest};
use nativelink_store::grpc_store::GrpcStore;
use nativelink_store::store_manager::StoreManager;
use nativelink_util::common::DigestInfo;
use nativelink_util::digest_hasher::make_ctx_for_hash_func;
use nativelink_util::store_trait::{Store, StoreKey, StoreLike};
use opentelemetry::context::FutureExt;
use prost::Message;
use tonic::{Request, Response, Status};
use tracing::{Instrument, Level, error, error_span, info, instrument, warn};

#[derive(Debug, Clone)]
pub struct AcStoreInfo {
    store: Store,
    read_only: bool,
    /// When set, every successful `GetActionResult` decodes the cached
    /// `ActionResult` and checks that every output file digest is present in
    /// this CAS store before returning. On any miss the AC read is converted
    /// to `NotFound`. See `AcStoreConfig::get_self_check_store`.
    get_self_check_store: Option<Store>,
}

pub struct AcServer {
    stores: HashMap<String, AcStoreInfo>,
}

impl Debug for AcServer {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("AcServer").finish()
    }
}

impl AcServer {
    pub fn new(
        configs: &[WithInstanceName<AcStoreConfig>],
        store_manager: &StoreManager,
    ) -> Result<Self, Error> {
        let mut stores = HashMap::with_capacity(configs.len());
        for config in configs {
            let store = store_manager.get_store(&config.ac_store).ok_or_else(|| {
                make_input_err!("'ac_store': '{}' does not exist", config.ac_store)
            })?;
            let get_self_check_store = match config.get_self_check_store.as_deref() {
                Some(name) if !name.is_empty() => Some(
                    store_manager.get_store(name).ok_or_else(|| {
                        make_input_err!(
                            "'get_self_check_store': '{name}' does not exist (referenced from ac instance '{}')",
                            config.instance_name,
                        )
                    })?,
                ),
                _ => None,
            };
            stores.insert(
                config.instance_name.to_string(),
                AcStoreInfo {
                    store,
                    read_only: config.read_only,
                    get_self_check_store,
                },
            );
        }
        Ok(Self {
            stores: stores.clone(),
        })
    }

    pub fn into_service(self) -> Server<Self> {
        Server::new(self)
    }

    async fn inner_get_action_result(
        &self,
        request: GetActionResultRequest,
    ) -> Result<Response<ActionResult>, Error> {
        let instance_name = &request.instance_name;
        let store_info = self
            .stores
            .get(instance_name)
            .err_tip(|| format!("'instance_name' not configured for '{instance_name}'"))?;

        // TODO(palfrey) We should write a test for these errors.
        let digest: DigestInfo = request
            .action_digest
            .clone()
            .err_tip(|| "Action digest was not set in message")?
            .try_into()?;

        // If we are a GrpcStore we shortcut here, as this is a special store.
        if let Some(grpc_store) = store_info
            .store
            .downcast_ref::<GrpcStore>(Some(digest.into()))
        {
            return grpc_store.get_action_result(Request::new(request)).await;
        }

        let res = get_and_decode_digest::<ActionResult>(&store_info.store, digest.into()).await;
        match res {
            Ok(action_result) => {
                // CAS self-check on the AC read path. Mirrors the scheduler's
                // `completed_cas_self_check_store` logic in
                // `simple_scheduler_state_manager.rs::inner_update_operation`,
                // but on the cache-hit side: a stale `ActionResult` whose
                // output blobs are no longer in the current CAS would
                // otherwise be returned to the client and surface as a
                // missing-output failure at the next step (e.g. SOLINK / AR
                // failing on a `.o` that was never materialized).
                if let Some(cas) = store_info.get_self_check_store.as_ref() {
                    let digests: Vec<StoreKey<'static>> = action_result
                        .output_files
                        .iter()
                        .filter_map(|f| f.digest.as_ref())
                        .filter_map(|d| DigestInfo::try_from(d.clone()).ok())
                        .map(|d| StoreKey::from(d).into_owned())
                        .collect();
                    if !digests.is_empty() {
                        match cas.has_many(&digests).await {
                            Ok(results) => {
                                let missing_count =
                                    results.iter().filter(|present| present.is_none()).count();
                                if missing_count > 0 {
                                    let first_missing = action_result
                                        .output_files
                                        .iter()
                                        .zip(results.iter())
                                        .find(|(_, present)| present.is_none())
                                        .and_then(|(f, _)| f.digest.clone());
                                    warn!(
                                        action_digest = ?digest,
                                        instance_name = %instance_name,
                                        num_missing = missing_count,
                                        ?first_missing,
                                        "ac_server: AC self-check FAILED — cached ActionResult references output digests not present in CAS; converting hit to NotFound to force re-Execute",
                                    );
                                    return Err(make_err!(
                                        Code::NotFound,
                                        "AC self-check failed: {missing_count} output digest(s) missing from CAS",
                                    ));
                                }
                                info!(
                                    action_digest = ?digest,
                                    instance_name = %instance_name,
                                    num_output_files = action_result.output_files.len(),
                                    "ac_server: AC self-check OK",
                                );
                            }
                            Err(err) => {
                                // Match the scheduler's behaviour: treat a
                                // self-check store error as transient and
                                // serve the cached result rather than
                                // synthesising a NotFound. The alternative
                                // would degrade availability whenever the
                                // CAS store backend hiccups.
                                warn!(
                                    action_digest = ?digest,
                                    instance_name = %instance_name,
                                    ?err,
                                    "ac_server: AC self-check has_many() failed; serving cached ActionResult (treating store error as transient)",
                                );
                            }
                        }
                    }
                }
                Ok(Response::new(action_result))
            }
            Err(mut e) => {
                if e.code == Code::NotFound {
                    // `get_action_result` is frequent to get NotFound errors, so remove all
                    // messages to save space.
                    e.messages.clear();
                }
                Err(e)
            }
        }
    }

    async fn inner_update_action_result(
        &self,
        request: UpdateActionResultRequest,
    ) -> Result<Response<ActionResult>, Error> {
        let instance_name = &request.instance_name;
        let store_info = self
            .stores
            .get(instance_name)
            .err_tip(|| format!("'instance_name' not configured for '{instance_name}'"))?;

        if store_info.read_only {
            return Err(make_err!(
                Code::PermissionDenied,
                "The store '{instance_name}' is read only on this endpoint",
            ));
        }

        let digest: DigestInfo = request
            .action_digest
            .clone()
            .err_tip(|| "Action digest was not set in message")?
            .try_into()?;

        // If we are a GrpcStore we shortcut here, as this is a special store.
        if let Some(grpc_store) = store_info
            .store
            .downcast_ref::<GrpcStore>(Some(digest.into()))
        {
            return grpc_store.update_action_result(Request::new(request)).await;
        }

        let action_result = request
            .action_result
            .err_tip(|| "Action result was not set in message")?;

        let mut store_data = BytesMut::with_capacity(ESTIMATED_DIGEST_SIZE);
        action_result
            .encode(&mut store_data)
            .err_tip(|| "Provided ActionResult could not be serialized")?;

        store_info
            .store
            .update_oneshot(digest, store_data.freeze())
            .await
            .err_tip(|| "Failed to update in action cache")?;
        Ok(Response::new(action_result))
    }
}

#[tonic::async_trait]
impl ActionCache for AcServer {
    #[instrument(
        ret(level = Level::DEBUG),
        level = Level::ERROR,
        skip_all,
        fields(request = ?grpc_request.get_ref())
    )]
    async fn get_action_result(
        &self,
        grpc_request: Request<GetActionResultRequest>,
    ) -> Result<Response<ActionResult>, Status> {
        let request = grpc_request.into_inner();
        let digest_function = request.digest_function;
        let result = self
            .inner_get_action_result(request)
            .instrument(error_span!("ac_server_get_action_result"))
            .with_context(
                make_ctx_for_hash_func(digest_function)
                    .err_tip(|| "In AcServer::get_action_result")?,
            )
            .await;

        if let Err(ref err) = result {
            if err.code != Code::NotFound {
                error!(error = ?err, "Error in get_action_result");
            }
        }

        result.map_err(Into::into)
    }

    #[instrument(
        err,
        ret(level = Level::TRACE),
        level = Level::ERROR,
        skip_all,
        fields(request = ?grpc_request.get_ref())
    )]
    async fn update_action_result(
        &self,
        grpc_request: Request<UpdateActionResultRequest>,
    ) -> Result<Response<ActionResult>, Status> {
        let request = grpc_request.into_inner();
        let digest_function = request.digest_function;
        self.inner_update_action_result(request)
            .instrument(error_span!("ac_server_update_action_result"))
            .with_context(
                make_ctx_for_hash_func(digest_function)
                    .err_tip(|| "In AcServer::update_action_result")?,
            )
            .await
            .map_err(Into::into)
    }
}
