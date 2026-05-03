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
use nativelink_util::store_trait::{Store, StoreLike};
use opentelemetry::context::FutureExt;
use prost::Message;
use tonic::{Request, Response, Status};
use tracing::{Instrument, Level, error, error_span, instrument};

#[derive(Debug, Clone)]
pub struct AcStoreInfo {
    store: Store,
    read_only: bool,
    /// Optional CAS store referenced by `get_self_check_store` config.
    /// When set, AC reads invalidate themselves on a CAS-miss for the
    /// first output digest.
    cas_self_check: Option<Store>,
}

pub struct AcServer {
    stores: HashMap<String, AcStoreInfo>,
}

impl Debug for AcServer {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("AcServer").finish()
    }
}

/// Pick the first output blob's digest off an `ActionResult` — used
/// by the optional CAS self-check on AC reads. Prefers `output_files`
/// (most actions); falls back to `stdout_digest` so an action that
/// only produces stdout still has a verifiable witness in CAS.
fn first_output_digest(ar: &ActionResult) -> Option<DigestInfo> {
    if let Some(f) = ar.output_files.first() {
        if let Some(d) = &f.digest {
            return DigestInfo::try_from(d.clone()).ok();
        }
    }
    if let Some(d) = &ar.stdout_digest {
        return DigestInfo::try_from(d.clone()).ok();
    }
    None
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
            let cas_self_check = match &config.get_self_check_store {
                Some(name) => Some(store_manager.get_store(name).ok_or_else(|| {
                    make_input_err!("'get_self_check_store': '{name}' does not exist")
                })?),
                None => None,
            };
            stores.insert(
                config.instance_name.to_string(),
                AcStoreInfo {
                    store,
                    read_only: config.read_only,
                    cas_self_check,
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
                // Optional self-check: invalidate the AC entry if its
                // first output blob is no longer in CAS. This is a
                // cheap defense-in-depth catch for AC-says-yes /
                // CAS-says-no, observed in the 3-Mac runbook.
                if let Some(cas) = &store_info.cas_self_check {
                    if let Some(first_digest) = first_output_digest(&action_result) {
                        let key: nativelink_util::store_trait::StoreKey<'static> =
                            first_digest.into();
                        match cas.has(key).await {
                            Ok(Some(_)) => { /* present — fall through */ }
                            Ok(None) => {
                                tracing::warn!(
                                    %digest,
                                    %first_digest,
                                    "AC self-check FAILED: first output blob missing from CAS — \
                                     returning NotFound to invalidate the stale AC entry",
                                );
                                return Err(make_err!(
                                    Code::NotFound,
                                    "AC self-check failed: first output digest {first_digest} \
                                     not in CAS for AC entry {digest}"
                                ));
                            }
                            Err(e) => {
                                tracing::warn!(
                                    %digest,
                                    error = ?e,
                                    "AC self-check: CAS .has() errored; treating as pass to avoid \
                                     false-invalidation",
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
