// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use async_recursion::async_recursion;
use async_trait::async_trait;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use log::debug;
use log::info;
use tokio::sync::Semaphore;
use tokio::time::sleep;

use crate::backoff::Backoff;
use crate::pd::PdClient;
use crate::proto::errorpb;
use crate::proto::errorpb::EpochNotMatch;
use crate::proto::kvrpcpb;
use crate::proto::metapb;
use crate::region::StoreId;
use crate::region::{RegionVerId, RegionWithLeader};
use crate::request::shard::HasNextBatch;
use crate::request::NextBatch;
use crate::request::ReadRouting;
use crate::request::Shardable;
use crate::request::{KvRequest, StoreRequest};
use crate::stats::tikv_stats;
use crate::store::HasRegionError;
use crate::store::HasRegionErrors;
use crate::store::KvClient;
use crate::store::RegionStore;
use crate::store::{HasKeyErrors, SetRegionError, Store};
use crate::transaction::resolve_locks;
use crate::transaction::HasLocks;
use crate::transaction::ResolveLocksContext;
use crate::transaction::ResolveLocksOptions;
use crate::Error;
use crate::RequestContext;
use crate::Result;

use super::keyspace::Keyspace;
use super::pending_backoff::PendingBackoffKind;

fn replica_kind_for_peer(
    leader: &metapb::Peer,
    peer: &metapb::Peer,
) -> crate::interceptor::ReplicaKind {
    if peer.store_id == leader.store_id {
        return crate::interceptor::ReplicaKind::Leader;
    }
    match metapb::PeerRole::try_from(peer.role).unwrap_or(metapb::PeerRole::Voter) {
        metapb::PeerRole::Learner => crate::interceptor::ReplicaKind::Learner,
        _ => crate::interceptor::ReplicaKind::Follower,
    }
}

fn replica_kind_str(kind: crate::interceptor::ReplicaKind) -> &'static str {
    match kind {
        crate::interceptor::ReplicaKind::Leader => "leader",
        crate::interceptor::ReplicaKind::Follower => "follower",
        crate::interceptor::ReplicaKind::Learner => "learner",
    }
}

/// A plan for how to execute a request. A user builds up a plan with various
/// options, then exectutes it.
#[async_trait]
pub trait Plan: Sized + Clone + Sync + Send + 'static {
    /// The ultimate result of executing the plan (should be a high-level type, not a GRPC response).
    type Result: Send;

    /// Execute the plan.
    async fn execute(&self) -> Result<Self::Result>;
}

/// The simplest plan which just dispatches a request to a specific kv server.
#[derive(Clone)]
pub struct Dispatch<Req: KvRequest> {
    pub request: Req,
    pub kv_client: Option<Arc<dyn KvClient + Send + Sync>>,
}

#[async_trait]
impl<Req: KvRequest> Plan for Dispatch<Req> {
    type Result = Req::Response;

    async fn execute(&self) -> Result<Self::Result> {
        let stats = tikv_stats(self.request.label());
        let kv_client = self.kv_client.as_ref().ok_or_else(|| {
            crate::internal_err!("Dispatch plan executed without an attached kv_client")
        })?;
        let result = kv_client.dispatch(&self.request).await;
        let result = stats.done(result);
        result.and_then(|r| {
            r.downcast::<Req::Response>()
                .map(|r| *r)
                .map_err(|_| crate::internal_err!("KvClient returned an unexpected response type"))
        })
    }
}

impl<Req: KvRequest + StoreRequest> StoreRequest for Dispatch<Req> {
    fn apply_store(&mut self, store: &Store) {
        self.kv_client = Some(store.client.clone());
        self.request.apply_store(store);
    }
}

pub(crate) const MULTI_REGION_CONCURRENCY: usize = 16;
pub(crate) const MULTI_STORES_CONCURRENCY: usize = 16;

fn is_grpc_error(e: &Error) -> bool {
    matches!(e, Error::GrpcAPI(_) | Error::Grpc(_))
}

pub struct RetryableMultiRegion<P: Plan, PdC: PdClient> {
    pub(super) inner: P,
    pub pd_client: Arc<PdC>,
    pub backoff: Backoff,
    pub concurrency: usize,

    /// Preserve all regions' results for other downstream plans to handle.
    /// If true, return Ok and preserve all regions' results, even if some of them are Err.
    /// Otherwise, return the first Err if there is any.
    pub preserve_region_results: bool,

    pub(crate) request_context: RequestContext,
    pub(crate) read_routing: ReadRouting,
}

impl<P: Plan + Shardable, PdC: PdClient> RetryableMultiRegion<P, PdC>
where
    P::Result: HasKeyErrors + HasRegionError,
{
    // A plan may involve multiple shards
    #[allow(clippy::too_many_arguments)]
    #[async_recursion]
    async fn single_plan_handler(
        pd_client: Arc<PdC>,
        current_plan: P,
        backoff: Backoff,
        permits: Arc<Semaphore>,
        preserve_region_results: bool,
        request_context: RequestContext,
        read_routing: ReadRouting,
        attempt: usize,
    ) -> Result<<Self as Plan>::Result> {
        let shards = current_plan.shards(&pd_client).collect::<Vec<_>>().await;
        debug!("single_plan_handler, shards: {}", shards.len());

        // Preserve shard order for the returned results. This is important for callers that
        // concatenate region results (e.g. range-based scans / keep-order semantics) and expect
        // the output to follow the shard stream order, even when per-shard requests run
        // concurrently.
        let shard_count = shards.len();
        let mut tasks = FuturesUnordered::new();
        for (idx, shard) in shards.into_iter().enumerate() {
            let (shard, region) = shard?;
            let clone = current_plan.clone_then_apply_shard(shard);
            let pd_client = pd_client.clone();
            let backoff = backoff.clone();
            let permits = permits.clone();
            let request_context = request_context.clone();
            let read_routing = read_routing.clone();
            let fut = async move {
                let res = Self::single_shard_handler(
                    pd_client,
                    clone,
                    region,
                    backoff,
                    permits,
                    preserve_region_results,
                    request_context,
                    read_routing,
                    attempt,
                )
                .await;
                (idx, res)
            };
            tasks.push(fut);
        }

        let mut slots: Vec<Option<Result<<Self as Plan>::Result>>> =
            std::iter::repeat_with(|| None).take(shard_count).collect();
        if preserve_region_results {
            while let Some((idx, res)) = tasks.next().await {
                slots[idx] = Some(res);
            }
        } else {
            while let Some((idx, res)) = tasks.next().await {
                let ok = res?;
                slots[idx] = Some(Ok(ok));
            }
        }

        let mut results = Vec::new();
        for (idx, slot) in slots.into_iter().enumerate() {
            let Some(res) = slot else {
                return Err(crate::internal_err!(
                    "RetryableMultiRegion missing shard result at index {}",
                    idx
                ));
            };
            match res {
                Ok(v) => results.extend(v),
                Err(e) => results.push(Err(e)),
            }
        }
        Ok(results)
    }

    #[allow(clippy::too_many_arguments)]
    #[async_recursion]
    async fn single_shard_handler(
        pd_client: Arc<PdC>,
        mut plan: P,
        region: RegionWithLeader,
        mut backoff: Backoff,
        permits: Arc<Semaphore>,
        preserve_region_results: bool,
        request_context: RequestContext,
        read_routing: ReadRouting,
        attempt: usize,
    ) -> Result<<Self as Plan>::Result> {
        debug!("single_shard_handler");
        let read_peer = match read_routing.select_peer(&region, attempt) {
            Ok(read_peer) => read_peer,
            Err(Error::LeaderNotFound { region }) => {
                debug!(
                    "single_shard_handler::select_peer: leader not found: {:?}",
                    region
                );
                return Self::handle_other_error(
                    pd_client,
                    plan,
                    region.clone(),
                    None,
                    backoff,
                    permits,
                    preserve_region_results,
                    request_context.clone(),
                    read_routing,
                    attempt + 1,
                    Error::LeaderNotFound { region },
                )
                .await;
            }
            Err(err) => return Err(err),
        };

        let Some(leader_peer) = region.leader.as_ref() else {
            return Self::handle_other_error(
                pd_client,
                plan,
                region.ver_id(),
                None,
                backoff,
                permits,
                preserve_region_results,
                request_context.clone(),
                read_routing,
                attempt + 1,
                Error::LeaderNotFound {
                    region: region.ver_id(),
                },
            )
            .await;
        };
        let current_replica_kind = replica_kind_for_peer(leader_peer, &read_peer.target_peer);
        let request_source_override = match read_routing.request_source() {
            None => None,
            Some("") => None,
            Some(input) => {
                let base_peer = read_routing.select_peer_for_request_source(&region)?;
                let base_replica_kind = replica_kind_for_peer(leader_peer, &base_peer.target_peer);
                let base_read_type = if base_peer.stale_read {
                    format!("stale_{}", replica_kind_str(base_replica_kind))
                } else {
                    replica_kind_str(base_replica_kind).to_owned()
                };
                if attempt == 0 {
                    Some(format!("{base_read_type}_{input}"))
                } else {
                    Some(format!(
                        "retry_{base_read_type}_{}_{}",
                        replica_kind_str(current_replica_kind),
                        input
                    ))
                }
            }
        };
        let mut target_region = region.clone();
        // `map_region_to_store` is keyed by `RegionWithLeader::leader.store_id`, so we overwrite it
        // with the selected target peer for this request.
        target_region.leader = Some(read_peer.target_peer.clone());

        let region_store = match pd_client
            .clone()
            .map_region_to_store(target_region)
            .await
            .and_then(|region_store| {
                let mut region_store = region_store;
                region_store.replica_read = read_peer.replica_read;
                region_store.stale_read = read_peer.stale_read;
                region_store.attempt = attempt;
                region_store.request_context = request_context.clone();
                region_store.replica_kind = Some(current_replica_kind);
                region_store.patched_request_source = request_source_override.clone();
                plan.apply_store(&region_store)?;
                Ok(region_store)
            }) {
            Ok(region_store) => region_store,
            Err(Error::LeaderNotFound { region }) => {
                debug!(
                    "single_shard_handler::sharding: leader not found: {:?}",
                    region
                );
                return Self::handle_other_error(
                    pd_client,
                    plan,
                    region.clone(),
                    None,
                    backoff,
                    permits,
                    preserve_region_results,
                    request_context.clone(),
                    read_routing,
                    attempt + 1,
                    Error::LeaderNotFound { region },
                )
                .await;
            }
            Err(err) => {
                debug!("single_shard_handler::sharding, error: {:?}", err);
                return Err(err);
            }
        };

        // If we previously fast-retried away from this store, apply a pending backoff before
        // dispatching another request to it.
        if let Ok(store_id) = region_store.region_with_leader.get_store_id() {
            if let Some(pending) = read_routing.take_pending_backoff_for_retry(Some(store_id)) {
                debug!(
                    "applying pending backoff: store_id={store_id} kind={:?} err={:?}",
                    pending.kind, pending.error
                );
                match backoff.next_delay_duration() {
                    Some(duration) => {
                        crate::stats::observe_backoff_sleep("pending_backoff", duration);
                        sleep(duration).await;
                    }
                    None => {
                        return Err(Error::RegionError(Box::new(errorpb::Error {
                            message: "backoff attempts exhausted while applying pending backoff"
                                .to_owned(),
                            ..Default::default()
                        })));
                    }
                }
            }
        }

        // limit concurrent requests
        let permit = permits.acquire().await.map_err(|e| {
            crate::internal_err!("semaphore closed while acquiring permit: {:?}", e)
        })?;
        let res = plan.execute().await;
        drop(permit);

        let mut resp = match res {
            Ok(resp) => resp,
            Err(e) if is_grpc_error(&e) => {
                debug!("single_shard_handler:execute: grpc error: {:?}", e);
                if let Ok(store_id) = region_store.region_with_leader.get_store_id() {
                    // Best-effort: treat transport errors as a signal that the target store may be
                    // unhealthy and should be deprioritized on subsequent replica selections.
                    read_routing.mark_store_slow_for(store_id, Duration::from_millis(500));
                }
                return Self::handle_other_error(
                    pd_client,
                    plan,
                    region_store.region_with_leader.ver_id(),
                    region_store.region_with_leader.get_store_id().ok(),
                    backoff,
                    permits,
                    preserve_region_results,
                    request_context.clone(),
                    read_routing,
                    attempt + 1,
                    e,
                )
                .await;
            }
            Err(e) => {
                debug!("single_shard_handler:execute: error: {:?}", e);
                return Err(e);
            }
        };

        if let Some(e) = resp.key_errors() {
            debug!("single_shard_handler:execute: key errors: {:?}", e);
            Ok(vec![Err(Error::MultipleKeyErrors(e))])
        } else if let Some(e) = resp.region_error() {
            debug!("single_shard_handler:execute: region error: {:?}", e);
            if e.data_is_not_ready.is_some() && region_store.stale_read {
                // `data_is_not_ready` indicates the target peer's safe-ts has not caught up enough
                // to serve a stale read at the requested read-ts. This is not a region-metadata
                // issue, so do not invalidate region/store caches; instead, wait and retry.
                //
                // NOTE: default region backoff is ~1.5s total, which is often insufficient for
                // newly-started clusters / CI environments to advance safe-ts.
                const MAX_RETRIES: usize = 120;
                const SLEEP: Duration = Duration::from_millis(200);

                if attempt >= MAX_RETRIES {
                    return Err(Error::RegionError(Box::new(e)));
                }

                crate::stats::observe_backoff_sleep("stale_read_not_ready", SLEEP);
                sleep(SLEEP).await;
                return Self::single_plan_handler(
                    pd_client,
                    plan,
                    backoff,
                    permits,
                    preserve_region_results,
                    request_context.clone(),
                    read_routing,
                    attempt + 1,
                )
                .await;
            }

            let region_id = region_store.region_with_leader.id();
            let keep_peer = e.max_timestamp_not_synced.is_some()
                || e.read_index_not_ready.is_some()
                || e.proposal_in_merging_mode.is_some();
            if keep_peer {
                if let Ok(store_id) = region_store.region_with_leader.get_store_id() {
                    read_routing.pin_store_for_region(region_id, store_id);
                }
            } else {
                read_routing.clear_pinned_store_for_region(region_id);
            }

            if read_routing.is_replica_read_mode() && e.stale_command.is_some() {
                // client-go's replica selector does not backoff on stale-command; it simply tries
                // another peer. Keep a small bound (number of non-witness peers) to avoid
                // unbounded recursion when all peers keep returning stale-command.
                read_routing.clear_pinned_store_for_region(region_id);
                let non_witness_peers = region_store
                    .region_with_leader
                    .region
                    .peers
                    .iter()
                    .filter(|peer| !peer.is_witness)
                    .count();
                if attempt + 1 >= non_witness_peers {
                    pd_client
                        .invalidate_region_cache(region_store.region_with_leader.ver_id())
                        .await;
                    return Err(Error::RegionError(Box::new(e)));
                }

                return Self::single_plan_handler(
                    pd_client,
                    plan,
                    backoff,
                    permits,
                    preserve_region_results,
                    request_context.clone(),
                    read_routing,
                    attempt + 1,
                )
                .await;
            }

            let non_witness_peers = region_store
                .region_with_leader
                .region
                .peers
                .iter()
                .filter(|peer| !peer.is_witness)
                .count();
            let store_id = region_store.region_with_leader.get_store_id().ok();

            let region_error = e.clone();
            let region_error_resolved =
                handle_region_error(pd_client.clone(), e, region_store).await?;
            // If we've resolved the region error (e.g. NotLeader with a leader), retry immediately
            // without consuming a backoff attempt. This matches client-go's behavior: backoff only
            // applies when we need to wait for the cluster to converge (e.g. leader election).
            if region_error_resolved {
                return Self::single_plan_handler(
                    pd_client,
                    plan,
                    backoff,
                    permits,
                    preserve_region_results,
                    request_context.clone(),
                    read_routing,
                    attempt + 1,
                )
                .await;
            }

            if read_routing.is_replica_read_mode() && region_error.server_is_busy.is_some() {
                // Port client-go's replica selector fast retry behavior for server-is-busy: try a
                // different peer first (bounded by the number of peers), and only then back off.
                if non_witness_peers > 1 && attempt + 1 < non_witness_peers {
                    if let Some(store_id) = store_id {
                        read_routing.add_pending_backoff(
                            Some(store_id),
                            PendingBackoffKind::TikvServerBusy,
                            region_error.clone(),
                        );
                        read_routing.mark_store_slow_for(store_id, Duration::from_millis(500));
                    }
                    return Self::single_plan_handler(
                        pd_client,
                        plan,
                        backoff,
                        permits,
                        preserve_region_results,
                        request_context.clone(),
                        read_routing,
                        attempt + 1,
                    )
                    .await;
                }
            }

            match backoff.next_delay_duration() {
                Some(duration) => {
                    crate::stats::observe_backoff_sleep("region", duration);
                    sleep(duration).await;
                    Self::single_plan_handler(
                        pd_client,
                        plan,
                        backoff,
                        permits,
                        preserve_region_results,
                        request_context.clone(),
                        read_routing,
                        attempt + 1,
                    )
                    .await
                }
                None => Err(Error::RegionError(Box::new(region_error))),
            }
        } else {
            read_routing.clear_pinned_store_for_region(region_store.region_with_leader.id());
            Ok(vec![Ok(resp)])
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn handle_other_error(
        pd_client: Arc<PdC>,
        plan: P,
        region: RegionVerId,
        store: Option<StoreId>,
        mut backoff: Backoff,
        permits: Arc<Semaphore>,
        preserve_region_results: bool,
        request_context: RequestContext,
        read_routing: ReadRouting,
        attempt: usize,
        e: Error,
    ) -> Result<<Self as Plan>::Result> {
        debug!("handle_other_error: {:?}", e);
        pd_client.invalidate_region_cache(region).await;
        if is_grpc_error(&e) {
            if let Some(store_id) = store {
                pd_client.invalidate_store_cache(store_id).await;
            }
        }
        match backoff.next_delay_duration() {
            Some(duration) => {
                crate::stats::observe_backoff_sleep("grpc", duration);
                sleep(duration).await;
                Self::single_plan_handler(
                    pd_client,
                    plan,
                    backoff,
                    permits,
                    preserve_region_results,
                    request_context,
                    read_routing,
                    attempt + 1,
                )
                .await
            }
            None => Err(e),
        }
    }
}

// Returns
// 1. Ok(true): error has been resolved, retry immediately
// 2. Ok(false): backoff, and then retry
// 3. Err(Error): can't be resolved, return the error to upper level
pub(crate) async fn handle_region_error<PdC: PdClient>(
    pd_client: Arc<PdC>,
    e: errorpb::Error,
    region_store: RegionStore,
) -> Result<bool> {
    debug!("handle_region_error: {:?}", e);
    let ver_id = region_store.region_with_leader.ver_id();
    let store_id = region_store.region_with_leader.get_store_id();
    if let Some(not_leader) = e.not_leader {
        if let Some(leader) = not_leader.leader {
            match pd_client
                .update_leader(region_store.region_with_leader.ver_id(), leader)
                .await
            {
                Ok(_) => Ok(true),
                Err(e) => {
                    pd_client.invalidate_region_cache(ver_id).await;
                    Err(e)
                }
            }
        } else {
            // The peer doesn't know who is the current leader. Generally it's because
            // the Raft group is in an election, but it's possible that the peer is
            // isolated and removed from the Raft group. So it's necessary to reload
            // the region from PD.
            pd_client.invalidate_region_cache(ver_id).await;
            Ok(false)
        }
    } else if e.store_not_match.is_some() {
        pd_client.invalidate_region_cache(ver_id).await;
        if let Ok(store_id) = store_id {
            pd_client.invalidate_store_cache(store_id).await;
        }
        Ok(true)
    } else if let Some(epoch_not_match) = e.epoch_not_match {
        on_region_epoch_not_match(pd_client.clone(), region_store, epoch_not_match).await
    } else if e.region_not_found.is_some() || e.key_not_in_region.is_some() {
        pd_client.invalidate_region_cache(ver_id).await;
        Ok(true)
    } else if e.stale_command.is_some() {
        pd_client.invalidate_region_cache(ver_id).await;
        Ok(false)
    } else if e.server_is_busy.is_some()
        || e.max_timestamp_not_synced.is_some()
        || e.read_index_not_ready.is_some()
        || e.proposal_in_merging_mode.is_some()
    {
        Ok(false)
    } else if e.raft_entry_too_large.is_some() {
        Err(Error::RegionError(Box::new(e)))
    } else {
        debug!("handle_region_error: unknown region error: {:?}", e);
        pd_client.invalidate_region_cache(ver_id).await;
        Ok(false)
    }
}

// Returns
// 1. Ok(true): error has been resolved, retry immediately
// 2. Ok(false): backoff, and then retry
// 3. Err(Error): can't be resolved, return the error to upper level
pub(crate) async fn on_region_epoch_not_match<PdC: PdClient>(
    pd_client: Arc<PdC>,
    region_store: RegionStore,
    error: EpochNotMatch,
) -> Result<bool> {
    let ver_id = region_store.region_with_leader.ver_id();
    if error.current_regions.is_empty() {
        pd_client.invalidate_region_cache(ver_id).await;
        // client-go treats this as a "fake" EpochNotMatch and backs off (MayBackoffForRegionError).
        return Ok(false);
    }

    for r in error.current_regions {
        if r.id == region_store.region_with_leader.id() {
            let Some(region_epoch) = r.region_epoch else {
                pd_client.invalidate_region_cache(ver_id).await;
                return Ok(true);
            };
            let returned_conf_ver = region_epoch.conf_ver;
            let returned_version = region_epoch.version;
            let Some(current_region_epoch) =
                region_store.region_with_leader.region.region_epoch.clone()
            else {
                pd_client.invalidate_region_cache(ver_id).await;
                return Ok(true);
            };
            let current_conf_ver = current_region_epoch.conf_ver;
            let current_version = current_region_epoch.version;

            // Find whether the current region is ahead of TiKV's. If so, backoff.
            if returned_conf_ver < current_conf_ver || returned_version < current_version {
                return Ok(false);
            }
        }
    }
    // We are behind TiKV's region epoch (or the mismatch is from a split/merge). Invalidate our
    // cached region and retry immediately to re-shard on the new region layout.
    pd_client.invalidate_region_cache(ver_id).await;
    Ok(true)
}

impl<P: Plan, PdC: PdClient> Clone for RetryableMultiRegion<P, PdC> {
    fn clone(&self) -> Self {
        RetryableMultiRegion {
            inner: self.inner.clone(),
            pd_client: self.pd_client.clone(),
            backoff: self.backoff.clone(),
            concurrency: self.concurrency,
            preserve_region_results: self.preserve_region_results,
            request_context: self.request_context.clone(),
            read_routing: self.read_routing.clone(),
        }
    }
}

#[async_trait]
impl<P: Plan + Shardable, PdC: PdClient> Plan for RetryableMultiRegion<P, PdC>
where
    P::Result: HasKeyErrors + HasRegionError,
{
    type Result = Vec<Result<P::Result>>;

    async fn execute(&self) -> Result<Self::Result> {
        // Limit the maximum concurrency of multi-region request. If there are
        // too many concurrent requests, TiKV is more likely to return a "TiKV
        // is busy" error
        let concurrency = self.concurrency.max(1);
        let concurrency_permits = Arc::new(Semaphore::new(concurrency));
        Self::single_plan_handler(
            self.pd_client.clone(),
            self.inner.clone(),
            self.backoff.clone(),
            concurrency_permits.clone(),
            self.preserve_region_results,
            self.request_context.clone(),
            self.read_routing.clone(),
            0,
        )
        .await
    }
}

pub struct RetryableAllStores<P: Plan, PdC: PdClient> {
    pub(super) inner: P,
    pub pd_client: Arc<PdC>,
    pub backoff: Backoff,
}

impl<P: Plan, PdC: PdClient> Clone for RetryableAllStores<P, PdC> {
    fn clone(&self) -> Self {
        RetryableAllStores {
            inner: self.inner.clone(),
            pd_client: self.pd_client.clone(),
            backoff: self.backoff.clone(),
        }
    }
}

// About `HasRegionError`:
// Store requests should be return region errors.
// But as the response of only store request by now (UnsafeDestroyRangeResponse) has the `region_error` field,
// we require `HasRegionError` to check whether there is region error returned from TiKV.
#[async_trait]
impl<P: Plan + StoreRequest, PdC: PdClient> Plan for RetryableAllStores<P, PdC>
where
    P::Result: HasKeyErrors + HasRegionError,
{
    type Result = Vec<Result<P::Result>>;

    async fn execute(&self) -> Result<Self::Result> {
        let concurrency_permits = Arc::new(Semaphore::new(MULTI_STORES_CONCURRENCY));
        let stores = self.pd_client.clone().all_stores().await?;
        let mut tasks = FuturesUnordered::new();
        for store in stores {
            let mut clone = self.inner.clone();
            clone.apply_store(&store);
            let fut = Self::single_store_handler(
                clone,
                self.backoff.clone(),
                concurrency_permits.clone(),
            );
            tasks.push(fut);
        }

        let mut results = Vec::new();
        while let Some(res) = tasks.next().await {
            results.push(res);
        }
        Ok(results)
    }
}

impl<P: Plan, PdC: PdClient> RetryableAllStores<P, PdC>
where
    P::Result: HasKeyErrors + HasRegionError,
{
    async fn single_store_handler(
        plan: P,
        mut backoff: Backoff,
        permits: Arc<Semaphore>,
    ) -> Result<P::Result> {
        loop {
            let permit = permits.acquire().await.map_err(|e| {
                crate::internal_err!("semaphore closed while acquiring permit: {:?}", e)
            })?;
            let res = plan.execute().await;
            drop(permit);

            match res {
                Ok(mut resp) => {
                    if let Some(e) = resp.key_errors() {
                        return Err(Error::MultipleKeyErrors(e));
                    } else if let Some(e) = resp.region_error() {
                        // Store request should not return region error.
                        return Err(Error::RegionError(Box::new(e)));
                    } else {
                        return Ok(resp);
                    }
                }
                Err(e) if is_grpc_error(&e) => match backoff.next_delay_duration() {
                    Some(duration) => {
                        crate::stats::observe_backoff_sleep("grpc", duration);
                        sleep(duration).await;
                        continue;
                    }
                    None => return Err(e),
                },
                Err(e) => return Err(e),
            }
        }
    }
}

/// A technique for merging responses into a single result (with type `Out`).
pub trait Merge<In>: Sized + Clone + Send + Sync + 'static {
    type Out: Send;

    fn merge(&self, input: Vec<Result<In>>) -> Result<Self::Out>;
}

#[derive(Clone)]
pub struct MergeResponse<P: Plan, In, M: Merge<In>> {
    pub inner: P,
    pub merge: M,
    pub phantom: PhantomData<In>,
}

#[async_trait]
impl<In: Clone + Send + Sync + 'static, P: Plan<Result = Vec<Result<In>>>, M: Merge<In>> Plan
    for MergeResponse<P, In, M>
{
    type Result = M::Out;

    async fn execute(&self) -> Result<Self::Result> {
        self.merge.merge(self.inner.execute().await?)
    }
}

/// A merge strategy which collects data from a response into a single type.
#[derive(Clone, Copy)]
pub struct Collect;

/// A merge strategy that only takes the first element. It's used for requests
/// that should have exactly one response, e.g. a get request.
#[derive(Clone, Copy)]
pub struct CollectSingle;

#[doc(hidden)]
#[macro_export]
macro_rules! collect_single {
    ($type_: ty) => {
        impl Merge<$type_> for CollectSingle {
            type Out = $type_;

            fn merge(&self, mut input: Vec<Result<$type_>>) -> Result<Self::Out> {
                if input.len() != 1 {
                    return Err($crate::internal_err!(
                        "CollectSingle expected 1 response, got {}",
                        input.len()
                    ));
                }
                input
                    .pop()
                    .ok_or_else(|| $crate::internal_err!("CollectSingle missing response"))?
            }
        }
    };
}

/// A merge strategy to be used with
/// [`preserve_shard`](super::plan_builder::PlanBuilder::preserve_shard).
/// It matches the shards preserved before and the values returned in the response.
#[derive(Clone, Debug)]
pub struct CollectWithShard;

/// A merge strategy which returns an error if any response is an error and
/// otherwise returns a Vec of the results.
#[derive(Clone, Copy)]
pub struct CollectError;

impl<T: Send> Merge<T> for CollectError {
    type Out = Vec<T>;

    fn merge(&self, input: Vec<Result<T>>) -> Result<Self::Out> {
        input.into_iter().collect()
    }
}

/// Process data into another kind of data.
pub trait Process<In>: Sized + Clone + Send + Sync + 'static {
    type Out: Send;

    fn process(&self, input: Result<In>) -> Result<Self::Out>;
}

#[derive(Clone)]
pub struct ProcessResponse<P: Plan, Pr: Process<P::Result>> {
    pub inner: P,
    pub processor: Pr,
}

#[async_trait]
impl<P: Plan, Pr: Process<P::Result>> Plan for ProcessResponse<P, Pr> {
    type Result = Pr::Out;

    async fn execute(&self) -> Result<Self::Result> {
        self.processor.process(self.inner.execute().await)
    }
}

#[derive(Clone, Copy, Debug)]
pub struct DefaultProcessor;

pub struct ResolveLock<P: Plan, PdC: PdClient> {
    pub inner: P,
    pub pd_client: Arc<PdC>,
    pub backoff: Backoff,
    pub keyspace: Keyspace,
    pub(crate) request_context: RequestContext,
    pub(crate) read_routing: ReadRouting,
}

impl<P: Plan, PdC: PdClient> Clone for ResolveLock<P, PdC> {
    fn clone(&self) -> Self {
        ResolveLock {
            inner: self.inner.clone(),
            pd_client: self.pd_client.clone(),
            backoff: self.backoff.clone(),
            keyspace: self.keyspace,
            request_context: self.request_context.clone(),
            read_routing: self.read_routing.clone(),
        }
    }
}

#[async_trait]
impl<P: Plan, PdC: PdClient> Plan for ResolveLock<P, PdC>
where
    P::Result: HasLocks + Default + SetRegionError,
{
    type Result = P::Result;

    async fn execute(&self) -> Result<Self::Result> {
        let mut result = self.inner.execute().await?;
        let mut clone = self.clone();
        loop {
            let locks = result.take_locks();
            if locks.is_empty() {
                return Ok(result);
            }

            if self.backoff.is_none() {
                return Err(Error::ResolveLockError(locks));
            }

            let stale_read_meet_lock = self.read_routing.is_stale_read();
            if stale_read_meet_lock {
                // Align with client-go: once stale-read sees a lock, it falls back to a leader read
                // after resolving locks.
                self.read_routing.force_leader();
            }

            let pd_client = self.pd_client.clone();
            let live_locks = resolve_locks(
                locks,
                pd_client.clone(),
                self.keyspace,
                &self.request_context,
            )
            .await?;

            if stale_read_meet_lock {
                // Trigger a region-level retry so the request can be re-routed (to leader). We use
                // `epoch_not_match` with empty `current_regions` to avoid region-backoff sleeps.
                if live_locks.is_empty() {
                    return Ok(reroute_to_leader::<P::Result>());
                }
                return match clone.backoff.next_delay_duration() {
                    None => Err(Error::ResolveLockError(live_locks)),
                    Some(delay_duration) => {
                        crate::stats::observe_backoff_sleep("lock", delay_duration);
                        sleep(delay_duration).await;
                        Ok(reroute_to_leader::<P::Result>())
                    }
                };
            }

            if live_locks.is_empty() {
                result = self.inner.execute().await?;
                continue;
            }

            match clone.backoff.next_delay_duration() {
                None => return Err(Error::ResolveLockError(live_locks)),
                Some(delay_duration) => {
                    crate::stats::observe_backoff_sleep("lock", delay_duration);
                    sleep(delay_duration).await;
                    result = clone.inner.execute().await?;
                }
            }
        }
    }
}

fn reroute_to_leader<R: Default + SetRegionError>() -> R {
    let mut resp = R::default();
    let mut region_error = errorpb::Error::default();
    region_error.epoch_not_match = Some(EpochNotMatch::default());
    resp.set_region_error(region_error);
    resp
}

#[derive(Debug, Default)]
pub struct CleanupLocksResult {
    pub region_error: Option<errorpb::Error>,
    pub key_error: Option<Vec<Error>>,
    pub resolved_locks: usize,
}

impl Clone for CleanupLocksResult {
    fn clone(&self) -> Self {
        Self {
            resolved_locks: self.resolved_locks,
            ..Default::default() // Ignore errors, which should be extracted by `extract_error()`.
        }
    }
}

impl HasRegionError for CleanupLocksResult {
    fn region_error(&mut self) -> Option<errorpb::Error> {
        self.region_error.take()
    }
}

impl HasKeyErrors for CleanupLocksResult {
    fn key_errors(&mut self) -> Option<Vec<Error>> {
        self.key_error.take()
    }
}

impl Merge<CleanupLocksResult> for Collect {
    type Out = CleanupLocksResult;

    fn merge(&self, input: Vec<Result<CleanupLocksResult>>) -> Result<Self::Out> {
        input
            .into_iter()
            .try_fold(CleanupLocksResult::default(), |acc, x| {
                Ok(CleanupLocksResult {
                    resolved_locks: acc.resolved_locks + x?.resolved_locks,
                    ..Default::default()
                })
            })
    }
}

pub struct CleanupLocks<P: Plan, PdC: PdClient> {
    pub inner: P,
    pub ctx: ResolveLocksContext,
    pub options: ResolveLocksOptions,
    pub store: Option<RegionStore>,
    pub pd_client: Arc<PdC>,
    pub keyspace: Keyspace,
    pub backoff: Backoff,
    pub(crate) request_context: RequestContext,
}

impl<P: Plan, PdC: PdClient> Clone for CleanupLocks<P, PdC> {
    fn clone(&self) -> Self {
        CleanupLocks {
            inner: self.inner.clone(),
            ctx: self.ctx.clone(),
            options: self.options,
            store: None,
            pd_client: self.pd_client.clone(),
            keyspace: self.keyspace,
            backoff: self.backoff.clone(),
            request_context: self.request_context.clone(),
        }
    }
}

#[async_trait]
impl<P: Plan + Shardable + NextBatch, PdC: PdClient> Plan for CleanupLocks<P, PdC>
where
    P::Result: HasLocks + HasNextBatch + HasKeyErrors + HasRegionError,
{
    type Result = CleanupLocksResult;

    async fn execute(&self) -> Result<Self::Result> {
        let mut result = CleanupLocksResult::default();
        let mut inner = self.inner.clone();
        let store = self.store.as_ref().ok_or_else(|| {
            crate::internal_err!("CleanupLocks executed without an attached store")
        })?;
        let mut lock_resolver = crate::transaction::LockResolver::new(
            self.ctx.clone(),
            self.pd_client.clone(),
            self.keyspace,
        )
        .with_request_context(self.request_context.clone());
        let region = &store.region_with_leader;
        let mut has_more_batch = true;
        let mut backoff = self.backoff.clone();

        while has_more_batch {
            let mut scan_lock_resp = inner.execute().await?;

            // Propagate errors to `retry_multi_region` for retry.
            if let Some(e) = scan_lock_resp.key_errors() {
                info!("CleanupLocks::execute, inner key errors:{:?}", e);
                result.key_error = Some(e);
                return Ok(result);
            } else if let Some(e) = scan_lock_resp.region_error() {
                info!("CleanupLocks::execute, inner region error:{}", e.message);
                result.region_error = Some(e);
                return Ok(result);
            }

            // Prepare next batch bounds, but do not advance `inner` until we have successfully
            // cleaned the current batch. Otherwise a retry could skip unresolved locks.
            let end_key = inner.end_key();
            let next_range = match scan_lock_resp.has_next_batch() {
                Some(range) if region.contains(range.0.as_ref()) => Some(range),
                _ => None,
            }
            .filter(|(start_key, _)| end_key.is_empty() || start_key.as_slice() < end_key);

            let mut locks = scan_lock_resp.take_locks();
            if locks.is_empty() {
                break;
            }
            let mut next_has_more_batch = next_range.is_some();
            if locks.len() < self.options.batch_size as usize {
                next_has_more_batch = false;
            }

            if self.options.async_commit_only {
                locks = locks
                    .into_iter()
                    .filter(|l| l.use_async_commit)
                    .collect::<Vec<_>>();
            }
            if locks.is_empty() {
                if next_has_more_batch {
                    if let Some(range) = next_range {
                        debug!("CleanupLocks::execute, next range:{:?}", range);
                        inner.next_batch(range);
                    }
                }
                has_more_batch = next_has_more_batch;
                continue;
            }
            debug!("CleanupLocks::execute, meet locks:{}", locks.len());

            let lock_size = locks.len();
            match lock_resolver.cleanup_locks(store.clone(), locks).await {
                Ok(()) => {
                    result.resolved_locks += lock_size;
                }
                Err(Error::ResolveLockError(live_locks)) => match backoff.next_delay_duration() {
                    Some(delay_duration) => {
                        crate::stats::observe_backoff_sleep("lock", delay_duration);
                        sleep(delay_duration).await;
                        // Retry scanning the same range; do not advance `inner`.
                        continue;
                    }
                    None => return Err(Error::ResolveLockError(live_locks)),
                },
                Err(Error::ExtractedErrors(mut errors)) => {
                    // Propagate errors to `retry_multi_region` for retry.
                    let Some(first) = errors.pop() else {
                        return Err(crate::internal_err!("ExtractedErrors was empty"));
                    };
                    if let Error::RegionError(e) = first {
                        result.region_error = Some(*e);
                        if !errors.is_empty() {
                            result.key_error = Some(errors);
                        }
                    } else {
                        errors.push(first);
                        result.key_error = Some(errors);
                    }
                    return Ok(result);
                }
                Err(e) => {
                    return Err(e);
                }
            }

            if next_has_more_batch {
                if let Some(range) = next_range {
                    debug!("CleanupLocks::execute, next range:{:?}", range);
                    inner.next_batch(range);
                }
            }
            has_more_batch = next_has_more_batch;
        }

        Ok(result)
    }
}

/// When executed, the plan extracts errors from its inner plan, and returns an
/// `Err` wrapping the error.
///
/// We usually need to apply this plan if (and only if) the output of the inner
/// plan is of a response type.
///
/// The errors come from two places: `Err` from inner plans, and `Ok(response)`
/// where `response` contains unresolved errors (`error` and `region_error`).
pub struct ExtractError<P: Plan> {
    pub inner: P,
}

impl<P: Plan> Clone for ExtractError<P> {
    fn clone(&self) -> Self {
        ExtractError {
            inner: self.inner.clone(),
        }
    }
}

#[async_trait]
impl<P: Plan> Plan for ExtractError<P>
where
    P::Result: HasKeyErrors + HasRegionErrors,
{
    type Result = P::Result;

    async fn execute(&self) -> Result<Self::Result> {
        let mut result = self.inner.execute().await?;
        if let Some(errors) = result.key_errors() {
            Err(Error::ExtractedErrors(errors))
        } else if let Some(errors) = result.region_errors() {
            Err(Error::ExtractedErrors(
                errors
                    .into_iter()
                    .map(|e| Error::RegionError(Box::new(e)))
                    .collect(),
            ))
        } else {
            Ok(result)
        }
    }
}

/// When executed, the plan clones the shard and execute its inner plan, then
/// returns `(shard, response)`.
///
/// It's useful when the information of shard are lost in the response but needed
/// for processing.
pub struct PreserveShard<P: Plan + Shardable> {
    pub inner: P,
    pub shard: Option<P::Shard>,
}

impl<P: Plan + Shardable> Clone for PreserveShard<P> {
    fn clone(&self) -> Self {
        PreserveShard {
            inner: self.inner.clone(),
            shard: None,
        }
    }
}

#[async_trait]
impl<P> Plan for PreserveShard<P>
where
    P: Plan + Shardable,
{
    type Result = ResponseWithShard<P::Result, P::Shard>;

    async fn execute(&self) -> Result<Self::Result> {
        let res = self.inner.execute().await?;
        let shard = self.shard.as_ref().cloned().ok_or_else(|| {
            crate::internal_err!("PreserveShard executed without an attached shard")
        })?;
        Ok(ResponseWithShard(res, shard))
    }
}

// contains a response and the corresponding shards
#[derive(Debug, Clone)]
pub struct ResponseWithShard<Resp, Shard>(pub Resp, pub Shard);

impl<Resp: HasKeyErrors, Shard> HasKeyErrors for ResponseWithShard<Resp, Shard> {
    fn key_errors(&mut self) -> Option<Vec<Error>> {
        self.0.key_errors()
    }
}

impl<Resp: HasLocks, Shard> HasLocks for ResponseWithShard<Resp, Shard> {
    fn take_locks(&mut self) -> Vec<kvrpcpb::LockInfo> {
        self.0.take_locks()
    }
}

impl<Resp: HasRegionError, Shard> HasRegionError for ResponseWithShard<Resp, Shard> {
    fn region_error(&mut self) -> Option<errorpb::Error> {
        self.0.region_error()
    }
}

#[cfg(test)]
mod test {
    use std::any::Any;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::sync::Mutex;
    use std::time::Duration;

    use futures::stream::BoxStream;
    use futures::stream::{self};
    use tokio::sync::Notify;
    use tonic::Status;

    use super::*;
    use crate::mock::MockKvClient;
    use crate::mock::MockPdClient;
    use crate::proto::keyspacepb;
    use crate::proto::kvrpcpb::BatchGetResponse;
    use crate::region::RegionId;
    use crate::store::Store;
    use crate::store::StoreHealthMap;
    use crate::Key;
    use crate::Timestamp;

    #[derive(Default)]
    struct CountingPdClient {
        invalidated_regions: AtomicUsize,
        invalidated_stores: AtomicUsize,
        update_leader_calls: AtomicUsize,
        fail_update_leader: bool,
    }

    struct LeaderSwitchPdClient {
        client: MockKvClient,
        region: Mutex<RegionWithLeader>,
        invalidated_regions: AtomicUsize,
        invalidated_stores: AtomicUsize,
        update_leader_calls: AtomicUsize,
    }

    struct StoreNotMatchLeaderSwitchPdClient {
        client: MockKvClient,
        region: Mutex<RegionWithLeader>,
        invalidated_regions: AtomicUsize,
        invalidated_stores: AtomicUsize,
    }

    impl LeaderSwitchPdClient {
        fn new(client: MockKvClient) -> Self {
            let leader = metapb::Peer {
                id: 101,
                store_id: 1,
                role: metapb::PeerRole::Voter as i32,
                is_witness: false,
            };
            let follower_1 = metapb::Peer {
                id: 102,
                store_id: 2,
                role: metapb::PeerRole::Voter as i32,
                is_witness: false,
            };
            let follower_2 = metapb::Peer {
                id: 103,
                store_id: 3,
                role: metapb::PeerRole::Voter as i32,
                is_witness: false,
            };

            let mut region = metapb::Region::default();
            region.id = 1;
            region.start_key = vec![];
            region.end_key = vec![];
            region.region_epoch = Some(metapb::RegionEpoch {
                conf_ver: 0,
                version: 0,
            });
            region.peers = vec![leader.clone(), follower_1, follower_2];

            Self {
                client,
                region: Mutex::new(RegionWithLeader {
                    region,
                    leader: Some(leader),
                }),
                invalidated_regions: AtomicUsize::new(0),
                invalidated_stores: AtomicUsize::new(0),
                update_leader_calls: AtomicUsize::new(0),
            }
        }
    }

    impl StoreNotMatchLeaderSwitchPdClient {
        fn new(client: MockKvClient) -> Self {
            // Reuse the same region layout as `LeaderSwitchPdClient`: 3 peers, leader on store 1.
            let leader = metapb::Peer {
                id: 101,
                store_id: 1,
                role: metapb::PeerRole::Voter as i32,
                is_witness: false,
            };
            let follower_1 = metapb::Peer {
                id: 102,
                store_id: 2,
                role: metapb::PeerRole::Voter as i32,
                is_witness: false,
            };
            let follower_2 = metapb::Peer {
                id: 103,
                store_id: 3,
                role: metapb::PeerRole::Voter as i32,
                is_witness: false,
            };

            let mut region = metapb::Region::default();
            region.id = 1;
            region.start_key = vec![];
            region.end_key = vec![];
            region.region_epoch = Some(metapb::RegionEpoch {
                conf_ver: 0,
                version: 0,
            });
            region.peers = vec![leader.clone(), follower_1, follower_2];

            Self {
                client,
                region: Mutex::new(RegionWithLeader {
                    region,
                    leader: Some(leader),
                }),
                invalidated_regions: AtomicUsize::new(0),
                invalidated_stores: AtomicUsize::new(0),
            }
        }
    }

    #[async_trait]
    impl PdClient for CountingPdClient {
        type KvClient = MockKvClient;

        async fn map_region_to_store(
            self: Arc<Self>,
            _region: RegionWithLeader,
        ) -> Result<RegionStore> {
            Err(Error::Unimplemented)
        }

        async fn region_for_key(&self, _key: &Key) -> Result<RegionWithLeader> {
            Err(Error::Unimplemented)
        }

        async fn region_for_id(&self, _id: RegionId) -> Result<RegionWithLeader> {
            Err(Error::Unimplemented)
        }

        async fn get_timestamp(self: Arc<Self>) -> Result<Timestamp> {
            Err(Error::Unimplemented)
        }

        async fn get_min_ts(self: Arc<Self>) -> Result<Timestamp> {
            Err(Error::Unimplemented)
        }

        async fn update_safepoint(self: Arc<Self>, _safepoint: u64) -> Result<bool> {
            Err(Error::Unimplemented)
        }

        async fn load_keyspace(&self, _keyspace: &str) -> Result<keyspacepb::KeyspaceMeta> {
            Err(Error::Unimplemented)
        }

        async fn all_stores(&self) -> Result<Vec<Store>> {
            Err(Error::Unimplemented)
        }

        async fn update_leader(&self, _ver_id: RegionVerId, _leader: metapb::Peer) -> Result<()> {
            self.update_leader_calls.fetch_add(1, Ordering::SeqCst);
            if self.fail_update_leader {
                Err(Error::Unimplemented)
            } else {
                Ok(())
            }
        }

        async fn invalidate_region_cache(&self, _ver_id: RegionVerId) {
            self.invalidated_regions.fetch_add(1, Ordering::SeqCst);
        }

        async fn invalidate_store_cache(&self, _store_id: StoreId) {
            self.invalidated_stores.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[async_trait]
    impl PdClient for LeaderSwitchPdClient {
        type KvClient = MockKvClient;

        async fn map_region_to_store(
            self: Arc<Self>,
            region: RegionWithLeader,
        ) -> Result<RegionStore> {
            Ok(RegionStore::new(region, Arc::new(self.client.clone())))
        }

        async fn region_for_key(&self, _key: &Key) -> Result<RegionWithLeader> {
            Ok(self.region.lock().unwrap().clone())
        }

        async fn region_for_id(&self, _id: RegionId) -> Result<RegionWithLeader> {
            Ok(self.region.lock().unwrap().clone())
        }

        async fn get_timestamp(self: Arc<Self>) -> Result<Timestamp> {
            Ok(Timestamp::default())
        }

        async fn get_min_ts(self: Arc<Self>) -> Result<Timestamp> {
            Ok(Timestamp::default())
        }

        async fn update_safepoint(self: Arc<Self>, _safepoint: u64) -> Result<bool> {
            Ok(true)
        }

        async fn load_keyspace(&self, keyspace: &str) -> Result<keyspacepb::KeyspaceMeta> {
            Ok(keyspacepb::KeyspaceMeta {
                id: 0,
                name: keyspace.to_owned(),
                state: keyspacepb::KeyspaceState::Enabled as i32,
                ..Default::default()
            })
        }

        async fn all_stores(&self) -> Result<Vec<Store>> {
            Ok(vec![Store::new(Arc::new(self.client.clone()))])
        }

        async fn update_leader(&self, _ver_id: RegionVerId, leader: metapb::Peer) -> Result<()> {
            self.update_leader_calls.fetch_add(1, Ordering::SeqCst);
            self.region.lock().unwrap().leader = Some(leader);
            Ok(())
        }

        async fn invalidate_region_cache(&self, _ver_id: RegionVerId) {
            self.invalidated_regions.fetch_add(1, Ordering::SeqCst);
        }

        async fn invalidate_store_cache(&self, _store_id: StoreId) {
            self.invalidated_stores.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[async_trait]
    impl PdClient for StoreNotMatchLeaderSwitchPdClient {
        type KvClient = MockKvClient;

        async fn map_region_to_store(
            self: Arc<Self>,
            region: RegionWithLeader,
        ) -> Result<RegionStore> {
            Ok(RegionStore::new(region, Arc::new(self.client.clone())))
        }

        async fn region_for_key(&self, _key: &Key) -> Result<RegionWithLeader> {
            Ok(self.region.lock().unwrap().clone())
        }

        async fn region_for_id(&self, _id: RegionId) -> Result<RegionWithLeader> {
            Ok(self.region.lock().unwrap().clone())
        }

        async fn get_timestamp(self: Arc<Self>) -> Result<Timestamp> {
            Ok(Timestamp::default())
        }

        async fn get_min_ts(self: Arc<Self>) -> Result<Timestamp> {
            Ok(Timestamp::default())
        }

        async fn update_safepoint(self: Arc<Self>, _safepoint: u64) -> Result<bool> {
            Ok(true)
        }

        async fn load_keyspace(&self, keyspace: &str) -> Result<keyspacepb::KeyspaceMeta> {
            Ok(keyspacepb::KeyspaceMeta {
                id: 0,
                name: keyspace.to_owned(),
                state: keyspacepb::KeyspaceState::Enabled as i32,
                ..Default::default()
            })
        }

        async fn all_stores(&self) -> Result<Vec<Store>> {
            Ok(vec![Store::new(Arc::new(self.client.clone()))])
        }

        async fn update_leader(&self, _ver_id: RegionVerId, _leader: metapb::Peer) -> Result<()> {
            Ok(())
        }

        async fn invalidate_region_cache(&self, _ver_id: RegionVerId) {
            self.invalidated_regions.fetch_add(1, Ordering::SeqCst);
            // Simulate PD refreshing the region and electing store 3 as leader (e.g. after store
            // replacement / address reuse leading to StoreNotMatch).
            let mut region = self.region.lock().unwrap();
            let new_leader = region
                .region
                .peers
                .iter()
                .find(|peer| peer.store_id == 3)
                .cloned()
                .expect("peer on store 3 must exist");
            region.leader = Some(new_leader);
        }

        async fn invalidate_store_cache(&self, _store_id: StoreId) {
            self.invalidated_stores.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[derive(Debug, Default)]
    struct MockStoreResponse {
        key_errors: Option<Vec<Error>>,
        region_error: Option<errorpb::Error>,
    }

    impl HasKeyErrors for MockStoreResponse {
        fn key_errors(&mut self) -> Option<Vec<Error>> {
            self.key_errors.take()
        }
    }

    impl HasRegionError for MockStoreResponse {
        fn region_error(&mut self) -> Option<errorpb::Error> {
            self.region_error.take()
        }
    }

    #[derive(Clone)]
    struct StoreKeyErrorsPlan;

    #[async_trait]
    impl Plan for StoreKeyErrorsPlan {
        type Result = MockStoreResponse;

        async fn execute(&self) -> Result<Self::Result> {
            Ok(MockStoreResponse {
                key_errors: Some(vec![Error::Unimplemented]),
                region_error: None,
            })
        }
    }

    #[derive(Clone)]
    struct StoreRegionErrorPlan;

    #[async_trait]
    impl Plan for StoreRegionErrorPlan {
        type Result = MockStoreResponse;

        async fn execute(&self) -> Result<Self::Result> {
            Ok(MockStoreResponse {
                key_errors: None,
                region_error: Some(errorpb::Error::default()),
            })
        }
    }

    #[derive(Clone)]
    struct StoreOkPlan;

    #[async_trait]
    impl Plan for StoreOkPlan {
        type Result = MockStoreResponse;

        async fn execute(&self) -> Result<Self::Result> {
            Ok(MockStoreResponse::default())
        }
    }

    #[derive(Clone)]
    struct BlockingPlan {
        calls: Arc<AtomicUsize>,
        in_flight: Arc<AtomicUsize>,
        max_in_flight: Arc<AtomicUsize>,
        release: Arc<Notify>,
    }

    #[async_trait]
    impl Plan for BlockingPlan {
        type Result = MockStoreResponse;

        async fn execute(&self) -> Result<Self::Result> {
            let current = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            self.calls.fetch_add(1, Ordering::SeqCst);
            loop {
                let prev = self.max_in_flight.load(Ordering::SeqCst);
                if current <= prev {
                    break;
                }
                if self
                    .max_in_flight
                    .compare_exchange(prev, current, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
                {
                    break;
                }
            }

            self.release.notified().await;
            self.in_flight.fetch_sub(1, Ordering::SeqCst);
            Ok(MockStoreResponse::default())
        }
    }

    #[derive(Clone)]
    struct NotifyPlan {
        started: Arc<AtomicUsize>,
        finished: Arc<AtomicUsize>,
        gate: Arc<Notify>,
    }

    #[async_trait]
    impl Plan for NotifyPlan {
        type Result = MockStoreResponse;

        async fn execute(&self) -> Result<Self::Result> {
            self.started.fetch_add(1, Ordering::SeqCst);
            self.gate.notified().await;
            self.finished.fetch_add(1, Ordering::SeqCst);
            Ok(MockStoreResponse::default())
        }
    }

    impl StoreRequest for NotifyPlan {
        fn apply_store(&mut self, _store: &Store) {}
    }

    #[derive(Clone)]
    struct FlakyGrpcPlan {
        calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl Plan for FlakyGrpcPlan {
        type Result = MockStoreResponse;

        async fn execute(&self) -> Result<Self::Result> {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            if call == 0 {
                Err(Error::GrpcAPI(Status::unavailable("grpc error")))
            } else {
                Ok(MockStoreResponse::default())
            }
        }
    }

    #[derive(Clone)]
    struct AlwaysGrpcPlan;

    #[async_trait]
    impl Plan for AlwaysGrpcPlan {
        type Result = MockStoreResponse;

        async fn execute(&self) -> Result<Self::Result> {
            Err(Error::GrpcAPI(Status::unavailable("grpc error")))
        }
    }

    #[derive(Clone)]
    struct NonGrpcErrorPlan;

    #[async_trait]
    impl Plan for NonGrpcErrorPlan {
        type Result = MockStoreResponse;

        async fn execute(&self) -> Result<Self::Result> {
            Err(Error::Unimplemented)
        }
    }

    #[derive(Clone)]
    struct ErrPlan;

    #[async_trait]
    impl Plan for ErrPlan {
        type Result = BatchGetResponse;

        async fn execute(&self) -> Result<Self::Result> {
            Err(Error::Unimplemented)
        }
    }

    impl Shardable for ErrPlan {
        type Shard = ();

        fn shards(
            &self,
            _: &Arc<impl crate::pd::PdClient>,
        ) -> BoxStream<'static, crate::Result<(Self::Shard, RegionWithLeader)>> {
            Box::pin(stream::iter(1..=3).map(|_| Err(Error::Unimplemented))).boxed()
        }

        fn apply_shard(&mut self, _: Self::Shard) {}

        fn apply_store(&mut self, _: &crate::store::RegionStore) -> Result<()> {
            Ok(())
        }
    }

    #[derive(Clone, Default)]
    struct DelayedRawGetPlan {
        shard_id: Option<u8>,
    }

    #[async_trait]
    impl Plan for DelayedRawGetPlan {
        type Result = crate::proto::kvrpcpb::RawGetResponse;

        async fn execute(&self) -> Result<Self::Result> {
            let shard = self
                .shard_id
                .ok_or_else(|| crate::internal_err!("DelayedRawGetPlan missing shard_id"))?;

            // Delays are chosen so shards complete out-of-order when executed concurrently.
            let delay_ms = match shard {
                0 => 60,
                1 => 10,
                2 => 30,
                _ => 0,
            };
            tokio::time::sleep(Duration::from_millis(delay_ms)).await;

            let mut resp = crate::proto::kvrpcpb::RawGetResponse::default();
            resp.value = vec![shard];
            Ok(resp)
        }
    }

    impl Shardable for DelayedRawGetPlan {
        type Shard = u8;

        fn shards(
            &self,
            _: &Arc<impl crate::pd::PdClient>,
        ) -> BoxStream<'static, crate::Result<(Self::Shard, RegionWithLeader)>> {
            stream::iter(vec![
                Ok((0, MockPdClient::region1())),
                Ok((1, MockPdClient::region2())),
                Ok((2, MockPdClient::region3())),
            ])
            .boxed()
        }

        fn apply_shard(&mut self, shard: Self::Shard) {
            self.shard_id = Some(shard);
        }

        fn apply_store(&mut self, _: &crate::store::RegionStore) -> Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_err() {
        let plan = RetryableMultiRegion {
            inner: ResolveLock {
                inner: ErrPlan,
                backoff: Backoff::no_backoff(),
                pd_client: Arc::new(MockPdClient::default()),
                keyspace: Keyspace::Disable,
                request_context: RequestContext::default(),
                read_routing: ReadRouting::default(),
            },
            pd_client: Arc::new(MockPdClient::default()),
            backoff: Backoff::no_backoff(),
            concurrency: MULTI_REGION_CONCURRENCY,
            preserve_region_results: false,
            request_context: RequestContext::default(),
            read_routing: ReadRouting::default(),
        };
        assert!(plan.execute().await.is_err())
    }

    #[tokio::test]
    async fn test_retryable_multi_region_preserves_shard_order_for_successes() -> Result<()> {
        let plan = RetryableMultiRegion {
            inner: DelayedRawGetPlan::default(),
            pd_client: Arc::new(MockPdClient::default()),
            backoff: Backoff::no_backoff(),
            concurrency: 3,
            preserve_region_results: false,
            request_context: RequestContext::default(),
            read_routing: ReadRouting::default(),
        };

        let results = plan.execute().await?;
        let got: Vec<u8> = results
            .into_iter()
            .map(|r| r.expect("unexpected shard error").value[0])
            .collect();
        assert_eq!(got, vec![0, 1, 2]);
        Ok(())
    }

    #[tokio::test]
    async fn test_err_plan_execute_and_apply_store_are_reachable() -> Result<()> {
        let mut plan = ErrPlan;
        plan.apply_shard(());
        let store = RegionStore::new(MockPdClient::region1(), Arc::new(MockKvClient::default()));
        plan.apply_store(&store)?;
        assert!(plan.execute().await.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn test_retryable_multi_region_retries_grpc_error_with_backoff() -> Result<()> {
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_for_hook = calls.clone();
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let Some(_) = req.downcast_ref::<kvrpcpb::RawGetRequest>() else {
                    unreachable!("unexpected request type");
                };
                let call = calls_for_hook.fetch_add(1, Ordering::SeqCst);
                if call == 0 {
                    return Err(Error::GrpcAPI(Status::unavailable("grpc error")));
                }
                Ok(Box::new(kvrpcpb::RawGetResponse {
                    not_found: true,
                    ..Default::default()
                }) as Box<dyn Any + Send>)
            },
        )));

        let mut request = kvrpcpb::RawGetRequest::default();
        request.key = vec![5];
        let plan = RetryableMultiRegion {
            inner: Dispatch {
                request,
                kv_client: None,
            },
            pd_client: pd_client.clone(),
            backoff: Backoff::no_jitter_backoff(0, 0, 1),
            concurrency: 1,
            preserve_region_results: false,
            request_context: RequestContext::default(),
            read_routing: ReadRouting::default(),
        };

        let _ = plan.clone();
        let results = plan.execute().await?;
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(results.len(), 1);
        let response = results.into_iter().next().unwrap()?;
        assert!(response.not_found);
        Ok(())
    }

    #[tokio::test]
    async fn test_retryable_multi_region_not_leader_with_leader_does_not_consume_backoff(
    ) -> Result<()> {
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_for_hook = calls.clone();

        let seen = Arc::new(Mutex::new(Vec::new()));
        let seen_for_hook = seen.clone();

        let pd_client = Arc::new(LeaderSwitchPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let Some(req) = req.downcast_ref::<kvrpcpb::RawGetRequest>() else {
                    unreachable!("unexpected request type");
                };
                let ctx = req
                    .context
                    .as_ref()
                    .expect("missing context on RawGetRequest");
                let peer = ctx.peer.as_ref().expect("missing peer in context");
                seen_for_hook.lock().unwrap().push(peer.store_id);

                let call = calls_for_hook.fetch_add(1, Ordering::SeqCst);
                match call {
                    0 => Ok(Box::new(kvrpcpb::RawGetResponse {
                        region_error: Some(errorpb::Error {
                            not_leader: Some(errorpb::NotLeader {
                                leader: Some(metapb::Peer {
                                    id: 102,
                                    store_id: 2,
                                    role: metapb::PeerRole::Voter as i32,
                                    is_witness: false,
                                }),
                                ..Default::default()
                            }),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }) as Box<dyn Any + Send>),
                    1 => Ok(Box::new(kvrpcpb::RawGetResponse {
                        region_error: Some(errorpb::Error {
                            not_leader: Some(errorpb::NotLeader {
                                leader: Some(metapb::Peer {
                                    id: 103,
                                    store_id: 3,
                                    role: metapb::PeerRole::Voter as i32,
                                    is_witness: false,
                                }),
                                ..Default::default()
                            }),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }) as Box<dyn Any + Send>),
                    _ => Ok(Box::new(kvrpcpb::RawGetResponse {
                        not_found: true,
                        ..Default::default()
                    }) as Box<dyn Any + Send>),
                }
            },
        )));

        let mut request = kvrpcpb::RawGetRequest::default();
        request.key = vec![5];
        let plan = RetryableMultiRegion {
            inner: Dispatch {
                request,
                kv_client: None,
            },
            pd_client: pd_client.clone(),
            // Only one backoff attempt. If we consume backoff for NotLeader-with-leader, the
            // second NotLeader would run out of attempts and fail.
            backoff: Backoff::no_jitter_backoff(50, 50, 1),
            concurrency: 1,
            preserve_region_results: false,
            request_context: RequestContext::default(),
            read_routing: ReadRouting::default(),
        };

        let results = plan.execute().await?;
        assert_eq!(calls.load(Ordering::SeqCst), 3);
        assert_eq!(results.len(), 1);

        let response = results.into_iter().next().unwrap()?;
        assert!(response.not_found);

        assert_eq!(pd_client.update_leader_calls.load(Ordering::SeqCst), 2);
        assert_eq!(pd_client.invalidated_regions.load(Ordering::SeqCst), 0);

        let seen = seen.lock().unwrap();
        assert_eq!(seen.as_slice(), &[1, 2, 3]);
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn test_retryable_multi_region_not_leader_without_leader_backoffs() -> Result<()> {
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_for_hook = calls.clone();

        let pd_client = Arc::new(LeaderSwitchPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let Some(req) = req.downcast_ref::<kvrpcpb::RawGetRequest>() else {
                    unreachable!("unexpected request type");
                };
                let ctx = req
                    .context
                    .as_ref()
                    .expect("missing context on RawGetRequest");
                let peer = ctx.peer.as_ref().expect("missing peer in context");
                assert_eq!(peer.store_id, 1);

                let call = calls_for_hook.fetch_add(1, Ordering::SeqCst);
                match call {
                    0 => Ok(Box::new(kvrpcpb::RawGetResponse {
                        region_error: Some(errorpb::Error {
                            not_leader: Some(errorpb::NotLeader::default()),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }) as Box<dyn Any + Send>),
                    _ => Ok(Box::new(kvrpcpb::RawGetResponse {
                        not_found: true,
                        ..Default::default()
                    }) as Box<dyn Any + Send>),
                }
            },
        )));

        let mut request = kvrpcpb::RawGetRequest::default();
        request.key = vec![5];
        let plan = RetryableMultiRegion {
            inner: Dispatch {
                request,
                kv_client: None,
            },
            pd_client: pd_client.clone(),
            backoff: Backoff::no_jitter_backoff(50, 50, 1),
            concurrency: 1,
            preserve_region_results: false,
            request_context: RequestContext::default(),
            read_routing: ReadRouting::default(),
        };

        let handle = tokio::spawn(async move { plan.execute().await });

        while calls.load(Ordering::SeqCst) < 1 {
            tokio::task::yield_now().await;
        }
        tokio::task::yield_now().await;
        assert!(
            calls.load(Ordering::SeqCst) >= 1,
            "expected at least one dispatch attempt"
        );

        tokio::time::advance(Duration::from_millis(49)).await;
        tokio::task::yield_now().await;
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        tokio::time::advance(Duration::from_millis(1)).await;
        tokio::task::yield_now().await;

        let results = handle.await.expect("task panicked")?;
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(results.len(), 1);
        let response = results.into_iter().next().unwrap()?;
        assert!(response.not_found);

        assert_eq!(pd_client.update_leader_calls.load(Ordering::SeqCst), 0);
        assert_eq!(pd_client.invalidated_regions.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_retryable_multi_region_region_not_found_retries_without_backoff() -> Result<()> {
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_for_hook = calls.clone();

        let pd_client = Arc::new(LeaderSwitchPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let Some(_) = req.downcast_ref::<kvrpcpb::RawGetRequest>() else {
                    unreachable!("unexpected request type");
                };
                let call = calls_for_hook.fetch_add(1, Ordering::SeqCst);
                match call {
                    0 => Ok(Box::new(kvrpcpb::RawGetResponse {
                        region_error: Some(errorpb::Error {
                            region_not_found: Some(errorpb::RegionNotFound::default()),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }) as Box<dyn Any + Send>),
                    _ => Ok(Box::new(kvrpcpb::RawGetResponse {
                        not_found: true,
                        ..Default::default()
                    }) as Box<dyn Any + Send>),
                }
            },
        )));

        let mut request = kvrpcpb::RawGetRequest::default();
        request.key = vec![5];
        let plan = RetryableMultiRegion {
            inner: Dispatch {
                request,
                kv_client: None,
            },
            pd_client: pd_client.clone(),
            // No backoff attempts available: region_not_found must be handled without backoff.
            backoff: Backoff::no_jitter_backoff(50, 50, 0),
            concurrency: 1,
            preserve_region_results: false,
            request_context: RequestContext::default(),
            read_routing: ReadRouting::default(),
        };

        let results = plan.execute().await?;
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(results.len(), 1);
        let resp = results.into_iter().next().unwrap()?;
        assert!(resp.not_found);

        assert_eq!(pd_client.invalidated_regions.load(Ordering::SeqCst), 1);
        assert_eq!(pd_client.invalidated_stores.load(Ordering::SeqCst), 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_retryable_multi_region_store_not_match_retries_without_backoff() -> Result<()> {
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_for_hook = calls.clone();

        let pd_client = Arc::new(LeaderSwitchPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let Some(_) = req.downcast_ref::<kvrpcpb::RawGetRequest>() else {
                    unreachable!("unexpected request type");
                };
                let call = calls_for_hook.fetch_add(1, Ordering::SeqCst);
                match call {
                    0 => Ok(Box::new(kvrpcpb::RawGetResponse {
                        region_error: Some(errorpb::Error {
                            store_not_match: Some(errorpb::StoreNotMatch::default()),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }) as Box<dyn Any + Send>),
                    _ => Ok(Box::new(kvrpcpb::RawGetResponse {
                        not_found: true,
                        ..Default::default()
                    }) as Box<dyn Any + Send>),
                }
            },
        )));

        let mut request = kvrpcpb::RawGetRequest::default();
        request.key = vec![5];
        let plan = RetryableMultiRegion {
            inner: Dispatch {
                request,
                kv_client: None,
            },
            pd_client: pd_client.clone(),
            // No backoff attempts available: store_not_match must be handled without backoff.
            backoff: Backoff::no_jitter_backoff(50, 50, 0),
            concurrency: 1,
            preserve_region_results: false,
            request_context: RequestContext::default(),
            read_routing: ReadRouting::default(),
        };

        let results = plan.execute().await?;
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(results.len(), 1);
        let resp = results.into_iter().next().unwrap()?;
        assert!(resp.not_found);

        assert_eq!(pd_client.invalidated_regions.load(Ordering::SeqCst), 1);
        assert_eq!(pd_client.invalidated_stores.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_retryable_multi_region_store_not_match_switches_leader_after_invalidate(
    ) -> Result<()> {
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_for_hook = calls.clone();

        let store_ids = Arc::new(Mutex::new(Vec::<u64>::new()));
        let store_ids_for_hook = store_ids.clone();

        let pd_client = Arc::new(StoreNotMatchLeaderSwitchPdClient::new(
            MockKvClient::with_dispatch_hook(move |req: &dyn Any| {
                let Some(req) = req.downcast_ref::<kvrpcpb::RawGetRequest>() else {
                    unreachable!("unexpected request type");
                };
                let store_id = req
                    .context
                    .as_ref()
                    .and_then(|ctx| ctx.peer.as_ref())
                    .map(|peer| peer.store_id)
                    .expect("store_id should be set");
                store_ids_for_hook.lock().unwrap().push(store_id);

                let call = calls_for_hook.fetch_add(1, Ordering::SeqCst);
                match call {
                    0 => {
                        assert_eq!(store_id, 1);
                        Ok(Box::new(kvrpcpb::RawGetResponse {
                            region_error: Some(errorpb::Error {
                                store_not_match: Some(errorpb::StoreNotMatch {
                                    request_store_id: store_id,
                                    actual_store_id: 3,
                                }),
                                ..Default::default()
                            }),
                            ..Default::default()
                        }) as Box<dyn Any + Send>)
                    }
                    1 => {
                        assert_eq!(store_id, 3);
                        Ok(Box::new(kvrpcpb::RawGetResponse {
                            not_found: true,
                            ..Default::default()
                        }) as Box<dyn Any + Send>)
                    }
                    _ => unreachable!("unexpected extra dispatch attempts"),
                }
            }),
        ));

        let mut request = kvrpcpb::RawGetRequest::default();
        request.key = vec![5];
        let plan = RetryableMultiRegion {
            inner: Dispatch {
                request,
                kv_client: None,
            },
            pd_client: pd_client.clone(),
            // No backoff budget: store_not_match must trigger an immediate retry with refreshed
            // region/store caches (e.g. after store replacement).
            backoff: Backoff::no_jitter_backoff(50, 50, 0),
            concurrency: 1,
            preserve_region_results: false,
            request_context: RequestContext::default(),
            read_routing: ReadRouting::default(),
        };

        let results = plan.execute().await?;
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(results.len(), 1);
        let resp = results.into_iter().next().unwrap()?;
        assert!(resp.not_found);

        assert_eq!(&*store_ids.lock().unwrap(), &[1, 3]);
        assert_eq!(pd_client.invalidated_regions.load(Ordering::SeqCst), 1);
        assert_eq!(pd_client.invalidated_stores.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_retryable_multi_region_key_not_in_region_retries_without_backoff() -> Result<()> {
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_for_hook = calls.clone();

        let pd_client = Arc::new(LeaderSwitchPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let Some(_) = req.downcast_ref::<kvrpcpb::RawGetRequest>() else {
                    unreachable!("unexpected request type");
                };
                let call = calls_for_hook.fetch_add(1, Ordering::SeqCst);
                match call {
                    0 => Ok(Box::new(kvrpcpb::RawGetResponse {
                        region_error: Some(errorpb::Error {
                            key_not_in_region: Some(errorpb::KeyNotInRegion {
                                key: vec![1],
                                region_id: 1,
                                start_key: vec![],
                                end_key: vec![10],
                            }),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }) as Box<dyn Any + Send>),
                    _ => Ok(Box::new(kvrpcpb::RawGetResponse {
                        not_found: true,
                        ..Default::default()
                    }) as Box<dyn Any + Send>),
                }
            },
        )));

        let mut request = kvrpcpb::RawGetRequest::default();
        request.key = vec![5];
        let plan = RetryableMultiRegion {
            inner: Dispatch {
                request,
                kv_client: None,
            },
            pd_client: pd_client.clone(),
            // No backoff attempts available: key_not_in_region must be handled without backoff.
            backoff: Backoff::no_jitter_backoff(50, 50, 0),
            concurrency: 1,
            preserve_region_results: false,
            request_context: RequestContext::default(),
            read_routing: ReadRouting::default(),
        };

        let results = plan.execute().await?;
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(results.len(), 1);
        let resp = results.into_iter().next().unwrap()?;
        assert!(resp.not_found);

        assert_eq!(pd_client.invalidated_regions.load(Ordering::SeqCst), 1);
        assert_eq!(pd_client.invalidated_stores.load(Ordering::SeqCst), 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_retryable_multi_region_unknown_region_error_requires_backoff_budget() -> Result<()>
    {
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_for_hook = calls.clone();

        let pd_client = Arc::new(LeaderSwitchPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let Some(_) = req.downcast_ref::<kvrpcpb::RawGetRequest>() else {
                    unreachable!("unexpected request type");
                };
                let call = calls_for_hook.fetch_add(1, Ordering::SeqCst);
                match call {
                    0 => Ok(Box::new(kvrpcpb::RawGetResponse {
                        region_error: Some(errorpb::Error {
                            message: "unknown".to_owned(),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }) as Box<dyn Any + Send>),
                    _ => Ok(Box::new(kvrpcpb::RawGetResponse {
                        not_found: true,
                        ..Default::default()
                    }) as Box<dyn Any + Send>),
                }
            },
        )));

        let mut request = kvrpcpb::RawGetRequest::default();
        request.key = vec![5];
        let plan = RetryableMultiRegion {
            inner: Dispatch {
                request,
                kv_client: None,
            },
            pd_client: pd_client.clone(),
            // No backoff attempts available: unknown region error can't be retried and must be
            // returned to the caller.
            backoff: Backoff::no_jitter_backoff(50, 50, 0),
            concurrency: 1,
            preserve_region_results: false,
            request_context: RequestContext::default(),
            read_routing: ReadRouting::default(),
        };

        let err = plan
            .execute()
            .await
            .expect_err("unknown region error should require a backoff budget to retry");
        assert!(matches!(err, Error::RegionError(_)));
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        assert_eq!(pd_client.invalidated_regions.load(Ordering::SeqCst), 1);
        assert_eq!(pd_client.invalidated_stores.load(Ordering::SeqCst), 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_retryable_multi_region_unknown_region_error_retries_with_backoff_budget(
    ) -> Result<()> {
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_for_hook = calls.clone();

        let pd_client = Arc::new(LeaderSwitchPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let Some(_) = req.downcast_ref::<kvrpcpb::RawGetRequest>() else {
                    unreachable!("unexpected request type");
                };
                let call = calls_for_hook.fetch_add(1, Ordering::SeqCst);
                match call {
                    0 => Ok(Box::new(kvrpcpb::RawGetResponse {
                        region_error: Some(errorpb::Error {
                            message: "unknown".to_owned(),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }) as Box<dyn Any + Send>),
                    _ => Ok(Box::new(kvrpcpb::RawGetResponse {
                        not_found: true,
                        ..Default::default()
                    }) as Box<dyn Any + Send>),
                }
            },
        )));

        let mut request = kvrpcpb::RawGetRequest::default();
        request.key = vec![5];
        let plan = RetryableMultiRegion {
            inner: Dispatch {
                request,
                kv_client: None,
            },
            pd_client: pd_client.clone(),
            // One retry is enough; use a 0ms delay to keep the test deterministic.
            backoff: Backoff::no_jitter_backoff(0, 0, 1),
            concurrency: 1,
            preserve_region_results: false,
            request_context: RequestContext::default(),
            read_routing: ReadRouting::default(),
        };

        let results = plan.execute().await?;
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(results.len(), 1);
        let resp = results.into_iter().next().unwrap()?;
        assert!(resp.not_found);

        assert_eq!(pd_client.invalidated_regions.load(Ordering::SeqCst), 1);
        assert_eq!(pd_client.invalidated_stores.load(Ordering::SeqCst), 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_retryable_multi_region_grpc_api_error_requires_backoff_budget() -> Result<()> {
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_for_hook = calls.clone();

        let pd_client = Arc::new(LeaderSwitchPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let Some(_) = req.downcast_ref::<kvrpcpb::RawGetRequest>() else {
                    unreachable!("unexpected request type");
                };
                let call = calls_for_hook.fetch_add(1, Ordering::SeqCst);
                match call {
                    0 => Err(Status::unavailable("injected grpc error").into()),
                    _ => Ok(Box::new(kvrpcpb::RawGetResponse {
                        not_found: true,
                        ..Default::default()
                    }) as Box<dyn Any + Send>),
                }
            },
        )));

        let mut request = kvrpcpb::RawGetRequest::default();
        request.key = vec![5];
        let plan = RetryableMultiRegion {
            inner: Dispatch {
                request,
                kv_client: None,
            },
            pd_client: pd_client.clone(),
            backoff: Backoff::no_jitter_backoff(50, 50, 0),
            concurrency: 1,
            preserve_region_results: false,
            request_context: RequestContext::default(),
            read_routing: ReadRouting::default(),
        };

        let err = plan
            .execute()
            .await
            .expect_err("grpc errors should require a backoff budget to retry");
        assert!(matches!(err, Error::GrpcAPI(_)));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(pd_client.invalidated_regions.load(Ordering::SeqCst), 1);
        assert_eq!(pd_client.invalidated_stores.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_retryable_multi_region_grpc_api_error_retries_with_backoff_budget() -> Result<()>
    {
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_for_hook = calls.clone();

        let pd_client = Arc::new(LeaderSwitchPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let Some(_) = req.downcast_ref::<kvrpcpb::RawGetRequest>() else {
                    unreachable!("unexpected request type");
                };
                let call = calls_for_hook.fetch_add(1, Ordering::SeqCst);
                match call {
                    0 => Err(Status::deadline_exceeded("injected timeout").into()),
                    _ => Ok(Box::new(kvrpcpb::RawGetResponse {
                        not_found: true,
                        ..Default::default()
                    }) as Box<dyn Any + Send>),
                }
            },
        )));

        let mut request = kvrpcpb::RawGetRequest::default();
        request.key = vec![5];
        let store_health = StoreHealthMap::default();
        let slow_check_epoch_ms = StoreHealthMap::now_epoch_ms();
        let plan = RetryableMultiRegion {
            inner: Dispatch {
                request,
                kv_client: None,
            },
            pd_client: pd_client.clone(),
            backoff: Backoff::no_jitter_backoff(0, 0, 1),
            concurrency: 1,
            preserve_region_results: false,
            request_context: RequestContext::default(),
            read_routing: ReadRouting::default().with_store_health(store_health.clone()),
        };

        let results = plan.execute().await?;
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(results.len(), 1);
        let resp = results.into_iter().next().unwrap()?;
        assert!(resp.not_found);

        assert_eq!(pd_client.invalidated_regions.load(Ordering::SeqCst), 1);
        assert_eq!(pd_client.invalidated_stores.load(Ordering::SeqCst), 1);
        assert!(
            store_health.is_slow(1, slow_check_epoch_ms),
            "grpc send-failure should mark the target store as slow"
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_retryable_multi_region_resource_group_throttled_does_not_invalidate_caches(
    ) -> Result<()> {
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_for_hook = calls.clone();

        let store_health = StoreHealthMap::default();
        let slow_check_epoch_ms = StoreHealthMap::now_epoch_ms();

        let pd_client = Arc::new(LeaderSwitchPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let Some(_) = req.downcast_ref::<kvrpcpb::RawGetRequest>() else {
                    unreachable!("unexpected request type");
                };
                calls_for_hook.fetch_add(1, Ordering::SeqCst);
                Err(Error::ResourceGroupThrottled)
            },
        )));

        let mut request = kvrpcpb::RawGetRequest::default();
        request.key = vec![5];
        let plan = RetryableMultiRegion {
            inner: Dispatch {
                request,
                kv_client: None,
            },
            pd_client: pd_client.clone(),
            backoff: Backoff::no_jitter_backoff(0, 0, 1),
            concurrency: 1,
            preserve_region_results: false,
            request_context: RequestContext::default(),
            read_routing: ReadRouting::default().with_store_health(store_health.clone()),
        };

        let err = plan
            .execute()
            .await
            .expect_err("resource group throttled should be returned to caller");
        assert!(matches!(err, Error::ResourceGroupThrottled));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(pd_client.invalidated_regions.load(Ordering::SeqCst), 0);
        assert_eq!(pd_client.invalidated_stores.load(Ordering::SeqCst), 0);
        assert!(
            !store_health.is_slow(1, slow_check_epoch_ms),
            "resource group throttling should not mark stores as slow"
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_retryable_multi_region_replica_read_stale_command_switches_peer_without_backoff(
    ) -> Result<()> {
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_for_hook = calls.clone();
        let store_ids = Arc::new(Mutex::new(Vec::<u64>::new()));
        let store_ids_for_hook = store_ids.clone();

        let pd_client = Arc::new(LeaderSwitchPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let Some(req) = req.downcast_ref::<kvrpcpb::RawGetRequest>() else {
                    unreachable!("unexpected request type");
                };
                let store_id = req
                    .context
                    .as_ref()
                    .and_then(|ctx| ctx.peer.as_ref())
                    .map(|peer| peer.store_id)
                    .expect("store_id should be set");
                store_ids_for_hook.lock().unwrap().push(store_id);

                let call = calls_for_hook.fetch_add(1, Ordering::SeqCst);
                match call {
                    0 => Ok(Box::new(kvrpcpb::RawGetResponse {
                        region_error: Some(errorpb::Error {
                            stale_command: Some(errorpb::StaleCommand::default()),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }) as Box<dyn Any + Send>),
                    _ => Ok(Box::new(kvrpcpb::RawGetResponse {
                        not_found: true,
                        ..Default::default()
                    }) as Box<dyn Any + Send>),
                }
            },
        )));

        let mut request = kvrpcpb::RawGetRequest::default();
        request.key = vec![5];

        let mut read_routing = ReadRouting::default();
        read_routing.set_replica_read(crate::ReplicaReadType::PreferLeader);
        read_routing.set_seed(0);

        let plan = RetryableMultiRegion {
            inner: Dispatch {
                request,
                kv_client: None,
            },
            pd_client: pd_client.clone(),
            // No backoff budget: replica reads should switch peers immediately on stale-command.
            backoff: Backoff::no_jitter_backoff(50, 50, 0),
            concurrency: 1,
            preserve_region_results: false,
            request_context: RequestContext::default(),
            read_routing,
        };

        let results = plan.execute().await?;
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(results.len(), 1);
        let resp = results.into_iter().next().unwrap()?;
        assert!(resp.not_found);

        assert_eq!(&*store_ids.lock().unwrap(), &[1, 2]);
        assert_eq!(pd_client.invalidated_regions.load(Ordering::SeqCst), 0);
        assert_eq!(pd_client.invalidated_stores.load(Ordering::SeqCst), 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_retryable_multi_region_replica_read_stale_command_exhausts_peers_and_fails(
    ) -> Result<()> {
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_for_hook = calls.clone();
        let store_ids = Arc::new(Mutex::new(Vec::<u64>::new()));
        let store_ids_for_hook = store_ids.clone();

        let pd_client = Arc::new(LeaderSwitchPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let Some(req) = req.downcast_ref::<kvrpcpb::RawGetRequest>() else {
                    unreachable!("unexpected request type");
                };
                let store_id = req
                    .context
                    .as_ref()
                    .and_then(|ctx| ctx.peer.as_ref())
                    .map(|peer| peer.store_id)
                    .expect("store_id should be set");
                store_ids_for_hook.lock().unwrap().push(store_id);

                calls_for_hook.fetch_add(1, Ordering::SeqCst);
                Ok(Box::new(kvrpcpb::RawGetResponse {
                    region_error: Some(errorpb::Error {
                        stale_command: Some(errorpb::StaleCommand::default()),
                        ..Default::default()
                    }),
                    ..Default::default()
                }) as Box<dyn Any + Send>)
            },
        )));

        let mut request = kvrpcpb::RawGetRequest::default();
        request.key = vec![5];

        let mut read_routing = ReadRouting::default();
        read_routing.set_replica_read(crate::ReplicaReadType::PreferLeader);
        read_routing.set_seed(0);

        let plan = RetryableMultiRegion {
            inner: Dispatch {
                request,
                kv_client: None,
            },
            pd_client: pd_client.clone(),
            // No backoff budget: replica reads should switch peers immediately on stale-command,
            // but should still have a bounded number of attempts.
            backoff: Backoff::no_jitter_backoff(50, 50, 0),
            concurrency: 1,
            preserve_region_results: false,
            request_context: RequestContext::default(),
            read_routing,
        };

        let err = plan
            .execute()
            .await
            .expect_err("stale-command should fail after exhausting replicas");
        assert!(matches!(err, Error::RegionError(_)));

        assert_eq!(calls.load(Ordering::SeqCst), 3);
        assert_eq!(&*store_ids.lock().unwrap(), &[1, 2, 3]);
        assert_eq!(pd_client.invalidated_regions.load(Ordering::SeqCst), 1);
        assert_eq!(pd_client.invalidated_stores.load(Ordering::SeqCst), 0);
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn test_retryable_multi_region_stale_read_data_is_not_ready_waits_and_retries(
    ) -> Result<()> {
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_for_hook = calls.clone();
        let store_ids = Arc::new(Mutex::new(Vec::<u64>::new()));
        let store_ids_for_hook = store_ids.clone();
        let stale_read_flags = Arc::new(Mutex::new(Vec::<bool>::new()));
        let stale_read_flags_for_hook = stale_read_flags.clone();

        let pd_client = Arc::new(LeaderSwitchPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let Some(req) = req.downcast_ref::<kvrpcpb::RawGetRequest>() else {
                    unreachable!("unexpected request type");
                };
                let ctx = req
                    .context
                    .as_ref()
                    .expect("missing context on RawGetRequest");
                let store_id = ctx
                    .peer
                    .as_ref()
                    .map(|peer| peer.store_id)
                    .expect("store_id should be set");
                store_ids_for_hook.lock().unwrap().push(store_id);
                stale_read_flags_for_hook
                    .lock()
                    .unwrap()
                    .push(ctx.stale_read);

                let call = calls_for_hook.fetch_add(1, Ordering::SeqCst);
                match call {
                    0 => Ok(Box::new(kvrpcpb::RawGetResponse {
                        region_error: Some(errorpb::Error {
                            data_is_not_ready: Some(errorpb::DataIsNotReady {
                                region_id: 1,
                                peer_id: 0,
                                safe_ts: 0,
                            }),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }) as Box<dyn Any + Send>),
                    _ => Ok(Box::new(kvrpcpb::RawGetResponse {
                        not_found: true,
                        ..Default::default()
                    }) as Box<dyn Any + Send>),
                }
            },
        )));

        let mut request = kvrpcpb::RawGetRequest::default();
        request.key = vec![5];

        let mut read_routing = ReadRouting::default();
        read_routing.set_stale_read(true);
        // Deterministically select a follower on the first attempt.
        read_routing.set_seed(1);

        let plan = RetryableMultiRegion {
            inner: Dispatch {
                request,
                kv_client: None,
            },
            pd_client: pd_client.clone(),
            // No region backoff: data_is_not_ready on stale-read uses a dedicated retry loop.
            backoff: Backoff::no_jitter_backoff(50, 50, 0),
            concurrency: 1,
            preserve_region_results: false,
            request_context: RequestContext::default(),
            read_routing,
        };

        let handle = tokio::spawn(async move { plan.execute().await });

        while calls.load(Ordering::SeqCst) < 1 {
            tokio::task::yield_now().await;
        }
        tokio::task::yield_now().await;
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        tokio::time::advance(Duration::from_millis(199)).await;
        tokio::task::yield_now().await;
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        tokio::time::advance(Duration::from_millis(1)).await;
        tokio::task::yield_now().await;

        let results = handle.await.expect("task panicked")?;
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(results.len(), 1);
        let resp = results.into_iter().next().unwrap()?;
        assert!(resp.not_found);

        // Stale-read retries fall back to leader, while keeping `Context.stale_read=true`.
        assert_eq!(&*store_ids.lock().unwrap(), &[2, 1]);
        assert_eq!(&*stale_read_flags.lock().unwrap(), &[true, true]);
        assert_eq!(pd_client.invalidated_regions.load(Ordering::SeqCst), 0);
        assert_eq!(pd_client.invalidated_stores.load(Ordering::SeqCst), 0);
        Ok(())
    }

    async fn run_replica_read_keep_peer_region_error_test(
        region_error: errorpb::Error,
    ) -> Result<()> {
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_for_hook = calls.clone();
        let store_ids = Arc::new(Mutex::new(Vec::<u64>::new()));
        let store_ids_for_hook = store_ids.clone();

        let region_error_for_hook = region_error.clone();
        let pd_client = Arc::new(LeaderSwitchPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let Some(req) = req.downcast_ref::<kvrpcpb::RawGetRequest>() else {
                    unreachable!("unexpected request type");
                };
                let store_id = req
                    .context
                    .as_ref()
                    .and_then(|ctx| ctx.peer.as_ref())
                    .map(|peer| peer.store_id)
                    .expect("store_id should be set");
                store_ids_for_hook.lock().unwrap().push(store_id);

                let call = calls_for_hook.fetch_add(1, Ordering::SeqCst);
                match call {
                    0 => Ok(Box::new(kvrpcpb::RawGetResponse {
                        region_error: Some(region_error_for_hook.clone()),
                        ..Default::default()
                    }) as Box<dyn Any + Send>),
                    _ => Ok(Box::new(kvrpcpb::RawGetResponse {
                        not_found: true,
                        ..Default::default()
                    }) as Box<dyn Any + Send>),
                }
            },
        )));

        let mut request = kvrpcpb::RawGetRequest::default();
        request.key = vec![5];

        let mut read_routing = ReadRouting::default();
        read_routing.set_replica_read(crate::ReplicaReadType::PreferLeader);
        read_routing.set_seed(0);

        let plan = RetryableMultiRegion {
            inner: Dispatch {
                request,
                kv_client: None,
            },
            pd_client: pd_client.clone(),
            // One retry is enough; use a 0ms delay to keep the test deterministic.
            backoff: Backoff::no_jitter_backoff(0, 0, 1),
            concurrency: 1,
            preserve_region_results: false,
            request_context: RequestContext::default(),
            read_routing,
        };

        let results = plan.execute().await?;
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(results.len(), 1);
        let resp = results.into_iter().next().unwrap()?;
        assert!(resp.not_found);

        assert_eq!(&*store_ids.lock().unwrap(), &[1, 1]);
        assert_eq!(pd_client.invalidated_regions.load(Ordering::SeqCst), 0);
        assert_eq!(pd_client.invalidated_stores.load(Ordering::SeqCst), 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_retryable_multi_region_replica_read_keep_peer_errors_retry_same_store(
    ) -> Result<()> {
        for region_error in [
            errorpb::Error {
                max_timestamp_not_synced: Some(errorpb::MaxTimestampNotSynced::default()),
                ..Default::default()
            },
            errorpb::Error {
                read_index_not_ready: Some(errorpb::ReadIndexNotReady::default()),
                ..Default::default()
            },
            errorpb::Error {
                proposal_in_merging_mode: Some(errorpb::ProposalInMergingMode::default()),
                ..Default::default()
            },
        ] {
            run_replica_read_keep_peer_region_error_test(region_error).await?;
        }
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn test_retryable_multi_region_replica_read_server_is_busy_fast_retries() -> Result<()> {
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_for_hook = calls.clone();
        let store_ids = Arc::new(Mutex::new(Vec::<u64>::new()));
        let store_ids_for_hook = store_ids.clone();

        let pd_client = Arc::new(LeaderSwitchPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let Some(req) = req.downcast_ref::<kvrpcpb::RawGetRequest>() else {
                    unreachable!("unexpected request type");
                };
                let store_id = req
                    .context
                    .as_ref()
                    .and_then(|ctx| ctx.peer.as_ref())
                    .map(|peer| peer.store_id)
                    .expect("store_id should be set");
                store_ids_for_hook.lock().unwrap().push(store_id);

                let call = calls_for_hook.fetch_add(1, Ordering::SeqCst);
                match call {
                    0 => Ok(Box::new(kvrpcpb::RawGetResponse {
                        region_error: Some(errorpb::Error {
                            server_is_busy: Some(errorpb::ServerIsBusy::default()),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }) as Box<dyn Any + Send>),
                    _ => Ok(Box::new(kvrpcpb::RawGetResponse {
                        not_found: true,
                        ..Default::default()
                    }) as Box<dyn Any + Send>),
                }
            },
        )));

        let mut request = kvrpcpb::RawGetRequest::default();
        request.key = vec![5];

        let mut read_routing = ReadRouting::default();
        read_routing.set_replica_read(crate::ReplicaReadType::PreferLeader);
        read_routing.set_seed(0);

        let plan = RetryableMultiRegion {
            inner: Dispatch {
                request,
                kv_client: None,
            },
            pd_client: pd_client.clone(),
            backoff: Backoff::no_jitter_backoff(50, 50, 1),
            concurrency: 1,
            preserve_region_results: false,
            request_context: RequestContext::default(),
            read_routing,
        };

        let handle = tokio::spawn(async move { plan.execute().await });

        while calls.load(Ordering::SeqCst) < 1 {
            tokio::task::yield_now().await;
        }
        tokio::task::yield_now().await;
        assert!(
            calls.load(Ordering::SeqCst) >= 1,
            "expected at least one dispatch attempt"
        );

        // ServerIsBusy in replica-read mode should fast-retry another peer, without waiting for
        // the backoff duration.
        for _ in 0..10 {
            if calls.load(Ordering::SeqCst) >= 2 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(calls.load(Ordering::SeqCst), 2);

        let results = handle.await.expect("task panicked")?;
        assert_eq!(results.len(), 1);
        let resp = results.into_iter().next().unwrap()?;
        assert!(resp.not_found);

        assert_eq!(&*store_ids.lock().unwrap(), &[1, 2]);
        assert_eq!(pd_client.invalidated_regions.load(Ordering::SeqCst), 0);
        assert_eq!(pd_client.invalidated_stores.load(Ordering::SeqCst), 0);
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn test_retryable_multi_region_stale_command_backoffs() -> Result<()> {
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_for_hook = calls.clone();

        let pd_client = Arc::new(LeaderSwitchPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let Some(_) = req.downcast_ref::<kvrpcpb::RawGetRequest>() else {
                    unreachable!("unexpected request type");
                };
                let call = calls_for_hook.fetch_add(1, Ordering::SeqCst);
                match call {
                    0 => Ok(Box::new(kvrpcpb::RawGetResponse {
                        region_error: Some(errorpb::Error {
                            stale_command: Some(errorpb::StaleCommand::default()),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }) as Box<dyn Any + Send>),
                    _ => Ok(Box::new(kvrpcpb::RawGetResponse {
                        not_found: true,
                        ..Default::default()
                    }) as Box<dyn Any + Send>),
                }
            },
        )));

        let mut request = kvrpcpb::RawGetRequest::default();
        request.key = vec![5];
        let plan = RetryableMultiRegion {
            inner: Dispatch {
                request,
                kv_client: None,
            },
            pd_client: pd_client.clone(),
            backoff: Backoff::no_jitter_backoff(50, 50, 1),
            concurrency: 1,
            preserve_region_results: false,
            request_context: RequestContext::default(),
            read_routing: ReadRouting::default(),
        };

        let handle = tokio::spawn(async move { plan.execute().await });

        while calls.load(Ordering::SeqCst) < 1 {
            tokio::task::yield_now().await;
        }
        tokio::task::yield_now().await;
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        tokio::time::advance(Duration::from_millis(49)).await;
        tokio::task::yield_now().await;
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        tokio::time::advance(Duration::from_millis(1)).await;
        tokio::task::yield_now().await;

        let results = handle.await.expect("task panicked")?;
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(results.len(), 1);
        let resp = results.into_iter().next().unwrap()?;
        assert!(resp.not_found);

        assert_eq!(pd_client.invalidated_regions.load(Ordering::SeqCst), 1);
        assert_eq!(pd_client.invalidated_stores.load(Ordering::SeqCst), 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_retryable_multi_region_epoch_not_match_resolved_does_not_consume_backoff(
    ) -> Result<()> {
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_for_hook = calls.clone();

        let pd_client = Arc::new(LeaderSwitchPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let Some(_) = req.downcast_ref::<kvrpcpb::RawGetRequest>() else {
                    unreachable!("unexpected request type");
                };
                let call = calls_for_hook.fetch_add(1, Ordering::SeqCst);
                match call {
                    0 => Ok(Box::new(kvrpcpb::RawGetResponse {
                        region_error: Some(errorpb::Error {
                            epoch_not_match: Some(errorpb::EpochNotMatch {
                                current_regions: vec![metapb::Region {
                                    id: 1,
                                    region_epoch: Some(metapb::RegionEpoch {
                                        conf_ver: 10,
                                        version: 10,
                                    }),
                                    ..Default::default()
                                }],
                            }),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }) as Box<dyn Any + Send>),
                    _ => Ok(Box::new(kvrpcpb::RawGetResponse {
                        not_found: true,
                        ..Default::default()
                    }) as Box<dyn Any + Send>),
                }
            },
        )));

        // Ensure the cached epoch is behind, so epoch-not-match is "resolved" (retry immediately).
        pd_client.region.lock().unwrap().region.region_epoch = Some(metapb::RegionEpoch {
            conf_ver: 1,
            version: 1,
        });

        let mut request = kvrpcpb::RawGetRequest::default();
        request.key = vec![5];
        let plan = RetryableMultiRegion {
            inner: Dispatch {
                request,
                kv_client: None,
            },
            pd_client: pd_client.clone(),
            // No backoff attempts available: resolved epoch-not-match must not consume backoff.
            backoff: Backoff::no_jitter_backoff(50, 50, 0),
            concurrency: 1,
            preserve_region_results: false,
            request_context: RequestContext::default(),
            read_routing: ReadRouting::default(),
        };

        let results = plan.execute().await?;
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(results.len(), 1);
        let resp = results.into_iter().next().unwrap()?;
        assert!(resp.not_found);

        assert_eq!(pd_client.invalidated_regions.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn test_retryable_multi_region_epoch_not_match_unresolved_backoffs() -> Result<()> {
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_for_hook = calls.clone();

        let pd_client = Arc::new(LeaderSwitchPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let Some(_) = req.downcast_ref::<kvrpcpb::RawGetRequest>() else {
                    unreachable!("unexpected request type");
                };
                let call = calls_for_hook.fetch_add(1, Ordering::SeqCst);
                match call {
                    0 => Ok(Box::new(kvrpcpb::RawGetResponse {
                        region_error: Some(errorpb::Error {
                            epoch_not_match: Some(errorpb::EpochNotMatch {
                                current_regions: vec![metapb::Region {
                                    id: 1,
                                    region_epoch: Some(metapb::RegionEpoch {
                                        conf_ver: 1,
                                        version: 1,
                                    }),
                                    ..Default::default()
                                }],
                            }),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }) as Box<dyn Any + Send>),
                    _ => Ok(Box::new(kvrpcpb::RawGetResponse {
                        not_found: true,
                        ..Default::default()
                    }) as Box<dyn Any + Send>),
                }
            },
        )));

        // Ensure the cached epoch is ahead, so epoch-not-match is "unresolved" (backoff).
        pd_client.region.lock().unwrap().region.region_epoch = Some(metapb::RegionEpoch {
            conf_ver: 10,
            version: 10,
        });

        let mut request = kvrpcpb::RawGetRequest::default();
        request.key = vec![5];
        let plan = RetryableMultiRegion {
            inner: Dispatch {
                request,
                kv_client: None,
            },
            pd_client: pd_client.clone(),
            backoff: Backoff::no_jitter_backoff(50, 50, 1),
            concurrency: 1,
            preserve_region_results: false,
            request_context: RequestContext::default(),
            read_routing: ReadRouting::default(),
        };

        let handle = tokio::spawn(async move { plan.execute().await });

        while calls.load(Ordering::SeqCst) < 1 {
            tokio::task::yield_now().await;
        }
        tokio::task::yield_now().await;
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        tokio::time::advance(Duration::from_millis(49)).await;
        tokio::task::yield_now().await;
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        tokio::time::advance(Duration::from_millis(1)).await;
        tokio::task::yield_now().await;

        let results = handle.await.expect("task panicked")?;
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(results.len(), 1);
        let resp = results.into_iter().next().unwrap()?;
        assert!(resp.not_found);

        assert_eq!(pd_client.invalidated_regions.load(Ordering::SeqCst), 0);
        Ok(())
    }

    #[derive(Clone)]
    struct MultiStorePdClient {
        client: MockKvClient,
        region: RegionWithLeader,
    }

    impl MultiStorePdClient {
        fn new(client: MockKvClient) -> Self {
            let leader = metapb::Peer {
                id: 101,
                store_id: 1,
                role: metapb::PeerRole::Voter as i32,
                is_witness: false,
            };
            let follower_1 = metapb::Peer {
                id: 102,
                store_id: 2,
                role: metapb::PeerRole::Voter as i32,
                is_witness: false,
            };
            let follower_2 = metapb::Peer {
                id: 103,
                store_id: 3,
                role: metapb::PeerRole::Voter as i32,
                is_witness: false,
            };

            let mut region = metapb::Region::default();
            region.id = 1;
            region.start_key = vec![];
            region.end_key = vec![];
            region.region_epoch = Some(metapb::RegionEpoch {
                conf_ver: 0,
                version: 0,
            });
            region.peers = vec![leader.clone(), follower_1, follower_2];

            Self {
                client,
                region: RegionWithLeader {
                    region,
                    leader: Some(leader),
                },
            }
        }
    }

    #[async_trait]
    impl PdClient for MultiStorePdClient {
        type KvClient = MockKvClient;

        async fn map_region_to_store(
            self: Arc<Self>,
            region: RegionWithLeader,
        ) -> Result<RegionStore> {
            Ok(RegionStore::new(region, Arc::new(self.client.clone())))
        }

        async fn region_for_key(&self, _key: &Key) -> Result<RegionWithLeader> {
            Ok(self.region.clone())
        }

        async fn region_for_id(&self, _id: RegionId) -> Result<RegionWithLeader> {
            Ok(self.region.clone())
        }

        async fn get_timestamp(self: Arc<Self>) -> Result<Timestamp> {
            Ok(Timestamp::default())
        }

        async fn get_min_ts(self: Arc<Self>) -> Result<Timestamp> {
            Ok(Timestamp::default())
        }

        async fn update_safepoint(self: Arc<Self>, _safepoint: u64) -> Result<bool> {
            Ok(true)
        }

        async fn load_keyspace(&self, keyspace: &str) -> Result<keyspacepb::KeyspaceMeta> {
            Ok(keyspacepb::KeyspaceMeta {
                id: 0,
                name: keyspace.to_owned(),
                state: keyspacepb::KeyspaceState::Enabled as i32,
                ..Default::default()
            })
        }

        async fn all_stores(&self) -> Result<Vec<Store>> {
            Ok(vec![Store::new(Arc::new(self.client.clone()))])
        }

        async fn update_leader(&self, _ver_id: RegionVerId, _leader: metapb::Peer) -> Result<()> {
            Ok(())
        }

        async fn invalidate_region_cache(&self, _ver_id: RegionVerId) {}

        async fn invalidate_store_cache(&self, _store_id: StoreId) {}
    }

    #[tokio::test]
    async fn test_fast_retry_avoids_slow_store_after_grpc_error() -> Result<()> {
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_for_hook = calls.clone();

        let seen = Arc::new(Mutex::new(Vec::new()));
        let seen_for_hook = seen.clone();

        let pd_client = Arc::new(MultiStorePdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let Some(req) = req.downcast_ref::<kvrpcpb::RawGetRequest>() else {
                    unreachable!("unexpected request type");
                };
                let ctx = req
                    .context
                    .as_ref()
                    .expect("missing context on RawGetRequest");
                let peer = ctx.peer.as_ref().expect("missing peer in context");
                seen_for_hook.lock().unwrap().push((
                    peer.store_id,
                    ctx.is_retry_request,
                    ctx.request_source.clone(),
                ));

                let call = calls_for_hook.fetch_add(1, Ordering::SeqCst);
                if call == 0 {
                    return Err(Error::GrpcAPI(Status::unavailable("grpc error")));
                }
                Ok(Box::new(kvrpcpb::RawGetResponse {
                    not_found: true,
                    ..Default::default()
                }) as Box<dyn Any + Send>)
            },
        )));

        let mut request = kvrpcpb::RawGetRequest::default();
        request.key = vec![5];

        let read_routing = ReadRouting::new(crate::ReplicaReadType::PreferLeader, false)
            .with_seed(1)
            .with_request_source(Some(Arc::<str>::from("fast-retry")));
        // Force the first attempt to pick a replica; then assert we avoid it after it becomes slow.
        read_routing.mark_store_slow_for(1, Duration::from_secs(60));

        let plan = RetryableMultiRegion {
            inner: Dispatch {
                request,
                kv_client: None,
            },
            pd_client: pd_client.clone(),
            backoff: Backoff::no_jitter_backoff(0, 0, 1),
            concurrency: 1,
            preserve_region_results: false,
            request_context: RequestContext::default(),
            read_routing,
        };

        let results = plan.execute().await?;
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(results.len(), 1);

        let response = results.into_iter().next().unwrap()?;
        assert!(response.not_found);

        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 2);
        assert_eq!(seen[0].0, 2);
        assert!(!seen[0].1);
        assert_eq!(seen[0].2, "follower_fast-retry");

        assert_eq!(seen[1].0, 3);
        assert!(seen[1].1);
        assert_eq!(seen[1].2, "retry_follower_follower_fast-retry");
        Ok(())
    }

    #[tokio::test]
    async fn test_retryable_multi_region_grpc_error_without_backoff_returns_err() {
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_for_hook = calls.clone();
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let Some(_) = req.downcast_ref::<kvrpcpb::RawGetRequest>() else {
                    unreachable!("unexpected request type");
                };
                calls_for_hook.fetch_add(1, Ordering::SeqCst);
                Err(Error::GrpcAPI(Status::unavailable("grpc error")))
            },
        )));

        let mut request = kvrpcpb::RawGetRequest::default();
        request.key = vec![5];
        let plan = RetryableMultiRegion {
            inner: Dispatch {
                request,
                kv_client: None,
            },
            pd_client,
            backoff: Backoff::no_backoff(),
            concurrency: 1,
            preserve_region_results: false,
            request_context: RequestContext::default(),
            read_routing: ReadRouting::default(),
        };

        assert!(plan.execute().await.is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_handle_region_error_not_leader_update_leader_failure_invalidates_region() {
        let pd_client = Arc::new(CountingPdClient {
            fail_update_leader: true,
            ..Default::default()
        });
        let region_store =
            RegionStore::new(MockPdClient::region1(), Arc::new(MockKvClient::default()));
        let err = errorpb::Error {
            not_leader: Some(errorpb::NotLeader {
                leader: Some(metapb::Peer {
                    store_id: 99,
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        };

        assert!(handle_region_error(pd_client.clone(), err, region_store)
            .await
            .is_err());
        assert_eq!(pd_client.update_leader_calls.load(Ordering::SeqCst), 1);
        assert_eq!(pd_client.invalidated_regions.load(Ordering::SeqCst), 1);
        assert_eq!(pd_client.invalidated_stores.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_handle_region_error_not_leader_updates_leader_and_retries() -> Result<()> {
        let pd_client = Arc::new(CountingPdClient::default());
        let region_store =
            RegionStore::new(MockPdClient::region1(), Arc::new(MockKvClient::default()));
        let err = errorpb::Error {
            not_leader: Some(errorpb::NotLeader {
                leader: Some(metapb::Peer {
                    store_id: 99,
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        };

        assert!(handle_region_error(pd_client.clone(), err, region_store).await?);
        assert_eq!(pd_client.update_leader_calls.load(Ordering::SeqCst), 1);
        assert_eq!(pd_client.invalidated_regions.load(Ordering::SeqCst), 0);
        assert_eq!(pd_client.invalidated_stores.load(Ordering::SeqCst), 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_region_error_not_leader_without_leader_invalidates_region() -> Result<()> {
        let pd_client = Arc::new(CountingPdClient::default());
        let region_store =
            RegionStore::new(MockPdClient::region1(), Arc::new(MockKvClient::default()));
        let err = errorpb::Error {
            not_leader: Some(errorpb::NotLeader::default()),
            ..Default::default()
        };

        assert!(!handle_region_error(pd_client.clone(), err, region_store).await?);
        assert_eq!(pd_client.update_leader_calls.load(Ordering::SeqCst), 0);
        assert_eq!(pd_client.invalidated_regions.load(Ordering::SeqCst), 1);
        assert_eq!(pd_client.invalidated_stores.load(Ordering::SeqCst), 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_region_error_store_not_match_invalidates_region_and_store() -> Result<()> {
        let pd_client = Arc::new(CountingPdClient::default());
        let region_store =
            RegionStore::new(MockPdClient::region1(), Arc::new(MockKvClient::default()));
        let err = errorpb::Error {
            store_not_match: Some(errorpb::StoreNotMatch::default()),
            ..Default::default()
        };

        assert!(handle_region_error(pd_client.clone(), err, region_store).await?);
        assert_eq!(pd_client.invalidated_regions.load(Ordering::SeqCst), 1);
        assert_eq!(pd_client.invalidated_stores.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_region_error_store_not_match_without_leader_only_invalidates_region(
    ) -> Result<()> {
        let pd_client = Arc::new(CountingPdClient::default());
        let mut region = MockPdClient::region1();
        region.leader = None;
        let region_store = RegionStore::new(region, Arc::new(MockKvClient::default()));
        let err = errorpb::Error {
            store_not_match: Some(errorpb::StoreNotMatch::default()),
            ..Default::default()
        };

        assert!(handle_region_error(pd_client.clone(), err, region_store).await?);
        assert_eq!(pd_client.invalidated_regions.load(Ordering::SeqCst), 1);
        assert_eq!(pd_client.invalidated_stores.load(Ordering::SeqCst), 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_region_error_stale_command_invalidates_region() -> Result<()> {
        let pd_client = Arc::new(CountingPdClient::default());
        let region_store =
            RegionStore::new(MockPdClient::region1(), Arc::new(MockKvClient::default()));
        let err = errorpb::Error {
            stale_command: Some(errorpb::StaleCommand::default()),
            ..Default::default()
        };

        assert!(!handle_region_error(pd_client.clone(), err, region_store).await?);
        assert_eq!(pd_client.invalidated_regions.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_region_error_server_is_busy_backoffs_without_invalidation() -> Result<()> {
        let pd_client = Arc::new(CountingPdClient::default());
        let region_store =
            RegionStore::new(MockPdClient::region1(), Arc::new(MockKvClient::default()));
        let err = errorpb::Error {
            server_is_busy: Some(errorpb::ServerIsBusy::default()),
            ..Default::default()
        };

        assert!(!handle_region_error(pd_client.clone(), err, region_store).await?);
        assert_eq!(pd_client.invalidated_regions.load(Ordering::SeqCst), 0);
        assert_eq!(pd_client.invalidated_stores.load(Ordering::SeqCst), 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_region_error_max_timestamp_not_synced_backoffs_without_invalidation(
    ) -> Result<()> {
        let pd_client = Arc::new(CountingPdClient::default());
        let region_store =
            RegionStore::new(MockPdClient::region1(), Arc::new(MockKvClient::default()));
        let err = errorpb::Error {
            max_timestamp_not_synced: Some(errorpb::MaxTimestampNotSynced::default()),
            ..Default::default()
        };

        assert!(!handle_region_error(pd_client.clone(), err, region_store).await?);
        assert_eq!(pd_client.invalidated_regions.load(Ordering::SeqCst), 0);
        assert_eq!(pd_client.invalidated_stores.load(Ordering::SeqCst), 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_region_error_read_index_not_ready_backoffs_without_invalidation(
    ) -> Result<()> {
        let pd_client = Arc::new(CountingPdClient::default());
        let region_store =
            RegionStore::new(MockPdClient::region1(), Arc::new(MockKvClient::default()));
        let err = errorpb::Error {
            read_index_not_ready: Some(errorpb::ReadIndexNotReady::default()),
            ..Default::default()
        };

        assert!(!handle_region_error(pd_client.clone(), err, region_store).await?);
        assert_eq!(pd_client.invalidated_regions.load(Ordering::SeqCst), 0);
        assert_eq!(pd_client.invalidated_stores.load(Ordering::SeqCst), 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_region_error_proposal_in_merging_mode_backoffs_without_invalidation(
    ) -> Result<()> {
        let pd_client = Arc::new(CountingPdClient::default());
        let region_store =
            RegionStore::new(MockPdClient::region1(), Arc::new(MockKvClient::default()));
        let err = errorpb::Error {
            proposal_in_merging_mode: Some(errorpb::ProposalInMergingMode::default()),
            ..Default::default()
        };

        assert!(!handle_region_error(pd_client.clone(), err, region_store).await?);
        assert_eq!(pd_client.invalidated_regions.load(Ordering::SeqCst), 0);
        assert_eq!(pd_client.invalidated_stores.load(Ordering::SeqCst), 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_region_error_unknown_invalidates_region_and_backoffs() -> Result<()> {
        let pd_client = Arc::new(CountingPdClient::default());
        let region_store =
            RegionStore::new(MockPdClient::region1(), Arc::new(MockKvClient::default()));
        let err = errorpb::Error {
            message: "unknown".to_owned(),
            ..Default::default()
        };

        assert!(!handle_region_error(pd_client.clone(), err, region_store).await?);
        assert_eq!(pd_client.invalidated_regions.load(Ordering::SeqCst), 1);
        assert_eq!(pd_client.invalidated_stores.load(Ordering::SeqCst), 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_region_error_key_not_in_region_invalidates_region_and_retries(
    ) -> Result<()> {
        let pd_client = Arc::new(CountingPdClient::default());
        let region_store =
            RegionStore::new(MockPdClient::region1(), Arc::new(MockKvClient::default()));
        let err = errorpb::Error {
            key_not_in_region: Some(errorpb::KeyNotInRegion {
                key: vec![1],
                region_id: 1,
                start_key: vec![],
                end_key: vec![10],
            }),
            ..Default::default()
        };

        assert!(handle_region_error(pd_client.clone(), err, region_store).await?);
        assert_eq!(pd_client.invalidated_regions.load(Ordering::SeqCst), 1);
        assert_eq!(pd_client.invalidated_stores.load(Ordering::SeqCst), 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_on_region_epoch_not_match_missing_region_epoch_invalidates_and_retries(
    ) -> Result<()> {
        let pd_client = Arc::new(CountingPdClient::default());
        let region_store =
            RegionStore::new(MockPdClient::region1(), Arc::new(MockKvClient::default()));
        let err = EpochNotMatch {
            current_regions: vec![metapb::Region {
                id: 1,
                ..Default::default()
            }],
        };

        assert!(on_region_epoch_not_match(pd_client.clone(), region_store, err).await?);
        assert_eq!(pd_client.invalidated_regions.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_on_region_epoch_not_match_empty_current_regions_invalidates_and_backoffs(
    ) -> Result<()> {
        let pd_client = Arc::new(CountingPdClient::default());
        let region_store =
            RegionStore::new(MockPdClient::region1(), Arc::new(MockKvClient::default()));
        let err = EpochNotMatch::default();

        assert!(!on_region_epoch_not_match(pd_client.clone(), region_store, err).await?);
        assert_eq!(pd_client.invalidated_regions.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_on_region_epoch_not_match_missing_cached_epoch_invalidates_and_retries(
    ) -> Result<()> {
        let pd_client = Arc::new(CountingPdClient::default());
        let mut region = MockPdClient::region1();
        region.region.region_epoch = None;
        let region_store = RegionStore::new(region, Arc::new(MockKvClient::default()));
        let err = EpochNotMatch {
            current_regions: vec![metapb::Region {
                id: 1,
                region_epoch: Some(metapb::RegionEpoch {
                    conf_ver: 1,
                    version: 1,
                }),
                ..Default::default()
            }],
        };

        assert!(on_region_epoch_not_match(pd_client.clone(), region_store, err).await?);
        assert_eq!(pd_client.invalidated_regions.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_on_region_epoch_not_match_current_epoch_ahead_backoffs() -> Result<()> {
        let pd_client = Arc::new(CountingPdClient::default());
        let mut region = MockPdClient::region1();
        region.region.region_epoch = Some(metapb::RegionEpoch {
            conf_ver: 2,
            version: 2,
        });
        let region_store = RegionStore::new(region, Arc::new(MockKvClient::default()));
        let err = EpochNotMatch {
            current_regions: vec![metapb::Region {
                id: 1,
                region_epoch: Some(metapb::RegionEpoch {
                    conf_ver: 1,
                    version: 1,
                }),
                ..Default::default()
            }],
        };

        assert!(!on_region_epoch_not_match(pd_client.clone(), region_store, err).await?);
        assert_eq!(pd_client.invalidated_regions.load(Ordering::SeqCst), 0);
        Ok(())
    }

    #[test]
    fn test_cleanup_locks_result_clone_preserves_count_and_drops_errors() {
        let mut region_error = errorpb::Error::default();
        region_error.message = "err".to_owned();
        let result = CleanupLocksResult {
            region_error: Some(region_error),
            key_error: Some(vec![Error::Unimplemented]),
            resolved_locks: 7,
        };

        let cloned = result.clone();
        assert_eq!(cloned.resolved_locks, 7);
        assert!(cloned.region_error.is_none());
        assert!(cloned.key_error.is_none());
    }

    #[test]
    fn test_extract_error_is_cloneable() {
        let extractor = ExtractError { inner: ErrPlan };
        let _ = extractor.clone();
    }

    #[test]
    fn test_collect_single_errors_when_not_singleton() {
        let merge = CollectSingle;
        let input = vec![
            Ok(kvrpcpb::RawGetResponse::default()),
            Ok(kvrpcpb::RawGetResponse::default()),
        ];
        assert!(merge.merge(input).is_err());
    }

    #[test]
    fn test_response_with_shard_take_locks_delegates() {
        let mut resp = kvrpcpb::ScanLockResponse::default();
        resp.locks.push(kvrpcpb::LockInfo {
            key: vec![1],
            ..Default::default()
        });
        let mut wrapped = ResponseWithShard(resp, ());
        let locks = wrapped.take_locks();
        assert_eq!(locks.len(), 1);
    }

    #[test]
    fn test_retryable_all_stores_is_cloneable() {
        let pd_client = Arc::new(MockPdClient::default());
        let plan = RetryableAllStores {
            inner: StoreOkPlan,
            pd_client,
            backoff: Backoff::no_backoff(),
        };
        let _ = plan.clone();
    }

    #[tokio::test]
    async fn test_retryable_all_stores_single_store_handler_fails_if_semaphore_closed() {
        let permits = Arc::new(Semaphore::new(1));
        permits.close();
        assert!(
            RetryableAllStores::<StoreOkPlan, MockPdClient>::single_store_handler(
                StoreOkPlan,
                Backoff::no_backoff(),
                permits,
            )
            .await
            .is_err()
        );
    }

    #[tokio::test]
    async fn test_retryable_all_stores_single_store_handler_limits_concurrency() -> Result<()> {
        let permits = Arc::new(Semaphore::new(1));
        let calls = Arc::new(AtomicUsize::new(0));
        let in_flight = Arc::new(AtomicUsize::new(0));
        let max_in_flight = Arc::new(AtomicUsize::new(0));
        let release = Arc::new(Notify::new());

        let plan = BlockingPlan {
            calls: calls.clone(),
            in_flight,
            max_in_flight: max_in_flight.clone(),
            release: release.clone(),
        };

        let handle1 = tokio::spawn(
            RetryableAllStores::<BlockingPlan, MockPdClient>::single_store_handler(
                plan.clone(),
                Backoff::no_backoff(),
                permits.clone(),
            ),
        );
        let handle2 = tokio::spawn(
            RetryableAllStores::<BlockingPlan, MockPdClient>::single_store_handler(
                plan,
                Backoff::no_backoff(),
                permits.clone(),
            ),
        );

        while calls.load(Ordering::SeqCst) < 1 {
            tokio::task::yield_now().await;
        }
        assert_eq!(max_in_flight.load(Ordering::SeqCst), 1);
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        // Release the first request; the second one should not enter `execute` until the permit is
        // dropped (so `max_in_flight` must remain 1).
        release.notify_one();

        while calls.load(Ordering::SeqCst) < 2 {
            tokio::task::yield_now().await;
        }
        release.notify_one();

        handle1.await.unwrap()?;
        handle2.await.unwrap()?;

        assert_eq!(max_in_flight.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_retryable_all_stores_execute_is_cancel_safe() {
        let gate = Arc::new(Notify::new());
        let started = Arc::new(AtomicUsize::new(0));
        let finished = Arc::new(AtomicUsize::new(0));

        let plan = NotifyPlan {
            started: started.clone(),
            finished: finished.clone(),
            gate: gate.clone(),
        };

        let outer = RetryableAllStores {
            inner: plan,
            pd_client: Arc::new(MockPdClient::default()),
            backoff: Backoff::no_backoff(),
        };

        let handle = tokio::spawn(async move { outer.execute().await });

        while started.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }

        handle.abort();
        let _ = handle.await;

        // If inner tasks were detached, releasing the gate would allow them to complete and bump
        // `finished`. Cancellation should prevent that.
        gate.notify_waiters();
        tokio::time::sleep(Duration::from_millis(50)).await;

        assert_eq!(finished.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_retryable_all_stores_single_store_handler_key_errors_are_wrapped() {
        let permits = Arc::new(Semaphore::new(1));
        let err = RetryableAllStores::<StoreKeyErrorsPlan, MockPdClient>::single_store_handler(
            StoreKeyErrorsPlan,
            Backoff::no_backoff(),
            permits,
        )
        .await
        .expect_err("expected key errors to be surfaced as an error");
        assert!(matches!(err, Error::MultipleKeyErrors(_)));
    }

    #[tokio::test]
    async fn test_retryable_all_stores_single_store_handler_region_errors_are_wrapped() {
        let permits = Arc::new(Semaphore::new(1));
        let err = RetryableAllStores::<StoreRegionErrorPlan, MockPdClient>::single_store_handler(
            StoreRegionErrorPlan,
            Backoff::no_backoff(),
            permits,
        )
        .await
        .expect_err("expected region errors to be surfaced as an error");
        assert!(matches!(err, Error::RegionError(_)));
    }

    #[tokio::test]
    async fn test_retryable_all_stores_single_store_handler_retries_grpc_error_with_backoff(
    ) -> Result<()> {
        let permits = Arc::new(Semaphore::new(1));
        let calls = Arc::new(AtomicUsize::new(0));
        let plan = FlakyGrpcPlan { calls };
        let resp = RetryableAllStores::<FlakyGrpcPlan, MockPdClient>::single_store_handler(
            plan,
            Backoff::no_jitter_backoff(0, 0, 1),
            permits,
        )
        .await?;
        let mut resp = resp;
        assert!(resp.key_errors().is_none());
        assert!(resp.region_error().is_none());
        Ok(())
    }

    #[tokio::test]
    async fn test_retryable_all_stores_single_store_handler_grpc_error_without_backoff_returns_err()
    {
        let permits = Arc::new(Semaphore::new(1));
        assert!(
            RetryableAllStores::<AlwaysGrpcPlan, MockPdClient>::single_store_handler(
                AlwaysGrpcPlan,
                Backoff::no_backoff(),
                permits,
            )
            .await
            .is_err()
        );
    }

    #[tokio::test]
    async fn test_retryable_all_stores_single_store_handler_non_grpc_error_returns_err() {
        let permits = Arc::new(Semaphore::new(1));
        let err = RetryableAllStores::<NonGrpcErrorPlan, MockPdClient>::single_store_handler(
            NonGrpcErrorPlan,
            Backoff::no_backoff(),
            permits,
        )
        .await
        .expect_err("expected non-grpc error to be propagated");
        assert!(matches!(err, Error::Unimplemented));
    }

    #[tokio::test]
    async fn test_cleanup_locks_backoff_on_resolve_lock_error() -> Result<()> {
        let check_txn_status_calls = Arc::new(AtomicUsize::new(0));
        let check_txn_status_calls_for_hook = check_txn_status_calls.clone();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req| {
                if req.downcast_ref::<kvrpcpb::ScanLockRequest>().is_some() {
                    let mut resp = kvrpcpb::ScanLockResponse::default();
                    resp.locks.push(kvrpcpb::LockInfo {
                        key: vec![1],
                        primary_lock: vec![1],
                        lock_version: 42,
                        lock_type: kvrpcpb::Op::Put as i32,
                        ..Default::default()
                    });
                    return Ok(Box::new(resp));
                }

                if req
                    .downcast_ref::<kvrpcpb::CheckSecondaryLocksRequest>()
                    .is_some()
                {
                    let mut resp = kvrpcpb::CheckSecondaryLocksResponse::default();
                    // Force `fallback_2pc=true` so GC path re-checks txn status.
                    resp.locks.push(kvrpcpb::LockInfo {
                        use_async_commit: false,
                        ..Default::default()
                    });
                    return Ok(Box::new(resp));
                }

                if req.downcast_ref::<kvrpcpb::ResolveLockRequest>().is_some() {
                    return Ok(Box::new(kvrpcpb::ResolveLockResponse::default()));
                }

                if req
                    .downcast_ref::<kvrpcpb::CheckTxnStatusRequest>()
                    .is_some()
                {
                    let call = check_txn_status_calls_for_hook.fetch_add(1, Ordering::SeqCst);
                    if call < 2 {
                        let mut resp = kvrpcpb::CheckTxnStatusResponse::default();
                        resp.lock_ttl = 1;
                        resp.commit_version = 0;
                        resp.action = kvrpcpb::Action::NoAction as i32;
                        resp.lock_info = Some(kvrpcpb::LockInfo {
                            lock_version: 42,
                            secondaries: vec![vec![2]],
                            ..Default::default()
                        });
                        return Ok(Box::new(resp));
                    }
                    return Ok(Box::new(kvrpcpb::CheckTxnStatusResponse::default()));
                }

                panic!("unexpected request type: {:?}", req.type_id());
            },
        )));

        let store = pd_client
            .clone()
            .map_region_to_store(MockPdClient::region1())
            .await?;

        let mut scan_lock = kvrpcpb::ScanLockRequest::default();
        scan_lock.start_key = vec![1];
        scan_lock.end_key = vec![10];
        scan_lock.max_version = 0;
        scan_lock.limit = 2;

        let mut inner = Dispatch {
            request: scan_lock,
            kv_client: None,
        };
        inner.apply_store(&store)?;

        let plan = CleanupLocks {
            inner,
            ctx: ResolveLocksContext::default(),
            options: ResolveLocksOptions {
                async_commit_only: false,
                batch_size: 2,
            },
            store: Some(store),
            pd_client,
            keyspace: Keyspace::Disable,
            // One retry, 0ms delay.
            backoff: Backoff::no_jitter_backoff(0, 0, 1),
            request_context: RequestContext::default(),
        };

        let result = plan.execute().await?;
        assert_eq!(result.resolved_locks, 1);
        assert_eq!(check_txn_status_calls.load(Ordering::SeqCst), 3);
        Ok(())
    }

    #[tokio::test]
    async fn test_cleanup_locks_stops_at_end_key() -> Result<()> {
        let scan_lock_calls = Arc::new(AtomicUsize::new(0));
        let scan_lock_calls_for_hook = scan_lock_calls.clone();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req| {
                if req.downcast_ref::<kvrpcpb::ScanLockRequest>().is_some() {
                    let call = scan_lock_calls_for_hook.fetch_add(1, Ordering::SeqCst);
                    let mut resp = kvrpcpb::ScanLockResponse::default();
                    if call == 0 {
                        resp.locks.push(kvrpcpb::LockInfo {
                            key: vec![1],
                            primary_lock: vec![1],
                            lock_version: 42,
                            lock_type: kvrpcpb::Op::Put as i32,
                            ..Default::default()
                        });
                    }
                    return Ok(Box::new(resp));
                }
                if req
                    .downcast_ref::<kvrpcpb::CheckTxnStatusRequest>()
                    .is_some()
                {
                    // Always treat the txn as rolled back so cleanup can proceed.
                    return Ok(Box::new(kvrpcpb::CheckTxnStatusResponse::default()));
                }
                if req.downcast_ref::<kvrpcpb::ResolveLockRequest>().is_some() {
                    return Ok(Box::new(kvrpcpb::ResolveLockResponse::default()));
                }
                panic!("unexpected request type: {:?}", req.type_id());
            },
        )));

        let store = pd_client
            .clone()
            .map_region_to_store(MockPdClient::region1())
            .await?;

        let mut scan_lock = kvrpcpb::ScanLockRequest::default();
        scan_lock.start_key = vec![1];
        scan_lock.end_key = vec![1, 0];
        scan_lock.max_version = 0;
        scan_lock.limit = 1;

        let mut inner = Dispatch {
            request: scan_lock,
            kv_client: None,
        };
        inner.apply_store(&store)?;

        let plan = CleanupLocks {
            inner,
            ctx: ResolveLocksContext::default(),
            options: ResolveLocksOptions {
                async_commit_only: false,
                batch_size: 1,
            },
            store: Some(store),
            pd_client,
            keyspace: Keyspace::Disable,
            backoff: Backoff::no_backoff(),
            request_context: RequestContext::default(),
        };

        let result = plan.execute().await?;
        assert_eq!(result.resolved_locks, 1);
        assert_eq!(scan_lock_calls.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_cleanup_locks_async_commit_only_can_rollback_when_fallback_2pc() -> Result<()> {
        let check_txn_status_calls = Arc::new(AtomicUsize::new(0));
        let check_txn_status_calls_for_hook = check_txn_status_calls.clone();
        let resolve_lock_requests = Arc::new(Mutex::new(Vec::new()));
        let resolve_lock_requests_for_hook = resolve_lock_requests.clone();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req| {
                if req.downcast_ref::<kvrpcpb::ScanLockRequest>().is_some() {
                    let mut resp = kvrpcpb::ScanLockResponse::default();
                    resp.locks.push(kvrpcpb::LockInfo {
                        key: vec![1],
                        primary_lock: vec![1],
                        lock_version: 42,
                        lock_type: kvrpcpb::Op::Put as i32,
                        use_async_commit: true,
                        ..Default::default()
                    });
                    return Ok(Box::new(resp));
                }

                if req
                    .downcast_ref::<kvrpcpb::CheckSecondaryLocksRequest>()
                    .is_some()
                {
                    let mut resp = kvrpcpb::CheckSecondaryLocksResponse::default();
                    // Force `fallback_2pc=true` so the GC path re-checks txn status and can roll
                    // the txn back.
                    resp.locks.push(kvrpcpb::LockInfo {
                        use_async_commit: false,
                        ..Default::default()
                    });
                    return Ok(Box::new(resp));
                }

                if req
                    .downcast_ref::<kvrpcpb::CheckTxnStatusRequest>()
                    .is_some()
                {
                    let call = check_txn_status_calls_for_hook.fetch_add(1, Ordering::SeqCst);
                    if call == 0 {
                        let mut resp = kvrpcpb::CheckTxnStatusResponse::default();
                        resp.lock_ttl = 1;
                        resp.commit_version = 0;
                        resp.action = kvrpcpb::Action::NoAction as i32;
                        resp.lock_info = Some(kvrpcpb::LockInfo {
                            lock_version: 42,
                            use_async_commit: true,
                            secondaries: vec![vec![2]],
                            ..Default::default()
                        });
                        return Ok(Box::new(resp));
                    }
                    // Treat the txn as rolled back (no lock_info / commit_version=0).
                    return Ok(Box::new(kvrpcpb::CheckTxnStatusResponse::default()));
                }

                if let Some(req) = req.downcast_ref::<kvrpcpb::ResolveLockRequest>() {
                    if !req.txn_infos.is_empty() {
                        resolve_lock_requests_for_hook
                            .lock()
                            .unwrap()
                            .push(req.clone());
                    }
                    return Ok(Box::new(kvrpcpb::ResolveLockResponse::default()));
                }

                panic!("unexpected request type: {:?}", req.type_id());
            },
        )));

        let store = pd_client
            .clone()
            .map_region_to_store(MockPdClient::region1())
            .await?;

        let mut scan_lock = kvrpcpb::ScanLockRequest::default();
        scan_lock.start_key = vec![1];
        scan_lock.end_key = vec![10];
        scan_lock.max_version = 0;
        scan_lock.limit = 2;

        let mut inner = Dispatch {
            request: scan_lock,
            kv_client: None,
        };
        inner.apply_store(&store)?;

        let plan = CleanupLocks {
            inner,
            ctx: ResolveLocksContext::default(),
            options: ResolveLocksOptions {
                async_commit_only: true,
                // Stop after the first batch by making it larger than the mocked response size.
                batch_size: 2,
            },
            store: Some(store),
            pd_client,
            keyspace: Keyspace::Disable,
            backoff: Backoff::no_backoff(),
            request_context: RequestContext::default(),
        };

        let result = plan.execute().await?;
        assert_eq!(result.resolved_locks, 1);

        let requests = resolve_lock_requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].txn_infos.len(), 1);
        assert_eq!(requests[0].txn_infos[0].txn, 42);
        assert_eq!(requests[0].txn_infos[0].status, 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_cleanup_locks_retries_on_region_error_from_batch_resolve_lock() -> Result<()> {
        let resolve_lock_calls = Arc::new(AtomicUsize::new(0));
        let resolve_lock_calls_for_hook = resolve_lock_calls.clone();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req| {
                if req.downcast_ref::<kvrpcpb::ScanLockRequest>().is_some() {
                    let mut resp = kvrpcpb::ScanLockResponse::default();
                    resp.locks.push(kvrpcpb::LockInfo {
                        key: vec![1],
                        primary_lock: vec![1],
                        lock_version: 42,
                        lock_type: kvrpcpb::Op::Put as i32,
                        use_async_commit: true,
                        ..Default::default()
                    });
                    return Ok(Box::new(resp));
                }

                if req
                    .downcast_ref::<kvrpcpb::CheckTxnStatusRequest>()
                    .is_some()
                {
                    // Always treat the txn as rolled back so cleanup can proceed.
                    return Ok(Box::new(kvrpcpb::CheckTxnStatusResponse::default()));
                }

                if req.downcast_ref::<kvrpcpb::ResolveLockRequest>().is_some() {
                    let call = resolve_lock_calls_for_hook.fetch_add(1, Ordering::SeqCst);
                    if call == 0 {
                        let mut resp = kvrpcpb::ResolveLockResponse::default();
                        resp.region_error = Some(errorpb::Error::default());
                        return Ok(Box::new(resp));
                    }
                    return Ok(Box::new(kvrpcpb::ResolveLockResponse::default()));
                }

                panic!("unexpected request type: {:?}", req.type_id());
            },
        )));

        let mut scan_lock = kvrpcpb::ScanLockRequest::default();
        scan_lock.start_key = vec![1];
        scan_lock.end_key = vec![10];
        scan_lock.max_version = 0;
        scan_lock.limit = 2;

        let ctx = ResolveLocksContext::default();
        let options = ResolveLocksOptions {
            async_commit_only: true,
            // Stop after the first batch by making it larger than the mocked response size.
            batch_size: 2,
        };

        let plan =
            crate::request::PlanBuilder::new(pd_client.clone(), Keyspace::Disable, scan_lock)
                .cleanup_locks(ctx, options, Backoff::no_backoff(), Keyspace::Disable)
                // One retry with 0ms delay.
                .retry_multi_region(Backoff::no_jitter_backoff(0, 0, 1))
                .extract_error()
                .merge(crate::request::Collect)
                .plan();

        let result = plan.execute().await?;
        assert_eq!(result.resolved_locks, 1);
        assert_eq!(resolve_lock_calls.load(Ordering::SeqCst), 2);
        Ok(())
    }
}
