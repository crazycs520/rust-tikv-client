// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use super::plan::PreserveShard;
use super::Keyspace;
use crate::backoff::Backoff;
use crate::pd::PdClient;
use crate::request::plan::{CleanupLocks, RetryableAllStores};
use crate::request::shard::HasNextBatch;
use crate::request::Dispatch;
use crate::request::ExtractError;
use crate::request::KvRequest;
use crate::request::Merge;
use crate::request::MergeResponse;
use crate::request::NextBatch;
use crate::request::Plan;
use crate::request::Process;
use crate::request::ProcessResponse;
use crate::request::ReadRouting;
use crate::request::ResolveLock;
use crate::request::RetryableMultiRegion;
use crate::request::Shardable;
use crate::request::{DefaultProcessor, StoreRequest};
use crate::store::HasKeyErrors;
use crate::store::HasRegionError;
use crate::store::HasRegionErrors;
use crate::store::RegionStore;
use crate::store::SetRegionError;
use crate::transaction::HasLocks;
use crate::transaction::ResolveLocksContext;
use crate::transaction::ResolveLocksOptions;
use crate::CommandPriority;
use crate::DiskFullOpt;
use crate::ReplicaReadType;
use crate::RequestContext;
use crate::Result;

/// Builder type for plans (see that module for more).
pub struct PlanBuilder<PdC: PdClient, P: Plan, Ph: PlanBuilderPhase> {
    pd_client: Arc<PdC>,
    plan: P,
    request_context: RequestContext,
    read_routing: ReadRouting,
    phantom: PhantomData<Ph>,
}

/// Used to ensure that a plan has a designated target or targets, a target is
/// a particular TiKV server.
pub trait PlanBuilderPhase {}
pub struct NoTarget;
impl PlanBuilderPhase for NoTarget {}
pub struct Targetted;
impl PlanBuilderPhase for Targetted {}

impl<PdC: PdClient, Req: KvRequest> PlanBuilder<PdC, Dispatch<Req>, NoTarget> {
    pub fn new(pd_client: Arc<PdC>, keyspace: Keyspace, mut request: Req) -> Self {
        request.set_api_version(keyspace.api_version());
        // Mirror the default client behavior by ensuring a non-empty request_source, but do not
        // override an explicitly-set one (e.g. applied by `RawClient` / `TransactionClient`).
        {
            let ctx = request.context_mut();
            if ctx.request_source.is_empty() {
                ctx.request_source = "unknown".to_owned();
            }
        }
        let request_context = RequestContext::default();
        let store_health = pd_client.store_health();
        PlanBuilder {
            pd_client,
            plan: Dispatch {
                request,
                kv_client: None,
            },
            request_context,
            read_routing: ReadRouting::default().with_store_health(store_health),
            phantom: PhantomData,
        }
    }

    /// Set `kvrpcpb::Context.request_source` for all requests executed by this plan.
    #[must_use]
    pub fn with_request_source(mut self, source: impl Into<String>) -> Self {
        self.request_context = self.request_context.with_request_source(source);
        self.plan.request = self.request_context.apply_to(self.plan.request);
        self.read_routing = self
            .read_routing
            .clone()
            .with_request_source(self.request_context.request_source());
        self
    }

    /// Set `kvrpcpb::Context.resource_group_tag` for all requests executed by this plan.
    #[must_use]
    pub fn with_resource_group_tag(mut self, tag: impl Into<Vec<u8>>) -> Self {
        self.request_context = self.request_context.with_resource_group_tag(tag);
        self.plan.request = self.request_context.apply_to(self.plan.request);
        self
    }

    /// Set `kvrpcpb::Context.resource_control_context.resource_group_name` for all requests executed by this plan.
    #[must_use]
    pub fn with_resource_group_name(mut self, name: impl Into<String>) -> Self {
        self.request_context = self.request_context.with_resource_group_name(name);
        self.plan.request = self.request_context.apply_to(self.plan.request);
        self
    }

    /// Set `kvrpcpb::Context.priority` for all requests executed by this plan.
    #[must_use]
    pub fn with_priority(mut self, priority: CommandPriority) -> Self {
        self.request_context = self.request_context.with_priority(priority);
        self.plan.request = self.request_context.apply_to(self.plan.request);
        self
    }

    /// Set `kvrpcpb::Context.disk_full_opt` for all requests executed by this plan.
    #[must_use]
    pub fn with_disk_full_opt(mut self, disk_full_opt: DiskFullOpt) -> Self {
        self.request_context = self.request_context.with_disk_full_opt(disk_full_opt);
        self.plan.request = self.request_context.apply_to(self.plan.request);
        self
    }

    /// Set `kvrpcpb::Context.txn_source` for all requests executed by this plan.
    #[must_use]
    pub fn with_txn_source(mut self, txn_source: u64) -> Self {
        self.request_context = self.request_context.with_txn_source(txn_source);
        self.plan.request = self.request_context.apply_to(self.plan.request);
        self
    }

    /// Set `kvrpcpb::Context.trace_id` for all requests executed by this plan.
    #[must_use]
    pub fn with_trace_id(mut self, trace_id: impl Into<Vec<u8>>) -> Self {
        self.request_context = self.request_context.with_trace_id(trace_id);
        self.plan.request = self.request_context.apply_to(self.plan.request);
        self
    }

    /// Set `kvrpcpb::Context.trace_control_flags` for all requests executed by this plan.
    #[must_use]
    pub fn with_trace_control_flags(mut self, flags: u64) -> Self {
        self.request_context = self.request_context.with_trace_control_flags(flags);
        self.plan.request = self.request_context.apply_to(self.plan.request);
        self
    }

    /// Set `kvrpcpb::Context.trace_control_flags` for all requests executed by this plan.
    #[must_use]
    pub fn with_trace_control(mut self, flags: crate::trace::TraceControlFlags) -> Self {
        self.request_context = self.request_context.with_trace_control(flags);
        self.plan.request = self.request_context.apply_to(self.plan.request);
        self
    }

    /// Set `kvrpcpb::Context.resource_control_context.override_priority` for all requests executed by this plan.
    #[must_use]
    pub fn with_resource_control_override_priority(mut self, override_priority: u64) -> Self {
        self.request_context = self
            .request_context
            .with_resource_control_override_priority(override_priority);
        self.plan.request = self.request_context.apply_to(self.plan.request);
        self
    }

    /// Set `kvrpcpb::Context.resource_control_context.penalty` for all requests executed by this plan.
    #[must_use]
    pub fn with_resource_control_penalty(
        mut self,
        penalty: impl Into<crate::resource_manager::Consumption>,
    ) -> Self {
        self.request_context = self.request_context.with_resource_control_penalty(penalty);
        self.plan.request = self.request_context.apply_to(self.plan.request);
        self
    }

    /// Set a resource group tagger for all requests executed by this plan.
    ///
    /// If a fixed resource group tag is set via [`with_resource_group_tag`](Self::with_resource_group_tag),
    /// it takes precedence over this tagger.
    #[must_use]
    pub fn with_resource_group_tagger(
        mut self,
        tagger: impl Fn(&crate::interceptor::RpcContextInfo, &crate::kvrpcpb::Context) -> Vec<u8>
            + Send
            + Sync
            + 'static,
    ) -> Self {
        self.request_context = self
            .request_context
            .with_resource_group_tagger(Arc::new(tagger));
        self
    }

    /// Replace the RPC interceptor chain for all requests executed by this plan.
    #[must_use]
    pub fn with_rpc_interceptor(
        mut self,
        interceptor: Arc<dyn crate::interceptor::RpcInterceptor>,
    ) -> Self {
        let mut chain = crate::interceptor::RpcInterceptorChain::new();
        chain.link(interceptor);
        self.request_context = self.request_context.with_rpc_interceptors(chain);
        self
    }

    /// Add an RPC interceptor for all requests executed by this plan.
    ///
    /// If another interceptor with the same name exists, it is replaced.
    #[must_use]
    pub fn with_added_rpc_interceptor(
        mut self,
        interceptor: Arc<dyn crate::interceptor::RpcInterceptor>,
    ) -> Self {
        self.request_context = self.request_context.add_rpc_interceptor(interceptor);
        self
    }

    /// Configure where snapshot reads are sent (leader/follower/learner...).
    #[must_use]
    pub fn replica_read(mut self, replica_read: ReplicaReadType) -> Self {
        self.read_routing.set_replica_read(replica_read);
        self
    }

    /// Enable/disable *stale reads* for this plan.
    #[must_use]
    pub fn stale_read(mut self, stale_read: bool) -> Self {
        self.read_routing.set_stale_read(stale_read);
        self
    }

    /// Configure the deterministic seed for replica selection (when applicable).
    #[must_use]
    pub fn replica_read_seed(mut self, seed: u32) -> Self {
        self.read_routing.set_seed(seed);
        self
    }
}

impl<PdC: PdClient, P: Plan> PlanBuilder<PdC, P, Targetted> {
    /// Return the built plan, note that this can only be called once the plan
    /// has a target.
    pub fn plan(self) -> P {
        self.plan
    }
}

impl<PdC: PdClient, P: Plan, Ph: PlanBuilderPhase> PlanBuilder<PdC, P, Ph> {
    pub(crate) fn with_request_context(mut self, request_context: RequestContext) -> Self {
        self.request_context = request_context;
        self
    }

    pub(crate) fn with_read_routing(mut self, read_routing: ReadRouting) -> Self {
        self.read_routing = read_routing;
        self
    }

    /// If there is a lock error, then resolve the lock and retry the request.
    pub fn resolve_lock(
        self,
        backoff: Backoff,
        keyspace: Keyspace,
    ) -> PlanBuilder<PdC, ResolveLock<P, PdC>, Ph>
    where
        P::Result: HasLocks + Default + SetRegionError,
    {
        self.resolve_lock_with_timeout(backoff, keyspace, None)
    }

    pub fn resolve_lock_with_timeout(
        self,
        backoff: Backoff,
        keyspace: Keyspace,
        lock_wait_timeout: Option<Duration>,
    ) -> PlanBuilder<PdC, ResolveLock<P, PdC>, Ph>
    where
        P::Result: HasLocks + Default + SetRegionError,
    {
        PlanBuilder {
            pd_client: self.pd_client.clone(),
            plan: ResolveLock {
                inner: self.plan,
                pd_client: self.pd_client,
                backoff,
                lock_wait_timeout,
                keyspace,
                request_context: self.request_context.clone(),
                read_routing: self.read_routing.clone(),
            },
            request_context: self.request_context,
            read_routing: self.read_routing,
            phantom: PhantomData,
        }
    }

    pub fn cleanup_locks(
        self,
        ctx: ResolveLocksContext,
        options: ResolveLocksOptions,
        backoff: Backoff,
        keyspace: Keyspace,
    ) -> PlanBuilder<PdC, CleanupLocks<P, PdC>, Ph>
    where
        P: Shardable + NextBatch,
        P::Result: HasLocks + HasNextBatch + HasRegionError + HasKeyErrors,
    {
        PlanBuilder {
            pd_client: self.pd_client.clone(),
            plan: CleanupLocks {
                inner: self.plan,
                ctx,
                options,
                store: None,
                backoff,
                pd_client: self.pd_client,
                keyspace,
                request_context: self.request_context.clone(),
            },
            request_context: self.request_context,
            read_routing: self.read_routing,
            phantom: PhantomData,
        }
    }

    /// Merge the results of a request. Usually used where a request is sent to multiple regions
    /// to combine the responses from each region.
    pub fn merge<In, M: Merge<In>>(self, merge: M) -> PlanBuilder<PdC, MergeResponse<P, In, M>, Ph>
    where
        In: Clone + Send + Sync + 'static,
        P: Plan<Result = Vec<Result<In>>>,
    {
        PlanBuilder {
            pd_client: self.pd_client.clone(),
            plan: MergeResponse {
                inner: self.plan,
                merge,
                phantom: PhantomData,
            },
            request_context: self.request_context,
            read_routing: self.read_routing,
            phantom: PhantomData,
        }
    }

    /// Apply the default processing step to a response (usually only needed if the request is sent
    /// to a single region because post-porcessing can be incorporated in the merge step for
    /// multi-region requests).
    pub fn post_process_default(self) -> PlanBuilder<PdC, ProcessResponse<P, DefaultProcessor>, Ph>
    where
        P: Plan,
        DefaultProcessor: Process<P::Result>,
    {
        PlanBuilder {
            pd_client: self.pd_client.clone(),
            plan: ProcessResponse {
                inner: self.plan,
                processor: DefaultProcessor,
            },
            request_context: self.request_context,
            read_routing: self.read_routing,
            phantom: PhantomData,
        }
    }
}

impl<PdC: PdClient, P: Plan + Shardable> PlanBuilder<PdC, P, NoTarget>
where
    P::Result: HasKeyErrors + HasRegionError,
{
    /// Split the request into shards sending a request to the region of each shard.
    pub fn retry_multi_region(
        self,
        backoff: Backoff,
    ) -> PlanBuilder<PdC, RetryableMultiRegion<P, PdC>, Targetted> {
        self.make_retry_multi_region(
            backoff,
            false,
            crate::request::plan::MULTI_REGION_CONCURRENCY,
        )
    }

    /// Split the request into shards with a custom per-request concurrency limit.
    ///
    /// This is useful for protocols that want to cap their own fan-out (e.g. pipelined flush).
    pub fn retry_multi_region_with_concurrency(
        self,
        backoff: Backoff,
        concurrency: usize,
    ) -> PlanBuilder<PdC, RetryableMultiRegion<P, PdC>, Targetted> {
        self.make_retry_multi_region(backoff, false, concurrency)
    }

    /// Preserve all results, even some of them are Err.
    /// To pass all responses to merge, and handle partial successful results correctly.
    pub fn retry_multi_region_preserve_results(
        self,
        backoff: Backoff,
    ) -> PlanBuilder<PdC, RetryableMultiRegion<P, PdC>, Targetted> {
        self.make_retry_multi_region(
            backoff,
            true,
            crate::request::plan::MULTI_REGION_CONCURRENCY,
        )
    }

    /// Preserve all results with a custom per-request concurrency limit.
    pub fn retry_multi_region_preserve_results_with_concurrency(
        self,
        backoff: Backoff,
        concurrency: usize,
    ) -> PlanBuilder<PdC, RetryableMultiRegion<P, PdC>, Targetted> {
        self.make_retry_multi_region(backoff, true, concurrency)
    }

    fn make_retry_multi_region(
        self,
        backoff: Backoff,
        preserve_region_results: bool,
        concurrency: usize,
    ) -> PlanBuilder<PdC, RetryableMultiRegion<P, PdC>, Targetted> {
        PlanBuilder {
            pd_client: self.pd_client.clone(),
            plan: RetryableMultiRegion {
                inner: self.plan,
                pd_client: self.pd_client,
                backoff,
                concurrency,
                preserve_region_results,
                request_context: self.request_context.clone(),
                read_routing: self.read_routing.clone(),
            },
            request_context: self.request_context,
            read_routing: self.read_routing,
            phantom: PhantomData,
        }
    }
}

impl<PdC: PdClient, R: KvRequest> PlanBuilder<PdC, Dispatch<R>, NoTarget> {
    /// Target the request at a single region; caller supplies the store to target.
    pub async fn single_region_with_store(
        self,
        store: RegionStore,
    ) -> Result<PlanBuilder<PdC, Dispatch<R>, Targetted>> {
        set_single_region_store(
            self.plan,
            store,
            self.pd_client,
            self.request_context,
            self.read_routing,
        )
    }
}

impl<PdC: PdClient, P: Plan + StoreRequest> PlanBuilder<PdC, P, NoTarget>
where
    P::Result: HasKeyErrors + HasRegionError,
{
    pub fn all_stores(
        self,
        backoff: Backoff,
    ) -> PlanBuilder<PdC, RetryableAllStores<P, PdC>, Targetted> {
        PlanBuilder {
            pd_client: self.pd_client.clone(),
            plan: RetryableAllStores {
                inner: self.plan,
                pd_client: self.pd_client,
                backoff,
            },
            request_context: self.request_context,
            read_routing: self.read_routing,
            phantom: PhantomData,
        }
    }
}

impl<PdC: PdClient, P: Plan + Shardable> PlanBuilder<PdC, P, NoTarget>
where
    P::Result: HasKeyErrors,
{
    pub fn preserve_shard(self) -> PlanBuilder<PdC, PreserveShard<P>, NoTarget> {
        PlanBuilder {
            pd_client: self.pd_client.clone(),
            plan: PreserveShard {
                inner: self.plan,
                shard: None,
            },
            request_context: self.request_context,
            read_routing: self.read_routing,
            phantom: PhantomData,
        }
    }
}

impl<PdC: PdClient, P: Plan> PlanBuilder<PdC, P, Targetted>
where
    P::Result: HasKeyErrors + HasRegionErrors,
{
    pub fn extract_error(self) -> PlanBuilder<PdC, ExtractError<P>, Targetted> {
        PlanBuilder {
            pd_client: self.pd_client,
            plan: ExtractError { inner: self.plan },
            request_context: self.request_context,
            read_routing: self.read_routing,
            phantom: self.phantom,
        }
    }
}

fn set_single_region_store<PdC: PdClient, R: KvRequest>(
    mut plan: Dispatch<R>,
    store: RegionStore,
    pd_client: Arc<PdC>,
    request_context: RequestContext,
    read_routing: ReadRouting,
) -> Result<PlanBuilder<PdC, Dispatch<R>, Targetted>> {
    store.apply_to_request(&mut plan.request)?;
    plan.kv_client = Some(store.client);
    Ok(PlanBuilder {
        plan,
        pd_client,
        request_context,
        read_routing,
        phantom: PhantomData,
    })
}

/// Indicates that a request operates on a single key.
pub trait SingleKey {
    #[allow(clippy::ptr_arg)]
    fn key(&self) -> &Vec<u8>;
}

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    use super::*;
    use crate::mock::MockKvClient;
    use crate::mock::MockPdClient;
    use crate::proto::kvrpcpb;
    use crate::request::CollectSingle;
    use crate::trace::TraceControlFlags;
    use crate::Backoff;
    use crate::Result;

    #[tokio::test]
    async fn test_plan_builder_request_context_and_interceptors() -> Result<()> {
        let expected_source = "unit-test".to_owned();
        let expected_group_name = "unit-test-group".to_owned();
        let expected_dynamic_tag = vec![9_u8];
        let expected_trace_id = vec![1_u8, 2, 3, 4];
        let expected_trace_flags = 0x1234_u64;

        let interceptor_called = Arc::new(AtomicBool::new(false));
        let interceptor_called_cloned = interceptor_called.clone();

        let interceptor =
            crate::interceptor::rpc_interceptor("unit_test.interceptor", move |info, ctx| {
                assert_eq!(info.label, "raw_get");
                interceptor_called_cloned.store(true, Ordering::SeqCst);
                // Interceptors run after `RequestContext` fields have been applied.
                ctx.request_source = "from-interceptor".to_owned();
            });

        let hook_expected_group_name = expected_group_name.clone();
        let hook_expected_dynamic_tag = expected_dynamic_tag.clone();
        let hook_expected_trace_id = expected_trace_id.clone();
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let Some(req) = req.downcast_ref::<kvrpcpb::RawGetRequest>() else {
                    unreachable!("unexpected request type in plan builder context test");
                };
                let ctx = req.context.as_ref().expect("context should be set");
                assert_eq!(ctx.request_source, "from-interceptor");
                assert_eq!(ctx.resource_group_tag, hook_expected_dynamic_tag);
                assert_eq!(ctx.trace_id, hook_expected_trace_id);
                assert_eq!(ctx.trace_control_flags, expected_trace_flags);
                let resource_ctl_ctx = ctx
                    .resource_control_context
                    .as_ref()
                    .expect("resource_control_context should be set");
                assert_eq!(
                    resource_ctl_ctx.resource_group_name,
                    hook_expected_group_name
                );

                let resp = kvrpcpb::RawGetResponse {
                    not_found: true,
                    ..Default::default()
                };
                Ok(Box::new(resp))
            },
        )));

        let mut req = kvrpcpb::RawGetRequest::default();
        // Choose a key in `MockPdClient::region1`.
        req.key = vec![5];

        let plan = PlanBuilder::new(pd_client, Keyspace::Disable, req)
            .with_request_source(expected_source.clone())
            .with_resource_group_name(expected_group_name.clone())
            .with_trace_id(expected_trace_id)
            .with_trace_control_flags(expected_trace_flags)
            .with_resource_group_tagger(move |info, ctx| {
                // Tagger runs before interceptors.
                assert_eq!(info.label, "raw_get");
                assert_eq!(ctx.request_source, format!("leader_{expected_source}"));
                expected_dynamic_tag.clone()
            })
            .with_added_rpc_interceptor(interceptor)
            .retry_multi_region(Backoff::no_backoff())
            .merge(CollectSingle)
            .post_process_default()
            .plan();

        let value = plan.execute().await?;
        assert!(value.is_none());
        assert!(interceptor_called.load(Ordering::SeqCst));
        Ok(())
    }

    #[tokio::test]
    async fn test_plan_builder_context_setters_and_routing_knobs() -> Result<()> {
        let expected_source = "unit-test2".to_owned();
        let expected_group_name = "unit-test-group2".to_owned();
        let expected_tag = vec![1_u8, 2, 3];
        let expected_trace_id = vec![7_u8, 7, 7];
        let expected_trace_flags = 0x42_u64;
        let expected_override_priority = 16_u64;

        let interceptor1_called = Arc::new(AtomicBool::new(false));
        let interceptor2_called = Arc::new(AtomicBool::new(false));

        let interceptor1_called_cloned = interceptor1_called.clone();
        let interceptor2_called_cloned = interceptor2_called.clone();
        let interceptor1 =
            crate::interceptor::rpc_interceptor("unit_test.interceptor1", move |info, _| {
                assert_eq!(info.label, "raw_get");
                interceptor1_called_cloned.store(true, Ordering::SeqCst);
            });
        let interceptor2 =
            crate::interceptor::rpc_interceptor("unit_test.interceptor2", move |info, _| {
                assert_eq!(info.label, "raw_get");
                interceptor2_called_cloned.store(true, Ordering::SeqCst);
            });

        let expected_source_for_hook = expected_source.clone();
        let expected_tag_for_hook = expected_tag.clone();
        let expected_group_name_for_hook = expected_group_name.clone();
        let expected_trace_id_for_hook = expected_trace_id.clone();
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let Some(req) = req.downcast_ref::<kvrpcpb::RawGetRequest>() else {
                    unreachable!("unexpected request type in plan builder setters test");
                };
                let ctx = req.context.as_ref().expect("context should be set");
                assert!(
                    ctx.request_source.ends_with(&expected_source_for_hook),
                    "unexpected request_source: {}",
                    ctx.request_source
                );
                assert_eq!(ctx.resource_group_tag, expected_tag_for_hook);
                assert_eq!(ctx.trace_id, expected_trace_id_for_hook);
                assert_eq!(ctx.trace_control_flags, expected_trace_flags);
                assert_eq!(ctx.priority, i32::from(CommandPriority::High));
                assert_eq!(
                    ctx.disk_full_opt,
                    i32::from(DiskFullOpt::AllowedOnAlmostFull)
                );
                assert_eq!(ctx.txn_source, 7);
                let resource_ctl_ctx = ctx
                    .resource_control_context
                    .as_ref()
                    .expect("resource_control_context should be set");
                assert_eq!(
                    resource_ctl_ctx.resource_group_name,
                    expected_group_name_for_hook
                );
                assert_eq!(
                    resource_ctl_ctx.override_priority,
                    expected_override_priority
                );
                assert!(resource_ctl_ctx.penalty.is_some());

                let resp = kvrpcpb::RawGetResponse {
                    not_found: true,
                    ..Default::default()
                };
                Ok(Box::new(resp))
            },
        )));

        let mut req = kvrpcpb::RawGetRequest::default();
        // Choose a key in `MockPdClient::region1`.
        req.key = vec![5];

        let plan = PlanBuilder::new(pd_client.clone(), Keyspace::Disable, req)
            .with_request_source(expected_source)
            .with_resource_group_tag(expected_tag)
            .with_resource_group_name(expected_group_name)
            .with_priority(CommandPriority::High)
            .with_disk_full_opt(DiskFullOpt::AllowedOnAlmostFull)
            .with_txn_source(7)
            .with_trace_id(expected_trace_id)
            .with_trace_control(TraceControlFlags(expected_trace_flags))
            .with_resource_control_override_priority(expected_override_priority)
            .with_resource_control_penalty(crate::resource_manager::Consumption::default())
            .with_rpc_interceptor(interceptor1)
            .with_added_rpc_interceptor(interceptor2)
            .replica_read(ReplicaReadType::Follower)
            .stale_read(true)
            .replica_read_seed(42)
            .retry_multi_region_with_concurrency(Backoff::no_backoff(), 1)
            .merge(CollectSingle)
            .post_process_default()
            .plan();

        let value = plan.execute().await?;
        assert!(value.is_none());
        assert!(interceptor1_called.load(Ordering::SeqCst));
        assert!(interceptor2_called.load(Ordering::SeqCst));

        // Also exercise the preserve-results variant (it should behave identically on success).
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let Some(_) = req.downcast_ref::<kvrpcpb::RawGetRequest>() else {
                    unreachable!("unexpected request type in preserve-results test");
                };
                Ok(Box::new(kvrpcpb::RawGetResponse {
                    not_found: true,
                    ..Default::default()
                }))
            },
        )));
        let mut req = kvrpcpb::RawGetRequest::default();
        req.key = vec![5];
        let plan = PlanBuilder::new(pd_client, Keyspace::Disable, req)
            .with_request_source("preserve-results")
            .retry_multi_region_preserve_results_with_concurrency(Backoff::no_backoff(), 1)
            .merge(CollectSingle)
            .plan();

        let _ = plan.execute().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_plan_builder_retry_increments_attempt_and_patches_request_source() -> Result<()> {
        use crate::proto::errorpb;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let expected_source = "unit-test-retry".to_owned();
        let dispatches = Arc::new(AtomicUsize::new(0));
        let dispatches_cloned = dispatches.clone();

        let expected_source_for_hook = expected_source.clone();
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let Some(req) = req.downcast_ref::<kvrpcpb::RawGetRequest>() else {
                    unreachable!("unexpected request type in retry attempt test");
                };
                let ctx = req.context.as_ref().expect("context should be set");

                let attempt = dispatches_cloned.fetch_add(1, Ordering::SeqCst);
                match attempt {
                    0 => {
                        assert!(!ctx.is_retry_request);
                        assert_eq!(
                            ctx.request_source,
                            format!("leader_{expected_source_for_hook}")
                        );
                        let mut resp = kvrpcpb::RawGetResponse::default();
                        resp.region_error = Some(errorpb::Error::default());
                        Ok(Box::new(resp))
                    }
                    1 => {
                        assert!(ctx.is_retry_request);
                        assert_eq!(
                            ctx.request_source,
                            format!("retry_leader_leader_{expected_source_for_hook}")
                        );
                        Ok(Box::new(kvrpcpb::RawGetResponse {
                            not_found: true,
                            ..Default::default()
                        }))
                    }
                    other => unreachable!("unexpected dispatch attempt {other}"),
                }
            },
        )));

        let mut req = kvrpcpb::RawGetRequest::default();
        // Choose a key in `MockPdClient::region1`.
        req.key = vec![5];

        let plan = PlanBuilder::new(pd_client, Keyspace::Disable, req)
            .with_request_source(expected_source)
            .retry_multi_region(Backoff::no_jitter_backoff(0, 0, 1))
            .merge(CollectSingle)
            .post_process_default()
            .plan();

        let value = plan.execute().await?;
        assert!(value.is_none());
        assert_eq!(dispatches.load(Ordering::SeqCst), 2);
        Ok(())
    }
}
