// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

mod batch_commands;
mod client;
mod errors;
mod health;
mod request;
pub(crate) mod safe_ts;

use std::cmp::max;
use std::cmp::min;
use std::sync::Arc;

use derive_new::new;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};

pub use self::client::KvClient;
pub use self::client::KvConnect;
pub use self::client::KvRpcClient;
pub use self::client::TikvConnect;
pub use self::errors::HasKeyErrors;
pub use self::errors::HasRegionError;
pub use self::errors::HasRegionErrors;
pub use self::errors::SetRegionError;
pub(crate) use self::health::StoreHealthMap;
pub use self::request::Request;
use crate::interceptor::ReplicaKind;
use crate::interceptor::RpcContextInfo;
use crate::pd::PdClient;
use crate::proto::kvrpcpb;
use crate::region::RegionWithLeader;
use crate::BoundRange;
use crate::Key;
use crate::RequestContext;
use crate::Result;

#[derive(new, Clone)]
pub struct RegionStore {
    pub region_with_leader: RegionWithLeader,
    pub client: Arc<dyn KvClient + Send + Sync>,
    #[new(default)]
    pub(crate) replica_read: bool,
    #[new(default)]
    pub(crate) stale_read: bool,
    #[new(default)]
    pub(crate) attempt: usize,
    #[new(default)]
    pub(crate) patched_request_source: Option<String>,
    #[new(default)]
    pub(crate) request_context: RequestContext,
    #[new(default)]
    pub(crate) replica_kind: Option<ReplicaKind>,
}

impl RegionStore {
    pub(crate) fn apply_to_request<R: Request>(&self, request: &mut R) -> Result<()> {
        request.set_leader(&self.region_with_leader)?;
        request.set_replica_read(self.replica_read);
        request.set_stale_read(self.stale_read);

        let rpc_context = RpcContextInfo {
            label: request.label(),
            attempt: self.attempt,
            region_id: Some(self.region_with_leader.region.id),
            store_id: self.region_with_leader.get_store_id().ok(),
            replica_kind: self.replica_kind,
            replica_read: self.replica_read,
            stale_read: self.stale_read,
        };
        let ctx = request.context_mut();
        ctx.is_retry_request = self.attempt > 0;
        if let Some(source) = &self.patched_request_source {
            ctx.request_source = source.clone();
        }
        if !self.request_context.has_resource_group_tag() {
            if let Some(tagger) = self.request_context.resource_group_tagger() {
                let tag = (tagger)(&rpc_context, ctx);
                ctx.resource_group_tag = tag;
            }
        }
        self.request_context
            .rpc_interceptors()
            .apply(&rpc_context, ctx);
        Ok(())
    }
}

#[derive(new, Clone)]
pub struct Store {
    pub client: Arc<dyn KvClient + Send + Sync>,
}

/// Maps keys to a stream of stores. `key_data` must be sorted in increasing order
pub fn region_stream_for_keys<K, KOut, PdC>(
    key_data: impl Iterator<Item = K> + Send + Sync + 'static,
    pd_client: Arc<PdC>,
) -> BoxStream<'static, Result<(Vec<KOut>, RegionWithLeader)>>
where
    PdC: PdClient,
    K: AsRef<Key> + Into<KOut> + Send + Sync + 'static,
    KOut: Send + Sync + 'static,
{
    pd_client.clone().group_keys_by_region(key_data)
}

#[allow(clippy::type_complexity)]
pub fn region_stream_for_range<PdC: PdClient>(
    range: (Vec<u8>, Vec<u8>),
    pd_client: Arc<PdC>,
) -> BoxStream<'static, Result<((Vec<u8>, Vec<u8>), RegionWithLeader)>> {
    let bnd_range = if range.1.is_empty() {
        BoundRange::range_from(range.0.clone().into())
    } else {
        BoundRange::from(range.clone())
    };
    pd_client
        .regions_for_range(bnd_range)
        .map_ok(move |region| {
            let region_range = region.range();
            let result_range = range_intersection(
                region_range,
                (range.0.clone().into(), range.1.clone().into()),
            );
            ((result_range.0.into(), result_range.1.into()), region)
        })
        .boxed()
}

/// The range used for request should be the intersection of `region_range` and `range`.
fn range_intersection(region_range: (Key, Key), range: (Key, Key)) -> (Key, Key) {
    let (lower, upper) = region_range;
    let up = if upper.is_empty() {
        range.1
    } else if range.1.is_empty() {
        upper
    } else {
        min(upper, range.1)
    };
    (max(lower, range.0), up)
}

pub fn region_stream_for_ranges<PdC: PdClient>(
    ranges: Vec<kvrpcpb::KeyRange>,
    pd_client: Arc<PdC>,
) -> BoxStream<'static, Result<(Vec<kvrpcpb::KeyRange>, RegionWithLeader)>> {
    pd_client.clone().group_ranges_by_region(ranges)
}

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use async_trait::async_trait;
    use tonic::transport::Channel;

    use super::*;
    use crate::interceptor::{rpc_interceptor, ResourceGroupTagger, RpcInterceptorChain};
    use crate::mock::{MockKvClient, MockPdClient};
    use crate::proto::kvrpcpb;
    use crate::proto::tikvpb::tikv_client::TikvClient;

    #[derive(Default)]
    struct TestRequest {
        leader_called: bool,
        replica_read: bool,
        stale_read: bool,
        context: Option<kvrpcpb::Context>,
    }

    #[async_trait]
    impl Request for TestRequest {
        async fn dispatch(
            &self,
            _client: &TikvClient<Channel>,
            _timeout: Duration,
        ) -> Result<Box<dyn Any + Send>> {
            unreachable!("dispatch not used in RegionStore::apply_to_request tests");
        }

        fn label(&self) -> &'static str {
            "test_request"
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn context_mut(&mut self) -> &mut kvrpcpb::Context {
            self.context.get_or_insert_with(kvrpcpb::Context::default)
        }

        fn set_leader(&mut self, leader: &RegionWithLeader) -> Result<()> {
            self.leader_called = true;
            let ctx = self.context_mut();
            ctx.region_id = leader.region.id;
            ctx.region_epoch = leader.region.region_epoch.clone();
            ctx.peer = leader.leader.clone();
            Ok(())
        }

        fn set_api_version(&mut self, api_version: kvrpcpb::ApiVersion) {
            let ctx = self.context_mut();
            ctx.api_version = api_version.into();
        }

        fn set_replica_read(&mut self, replica_read: bool) {
            self.replica_read = replica_read;
            let ctx = self.context_mut();
            ctx.replica_read = replica_read;
        }

        fn set_stale_read(&mut self, stale_read: bool) {
            self.stale_read = stale_read;
            let ctx = self.context_mut();
            ctx.stale_read = stale_read;
        }
    }

    #[test]
    fn region_store_apply_sets_retry_flag_and_patched_request_source() {
        let region = MockPdClient::region1();
        let mut store = RegionStore::new(
            region,
            Arc::new(MockKvClient::default()) as Arc<dyn KvClient + Send + Sync>,
        );
        store.attempt = 1;
        store.patched_request_source = Some("patched".to_owned());
        store.replica_read = true;

        let request_context = RequestContext::default().with_request_source("base");
        store.request_context = request_context.clone();

        let mut req = request_context.apply_to(TestRequest::default());
        store.apply_to_request(&mut req).unwrap();

        assert!(req.leader_called);
        assert!(req.replica_read);
        assert!(!req.stale_read);

        let ctx = req.context.expect("context must be initialized");
        assert!(ctx.is_retry_request);
        assert_eq!(ctx.request_source, "patched");
        assert_eq!(ctx.region_id, 1);
        assert_eq!(ctx.peer.as_ref().unwrap().store_id, 41);
    }

    #[test]
    fn region_store_apply_invokes_resource_group_tagger_only_when_tag_unset() {
        let region = MockPdClient::region1();
        let base_store = RegionStore::new(
            region,
            Arc::new(MockKvClient::default()) as Arc<dyn KvClient + Send + Sync>,
        );

        let calls = Arc::new(AtomicUsize::new(0));
        let calls_cloned = calls.clone();
        let tagger: ResourceGroupTagger = Arc::new(move |info, ctx| {
            calls_cloned.fetch_add(1, Ordering::SeqCst);
            assert_eq!(info.label, "test_request");
            assert_eq!(info.region_id, Some(1));
            assert_eq!(ctx.region_id, 1);
            vec![7, 8, 9]
        });

        // No fixed tag -> tagger runs.
        {
            let mut store = base_store.clone();
            let request_context =
                RequestContext::default().with_resource_group_tagger(tagger.clone());
            store.request_context = request_context.clone();

            let mut req = request_context.apply_to(TestRequest::default());
            store.apply_to_request(&mut req).unwrap();

            let ctx = req.context.expect("context must be initialized");
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            assert_eq!(ctx.resource_group_tag, vec![7, 8, 9]);
        }

        // Fixed tag -> tagger must not run.
        {
            calls.store(0, Ordering::SeqCst);

            let mut store = base_store;
            let request_context = RequestContext::default()
                .with_resource_group_tag(vec![1, 2, 3])
                .with_resource_group_tagger(tagger);
            store.request_context = request_context.clone();

            let mut req = request_context.apply_to(TestRequest::default());
            store.apply_to_request(&mut req).unwrap();

            let ctx = req.context.expect("context must be initialized");
            assert_eq!(calls.load(Ordering::SeqCst), 0);
            assert_eq!(ctx.resource_group_tag, vec![1, 2, 3]);
        }
    }

    #[test]
    fn region_store_apply_runs_rpc_interceptors_with_context_info() {
        let region = MockPdClient::region1();
        let mut store = RegionStore::new(
            region,
            Arc::new(MockKvClient::default()) as Arc<dyn KvClient + Send + Sync>,
        );
        store.attempt = 2;
        store.replica_read = true;
        store.stale_read = true;

        let seen = Arc::new(Mutex::new(Vec::new()));
        let seen_cloned = seen.clone();
        let interceptor = rpc_interceptor("capture", move |info, ctx| {
            seen_cloned.lock().unwrap_or_else(|e| e.into_inner()).push((
                info.label.to_owned(),
                info.attempt,
                info.region_id,
                info.store_id,
                info.replica_read,
                info.stale_read,
                ctx.is_retry_request,
                ctx.request_source.clone(),
                ctx.replica_read,
                ctx.stale_read,
            ));
        });
        let mut chain = RpcInterceptorChain::new();
        chain.link(interceptor);

        let request_context = RequestContext::default()
            .with_request_source("src")
            .with_rpc_interceptors(chain);
        store.request_context = request_context.clone();

        let mut req = request_context.apply_to(TestRequest::default());
        store.apply_to_request(&mut req).unwrap();

        let seen = seen.lock().unwrap_or_else(|e| e.into_inner());
        assert_eq!(seen.len(), 1);
        assert_eq!(
            seen[0],
            (
                "test_request".to_owned(),
                2,
                Some(1),
                Some(41),
                true,
                true,
                true,
                "src".to_owned(),
                true,
                true,
            )
        );
    }
}
