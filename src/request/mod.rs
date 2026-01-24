// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use async_trait::async_trait;
use derive_new::new;

pub use self::keyspace::EncodeKeyspace;
pub use self::keyspace::KeyMode;
pub use self::keyspace::Keyspace;
pub use self::keyspace::TruncateKeyspace;
pub use self::plan::Collect;
pub use self::plan::CollectError;
pub use self::plan::CollectSingle;
pub use self::plan::CollectWithShard;
pub use self::plan::DefaultProcessor;
pub use self::plan::Dispatch;
pub use self::plan::ExtractError;
pub use self::plan::Merge;
pub use self::plan::MergeResponse;
pub use self::plan::Plan;
pub use self::plan::Process;
pub use self::plan::ProcessResponse;
pub use self::plan::ResolveLock;
pub use self::plan::ResponseWithShard;
pub use self::plan::RetryableMultiRegion;
pub use self::plan_builder::PlanBuilder;
pub use self::plan_builder::SingleKey;
pub(crate) use self::read_routing::ReadRouting;
pub use self::shard::Batchable;
pub use self::shard::HasNextBatch;
pub use self::shard::NextBatch;
pub use self::shard::RangeRequest;
pub use self::shard::Shardable;
use crate::backoff::Backoff;
use crate::backoff::DEFAULT_REGION_BACKOFF;
use crate::backoff::OPTIMISTIC_BACKOFF;
use crate::backoff::PESSIMISTIC_BACKOFF;
use crate::store::Request;
use crate::store::{HasKeyErrors, Store};
use crate::transaction::HasLocks;

mod keyspace;
#[cfg(test)]
mod metrics_collector;
mod pending_backoff;
pub mod plan;
mod plan_builder;
mod read_routing;
mod shard;

/// Abstracts any request sent to a TiKV server.
#[async_trait]
pub trait KvRequest: Request + Sized + Clone + Sync + Send + 'static {
    /// The expected response to the request.
    type Response: HasKeyErrors + HasLocks + Clone + Send + 'static;
}

impl KvRequest for crate::proto::coprocessor::Request {
    type Response = crate::proto::coprocessor::Response;
}

/// For requests or plans which are handled at TiKV store (other than region) level.
pub trait StoreRequest {
    /// Apply the request to specified TiKV store.
    fn apply_store(&mut self, store: &Store);
}

#[derive(Clone, Debug, new, Eq, PartialEq)]
pub struct RetryOptions {
    /// How to retry when there is a region error and we need to resolve regions with PD.
    pub region_backoff: Backoff,
    /// How to retry when a key is locked.
    pub lock_backoff: Backoff,
}

impl RetryOptions {
    pub const fn default_optimistic() -> RetryOptions {
        RetryOptions {
            region_backoff: DEFAULT_REGION_BACKOFF,
            lock_backoff: OPTIMISTIC_BACKOFF,
        }
    }

    pub const fn default_pessimistic() -> RetryOptions {
        RetryOptions {
            region_backoff: DEFAULT_REGION_BACKOFF,
            lock_backoff: PESSIMISTIC_BACKOFF,
        }
    }

    pub const fn none() -> RetryOptions {
        RetryOptions {
            region_backoff: Backoff::no_backoff(),
            lock_backoff: Backoff::no_backoff(),
        }
    }
}

#[cfg(test)]
mod test {
    use std::any::Any;
    use std::iter;
    use std::sync::atomic::AtomicUsize;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::time::Duration;

    use tonic::transport::Channel;

    use super::*;
    use crate::mock::MockKvClient;
    use crate::mock::MockPdClient;
    use crate::proto::coprocessor;
    use crate::proto::errorpb;
    use crate::proto::kvrpcpb;
    use crate::proto::pdpb::Timestamp;
    use crate::proto::tikvpb::tikv_client::TikvClient;
    use crate::region::RegionWithLeader;
    use crate::store::region_stream_for_keys;
    use crate::store::HasRegionError;
    use crate::transaction::lowering::new_commit_request;
    use crate::Error;
    use crate::Key;
    use crate::Result;

    #[tokio::test]
    async fn test_region_retry() {
        #[derive(Debug, Clone, Default)]
        struct MockRpcResponse;

        impl HasKeyErrors for MockRpcResponse {
            fn key_errors(&mut self) -> Option<Vec<Error>> {
                None
            }
        }

        impl HasRegionError for MockRpcResponse {
            fn region_error(&mut self) -> Option<crate::proto::errorpb::Error> {
                Some(crate::proto::errorpb::Error::default())
            }
        }

        impl crate::store::SetRegionError for MockRpcResponse {
            fn set_region_error(&mut self, _error: crate::proto::errorpb::Error) {}
        }

        impl HasLocks for MockRpcResponse {}

        #[derive(Clone)]
        struct MockKvRequest {
            test_invoking_count: Arc<AtomicUsize>,
            context: Option<kvrpcpb::Context>,
        }

        #[async_trait]
        impl Request for MockKvRequest {
            async fn dispatch(
                &self,
                _: &TikvClient<Channel>,
                _: Duration,
            ) -> Result<Box<dyn Any + Send>> {
                Ok(Box::new(MockRpcResponse {}))
            }

            fn label(&self) -> &'static str {
                "mock"
            }

            fn as_any(&self) -> &dyn Any {
                self
            }

            fn context_mut(&mut self) -> &mut kvrpcpb::Context {
                self.context.get_or_insert(kvrpcpb::Context::default())
            }

            fn set_leader(&mut self, _: &RegionWithLeader) -> Result<()> {
                Ok(())
            }

            fn set_api_version(&mut self, _: kvrpcpb::ApiVersion) {}
        }

        #[async_trait]
        impl KvRequest for MockKvRequest {
            type Response = MockRpcResponse;
        }

        impl Shardable for MockKvRequest {
            type Shard = Vec<Vec<u8>>;

            fn shards(
                &self,
                pd_client: &std::sync::Arc<impl crate::pd::PdClient>,
            ) -> futures::stream::BoxStream<
                'static,
                crate::Result<(Self::Shard, crate::region::RegionWithLeader)>,
            > {
                // Increases by 1 for each call.
                self.test_invoking_count
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                region_stream_for_keys(
                    Some(Key::from("mock_key".to_owned())).into_iter(),
                    pd_client.clone(),
                )
            }

            fn apply_shard(&mut self, _shard: Self::Shard) {}

            fn apply_store(&mut self, _store: &crate::store::RegionStore) -> crate::Result<()> {
                Ok(())
            }
        }

        let invoking_count = Arc::new(AtomicUsize::new(0));

        let request = MockKvRequest {
            test_invoking_count: invoking_count.clone(),
            context: None,
        };

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            |_: &dyn Any| Ok(Box::new(MockRpcResponse)),
        )));

        let plan = crate::request::PlanBuilder::new(pd_client.clone(), Keyspace::Disable, request)
            .resolve_lock(Backoff::no_jitter_backoff(1, 1, 3), Keyspace::Disable)
            .retry_multi_region(Backoff::no_jitter_backoff(1, 1, 3))
            .extract_error()
            .plan();
        let _ = plan.execute().await;

        // Original call plus the 3 retries
        assert_eq!(invoking_count.load(std::sync::atomic::Ordering::SeqCst), 4);
    }

    #[tokio::test]
    async fn test_extract_error() {
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            |_: &dyn Any| {
                Ok(Box::new(kvrpcpb::CommitResponse {
                    error: Some(kvrpcpb::KeyError::default()),
                    ..Default::default()
                }))
            },
        )));

        let key: Key = "key".to_owned().into();
        let req = new_commit_request(iter::once(key), Timestamp::default(), Timestamp::default());

        // does not extract error
        let plan =
            crate::request::PlanBuilder::new(pd_client.clone(), Keyspace::Disable, req.clone())
                .resolve_lock(OPTIMISTIC_BACKOFF, Keyspace::Disable)
                .retry_multi_region(OPTIMISTIC_BACKOFF)
                .plan();
        assert!(plan.execute().await.is_ok());

        // extract error
        let plan = crate::request::PlanBuilder::new(pd_client.clone(), Keyspace::Disable, req)
            .resolve_lock(OPTIMISTIC_BACKOFF, Keyspace::Disable)
            .retry_multi_region(OPTIMISTIC_BACKOFF)
            .extract_error()
            .plan();
        assert!(plan.execute().await.is_err());
    }

    #[tokio::test]
    async fn test_coprocessor_request_is_sharded_by_region() -> Result<()> {
        let seen_region_ids: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
        let seen_for_hook = seen_region_ids.clone();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let Some(req) = req.downcast_ref::<coprocessor::Request>() else {
                    return Err(crate::internal_err!("unexpected request type"));
                };
                let region_id = req
                    .context
                    .as_ref()
                    .map(|ctx| ctx.region_id)
                    .unwrap_or_default();
                seen_for_hook.lock().unwrap().push(region_id);
                Ok(Box::new(coprocessor::Response {
                    data: vec![region_id as u8],
                    ..Default::default()
                }) as Box<dyn Any + Send>)
            },
        )));

        let request = coprocessor::Request {
            tp: 103, // DAG; the value is irrelevant for sharding tests.
            data: vec![1, 2, 3],
            ranges: vec![
                coprocessor::KeyRange {
                    start: vec![],
                    end: vec![5],
                },
                coprocessor::KeyRange {
                    start: vec![10],
                    end: vec![20],
                },
                coprocessor::KeyRange {
                    start: vec![250, 250],
                    end: vec![],
                },
            ],
            ..Default::default()
        };

        let plan = crate::request::PlanBuilder::new(pd_client, Keyspace::Disable, request)
            .resolve_lock(Backoff::no_backoff(), Keyspace::Disable)
            .retry_multi_region_with_concurrency(Backoff::no_backoff(), 1)
            .merge(CollectError)
            .extract_error()
            .plan();

        let responses = plan.execute().await?;
        let got: Vec<u8> = responses
            .into_iter()
            .map(|resp| resp.data.get(0).copied().unwrap_or_default())
            .collect();
        assert_eq!(got, vec![1, 2, 3]);
        assert_eq!(*seen_region_ids.lock().unwrap(), vec![1, 2, 3]);
        Ok(())
    }

    #[tokio::test]
    async fn test_coprocessor_request_resolve_lock_retries_on_locked_response() -> Result<()> {
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_for_hook = calls.clone();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let Some(_req) = req.downcast_ref::<coprocessor::Request>() else {
                    return Err(crate::internal_err!("unexpected request type"));
                };

                let call = calls_for_hook.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                if call == 0 {
                    let lock = kvrpcpb::LockInfo {
                        key: vec![1],
                        primary_lock: vec![1],
                        lock_version: 0,
                        lock_ttl: 1000,
                        ..Default::default()
                    };
                    return Ok(Box::new(coprocessor::Response {
                        locked: Some(lock),
                        ..Default::default()
                    }) as Box<dyn Any + Send>);
                }

                Ok(Box::new(coprocessor::Response {
                    data: vec![42],
                    ..Default::default()
                }) as Box<dyn Any + Send>)
            },
        )));

        let request = coprocessor::Request {
            tp: 103,
            data: vec![1],
            ranges: vec![coprocessor::KeyRange {
                start: vec![],
                end: vec![5],
            }],
            ..Default::default()
        };

        let plan = crate::request::PlanBuilder::new(pd_client, Keyspace::Disable, request)
            // Keep the backoff delay at 0ms to avoid slowing down tests.
            .resolve_lock(Backoff::no_jitter_backoff(0, 0, 1), Keyspace::Disable)
            .retry_multi_region_with_concurrency(Backoff::no_backoff(), 1)
            .merge(CollectError)
            .extract_error()
            .plan();

        let responses = plan.execute().await?;
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 2);
        assert_eq!(responses.len(), 1);
        assert_eq!(responses[0].data, vec![42]);
        assert!(responses[0].locked.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn test_resolve_lock_respects_lock_wait_timeout() {
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let Some(_req) = req.downcast_ref::<coprocessor::Request>() else {
                    return Err(crate::internal_err!("unexpected request type"));
                };
                let lock = kvrpcpb::LockInfo {
                    key: vec![1],
                    primary_lock: vec![1],
                    lock_version: 0,
                    lock_ttl: i64::MAX as u64,
                    ..Default::default()
                };
                Ok(Box::new(coprocessor::Response {
                    locked: Some(lock),
                    ..Default::default()
                }) as Box<dyn Any + Send>)
            },
        )));

        let request = coprocessor::Request {
            tp: 103,
            data: vec![1],
            ranges: vec![coprocessor::KeyRange {
                start: vec![],
                end: vec![5],
            }],
            ..Default::default()
        };

        let plan = crate::request::PlanBuilder::new(pd_client, Keyspace::Disable, request)
            .resolve_lock_with_timeout(
                Backoff::no_jitter_backoff(50, 50, 1),
                Keyspace::Disable,
                Some(std::time::Duration::from_millis(10)),
            )
            .retry_multi_region_with_concurrency(Backoff::no_backoff(), 1)
            .merge(CollectError)
            .extract_error()
            .plan();

        match plan.execute().await {
            Ok(resp) => panic!("expected lock wait timeout, got response: {resp:?}"),
            Err(crate::Error::LockWaitTimeout(_)) => {}
            Err(other) => panic!("expected lock wait timeout, got error: {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_coprocessor_request_resolve_lock_cleans_up_expired_lock() -> Result<()> {
        use std::sync::atomic::Ordering;

        let cop_calls = Arc::new(AtomicUsize::new(0));
        let cleanup_calls = Arc::new(AtomicUsize::new(0));
        let resolve_lock_calls = Arc::new(AtomicUsize::new(0));

        let cop_calls_for_hook = cop_calls.clone();
        let cleanup_calls_for_hook = cleanup_calls.clone();
        let resolve_lock_calls_for_hook = resolve_lock_calls.clone();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if let Some(_req) = req.downcast_ref::<coprocessor::Request>() {
                    let call = cop_calls_for_hook.fetch_add(1, Ordering::SeqCst);
                    if call == 0 {
                        // lock_ttl=0 makes the lock immediately "expired" so resolve_locks will
                        // actively cleanup + resolve it.
                        let lock = kvrpcpb::LockInfo {
                            key: vec![1],
                            primary_lock: vec![1],
                            lock_version: 0,
                            lock_ttl: 0,
                            ..Default::default()
                        };
                        return Ok(Box::new(coprocessor::Response {
                            locked: Some(lock),
                            ..Default::default()
                        }) as Box<dyn Any + Send>);
                    }
                    return Ok(Box::new(coprocessor::Response {
                        data: vec![99],
                        ..Default::default()
                    }) as Box<dyn Any + Send>);
                }

                if let Some(_req) = req.downcast_ref::<kvrpcpb::CleanupRequest>() {
                    cleanup_calls_for_hook.fetch_add(1, Ordering::SeqCst);
                    return Ok(Box::new(kvrpcpb::CleanupResponse {
                        commit_version: 0,
                        ..Default::default()
                    }) as Box<dyn Any + Send>);
                }

                if let Some(_req) = req.downcast_ref::<kvrpcpb::ResolveLockRequest>() {
                    resolve_lock_calls_for_hook.fetch_add(1, Ordering::SeqCst);
                    return Ok(
                        Box::new(kvrpcpb::ResolveLockResponse::default()) as Box<dyn Any + Send>
                    );
                }

                Err(crate::internal_err!("unexpected request type"))
            },
        )));

        let request = coprocessor::Request {
            tp: 103,
            data: vec![1],
            ranges: vec![coprocessor::KeyRange {
                start: vec![],
                end: vec![5],
            }],
            ..Default::default()
        };

        let plan = crate::request::PlanBuilder::new(pd_client, Keyspace::Disable, request)
            .resolve_lock(Backoff::no_jitter_backoff(0, 0, 3), Keyspace::Disable)
            .retry_multi_region_with_concurrency(Backoff::no_backoff(), 1)
            .merge(CollectError)
            .extract_error()
            .plan();

        let responses = plan.execute().await?;
        assert_eq!(responses.len(), 1);
        assert_eq!(responses[0].data, vec![99]);

        assert_eq!(cop_calls.load(Ordering::SeqCst), 2);
        assert_eq!(cleanup_calls.load(Ordering::SeqCst), 1);
        assert_eq!(resolve_lock_calls.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_coprocessor_request_retries_on_region_error_response() -> Result<()> {
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_for_hook = calls.clone();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let Some(_req) = req.downcast_ref::<coprocessor::Request>() else {
                    return Err(crate::internal_err!("unexpected request type"));
                };

                let call = calls_for_hook.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                if call == 0 {
                    let region_error = errorpb::Error {
                        region_not_found: Some(errorpb::RegionNotFound { region_id: 1 }),
                        ..Default::default()
                    };
                    return Ok(Box::new(coprocessor::Response {
                        region_error: Some(region_error),
                        ..Default::default()
                    }) as Box<dyn Any + Send>);
                }

                Ok(Box::new(coprocessor::Response {
                    data: vec![7],
                    ..Default::default()
                }) as Box<dyn Any + Send>)
            },
        )));

        let request = coprocessor::Request {
            tp: 103,
            data: vec![1],
            ranges: vec![coprocessor::KeyRange {
                start: vec![],
                end: vec![5],
            }],
            ..Default::default()
        };

        let plan = crate::request::PlanBuilder::new(pd_client, Keyspace::Disable, request)
            .resolve_lock(Backoff::no_backoff(), Keyspace::Disable)
            .retry_multi_region_with_concurrency(Backoff::no_backoff(), 1)
            .merge(CollectError)
            .extract_error()
            .plan();

        let responses = plan.execute().await?;
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 2);
        assert_eq!(responses.len(), 1);
        assert_eq!(responses[0].data, vec![7]);
        assert!(responses[0].region_error.is_none());
        Ok(())
    }
}
