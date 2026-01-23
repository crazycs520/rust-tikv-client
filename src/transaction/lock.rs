// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use fail::fail_point;
use log::debug;
use log::error;
use tokio::sync::RwLock;
use tokio::time::sleep;

use crate::backoff::Backoff;
use crate::backoff::DEFAULT_REGION_BACKOFF;
use crate::backoff::OPTIMISTIC_BACKOFF;
use crate::pd::PdClient;

use crate::proto::kvrpcpb;
use crate::proto::kvrpcpb::TxnInfo;
use crate::proto::pdpb::Timestamp;
use crate::region::RegionVerId;
use crate::request::CollectError;
use crate::request::CollectSingle;
use crate::request::Keyspace;
use crate::request::Plan;
use crate::store::RegionStore;
use crate::timestamp::TimestampExt;
use crate::transaction::requests;
use crate::transaction::requests::new_check_secondary_locks_request;
use crate::transaction::requests::new_check_txn_status_request;
use crate::transaction::requests::SecondaryLocksStatus;
use crate::transaction::requests::TransactionStatus;
use crate::transaction::requests::TransactionStatusKind;
use crate::Error;
use crate::RequestContext;
use crate::Result;

const RESOLVE_LOCK_RETRY_LIMIT: usize = 10;

/// _Resolves_ the given locks. Returns locks still live. When there is no live locks, all the given locks are resolved.
///
/// If a key has a lock, the latest status of the key is unknown. We need to "resolve" the lock,
/// which means the key is finally either committed or rolled back, before we read the value of
/// the key. We first use `CleanupRequest` to let the status of the primary lock converge and get
/// its status (committed or rolled back). Then, we use the status of its primary lock to determine
/// the status of the other keys in the same transaction.
pub async fn resolve_locks(
    locks: Vec<kvrpcpb::LockInfo>,
    pd_client: Arc<impl PdClient>,
    keyspace: Keyspace,
    request_context: &RequestContext,
) -> Result<Vec<kvrpcpb::LockInfo> /* live_locks */> {
    debug!("resolving locks");
    crate::trace::trace_event(
        crate::trace::CATEGORY_TXN_LOCK_RESOLVE,
        "txn.resolve_locks.start",
        [(
            "lock_count",
            crate::trace::TraceValue::U64(locks.len() as u64),
        )],
    );
    let ts = pd_client.clone().get_timestamp().await?;
    let (expired_locks, live_locks) =
        locks
            .into_iter()
            .partition::<Vec<kvrpcpb::LockInfo>, _>(|lock| {
                ts.physical - Timestamp::from_version(lock.lock_version).physical
                    >= lock.lock_ttl as i64
            });

    // records the commit version of each primary lock (representing the status of the transaction)
    let mut commit_versions: HashMap<u64, u64> = HashMap::new();
    let mut clean_regions: HashMap<u64, HashSet<RegionVerId>> = HashMap::new();
    for lock in expired_locks {
        let region_ver_id = pd_client
            .region_for_key(&lock.primary_lock.clone().into())
            .await?
            .ver_id();
        // skip if the region is cleaned
        if clean_regions
            .get(&lock.lock_version)
            .map(|regions| regions.contains(&region_ver_id))
            .unwrap_or(false)
        {
            continue;
        }

        let commit_version = match commit_versions.get(&lock.lock_version) {
            Some(&commit_version) => commit_version,
            None => {
                let request = request_context.apply_to(requests::new_cleanup_request(
                    lock.primary_lock,
                    lock.lock_version,
                ));
                let plan = crate::request::PlanBuilder::new(pd_client.clone(), keyspace, request)
                    .with_request_context(request_context.clone())
                    .resolve_lock(OPTIMISTIC_BACKOFF, keyspace)
                    .retry_multi_region(DEFAULT_REGION_BACKOFF)
                    .merge(CollectSingle)
                    .post_process_default()
                    .plan();
                let commit_version = plan.execute().await?;
                commit_versions.insert(lock.lock_version, commit_version);
                commit_version
            }
        };

        let cleaned_region = resolve_lock_with_retry(
            &lock.key,
            lock.lock_version,
            commit_version,
            DEFAULT_REGION_BACKOFF,
            pd_client.clone(),
            keyspace,
            request_context,
        )
        .await?;
        clean_regions
            .entry(lock.lock_version)
            .or_default()
            .insert(cleaned_region);
    }
    Ok(live_locks)
}

async fn resolve_lock_with_retry(
    #[allow(clippy::ptr_arg)] key: &Vec<u8>,
    start_version: u64,
    commit_version: u64,
    mut region_backoff: Backoff,
    pd_client: Arc<impl PdClient>,
    keyspace: Keyspace,
    request_context: &RequestContext,
) -> Result<RegionVerId> {
    debug!("resolving locks with retry");
    let mut error = None;
    for i in 0..RESOLVE_LOCK_RETRY_LIMIT {
        debug!("resolving locks: attempt {}", (i + 1));
        let mut store = pd_client.clone().store_for_key(key.into()).await?;
        store.attempt = i;
        let ver_id = store.region_with_leader.ver_id();
        let request = request_context.apply_to(requests::new_resolve_lock_request(
            start_version,
            commit_version,
        ));
        let region_store = store.clone();
        let plan = crate::request::PlanBuilder::new(pd_client.clone(), keyspace, request)
            .with_request_context(request_context.clone())
            .single_region_with_store(store)
            .await?
            .resolve_lock(Backoff::no_backoff(), keyspace)
            .extract_error()
            .plan();
        match plan.execute().await {
            Ok(_) => {
                return Ok(ver_id);
            }
            // Retry on region error
            Err(Error::ExtractedErrors(mut errors)) => {
                // ResolveLockResponse can have at most 1 error
                match errors.pop() {
                    Some(Error::RegionError(e)) => {
                        let region_error = *e;
                        error = Some(Error::RegionError(Box::new(region_error.clone())));
                        let resolved = crate::request::plan::handle_region_error(
                            pd_client.clone(),
                            region_error,
                            region_store,
                        )
                        .await?;
                        if !resolved && i + 1 < RESOLVE_LOCK_RETRY_LIMIT {
                            if let Some(duration) = region_backoff.next_delay_duration() {
                                sleep(duration).await;
                            }
                        }
                        continue;
                    }
                    Some(e) => return Err(e),
                    None => {
                        return Err(crate::internal_err!(
                            "ResolveLock returned ExtractedErrors([]), which violates plan invariants"
                        ));
                    }
                }
            }
            Err(e) => return Err(e),
        }
    }
    if let Some(err) = error {
        return Err(err);
    }
    Err(crate::internal_err!(
        "resolve lock retry loop exhausted without producing an error"
    ))
}

#[derive(Default, Clone)]
pub struct ResolveLocksContext {
    // Record the status of each transaction.
    pub(crate) resolved: Arc<RwLock<HashMap<u64, Arc<TransactionStatus>>>>,
    pub(crate) clean_regions: Arc<RwLock<HashMap<u64, HashSet<RegionVerId>>>>,
}

#[derive(Clone, Copy, Debug)]
pub struct ResolveLocksOptions {
    pub async_commit_only: bool,
    pub batch_size: u32,
}

// Default scan lock limit used by GC / cleanup-locks.
//
// This matches client-go defaults:
// - `GCScanLockLimit = txnlock.ResolvedCacheSize / 2` (client-go/tikv/gc.go)
// - `ResolvedCacheSize = 2048` (client-go/txnkv/txnlock/lock_resolver.go)
const DEFAULT_SCAN_LOCK_BATCH_SIZE: u32 = 1024;

impl Default for ResolveLocksOptions {
    fn default() -> Self {
        Self {
            async_commit_only: false,
            batch_size: DEFAULT_SCAN_LOCK_BATCH_SIZE,
        }
    }
}

impl ResolveLocksContext {
    pub async fn get_resolved(&self, txn_id: u64) -> Option<Arc<TransactionStatus>> {
        self.resolved.read().await.get(&txn_id).cloned()
    }

    pub async fn save_resolved(&mut self, txn_id: u64, txn_status: Arc<TransactionStatus>) {
        self.resolved.write().await.insert(txn_id, txn_status);
    }

    pub async fn is_region_cleaned(&self, txn_id: u64, region: &RegionVerId) -> bool {
        self.clean_regions
            .read()
            .await
            .get(&txn_id)
            .map(|regions| regions.contains(region))
            .unwrap_or(false)
    }

    pub async fn save_cleaned_region(&mut self, txn_id: u64, region: RegionVerId) {
        self.clean_regions
            .write()
            .await
            .entry(txn_id)
            .or_insert_with(HashSet::new)
            .insert(region);
    }
}

pub struct LockResolver<PdC: PdClient> {
    ctx: ResolveLocksContext,
    pd_client: Arc<PdC>,
    keyspace: Keyspace,
    request_context: RequestContext,
}

impl<PdC: PdClient> LockResolver<PdC> {
    pub fn new(ctx: ResolveLocksContext, pd_client: Arc<PdC>, keyspace: Keyspace) -> Self {
        Self {
            ctx,
            pd_client,
            keyspace,
            request_context: RequestContext::default(),
        }
    }

    pub(crate) fn with_request_context(mut self, request_context: RequestContext) -> Self {
        self.request_context = request_context;
        self
    }

    /// _Cleanup_ the given locks. Returns whether all the given locks are resolved.
    ///
    /// Note: Will rollback RUNNING transactions. ONLY use in GC.
    pub async fn cleanup_locks(
        &mut self,
        store: RegionStore,
        locks: Vec<kvrpcpb::LockInfo>,
    ) -> Result<()> {
        if locks.is_empty() {
            return Ok(());
        }

        fail_point!("before-cleanup-locks", |_| { Ok(()) });

        let region = store.region_with_leader.ver_id();

        let mut txn_infos = HashMap::new();
        for l in locks {
            let txn_id = l.lock_version;
            if txn_infos.contains_key(&txn_id) || self.ctx.is_region_cleaned(txn_id, &region).await
            {
                continue;
            }

            // Use currentTS = math.MaxUint64 means rollback the txn, no matter the lock is expired or not!
            let mut status = self
                .check_txn_status(
                    txn_id,
                    l.primary_lock.clone(),
                    0,
                    u64::MAX,
                    true,
                    false,
                    l.lock_type == kvrpcpb::Op::PessimisticLock as i32,
                )
                .await?;

            // If the transaction uses async commit, check_txn_status will reject rolling back the primary lock.
            // Then we need to check the secondary locks to determine the final status of the transaction.
            if let TransactionStatusKind::Locked(_, lock_info) = &status.kind {
                let secondary_status = self
                    .check_all_secondaries(
                        lock_info.secondaries.clone(),
                        txn_id,
                        lock_info.min_commit_ts,
                    )
                    .await?;
                debug!(
                    "secondary status, txn_id:{}, commit_ts:{:?}, min_commit_version:{}, fallback_2pc:{}",
                    txn_id,
                    secondary_status
                        .commit_ts
                        .as_ref()
                        .map_or(0, |ts| ts.version()),
                    secondary_status.min_commit_ts,
                    secondary_status.fallback_2pc,
                );

                if secondary_status.fallback_2pc {
                    debug!("fallback to 2pc, txn_id:{}, check_txn_status again", txn_id);
                    status = self
                        .check_txn_status(
                            txn_id,
                            l.primary_lock,
                            0,
                            u64::MAX,
                            true,
                            true,
                            l.lock_type == kvrpcpb::Op::PessimisticLock as i32,
                        )
                        .await?;
                } else {
                    let commit_ts = if let Some(commit_ts) = &secondary_status.commit_ts {
                        commit_ts.version()
                    } else {
                        secondary_status.min_commit_ts
                    };
                    txn_infos.insert(txn_id, commit_ts);
                    continue;
                }
            }

            match &status.kind {
                TransactionStatusKind::Locked(_, lock_info) => {
                    error!(
                        "cleanup_locks fail to clean locks, this result is not expected. txn_id:{}",
                        txn_id
                    );
                    return Err(Error::ResolveLockError(vec![lock_info.clone()]));
                }
                TransactionStatusKind::Committed(ts) => txn_infos.insert(txn_id, ts.version()),
                TransactionStatusKind::RolledBack => txn_infos.insert(txn_id, 0),
            };
        }

        debug!(
            "batch resolve locks, region:{:?}, txn:{:?}",
            store.region_with_leader.ver_id(),
            txn_infos
        );
        let mut txn_ids = Vec::with_capacity(txn_infos.len());
        let mut txn_info_vec = Vec::with_capacity(txn_infos.len());
        for (txn_id, commit_ts) in txn_infos.into_iter() {
            txn_ids.push(txn_id);
            let mut txn_info = TxnInfo::default();
            txn_info.txn = txn_id;
            txn_info.status = commit_ts;
            txn_info_vec.push(txn_info);
        }
        let cleaned_region = self
            .batch_resolve_locks(store.clone(), txn_info_vec)
            .await?;
        for txn_id in txn_ids {
            self.ctx
                .save_cleaned_region(txn_id, cleaned_region.clone())
                .await;
        }

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn check_txn_status(
        &mut self,
        txn_id: u64,
        primary: Vec<u8>,
        caller_start_ts: u64,
        current_ts: u64,
        rollback_if_not_exist: bool,
        force_sync_commit: bool,
        resolving_pessimistic_lock: bool,
    ) -> Result<Arc<TransactionStatus>> {
        if let Some(txn_status) = self.ctx.get_resolved(txn_id).await {
            return Ok(txn_status);
        }

        // CheckTxnStatus may meet the following cases:
        // 1. LOCK
        // 1.1 Lock expired -- orphan lock, fail to update TTL, crash recovery etc.
        // 1.2 Lock TTL -- active transaction holding the lock.
        // 2. NO LOCK
        // 2.1 Txn Committed
        // 2.2 Txn Rollbacked -- rollback itself, rollback by others, GC tomb etc.
        // 2.3 No lock -- pessimistic lock rollback, concurrence prewrite.
        let req = self.request_context.apply_to(new_check_txn_status_request(
            primary,
            txn_id,
            caller_start_ts,
            current_ts,
            rollback_if_not_exist,
            force_sync_commit,
            resolving_pessimistic_lock,
        ));
        let plan = crate::request::PlanBuilder::new(self.pd_client.clone(), self.keyspace, req)
            .retry_multi_region(DEFAULT_REGION_BACKOFF)
            .merge(CollectSingle)
            .extract_error()
            .post_process_default()
            .plan();
        let mut res: TransactionStatus = plan.execute().await?;

        let current = self.pd_client.clone().get_timestamp().await?;
        res.check_ttl(current);
        let res = Arc::new(res);
        if res.is_cacheable() {
            self.ctx.save_resolved(txn_id, res.clone()).await;
        }
        Ok(res)
    }

    async fn check_all_secondaries(
        &mut self,
        keys: Vec<Vec<u8>>,
        txn_id: u64,
        primary_min_commit_ts: u64,
    ) -> Result<SecondaryLocksStatus> {
        let req = self
            .request_context
            .apply_to(new_check_secondary_locks_request(keys, txn_id));
        let plan = crate::request::PlanBuilder::new(self.pd_client.clone(), self.keyspace, req)
            .preserve_shard()
            .retry_multi_region(DEFAULT_REGION_BACKOFF)
            .extract_error()
            .merge(CollectError)
            .plan();

        // Go client parity: `LockResolver.checkAllSecondaries`.
        //
        // - Start with the primary lock's min_commit_ts.
        // - If any response returns fewer locks than requested, the txn status is determined by
        //   `commit_ts` (0 means rolled back).
        // - Otherwise (all locks present), resolve using the largest `min_commit_ts` among all locks.
        //
        // TiKV may set `commit_ts` even when all locks are present; it must be ignored in that case.
        let responses = plan.execute().await?;

        let mut status = SecondaryLocksStatus {
            commit_ts: None,
            min_commit_ts: primary_min_commit_ts,
            fallback_2pc: false,
        };
        let mut min_commit_ts = primary_min_commit_ts;
        let mut missing_lock = false;
        let mut determined_commit_ts: Option<u64> = None;

        for crate::request::ResponseWithShard(resp, shard) in responses {
            let expected = shard.len();
            let locks = resp.locks;

            if locks.len() < expected {
                missing_lock = true;
                let resp_commit_ts = resp.commit_ts;
                match determined_commit_ts {
                    None => {
                        if resp_commit_ts != 0 && resp_commit_ts < min_commit_ts {
                            return Err(crate::internal_err!(
                                "commit ts must be >= min commit ts: commit_ts={}, min_commit_ts={}",
                                resp_commit_ts,
                                min_commit_ts
                            ));
                        }
                        determined_commit_ts = Some(resp_commit_ts);
                    }
                    Some(prev) if prev == resp_commit_ts => {}
                    Some(prev) => {
                        return Err(crate::internal_err!(
                            "commit ts mismatch in async commit recovery: {} and {}",
                            prev,
                            resp_commit_ts
                        ));
                    }
                }
                continue;
            }

            for lock in locks {
                if !lock.use_async_commit {
                    status.fallback_2pc = true;
                    return Ok(status);
                }
                if !missing_lock {
                    min_commit_ts = min_commit_ts.max(lock.min_commit_ts);
                }
            }
        }

        if let Some(commit_ts) = determined_commit_ts {
            status.commit_ts = Timestamp::try_from_version(commit_ts);
            status.min_commit_ts = commit_ts;
        } else {
            status.min_commit_ts = min_commit_ts;
        }

        Ok(status)
    }

    async fn batch_resolve_locks(
        &mut self,
        store: RegionStore,
        txn_infos: Vec<TxnInfo>,
    ) -> Result<RegionVerId> {
        let ver_id = store.region_with_leader.ver_id();
        let request = self
            .request_context
            .apply_to(requests::new_batch_resolve_lock_request(txn_infos.clone()));
        let plan = crate::request::PlanBuilder::new(self.pd_client.clone(), self.keyspace, request)
            .single_region_with_store(store.clone())
            .await?
            .extract_error()
            .plan();
        let _ = plan.execute().await?;
        Ok(ver_id)
    }
}

pub trait HasLocks {
    fn take_locks(&mut self) -> Vec<kvrpcpb::LockInfo> {
        Vec::new()
    }
}

impl HasLocks for crate::proto::coprocessor::Response {
    fn take_locks(&mut self) -> Vec<kvrpcpb::LockInfo> {
        self.locked.take().into_iter().collect()
    }
}

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use serial_test::serial;

    use super::*;
    use crate::mock::MockKvClient;
    use crate::mock::MockPdClient;
    use crate::proto::errorpb;
    use crate::Key;

    #[rstest::rstest]
    #[case(Keyspace::Disable)]
    #[case(Keyspace::Enable { keyspace_id: 0 })]
    #[tokio::test]
    #[serial]
    async fn test_resolve_lock_with_retry(#[case] keyspace: Keyspace) {
        // Test resolve lock within retry limit
        fail::cfg("region-error", "9*return").unwrap();

        let client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            |_: &dyn Any| {
                fail::fail_point!("region-error", |_| {
                    let resp = kvrpcpb::ResolveLockResponse {
                        region_error: Some(errorpb::Error::default()),
                        ..Default::default()
                    };
                    Ok(Box::new(resp) as Box<dyn Any + Send>)
                });
                Ok(Box::<kvrpcpb::ResolveLockResponse>::default() as Box<dyn Any + Send>)
            },
        )));

        let key = vec![1];
        let region1 = MockPdClient::region1();
        let request_context = RequestContext::default();
        let resolved_region = resolve_lock_with_retry(
            &key,
            1,
            2,
            Backoff::no_jitter_backoff(0, 0, RESOLVE_LOCK_RETRY_LIMIT as u32),
            client.clone(),
            keyspace,
            &request_context,
        )
        .await
        .unwrap();
        assert_eq!(region1.ver_id(), resolved_region);

        // Test resolve lock over retry limit
        fail::cfg("region-error", "10*return").unwrap();
        let key = vec![100];
        resolve_lock_with_retry(
            &key,
            3,
            4,
            Backoff::no_jitter_backoff(0, 0, RESOLVE_LOCK_RETRY_LIMIT as u32),
            client,
            keyspace,
            &request_context,
        )
        .await
        .expect_err("should return error");
    }

    #[rstest::rstest]
    #[case(Keyspace::Disable)]
    #[case(Keyspace::Enable { keyspace_id: 0 })]
    #[tokio::test]
    #[serial]
    async fn test_lock_resolver_cleanup_locks_uses_stored_pd_client(#[case] keyspace: Keyspace) {
        let txn_id = 42_u64;
        let commit_ts = 100_u64;
        let primary_key = vec![1, 2, 3];

        let check_count = Arc::new(AtomicUsize::new(0));
        let batch_count = Arc::new(AtomicUsize::new(0));
        let check_count_hook = check_count.clone();
        let batch_count_hook = batch_count.clone();
        let primary_key_hook = primary_key.clone();

        let client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if let Some(req) = req.downcast_ref::<kvrpcpb::CheckTxnStatusRequest>() {
                    check_count_hook.fetch_add(1, Ordering::SeqCst);
                    assert_eq!(req.lock_ts, txn_id);
                    assert_eq!(req.primary_key, primary_key_hook);
                    assert_eq!(req.caller_start_ts, 0);
                    assert_eq!(req.current_ts, u64::MAX);
                    assert!(req.rollback_if_not_exist);
                    assert!(!req.force_sync_commit);

                    let resp = kvrpcpb::CheckTxnStatusResponse {
                        commit_version: commit_ts,
                        action: kvrpcpb::Action::NoAction as i32,
                        ..Default::default()
                    };
                    return Ok(Box::new(resp));
                }
                if let Some(req) = req.downcast_ref::<kvrpcpb::ResolveLockRequest>() {
                    batch_count_hook.fetch_add(1, Ordering::SeqCst);
                    assert_eq!(req.txn_infos.len(), 1);
                    assert_eq!(req.txn_infos[0].txn, txn_id);
                    assert_eq!(req.txn_infos[0].status, commit_ts);
                    let resp = kvrpcpb::ResolveLockResponse::default();
                    return Ok(Box::new(resp));
                }
                panic!("unexpected request type in LockResolver test");
            },
        )));

        let key: Key = primary_key.clone().into();
        let store = client.clone().store_for_key(&key).await.unwrap();

        let mut lock_info = kvrpcpb::LockInfo::default();
        lock_info.lock_version = txn_id;
        lock_info.primary_lock = primary_key;
        lock_info.lock_type = kvrpcpb::Op::Put as i32;

        let mut resolver = LockResolver::new(ResolveLocksContext::default(), client, keyspace);
        resolver
            .cleanup_locks(store, vec![lock_info])
            .await
            .unwrap();

        assert_eq!(check_count.load(Ordering::SeqCst), 1);
        assert_eq!(batch_count.load(Ordering::SeqCst), 1);
    }

    #[rstest::rstest]
    #[case(Keyspace::Disable)]
    #[case(Keyspace::Enable { keyspace_id: 0 })]
    #[tokio::test]
    async fn test_lock_resolver_uses_resolved_cache_and_skips_rpc(#[case] keyspace: Keyspace) {
        let txn_id = 1_u64;
        let commit_ts = 10_u64;
        let primary_key = b"k1".to_vec();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if req
                    .downcast_ref::<kvrpcpb::CheckTxnStatusRequest>()
                    .is_some()
                {
                    panic!("CheckTxnStatusRequest should be served from ResolveLocksContext cache");
                }
                if req
                    .downcast_ref::<kvrpcpb::CheckSecondaryLocksRequest>()
                    .is_some()
                {
                    panic!("CheckSecondaryLocksRequest should not be issued for a committed txn");
                }
                if let Some(req) = req.downcast_ref::<kvrpcpb::ResolveLockRequest>() {
                    assert_eq!(req.txn_infos.len(), 1);
                    assert_eq!(req.txn_infos[0].txn, txn_id);
                    assert_eq!(req.txn_infos[0].status, commit_ts);
                    return Ok(Box::new(kvrpcpb::ResolveLockResponse::default()));
                }
                panic!("unexpected request type in lock resolver cache test");
            },
        )));

        let mut ctx = ResolveLocksContext::default();
        ctx.save_resolved(
            txn_id,
            Arc::new(TransactionStatus {
                kind: TransactionStatusKind::Committed(Timestamp::from_version(commit_ts)),
                action: kvrpcpb::Action::NoAction,
                is_expired: false,
            }),
        )
        .await;

        let store = pd_client
            .clone()
            .map_region_to_store(MockPdClient::region1())
            .await
            .unwrap();

        let mut lock_info = kvrpcpb::LockInfo::default();
        lock_info.lock_version = txn_id;
        lock_info.primary_lock = primary_key;
        lock_info.lock_type = kvrpcpb::Op::Put as i32;

        let mut resolver = LockResolver::new(ctx, pd_client, keyspace);
        resolver
            .cleanup_locks(store, vec![lock_info])
            .await
            .unwrap();
    }
}
