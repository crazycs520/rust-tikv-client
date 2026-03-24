// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;

use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::StreamExt;
use log::info;
use tokio::sync::{OnceCell, RwLock};

use crate::compat::stream_fn;
use crate::kv::codec;
use crate::pd::retry::RetryClientTrait;
use crate::pd::Cluster;
use crate::pd::RetryClient;
use crate::proto::keyspacepb;
use crate::proto::kvrpcpb;
use crate::proto::metapb;
use crate::region::RegionId;
use crate::region::RegionVerId;
use crate::region::RegionWithLeader;
use crate::region::StoreId;
use crate::region_cache::RegionCache;
use crate::region_cache::RegionLoadResult;
use crate::request::decode_bucket_keys;
use crate::request::parse_keyspace_id;
use crate::request::KeyMode;
use crate::request::Keyspace;
use crate::store::safe_ts::SafeTsManager;
use crate::store::safe_ts::StoreSafeTsProvider;
use crate::store::safe_ts::StoreSafeTsRequest;
use crate::store::KvConnect;
use crate::store::RegionStore;
use crate::store::StoreHealthMap;
use crate::store::TikvConnect;
use crate::store::{KvClient, Store};
use crate::BoundRange;
use crate::Config;
use crate::Key;
use crate::Result;
use crate::SecurityManager;
use crate::Timestamp;

type KvClientCache<KvC> = Arc<RwLock<HashMap<String, Arc<OnceCell<<KvC as KvConnect>::KvClient>>>>>;

/// The PdClient handles all the encoding stuff.
///
/// Raw APIs does not require encoding/decoding at all.
/// All keys in all places (client, PD, TiKV) are in the same encoding (here called "raw format").
///
/// Transactional APIs are a bit complicated.
/// We need encode and decode keys when we communicate with PD, but not with TiKV.
/// We encode keys before sending requests to PD, and decode keys in the response from PD.
/// That's all we need to do with encoding.
///
///  client -encoded-> PD, PD -encoded-> client
///  client -raw-> TiKV, TiKV -raw-> client
///
/// The reason for the behavior is that in transaction mode, TiKV encode keys for MVCC.
/// In raw mode, TiKV doesn't encode them.
/// TiKV tells PD using its internal representation, whatever the encoding is.
/// So if we use transactional APIs, keys in PD are encoded and PD does not know about the encoding stuff.
#[async_trait]
pub trait PdClient: Send + Sync + 'static {
    type KvClient: KvClient + Send + Sync + 'static;

    fn store_health(&self) -> crate::store::StoreHealthMap {
        crate::store::StoreHealthMap::default()
    }

    /// In transactional API, `region` is decoded (keys in raw format).
    async fn map_region_to_store(self: Arc<Self>, region: RegionWithLeader) -> Result<RegionStore>;

    /// In transactional API, the key and returned region are both decoded (keys in raw format).
    async fn region_for_key(&self, key: &Key) -> Result<RegionWithLeader>;

    /// In transactional API, the returned region is decoded (keys in raw format)
    async fn region_for_id(&self, id: RegionId) -> Result<RegionWithLeader>;

    async fn get_timestamp(self: Arc<Self>) -> Result<Timestamp>;

    async fn get_min_ts(self: Arc<Self>) -> Result<Timestamp>;

    async fn update_safepoint(self: Arc<Self>, safepoint: u64) -> Result<bool>;

    async fn load_keyspace(&self, keyspace: &str) -> Result<keyspacepb::KeyspaceMeta>;

    /// In transactional API, `key` is in raw format
    async fn store_for_key(self: Arc<Self>, key: &Key) -> Result<RegionStore> {
        let region = self.region_for_key(key).await?;
        self.map_region_to_store(region).await
    }

    async fn store_for_id(self: Arc<Self>, id: RegionId) -> Result<RegionStore> {
        let region = self.region_for_id(id).await?;
        self.map_region_to_store(region).await
    }

    async fn all_stores(&self) -> Result<Vec<Store>>;

    fn group_keys_by_region<K, K2>(
        self: Arc<Self>,
        keys: impl Iterator<Item = K> + Send + Sync + 'static,
    ) -> BoxStream<'static, Result<(Vec<K2>, RegionWithLeader)>>
    where
        K: AsRef<Key> + Into<K2> + Send + Sync + 'static,
        K2: Send + Sync + 'static,
    {
        let keys = keys.peekable();
        stream_fn(keys, move |mut keys| {
            let this = self.clone();
            async move {
                if let Some(key) = keys.next() {
                    let region = this.region_for_key(key.as_ref()).await?;
                    let mut grouped = vec![key.into()];
                    while let Some(key) = keys.peek() {
                        if !region.contains(key.as_ref()) {
                            break;
                        }
                        let Some(key) = keys.next() else {
                            break;
                        };
                        grouped.push(key.into());
                    }
                    Ok(Some((keys, (grouped, region))))
                } else {
                    Ok(None)
                }
            }
        })
        .boxed()
    }

    /// Returns a Stream which iterates over the contexts for each region covered by range.
    fn regions_for_range(
        self: Arc<Self>,
        range: BoundRange,
    ) -> BoxStream<'static, Result<RegionWithLeader>> {
        let (start_key, end_key) = range.into_keys();
        stream_fn(Some(start_key), move |start_key| {
            let end_key = end_key.clone();
            let this = self.clone();
            async move {
                let start_key = match start_key {
                    None => return Ok(None),
                    Some(sk) => sk,
                };

                let region = this.region_for_key(&start_key).await?;
                let region_end = region.end_key();
                if end_key
                    .map(|x| x <= region_end && !x.is_empty())
                    .unwrap_or(false)
                    || region_end.is_empty()
                {
                    return Ok(Some((None, region)));
                }
                Ok(Some((Some(region_end), region)))
            }
        })
        .boxed()
    }

    /// Returns a Stream which iterates over the contexts for ranges in the same region.
    fn group_ranges_by_region(
        self: Arc<Self>,
        mut ranges: Vec<kvrpcpb::KeyRange>,
    ) -> BoxStream<'static, Result<(Vec<kvrpcpb::KeyRange>, RegionWithLeader)>> {
        ranges.reverse();
        stream_fn(Some(ranges), move |ranges| {
            let this = self.clone();
            async move {
                let mut ranges = match ranges {
                    None => return Ok(None),
                    Some(r) => r,
                };

                if let Some(range) = ranges.pop() {
                    let start_key: Key = range.start_key.clone().into();
                    let end_key: Key = range.end_key.clone().into();
                    let region = this.region_for_key(&start_key).await?;
                    let region_start = region.start_key();
                    let region_end = region.end_key();
                    let mut grouped = vec![];
                    if !region_end.is_empty() && (end_key > region_end || end_key.is_empty()) {
                        grouped.push(make_key_range(start_key.into(), region_end.clone().into()));
                        ranges.push(make_key_range(region_end.into(), end_key.into()));
                        return Ok(Some((Some(ranges), (grouped, region))));
                    }
                    grouped.push(range);

                    while let Some(range) = ranges.pop() {
                        let start_key: Key = range.start_key.clone().into();
                        let end_key: Key = range.end_key.clone().into();
                        if start_key < region_start || start_key > region_end {
                            ranges.push(range);
                            break;
                        }
                        if !region_end.is_empty() && (end_key > region_end || end_key.is_empty()) {
                            grouped
                                .push(make_key_range(start_key.into(), region_end.clone().into()));
                            ranges.push(make_key_range(region_end.into(), end_key.into()));
                            return Ok(Some((Some(ranges), (grouped, region))));
                        }
                        grouped.push(range);
                    }
                    Ok(Some((Some(ranges), (grouped, region))))
                } else {
                    Ok(None)
                }
            }
        })
        .boxed()
    }

    fn decode_region(mut region: RegionWithLeader, enable_codec: bool) -> Result<RegionWithLeader> {
        if enable_codec {
            codec::decode_bytes_in_place(&mut region.region.start_key, false)?;
            codec::decode_bytes_in_place(&mut region.region.end_key, false)?;
        }
        Ok(region)
    }

    async fn update_leader(&self, ver_id: RegionVerId, leader: metapb::Peer) -> Result<()>;

    async fn invalidate_region_cache(&self, ver_id: RegionVerId);

    async fn invalidate_store_cache(&self, store_id: StoreId);
}

/// This client converts requests for the logical TiKV cluster into requests
/// for a single TiKV store using PD and internal logic.
pub struct PdRpcClient<KvC: KvConnect + Send + Sync + 'static = TikvConnect, Cl = Cluster> {
    pd: Arc<RetryClient<Cl>>,
    kv_connect: KvC,
    kv_client_cache: KvClientCache<KvC>,
    store_health: StoreHealthMap,
    safe_ts: SafeTsManager,
    enable_codec: bool,
    region_cache: RegionCache<RetryClient<Cl>>,
}

#[async_trait]
impl<KvC: KvConnect + Send + Sync + 'static> PdClient for PdRpcClient<KvC> {
    type KvClient = KvC::KvClient;

    fn store_health(&self) -> StoreHealthMap {
        self.store_health.clone()
    }

    async fn map_region_to_store(self: Arc<Self>, region: RegionWithLeader) -> Result<RegionStore> {
        let store_id = region.get_store_id()?;
        let store = self.region_cache.get_store_by_id(store_id).await?;
        let kv_client = self.kv_client(&store.address).await?;
        Ok(RegionStore::new(region, Arc::new(kv_client)))
    }

    async fn region_for_key(&self, key: &Key) -> Result<RegionWithLeader> {
        let enable_codec = self.enable_codec;
        let key = if enable_codec {
            key.to_encoded()
        } else {
            key.clone()
        };

        let region = self.region_cache.get_region_by_key(&key).await?;
        Self::decode_region(region, enable_codec)
    }

    async fn region_for_id(&self, id: RegionId) -> Result<RegionWithLeader> {
        let region = self.region_cache.get_region_by_id(id).await?;
        Self::decode_region(region, self.enable_codec)
    }

    async fn all_stores(&self) -> Result<Vec<Store>> {
        let pb_stores = self.region_cache.read_through_all_stores().await?;
        let mut stores = Vec::with_capacity(pb_stores.len());
        for store in pb_stores {
            let client = self.kv_client(&store.address).await?;
            stores.push(Store::new(Arc::new(client)));
        }
        Ok(stores)
    }

    async fn get_timestamp(self: Arc<Self>) -> Result<Timestamp> {
        self.pd.clone().get_timestamp().await
    }

    async fn get_min_ts(self: Arc<Self>) -> Result<Timestamp> {
        self.pd.clone().get_min_ts().await
    }

    async fn update_safepoint(self: Arc<Self>, safepoint: u64) -> Result<bool> {
        self.pd.clone().update_safepoint(safepoint).await
    }

    async fn update_leader(&self, ver_id: RegionVerId, leader: metapb::Peer) -> Result<()> {
        self.region_cache.update_leader(ver_id, leader).await
    }

    async fn invalidate_region_cache(&self, ver_id: RegionVerId) {
        self.region_cache.invalidate_region_cache(ver_id).await
    }

    async fn invalidate_store_cache(&self, store_id: StoreId) {
        self.region_cache.invalidate_store_cache(store_id).await
    }

    async fn load_keyspace(&self, keyspace: &str) -> Result<keyspacepb::KeyspaceMeta> {
        self.pd.load_keyspace(keyspace).await
    }
}

impl PdRpcClient<TikvConnect, Cluster> {
    pub async fn connect(
        pd_endpoints: &[String],
        config: Config,
        enable_codec: bool,
    ) -> Result<PdRpcClient> {
        PdRpcClient::new(
            config.clone(),
            |security_mgr, store_health| {
                TikvConnect::new(security_mgr, config.timeout, store_health)
            },
            |security_mgr| {
                RetryClient::connect(pd_endpoints, security_mgr, config.timeout, config.pd_retry)
            },
            enable_codec,
        )
        .await
    }

    pub(crate) async fn cluster_id(&self) -> u64 {
        self.pd.cluster_id().await
    }
}

impl<KvC: KvConnect + Send + Sync + 'static, Cl> PdRpcClient<KvC, Cl> {
    const KEYSPACE_PREFIX_LEN: usize = 4;

    fn infer_bucket_keyspace(encoded_bucket_keys: &[Vec<u8>]) -> Option<(Keyspace, KeyMode)> {
        let mut inferred = None;
        for encoded_key in encoded_bucket_keys {
            let mut decoded_key = encoded_key.clone();
            codec::decode_bytes_in_place(&mut decoded_key, false).ok()?;
            if decoded_key.is_empty() {
                continue;
            }

            let key_mode = match decoded_key.first().copied() {
                Some(b'r') => KeyMode::Raw,
                Some(b'x') => KeyMode::Txn,
                _ => return None,
            };
            let keyspace_id = parse_keyspace_id(&decoded_key).ok()?;

            match inferred {
                None => inferred = Some((keyspace_id, key_mode)),
                Some((expected_id, expected_mode))
                    if expected_mode == key_mode
                        && (keyspace_id == expected_id
                            || (decoded_key.len() == Self::KEYSPACE_PREFIX_LEN
                                && keyspace_id == expected_id.saturating_add(1))) => {}
                Some(_) => return None,
            }
        }

        inferred.map(|(keyspace_id, key_mode)| (Keyspace::Enable { keyspace_id }, key_mode))
    }

    fn decode_region_load(
        mut loaded: RegionLoadResult,
        enable_codec: bool,
    ) -> Result<RegionLoadResult> {
        if enable_codec {
            codec::decode_bytes_in_place(&mut loaded.region.region.start_key, false)?;
            codec::decode_bytes_in_place(&mut loaded.region.region.end_key, false)?;
            if let Some(buckets) = loaded.buckets.as_mut() {
                if let Some((keyspace, key_mode)) = Self::infer_bucket_keyspace(&buckets.keys) {
                    buckets.keys =
                        decode_bucket_keys(std::mem::take(&mut buckets.keys), keyspace, key_mode)?;
                } else {
                    for key in &mut buckets.keys {
                        codec::decode_bytes_in_place(key, false)?;
                    }
                }
            }
        }
        Ok(loaded)
    }

    pub async fn new<PdFut, MakeKvC, MakePd>(
        config: Config,
        kv_connect: MakeKvC,
        pd: MakePd,
        enable_codec: bool,
    ) -> Result<PdRpcClient<KvC, Cl>>
    where
        PdFut: Future<Output = Result<RetryClient<Cl>>>,
        MakeKvC: FnOnce(Arc<SecurityManager>, StoreHealthMap) -> KvC,
        MakePd: FnOnce(Arc<SecurityManager>) -> PdFut,
    {
        let security_mgr = Arc::new(
            if let (Some(ca_path), Some(cert_path), Some(key_path)) =
                (&config.ca_path, &config.cert_path, &config.key_path)
            {
                SecurityManager::load(ca_path, cert_path, key_path)?
            } else {
                SecurityManager::default()
            },
        );

        let pd = Arc::new(pd(security_mgr.clone()).await?);
        let store_health = StoreHealthMap::default();
        let kv_client_cache = Default::default();
        Ok(PdRpcClient {
            pd: pd.clone(),
            kv_client_cache,
            kv_connect: kv_connect(security_mgr, store_health.clone()),
            store_health,
            safe_ts: SafeTsManager::default(),
            enable_codec,
            region_cache: RegionCache::new_with_ttl(
                pd,
                config.region_cache_ttl,
                config.region_cache_ttl_jitter,
            ),
        })
    }

    pub(crate) async fn load_region_by_key_with_buckets(
        &self,
        key: &Key,
    ) -> Result<RegionLoadResult>
    where
        RetryClient<Cl>: RetryClientTrait + Send + Sync,
    {
        let enable_codec = self.enable_codec;
        let key = if enable_codec {
            key.to_encoded()
        } else {
            key.clone()
        };
        let loaded = self
            .region_cache
            .read_through_region_by_key_with_buckets(key)
            .await?;
        Self::decode_region_load(loaded, enable_codec)
    }

    pub(crate) async fn load_region_by_id_with_buckets(
        &self,
        id: RegionId,
    ) -> Result<RegionLoadResult>
    where
        RetryClient<Cl>: RetryClientTrait + Send + Sync,
    {
        let loaded = self
            .region_cache
            .read_through_region_by_id_with_buckets(id)
            .await?;
        Self::decode_region_load(loaded, self.enable_codec)
    }

    async fn kv_client(&self, address: &str) -> Result<KvC::KvClient> {
        // Avoid repeated concurrent dial attempts for the same address.
        let cached = { self.kv_client_cache.read().await.get(address).cloned() };
        let cell = match cached {
            Some(cell) => cell,
            None => {
                let new = Arc::new(OnceCell::new());
                self.kv_client_cache
                    .write()
                    .await
                    .entry(address.to_owned())
                    .or_insert_with(|| new.clone())
                    .clone()
            }
        };

        let client = cell
            .get_or_try_init(|| async {
                info!("connect to tikv endpoint: {:?}", address);
                self.kv_connect.connect(address).await
            })
            .await?;
        Ok(client.clone())
    }
}

impl<KvC: KvConnect + Send + Sync + 'static> PdRpcClient<KvC, Cluster> {
    pub(crate) fn min_safe_ts(&self, txn_scope: &str) -> u64 {
        self.safe_ts.get_min_safe_ts(txn_scope)
    }

    pub(crate) async fn refresh_safe_ts_cache_once(&self) -> Result<()> {
        struct Provider<'a, KvC: KvConnect + Send + Sync + 'static> {
            pd: &'a PdRpcClient<KvC, Cluster>,
        }

        #[async_trait]
        impl<KvC: KvConnect + Send + Sync + 'static> StoreSafeTsProvider for Provider<'_, KvC> {
            async fn get_store_safe_ts(&self, store: &metapb::Store) -> Result<u64> {
                let addr = SafeTsManager::store_safe_ts_address(store);
                let client = self.pd.kv_client(addr).await?;

                let req = StoreSafeTsRequest::new_all_ranges();
                let resp = client.dispatch(&req).await?;
                let resp = resp
                    .downcast::<kvrpcpb::StoreSafeTsResponse>()
                    .map_err(|_| crate::internal_err!("unexpected StoreSafeTS response type"))?;
                Ok(resp.safe_ts)
            }
        }

        let stores = self.region_cache.read_through_all_stores().await?;
        let provider = Provider { pd: self };
        self.safe_ts
            .update_safe_ts_once(&stores, None, &provider)
            .await
    }
}

fn make_key_range(start_key: Vec<u8>, end_key: Vec<u8>) -> kvrpcpb::KeyRange {
    let mut key_range = kvrpcpb::KeyRange::default();
    key_range.start_key = start_key;
    key_range.end_key = end_key;
    key_range
}

#[cfg(test)]
pub mod test {
    use futures::{pin_mut, StreamExt};

    use super::*;
    use crate::mock::*;
    use crate::region_cache::RegionLoadResult;
    use crate::request::{EncodeKeyspace, KeyMode, Keyspace};

    #[tokio::test]
    async fn test_kv_client_caching() {
        let client = pd_rpc_client().await;

        let addr1 = "foo";
        let addr2 = "bar";

        let kv1 = client.kv_client(addr1).await.unwrap();
        let kv2 = client.kv_client(addr2).await.unwrap();
        let kv3 = client.kv_client(addr2).await.unwrap();
        assert!(kv1.addr != kv2.addr);
        assert_eq!(kv2.addr, kv3.addr);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_kv_client_concurrent_connect_is_deduped_per_address() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::time::Duration;

        use tokio::sync::{watch, Barrier};

        #[derive(Clone)]
        struct CountingConnect {
            calls: Arc<AtomicUsize>,
            release_rx: watch::Receiver<bool>,
        }

        #[async_trait::async_trait]
        impl KvConnect for CountingConnect {
            type KvClient = MockKvClient;

            async fn connect(&self, address: &str) -> Result<Self::KvClient> {
                self.calls.fetch_add(1, Ordering::SeqCst);

                // Hold the dial so other tasks can race on `kv_client`.
                let mut rx = self.release_rx.clone();
                while !*rx.borrow() {
                    rx.changed().await.expect("watch sender dropped");
                }

                Ok(MockKvClient::new(address.to_owned(), None))
            }
        }

        let config = Config::default();

        let (release_tx, release_rx) = watch::channel(false);
        let calls = Arc::new(AtomicUsize::new(0));
        let connect = CountingConnect {
            calls: calls.clone(),
            release_rx,
        };

        let client = PdRpcClient::new(
            config.clone(),
            |_, _| connect.clone(),
            |sm| {
                futures::future::ok(crate::pd::RetryClient::new_with_cluster(
                    sm,
                    config.timeout,
                    config.pd_retry,
                    MockCluster,
                ))
            },
            false,
        )
        .await
        .unwrap();
        let client = Arc::new(client);

        let addr = "same-addr";
        let task_count = 16usize;
        let start = Arc::new(Barrier::new(task_count + 1));

        let mut handles = Vec::with_capacity(task_count);
        for _ in 0..task_count {
            let c = client.clone();
            let start = start.clone();
            handles.push(tokio::spawn(async move {
                start.wait().await;
                c.kv_client(addr).await
            }));
        }

        start.wait().await;

        tokio::time::timeout(Duration::from_secs(1), async {
            while calls.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("connect not observed");

        // Give other tasks time to contend if we accidentally attempt multiple dials.
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "expected concurrent kv_client() to dial only once per address"
        );

        release_tx.send(true).unwrap();
        for h in handles {
            h.await.unwrap().unwrap();
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_group_keys_by_region() {
        let client = MockPdClient::default();

        // NOTE: `group_keys_by_region` only batches consecutive keys that belong to the same
        // region. For optimal batching, callers should pre-group keys by region.
        let tasks: Vec<Key> = vec![
            vec![1].into(),
            vec![2].into(),
            vec![3].into(),
            vec![5, 2].into(),
            vec![12].into(),
            vec![11, 4].into(),
        ];

        let stream = Arc::new(client).group_keys_by_region(tasks.into_iter());
        pin_mut!(stream);

        let result: Vec<Key> = stream.next().await.unwrap().unwrap().0;
        assert_eq!(
            result,
            vec![
                vec![1].into(),
                vec![2].into(),
                vec![3].into(),
                vec![5, 2].into()
            ]
        );
        assert_eq!(
            stream.next().await.unwrap().unwrap().0,
            vec![vec![12].into(), vec![11, 4].into()]
        );
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn test_regions_for_range() {
        let client = Arc::new(MockPdClient::default());
        let k1: Key = vec![1].into();
        let k2: Key = vec![5, 2].into();
        let k3: Key = vec![11, 4].into();
        let range1 = (k1, k2.clone()).into();
        let stream = client.clone().regions_for_range(range1);
        pin_mut!(stream);
        assert_eq!(stream.next().await.unwrap().unwrap().id(), 1);
        assert!(stream.next().await.is_none());

        let range2 = (k2, k3).into();
        let stream = client.regions_for_range(range2);
        pin_mut!(stream);
        assert_eq!(stream.next().await.unwrap().unwrap().id(), 1);
        assert_eq!(stream.next().await.unwrap().unwrap().id(), 2);
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn test_group_ranges_by_region() {
        let client = Arc::new(MockPdClient::default());
        let k1 = vec![1];
        let k2 = vec![5, 2];
        let k3 = vec![11, 4];
        let k4 = vec![16, 4];
        let k5 = vec![250, 251];
        let k6 = vec![255, 251];
        let k_split = vec![10];
        let range1 = make_key_range(k1.clone(), k2.clone());
        let range2 = make_key_range(k1.clone(), k3.clone());
        let range3 = make_key_range(k2.clone(), k4.clone());
        let ranges = vec![range1, range2, range3];

        {
            let stream = client.clone().group_ranges_by_region(ranges);
            pin_mut!(stream);

            let ranges1 = stream.next().await.unwrap().unwrap();
            let ranges2 = stream.next().await.unwrap().unwrap();
            let ranges3 = stream.next().await.unwrap().unwrap();
            let ranges4 = stream.next().await.unwrap().unwrap();

            assert_eq!(ranges1.1.id(), 1);
            assert_eq!(
                ranges1.0,
                vec![
                    make_key_range(k1.clone(), k2.clone()),
                    make_key_range(k1.clone(), k_split.clone()),
                ]
            );
            assert_eq!(ranges2.1.id(), 2);
            assert_eq!(ranges2.0, vec![make_key_range(k_split.clone(), k3.clone())]);
            assert_eq!(ranges3.1.id(), 1);
            assert_eq!(ranges3.0, vec![make_key_range(k2.clone(), k_split.clone())]);
            assert_eq!(ranges4.1.id(), 2);
            assert_eq!(ranges4.0, vec![make_key_range(k_split, k4.clone())]);
            assert!(stream.next().await.is_none());
        }

        let range1 = make_key_range(k1.clone(), k2.clone());
        let range2 = make_key_range(k3.clone(), k4.clone());
        let range3 = make_key_range(k5.clone(), k6.clone());
        let ranges = vec![range1, range2, range3];
        let stream = client.group_ranges_by_region(ranges);
        pin_mut!(stream);
        let ranges1 = stream.next().await.unwrap().unwrap();
        let ranges2 = stream.next().await.unwrap().unwrap();
        let ranges3 = stream.next().await.unwrap().unwrap();
        assert_eq!(ranges1.1.id(), 1);
        assert_eq!(ranges1.0, vec![make_key_range(k1, k2)]);
        assert_eq!(ranges2.1.id(), 2);
        assert_eq!(ranges2.0, vec![make_key_range(k3, k4)]);
        assert_eq!(ranges3.1.id(), 3);
        assert_eq!(ranges3.0, vec![make_key_range(k5, k6)]);
    }

    #[tokio::test]
    async fn test_load_region_by_key_with_buckets_uses_mock_cluster() {
        let client = pd_rpc_client().await;

        let loaded = client
            .load_region_by_key_with_buckets(&Key::from(vec![5]))
            .await
            .unwrap();

        assert_eq!(loaded.region, MockPdClient::region1());
        assert_eq!(
            loaded.buckets,
            Some(region_buckets(&MockPdClient::region1()))
        );
    }

    #[tokio::test]
    async fn test_load_region_by_id_with_buckets_uses_mock_cluster() {
        let client = pd_rpc_client().await;

        let loaded = client.load_region_by_id_with_buckets(2).await.unwrap();

        assert_eq!(loaded.region, MockPdClient::region2());
        assert_eq!(
            loaded.buckets,
            Some(region_buckets(&MockPdClient::region2()))
        );
    }

    #[test]
    fn test_decode_region_load_decodes_bucket_keys_when_codec_enabled() {
        let expected_region = MockPdClient::region2();
        let mut encoded_region = expected_region.clone();
        encoded_region.region.start_key = Key::from(expected_region.region.start_key.clone())
            .to_encoded()
            .into();
        encoded_region.region.end_key = Key::from(expected_region.region.end_key.clone())
            .to_encoded()
            .into();

        let mut encoded_buckets = region_buckets(&expected_region);
        encoded_buckets.keys = encoded_buckets
            .keys
            .into_iter()
            .map(|key| Key::from(key).to_encoded().into())
            .collect();

        let decoded = PdRpcClient::<MockKvConnect, MockCluster>::decode_region_load(
            RegionLoadResult::new(encoded_region, Some(encoded_buckets)),
            true,
        )
        .unwrap();

        assert_eq!(decoded.region, expected_region);
        assert_eq!(
            decoded.buckets,
            Some(region_buckets(&MockPdClient::region2()))
        );
    }

    #[test]
    fn test_decode_region_load_decodes_keyspace_bucket_keys_when_codec_enabled() {
        let keyspace = Keyspace::Enable { keyspace_id: 42 };
        let region_start: Vec<u8> = Key::from(Vec::<u8>::new())
            .encode_keyspace(keyspace, KeyMode::Raw)
            .into();
        let region_end = vec![b'r', 0, 0, 43];

        let mut expected_region = MockPdClient::region1();
        expected_region.region.start_key = region_start.clone();
        expected_region.region.end_key = region_end.clone();

        let mut encoded_region = expected_region.clone();
        encoded_region.region.start_key = Key::from(region_start.clone()).to_encoded().into();
        encoded_region.region.end_key = Key::from(region_end.clone()).to_encoded().into();

        let bucket_inside: Vec<u8> = Key::from(b"a".to_vec())
            .encode_keyspace(keyspace, KeyMode::Raw)
            .into();
        let encoded_buckets = crate::proto::metapb::Buckets {
            region_id: expected_region.id(),
            version: 101,
            keys: vec![
                Key::from(region_start).to_encoded().into(),
                Key::from(bucket_inside).to_encoded().into(),
                Key::from(region_end).to_encoded().into(),
            ],
            ..Default::default()
        };

        let decoded = PdRpcClient::<MockKvConnect, MockCluster>::decode_region_load(
            RegionLoadResult::new(encoded_region, Some(encoded_buckets)),
            true,
        )
        .unwrap();

        assert_eq!(decoded.region, expected_region);
        assert_eq!(
            decoded.buckets.unwrap().keys,
            vec![Vec::new(), b"a".to_vec(), Vec::new()]
        );
    }
}
