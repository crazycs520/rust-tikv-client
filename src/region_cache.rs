// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use rand::Rng;
use tokio::sync::Mutex;
use tokio::sync::Notify;
use tokio::sync::RwLock;

use crate::pd::Cluster;
use crate::pd::RetryClient;
use crate::pd::RetryClientTrait;
use crate::proto::metapb::Store;
use crate::proto::metapb::{self};
use crate::region::RegionId;
use crate::region::RegionVerId;
use crate::region::RegionWithLeader;
use crate::region::StoreId;
use crate::Key;
use crate::Result;

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct RegionLoadResult {
    pub(crate) region: RegionWithLeader,
    pub(crate) buckets: Option<metapb::Buckets>,
}

impl RegionLoadResult {
    pub(crate) fn new(region: RegionWithLeader, buckets: Option<metapb::Buckets>) -> Self {
        Self { region, buckets }
    }

    pub(crate) fn into_region(self) -> RegionWithLeader {
        self.region
    }
}

impl From<RegionWithLeader> for RegionLoadResult {
    fn from(region: RegionWithLeader) -> Self {
        Self::new(region, None)
    }
}

/// The cached region entry along with its expiration timestamp.
///
/// `ttl_epoch_sec` is an epoch timestamp in seconds. It is updated on access (atomically) to
/// approximate an "idle TTL" (hot regions stay cached).
struct CachedRegion {
    region: RegionWithLeader,
    buckets: Option<metapb::Buckets>,
    ttl_epoch_sec: AtomicI64,
}

impl CachedRegion {
    fn new(
        region: RegionWithLeader,
        buckets: Option<metapb::Buckets>,
        ttl_epoch_sec: i64,
    ) -> CachedRegion {
        CachedRegion {
            region,
            buckets,
            ttl_epoch_sec: AtomicI64::new(ttl_epoch_sec),
        }
    }

    fn to_region_load(&self) -> RegionLoadResult {
        RegionLoadResult::new(self.region.clone(), self.buckets.clone())
    }
}

#[derive(Clone, Copy, Debug)]
struct RegionCacheTtl {
    base_sec: i64,
    jitter_sec: i64,
}

impl RegionCacheTtl {
    fn new(base: Duration, jitter: Duration) -> RegionCacheTtl {
        let base_sec = i64::try_from(base.as_secs()).unwrap_or(i64::MAX);
        let jitter_sec = i64::try_from(jitter.as_secs()).unwrap_or(i64::MAX);
        RegionCacheTtl {
            base_sec,
            jitter_sec,
        }
    }

    fn is_enabled(&self) -> bool {
        self.base_sec > 0
    }

    fn next_ttl(self, now_epoch_sec: i64) -> i64 {
        if !self.is_enabled() {
            return i64::MAX;
        }

        let jitter = if self.jitter_sec > 0 {
            rand::thread_rng().gen_range(0..self.jitter_sec)
        } else {
            0
        };
        now_epoch_sec
            .saturating_add(self.base_sec)
            .saturating_add(jitter)
    }

    /// Returns `true` if the TTL is still valid (and may refresh it); otherwise `false`.
    fn check_and_refresh(self, ttl_epoch_sec: &AtomicI64, now_epoch_sec: i64) -> bool {
        if !self.is_enabled() {
            return true;
        }

        let mut new_ttl = 0;
        loop {
            let ttl = ttl_epoch_sec.load(Ordering::Relaxed);
            if now_epoch_sec > ttl {
                return false;
            }

            // Avoid refreshing TTL too frequently. We only refresh when it's within the base TTL
            // window (the remaining time is <= base_sec).
            if ttl > now_epoch_sec.saturating_add(self.base_sec) {
                return true;
            }

            if new_ttl == 0 {
                new_ttl = self.next_ttl(now_epoch_sec);
            }

            if ttl_epoch_sec
                .compare_exchange(ttl, new_ttl, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                return true;
            }
        }
    }
}

fn now_epoch_sec() -> i64 {
    use std::time::SystemTime;
    use std::time::UNIX_EPOCH;

    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs(),
    )
    .unwrap_or(0)
}

struct RegionCacheMap {
    /// RegionVerID -> Region. It stores the concrete region caches.
    /// RegionVerID is the unique identifer of a region *across time*.
    ///
    /// The entry is removed by explicit invalidation or replaced by `add_region`. Additionally, a
    /// soft TTL is used to avoid keeping cold/stale regions forever.
    ver_id_to_region: HashMap<RegionVerId, CachedRegion>,
    /// Start_key -> RegionVerID
    ///
    /// Invariant: there are no intersecting regions in the map at any time.
    key_to_ver_id: BTreeMap<Key, RegionVerId>,
    /// RegionID -> RegionVerID. Note: regions with identical ID doesn't necessarily
    /// mean they are the same, they can be different regions across time.
    id_to_ver_id: HashMap<RegionId, RegionVerId>,
}

impl RegionCacheMap {
    fn new() -> RegionCacheMap {
        RegionCacheMap {
            ver_id_to_region: HashMap::new(),
            key_to_ver_id: BTreeMap::new(),
            id_to_ver_id: HashMap::new(),
        }
    }
}

pub struct RegionCache<Client = RetryClient<Cluster>> {
    region_cache: RwLock<RegionCacheMap>,
    store_cache: RwLock<HashMap<StoreId, Store>>,
    in_flight_region_by_key: Mutex<HashMap<Key, Arc<Notify>>>,
    in_flight_region_by_id: Mutex<HashMap<RegionId, Arc<Notify>>>,
    inner_client: Arc<Client>,
    ttl: RegionCacheTtl,
}

impl<Client> RegionCache<Client> {
    pub fn new_with_ttl(
        inner_client: Arc<Client>,
        region_cache_ttl: Duration,
        region_cache_ttl_jitter: Duration,
    ) -> RegionCache<Client> {
        RegionCache {
            region_cache: RwLock::new(RegionCacheMap::new()),
            store_cache: RwLock::new(HashMap::new()),
            in_flight_region_by_key: Mutex::new(HashMap::new()),
            in_flight_region_by_id: Mutex::new(HashMap::new()),
            inner_client,
            ttl: RegionCacheTtl::new(region_cache_ttl, region_cache_ttl_jitter),
        }
    }
}

impl<C: RetryClientTrait + Send + Sync> RegionCache<C> {
    fn cached_region_load_by_key(
        &self,
        cache: &RegionCacheMap,
        key: &Key,
        now_epoch_sec: i64,
        require_buckets: bool,
    ) -> Option<RegionLoadResult> {
        let (_, candidate_ver_id) = cache.key_to_ver_id.range(..=key.clone()).next_back()?;
        let cached = cache.ver_id_to_region.get(candidate_ver_id)?;
        if require_buckets && cached.buckets.is_none() {
            return None;
        }
        if !self
            .ttl
            .check_and_refresh(&cached.ttl_epoch_sec, now_epoch_sec)
        {
            return None;
        }
        cached.region.contains(key).then(|| cached.to_region_load())
    }

    fn cached_region_load_by_ver_id(
        &self,
        cache: &RegionCacheMap,
        ver_id: &RegionVerId,
        now_epoch_sec: i64,
        require_buckets: bool,
    ) -> Option<RegionLoadResult> {
        let cached = cache.ver_id_to_region.get(ver_id)?;
        if require_buckets && cached.buckets.is_none() {
            return None;
        }
        self.ttl
            .check_and_refresh(&cached.ttl_epoch_sec, now_epoch_sec)
            .then(|| cached.to_region_load())
    }

    fn cached_region_load_by_id(
        &self,
        cache: &RegionCacheMap,
        id: RegionId,
        now_epoch_sec: i64,
        require_buckets: bool,
    ) -> Option<RegionLoadResult> {
        let ver_id = cache.id_to_ver_id.get(&id)?;
        self.cached_region_load_by_ver_id(cache, ver_id, now_epoch_sec, require_buckets)
    }

    async fn get_region_load_by_key(
        &self,
        key: &Key,
        require_buckets: bool,
    ) -> Result<RegionLoadResult> {
        let key = key.clone();
        loop {
            // Fast path: cache hit.
            let now = now_epoch_sec();
            {
                let region_cache_guard = self.region_cache.read().await;
                if let Some(cached) =
                    self.cached_region_load_by_key(&region_cache_guard, &key, now, require_buckets)
                {
                    return Ok(cached);
                }
            }

            // Slow path: join an in-flight load, or become the loader.
            let (notify, should_fetch) = {
                let mut in_flight = self.in_flight_region_by_key.lock().await;
                match in_flight.entry(key.clone()) {
                    std::collections::hash_map::Entry::Occupied(e) => (e.get().clone(), false),
                    std::collections::hash_map::Entry::Vacant(e) => {
                        let notify = Arc::new(Notify::new());
                        e.insert(notify.clone());
                        (notify, true)
                    }
                }
            };

            if !should_fetch {
                notify.notified().await;
                continue;
            }

            // Double-check after becoming the loader (another path may have filled the cache).
            let now = now_epoch_sec();
            {
                let region_cache_guard = self.region_cache.read().await;
                if let Some(cached) =
                    self.cached_region_load_by_key(&region_cache_guard, &key, now, require_buckets)
                {
                    let mut in_flight = self.in_flight_region_by_key.lock().await;
                    in_flight.remove(&key);
                    drop(in_flight);
                    notify.notify_waiters();
                    return Ok(cached);
                }
            }

            // Fetch from PD without holding any cache locks.
            let fetched = if require_buckets {
                self.read_through_region_by_key_with_buckets(key.clone())
                    .await
            } else {
                self.read_through_region_by_key(key.clone())
                    .await
                    .map(RegionLoadResult::from)
            };

            // Always clear the in-flight marker first, then wake waiters.
            let mut in_flight = self.in_flight_region_by_key.lock().await;
            in_flight.remove(&key);
            drop(in_flight);
            notify.notify_waiters();
            return fetched;
        }
    }

    // Retrieve cache entry by key. If there's no entry, query PD and update cache.
    pub async fn get_region_by_key(&self, key: &Key) -> Result<RegionWithLeader> {
        self.get_region_load_by_key(key, false)
            .await
            .map(RegionLoadResult::into_region)
    }

    /// Returns the region for `key` together with its cached bucket version.
    pub async fn get_region_with_bucket_version_by_key(
        &self,
        key: &Key,
    ) -> Result<(RegionWithLeader, u64)> {
        let loaded = self.get_region_load_by_key(key, true).await?;
        let bucket_version = loaded.buckets.as_ref().map_or(0, |buckets| buckets.version);
        Ok((loaded.region, bucket_version))
    }

    // Retrieve cache entry by RegionId. If there's no entry, query PD and update cache.
    pub async fn get_region_by_id(&self, id: RegionId) -> Result<RegionWithLeader> {
        loop {
            // Fast path: cache hit.
            let now = now_epoch_sec();
            {
                let region_cache_guard = self.region_cache.read().await;
                if let Some(cached) =
                    self.cached_region_load_by_id(&region_cache_guard, id, now, false)
                {
                    return Ok(cached.into_region());
                }
            }

            // Slow path: join an in-flight load, or become the loader.
            let (notify, should_fetch) = {
                let mut in_flight = self.in_flight_region_by_id.lock().await;
                match in_flight.entry(id) {
                    std::collections::hash_map::Entry::Occupied(e) => (e.get().clone(), false),
                    std::collections::hash_map::Entry::Vacant(e) => {
                        let notify = Arc::new(Notify::new());
                        e.insert(notify.clone());
                        (notify, true)
                    }
                }
            };

            if !should_fetch {
                notify.notified().await;
                continue;
            }

            // Double-check after becoming the loader (another path may have filled the cache).
            let now = now_epoch_sec();
            {
                let region_cache_guard = self.region_cache.read().await;
                if let Some(cached) =
                    self.cached_region_load_by_id(&region_cache_guard, id, now, false)
                {
                    let mut in_flight = self.in_flight_region_by_id.lock().await;
                    in_flight.remove(&id);
                    drop(in_flight);
                    notify.notify_waiters();
                    return Ok(cached.into_region());
                }
            }

            // Fetch from PD without holding any cache locks.
            let fetched = self.inner_client.clone().get_region_by_id(id).await;
            if let Ok(region) = &fetched {
                self.add_region(region.clone()).await;
            }

            // Always clear the in-flight marker first, then wake waiters.
            let mut in_flight = self.in_flight_region_by_id.lock().await;
            in_flight.remove(&id);
            drop(in_flight);
            notify.notify_waiters();
            return fetched;
        }
    }

    pub async fn get_store_by_id(&self, id: StoreId) -> Result<Store> {
        let store = self.store_cache.read().await.get(&id).cloned();
        match store {
            Some(store) => Ok(store),
            None => self.read_through_store_by_id(id).await,
        }
    }

    /// Force read through (query from PD) and update cache
    pub async fn read_through_region_by_key(&self, key: Key) -> Result<RegionWithLeader> {
        let region = self.inner_client.clone().get_region(key.into()).await?;
        self.add_region(region.clone()).await;
        Ok(region)
    }

    pub(crate) async fn read_through_region_by_key_with_buckets(
        &self,
        key: Key,
    ) -> Result<RegionLoadResult> {
        let loaded = self
            .inner_client
            .clone()
            .get_region_with_buckets(key.into())
            .await?;
        self.add_region_load(loaded.clone()).await;
        Ok(loaded)
    }

    pub(crate) async fn read_through_region_by_id_with_buckets(
        &self,
        id: RegionId,
    ) -> Result<RegionLoadResult> {
        let loaded = self
            .inner_client
            .clone()
            .get_region_by_id_with_buckets(id)
            .await?;
        self.add_region_load(loaded.clone()).await;
        Ok(loaded)
    }

    pub(crate) async fn get_cached_region_buckets_by_id(
        &self,
        id: RegionId,
    ) -> Option<metapb::Buckets> {
        let now = now_epoch_sec();
        let region_cache_guard = self.region_cache.read().await;
        self.cached_region_load_by_id(&region_cache_guard, id, now, true)
            .and_then(|loaded| loaded.buckets)
    }

    pub(crate) async fn get_cached_region_buckets_by_ver_id(
        &self,
        ver_id: &RegionVerId,
    ) -> Option<metapb::Buckets> {
        let now = now_epoch_sec();
        let region_cache_guard = self.region_cache.read().await;
        self.cached_region_load_by_ver_id(&region_cache_guard, ver_id, now, true)
            .and_then(|loaded| loaded.buckets)
    }

    pub async fn update_region_buckets_from_mismatch(
        &self,
        ver_id: RegionVerId,
        version: u64,
        keys: Vec<Vec<u8>>,
    ) -> bool {
        let mut cache = self.region_cache.write().await;
        let Some(cached) = cache.ver_id_to_region.get_mut(&ver_id) else {
            return false;
        };
        let current_version = cached.buckets.as_ref().map_or(0, |buckets| buckets.version);
        if current_version >= version {
            return false;
        }

        cached.buckets = Some(metapb::Buckets {
            region_id: cached.region.id(),
            version,
            keys,
            ..Default::default()
        });
        cached
            .ttl_epoch_sec
            .store(self.ttl.next_ttl(now_epoch_sec()), Ordering::Relaxed);
        true
    }

    pub async fn refresh_region_buckets_if_stale(
        &self,
        ver_id: RegionVerId,
        latest_buckets_version: u64,
    ) -> Result<bool> {
        let region_id = {
            let cache = self.region_cache.read().await;
            let Some(cached) = cache.ver_id_to_region.get(&ver_id) else {
                return Ok(false);
            };
            let current_version = cached.buckets.as_ref().map_or(0, |buckets| buckets.version);
            if current_version >= latest_buckets_version {
                return Ok(false);
            }
            cached.region.id()
        };

        self.read_through_region_by_id_with_buckets(region_id)
            .await?;
        Ok(true)
    }

    async fn read_through_store_by_id(&self, id: StoreId) -> Result<Store> {
        let store = self.inner_client.clone().get_store(id).await?;
        self.store_cache.write().await.insert(id, store.clone());
        Ok(store)
    }

    pub async fn add_region(&self, region: RegionWithLeader) {
        self.add_region_load(RegionLoadResult::from(region)).await;
    }

    async fn add_region_load(&self, loaded: RegionLoadResult) {
        // Keep the critical section small: `RwLock` is global for region index invariants, so we
        // avoid any `.await` and do only local computations while holding it.
        let mut cache = self.region_cache.write().await;

        let now = now_epoch_sec();
        let ttl_epoch_sec = self.ttl.next_ttl(now);

        let RegionLoadResult {
            region,
            buckets: mut loaded_buckets,
        } = loaded;
        let ver_id = region.ver_id();
        if loaded_buckets.is_none() {
            loaded_buckets = cache
                .ver_id_to_region
                .get(&ver_id)
                .and_then(|cached| cached.buckets.clone());
        }

        let end_key = region.end_key();
        let mut to_be_removed: HashSet<RegionVerId> = HashSet::new();

        if let Some(ver_id) = cache.id_to_ver_id.get(&region.id()) {
            if ver_id != &region.ver_id() {
                to_be_removed.insert(ver_id.clone());
            }
        }

        // Collect overlapped regions by scanning backwards from `end_key`. This relies on the
        // invariant that cached regions are non-overlapping and ordered by their start key.
        //
        // NOTE: In TiKV an empty end_key means "+inf". We must treat it as always overlapping.
        let region_start_key = region.region.start_key.as_slice();
        let stale_start_keys = {
            let mut stale_start_keys = Vec::new();
            let mut search_range = {
                if end_key.is_empty() {
                    cache.key_to_ver_id.range(..)
                } else {
                    cache.key_to_ver_id.range(..end_key)
                }
            };
            while let Some((start_key_in_cache, ver_id_in_cache)) = search_range.next_back() {
                let Some(cached) = cache.ver_id_to_region.get(ver_id_in_cache) else {
                    stale_start_keys.push(start_key_in_cache.clone());
                    continue;
                };
                let end_key_in_cache = cached.region.region.end_key.as_slice();
                let overlaps = end_key_in_cache.is_empty() || end_key_in_cache > region_start_key;
                if overlaps {
                    to_be_removed.insert(ver_id_in_cache.clone());
                } else {
                    break;
                }
            }
            stale_start_keys
        };
        for start_key in stale_start_keys {
            cache.key_to_ver_id.remove(&start_key);
        }

        for ver_id in to_be_removed {
            let Some(region_to_remove) = cache.ver_id_to_region.remove(&ver_id) else {
                continue;
            };
            let start_key = region_to_remove.region.start_key();
            cache.key_to_ver_id.remove(&start_key);
            cache.id_to_ver_id.remove(&region_to_remove.region.id());
        }
        cache
            .key_to_ver_id
            .insert(region.start_key(), ver_id.clone());
        cache.id_to_ver_id.insert(region.id(), ver_id.clone());
        cache.ver_id_to_region.insert(
            ver_id,
            CachedRegion::new(region, loaded_buckets, ttl_epoch_sec),
        );
    }

    pub async fn update_leader(
        &self,
        ver_id: crate::region::RegionVerId,
        leader: metapb::Peer,
    ) -> Result<()> {
        let mut cache = self.region_cache.write().await;
        let region_entry = cache.ver_id_to_region.get_mut(&ver_id);
        if let Some(cached) = region_entry {
            cached.region.leader = Some(leader);
            cached
                .ttl_epoch_sec
                .store(self.ttl.next_ttl(now_epoch_sec()), Ordering::Relaxed);
        }

        Ok(())
    }

    pub async fn invalidate_region_cache(&self, ver_id: crate::region::RegionVerId) {
        let mut cache = self.region_cache.write().await;
        let region_entry = cache.ver_id_to_region.get(&ver_id);
        if let Some(region) = region_entry {
            let id = region.region.id();
            let start_key = region.region.start_key();
            cache.ver_id_to_region.remove(&ver_id);
            cache.id_to_ver_id.remove(&id);
            cache.key_to_ver_id.remove(&start_key);
        }
    }

    pub async fn invalidate_store_cache(&self, store_id: StoreId) {
        let mut cache = self.store_cache.write().await;
        cache.remove(&store_id);
    }

    pub async fn read_through_all_stores(&self) -> Result<Vec<Store>> {
        let stores = self
            .inner_client
            .clone()
            .get_all_stores()
            .await?
            .into_iter()
            .filter(is_valid_tikv_store)
            .collect::<Vec<_>>();
        for store in &stores {
            self.store_cache
                .write()
                .await
                .insert(store.id, store.clone());
        }
        Ok(stores)
    }
}

const ENGINE_LABEL_KEY: &str = "engine";
const ENGINE_LABEL_TIFLASH: &str = "tiflash";
const ENGINE_LABEL_TIFLASH_COMPUTE: &str = "tiflash_compute";

fn is_valid_tikv_store(store: &metapb::Store) -> bool {
    match metapb::StoreState::try_from(store.state) {
        Ok(metapb::StoreState::Tombstone) => return false,
        Ok(_) => {}
        Err(_) => return false,
    }
    let is_tiflash = store.labels.iter().any(|label| {
        label.key == ENGINE_LABEL_KEY
            && (label.value == ENGINE_LABEL_TIFLASH || label.value == ENGINE_LABEL_TIFLASH_COMPUTE)
    });
    !is_tiflash
}

#[cfg(test)]
mod test {
    use std::collections::BTreeMap;
    use std::collections::HashMap;
    use std::collections::HashSet;
    use std::sync::atomic::AtomicU64;
    use std::sync::atomic::Ordering;
    use std::sync::atomic::Ordering::SeqCst;
    use std::sync::Arc;
    use std::time::Duration;

    use async_trait::async_trait;
    use tokio::sync::Mutex;
    use tokio::sync::Notify;

    use super::RegionCache;
    use crate::common::Error;
    use crate::mock::region_buckets;
    use crate::pd::RetryClientTrait;
    use crate::proto::keyspacepb;
    use crate::proto::metapb::RegionEpoch;
    use crate::proto::metapb::{self};
    use crate::region::RegionId;
    use crate::region::RegionWithLeader;
    use crate::region_cache::is_valid_tikv_store;
    use crate::region_cache::RegionLoadResult;
    use crate::Key;
    use crate::Result;

    #[derive(Default)]
    struct MockRetryClient {
        pub regions: Mutex<HashMap<RegionId, RegionWithLeader>>,
        pub get_region_count: AtomicU64,
        pub get_region_with_buckets_count: AtomicU64,
    }

    struct BlockingGetRegionByIdClient {
        region: RegionWithLeader,
        started: Notify,
        release: Notify,
        calls: AtomicU64,
    }

    impl BlockingGetRegionByIdClient {
        fn new(region: RegionWithLeader) -> BlockingGetRegionByIdClient {
            BlockingGetRegionByIdClient {
                region,
                started: Notify::new(),
                release: Notify::new(),
                calls: AtomicU64::new(0),
            }
        }
    }

    struct FlakyGetRegionByIdClient {
        region: RegionWithLeader,
        started: Notify,
        release_first: Notify,
        calls: AtomicU64,
    }

    impl FlakyGetRegionByIdClient {
        fn new(region: RegionWithLeader) -> FlakyGetRegionByIdClient {
            FlakyGetRegionByIdClient {
                region,
                started: Notify::new(),
                release_first: Notify::new(),
                calls: AtomicU64::new(0),
            }
        }
    }

    struct BlockingGetRegionByKeyClient {
        region: RegionWithLeader,
        expected_key: Vec<u8>,
        started: Notify,
        release: Notify,
        calls: AtomicU64,
    }

    impl BlockingGetRegionByKeyClient {
        fn new(region: RegionWithLeader, expected_key: Vec<u8>) -> BlockingGetRegionByKeyClient {
            BlockingGetRegionByKeyClient {
                region,
                expected_key,
                started: Notify::new(),
                release: Notify::new(),
                calls: AtomicU64::new(0),
            }
        }
    }

    struct FlakyGetRegionByKeyClient {
        region: RegionWithLeader,
        expected_key: Vec<u8>,
        started: Notify,
        release_first: Notify,
        calls: AtomicU64,
    }

    impl FlakyGetRegionByKeyClient {
        fn new(region: RegionWithLeader, expected_key: Vec<u8>) -> FlakyGetRegionByKeyClient {
            FlakyGetRegionByKeyClient {
                region,
                expected_key,
                started: Notify::new(),
                release_first: Notify::new(),
                calls: AtomicU64::new(0),
            }
        }
    }

    #[async_trait]
    impl RetryClientTrait for MockRetryClient {
        async fn get_region(
            self: Arc<Self>,
            key: Vec<u8>,
        ) -> Result<crate::region::RegionWithLeader> {
            self.get_region_count.fetch_add(1, SeqCst);
            self.regions
                .lock()
                .await
                .iter()
                .filter(|(_, r)| r.contains(&key.clone().into()))
                .map(|(_, r)| r.clone())
                .next()
                .ok_or_else(|| Error::StringError("MockRetryClient: region not found".to_owned()))
        }

        async fn get_region_with_buckets(
            self: Arc<Self>,
            key: Vec<u8>,
        ) -> Result<RegionLoadResult> {
            self.get_region_with_buckets_count.fetch_add(1, SeqCst);
            self.regions
                .lock()
                .await
                .iter()
                .filter(|(_, r)| r.contains(&key.clone().into()))
                .map(|(_, r)| r.clone())
                .next()
                .map(|region| {
                    let buckets = region_buckets(&region);
                    RegionLoadResult::new(region, Some(buckets))
                })
                .ok_or_else(|| Error::StringError("MockRetryClient: region not found".to_owned()))
        }

        async fn get_region_by_id(
            self: Arc<Self>,
            region_id: crate::region::RegionId,
        ) -> Result<crate::region::RegionWithLeader> {
            self.get_region_count.fetch_add(1, SeqCst);
            self.regions
                .lock()
                .await
                .iter()
                .filter(|(id, _)| id == &&region_id)
                .map(|(_, r)| r.clone())
                .next()
                .ok_or_else(|| Error::StringError("MockRetryClient: region not found".to_owned()))
        }

        async fn get_region_by_id_with_buckets(
            self: Arc<Self>,
            region_id: crate::region::RegionId,
        ) -> Result<RegionLoadResult> {
            self.get_region_with_buckets_count.fetch_add(1, SeqCst);
            self.regions
                .lock()
                .await
                .iter()
                .filter(|(id, _)| id == &&region_id)
                .map(|(_, r)| r.clone())
                .next()
                .map(|region| {
                    let buckets = region_buckets(&region);
                    RegionLoadResult::new(region, Some(buckets))
                })
                .ok_or_else(|| Error::StringError("MockRetryClient: region not found".to_owned()))
        }

        async fn get_store(
            self: Arc<Self>,
            _id: crate::region::StoreId,
        ) -> Result<crate::proto::metapb::Store> {
            Err(Error::Unimplemented)
        }

        async fn get_all_stores(self: Arc<Self>) -> Result<Vec<crate::proto::metapb::Store>> {
            Err(Error::Unimplemented)
        }

        async fn get_timestamp(self: Arc<Self>) -> Result<crate::proto::pdpb::Timestamp> {
            Err(Error::Unimplemented)
        }

        async fn get_min_ts(self: Arc<Self>) -> Result<crate::proto::pdpb::Timestamp> {
            Err(Error::Unimplemented)
        }

        async fn update_safepoint(self: Arc<Self>, _safepoint: u64) -> Result<bool> {
            Err(Error::Unimplemented)
        }

        async fn load_keyspace(&self, _keyspace: &str) -> Result<keyspacepb::KeyspaceMeta> {
            Err(Error::Unimplemented)
        }
    }

    #[async_trait]
    impl RetryClientTrait for BlockingGetRegionByIdClient {
        async fn get_region(self: Arc<Self>, _key: Vec<u8>) -> Result<RegionWithLeader> {
            Err(Error::Unimplemented)
        }

        async fn get_region_by_id(
            self: Arc<Self>,
            region_id: RegionId,
        ) -> Result<RegionWithLeader> {
            self.calls.fetch_add(1, SeqCst);
            self.started.notify_one();
            self.release.notified().await;
            if region_id != self.region.id() {
                return Err(Error::StringError(
                    "MockRetryClient: region not found".to_owned(),
                ));
            }
            Ok(self.region.clone())
        }

        async fn get_store(self: Arc<Self>, _id: crate::region::StoreId) -> Result<metapb::Store> {
            Err(Error::Unimplemented)
        }

        async fn get_all_stores(self: Arc<Self>) -> Result<Vec<metapb::Store>> {
            Err(Error::Unimplemented)
        }

        async fn get_timestamp(self: Arc<Self>) -> Result<crate::proto::pdpb::Timestamp> {
            Err(Error::Unimplemented)
        }

        async fn get_min_ts(self: Arc<Self>) -> Result<crate::proto::pdpb::Timestamp> {
            Err(Error::Unimplemented)
        }

        async fn update_safepoint(self: Arc<Self>, _safepoint: u64) -> Result<bool> {
            Err(Error::Unimplemented)
        }

        async fn load_keyspace(&self, _keyspace: &str) -> Result<keyspacepb::KeyspaceMeta> {
            Err(Error::Unimplemented)
        }
    }

    #[async_trait]
    impl RetryClientTrait for FlakyGetRegionByIdClient {
        async fn get_region(self: Arc<Self>, _key: Vec<u8>) -> Result<RegionWithLeader> {
            Err(Error::Unimplemented)
        }

        async fn get_region_by_id(
            self: Arc<Self>,
            region_id: RegionId,
        ) -> Result<RegionWithLeader> {
            let call = self.calls.fetch_add(1, SeqCst);
            if call == 0 {
                self.started.notify_one();
                self.release_first.notified().await;
                return Err(Error::StringError("injected error".to_owned()));
            }
            if region_id != self.region.id() {
                return Err(Error::StringError(
                    "MockRetryClient: region not found".to_owned(),
                ));
            }
            Ok(self.region.clone())
        }

        async fn get_store(self: Arc<Self>, _id: crate::region::StoreId) -> Result<metapb::Store> {
            Err(Error::Unimplemented)
        }

        async fn get_all_stores(self: Arc<Self>) -> Result<Vec<metapb::Store>> {
            Err(Error::Unimplemented)
        }

        async fn get_timestamp(self: Arc<Self>) -> Result<crate::proto::pdpb::Timestamp> {
            Err(Error::Unimplemented)
        }

        async fn get_min_ts(self: Arc<Self>) -> Result<crate::proto::pdpb::Timestamp> {
            Err(Error::Unimplemented)
        }

        async fn update_safepoint(self: Arc<Self>, _safepoint: u64) -> Result<bool> {
            Err(Error::Unimplemented)
        }

        async fn load_keyspace(&self, _keyspace: &str) -> Result<keyspacepb::KeyspaceMeta> {
            Err(Error::Unimplemented)
        }
    }

    #[async_trait]
    impl RetryClientTrait for BlockingGetRegionByKeyClient {
        async fn get_region(self: Arc<Self>, key: Vec<u8>) -> Result<RegionWithLeader> {
            self.calls.fetch_add(1, SeqCst);
            assert_eq!(key, self.expected_key);
            self.started.notify_one();
            self.release.notified().await;
            Ok(self.region.clone())
        }

        async fn get_region_by_id(
            self: Arc<Self>,
            _region_id: RegionId,
        ) -> Result<RegionWithLeader> {
            Err(Error::Unimplemented)
        }

        async fn get_store(self: Arc<Self>, _id: crate::region::StoreId) -> Result<metapb::Store> {
            Err(Error::Unimplemented)
        }

        async fn get_all_stores(self: Arc<Self>) -> Result<Vec<metapb::Store>> {
            Err(Error::Unimplemented)
        }

        async fn get_timestamp(self: Arc<Self>) -> Result<crate::proto::pdpb::Timestamp> {
            Err(Error::Unimplemented)
        }

        async fn get_min_ts(self: Arc<Self>) -> Result<crate::proto::pdpb::Timestamp> {
            Err(Error::Unimplemented)
        }

        async fn update_safepoint(self: Arc<Self>, _safepoint: u64) -> Result<bool> {
            Err(Error::Unimplemented)
        }

        async fn load_keyspace(&self, _keyspace: &str) -> Result<keyspacepb::KeyspaceMeta> {
            Err(Error::Unimplemented)
        }
    }

    #[async_trait]
    impl RetryClientTrait for FlakyGetRegionByKeyClient {
        async fn get_region(self: Arc<Self>, key: Vec<u8>) -> Result<RegionWithLeader> {
            let call = self.calls.fetch_add(1, SeqCst);
            assert_eq!(key, self.expected_key);
            if call == 0 {
                self.started.notify_one();
                self.release_first.notified().await;
                return Err(Error::StringError("injected error".to_owned()));
            }
            Ok(self.region.clone())
        }

        async fn get_region_by_id(
            self: Arc<Self>,
            _region_id: RegionId,
        ) -> Result<RegionWithLeader> {
            Err(Error::Unimplemented)
        }

        async fn get_store(self: Arc<Self>, _id: crate::region::StoreId) -> Result<metapb::Store> {
            Err(Error::Unimplemented)
        }

        async fn get_all_stores(self: Arc<Self>) -> Result<Vec<metapb::Store>> {
            Err(Error::Unimplemented)
        }

        async fn get_timestamp(self: Arc<Self>) -> Result<crate::proto::pdpb::Timestamp> {
            Err(Error::Unimplemented)
        }

        async fn get_min_ts(self: Arc<Self>) -> Result<crate::proto::pdpb::Timestamp> {
            Err(Error::Unimplemented)
        }

        async fn update_safepoint(self: Arc<Self>, _safepoint: u64) -> Result<bool> {
            Err(Error::Unimplemented)
        }

        async fn load_keyspace(&self, _keyspace: &str) -> Result<keyspacepb::KeyspaceMeta> {
            Err(Error::Unimplemented)
        }
    }

    #[tokio::test]
    async fn cache_is_used() -> Result<()> {
        let retry_client = Arc::new(MockRetryClient::default());
        let cache = RegionCache::new_with_ttl(
            retry_client.clone(),
            Duration::from_secs(600),
            Duration::from_secs(60),
        );
        retry_client.regions.lock().await.insert(
            1,
            RegionWithLeader {
                region: metapb::Region {
                    id: 1,
                    start_key: vec![],
                    end_key: vec![100],
                    region_epoch: Some(RegionEpoch {
                        conf_ver: 0,
                        version: 0,
                    }),
                    ..Default::default()
                },
                leader: Some(metapb::Peer {
                    store_id: 1,
                    ..Default::default()
                }),
            },
        );
        retry_client.regions.lock().await.insert(
            2,
            RegionWithLeader {
                region: metapb::Region {
                    id: 2,
                    start_key: vec![101],
                    end_key: vec![],
                    region_epoch: Some(RegionEpoch {
                        conf_ver: 0,
                        version: 0,
                    }),
                    ..Default::default()
                },
                leader: Some(metapb::Peer {
                    store_id: 2,
                    ..Default::default()
                }),
            },
        );

        assert_eq!(retry_client.get_region_count.load(SeqCst), 0);

        // first query, read through
        assert_eq!(cache.get_region_by_id(1).await?.end_key(), vec![100].into());
        assert_eq!(retry_client.get_region_count.load(SeqCst), 1);

        // should read from cache
        assert_eq!(cache.get_region_by_id(1).await?.end_key(), vec![100].into());
        assert_eq!(retry_client.get_region_count.load(SeqCst), 1);

        // invalidate, should read through
        cache
            .invalidate_region_cache(cache.get_region_by_id(1).await?.ver_id())
            .await;
        assert_eq!(cache.get_region_by_id(1).await?.end_key(), vec![100].into());
        assert_eq!(retry_client.get_region_count.load(SeqCst), 2);

        // update leader should work
        cache
            .update_leader(
                cache.get_region_by_id(2).await?.ver_id(),
                metapb::Peer {
                    store_id: 102,
                    ..Default::default()
                },
            )
            .await?;
        assert_eq!(
            cache.get_region_by_id(2).await?.leader.unwrap().store_id,
            102
        );

        Ok(())
    }

    #[tokio::test]
    async fn cache_entry_expires_by_ttl() -> Result<()> {
        let retry_client = Arc::new(MockRetryClient::default());
        let cache = RegionCache::new_with_ttl(
            retry_client.clone(),
            Duration::from_secs(600),
            Duration::ZERO,
        );

        let region1 = region(1, vec![], vec![10]);
        retry_client.regions.lock().await.insert(1, region1);

        assert_eq!(retry_client.get_region_count.load(SeqCst), 0);
        let ver_id = cache.get_region_by_id(1).await?.ver_id();
        assert_eq!(retry_client.get_region_count.load(SeqCst), 1);

        // Force the cached entry to be expired, then verify it is reloaded from PD.
        {
            let guard = cache.region_cache.read().await;
            let cached = guard
                .ver_id_to_region
                .get(&ver_id)
                .expect("region must be cached after get_region_by_id");
            cached
                .ttl_epoch_sec
                .store(super::now_epoch_sec() - 1, Ordering::Relaxed);
        }

        cache.get_region_by_id(1).await?;
        assert_eq!(retry_client.get_region_count.load(SeqCst), 2);
        Ok(())
    }

    #[tokio::test]
    async fn test_get_region_by_id_deduplicates_in_flight_fetch() -> Result<()> {
        let client = Arc::new(BlockingGetRegionByIdClient::new(region(1, vec![], vec![])));
        let cache = Arc::new(RegionCache::new_with_ttl(
            client.clone(),
            Duration::from_secs(600),
            Duration::from_secs(60),
        ));

        let task1 = {
            let cache = cache.clone();
            tokio::spawn(async move { cache.get_region_by_id(1).await })
        };
        let task2 = {
            let cache = cache.clone();
            tokio::spawn(async move { cache.get_region_by_id(1).await })
        };

        client.started.notified().await;
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        assert_eq!(client.calls.load(SeqCst), 1);

        client.release.notify_one();

        let r1 = tokio::time::timeout(Duration::from_secs(1), task1)
            .await
            .expect("timeout waiting for task1")
            .expect("task1 panicked")?;
        let r2 = tokio::time::timeout(Duration::from_secs(1), task2)
            .await
            .expect("timeout waiting for task2")
            .expect("task2 panicked")?;

        assert_eq!(client.calls.load(SeqCst), 1);
        assert_eq!(r1.id(), 1);
        assert_eq!(r2.id(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_get_region_by_id_clears_in_flight_on_error() -> Result<()> {
        let client = Arc::new(FlakyGetRegionByIdClient::new(region(1, vec![], vec![])));
        let cache = Arc::new(RegionCache::new_with_ttl(
            client.clone(),
            Duration::from_secs(600),
            Duration::from_secs(60),
        ));

        let task1 = {
            let cache = cache.clone();
            tokio::spawn(async move { cache.get_region_by_id(1).await })
        };
        let task2 = {
            let cache = cache.clone();
            tokio::spawn(async move { cache.get_region_by_id(1).await })
        };

        client.started.notified().await;
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        assert_eq!(client.calls.load(SeqCst), 1);

        client.release_first.notify_one();

        let r1 = tokio::time::timeout(Duration::from_secs(1), task1)
            .await
            .expect("timeout waiting for task1")
            .expect("task1 panicked");
        let r2 = tokio::time::timeout(Duration::from_secs(1), task2)
            .await
            .expect("timeout waiting for task2")
            .expect("task2 panicked");

        assert_eq!(client.calls.load(SeqCst), 2);
        assert!(r1.is_err() ^ r2.is_err());
        let ok = r1.or(r2)?;
        assert_eq!(ok.id(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_get_region_by_key_deduplicates_in_flight_fetch() -> Result<()> {
        let expected_key = vec![5];
        let client = Arc::new(BlockingGetRegionByKeyClient::new(
            region(1, vec![], vec![]),
            expected_key.clone(),
        ));
        let cache = Arc::new(RegionCache::new_with_ttl(
            client.clone(),
            Duration::from_secs(600),
            Duration::from_secs(60),
        ));

        let key: Key = expected_key.clone().into();
        let task1 = {
            let cache = cache.clone();
            let key = key.clone();
            tokio::spawn(async move { cache.get_region_by_key(&key).await })
        };
        let task2 = {
            let cache = cache.clone();
            let key = key.clone();
            tokio::spawn(async move { cache.get_region_by_key(&key).await })
        };

        client.started.notified().await;
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        assert_eq!(client.calls.load(SeqCst), 1);

        client.release.notify_one();

        let r1 = tokio::time::timeout(Duration::from_secs(1), task1)
            .await
            .expect("timeout waiting for task1")
            .expect("task1 panicked")?;
        let r2 = tokio::time::timeout(Duration::from_secs(1), task2)
            .await
            .expect("timeout waiting for task2")
            .expect("task2 panicked")?;

        assert_eq!(client.calls.load(SeqCst), 1);
        assert_eq!(r1.id(), 1);
        assert_eq!(r2.id(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_get_region_by_key_clears_in_flight_on_error() -> Result<()> {
        let expected_key = vec![5];
        let client = Arc::new(FlakyGetRegionByKeyClient::new(
            region(1, vec![], vec![]),
            expected_key.clone(),
        ));
        let cache = Arc::new(RegionCache::new_with_ttl(
            client.clone(),
            Duration::from_secs(600),
            Duration::from_secs(60),
        ));

        let key: Key = expected_key.clone().into();
        let task1 = {
            let cache = cache.clone();
            let key = key.clone();
            tokio::spawn(async move { cache.get_region_by_key(&key).await })
        };
        let task2 = {
            let cache = cache.clone();
            let key = key.clone();
            tokio::spawn(async move { cache.get_region_by_key(&key).await })
        };

        client.started.notified().await;
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        assert_eq!(client.calls.load(SeqCst), 1);

        client.release_first.notify_one();

        let r1 = tokio::time::timeout(Duration::from_secs(1), task1)
            .await
            .expect("timeout waiting for task1")
            .expect("task1 panicked");
        let r2 = tokio::time::timeout(Duration::from_secs(1), task2)
            .await
            .expect("timeout waiting for task2")
            .expect("task2 panicked");

        assert_eq!(client.calls.load(SeqCst), 2);
        assert!(r1.is_err() ^ r2.is_err());
        let ok = r1.or(r2)?;
        assert_eq!(ok.id(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_add_disjoint_regions() {
        let retry_client = Arc::new(MockRetryClient::default());
        let cache = RegionCache::new_with_ttl(
            retry_client.clone(),
            Duration::from_secs(600),
            Duration::from_secs(60),
        );
        let region1 = region(1, vec![], vec![10]);
        let region2 = region(2, vec![10], vec![20]);
        let region3 = region(3, vec![30], vec![]);
        cache.add_region(region1.clone()).await;
        cache.add_region(region2.clone()).await;
        cache.add_region(region3.clone()).await;

        let mut expected_cache = BTreeMap::new();
        expected_cache.insert(vec![].into(), region1);
        expected_cache.insert(vec![10].into(), region2);
        expected_cache.insert(vec![30].into(), region3);

        assert(&cache, &expected_cache).await
    }

    #[tokio::test]
    async fn test_add_intersecting_regions() {
        let retry_client = Arc::new(MockRetryClient::default());
        let cache = RegionCache::new_with_ttl(
            retry_client.clone(),
            Duration::from_secs(600),
            Duration::from_secs(60),
        );

        cache.add_region(region(1, vec![], vec![10])).await;
        cache.add_region(region(2, vec![10], vec![20])).await;
        cache.add_region(region(3, vec![30], vec![40])).await;
        cache.add_region(region(4, vec![50], vec![60])).await;
        cache.add_region(region(5, vec![20], vec![35])).await;

        let mut expected_cache: BTreeMap<Key, _> = BTreeMap::new();
        expected_cache.insert(vec![].into(), region(1, vec![], vec![10]));
        expected_cache.insert(vec![10].into(), region(2, vec![10], vec![20]));
        expected_cache.insert(vec![20].into(), region(5, vec![20], vec![35]));
        expected_cache.insert(vec![50].into(), region(4, vec![50], vec![60]));
        assert(&cache, &expected_cache).await;

        cache.add_region(region(6, vec![15], vec![25])).await;
        let mut expected_cache = BTreeMap::new();
        expected_cache.insert(vec![].into(), region(1, vec![], vec![10]));
        expected_cache.insert(vec![15].into(), region(6, vec![15], vec![25]));
        expected_cache.insert(vec![50].into(), region(4, vec![50], vec![60]));
        assert(&cache, &expected_cache).await;

        cache.add_region(region(7, vec![20], vec![])).await;
        let mut expected_cache = BTreeMap::new();
        expected_cache.insert(vec![].into(), region(1, vec![], vec![10]));
        expected_cache.insert(vec![20].into(), region(7, vec![20], vec![]));
        assert(&cache, &expected_cache).await;

        cache.add_region(region(8, vec![], vec![15])).await;
        let mut expected_cache = BTreeMap::new();
        expected_cache.insert(vec![].into(), region(8, vec![], vec![15]));
        expected_cache.insert(vec![20].into(), region(7, vec![20], vec![]));
        assert(&cache, &expected_cache).await;
    }

    #[tokio::test]
    async fn test_add_region_removes_overlapping_tail_region() {
        let retry_client = Arc::new(MockRetryClient::default());
        let cache = RegionCache::new_with_ttl(
            retry_client.clone(),
            Duration::from_secs(600),
            Duration::from_secs(60),
        );

        let head = region(1, vec![], vec![30]);
        let tail = region(2, vec![30], vec![]);
        cache.add_region(head.clone()).await;
        cache.add_region(tail).await;

        // Simulate a split in the old tail region: the old `[30, +inf)` must be removed.
        let mid = region(3, vec![50], vec![60]);
        cache.add_region(mid.clone()).await;

        let mut expected_cache: BTreeMap<Key, _> = BTreeMap::new();
        expected_cache.insert(vec![].into(), head);
        expected_cache.insert(vec![50].into(), mid);
        assert(&cache, &expected_cache).await;
    }

    #[tokio::test]
    async fn test_get_region_by_key() -> Result<()> {
        let retry_client = Arc::new(MockRetryClient::default());
        let cache = RegionCache::new_with_ttl(
            retry_client.clone(),
            Duration::from_secs(600),
            Duration::from_secs(60),
        );

        let region1 = region(1, vec![], vec![10]);
        let region2 = region(2, vec![10], vec![20]);
        let region3 = region(3, vec![30], vec![40]);
        let region4 = region(4, vec![50], vec![]);
        cache.add_region(region1.clone()).await;
        cache.add_region(region2.clone()).await;
        cache.add_region(region3.clone()).await;
        cache.add_region(region4.clone()).await;

        assert_eq!(
            cache.get_region_by_key(&vec![].into()).await?,
            region1.clone()
        );
        assert_eq!(
            cache.get_region_by_key(&vec![5].into()).await?,
            region1.clone()
        );
        assert_eq!(
            cache.get_region_by_key(&vec![10].into()).await?,
            region2.clone()
        );
        assert!(cache.get_region_by_key(&vec![20].into()).await.is_err());
        assert!(cache.get_region_by_key(&vec![25].into()).await.is_err());
        assert_eq!(cache.get_region_by_key(&vec![60].into()).await?, region4);
        Ok(())
    }

    #[tokio::test]
    async fn test_get_region_with_bucket_version_by_key_uses_cache_and_reloads_after_invalidate(
    ) -> Result<()> {
        let retry_client = Arc::new(MockRetryClient::default());
        let cache = RegionCache::new_with_ttl(
            retry_client.clone(),
            Duration::from_secs(600),
            Duration::from_secs(60),
        );

        let region = region(1, vec![], vec![10]);
        let buckets = region_buckets(&region);
        let key: Key = vec![5].into();
        retry_client
            .regions
            .lock()
            .await
            .insert(region.id(), region.clone());

        let (resolved, bucket_version) = cache.get_region_with_bucket_version_by_key(&key).await?;
        assert_eq!(resolved, region);
        assert_eq!(bucket_version, buckets.version);
        assert_eq!(retry_client.get_region_with_buckets_count.load(SeqCst), 1);

        let (_, cached_bucket_version) = cache.get_region_with_bucket_version_by_key(&key).await?;
        assert_eq!(cached_bucket_version, buckets.version);
        assert_eq!(retry_client.get_region_with_buckets_count.load(SeqCst), 1);
        assert_eq!(
            cache.get_cached_region_buckets_by_id(1).await,
            Some(buckets.clone())
        );

        cache.invalidate_region_cache(resolved.ver_id()).await;
        assert_eq!(cache.get_cached_region_buckets_by_id(1).await, None);

        let (_, reloaded_bucket_version) =
            cache.get_region_with_bucket_version_by_key(&key).await?;
        assert_eq!(reloaded_bucket_version, buckets.version);
        assert_eq!(retry_client.get_region_with_buckets_count.load(SeqCst), 2);
        Ok(())
    }

    #[tokio::test]
    async fn test_read_through_region_by_id_with_buckets_populates_bucket_cache() -> Result<()> {
        let retry_client = Arc::new(MockRetryClient::default());
        let cache = RegionCache::new_with_ttl(
            retry_client.clone(),
            Duration::from_secs(600),
            Duration::from_secs(60),
        );

        let region = region(1, vec![], vec![10]);
        let buckets = region_buckets(&region);
        retry_client
            .regions
            .lock()
            .await
            .insert(region.id(), region.clone());

        let loaded = cache
            .read_through_region_by_id_with_buckets(region.id())
            .await?;
        assert_eq!(loaded.region, region);
        assert_eq!(loaded.buckets, Some(buckets.clone()));
        assert_eq!(
            cache.get_cached_region_buckets_by_id(region.id()).await,
            Some(buckets.clone())
        );
        assert_eq!(
            cache
                .get_cached_region_buckets_by_ver_id(&region.ver_id())
                .await,
            Some(buckets)
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_add_region_preserves_cached_buckets_for_same_ver_id() -> Result<()> {
        let retry_client = Arc::new(MockRetryClient::default());
        let cache = RegionCache::new_with_ttl(
            retry_client.clone(),
            Duration::from_secs(600),
            Duration::from_secs(60),
        );

        let region = region(1, vec![], vec![10]);
        let buckets = region_buckets(&region);
        retry_client
            .regions
            .lock()
            .await
            .insert(region.id(), region.clone());

        cache
            .read_through_region_by_key_with_buckets(vec![5].into())
            .await?;
        cache.add_region(region.clone()).await;

        assert_eq!(
            cache
                .get_cached_region_buckets_by_ver_id(&region.ver_id())
                .await,
            Some(buckets)
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_update_region_buckets_from_mismatch_updates_cache_without_pd_reload() -> Result<()>
    {
        let retry_client = Arc::new(MockRetryClient::default());
        let cache = RegionCache::new_with_ttl(
            retry_client.clone(),
            Duration::from_secs(600),
            Duration::from_secs(60),
        );

        let region = region(1, vec![], vec![10]);
        retry_client
            .regions
            .lock()
            .await
            .insert(region.id(), region.clone());

        cache
            .read_through_region_by_key_with_buckets(vec![5].into())
            .await?;
        assert_eq!(retry_client.get_region_with_buckets_count.load(SeqCst), 1);

        let updated_keys = vec![vec![], vec![3], vec![10]];
        assert!(
            cache
                .update_region_buckets_from_mismatch(region.ver_id(), 999, updated_keys.clone(),)
                .await
        );
        assert_eq!(retry_client.get_region_with_buckets_count.load(SeqCst), 1);
        assert_eq!(
            cache
                .get_cached_region_buckets_by_ver_id(&region.ver_id())
                .await,
            Some(metapb::Buckets {
                region_id: region.id(),
                version: 999,
                keys: updated_keys,
                ..Default::default()
            })
        );
        assert!(
            !cache
                .update_region_buckets_from_mismatch(
                    region.ver_id(),
                    11,
                    vec![vec![], vec![4], vec![10]],
                )
                .await
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_refresh_region_buckets_if_stale_reloads_by_region_id() -> Result<()> {
        let retry_client = Arc::new(MockRetryClient::default());
        let cache = RegionCache::new_with_ttl(
            retry_client.clone(),
            Duration::from_secs(600),
            Duration::from_secs(60),
        );

        let region = region(1, vec![], vec![10]);
        let stale_buckets = metapb::Buckets {
            region_id: region.id(),
            version: 1,
            keys: vec![vec![], vec![10]],
            ..Default::default()
        };
        let fresh_buckets = region_buckets(&region);
        retry_client
            .regions
            .lock()
            .await
            .insert(region.id(), region.clone());
        cache
            .add_region_load(RegionLoadResult::new(region.clone(), Some(stale_buckets)))
            .await;

        assert!(
            cache
                .refresh_region_buckets_if_stale(region.ver_id(), fresh_buckets.version)
                .await?
        );
        assert_eq!(retry_client.get_region_with_buckets_count.load(SeqCst), 1);
        assert_eq!(
            cache
                .get_cached_region_buckets_by_ver_id(&region.ver_id())
                .await,
            Some(fresh_buckets.clone())
        );
        assert!(
            !cache
                .refresh_region_buckets_if_stale(region.ver_id(), fresh_buckets.version)
                .await?
        );
        assert_eq!(retry_client.get_region_with_buckets_count.load(SeqCst), 1);
        Ok(())
    }

    // a helper function to assert the cache is in expected state
    async fn assert(
        cache: &RegionCache<MockRetryClient>,
        expected_cache: &BTreeMap<Key, RegionWithLeader>,
    ) {
        let guard = cache.region_cache.read().await;
        let mut actual_keys = guard
            .ver_id_to_region
            .values()
            .map(|r| &r.region)
            .collect::<Vec<_>>();
        let mut expected_keys = expected_cache.values().collect::<Vec<_>>();
        actual_keys.sort_by_cached_key(|r| r.id());
        expected_keys.sort_by_cached_key(|r| r.id());

        assert_eq!(actual_keys, expected_keys);
        assert_eq!(
            guard.key_to_ver_id.keys().collect::<HashSet<_>>(),
            expected_cache.keys().collect::<HashSet<_>>()
        )
    }

    fn region(id: RegionId, start_key: Vec<u8>, end_key: Vec<u8>) -> RegionWithLeader {
        let mut region = RegionWithLeader::default();
        region.region.id = id;
        region.region.start_key = start_key;
        region.region.end_key = end_key;
        region.region.region_epoch = Some(RegionEpoch {
            conf_ver: 0,
            version: 0,
        });
        // We don't care about other fields here

        region
    }

    #[test]
    fn test_region_cache_ttl_check_and_refresh() {
        use std::sync::atomic::AtomicI64;
        use std::sync::atomic::Ordering;

        let ttl = super::RegionCacheTtl::new(Duration::from_secs(10), Duration::from_secs(0));
        let now = 100;

        // Far in the future: keep TTL, do not refresh.
        let value = AtomicI64::new(now + 100);
        assert!(ttl.check_and_refresh(&value, now));
        assert_eq!(value.load(Ordering::Relaxed), now + 100);

        // Close enough to expire: refresh to now + base.
        let value = AtomicI64::new(now + 5);
        assert!(ttl.check_and_refresh(&value, now));
        assert_eq!(value.load(Ordering::Relaxed), now + 10);

        // Already expired.
        let value = AtomicI64::new(now - 1);
        assert!(!ttl.check_and_refresh(&value, now));
        assert_eq!(value.load(Ordering::Relaxed), now - 1);
    }

    #[tokio::test]
    async fn test_get_region_by_id_expired_ttl_refetches() -> Result<()> {
        use std::sync::atomic::Ordering;

        let retry_client = Arc::new(MockRetryClient::default());
        let cache = RegionCache::new_with_ttl(
            retry_client.clone(),
            Duration::from_secs(10),
            Duration::ZERO,
        );

        retry_client.regions.lock().await.insert(
            1,
            RegionWithLeader {
                region: metapb::Region {
                    id: 1,
                    start_key: vec![],
                    end_key: vec![100],
                    region_epoch: Some(RegionEpoch {
                        conf_ver: 0,
                        version: 0,
                    }),
                    ..Default::default()
                },
                leader: Some(metapb::Peer {
                    store_id: 1,
                    ..Default::default()
                }),
            },
        );

        assert_eq!(retry_client.get_region_count.load(SeqCst), 0);
        cache.get_region_by_id(1).await?;
        assert_eq!(retry_client.get_region_count.load(SeqCst), 1);

        // Force-expire the cached TTL.
        let now = super::now_epoch_sec();
        {
            let guard = cache.region_cache.read().await;
            let ver_id = guard
                .id_to_ver_id
                .get(&1)
                .expect("region should be cached")
                .clone();
            let cached = guard
                .ver_id_to_region
                .get(&ver_id)
                .expect("region cache entry should exist");
            cached
                .ttl_epoch_sec
                .store(now.saturating_sub(1), Ordering::Relaxed);
        }

        cache.get_region_by_id(1).await?;
        assert_eq!(retry_client.get_region_count.load(SeqCst), 2);
        Ok(())
    }

    #[test]
    fn test_is_valid_tikv_store() {
        let mut store = metapb::Store::default();
        assert!(is_valid_tikv_store(&store));

        store.state = metapb::StoreState::Tombstone.into();
        assert!(!is_valid_tikv_store(&store));

        store.state = metapb::StoreState::Up.into();
        assert!(is_valid_tikv_store(&store));

        store.labels.push(metapb::StoreLabel {
            key: "some_key".to_owned(),
            value: "some_value".to_owned(),
        });
        assert!(is_valid_tikv_store(&store));

        store.labels.push(metapb::StoreLabel {
            key: "engine".to_owned(),
            value: "tiflash".to_owned(),
        });
        assert!(!is_valid_tikv_store(&store));

        store.labels[1].value = "tiflash_compute".to_string();
        assert!(!is_valid_tikv_store(&store));

        store.labels[1].value = "other".to_string();
        assert!(is_valid_tikv_store(&store));
    }
}
