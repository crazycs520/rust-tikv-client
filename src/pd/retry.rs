// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

//! A utility module for managing and retrying PD requests.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use async_trait::async_trait;
use tokio::sync::RwLock;
use tokio::time::sleep;

use crate::pd::Cluster;
use crate::pd::Connection;
use crate::proto::keyspacepb;
use crate::proto::metapb;
use crate::proto::pdpb::Timestamp;
use crate::proto::pdpb::{self};
use crate::region::RegionId;
use crate::region::RegionWithLeader;
use crate::region::StoreId;
use crate::region_cache::RegionLoadResult;
use crate::stats::pd_stats;
use crate::Error;
use crate::PdRetryConfig;
use crate::Result;
use crate::SecurityManager;

#[allow(private_interfaces)]
#[async_trait]
pub trait RetryClientTrait {
    // These get_* functions will try multiple times to make a request, reconnecting as necessary.
    // It does not know about encoding. Caller should take care of it.
    async fn get_region(self: Arc<Self>, key: Vec<u8>) -> Result<RegionWithLeader>;

    async fn get_region_with_buckets(self: Arc<Self>, key: Vec<u8>) -> Result<RegionLoadResult> {
        self.get_region(key).await.map(RegionLoadResult::from)
    }

    async fn get_region_by_id(self: Arc<Self>, region_id: RegionId) -> Result<RegionWithLeader>;

    async fn get_region_by_id_with_buckets(
        self: Arc<Self>,
        region_id: RegionId,
    ) -> Result<RegionLoadResult> {
        self.get_region_by_id(region_id)
            .await
            .map(RegionLoadResult::from)
    }

    async fn get_store(self: Arc<Self>, id: StoreId) -> Result<metapb::Store>;

    async fn get_all_stores(self: Arc<Self>) -> Result<Vec<metapb::Store>>;

    async fn get_timestamp(self: Arc<Self>) -> Result<Timestamp>;

    async fn get_min_ts(self: Arc<Self>) -> Result<Timestamp>;

    async fn update_safepoint(self: Arc<Self>, safepoint: u64) -> Result<bool>;

    async fn load_keyspace(&self, keyspace: &str) -> Result<keyspacepb::KeyspaceMeta>;
}
/// Client for communication with a PD cluster. Has the facility to reconnect to the cluster.
pub struct RetryClient<Cl = Cluster> {
    // Tuple is the cluster and the time of the cluster's last reconnect.
    cluster: RwLock<(Cl, Instant)>,
    connection: Connection,
    timeout: Duration,
    retry_config: PdRetryConfig,
}

#[cfg(test)]
impl<Cl> RetryClient<Cl> {
    pub fn new_with_cluster(
        security_mgr: Arc<SecurityManager>,
        timeout: Duration,
        retry_config: PdRetryConfig,
        cluster: Cl,
    ) -> RetryClient<Cl> {
        let connection = Connection::new(security_mgr);
        RetryClient {
            cluster: RwLock::new((cluster, Instant::now())),
            connection,
            timeout,
            retry_config,
        }
    }
}

macro_rules! retry_core {
    ($self: ident, $retry_cfg: expr, $tag: literal, $call: expr) => {{
        let retry_cfg: PdRetryConfig = $retry_cfg;
        let stats = pd_stats($tag);
        let mut last_err = Ok(());
        for _ in 0..retry_cfg.leader_change_retry {
            let res = $call;

            match stats.done(res) {
                Ok(r) => return Ok(r),
                Err(e) => last_err = Err(e),
            }

            let mut reconnect_count = retry_cfg.max_reconnect_attempts;
            while let Err(e) = $self.reconnect(retry_cfg.reconnect_interval).await {
                reconnect_count -= 1;
                if reconnect_count == 0 {
                    return Err(e);
                }
                sleep(retry_cfg.reconnect_interval).await;
            }
        }

        if retry_cfg.leader_change_retry == 0 {
            return Err(crate::internal_err!(
                "PdRetryConfig.leader_change_retry must be > 0"
            ));
        }

        last_err?;
        Err(crate::internal_err!(
            "pd retry loop exhausted without producing an error"
        ))
    }};
}

macro_rules! retry_mut {
    ($self: ident, $retry_cfg: expr, $tag: literal, |$cluster: ident| $call: expr) => {{
        retry_core!($self, $retry_cfg, $tag, {
            // use the block here to drop the guard of the lock,
            // otherwise `reconnect` will try to acquire the write lock and results in a deadlock
            let $cluster = &mut $self.cluster.write().await.0;
            $call.await
        })
    }};
}

macro_rules! retry {
    ($self: ident, $retry_cfg: expr, $tag: literal, |$cluster: ident| $call: expr) => {{
        retry_core!($self, $retry_cfg, $tag, {
            // use the block here to drop the guard of the lock,
            // otherwise `reconnect` will try to acquire the write lock and results in a deadlock
            let $cluster = &$self.cluster.read().await.0;
            $call.await
        })
    }};
}

impl RetryClient<Cluster> {
    pub async fn connect(
        endpoints: &[String],
        security_mgr: Arc<SecurityManager>,
        timeout: Duration,
        retry_config: PdRetryConfig,
    ) -> Result<RetryClient> {
        let connection = Connection::new(security_mgr);
        let cluster = RwLock::new((
            connection.connect_cluster(endpoints, timeout).await?,
            Instant::now(),
        ));
        Ok(RetryClient {
            cluster,
            connection,
            timeout,
            retry_config,
        })
    }

    pub(crate) async fn cluster_id(&self) -> u64 {
        self.cluster.read().await.0.id()
    }
}

#[allow(private_interfaces)]
#[async_trait]
impl RetryClientTrait for RetryClient<Cluster> {
    // These get_* functions will try multiple times to make a request, reconnecting as necessary.
    // It does not know about encoding. Caller should take care of it.
    async fn get_region(self: Arc<Self>, key: Vec<u8>) -> Result<RegionWithLeader> {
        retry_mut!(self, self.retry_config, "get_region", |cluster| {
            let key = key.clone();
            async {
                cluster
                    .get_region(key.clone(), self.timeout)
                    .await
                    .and_then(|resp| {
                        region_from_response(resp, || Error::RegionForKeyNotFound { key })
                    })
            }
        })
    }

    async fn get_region_with_buckets(self: Arc<Self>, key: Vec<u8>) -> Result<RegionLoadResult> {
        retry_mut!(
            self,
            self.retry_config,
            "get_region_with_buckets",
            |cluster| {
                let key = key.clone();
                async {
                    cluster
                        .get_region_with_buckets(key.clone(), self.timeout)
                        .await
                        .and_then(|resp| {
                            region_load_from_response(resp, || Error::RegionForKeyNotFound { key })
                        })
                }
            }
        )
    }

    async fn get_region_by_id(self: Arc<Self>, region_id: RegionId) -> Result<RegionWithLeader> {
        retry_mut!(
            self,
            self.retry_config,
            "get_region_by_id",
            |cluster| async {
                cluster
                    .get_region_by_id(region_id, self.timeout)
                    .await
                    .and_then(|resp| {
                        region_from_response(resp, || Error::RegionNotFoundInResponse { region_id })
                    })
            }
        )
    }

    async fn get_region_by_id_with_buckets(
        self: Arc<Self>,
        region_id: RegionId,
    ) -> Result<RegionLoadResult> {
        retry_mut!(
            self,
            self.retry_config,
            "get_region_by_id_with_buckets",
            |cluster| async {
                cluster
                    .get_region_by_id_with_buckets(region_id, self.timeout)
                    .await
                    .and_then(|resp| {
                        region_load_from_response(resp, || Error::RegionNotFoundInResponse {
                            region_id,
                        })
                    })
            }
        )
    }

    async fn get_store(self: Arc<Self>, id: StoreId) -> Result<metapb::Store> {
        retry_mut!(self, self.retry_config, "get_store", |cluster| async {
            cluster.get_store(id, self.timeout).await.and_then(|resp| {
                resp.store.ok_or_else(|| {
                    crate::internal_err!("missing store in PD GetStoreResponse (store_id={})", id)
                })
            })
        })
    }

    async fn get_all_stores(self: Arc<Self>) -> Result<Vec<metapb::Store>> {
        retry_mut!(self, self.retry_config, "get_all_stores", |cluster| async {
            cluster
                .get_all_stores(self.timeout)
                .await
                .map(|resp| resp.stores.into_iter().map(Into::into).collect())
        })
    }

    async fn get_timestamp(self: Arc<Self>) -> Result<Timestamp> {
        retry!(self, self.retry_config, "get_timestamp", |cluster| cluster
            .get_timestamp())
    }

    async fn get_min_ts(self: Arc<Self>) -> Result<Timestamp> {
        retry_mut!(self, self.retry_config, "get_min_ts", |cluster| async {
            cluster.get_min_ts(self.timeout).await.and_then(|resp| {
                resp.timestamp
                    .ok_or_else(|| crate::internal_err!("missing timestamp in PD GetMinTsResponse"))
            })
        })
    }

    async fn update_safepoint(self: Arc<Self>, safepoint: u64) -> Result<bool> {
        retry_mut!(
            self,
            self.retry_config,
            "update_gc_safepoint",
            |cluster| async {
                cluster
                    .update_safepoint(safepoint, self.timeout)
                    .await
                    .map(|resp| resp.new_safe_point == safepoint)
            }
        )
    }

    async fn load_keyspace(&self, keyspace: &str) -> Result<keyspacepb::KeyspaceMeta> {
        retry_mut!(self, self.retry_config, "load_keyspace", |cluster| async {
            cluster.load_keyspace(keyspace, self.timeout).await
        })
    }
}

impl fmt::Debug for RetryClient {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("pd::RetryClient")
            .field("timeout", &self.timeout)
            .finish()
    }
}

fn region_from_response(
    resp: pdpb::GetRegionResponse,
    err: impl FnOnce() -> Error,
) -> Result<RegionWithLeader> {
    region_load_from_response(resp, err).map(RegionLoadResult::into_region)
}

fn region_load_from_response(
    mut resp: pdpb::GetRegionResponse,
    err: impl FnOnce() -> Error,
) -> Result<RegionLoadResult> {
    let region = resp.region.take().ok_or_else(err)?;
    Ok(RegionLoadResult::new(
        RegionWithLeader::new(region, resp.leader.take()),
        resp.buckets.take(),
    ))
}

#[cfg(test)]
#[allow(private_interfaces)]
#[async_trait]
impl RetryClientTrait for RetryClient<crate::mock::MockCluster> {
    async fn get_region(self: Arc<Self>, key: Vec<u8>) -> Result<RegionWithLeader> {
        Ok(crate::mock::MockPdClient::lookup_region_by_key(&key))
    }

    async fn get_region_with_buckets(self: Arc<Self>, key: Vec<u8>) -> Result<RegionLoadResult> {
        let region = crate::mock::MockPdClient::lookup_region_by_key(&key);
        Ok(RegionLoadResult::new(
            region.clone(),
            Some(crate::mock::region_buckets(&region)),
        ))
    }

    async fn get_region_by_id(self: Arc<Self>, region_id: RegionId) -> Result<RegionWithLeader> {
        crate::mock::MockPdClient::lookup_region_by_id(region_id)
    }

    async fn get_region_by_id_with_buckets(
        self: Arc<Self>,
        region_id: RegionId,
    ) -> Result<RegionLoadResult> {
        let region = crate::mock::MockPdClient::lookup_region_by_id(region_id)?;
        Ok(RegionLoadResult::new(
            region.clone(),
            Some(crate::mock::region_buckets(&region)),
        ))
    }

    async fn get_store(self: Arc<Self>, _id: StoreId) -> Result<metapb::Store> {
        Err(Error::Unimplemented)
    }

    async fn get_all_stores(self: Arc<Self>) -> Result<Vec<metapb::Store>> {
        Err(Error::Unimplemented)
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
}

// A node-like thing that can be connected to.
#[async_trait]
trait Reconnect {
    type Cl;
    async fn reconnect(&self, interval: Duration) -> Result<()>;
}

#[async_trait]
impl Reconnect for RetryClient<Cluster> {
    type Cl = Cluster;

    async fn reconnect(&self, interval: Duration) -> Result<()> {
        let reconnect_begin = Instant::now();
        let mut lock = self.cluster.write().await;
        let (cluster, last_connected) = &mut *lock;
        // If `last_connected + interval` is larger or equal than reconnect_begin,
        // a concurrent reconnect is just succeed when this thread trying to get write lock
        let should_connect = reconnect_begin > *last_connected + interval;
        if should_connect {
            self.connection.reconnect(cluster, self.timeout).await?;
            *last_connected = Instant::now();
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::sync::Mutex;

    use futures::future::ready;

    use super::*;
    use crate::internal_err;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_reconnect() {
        struct MockClient {
            reconnect_count: AtomicUsize,
            cluster: RwLock<((), Instant)>,
        }

        #[async_trait]
        impl Reconnect for MockClient {
            type Cl = ();

            async fn reconnect(&self, _: Duration) -> Result<()> {
                self.reconnect_count
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                // Not actually unimplemented, we just don't care about the error.
                Err(Error::Unimplemented)
            }
        }

        async fn retry_err(client: Arc<MockClient>) -> Result<()> {
            let cfg = PdRetryConfig {
                reconnect_interval: Duration::from_secs(0),
                max_reconnect_attempts: 2,
                leader_change_retry: 1,
            };
            retry_mut!(client, cfg, "test", |_c| {
                ready(Err(internal_err!("whoops")))
            })
        }

        async fn retry_ok(client: Arc<MockClient>) -> Result<()> {
            let cfg = PdRetryConfig {
                reconnect_interval: Duration::from_secs(0),
                max_reconnect_attempts: 2,
                leader_change_retry: 1,
            };
            retry!(client, cfg, "test", |_c| { ready(Ok::<_, Error>(())) })
        }

        let client = Arc::new(MockClient {
            reconnect_count: AtomicUsize::new(0),
            cluster: RwLock::new(((), Instant::now())),
        });

        assert!(retry_err(client.clone()).await.is_err());
        assert_eq!(client.reconnect_count.load(Ordering::SeqCst), 2);

        client.reconnect_count.store(0, Ordering::SeqCst);
        assert!(retry_ok(client.clone()).await.is_ok());
        assert_eq!(client.reconnect_count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_retry() {
        struct MockClient {
            cluster: RwLock<(AtomicUsize, Instant)>,
        }

        #[async_trait]
        impl Reconnect for MockClient {
            type Cl = Mutex<usize>;

            async fn reconnect(&self, _: Duration) -> Result<()> {
                Ok(())
            }
        }

        async fn retry_max_err(
            client: Arc<MockClient>,
            max_retries: Arc<AtomicUsize>,
            cfg: PdRetryConfig,
        ) -> Result<()> {
            retry_mut!(client, cfg, "test", |c| {
                c.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

                let max_retries = max_retries.fetch_sub(1, Ordering::SeqCst) - 1;
                if max_retries == 0 {
                    ready(Ok(()))
                } else {
                    ready(Err(internal_err!("whoops")))
                }
            })
        }

        async fn retry_max_ok(
            client: Arc<MockClient>,
            max_retries: Arc<AtomicUsize>,
            cfg: PdRetryConfig,
        ) -> Result<()> {
            retry!(client, cfg, "test", |c| {
                c.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

                let max_retries = max_retries.fetch_sub(1, Ordering::SeqCst) - 1;
                if max_retries == 0 {
                    ready(Ok(()))
                } else {
                    ready(Err(internal_err!("whoops")))
                }
            })
        }

        let client = Arc::new(MockClient {
            cluster: RwLock::new((AtomicUsize::new(0), Instant::now())),
        });
        let max_retries = Arc::new(AtomicUsize::new(1000));

        let cfg = PdRetryConfig {
            reconnect_interval: Duration::from_secs(0),
            max_reconnect_attempts: 1,
            leader_change_retry: 3,
        };
        assert!(retry_max_err(client.clone(), max_retries, cfg)
            .await
            .is_err());
        assert_eq!(
            client.cluster.read().await.0.load(Ordering::SeqCst),
            cfg.leader_change_retry
        );

        let client = Arc::new(MockClient {
            cluster: RwLock::new((AtomicUsize::new(0), Instant::now())),
        });
        let max_retries = Arc::new(AtomicUsize::new(2));

        assert!(retry_max_ok(client.clone(), max_retries, cfg).await.is_ok());
        assert_eq!(client.cluster.read().await.0.load(Ordering::SeqCst), 2);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_retry_rejects_zero_leader_change_retry() {
        struct MockClient {
            cluster: RwLock<((), Instant)>,
        }

        #[async_trait]
        impl Reconnect for MockClient {
            type Cl = ();

            async fn reconnect(&self, _: Duration) -> Result<()> {
                Ok(())
            }
        }

        let client = Arc::new(MockClient {
            cluster: RwLock::new(((), Instant::now())),
        });
        let cfg = PdRetryConfig {
            reconnect_interval: Duration::from_secs(0),
            max_reconnect_attempts: 1,
            leader_change_retry: 0,
        };

        async fn retry_ok(client: Arc<MockClient>, cfg: PdRetryConfig) -> Result<()> {
            retry!(client, cfg, "test", |_c| { ready(Ok::<_, Error>(())) })
        }

        let res = retry_ok(client, cfg).await;
        assert!(matches!(res, Err(Error::InternalError { .. })));
    }
}
