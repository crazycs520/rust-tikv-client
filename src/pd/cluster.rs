// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use async_trait::async_trait;
use log::error;
use log::info;
use log::warn;
use tonic::transport::Channel;
use tonic::IntoRequest;
use tonic::Request;

use super::timestamp::TimestampOracle;
use crate::internal_err;
use crate::proto::keyspacepb;
use crate::proto::pdpb;
use crate::Error;
use crate::Result;
use crate::SecurityManager;
use crate::Timestamp;

/// A PD cluster.
pub struct Cluster {
    id: u64,
    client: pdpb::pd_client::PdClient<Channel>,
    keyspace_client: keyspacepb::keyspace_client::KeyspaceClient<Channel>,
    members: pdpb::GetMembersResponse,
    tso: TimestampOracle,
}

macro_rules! pd_request {
    ($cluster_id:expr, $type:ty) => {{
        let mut request = <$type>::default();
        let mut header = pdpb::RequestHeader::default();
        header.cluster_id = $cluster_id;
        request.header = Some(header);
        request
    }};
}

// These methods make a single attempt to make a request.
impl Cluster {
    pub(crate) fn id(&self) -> u64 {
        self.id
    }

    pub async fn get_region(
        &mut self,
        key: Vec<u8>,
        timeout: Duration,
    ) -> Result<pdpb::GetRegionResponse> {
        let mut req = pd_request!(self.id, pdpb::GetRegionRequest);
        req.region_key = key;
        req.send(&mut self.client, timeout).await
    }

    pub async fn get_region_with_buckets(
        &mut self,
        key: Vec<u8>,
        timeout: Duration,
    ) -> Result<pdpb::GetRegionResponse> {
        let mut req = pd_request!(self.id, pdpb::GetRegionRequest);
        req.region_key = key;
        req.need_buckets = true;
        req.send(&mut self.client, timeout).await
    }

    pub async fn get_region_by_id(
        &mut self,
        id: u64,
        timeout: Duration,
    ) -> Result<pdpb::GetRegionResponse> {
        let mut req = pd_request!(self.id, pdpb::GetRegionByIdRequest);
        req.region_id = id;
        req.send(&mut self.client, timeout).await
    }

    pub async fn get_region_by_id_with_buckets(
        &mut self,
        id: u64,
        timeout: Duration,
    ) -> Result<pdpb::GetRegionResponse> {
        let mut req = pd_request!(self.id, pdpb::GetRegionByIdRequest);
        req.region_id = id;
        req.need_buckets = true;
        req.send(&mut self.client, timeout).await
    }

    pub async fn get_store(
        &mut self,
        id: u64,
        timeout: Duration,
    ) -> Result<pdpb::GetStoreResponse> {
        let mut req = pd_request!(self.id, pdpb::GetStoreRequest);
        req.store_id = id;
        req.send(&mut self.client, timeout).await
    }

    pub async fn get_all_stores(
        &mut self,
        timeout: Duration,
    ) -> Result<pdpb::GetAllStoresResponse> {
        let req = pd_request!(self.id, pdpb::GetAllStoresRequest);
        req.send(&mut self.client, timeout).await
    }

    pub async fn get_timestamp(&self) -> Result<Timestamp> {
        self.tso.clone().get_timestamp().await
    }

    pub async fn get_min_ts(&mut self, timeout: Duration) -> Result<pdpb::GetMinTsResponse> {
        let req = pd_request!(self.id, pdpb::GetMinTsRequest);
        req.send(&mut self.client, timeout).await
    }

    pub async fn update_safepoint(
        &mut self,
        safepoint: u64,
        timeout: Duration,
    ) -> Result<pdpb::UpdateGcSafePointResponse> {
        let mut req = pd_request!(self.id, pdpb::UpdateGcSafePointRequest);
        req.safe_point = safepoint;
        req.send(&mut self.client, timeout).await
    }

    pub async fn load_keyspace(
        &mut self,
        keyspace: &str,
        timeout: Duration,
    ) -> Result<keyspacepb::KeyspaceMeta> {
        let mut req = pd_request!(self.id, keyspacepb::LoadKeyspaceRequest);
        req.name = keyspace.to_string();
        let resp = req.send(&mut self.keyspace_client, timeout).await?;
        let keyspace = resp
            .keyspace
            .ok_or_else(|| Error::KeyspaceNotFound(keyspace.to_owned()))?;
        Ok(keyspace)
    }
}

/// An object for connecting and reconnecting to a PD cluster.
pub struct Connection {
    security_mgr: Arc<SecurityManager>,
}

impl Connection {
    pub fn new(security_mgr: Arc<SecurityManager>) -> Connection {
        Connection { security_mgr }
    }

    pub async fn connect_cluster(
        &self,
        endpoints: &[String],
        timeout: Duration,
    ) -> Result<Cluster> {
        let members = self.validate_endpoints(endpoints, timeout).await?;
        let (client, keyspace_client, members) = self.try_connect_leader(&members, timeout).await?;
        let id = Self::validate_members_response("pd", &members)?;
        let tso = TimestampOracle::new(id, &client)?;
        let cluster = Cluster {
            id,
            client,
            keyspace_client,
            members,
            tso,
        };
        Ok(cluster)
    }

    // Re-establish connection with PD leader in asynchronous fashion.
    pub async fn reconnect(&self, cluster: &mut Cluster, timeout: Duration) -> Result<()> {
        warn!("updating pd client");
        let start = Instant::now();
        let (client, keyspace_client, members) =
            self.try_connect_leader(&cluster.members, timeout).await?;
        let tso = TimestampOracle::new(cluster.id, &client)?;
        *cluster = Cluster {
            id: cluster.id,
            client,
            keyspace_client,
            members,
            tso,
        };

        info!("updating PD client done, spent {:?}", start.elapsed());
        Ok(())
    }

    async fn validate_endpoints(
        &self,
        endpoints: &[String],
        timeout: Duration,
    ) -> Result<pdpb::GetMembersResponse> {
        let mut endpoints_set = HashSet::with_capacity(endpoints.len());

        let mut members = None;
        let mut cluster_id = None;
        for ep in endpoints {
            if !endpoints_set.insert(ep) {
                return Err(internal_err!("duplicated PD endpoint {}", ep));
            }

            let (_, _, resp) = match self.connect(ep, timeout).await {
                Ok(resp) => resp,
                // Ignore failed PD node.
                Err(e) => {
                    warn!("PD endpoint {} failed to respond: {:?}", ep, e);
                    continue;
                }
            };

            // Check cluster ID.
            let cid = resp
                .header
                .as_ref()
                .map(|header| header.cluster_id)
                .ok_or_else(|| internal_err!("missing PD response header from {}", ep))?;
            if let Some(sample) = cluster_id {
                if sample != cid {
                    return Err(internal_err!(
                        "PD response cluster_id mismatch, want {}, got {}",
                        sample,
                        cid
                    ));
                }
            } else {
                cluster_id = Some(cid);
            }

            if members.is_none() {
                members = Some(resp);
            }
        }

        match members {
            Some(members) => {
                info!("All PD endpoints are consistent: {:?}", endpoints);
                Ok(members)
            }
            _ => Err(internal_err!("PD cluster failed to respond")),
        }
    }

    async fn connect(
        &self,
        addr: &str,
        _timeout: Duration,
    ) -> Result<(
        pdpb::pd_client::PdClient<Channel>,
        keyspacepb::keyspace_client::KeyspaceClient<Channel>,
        pdpb::GetMembersResponse,
    )> {
        let mut client = self
            .security_mgr
            .connect(addr, pdpb::pd_client::PdClient::<Channel>::new)
            .await?;
        let keyspace_client = self
            .security_mgr
            .connect(
                addr,
                keyspacepb::keyspace_client::KeyspaceClient::<Channel>::new,
            )
            .await?;
        let resp: pdpb::GetMembersResponse = client
            .get_members(pdpb::GetMembersRequest::default())
            .await?
            .into_inner();
        Self::validate_members_response(addr, &resp)?;
        Ok((client, keyspace_client, resp))
    }

    fn validate_members_response(addr: &str, members: &pdpb::GetMembersResponse) -> Result<u64> {
        let header = members
            .header
            .as_ref()
            .ok_or_else(|| internal_err!("missing PD response header from {}", addr))?;
        if let Some(err) = header.error.as_ref() {
            return Err(internal_err!(
                "failed to get PD members from {}, err {:?}",
                addr,
                err
            ));
        }
        if header.cluster_id == 0 {
            return Err(internal_err!(
                "PD response cluster_id is zero from {}",
                addr
            ));
        }

        let leader = members.leader.as_ref().ok_or_else(|| {
            internal_err!(
                "unexpected no PD leader in get member resp from {}: {:?}",
                addr,
                members
            )
        })?;
        if leader.client_urls.is_empty() {
            return Err(internal_err!(
                "PD leader has empty client_urls in get member resp from {}",
                addr
            ));
        }
        if members.members.is_empty() {
            return Err(internal_err!(
                "PD members list is empty in get member resp from {}",
                addr
            ));
        }
        if !members
            .members
            .iter()
            .any(|member| member.member_id == leader.member_id)
        {
            return Err(internal_err!(
                "PD leader {} is not present in members list from {}",
                leader.member_id,
                addr
            ));
        }
        for member in &members.members {
            if member.client_urls.is_empty() {
                return Err(internal_err!(
                    "PD member {} has empty client_urls in get member resp from {}",
                    member.member_id,
                    addr
                ));
            }
        }

        Ok(header.cluster_id)
    }

    async fn try_connect(
        &self,
        addr: &str,
        cluster_id: u64,
        timeout: Duration,
    ) -> Result<(
        pdpb::pd_client::PdClient<Channel>,
        keyspacepb::keyspace_client::KeyspaceClient<Channel>,
        pdpb::GetMembersResponse,
    )> {
        let (client, keyspace_client, r) = self.connect(addr, timeout).await?;
        Connection::validate_cluster_id(addr, &r, cluster_id)?;
        Ok((client, keyspace_client, r))
    }

    fn validate_cluster_id(
        addr: &str,
        members: &pdpb::GetMembersResponse,
        cluster_id: u64,
    ) -> Result<()> {
        let new_cluster_id = members
            .header
            .as_ref()
            .map(|header| header.cluster_id)
            .ok_or_else(|| internal_err!("missing PD response header from {}", addr))?;
        if new_cluster_id != cluster_id {
            Err(internal_err!(
                "{} no longer belongs to cluster {}, it is in {}",
                addr,
                cluster_id,
                new_cluster_id
            ))
        } else {
            Ok(())
        }
    }

    async fn try_connect_leader(
        &self,
        previous: &pdpb::GetMembersResponse,
        timeout: Duration,
    ) -> Result<(
        pdpb::pd_client::PdClient<Channel>,
        keyspacepb::keyspace_client::KeyspaceClient<Channel>,
        pdpb::GetMembersResponse,
    )> {
        let previous_leader = previous
            .leader
            .as_ref()
            .ok_or_else(|| internal_err!("no leader found in GetMembersResponse"))?;
        let members = &previous.members;
        let cluster_id = previous
            .header
            .as_ref()
            .map(|header| header.cluster_id)
            .ok_or_else(|| internal_err!("missing PD response header"))?;

        let mut resp = None;
        // Try to connect to other members, then the previous leader.
        'outer: for m in members
            .iter()
            .filter(|m| *m != previous_leader)
            .chain(Some(previous_leader))
        {
            for ep in &m.client_urls {
                match self.try_connect(ep.as_str(), cluster_id, timeout).await {
                    Ok((_, _, r)) => {
                        resp = Some(r);
                        break 'outer;
                    }
                    Err(e) => {
                        error!("failed to connect to {}, {:?}", ep, e);
                        continue;
                    }
                }
            }
        }

        // Then try to connect the PD cluster leader.
        if let Some(resp) = resp {
            let leader = resp
                .leader
                .as_ref()
                .ok_or_else(|| internal_err!("no leader found in GetMembersResponse"))?;

            for ep in &leader.client_urls {
                if let Ok((client, keyspace_client, members)) =
                    self.try_connect(ep.as_str(), cluster_id, timeout).await
                {
                    return Ok((client, keyspace_client, members));
                }
            }
        }

        Err(internal_err!("failed to connect to {:?}", members))
    }
}

type GrpcResult<T> = std::result::Result<T, tonic::Status>;

#[async_trait]
trait PdMessage: Sized {
    type Client: Send;
    type Response: PdResponse;

    async fn rpc(req: Request<Self>, client: &mut Self::Client) -> GrpcResult<Self::Response>;

    async fn send(self, client: &mut Self::Client, timeout: Duration) -> Result<Self::Response> {
        let mut req = self.into_request();
        req.set_timeout(timeout);
        let response = Self::rpc(req, client).await?;

        let header = response
            .header()
            .ok_or_else(|| internal_err!("missing PD response header"))?;
        if let Some(err) = &header.error {
            Err(internal_err!(err.message))
        } else {
            Ok(response)
        }
    }
}

#[async_trait]
impl PdMessage for pdpb::GetRegionRequest {
    type Client = pdpb::pd_client::PdClient<Channel>;
    type Response = pdpb::GetRegionResponse;

    async fn rpc(req: Request<Self>, client: &mut Self::Client) -> GrpcResult<Self::Response> {
        Ok(client.get_region(req).await?.into_inner())
    }
}

#[async_trait]
impl PdMessage for pdpb::GetRegionByIdRequest {
    type Client = pdpb::pd_client::PdClient<Channel>;
    type Response = pdpb::GetRegionResponse;

    async fn rpc(req: Request<Self>, client: &mut Self::Client) -> GrpcResult<Self::Response> {
        Ok(client.get_region_by_id(req).await?.into_inner())
    }
}

#[async_trait]
impl PdMessage for pdpb::GetStoreRequest {
    type Client = pdpb::pd_client::PdClient<Channel>;
    type Response = pdpb::GetStoreResponse;

    async fn rpc(req: Request<Self>, client: &mut Self::Client) -> GrpcResult<Self::Response> {
        Ok(client.get_store(req).await?.into_inner())
    }
}

#[async_trait]
impl PdMessage for pdpb::GetAllStoresRequest {
    type Client = pdpb::pd_client::PdClient<Channel>;
    type Response = pdpb::GetAllStoresResponse;

    async fn rpc(req: Request<Self>, client: &mut Self::Client) -> GrpcResult<Self::Response> {
        Ok(client.get_all_stores(req).await?.into_inner())
    }
}

#[async_trait]
impl PdMessage for pdpb::GetMinTsRequest {
    type Client = pdpb::pd_client::PdClient<Channel>;
    type Response = pdpb::GetMinTsResponse;

    async fn rpc(req: Request<Self>, client: &mut Self::Client) -> GrpcResult<Self::Response> {
        Ok(client.get_min_ts(req).await?.into_inner())
    }
}

#[async_trait]
impl PdMessage for pdpb::UpdateGcSafePointRequest {
    type Client = pdpb::pd_client::PdClient<Channel>;
    type Response = pdpb::UpdateGcSafePointResponse;

    async fn rpc(req: Request<Self>, client: &mut Self::Client) -> GrpcResult<Self::Response> {
        Ok(client.update_gc_safe_point(req).await?.into_inner())
    }
}

#[async_trait]
impl PdMessage for keyspacepb::LoadKeyspaceRequest {
    type Client = keyspacepb::keyspace_client::KeyspaceClient<Channel>;
    type Response = keyspacepb::LoadKeyspaceResponse;

    async fn rpc(req: Request<Self>, client: &mut Self::Client) -> GrpcResult<Self::Response> {
        Ok(client.load_keyspace(req).await?.into_inner())
    }
}

trait PdResponse {
    fn header(&self) -> Option<&pdpb::ResponseHeader>;
}

impl PdResponse for pdpb::GetStoreResponse {
    fn header(&self) -> Option<&pdpb::ResponseHeader> {
        self.header.as_ref()
    }
}

impl PdResponse for pdpb::GetRegionResponse {
    fn header(&self) -> Option<&pdpb::ResponseHeader> {
        self.header.as_ref()
    }
}

impl PdResponse for pdpb::GetAllStoresResponse {
    fn header(&self) -> Option<&pdpb::ResponseHeader> {
        self.header.as_ref()
    }
}

impl PdResponse for pdpb::GetMinTsResponse {
    fn header(&self) -> Option<&pdpb::ResponseHeader> {
        self.header.as_ref()
    }
}

impl PdResponse for pdpb::UpdateGcSafePointResponse {
    fn header(&self) -> Option<&pdpb::ResponseHeader> {
        self.header.as_ref()
    }
}

impl PdResponse for keyspacepb::LoadKeyspaceResponse {
    fn header(&self) -> Option<&pdpb::ResponseHeader> {
        self.header.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_member(member_id: u64, url: &str) -> pdpb::Member {
        pdpb::Member {
            member_id,
            client_urls: vec![url.to_owned()],
            ..Default::default()
        }
    }

    #[test]
    fn test_validate_members_response_ok() {
        let leader = make_member(1, "127.0.0.1:2379");
        let mut resp = pdpb::GetMembersResponse::default();
        resp.header = Some(pdpb::ResponseHeader {
            cluster_id: 42,
            ..Default::default()
        });
        resp.leader = Some(leader.clone());
        resp.members = vec![leader];
        assert_eq!(
            Connection::validate_members_response("pd", &resp).unwrap(),
            42
        );
    }

    #[test]
    fn test_validate_members_response_missing_header() {
        let leader = make_member(1, "127.0.0.1:2379");
        let mut resp = pdpb::GetMembersResponse::default();
        resp.leader = Some(leader.clone());
        resp.members = vec![leader];
        assert!(Connection::validate_members_response("pd", &resp).is_err());
    }

    #[test]
    fn test_validate_members_response_missing_leader() {
        let mut resp = pdpb::GetMembersResponse::default();
        resp.header = Some(pdpb::ResponseHeader {
            cluster_id: 42,
            ..Default::default()
        });
        resp.members = vec![make_member(1, "127.0.0.1:2379")];
        assert!(Connection::validate_members_response("pd", &resp).is_err());
    }

    #[test]
    fn test_validate_members_response_leader_not_in_members() {
        let leader = make_member(1, "127.0.0.1:2379");
        let mut resp = pdpb::GetMembersResponse::default();
        resp.header = Some(pdpb::ResponseHeader {
            cluster_id: 42,
            ..Default::default()
        });
        resp.leader = Some(leader);
        resp.members = vec![make_member(2, "127.0.0.1:22379")];
        assert!(Connection::validate_members_response("pd", &resp).is_err());
    }

    #[test]
    fn test_validate_members_response_member_missing_urls() {
        let leader = make_member(1, "127.0.0.1:2379");
        let mut resp = pdpb::GetMembersResponse::default();
        resp.header = Some(pdpb::ResponseHeader {
            cluster_id: 42,
            ..Default::default()
        });
        resp.leader = Some(leader.clone());
        resp.members = vec![
            leader,
            pdpb::Member {
                member_id: 2,
                client_urls: Vec::new(),
                ..Default::default()
            },
        ];
        assert!(Connection::validate_members_response("pd", &resp).is_err());
    }
}
