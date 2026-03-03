// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

//! This crate provides an easy-to-use client for [TiKV](https://github.com/tikv/tikv), a
//! distributed, transactional key-value database written in Rust.
//!
//! This crate lets you connect to a TiKV cluster and use either a transactional or raw (simple
//! get/put style without transactional consistency guarantees) API to access and update your data.
//!
//! ## Project status (this repo)
//!
//! This crate is developed in this repo (crate at the repo root) and targets parity with
//! `client-go` v2.
//! See `doc/client-go-v2-parity-roadmap.md` for the staged plan.
//!
//! The TiKV Rust client supports several levels of abstraction. The most convenient way to use the
//! client is via [`RawClient`] and [`TransactionClient`]. This gives a very high-level API which
//! mostly abstracts over the distributed nature of the store and has sensible defaults for all
//! protocols. This interface can be configured, primarily when creating the client or transaction
//! objects via the [`Config`] and [`TransactionOptions`] structs. Using some options, you can take
//! over parts of the protocols (such as retrying failed messages) yourself.
//!
//! The lowest public abstraction is to create and execute a [`request::PlanBuilder`] over typed
//! kvproto request/response types (see [`kvrpcpb`]). Plans reuse the same sharding/retry/merge
//! machinery used by the high-level clients, but keep request/response types explicit.
//!
//! Convenience helpers for constructing kvproto requests live in [`raw_lowering`] and
//! [`transaction_lowering`].
//!
//! For an implementation-oriented overview (module boundaries and request flow), see
//! `doc/architecture.md` in this repo.
//!
//! ## Choosing an API
//!
//! This crate offers both [raw](RawClient) and
//! [transactional](Transaction) APIs. You should choose just one for your system.
//!
//! The consequence of supporting transactions is increased overhead of coordination with the
//! placement driver and TiKV, and additional code complexity.
//!
//! *While it is possible to use both APIs at the same time, doing so is unsafe and unsupported.*
//!
//! ### Transactional
//!
//! The [transactional](Transaction) API supports **transactions** via multi-version
//! concurrency control (MVCC).
//!
//! Best when you mostly do complex sets of actions, actions which may require a rollback,
//! operations affecting multiple keys or values, or operations that depend on strong consistency.
//!
//!
//! ### Raw
//!
//! The [raw](RawClient) API has reduced coordination overhead, but lacks any
//! transactional abilities.
//!
//! Best when you mostly do single value changes, and have very limited cross-value
//! requirements. You will not be able to use transactions with this API.
//!
//! ## Usage
//!
//! The general flow of using the client crate is to create either a raw or transaction client
//! object (which can be configured) then send commands using the client object, or use it to create
//! transactions objects. In the latter case, the transaction is built up using various commands and
//! then committed (or rolled back).
//!
//! ### Examples
//!
//! Raw mode:
//!
//! ```rust,no_run
//! # use tikv_client::{RawClient, Result};
//! # async fn example() -> Result<()> {
//! let client = RawClient::new(vec!["127.0.0.1:2379"]).await?;
//! client.put("key".to_owned(), "value".to_owned()).await?;
//! let _value = client.get("key".to_owned()).await?;
//! # Ok(())
//! # }
//! ```
//!
//! Transactional mode:
//!
//! ```rust,no_run
//! # use tikv_client::{TransactionClient, Result};
//! # async fn example() -> Result<()> {
//! let txn_client = TransactionClient::new(vec!["127.0.0.1:2379"]).await?;
//! let mut txn = txn_client.begin_optimistic().await?;
//! txn.put("key".to_owned(), "value".to_owned()).await?;
//! let _value = txn.get("key".to_owned()).await?;
//! txn.commit().await?;
//! # Ok(())
//! # }
//! ```
//!
//! Since this crate provides an async API, you need an async runtime (Tokio-only).

#![allow(clippy::field_reassign_with_default)]

pub mod backoff;
#[cfg(any(test, feature = "test-util"))]
#[cfg_attr(feature = "test-util", allow(dead_code))]
mod backoffer;
pub mod interceptor;
pub mod metrics;
#[doc(hidden)]
pub mod raw;
pub mod request;
pub mod trace;
#[doc(hidden)]
pub mod transaction;

mod common;
mod compat;
mod config;
mod disk_full_opt;
#[doc(hidden)]
pub mod coprocessor {
    pub use crate::proto::coprocessor::*;
}
#[doc(hidden)]
pub mod kvrpcpb {
    pub use crate::proto::kvrpcpb::*;
}
mod kv;
mod pd;
mod priority;
mod proto;
mod region;
mod region_cache;
mod replica_read;
mod request_context;
#[cfg(test)]
mod resource_control;
mod stats;
mod store;
mod timestamp;
mod util;
#[doc(hidden)]
pub mod resource_manager {
    pub use crate::proto::resource_manager::*;
}

#[cfg(any(test, feature = "test-util"))]
#[cfg_attr(feature = "test-util", allow(dead_code))]
mod mock;

/// Test and benchmark utilities (feature-gated).
#[cfg(feature = "test-util")]
pub mod test_util {
    pub use super::mock::MockKvClient;
    pub use super::mock::MockPdClient;
}

#[doc(inline)]
pub use common::security::SecurityManager;
#[doc(inline)]
pub use common::Error;
#[doc(inline)]
pub use common::Result;
#[doc(inline)]
pub use config::parse_path;
#[doc(inline)]
pub use config::txn_scope_from_config;
#[doc(inline)]
pub use config::Config;
#[doc(inline)]
pub use config::ParsedPath;
#[doc(inline)]
pub use config::PdRetryConfig;

#[doc(inline)]
pub use crate::backoff::Backoff;
#[doc(inline)]
pub use crate::kv::batch_get_to_get_options;
#[doc(inline)]
pub use crate::kv::with_return_commit_ts;
#[doc(inline)]
pub use crate::kv::BatchGetOption;
#[doc(inline)]
pub use crate::kv::BatchGetOptions;
#[doc(inline)]
pub use crate::kv::BoundRange;
#[doc(inline)]
pub use crate::kv::GetOption;
#[doc(inline)]
pub use crate::kv::GetOptions;
#[doc(inline)]
pub use crate::kv::GetOrBatchGetOption;
#[doc(inline)]
pub use crate::kv::IntoOwnedRange;
#[doc(inline)]
pub use crate::kv::Key;
#[doc(inline)]
pub use crate::kv::KvPair;
#[doc(inline)]
pub use crate::kv::Value;
#[doc(inline)]
pub use crate::kv::ValueEntry;
#[doc(hidden)]
pub use crate::pd::{PdClient, PdRpcClient, RetryClientTrait};
#[doc(hidden)]
pub use crate::proto::metapb;
#[doc(hidden)]
pub use crate::region_cache::RegionCache;
#[doc(inline)]
pub use crate::raw::lowering as raw_lowering;
#[doc(inline)]
pub use crate::raw::Client as RawClient;
#[doc(inline)]
pub use crate::raw::ColumnFamily;
#[doc(inline)]
pub use crate::raw::RawChecksum;
#[doc(hidden)]
pub use crate::region::{RegionId, RegionVerId, RegionWithLeader, StoreId};
#[doc(inline)]
pub use crate::request::RetryOptions;
#[doc(inline)]
pub use crate::timestamp::compose_ts;
#[doc(inline)]
pub use crate::timestamp::extract_logical;
#[doc(inline)]
pub use crate::timestamp::extract_physical;
#[doc(inline)]
pub use crate::timestamp::get_physical;
#[doc(inline)]
pub use crate::timestamp::get_time_from_ts;
#[doc(inline)]
pub use crate::timestamp::lower_limit_start_ts;
#[doc(inline)]
pub use crate::timestamp::time_to_ts;
#[doc(inline)]
pub use crate::timestamp::Timestamp;
#[doc(inline)]
pub use crate::timestamp::TimestampExt;
#[doc(inline)]
pub use crate::timestamp::GLOBAL_TXN_SCOPE;
#[doc(inline)]
pub use crate::transaction::lowering as transaction_lowering;
#[doc(inline)]
pub use crate::transaction::CheckLevel;
#[doc(inline)]
pub use crate::transaction::Client as TransactionClient;
#[doc(inline)]
pub use crate::transaction::Snapshot;
#[doc(inline)]
pub use crate::transaction::Transaction;
#[doc(inline)]
pub use crate::transaction::TransactionOptions;
#[doc(inline)]
pub use disk_full_opt::DiskFullOpt;
#[doc(inline)]
pub use priority::CommandPriority;
#[doc(inline)]
pub use replica_read::ReplicaReadType;

pub(crate) use request_context::RequestContext;
