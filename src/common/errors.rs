// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::fmt;
use std::result;

use thiserror::Error;

use crate::proto::kvrpcpb;
use crate::proto::kvrpcpb::write_conflict;
use crate::region::RegionVerId;
use crate::BoundRange;

/// Reason for a write conflict returned by TiKV.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum WriteConflictReason {
    Unknown,
    Optimistic,
    PessimisticRetry,
    SelfRolledBack,
    RcCheckTs,
    LazyUniquenessCheck,
    NotLockedKeyConflict,
}

impl From<i32> for WriteConflictReason {
    fn from(value: i32) -> Self {
        match value {
            x if x == write_conflict::Reason::Optimistic as i32 => WriteConflictReason::Optimistic,
            x if x == write_conflict::Reason::PessimisticRetry as i32 => {
                WriteConflictReason::PessimisticRetry
            }
            x if x == write_conflict::Reason::SelfRolledBack as i32 => {
                WriteConflictReason::SelfRolledBack
            }
            x if x == write_conflict::Reason::RcCheckTs as i32 => WriteConflictReason::RcCheckTs,
            x if x == write_conflict::Reason::LazyUniquenessCheck as i32 => {
                WriteConflictReason::LazyUniquenessCheck
            }
            x if x == write_conflict::Reason::NotLockedKeyConflict as i32 => {
                WriteConflictReason::NotLockedKeyConflict
            }
            _ => WriteConflictReason::Unknown,
        }
    }
}

impl fmt::Display for WriteConflictReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            WriteConflictReason::Unknown => "unknown",
            WriteConflictReason::Optimistic => "optimistic",
            WriteConflictReason::PessimisticRetry => "pessimistic_retry",
            WriteConflictReason::SelfRolledBack => "self_rolled_back",
            WriteConflictReason::RcCheckTs => "rc_check_ts",
            WriteConflictReason::LazyUniquenessCheck => "lazy_uniqueness_check",
            WriteConflictReason::NotLockedKeyConflict => "not_locked_key_conflict",
        };
        f.write_str(s)
    }
}

/// A write conflict returned by TiKV.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WriteConflictError {
    pub start_ts: u64,
    pub conflict_ts: u64,
    pub conflict_commit_ts: u64,
    pub key: Vec<u8>,
    pub primary: Vec<u8>,
    pub reason: WriteConflictReason,
}

impl From<kvrpcpb::WriteConflict> for WriteConflictError {
    fn from(conflict: kvrpcpb::WriteConflict) -> Self {
        Self {
            start_ts: conflict.start_ts,
            conflict_ts: conflict.conflict_ts,
            conflict_commit_ts: conflict.conflict_commit_ts,
            key: conflict.key,
            primary: conflict.primary,
            reason: conflict.reason.into(),
        }
    }
}

impl fmt::Display for WriteConflictError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "start_ts={}, conflict_ts={}, conflict_commit_ts={}, reason={}, key_len={}",
            self.start_ts,
            self.conflict_ts,
            self.conflict_commit_ts,
            self.reason,
            self.key.len()
        )
    }
}

/// A duplicate key write detected by TiKV (e.g. `Insert` on an existing key).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KeyExistsError {
    pub key: Vec<u8>,
}

impl From<kvrpcpb::AlreadyExist> for KeyExistsError {
    fn from(exist: kvrpcpb::AlreadyExist) -> Self {
        Self { key: exist.key }
    }
}

impl fmt::Display for KeyExistsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "key_len={}", self.key.len())
    }
}

/// Assertion kind used by TiKV's assertion mechanism.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum AssertionKind {
    None,
    Exist,
    NotExist,
}

impl From<i32> for AssertionKind {
    fn from(value: i32) -> Self {
        match value {
            x if x == kvrpcpb::Assertion::Exist as i32 => AssertionKind::Exist,
            x if x == kvrpcpb::Assertion::NotExist as i32 => AssertionKind::NotExist,
            _ => AssertionKind::None,
        }
    }
}

impl fmt::Display for AssertionKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            AssertionKind::None => "none",
            AssertionKind::Exist => "exist",
            AssertionKind::NotExist => "not_exist",
        };
        f.write_str(s)
    }
}

/// An assertion failure returned by TiKV.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AssertionFailedError {
    pub start_ts: u64,
    pub key: Vec<u8>,
    pub assertion: AssertionKind,
    pub existing_start_ts: u64,
    pub existing_commit_ts: u64,
}

impl From<kvrpcpb::AssertionFailed> for AssertionFailedError {
    fn from(failed: kvrpcpb::AssertionFailed) -> Self {
        Self {
            start_ts: failed.start_ts,
            key: failed.key,
            assertion: failed.assertion.into(),
            existing_start_ts: failed.existing_start_ts,
            existing_commit_ts: failed.existing_commit_ts,
        }
    }
}

impl fmt::Display for AssertionFailedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "start_ts={}, assertion={}, key_len={}, existing_start_ts={}, existing_commit_ts={}",
            self.start_ts,
            self.assertion,
            self.key.len(),
            self.existing_start_ts,
            self.existing_commit_ts
        )
    }
}

/// Wait-for entry of a deadlock.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeadlockWaitForEntry {
    pub txn: u64,
    pub wait_for_txn: u64,
    pub key_hash: u64,
    pub key: Vec<u8>,
    pub resource_group_tag: Vec<u8>,
    pub wait_time_ms: u64,
}

/// A deadlock error returned by TiKV.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeadlockError {
    pub lock_ts: u64,
    pub lock_key: Vec<u8>,
    pub deadlock_key_hash: u64,
    pub wait_chain: Vec<DeadlockWaitForEntry>,
    pub deadlock_key: Vec<u8>,
}

impl From<kvrpcpb::Deadlock> for DeadlockError {
    fn from(deadlock: kvrpcpb::Deadlock) -> Self {
        let wait_chain = deadlock
            .wait_chain
            .into_iter()
            .map(|entry| DeadlockWaitForEntry {
                txn: entry.txn,
                wait_for_txn: entry.wait_for_txn,
                key_hash: entry.key_hash,
                key: entry.key,
                resource_group_tag: entry.resource_group_tag,
                wait_time_ms: entry.wait_time,
            })
            .collect();

        Self {
            lock_ts: deadlock.lock_ts,
            lock_key: deadlock.lock_key,
            deadlock_key_hash: deadlock.deadlock_key_hash,
            wait_chain,
            deadlock_key: deadlock.deadlock_key,
        }
    }
}

impl fmt::Display for DeadlockError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "lock_ts={}, lock_key_len={}, deadlock_key_len={}, wait_chain_len={}",
            self.lock_ts,
            self.lock_key.len(),
            self.deadlock_key.len(),
            self.wait_chain.len()
        )
    }
}

/// An error originating from the TiKV client or dependencies.
#[derive(Debug, Error)]
#[allow(clippy::large_enum_variant)]
pub enum Error {
    /// Feature is not implemented.
    #[error("Unimplemented feature")]
    Unimplemented,
    /// Duplicate key insertion happens.
    #[error("Duplicate key insertion")]
    DuplicateKeyInsertion,
    /// Write conflict returned by TiKV.
    #[error("Write conflict: {0}")]
    WriteConflict(WriteConflictError),
    /// Deadlock returned by TiKV.
    #[error("Deadlock: {0}")]
    Deadlock(DeadlockError),
    /// Key already exists (e.g. Insert on an existing key).
    #[error("Key already exists: {0}")]
    KeyExists(KeyExistsError),
    /// Assertion failed on TiKV side.
    #[error("Assertion failed: {0}")]
    AssertionFailed(AssertionFailedError),
    /// Retryable error returned by TiKV.
    #[error("Retryable error: {message}")]
    Retryable { message: String },
    /// Client-side resource control throttled the request (e.g. resource group token exhausted).
    #[error("resource group throttled")]
    ResourceGroupThrottled,
    /// TiKV aborts the transaction with a reason.
    #[error("TiKV aborts txn: {message}")]
    TxnAborted { message: String },
    /// Commit TS is too large for TiKV.
    #[error("Commit TS {commit_ts} is too large")]
    CommitTsTooLarge { commit_ts: u64 },
    /// Commit TS is behind a user-specified commit-wait TSO.
    #[error("Commit TS {commit_ts} is behind commit-wait TSO {wait_until_tso}")]
    CommitTsLag { commit_ts: u64, wait_until_tso: u64 },
    /// The transaction is not found on TiKV.
    #[error("Txn {start_ts} not found")]
    TxnNotFound { start_ts: u64 },
    /// Failed to resolve a lock
    #[error("Failed to resolve lock")]
    ResolveLockError(Vec<kvrpcpb::LockInfo>),
    /// Lock wait timeout exceeded.
    #[error("Lock wait timeout exceeded; try restarting transaction")]
    LockWaitTimeout(Vec<kvrpcpb::LockInfo>),
    /// Will raise this error when using a pessimistic txn only operation on an optimistic txn
    #[error("Invalid operation for this type of transaction")]
    InvalidTransactionType,
    /// It's not allowed to perform operations in a transaction after it has been committed or rolled back.
    #[error("Cannot read or write data after any attempt to commit or roll back the transaction")]
    OperationAfterCommitError,
    /// We tried to use 1pc for a transaction, but it didn't work. Probably should have used 2pc.
    #[error("1PC transaction could not be committed.")]
    OnePcFailure,
    /// An operation requires a primary key, but the transaction was empty.
    #[error("transaction has no primary key")]
    NoPrimaryKey,
    /// Transaction became stale while waiting for local latches.
    #[error("Transaction is stale while waiting for local latches (start_ts={start_ts})")]
    WriteConflictInLatch { start_ts: u64 },
    /// Pipelined transaction options are invalid or unsupported.
    #[error("Invalid pipelined transaction: {message}")]
    InvalidPipelinedTransaction { message: String },
    /// Invalid pessimistic lock options (client-side validation failure).
    #[error("Invalid pessimistic lock options: {message}")]
    InvalidPessimisticLockOptions { message: String },
    /// For raw client, operation is not supported in atomic/non-atomic mode.
    #[error(
        "The operation is not supported in current mode, please consider using RawClient with or without atomic mode"
    )]
    UnsupportedMode,
    #[error("There is no current_regions in the EpochNotMatch error")]
    NoCurrentRegions,
    #[error("The specified entry is not found in the region cache")]
    EntryNotFoundInRegionCache,
    /// Wraps a `std::io::Error`.
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    /// Wraps a `std::io::Error`.
    #[error("tokio channel error: {0}")]
    Channel(#[from] tokio::sync::oneshot::error::RecvError),
    /// Wraps a `grpcio::Error`.
    #[error("gRPC error: {0}")]
    Grpc(#[from] tonic::transport::Error),
    /// Wraps a `reqwest::Error`.
    /// Wraps a `grpcio::Error`.
    #[error("gRPC api error: {0}")]
    GrpcAPI(#[from] tonic::Status),
    /// Wraps a `grpcio::Error`.
    #[error("url error: {0}")]
    Url(#[from] tonic::codegen::http::uri::InvalidUri),
    /// Represents that a futures oneshot channel was cancelled.
    #[error("A futures oneshot channel was canceled. {0}")]
    Canceled(#[from] futures::channel::oneshot::Canceled),
    /// Errors caused by changes of region information
    #[error("Region error: {0:?}")]
    RegionError(Box<crate::proto::errorpb::Error>),
    /// Whether the transaction is committed or not is undetermined
    #[error("Whether the transaction is committed or not is undetermined")]
    UndeterminedError(Box<Error>),
    /// Wraps `crate::proto::kvrpcpb::KeyError`
    #[error("{0:?}")]
    KeyError(Box<crate::proto::kvrpcpb::KeyError>),
    /// Multiple errors generated from the ExtractError plan.
    #[error("Multiple errors: {0:?}")]
    ExtractedErrors(Vec<Error>),
    /// Multiple key errors
    #[error("Multiple key errors: {0:?}")]
    MultipleKeyErrors(Vec<Error>),
    /// Invalid ColumnFamily
    #[error("Unsupported column family {}", _0)]
    ColumnFamilyError(String),
    /// Can't join tokio tasks
    #[error("Failed to join tokio tasks")]
    JoinError(#[from] tokio::task::JoinError),
    /// No region is found for the given key.
    #[error("Region is not found for key: {:?}", key)]
    RegionForKeyNotFound { key: Vec<u8> },
    #[error("Region is not found for range: {:?}", range)]
    RegionForRangeNotFound { range: BoundRange },
    /// No region is found for the given id. note: distinguish it with the RegionNotFound error in errorpb.
    #[error("Region {} is not found in the response", region_id)]
    RegionNotFoundInResponse { region_id: u64 },
    /// No leader is found for the given id.
    #[error("Leader of region {} is not found", region.id)]
    LeaderNotFound { region: RegionVerId },
    /// Scan limit exceeds the maximum
    #[error("Limit {} exceeds max scan limit {}", limit, max_limit)]
    MaxScanLimitExceeded { limit: u32, max_limit: u32 },
    #[error("Invalid Semver string: {0:?}")]
    InvalidSemver(#[from] semver::Error),
    /// A string error returned by TiKV server
    #[error("Kv error. {}", message)]
    KvError { message: String },
    #[error("{}", message)]
    InternalError { message: String },
    #[error("{0}")]
    StringError(String),
    #[error("PessimisticLock error: {:?}", inner)]
    PessimisticLockError {
        inner: Box<Error>,
        success_keys: Vec<Vec<u8>>,
    },
    #[error("Keyspace not found: {0}")]
    KeyspaceNotFound(String),
}

impl From<crate::proto::errorpb::Error> for Error {
    fn from(e: crate::proto::errorpb::Error) -> Error {
        Error::RegionError(Box::new(e))
    }
}

impl From<crate::proto::kvrpcpb::KeyError> for Error {
    fn from(mut e: crate::proto::kvrpcpb::KeyError) -> Error {
        if let Some(conflict) = e.conflict.take() {
            return Error::WriteConflict(conflict.into());
        }
        if !e.retryable.is_empty() {
            return Error::Retryable {
                message: std::mem::take(&mut e.retryable),
            };
        }
        if let Some(failed) = e.assertion_failed.take() {
            return Error::AssertionFailed(failed.into());
        }
        if let Some(exist) = e.already_exist.take() {
            return Error::KeyExists(exist.into());
        }
        if let Some(deadlock) = e.deadlock.take() {
            return Error::Deadlock(deadlock.into());
        }
        if !e.abort.is_empty() {
            return Error::TxnAborted {
                message: std::mem::take(&mut e.abort),
            };
        }
        if let Some(commit_ts_too_large) = e.commit_ts_too_large.take() {
            return Error::CommitTsTooLarge {
                commit_ts: commit_ts_too_large.commit_ts,
            };
        }
        if let Some(txn_not_found) = e.txn_not_found.take() {
            return Error::TxnNotFound {
                start_ts: txn_not_found.start_ts,
            };
        }

        Error::KeyError(Box::new(e))
    }
}

/// A result holding an [`Error`](enum@Error).
pub type Result<T> = result::Result<T, Error>;

impl Error {
    pub fn is_write_conflict(&self) -> bool {
        match self {
            Error::WriteConflict(_) => true,
            Error::UndeterminedError(inner) => inner.is_write_conflict(),
            Error::PessimisticLockError { inner, .. } => inner.is_write_conflict(),
            Error::MultipleKeyErrors(errors) | Error::ExtractedErrors(errors) => {
                errors.iter().any(Error::is_write_conflict)
            }
            _ => false,
        }
    }

    pub fn is_deadlock(&self) -> bool {
        match self {
            Error::Deadlock(_) => true,
            Error::UndeterminedError(inner) => inner.is_deadlock(),
            Error::PessimisticLockError { inner, .. } => inner.is_deadlock(),
            Error::MultipleKeyErrors(errors) | Error::ExtractedErrors(errors) => {
                errors.iter().any(Error::is_deadlock)
            }
            _ => false,
        }
    }

    pub fn is_key_exists(&self) -> bool {
        match self {
            Error::KeyExists(_) => true,
            Error::UndeterminedError(inner) => inner.is_key_exists(),
            Error::PessimisticLockError { inner, .. } => inner.is_key_exists(),
            Error::MultipleKeyErrors(errors) | Error::ExtractedErrors(errors) => {
                errors.iter().any(Error::is_key_exists)
            }
            _ => false,
        }
    }

    pub fn is_assertion_failed(&self) -> bool {
        match self {
            Error::AssertionFailed(_) => true,
            Error::UndeterminedError(inner) => inner.is_assertion_failed(),
            Error::PessimisticLockError { inner, .. } => inner.is_assertion_failed(),
            Error::MultipleKeyErrors(errors) | Error::ExtractedErrors(errors) => {
                errors.iter().any(Error::is_assertion_failed)
            }
            _ => false,
        }
    }

    pub fn is_undetermined(&self) -> bool {
        match self {
            Error::UndeterminedError(_) => true,
            Error::PessimisticLockError { inner, .. } => inner.is_undetermined(),
            Error::MultipleKeyErrors(errors) | Error::ExtractedErrors(errors) => {
                errors.iter().any(Error::is_undetermined)
            }
            _ => false,
        }
    }
}

#[doc(hidden)]
#[macro_export]
macro_rules! internal_err {
    ($e:expr) => ({
        $crate::Error::InternalError {
            message: format!("[{}:{}]: {}", file!(), line!(),  $e)
        }
    });
    ($f:tt, $($arg:expr),+) => ({
        $crate::internal_err!(format!($f, $($arg),+))
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::deadlock;
    use base64::{engine::general_purpose::STANDARD, Engine};
    use serde_json::Value;
    use serial_test::serial;
    use std::sync::atomic::{AtomicBool, Ordering};

    static REDACT_LOG_ENABLED: AtomicBool = AtomicBool::new(false);

    struct RedactLogGuard {
        prev: bool,
    }

    impl RedactLogGuard {
        fn set(enabled: bool) -> Self {
            let prev = REDACT_LOG_ENABLED.swap(enabled, Ordering::SeqCst);
            Self { prev }
        }
    }

    impl Drop for RedactLogGuard {
        fn drop(&mut self) {
            REDACT_LOG_ENABLED.store(self.prev, Ordering::SeqCst);
        }
    }

    fn encode_bytes(bytes: &[u8]) -> String {
        let bytes: &[u8] = if REDACT_LOG_ENABLED.load(Ordering::SeqCst) && !bytes.is_empty() {
            b"?"
        } else {
            bytes
        };
        STANDARD.encode(bytes)
    }

    fn debug_info_to_json(debug_info: &kvrpcpb::DebugInfo) -> Value {
        use serde_json::{Map, Value};

        let mvcc_info = debug_info
            .mvcc_info
            .iter()
            .map(|mvcc_debug_info| {
                let mut mvcc_debug_info_json = Map::new();
                if !mvcc_debug_info.key.is_empty() {
                    mvcc_debug_info_json.insert(
                        "key".to_owned(),
                        Value::String(encode_bytes(&mvcc_debug_info.key)),
                    );
                }
                if let Some(mvcc) = mvcc_debug_info.mvcc.as_ref() {
                    let mut mvcc_json = Map::new();

                    if let Some(lock) = mvcc.lock.as_ref() {
                        let mut lock_json = Map::new();
                        if lock.r#type != 0 {
                            lock_json.insert("type".to_owned(), lock.r#type.into());
                        }
                        if lock.start_ts != 0 {
                            lock_json.insert("start_ts".to_owned(), lock.start_ts.into());
                        }
                        if !lock.primary.is_empty() {
                            lock_json.insert(
                                "primary".to_owned(),
                                Value::String(encode_bytes(&lock.primary)),
                            );
                        }
                        if !lock.short_value.is_empty() {
                            lock_json.insert(
                                "short_value".to_owned(),
                                Value::String(encode_bytes(&lock.short_value)),
                            );
                        }
                        if !lock.secondaries.is_empty() {
                            lock_json.insert(
                                "secondaries".to_owned(),
                                Value::Array(
                                    lock.secondaries
                                        .iter()
                                        .map(|secondary| Value::String(encode_bytes(secondary)))
                                        .collect(),
                                ),
                            );
                        }

                        mvcc_json.insert("lock".to_owned(), Value::Object(lock_json));
                    }

                    if !mvcc.writes.is_empty() {
                        mvcc_json.insert(
                            "writes".to_owned(),
                            Value::Array(
                                mvcc.writes
                                    .iter()
                                    .map(|write| {
                                        let mut write_json = Map::new();
                                        if write.r#type != 0 {
                                            write_json
                                                .insert("type".to_owned(), write.r#type.into());
                                        }
                                        if write.start_ts != 0 {
                                            write_json.insert(
                                                "start_ts".to_owned(),
                                                write.start_ts.into(),
                                            );
                                        }
                                        if write.commit_ts != 0 {
                                            write_json.insert(
                                                "commit_ts".to_owned(),
                                                write.commit_ts.into(),
                                            );
                                        }
                                        if !write.short_value.is_empty() {
                                            write_json.insert(
                                                "short_value".to_owned(),
                                                Value::String(encode_bytes(&write.short_value)),
                                            );
                                        }
                                        Value::Object(write_json)
                                    })
                                    .collect(),
                            ),
                        );
                    }

                    if !mvcc.values.is_empty() {
                        mvcc_json.insert(
                            "values".to_owned(),
                            Value::Array(
                                mvcc.values
                                    .iter()
                                    .map(|mvcc_value| {
                                        let mut value_json = Map::new();
                                        if mvcc_value.start_ts != 0 {
                                            value_json.insert(
                                                "start_ts".to_owned(),
                                                mvcc_value.start_ts.into(),
                                            );
                                        }
                                        if !mvcc_value.value.is_empty() {
                                            value_json.insert(
                                                "value".to_owned(),
                                                Value::String(encode_bytes(&mvcc_value.value)),
                                            );
                                        }
                                        Value::Object(value_json)
                                    })
                                    .collect(),
                            ),
                        );
                    }

                    mvcc_debug_info_json.insert("mvcc".to_owned(), Value::Object(mvcc_json));
                }

                Value::Object(mvcc_debug_info_json)
            })
            .collect();

        let mut debug_info_json = Map::new();
        debug_info_json.insert("mvcc_info".to_owned(), Value::Array(mvcc_info));
        Value::Object(debug_info_json)
    }

    fn extract_debug_info_str_from_key_err(key_error: &kvrpcpb::KeyError) -> String {
        let Some(debug_info) = key_error.debug_info.as_ref() else {
            return String::new();
        };

        serde_json::to_string(&debug_info_to_json(debug_info))
            .expect("debug_info_to_json should always be serializable")
    }

    #[test]
    fn key_error_conflict_maps_to_write_conflict() {
        let mut key_error = kvrpcpb::KeyError::default();
        key_error.conflict = Some(kvrpcpb::WriteConflict {
            start_ts: 1,
            conflict_ts: 2,
            key: vec![0xAA],
            primary: vec![0xBB],
            conflict_commit_ts: 3,
            reason: write_conflict::Reason::Optimistic as i32,
        });
        let err: Error = key_error.into();
        let Error::WriteConflict(conflict) = err else {
            panic!("expected Error::WriteConflict");
        };
        assert_eq!(conflict.start_ts, 1);
        assert_eq!(conflict.conflict_ts, 2);
        assert_eq!(conflict.conflict_commit_ts, 3);
        assert_eq!(conflict.key, vec![0xAA]);
        assert_eq!(conflict.primary, vec![0xBB]);
        assert_eq!(conflict.reason, WriteConflictReason::Optimistic);
    }

    #[test]
    fn key_error_retryable_maps_to_retryable() {
        let mut key_error = kvrpcpb::KeyError::default();
        key_error.retryable = "retry me".to_owned();
        let err: Error = key_error.into();
        let Error::Retryable { message } = err else {
            panic!("expected Error::Retryable");
        };
        assert_eq!(message, "retry me");
    }

    #[test]
    fn key_error_assertion_failed_maps_to_assertion_failed() {
        let mut key_error = kvrpcpb::KeyError::default();
        key_error.assertion_failed = Some(kvrpcpb::AssertionFailed {
            start_ts: 1,
            key: vec![0xAB],
            assertion: kvrpcpb::Assertion::NotExist as i32,
            existing_start_ts: 2,
            existing_commit_ts: 3,
        });
        let err: Error = key_error.into();
        let Error::AssertionFailed(failed) = err else {
            panic!("expected Error::AssertionFailed");
        };
        assert_eq!(failed.start_ts, 1);
        assert_eq!(failed.key, vec![0xAB]);
        assert_eq!(failed.assertion, AssertionKind::NotExist);
        assert_eq!(failed.existing_start_ts, 2);
        assert_eq!(failed.existing_commit_ts, 3);
    }

    #[test]
    fn key_error_already_exist_maps_to_key_exists() {
        let mut key_error = kvrpcpb::KeyError::default();
        key_error.already_exist = Some(kvrpcpb::AlreadyExist { key: vec![0x01] });
        let err: Error = key_error.into();
        let Error::KeyExists(exist) = err else {
            panic!("expected Error::KeyExists");
        };
        assert_eq!(exist.key, vec![0x01]);
    }

    #[test]
    fn key_error_deadlock_maps_to_deadlock() {
        let mut key_error = kvrpcpb::KeyError::default();
        key_error.deadlock = Some(kvrpcpb::Deadlock {
            lock_ts: 10,
            lock_key: vec![0x11, 0x22],
            deadlock_key_hash: 42,
            wait_chain: vec![deadlock::WaitForEntry {
                txn: 1,
                wait_for_txn: 2,
                key_hash: 3,
                key: vec![0x01],
                resource_group_tag: vec![0x02],
                wait_time: 7,
            }],
            deadlock_key: vec![0x33],
        });
        let err: Error = key_error.into();
        let Error::Deadlock(deadlock) = err else {
            panic!("expected Error::Deadlock");
        };
        assert_eq!(deadlock.lock_ts, 10);
        assert_eq!(deadlock.lock_key, vec![0x11, 0x22]);
        assert_eq!(deadlock.deadlock_key_hash, 42);
        assert_eq!(deadlock.deadlock_key, vec![0x33]);
        assert_eq!(deadlock.wait_chain.len(), 1);
        assert_eq!(deadlock.wait_chain[0].txn, 1);
        assert_eq!(deadlock.wait_chain[0].wait_for_txn, 2);
        assert_eq!(deadlock.wait_chain[0].key_hash, 3);
        assert_eq!(deadlock.wait_chain[0].key, vec![0x01]);
        assert_eq!(deadlock.wait_chain[0].resource_group_tag, vec![0x02]);
        assert_eq!(deadlock.wait_chain[0].wait_time_ms, 7);
    }

    #[test]
    fn key_error_abort_maps_to_txn_aborted() {
        let mut key_error = kvrpcpb::KeyError::default();
        key_error.abort = "boom".to_owned();
        let err: Error = key_error.into();
        let Error::TxnAborted { message } = err else {
            panic!("expected Error::TxnAborted");
        };
        assert_eq!(message, "boom");
    }

    #[test]
    fn key_error_commit_ts_too_large_maps() {
        let mut key_error = kvrpcpb::KeyError::default();
        key_error.commit_ts_too_large = Some(kvrpcpb::CommitTsTooLarge { commit_ts: 123 });
        let err: Error = key_error.into();
        let Error::CommitTsTooLarge { commit_ts } = err else {
            panic!("expected Error::CommitTsTooLarge");
        };
        assert_eq!(commit_ts, 123);
    }

    #[test]
    fn key_error_txn_not_found_maps() {
        let mut key_error = kvrpcpb::KeyError::default();
        key_error.txn_not_found = Some(kvrpcpb::TxnNotFound {
            start_ts: 7,
            primary_key: vec![0x01],
        });
        let err: Error = key_error.into();
        let Error::TxnNotFound { start_ts } = err else {
            panic!("expected Error::TxnNotFound");
        };
        assert_eq!(start_ts, 7);
    }

    #[test]
    fn undetermined_error_query() {
        assert!(Error::UndeterminedError(Box::new(Error::Unimplemented)).is_undetermined());
        assert!(!Error::Unimplemented.is_undetermined());
    }

    #[test]
    fn reason_and_assertion_kind_mappings_are_stable() {
        assert_eq!(
            WriteConflictReason::from(write_conflict::Reason::Optimistic as i32).to_string(),
            "optimistic"
        );
        assert_eq!(
            WriteConflictReason::from(write_conflict::Reason::PessimisticRetry as i32).to_string(),
            "pessimistic_retry"
        );
        assert_eq!(
            WriteConflictReason::from(write_conflict::Reason::SelfRolledBack as i32).to_string(),
            "self_rolled_back"
        );
        assert_eq!(
            WriteConflictReason::from(write_conflict::Reason::RcCheckTs as i32).to_string(),
            "rc_check_ts"
        );
        assert_eq!(
            WriteConflictReason::from(write_conflict::Reason::LazyUniquenessCheck as i32)
                .to_string(),
            "lazy_uniqueness_check"
        );
        assert_eq!(
            WriteConflictReason::from(write_conflict::Reason::NotLockedKeyConflict as i32)
                .to_string(),
            "not_locked_key_conflict"
        );
        assert_eq!(WriteConflictReason::from(-1).to_string(), "unknown");

        assert_eq!(
            AssertionKind::from(kvrpcpb::Assertion::Exist as i32).to_string(),
            "exist"
        );
        assert_eq!(
            AssertionKind::from(kvrpcpb::Assertion::NotExist as i32).to_string(),
            "not_exist"
        );
        assert_eq!(AssertionKind::from(-1).to_string(), "none");
    }

    #[test]
    fn error_queries_recurse_through_wrappers() {
        fn make_wc() -> Error {
            Error::WriteConflict(WriteConflictError {
                start_ts: 1,
                conflict_ts: 2,
                conflict_commit_ts: 3,
                key: vec![0x01],
                primary: vec![0x02],
                reason: WriteConflictReason::Optimistic,
            })
        }

        let wc = make_wc();
        assert!(wc.is_write_conflict());
        assert!(!wc.is_deadlock());
        assert!(!wc.is_key_exists());
        assert!(!wc.is_assertion_failed());

        let wrapped = Error::UndeterminedError(Box::new(make_wc()));
        assert!(wrapped.is_write_conflict());
        assert!(wrapped.is_undetermined());

        let pessimistic = Error::PessimisticLockError {
            inner: Box::new(make_wc()),
            success_keys: vec![],
        };
        assert!(pessimistic.is_write_conflict());

        let multi = Error::MultipleKeyErrors(vec![Error::Unimplemented, make_wc()]);
        assert!(multi.is_write_conflict());

        let extracted = Error::ExtractedErrors(vec![Error::Unimplemented, make_wc()]);
        assert!(extracted.is_write_conflict());
    }

    #[test]
    fn internal_err_macro_contains_message() {
        let err = crate::internal_err!("boom");
        let msg = err.to_string();
        assert!(msg.contains("boom"), "{msg}");
    }

    #[test]
    #[serial]
    fn extract_debug_info_str_from_key_err_matches_client_go() {
        let debug_info = kvrpcpb::DebugInfo {
            mvcc_info: vec![kvrpcpb::MvccDebugInfo {
                key: b"byte".to_vec(),
                mvcc: Some(kvrpcpb::MvccInfo {
                    lock: Some(kvrpcpb::MvccLock {
                        r#type: kvrpcpb::Op::Del as i32,
                        start_ts: 128,
                        primary: b"k1".to_vec(),
                        secondaries: vec![b"k1".to_vec(), b"k2".to_vec()],
                        short_value: b"v1".to_vec(),
                        ..Default::default()
                    }),
                    writes: vec![kvrpcpb::MvccWrite {
                        r#type: kvrpcpb::Op::Insert as i32,
                        start_ts: 64,
                        commit_ts: 86,
                        short_value: vec![0x1, 0x2, 0x3, 0x4, 0x5, 0x6],
                        ..Default::default()
                    }],
                    values: vec![kvrpcpb::MvccValue {
                        start_ts: 64,
                        value: vec![0x11, 0x12],
                    }],
                }),
            }],
        };

        {
            let _guard = RedactLogGuard::set(false);
            assert_eq!(
                extract_debug_info_str_from_key_err(&kvrpcpb::KeyError {
                    txn_lock_not_found: Some(kvrpcpb::TxnLockNotFound {
                        key: b"byte".to_vec(),
                    }),
                    ..Default::default()
                }),
                ""
            );

            let expected_str = r#"{"mvcc_info":[{"key":"Ynl0ZQ==","mvcc":{"lock":{"type":1,"start_ts":128,"primary":"azE=","short_value":"djE=","secondaries":["azE=","azI="]},"writes":[{"type":4,"start_ts":64,"commit_ts":86,"short_value":"AQIDBAUG"}],"values":[{"start_ts":64,"value":"ERI="}]}}]}"#;
            let actual = extract_debug_info_str_from_key_err(&kvrpcpb::KeyError {
                txn_lock_not_found: Some(kvrpcpb::TxnLockNotFound {
                    key: b"byte".to_vec(),
                }),
                debug_info: Some(debug_info.clone()),
                ..Default::default()
            });

            let expected_json: Value = serde_json::from_str(expected_str).unwrap();
            let actual_json: Value = serde_json::from_str(&actual).unwrap();
            assert_eq!(actual_json, expected_json);
        }

        {
            let _guard = RedactLogGuard::set(true);
            let expected_str = r#"{"mvcc_info":[{"key":"Pw==","mvcc":{"lock":{"type":1,"start_ts":128,"primary":"Pw==","short_value":"Pw==","secondaries":["Pw==","Pw=="]},"writes":[{"type":4,"start_ts":64,"commit_ts":86,"short_value":"Pw=="}],"values":[{"start_ts":64,"value":"Pw=="}]}}]}"#;
            let actual = extract_debug_info_str_from_key_err(&kvrpcpb::KeyError {
                txn_lock_not_found: Some(kvrpcpb::TxnLockNotFound {
                    key: b"byte".to_vec(),
                }),
                debug_info: Some(debug_info),
                ..Default::default()
            });
            let expected_json: Value = serde_json::from_str(expected_str).unwrap();
            let actual_json: Value = serde_json::from_str(&actual).unwrap();
            assert_eq!(actual_json, expected_json);
        }
    }
}
