// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::ops::{Bound, Range};

use serde_derive::{Deserialize, Serialize};
use thiserror::Error;

use crate::transaction::Mutation;
use crate::{proto::errorpb, proto::metapb};
use crate::{proto::kvrpcpb, Key};
use crate::{BoundRange, KvPair};

pub const RAW_KEY_PREFIX: u8 = b'r';
pub const TXN_KEY_PREFIX: u8 = b'x';
pub const KEYSPACE_PREFIX_LEN: usize = 4;
const MAX_KEYSPACE_ID: u32 = (1 << 24) - 1;

#[cfg(test)]
pub(crate) const CODEC_V2_PREFIXES: [[u8; 1]; 2] = [[RAW_KEY_PREFIX], [TXN_KEY_PREFIX]];
#[cfg(test)]
pub(crate) const CODEC_V1_EXCLUDE_PREFIXES: [[u8; 1]; 2] = CODEC_V2_PREFIXES;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Keyspace {
    Disable,
    /// Enable APIv2 keyspace encoding.
    ///
    /// Note: TiKV keyspace IDs are 24-bit; values above `2^24-1` are invalid.
    Enable {
        keyspace_id: u32,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum KeyMode {
    Raw,
    Txn,
}

impl Keyspace {
    pub fn api_version(&self) -> kvrpcpb::ApiVersion {
        match self {
            Keyspace::Disable => kvrpcpb::ApiVersion::V1,
            Keyspace::Enable { .. } => kvrpcpb::ApiVersion::V2,
        }
    }

    pub fn try_enable(keyspace_id: u32) -> crate::Result<Self> {
        if keyspace_id > MAX_KEYSPACE_ID {
            return Err(crate::internal_err!(
                "invalid keyspace_id={} (max={})",
                keyspace_id,
                MAX_KEYSPACE_ID
            ));
        }
        Ok(Keyspace::Enable { keyspace_id })
    }
}

pub trait EncodeKeyspace {
    fn encode_keyspace(self, keyspace: Keyspace, key_mode: KeyMode) -> Self;
}

pub trait TruncateKeyspace {
    fn truncate_keyspace(self, keyspace: Keyspace) -> Self;
}

impl EncodeKeyspace for Key {
    fn encode_keyspace(mut self, keyspace: Keyspace, key_mode: KeyMode) -> Self {
        let prefix = match keyspace {
            Keyspace::Disable => {
                return self;
            }
            Keyspace::Enable { keyspace_id } => keyspace_prefix(keyspace_id, key_mode),
        };

        prepend_bytes(&mut self.0, &prefix);

        self
    }
}

impl EncodeKeyspace for KvPair {
    fn encode_keyspace(mut self, keyspace: Keyspace, key_mode: KeyMode) -> Self {
        self.key = self.key.encode_keyspace(keyspace, key_mode);
        self
    }
}

impl EncodeKeyspace for BoundRange {
    fn encode_keyspace(mut self, keyspace: Keyspace, key_mode: KeyMode) -> Self {
        self.from = match self.from {
            Bound::Included(key) => Bound::Included(key.encode_keyspace(keyspace, key_mode)),
            Bound::Excluded(key) => Bound::Excluded(key.encode_keyspace(keyspace, key_mode)),
            Bound::Unbounded => {
                let key = Key::from(vec![]);
                Bound::Included(key.encode_keyspace(keyspace, key_mode))
            }
        };
        self.to = match self.to {
            Bound::Included(key) if !key.is_empty() => {
                Bound::Included(key.encode_keyspace(keyspace, key_mode))
            }
            Bound::Excluded(key) if !key.is_empty() => {
                Bound::Excluded(key.encode_keyspace(keyspace, key_mode))
            }
            _ => {
                let mut key = Key::from(vec![]);
                if let Keyspace::Enable { keyspace_id } = keyspace {
                    let prefix = keyspace_end_prefix(keyspace_id, key_mode);
                    prepend_bytes(&mut key.0, &prefix);
                }
                Bound::Excluded(key)
            }
        };
        self
    }
}

impl EncodeKeyspace for Mutation {
    fn encode_keyspace(self, keyspace: Keyspace, key_mode: KeyMode) -> Self {
        match self {
            Mutation::Put(key, val) => Mutation::Put(key.encode_keyspace(keyspace, key_mode), val),
            Mutation::Delete(key) => Mutation::Delete(key.encode_keyspace(keyspace, key_mode)),
        }
    }
}

impl TruncateKeyspace for Key {
    fn truncate_keyspace(mut self, keyspace: Keyspace) -> Self {
        if let Keyspace::Disable = keyspace {
            return self;
        }

        pretruncate_bytes::<KEYSPACE_PREFIX_LEN>(&mut self.0);

        self
    }
}

impl TruncateKeyspace for KvPair {
    fn truncate_keyspace(mut self, keyspace: Keyspace) -> Self {
        self.key = self.key.truncate_keyspace(keyspace);
        self
    }
}

impl TruncateKeyspace for Range<Key> {
    fn truncate_keyspace(mut self, keyspace: Keyspace) -> Self {
        self.start = self.start.truncate_keyspace(keyspace);
        self.end = self.end.truncate_keyspace(keyspace);
        self
    }
}

impl TruncateKeyspace for Vec<Range<Key>> {
    fn truncate_keyspace(mut self, keyspace: Keyspace) -> Self {
        for range in &mut self {
            take_mut::take(range, |range| range.truncate_keyspace(keyspace));
        }
        self
    }
}

impl TruncateKeyspace for Vec<KvPair> {
    fn truncate_keyspace(mut self, keyspace: Keyspace) -> Self {
        for pair in &mut self {
            take_mut::take(pair, |pair| pair.truncate_keyspace(keyspace));
        }
        self
    }
}

impl TruncateKeyspace for Vec<crate::proto::kvrpcpb::LockInfo> {
    fn truncate_keyspace(mut self, keyspace: Keyspace) -> Self {
        for lock in &mut self {
            take_mut::take(&mut lock.key, |key| {
                Key::from(key).truncate_keyspace(keyspace).into()
            });
            take_mut::take(&mut lock.primary_lock, |primary| {
                Key::from(primary).truncate_keyspace(keyspace).into()
            });
            for secondary in lock.secondaries.iter_mut() {
                take_mut::take(secondary, |secondary| {
                    Key::from(secondary).truncate_keyspace(keyspace).into()
                });
            }
        }
        self
    }
}

impl TruncateKeyspace for kvrpcpb::LockInfo {
    fn truncate_keyspace(mut self, keyspace: Keyspace) -> Self {
        take_mut::take(&mut self.key, |key| {
            Key::from(key).truncate_keyspace(keyspace).into()
        });
        take_mut::take(&mut self.primary_lock, |primary| {
            Key::from(primary).truncate_keyspace(keyspace).into()
        });
        for secondary in self.secondaries.iter_mut() {
            take_mut::take(secondary, |secondary| {
                Key::from(secondary).truncate_keyspace(keyspace).into()
            });
        }
        self
    }
}

impl TruncateKeyspace for kvrpcpb::PrimaryMismatch {
    fn truncate_keyspace(mut self, keyspace: Keyspace) -> Self {
        self.lock_info = self
            .lock_info
            .take()
            .map(|lock| lock.truncate_keyspace(keyspace));
        self
    }
}

impl TruncateKeyspace for kvrpcpb::TxnLockNotFound {
    fn truncate_keyspace(mut self, keyspace: Keyspace) -> Self {
        take_mut::take(&mut self.key, |key| {
            Key::from(key).truncate_keyspace(keyspace).into()
        });
        self
    }
}

impl TruncateKeyspace for kvrpcpb::MvccDebugInfo {
    fn truncate_keyspace(mut self, keyspace: Keyspace) -> Self {
        take_mut::take(&mut self.key, |key| {
            Key::from(key).truncate_keyspace(keyspace).into()
        });
        self
    }
}

impl TruncateKeyspace for kvrpcpb::DebugInfo {
    fn truncate_keyspace(mut self, keyspace: Keyspace) -> Self {
        self.mvcc_info = self
            .mvcc_info
            .into_iter()
            .map(|info| info.truncate_keyspace(keyspace))
            .collect();
        self
    }
}

impl TruncateKeyspace for kvrpcpb::WriteConflict {
    fn truncate_keyspace(mut self, keyspace: Keyspace) -> Self {
        take_mut::take(&mut self.key, |key| {
            Key::from(key).truncate_keyspace(keyspace).into()
        });
        take_mut::take(&mut self.primary, |primary| {
            Key::from(primary).truncate_keyspace(keyspace).into()
        });
        self
    }
}

impl TruncateKeyspace for kvrpcpb::AlreadyExist {
    fn truncate_keyspace(mut self, keyspace: Keyspace) -> Self {
        take_mut::take(&mut self.key, |key| {
            Key::from(key).truncate_keyspace(keyspace).into()
        });
        self
    }
}

impl TruncateKeyspace for kvrpcpb::CommitTsExpired {
    fn truncate_keyspace(mut self, keyspace: Keyspace) -> Self {
        take_mut::take(&mut self.key, |key| {
            Key::from(key).truncate_keyspace(keyspace).into()
        });
        self
    }
}

impl TruncateKeyspace for kvrpcpb::TxnNotFound {
    fn truncate_keyspace(mut self, keyspace: Keyspace) -> Self {
        take_mut::take(&mut self.primary_key, |primary| {
            Key::from(primary).truncate_keyspace(keyspace).into()
        });
        self
    }
}

impl TruncateKeyspace for kvrpcpb::AssertionFailed {
    fn truncate_keyspace(mut self, keyspace: Keyspace) -> Self {
        take_mut::take(&mut self.key, |key| {
            Key::from(key).truncate_keyspace(keyspace).into()
        });
        self
    }
}

impl TruncateKeyspace for kvrpcpb::Deadlock {
    fn truncate_keyspace(mut self, keyspace: Keyspace) -> Self {
        take_mut::take(&mut self.lock_key, |key| {
            Key::from(key).truncate_keyspace(keyspace).into()
        });
        take_mut::take(&mut self.deadlock_key, |key| {
            Key::from(key).truncate_keyspace(keyspace).into()
        });
        for entry in &mut self.wait_chain {
            take_mut::take(&mut entry.key, |key| {
                Key::from(key).truncate_keyspace(keyspace).into()
            });
        }
        self
    }
}

impl TruncateKeyspace for kvrpcpb::KeyError {
    fn truncate_keyspace(mut self, keyspace: Keyspace) -> Self {
        if let Keyspace::Disable = keyspace {
            return self;
        }

        self.locked = self
            .locked
            .take()
            .map(|lock| lock.truncate_keyspace(keyspace));
        self.primary_mismatch = self
            .primary_mismatch
            .take()
            .map(|mismatch| mismatch.truncate_keyspace(keyspace));
        self.txn_lock_not_found = self
            .txn_lock_not_found
            .take()
            .map(|not_found| not_found.truncate_keyspace(keyspace));
        self.debug_info = self
            .debug_info
            .take()
            .map(|debug| debug.truncate_keyspace(keyspace));

        self.conflict = self
            .conflict
            .take()
            .map(|conflict| conflict.truncate_keyspace(keyspace));
        self.already_exist = self
            .already_exist
            .take()
            .map(|exist| exist.truncate_keyspace(keyspace));
        self.deadlock = self
            .deadlock
            .take()
            .map(|deadlock| deadlock.truncate_keyspace(keyspace));
        self.commit_ts_expired = self
            .commit_ts_expired
            .take()
            .map(|expired| expired.truncate_keyspace(keyspace));
        self.txn_not_found = self
            .txn_not_found
            .take()
            .map(|not_found| not_found.truncate_keyspace(keyspace));
        self.assertion_failed = self
            .assertion_failed
            .take()
            .map(|failed| failed.truncate_keyspace(keyspace));

        self
    }
}

impl TruncateKeyspace for errorpb::Error {
    fn truncate_keyspace(mut self, keyspace: Keyspace) -> Self {
        let Keyspace::Enable { keyspace_id } = keyspace else {
            return self;
        };

        if let Some(epoch_not_match) = self.epoch_not_match.take() {
            self.epoch_not_match = Some(
                truncate_epoch_not_match(epoch_not_match.clone(), keyspace_id)
                    // Best-effort: keep the original error payload if we fail to decode.
                    .unwrap_or(epoch_not_match),
            );
        }

        self
    }
}

fn truncate_epoch_not_match(
    mut err: errorpb::EpochNotMatch,
    keyspace_id: u32,
) -> crate::Result<errorpb::EpochNotMatch> {
    // The TiKV/PD region metadata keys are memcomparable encoded. Decode them first, then
    // intersect with the current keyspace range and strip the keyspace prefix. This mirrors
    // client-go's apicodec v2 behavior and avoids leaking encoded keys/ranges to callers.
    for region in &mut err.current_regions {
        crate::kv::codec::decode_bytes_in_place(&mut region.start_key, false)?;
        crate::kv::codec::decode_bytes_in_place(&mut region.end_key, false)?;
    }

    let key_mode = err
        .current_regions
        .iter()
        .find_map(infer_key_mode_from_region)
        .unwrap_or(KeyMode::Raw);
    let keyspace_prefix = keyspace_prefix(keyspace_id, key_mode).to_vec();
    let keyspace_end = keyspace_end_prefix(keyspace_id, key_mode).to_vec();

    let mut regions = Vec::with_capacity(err.current_regions.len());
    for mut region in err.current_regions.into_iter() {
        let mut start_key = std::mem::take(&mut region.start_key);
        let mut end_key = std::mem::take(&mut region.end_key);

        if !end_key.is_empty() && end_key <= keyspace_prefix {
            continue;
        }
        if !start_key.is_empty() && start_key >= keyspace_end {
            continue;
        }

        if start_key.is_empty() || start_key < keyspace_prefix {
            start_key = keyspace_prefix.clone();
        }
        if end_key.is_empty() || end_key > keyspace_end {
            end_key = keyspace_end.clone();
        }

        if start_key >= end_key {
            continue;
        }

        region.start_key = if start_key == keyspace_prefix {
            Vec::new()
        } else {
            start_key.split_off(KEYSPACE_PREFIX_LEN)
        };
        region.end_key = if end_key == keyspace_end {
            Vec::new()
        } else {
            end_key.split_off(KEYSPACE_PREFIX_LEN)
        };

        regions.push(region);
    }

    err.current_regions = regions;
    Ok(err)
}

fn infer_key_mode_from_region(region: &metapb::Region) -> Option<KeyMode> {
    infer_key_mode_from_key(&region.start_key).or_else(|| infer_key_mode_from_key(&region.end_key))
}

fn infer_key_mode_from_key(key: &[u8]) -> Option<KeyMode> {
    match key.first().copied() {
        Some(RAW_KEY_PREFIX) => Some(KeyMode::Raw),
        Some(TXN_KEY_PREFIX) => Some(KeyMode::Txn),
        _ => None,
    }
}

fn keyspace_prefix(keyspace_id: u32, key_mode: KeyMode) -> [u8; KEYSPACE_PREFIX_LEN] {
    let mut prefix = keyspace_id.to_be_bytes();
    prefix[0] = match key_mode {
        KeyMode::Raw => RAW_KEY_PREFIX,
        KeyMode::Txn => TXN_KEY_PREFIX,
    };
    prefix
}

fn keyspace_end_prefix(keyspace_id: u32, key_mode: KeyMode) -> [u8; KEYSPACE_PREFIX_LEN] {
    let mut end = keyspace_prefix(keyspace_id, key_mode);
    for byte in end.iter_mut().rev() {
        let (value, overflow) = byte.overflowing_add(1);
        *byte = value;
        if !overflow {
            break;
        }
    }
    end
}

pub(crate) fn decode_bucket_keys(
    bucket_keys: impl IntoIterator<Item = Vec<u8>>,
    keyspace: Keyspace,
    key_mode: KeyMode,
) -> crate::Result<Vec<Vec<u8>>> {
    let Keyspace::Enable { keyspace_id } = keyspace else {
        return Ok(bucket_keys.into_iter().collect());
    };

    let keyspace_prefix = keyspace_prefix(keyspace_id, key_mode).to_vec();
    let keyspace_end = keyspace_end_prefix(keyspace_id, key_mode).to_vec();

    let mut decoded = Vec::new();
    for mut key in bucket_keys.into_iter() {
        if key.is_empty() {
            push_dedup_key(&mut decoded, Vec::new());
            continue;
        }

        crate::kv::codec::decode_bytes_in_place(&mut key, false)?;
        if key.is_empty() {
            push_dedup_key(&mut decoded, Vec::new());
            continue;
        }

        if key < keyspace_prefix {
            continue;
        }
        if key >= keyspace_end {
            push_dedup_key(&mut decoded, Vec::new());
            break;
        }
        if key.len() < KEYSPACE_PREFIX_LEN || !key.starts_with(&keyspace_prefix) {
            // This should not happen for keyspace-prefixed v2 bucket keys.
            continue;
        }
        push_dedup_key(&mut decoded, key.split_off(KEYSPACE_PREFIX_LEN));
    }

    if decoded.first().map(|k| !k.is_empty()).unwrap_or(true) {
        decoded.insert(0, Vec::new());
    }
    if decoded.last().map(|k| !k.is_empty()).unwrap_or(true) {
        decoded.push(Vec::new());
    }

    Ok(decoded)
}

fn push_dedup_key(keys: &mut Vec<Vec<u8>>, key: Vec<u8>) {
    if keys.last().is_some_and(|prev| prev == &key) {
        return;
    }
    keys.push(key);
}

fn prepend_bytes<const N: usize>(vec: &mut Vec<u8>, prefix: &[u8; N]) {
    let original_len = vec.len();
    vec.reserve_exact(N);
    vec.resize(original_len + N, 0);
    vec.copy_within(0..original_len, N);
    vec[..N].copy_from_slice(prefix);
}

fn pretruncate_bytes<const N: usize>(vec: &mut Vec<u8>) {
    if vec.len() < N {
        return;
    }
    let original_len = vec.len();
    vec.copy_within(N..original_len, 0);
    vec.truncate(original_len - N);
}

impl TruncateKeyspace for crate::Error {
    fn truncate_keyspace(self, keyspace: Keyspace) -> Self {
        if let Keyspace::Disable = keyspace {
            return self;
        }

        match self {
            crate::Error::RegionError(err) => {
                crate::Error::RegionError(Box::new((*err).truncate_keyspace(keyspace)))
            }
            crate::Error::WriteConflict(mut conflict) => {
                conflict.key = Key::from(conflict.key).truncate_keyspace(keyspace).into();
                conflict.primary = Key::from(conflict.primary)
                    .truncate_keyspace(keyspace)
                    .into();
                crate::Error::WriteConflict(conflict)
            }
            crate::Error::Deadlock(mut deadlock) => {
                deadlock.lock_key = Key::from(deadlock.lock_key)
                    .truncate_keyspace(keyspace)
                    .into();
                deadlock.deadlock_key = Key::from(deadlock.deadlock_key)
                    .truncate_keyspace(keyspace)
                    .into();
                for entry in &mut deadlock.wait_chain {
                    take_mut::take(&mut entry.key, |key| {
                        Key::from(key).truncate_keyspace(keyspace).into()
                    });
                }
                crate::Error::Deadlock(deadlock)
            }
            crate::Error::KeyExists(mut exist) => {
                exist.key = Key::from(exist.key).truncate_keyspace(keyspace).into();
                crate::Error::KeyExists(exist)
            }
            crate::Error::AssertionFailed(mut failed) => {
                failed.key = Key::from(failed.key).truncate_keyspace(keyspace).into();
                crate::Error::AssertionFailed(failed)
            }
            crate::Error::ResolveLockError(locks) => {
                crate::Error::ResolveLockError(locks.truncate_keyspace(keyspace))
            }
            crate::Error::KeyError(key_error) => {
                crate::Error::KeyError(Box::new((*key_error).truncate_keyspace(keyspace)))
            }
            crate::Error::PessimisticLockError {
                inner,
                success_keys,
            } => {
                let success_keys = success_keys
                    .into_iter()
                    .map(|key| Key::from(key).truncate_keyspace(keyspace).into())
                    .collect();
                crate::Error::PessimisticLockError {
                    inner: Box::new(inner.truncate_keyspace(keyspace)),
                    success_keys,
                }
            }
            crate::Error::UndeterminedError(inner) => {
                crate::Error::UndeterminedError(Box::new(inner.truncate_keyspace(keyspace)))
            }
            crate::Error::MultipleKeyErrors(errors) => crate::Error::MultipleKeyErrors(
                errors
                    .into_iter()
                    .map(|err| err.truncate_keyspace(keyspace))
                    .collect(),
            ),
            crate::Error::ExtractedErrors(errors) => crate::Error::ExtractedErrors(
                errors
                    .into_iter()
                    .map(|err| err.truncate_keyspace(keyspace))
                    .collect(),
            ),
            other => other,
        }
    }
}

impl<T> TruncateKeyspace for std::result::Result<T, crate::Error> {
    fn truncate_keyspace(self, keyspace: Keyspace) -> Self {
        self.map_err(|err| err.truncate_keyspace(keyspace))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Error)]
pub(crate) enum KeyspaceDecodeError {
    #[error("invalid api v2 key: too short")]
    TooShort,
    #[error("invalid api v2 key: unknown mode prefix {prefix:#x}")]
    UnknownModePrefix { prefix: u8 },
}

fn check_v2_key(encoded: &[u8]) -> Result<(), KeyspaceDecodeError> {
    if encoded.len() < KEYSPACE_PREFIX_LEN {
        return Err(KeyspaceDecodeError::TooShort);
    }
    match encoded[0] {
        RAW_KEY_PREFIX | TXN_KEY_PREFIX => Ok(()),
        prefix => Err(KeyspaceDecodeError::UnknownModePrefix { prefix }),
    }
}

pub(crate) fn parse_keyspace_id(encoded: &[u8]) -> Result<u32, KeyspaceDecodeError> {
    check_v2_key(encoded)?;
    Ok(u32::from_be_bytes([0, encoded[1], encoded[2], encoded[3]]))
}

#[cfg(test)]
pub(crate) fn decode_key(
    encoded: &[u8],
    version: crate::proto::kvrpcpb::ApiVersion,
) -> Result<(&[u8], &[u8]), KeyspaceDecodeError> {
    match version {
        crate::proto::kvrpcpb::ApiVersion::V1 | crate::proto::kvrpcpb::ApiVersion::V1ttl => {
            Ok((&[], encoded))
        }
        crate::proto::kvrpcpb::ApiVersion::V2 => {
            check_v2_key(encoded)?;
            Ok((
                &encoded[..KEYSPACE_PREFIX_LEN],
                &encoded[KEYSPACE_PREFIX_LEN..],
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encode_memcomparable(key: &[u8]) -> Vec<u8> {
        use crate::kv::codec::{max_encoded_bytes_size, BytesEncoder};

        let mut encoded = Vec::with_capacity(max_encoded_bytes_size(key.len()));
        encoded.encode_bytes(key, false).unwrap();
        encoded
    }

    #[test]
    fn test_keyspace_prefix() {
        let key_mode = KeyMode::Raw;
        assert_eq!(keyspace_prefix(0, key_mode), [b'r', 0, 0, 0]);
        assert_eq!(keyspace_prefix(1, key_mode), [b'r', 0, 0, 1]);
        assert_eq!(keyspace_prefix(0xFFFF, key_mode), [b'r', 0, 0xFF, 0xFF]);

        let key_mode = KeyMode::Txn;
        assert_eq!(keyspace_prefix(0, key_mode), [b'x', 0, 0, 0]);
        assert_eq!(keyspace_prefix(1, key_mode), [b'x', 0, 0, 1]);
        assert_eq!(keyspace_prefix(0xFFFF, key_mode), [b'x', 0, 0xFF, 0xFF]);
    }

    #[test]
    fn try_enable_rejects_out_of_range_keyspace_id() {
        assert!(Keyspace::try_enable(u32::MAX).is_err());
        assert_eq!(
            Keyspace::try_enable((1 << 24) - 1).unwrap(),
            Keyspace::Enable {
                keyspace_id: (1 << 24) - 1
            }
        );
    }

    #[test]
    fn test_keyspace_end_prefix_carries_over_mode_byte() {
        assert_eq!(
            keyspace_end_prefix((1 << 8) - 1, KeyMode::Txn),
            [b'x', 0, 1, 0]
        );
        assert_eq!(
            keyspace_end_prefix((1 << 16) - 1, KeyMode::Txn),
            [b'x', 1, 0, 0]
        );
        assert_eq!(
            keyspace_end_prefix((1 << 24) - 2, KeyMode::Raw),
            [b'r', 255, 255, 255]
        );
        assert_eq!(
            keyspace_end_prefix((1 << 24) - 1, KeyMode::Raw),
            [b's', 0, 0, 0]
        );
    }

    #[test]
    fn test_encode_version() {
        let keyspace = Keyspace::Enable {
            keyspace_id: 0xDEAD,
        };
        let key_mode = KeyMode::Raw;

        let key = Key::from(vec![0xBE, 0xEF]);
        let expected_key = Key::from(vec![b'r', 0, 0xDE, 0xAD, 0xBE, 0xEF]);
        assert_eq!(key.encode_keyspace(keyspace, key_mode), expected_key);

        let bound: BoundRange = (Key::from(vec![0xDE, 0xAD])..Key::from(vec![0xBE, 0xEF])).into();
        let expected_bound: BoundRange = (Key::from(vec![b'r', 0, 0xDE, 0xAD, 0xDE, 0xAD])
            ..Key::from(vec![b'r', 0, 0xDE, 0xAD, 0xBE, 0xEF]))
            .into();
        assert_eq!(bound.encode_keyspace(keyspace, key_mode), expected_bound);

        let bound: BoundRange = (..).into();
        let expected_bound: BoundRange =
            (Key::from(vec![b'r', 0, 0xDE, 0xAD])..Key::from(vec![b'r', 0, 0xDE, 0xAE])).into();
        assert_eq!(bound.encode_keyspace(keyspace, key_mode), expected_bound);

        let bound: BoundRange = (Key::from(vec![])..Key::from(vec![])).into();
        let expected_bound: BoundRange =
            (Key::from(vec![b'r', 0, 0xDE, 0xAD])..Key::from(vec![b'r', 0, 0xDE, 0xAE])).into();
        assert_eq!(bound.encode_keyspace(keyspace, key_mode), expected_bound);

        let bound: BoundRange = (Key::from(vec![])..=Key::from(vec![])).into();
        let expected_bound: BoundRange =
            (Key::from(vec![b'r', 0, 0xDE, 0xAD])..Key::from(vec![b'r', 0, 0xDE, 0xAE])).into();
        assert_eq!(bound.encode_keyspace(keyspace, key_mode), expected_bound);

        let mutation = Mutation::Put(Key::from(vec![0xBE, 0xEF]), vec![4, 5, 6]);
        let expected_mutation = Mutation::Put(
            Key::from(vec![b'r', 0, 0xDE, 0xAD, 0xBE, 0xEF]),
            vec![4, 5, 6],
        );
        assert_eq!(
            mutation.encode_keyspace(keyspace, key_mode),
            expected_mutation
        );

        let mutation = Mutation::Delete(Key::from(vec![0xBE, 0xEF]));
        let expected_mutation = Mutation::Delete(Key::from(vec![b'r', 0, 0xDE, 0xAD, 0xBE, 0xEF]));
        assert_eq!(
            mutation.encode_keyspace(keyspace, key_mode),
            expected_mutation
        );

        let keyspace = Keyspace::Enable {
            keyspace_id: (1 << 24) - 1,
        };
        let expected_bound: BoundRange =
            (Key::from(vec![b'r', 255, 255, 255])..Key::from(vec![b's', 0, 0, 0])).into();
        let bound: BoundRange = (..).into();
        assert_eq!(
            bound.encode_keyspace(keyspace, KeyMode::Raw),
            expected_bound
        );
    }

    #[test]
    fn test_encode_v2_key_ranges_matches_apicodec() {
        let keyspace = Keyspace::Enable { keyspace_id: 4242 };
        let key_mode = KeyMode::Raw;

        let key_ranges: [BoundRange; 4] = [
            (Key::from(vec![])..Key::from(vec![])).into(),
            (Key::from(vec![])..Key::from(vec![b'z'])).into(),
            (Key::from(vec![b'a'])..Key::from(vec![])).into(),
            (Key::from(vec![b'a'])..Key::from(vec![b'z'])).into(),
        ];

        let keyspace_prefix = vec![b'r', 0, 16, 146];
        let keyspace_end = vec![b'r', 0, 16, 147];

        let mut keyspace_prefix_z = keyspace_prefix.clone();
        keyspace_prefix_z.push(b'z');
        let mut keyspace_prefix_a = keyspace_prefix.clone();
        keyspace_prefix_a.push(b'a');

        let expected: [BoundRange; 4] = [
            (Key::from(keyspace_prefix.clone())..Key::from(keyspace_end.clone())).into(),
            (Key::from(keyspace_prefix.clone())..Key::from(keyspace_prefix_z.clone())).into(),
            (Key::from(keyspace_prefix_a.clone())..Key::from(keyspace_end.clone())).into(),
            (Key::from(keyspace_prefix_a)..Key::from(keyspace_prefix_z)).into(),
        ];

        for (range, expected) in key_ranges.into_iter().zip(expected) {
            assert_eq!(range.encode_keyspace(keyspace, key_mode), expected);
        }
    }

    #[test]
    fn test_truncate_version() {
        let key = Key::from(vec![b'r', 0, 0xDE, 0xAD, 0xBE, 0xEF]);
        let keyspace = Keyspace::Enable {
            keyspace_id: 0xDEAD,
        };
        let expected_key = Key::from(vec![0xBE, 0xEF]);
        assert_eq!(key.truncate_keyspace(keyspace), expected_key);

        let key = Key::from(vec![b'x', 0, 0xDE, 0xAD, 0xBE, 0xEF]);
        let keyspace = Keyspace::Enable {
            keyspace_id: 0xDEAD,
        };
        let expected_key = Key::from(vec![0xBE, 0xEF]);
        assert_eq!(key.truncate_keyspace(keyspace), expected_key);
    }

    #[test]
    fn truncate_keyspace_is_noop_on_short_keys() {
        let keyspace = Keyspace::Enable { keyspace_id: 1 };
        let key = Key::from(vec![0xAB, 0xCD, 0xEF]);
        assert_eq!(key.clone().truncate_keyspace(keyspace), key);
    }

    #[test]
    fn test_prepend_and_pretruncate_bytes_round_trip() {
        const N: usize = KEYSPACE_PREFIX_LEN;
        let prefix = [0xAA, 0xBB, 0xCC, 0xDD];
        assert_eq!(prefix.len(), N);

        // Keep the test deterministic and fast; cover a range of sizes and contents.
        for len in 0..64usize {
            let original = (0..len as u8).collect::<Vec<_>>();

            let mut buf = original.clone();
            prepend_bytes(&mut buf, &prefix);
            assert_eq!(&buf[..N], &prefix);
            assert_eq!(&buf[N..], original.as_slice());

            pretruncate_bytes::<N>(&mut buf);
            assert_eq!(buf, original);
        }

        // Shorter-than-prefix input is a no-op.
        let mut short = vec![1, 2, 3];
        pretruncate_bytes::<N>(&mut short);
        assert_eq!(short, vec![1, 2, 3]);
    }

    #[test]
    fn test_parse_keyspace_id_matches_apicodec() {
        assert_eq!(
            parse_keyspace_id(&[b'x', 1, 2, 3, 1, 2, 3]).unwrap(),
            0x010203
        );
        assert_eq!(
            parse_keyspace_id(&[b'r', 1, 2, 3, 1, 2, 3, 4]).unwrap(),
            0x010203
        );

        assert_eq!(
            parse_keyspace_id(&[b't', 0, 0]).unwrap_err(),
            KeyspaceDecodeError::TooShort
        );
        assert_eq!(
            parse_keyspace_id(&[b't', 0, 0, 1, 1, 2, 3]).unwrap_err(),
            KeyspaceDecodeError::UnknownModePrefix { prefix: b't' }
        );
    }

    #[test]
    fn test_decode_key_matches_apicodec() {
        let (prefix, key) = decode_key(
            &[b'r', 1, 2, 3, 1, 2, 3, 4],
            crate::proto::kvrpcpb::ApiVersion::V2,
        )
        .unwrap();
        assert_eq!(prefix, &[b'r', 1, 2, 3]);
        assert_eq!(key, &[1, 2, 3, 4]);

        let (prefix, key) = decode_key(
            &[b'x', 1, 2, 3, 1, 2, 3, 4],
            crate::proto::kvrpcpb::ApiVersion::V2,
        )
        .unwrap();
        assert_eq!(prefix, &[b'x', 1, 2, 3]);
        assert_eq!(key, &[1, 2, 3, 4]);

        let (prefix, key) = decode_key(
            &[b't', 1, 2, 3, 1, 2, 3, 4],
            crate::proto::kvrpcpb::ApiVersion::V1,
        )
        .unwrap();
        assert!(prefix.is_empty());
        assert_eq!(key, &[b't', 1, 2, 3, 1, 2, 3, 4]);

        assert_eq!(
            decode_key(
                &[b't', 1, 2, 3, 1, 2, 3, 4],
                crate::proto::kvrpcpb::ApiVersion::V2,
            )
            .unwrap_err(),
            KeyspaceDecodeError::UnknownModePrefix { prefix: b't' }
        );
    }

    #[test]
    fn truncate_key_error_strips_keyspace_prefixes() {
        let keyspace = Keyspace::Enable { keyspace_id: 4242 };

        let prefix = keyspace_prefix(4242, KeyMode::Raw).to_vec();
        let mut encoded_key = prefix.clone();
        encoded_key.extend_from_slice(b"key1");

        let mut err = kvrpcpb::KeyError::default();
        err.txn_lock_not_found = Some(kvrpcpb::TxnLockNotFound {
            key: encoded_key.clone(),
        });
        err.debug_info = Some(kvrpcpb::DebugInfo {
            mvcc_info: vec![kvrpcpb::MvccDebugInfo {
                key: encoded_key.clone(),
                mvcc: Some(kvrpcpb::MvccInfo::default()),
            }],
        });

        let decoded = err.truncate_keyspace(keyspace);
        assert_eq!(
            decoded.txn_lock_not_found.as_ref().unwrap().key.as_slice(),
            b"key1"
        );
        assert_eq!(
            decoded
                .debug_info
                .as_ref()
                .unwrap()
                .mvcc_info
                .first()
                .unwrap()
                .key
                .as_slice(),
            b"key1"
        );
    }

    #[test]
    fn truncate_error_propagates_to_key_error_payload() {
        let keyspace = Keyspace::Enable { keyspace_id: 4242 };

        let prefix = keyspace_prefix(4242, KeyMode::Raw).to_vec();
        let mut encoded_key = prefix.clone();
        encoded_key.extend_from_slice(b"key1");

        let mut err = kvrpcpb::KeyError::default();
        err.txn_lock_not_found = Some(kvrpcpb::TxnLockNotFound {
            key: encoded_key.clone(),
        });
        err.debug_info = Some(kvrpcpb::DebugInfo {
            mvcc_info: vec![kvrpcpb::MvccDebugInfo {
                key: encoded_key.clone(),
                mvcc: Some(kvrpcpb::MvccInfo::default()),
            }],
        });

        let err = crate::Error::from(err).truncate_keyspace(keyspace);
        let crate::Error::KeyError(decoded) = err else {
            panic!("expected Error::KeyError");
        };
        assert_eq!(
            decoded.txn_lock_not_found.as_ref().unwrap().key.as_slice(),
            b"key1"
        );
        assert_eq!(
            decoded
                .debug_info
                .as_ref()
                .unwrap()
                .mvcc_info
                .first()
                .unwrap()
                .key
                .as_slice(),
            b"key1"
        );
    }

    #[test]
    fn test_keyspace_mode_prefixes_are_sorted() {
        // Mirrors client-go `CodecV2Prefixes()` / `CodecV1ExcludePrefixes()` ordering.
        assert!(CODEC_V2_PREFIXES.is_sorted());
        assert!(CODEC_V1_EXCLUDE_PREFIXES.is_sorted());
        assert_eq!(CODEC_V2_PREFIXES, [[b'r'], [b'x']]);
        assert_eq!(CODEC_V1_EXCLUDE_PREFIXES, [[b'r'], [b'x']]);
    }

    #[test]
    fn decode_epoch_not_match_intersects_keyspace_range_and_strips_prefix() {
        let keyspace_id = 4242;
        let keyspace = Keyspace::Enable { keyspace_id };

        // Keys below are ordered as following:
        // before_prefix, keyspace_prefix, inside_left, inside_right, keyspace_end_key, after_end_key
        // where valid keyspace range is [keyspace_prefix, keyspace_end_key).
        let keyspace_prefix = keyspace_prefix(keyspace_id, KeyMode::Raw).to_vec();
        let keyspace_end = keyspace_end_prefix(keyspace_id, KeyMode::Raw).to_vec();

        let before_prefix = vec![RAW_KEY_PREFIX, 0, 0, 1];
        let after_end_key = vec![RAW_KEY_PREFIX, 1, 0, 0];

        let mut inside_left = keyspace_prefix.clone();
        inside_left.push(100);
        let mut inside_right = keyspace_prefix.clone();
        inside_right.push(200);

        let err = errorpb::Error {
            epoch_not_match: Some(errorpb::EpochNotMatch {
                current_regions: vec![
                    metapb::Region {
                        id: 1,
                        start_key: encode_memcomparable(&keyspace_prefix),
                        end_key: encode_memcomparable(&keyspace_end),
                        ..Default::default()
                    },
                    metapb::Region {
                        id: 2,
                        start_key: encode_memcomparable(&before_prefix),
                        end_key: encode_memcomparable(&after_end_key),
                        ..Default::default()
                    },
                    metapb::Region {
                        id: 3,
                        start_key: encode_memcomparable(&before_prefix),
                        end_key: encode_memcomparable(&inside_left),
                        ..Default::default()
                    },
                    metapb::Region {
                        id: 4,
                        start_key: encode_memcomparable(&inside_left),
                        end_key: encode_memcomparable(&inside_right),
                        ..Default::default()
                    },
                    // Region 5 (empty intersection, should be removed).
                    metapb::Region {
                        id: 5,
                        start_key: encode_memcomparable(&before_prefix),
                        end_key: encode_memcomparable(&keyspace_prefix),
                        ..Default::default()
                    },
                    // Region 6 (empty intersection, should be removed).
                    metapb::Region {
                        id: 6,
                        start_key: encode_memcomparable(&keyspace_end),
                        end_key: encode_memcomparable(&after_end_key),
                        ..Default::default()
                    },
                ],
            }),
            ..Default::default()
        };

        let decoded = err.truncate_keyspace(keyspace);
        let regions = decoded
            .epoch_not_match
            .expect("epoch_not_match must be present after truncation")
            .current_regions;

        assert_eq!(regions.len(), 4);
        assert_eq!(regions[0].id, 1);
        assert!(regions[0].start_key.is_empty());
        assert!(regions[0].end_key.is_empty());

        assert_eq!(regions[1].id, 2);
        assert!(regions[1].start_key.is_empty());
        assert!(regions[1].end_key.is_empty());

        assert_eq!(regions[2].id, 3);
        assert!(regions[2].start_key.is_empty());
        assert_eq!(regions[2].end_key.as_slice(), &[100]);

        assert_eq!(regions[3].id, 4);
        assert_eq!(regions[3].start_key.as_slice(), &[100]);
        assert_eq!(regions[3].end_key.as_slice(), &[200]);
    }

    #[test]
    fn decode_bucket_keys_intersects_keyspace_range_and_strips_prefix() {
        let keyspace_id = 4242;
        let keyspace = Keyspace::Enable { keyspace_id };

        let prev_prefix = keyspace_prefix(keyspace_id - 1, KeyMode::Raw).to_vec();
        let keyspace_prefix = keyspace_prefix(keyspace_id, KeyMode::Raw).to_vec();
        let keyspace_end = keyspace_end_prefix(keyspace_id, KeyMode::Raw).to_vec();

        let encode_with_prefix = |prefix: &[u8], key: &[u8]| {
            let mut raw = Vec::with_capacity(prefix.len() + key.len());
            raw.extend_from_slice(prefix);
            raw.extend_from_slice(key);
            encode_memcomparable(&raw)
        };

        let expected = vec![
            Vec::new(),
            b"a".to_vec(),
            b"b".to_vec(),
            b"c".to_vec(),
            Vec::new(),
        ];

        let bucket_keys = vec![
            encode_with_prefix(&prev_prefix, b"a"),
            encode_with_prefix(&prev_prefix, b"b"),
            encode_with_prefix(&prev_prefix, b"c"),
            encode_with_prefix(&keyspace_prefix, b"a"),
            encode_with_prefix(&keyspace_prefix, b"b"),
            encode_with_prefix(&keyspace_prefix, b"c"),
            encode_with_prefix(&keyspace_end, b""),
            encode_with_prefix(&keyspace_end, b"a"),
            encode_with_prefix(&keyspace_end, b"b"),
            encode_with_prefix(&keyspace_end, b"c"),
        ];
        assert_eq!(
            decode_bucket_keys(bucket_keys, keyspace, KeyMode::Raw).unwrap(),
            expected
        );

        let bucket_keys = vec![
            encode_with_prefix(&prev_prefix, b"a"),
            encode_with_prefix(&prev_prefix, b"b"),
            encode_with_prefix(&prev_prefix, b"c"),
            encode_with_prefix(&keyspace_prefix, b""),
            encode_with_prefix(&keyspace_prefix, b"a"),
            encode_with_prefix(&keyspace_prefix, b"b"),
            encode_with_prefix(&keyspace_prefix, b"c"),
            Vec::new(),
        ];
        assert_eq!(
            decode_bucket_keys(bucket_keys, keyspace, KeyMode::Raw).unwrap(),
            expected
        );

        let bucket_keys = vec![
            Vec::new(),
            encode_with_prefix(&prev_prefix, b"a"),
            encode_with_prefix(&prev_prefix, b"b"),
            encode_with_prefix(&prev_prefix, b"c"),
            encode_with_prefix(&keyspace_prefix, b""),
            encode_with_prefix(&keyspace_prefix, b"a"),
            encode_with_prefix(&keyspace_prefix, b"b"),
            encode_with_prefix(&keyspace_prefix, b"c"),
            Vec::new(),
        ];
        assert_eq!(
            decode_bucket_keys(bucket_keys, keyspace, KeyMode::Raw).unwrap(),
            expected
        );
    }
}
