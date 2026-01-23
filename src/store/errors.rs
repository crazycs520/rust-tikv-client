// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::fmt::Display;

use crate::proto::kvrpcpb;
use crate::Error;

// Those that can have a single region error
pub trait HasRegionError {
    fn region_error(&mut self) -> Option<crate::proto::errorpb::Error>;
}

// Those that can have multiple region errors
pub trait HasRegionErrors {
    fn region_errors(&mut self) -> Option<Vec<crate::proto::errorpb::Error>>;
}

pub trait HasKeyErrors {
    fn key_errors(&mut self) -> Option<Vec<Error>>;
}

/// Allows setting a region error on a response type.
///
/// This is used internally to trigger a region-level retry when a request needs to be re-routed
/// (e.g. stale-read meeting locks and falling back to leader).
pub trait SetRegionError {
    fn set_region_error(&mut self, error: crate::proto::errorpb::Error);
}

impl<T: HasRegionError> HasRegionErrors for T {
    fn region_errors(&mut self) -> Option<Vec<crate::proto::errorpb::Error>> {
        self.region_error().map(|e| vec![e])
    }
}

macro_rules! has_region_error {
    ($type:ty) => {
        impl HasRegionError for $type {
            fn region_error(&mut self) -> Option<crate::proto::errorpb::Error> {
                self.region_error.take().map(|e| e.into())
            }
        }
    };
}

has_region_error!(kvrpcpb::GetResponse);
has_region_error!(kvrpcpb::ScanResponse);
has_region_error!(kvrpcpb::PrewriteResponse);
has_region_error!(kvrpcpb::CommitResponse);
has_region_error!(kvrpcpb::PessimisticLockResponse);
has_region_error!(kvrpcpb::ImportResponse);
has_region_error!(kvrpcpb::BatchRollbackResponse);
has_region_error!(kvrpcpb::PessimisticRollbackResponse);
has_region_error!(kvrpcpb::CleanupResponse);
has_region_error!(kvrpcpb::BatchGetResponse);
has_region_error!(kvrpcpb::ScanLockResponse);
has_region_error!(kvrpcpb::ResolveLockResponse);
has_region_error!(kvrpcpb::TxnHeartBeatResponse);
has_region_error!(kvrpcpb::CheckTxnStatusResponse);
has_region_error!(kvrpcpb::CheckSecondaryLocksResponse);
has_region_error!(kvrpcpb::DeleteRangeResponse);
has_region_error!(kvrpcpb::GcResponse);
has_region_error!(kvrpcpb::UnsafeDestroyRangeResponse);
has_region_error!(kvrpcpb::RawGetResponse);
has_region_error!(kvrpcpb::RawBatchGetResponse);
has_region_error!(kvrpcpb::RawGetKeyTtlResponse);
has_region_error!(kvrpcpb::RawPutResponse);
has_region_error!(kvrpcpb::RawBatchPutResponse);
has_region_error!(kvrpcpb::RawDeleteResponse);
has_region_error!(kvrpcpb::RawBatchDeleteResponse);
has_region_error!(kvrpcpb::RawDeleteRangeResponse);
has_region_error!(kvrpcpb::RawScanResponse);
has_region_error!(kvrpcpb::RawBatchScanResponse);
has_region_error!(kvrpcpb::RawCasResponse);
has_region_error!(kvrpcpb::RawCoprocessorResponse);
has_region_error!(kvrpcpb::RawChecksumResponse);
has_region_error!(kvrpcpb::FlushResponse);
has_region_error!(crate::proto::coprocessor::Response);

macro_rules! set_region_error {
    ($type:ty) => {
        impl SetRegionError for $type {
            fn set_region_error(&mut self, error: crate::proto::errorpb::Error) {
                self.region_error = Some(error);
            }
        }
    };
}

set_region_error!(kvrpcpb::GetResponse);
set_region_error!(kvrpcpb::ScanResponse);
set_region_error!(kvrpcpb::PrewriteResponse);
set_region_error!(kvrpcpb::CommitResponse);
set_region_error!(kvrpcpb::PessimisticLockResponse);
set_region_error!(kvrpcpb::ImportResponse);
set_region_error!(kvrpcpb::BatchRollbackResponse);
set_region_error!(kvrpcpb::PessimisticRollbackResponse);
set_region_error!(kvrpcpb::CleanupResponse);
set_region_error!(kvrpcpb::BatchGetResponse);
set_region_error!(kvrpcpb::ScanLockResponse);
set_region_error!(kvrpcpb::ResolveLockResponse);
set_region_error!(kvrpcpb::TxnHeartBeatResponse);
set_region_error!(kvrpcpb::CheckTxnStatusResponse);
set_region_error!(kvrpcpb::CheckSecondaryLocksResponse);
set_region_error!(kvrpcpb::DeleteRangeResponse);
set_region_error!(kvrpcpb::GcResponse);
set_region_error!(kvrpcpb::UnsafeDestroyRangeResponse);
set_region_error!(kvrpcpb::RawGetResponse);
set_region_error!(kvrpcpb::RawBatchGetResponse);
set_region_error!(kvrpcpb::RawGetKeyTtlResponse);
set_region_error!(kvrpcpb::RawPutResponse);
set_region_error!(kvrpcpb::RawBatchPutResponse);
set_region_error!(kvrpcpb::RawDeleteResponse);
set_region_error!(kvrpcpb::RawBatchDeleteResponse);
set_region_error!(kvrpcpb::RawDeleteRangeResponse);
set_region_error!(kvrpcpb::RawScanResponse);
set_region_error!(kvrpcpb::RawBatchScanResponse);
set_region_error!(kvrpcpb::RawCasResponse);
set_region_error!(kvrpcpb::RawCoprocessorResponse);
set_region_error!(kvrpcpb::RawChecksumResponse);
set_region_error!(kvrpcpb::FlushResponse);
set_region_error!(crate::proto::coprocessor::Response);

macro_rules! has_key_error {
    ($type:ty) => {
        impl HasKeyErrors for $type {
            fn key_errors(&mut self) -> Option<Vec<Error>> {
                self.error.take().map(|e| vec![e.into()])
            }
        }
    };
}

has_key_error!(kvrpcpb::GetResponse);
has_key_error!(kvrpcpb::CommitResponse);
has_key_error!(kvrpcpb::BatchRollbackResponse);
has_key_error!(kvrpcpb::CleanupResponse);
has_key_error!(kvrpcpb::ScanLockResponse);
has_key_error!(kvrpcpb::ResolveLockResponse);
has_key_error!(kvrpcpb::GcResponse);
has_key_error!(kvrpcpb::TxnHeartBeatResponse);
has_key_error!(kvrpcpb::CheckTxnStatusResponse);
has_key_error!(kvrpcpb::CheckSecondaryLocksResponse);

macro_rules! has_str_error {
    ($type:ty) => {
        impl HasKeyErrors for $type {
            fn key_errors(&mut self) -> Option<Vec<Error>> {
                if self.error.is_empty() {
                    None
                } else {
                    Some(vec![Error::KvError {
                        message: std::mem::take(&mut self.error),
                    }])
                }
            }
        }
    };
}

has_str_error!(kvrpcpb::RawGetResponse);
has_str_error!(kvrpcpb::RawGetKeyTtlResponse);
has_str_error!(kvrpcpb::RawPutResponse);
has_str_error!(kvrpcpb::RawBatchPutResponse);
has_str_error!(kvrpcpb::RawDeleteResponse);
has_str_error!(kvrpcpb::RawBatchDeleteResponse);
has_str_error!(kvrpcpb::RawDeleteRangeResponse);
has_str_error!(kvrpcpb::RawCasResponse);
has_str_error!(kvrpcpb::RawCoprocessorResponse);
has_str_error!(kvrpcpb::RawChecksumResponse);
has_str_error!(kvrpcpb::ImportResponse);
has_str_error!(kvrpcpb::DeleteRangeResponse);
has_str_error!(kvrpcpb::UnsafeDestroyRangeResponse);

impl HasKeyErrors for crate::proto::coprocessor::Response {
    fn key_errors(&mut self) -> Option<Vec<Error>> {
        if self.other_error.is_empty() {
            None
        } else {
            Some(vec![Error::KvError {
                message: std::mem::take(&mut self.other_error),
            }])
        }
    }
}

impl HasKeyErrors for kvrpcpb::ScanResponse {
    fn key_errors(&mut self) -> Option<Vec<Error>> {
        extract_errors(self.pairs.iter_mut().map(|pair| pair.error.take()))
    }
}

impl HasKeyErrors for kvrpcpb::BatchGetResponse {
    fn key_errors(&mut self) -> Option<Vec<Error>> {
        extract_errors(self.pairs.iter_mut().map(|pair| pair.error.take()))
    }
}

impl HasKeyErrors for kvrpcpb::RawBatchGetResponse {
    fn key_errors(&mut self) -> Option<Vec<Error>> {
        extract_errors(self.pairs.iter_mut().map(|pair| pair.error.take()))
    }
}

impl HasKeyErrors for kvrpcpb::RawScanResponse {
    fn key_errors(&mut self) -> Option<Vec<Error>> {
        extract_errors(self.kvs.iter_mut().map(|pair| pair.error.take()))
    }
}

impl HasKeyErrors for kvrpcpb::RawBatchScanResponse {
    fn key_errors(&mut self) -> Option<Vec<Error>> {
        extract_errors(self.kvs.iter_mut().map(|pair| pair.error.take()))
    }
}

impl HasKeyErrors for kvrpcpb::PrewriteResponse {
    fn key_errors(&mut self) -> Option<Vec<Error>> {
        extract_errors(std::mem::take(&mut self.errors).into_iter().map(Some))
    }
}

impl HasKeyErrors for kvrpcpb::FlushResponse {
    fn key_errors(&mut self) -> Option<Vec<Error>> {
        extract_errors(std::mem::take(&mut self.errors).into_iter().map(Some))
    }
}

impl HasKeyErrors for kvrpcpb::PessimisticLockResponse {
    fn key_errors(&mut self) -> Option<Vec<Error>> {
        extract_errors(std::mem::take(&mut self.errors).into_iter().map(Some))
    }
}

impl HasKeyErrors for kvrpcpb::PessimisticRollbackResponse {
    fn key_errors(&mut self) -> Option<Vec<Error>> {
        extract_errors(std::mem::take(&mut self.errors).into_iter().map(Some))
    }
}

impl<T: HasKeyErrors, E: Display> HasKeyErrors for Result<T, E> {
    fn key_errors(&mut self) -> Option<Vec<Error>> {
        match self {
            Ok(x) => x.key_errors(),
            Err(e) => Some(vec![Error::StringError(e.to_string())]),
        }
    }
}

impl<T: HasKeyErrors> HasKeyErrors for Vec<T> {
    fn key_errors(&mut self) -> Option<Vec<Error>> {
        for t in self {
            if let Some(e) = t.key_errors() {
                return Some(e);
            }
        }

        None
    }
}

impl<T: HasRegionError, E> HasRegionError for Result<T, E> {
    fn region_error(&mut self) -> Option<crate::proto::errorpb::Error> {
        self.as_mut().ok().and_then(|t| t.region_error())
    }
}

impl<T: HasRegionError> HasRegionErrors for Vec<T> {
    fn region_errors(&mut self) -> Option<Vec<crate::proto::errorpb::Error>> {
        let errors: Vec<_> = self.iter_mut().filter_map(|x| x.region_error()).collect();
        if errors.is_empty() {
            None
        } else {
            Some(errors)
        }
    }
}

fn extract_errors(
    error_iter: impl Iterator<Item = Option<kvrpcpb::KeyError>>,
) -> Option<Vec<Error>> {
    let errors: Vec<Error> = error_iter.flatten().map(Into::into).collect();
    if errors.is_empty() {
        None
    } else {
        Some(errors)
    }
}

#[cfg(test)]
mod test {
    use super::HasKeyErrors;
    use crate::common::Error;
    use crate::internal_err;
    use crate::proto::errorpb;
    use crate::proto::kvrpcpb;
    #[test]
    fn result_haslocks() {
        let mut resp: Result<_, Error> = Ok(kvrpcpb::CommitResponse::default());
        assert!(resp.key_errors().is_none());

        let mut resp: Result<_, Error> = Ok(kvrpcpb::CommitResponse {
            error: Some(kvrpcpb::KeyError::default()),
            ..Default::default()
        });
        assert!(resp.key_errors().is_some());

        let mut resp: Result<kvrpcpb::CommitResponse, _> = Err(internal_err!("some error"));
        assert!(resp.key_errors().is_some());
    }

    #[test]
    fn raw_checksum_str_error_is_key_error() {
        let mut resp = kvrpcpb::RawChecksumResponse {
            error: "boom".to_owned(),
            ..Default::default()
        };
        let errs = resp.key_errors().expect("expected error to be extracted");
        assert_eq!(errs.len(), 1);
        match &errs[0] {
            Error::KvError { message } => assert_eq!(message, "boom"),
            other => panic!("unexpected error variant: {other:?}"),
        }
    }

    #[test]
    fn vec_region_errors_are_collected_and_consumed() {
        use super::HasRegionErrors;

        let mut r1 = kvrpcpb::GetResponse::default();
        r1.region_error = Some(errorpb::Error {
            message: "e1".to_owned(),
            ..Default::default()
        });
        let mut r2 = kvrpcpb::GetResponse::default();
        r2.region_error = Some(errorpb::Error {
            message: "e2".to_owned(),
            ..Default::default()
        });

        let mut responses = vec![r1, r2];
        let errs = responses
            .region_errors()
            .expect("expected region errors to be extracted");
        assert_eq!(errs.len(), 2);
        assert_eq!(errs[0].message, "e1");
        assert_eq!(errs[1].message, "e2");

        // `region_error()` uses `take()`, so a second extraction should see none.
        assert!(responses.region_errors().is_none());
    }

    #[test]
    fn set_region_error_sets_response_field() {
        use super::SetRegionError;

        let mut resp = kvrpcpb::GetResponse::default();
        assert!(resp.region_error.is_none());

        resp.set_region_error(errorpb::Error {
            message: "boom".to_owned(),
            ..Default::default()
        });
        assert_eq!(resp.region_error.as_ref().unwrap().message, "boom");
    }
}
