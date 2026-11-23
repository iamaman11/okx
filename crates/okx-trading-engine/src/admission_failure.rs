//! Bounded admission errors retain their operation stage and safe cause.
use okx_trade_execution::StoreError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AdmissionStage {
    ReserveResolve,
    ReserveInput,
    ReserveOwner,
    ReserveResult,
    PostResolve,
    PostInput,
    PostOwner,
    PostResult,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AdmissionCause {
    Local,
    Invalid,
    Sql(Option<rusqlite::ErrorCode>),
    Io(std::io::ErrorKind),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct AdmissionFailure {
    pub(crate) stage: AdmissionStage,
    pub(crate) cause: AdmissionCause,
    pub(crate) stable_wire_code: &'static str,
}

impl AdmissionFailure {
    pub(crate) fn local(stage: AdmissionStage, code: &'static str) -> Self {
        Self {
            stage,
            cause: AdmissionCause::Local,
            stable_wire_code: code,
        }
    }

    /// SQL and I/O errors do not establish whether a transaction committed.
    pub(crate) fn store(stage: AdmissionStage, error: StoreError) -> Self {
        let unknown = match stage {
            AdmissionStage::ReserveOwner => "NATIVE_RESERVE_OUTCOME_UNKNOWN",
            AdmissionStage::PostOwner => "NATIVE_POST_OUTCOME_UNKNOWN_DO_NOT_REPLAY",
            _ => unreachable!("store errors require an admission owner stage"),
        };
        let (cause, stable_wire_code) = match error {
            StoreError::Invalid(code) => (AdmissionCause::Invalid, code),
            StoreError::Sql(error) => {
                let code = match error {
                    rusqlite::Error::SqliteFailure(code, _) => Some(code.code),
                    _ => None,
                };
                (AdmissionCause::Sql(code), unknown)
            }
            StoreError::Io(error) => (AdmissionCause::Io(error.kind()), unknown),
        };
        Self {
            stage,
            cause,
            stable_wire_code,
        }
    }

    pub(crate) fn into_reason(self) -> &'static str {
        tracing::warn!(stage = ?self.stage, cause = ?self.cause, code = self.stable_wire_code, "admission failed");
        self.stable_wire_code
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn store_failure_preserves_typed_cause_and_safe_wire_code() {
        for (stage, unknown) in [
            (
                AdmissionStage::ReserveOwner,
                "NATIVE_RESERVE_OUTCOME_UNKNOWN",
            ),
            (
                AdmissionStage::PostOwner,
                "NATIVE_POST_OUTCOME_UNKNOWN_DO_NOT_REPLAY",
            ),
        ] {
            let invalid = AdmissionFailure::store(stage, StoreError::Invalid("SOURCE_STALE"));
            assert_eq!(invalid.cause, AdmissionCause::Invalid);
            assert_eq!(invalid.into_reason(), "SOURCE_STALE");
            let sql = AdmissionFailure::store(
                stage,
                StoreError::Sql(rusqlite::Error::SqliteFailure(
                    rusqlite::ffi::Error::new(5),
                    Some("private".into()),
                )),
            );
            assert_eq!(sql.stage, stage);
            assert_eq!(
                sql.cause,
                AdmissionCause::Sql(Some(rusqlite::ErrorCode::DatabaseBusy))
            );
            assert_eq!(sql.into_reason(), unknown);
            let io = AdmissionFailure::store(
                stage,
                StoreError::Io(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    "private",
                )),
            );
            assert_eq!(
                io.cause,
                AdmissionCause::Io(std::io::ErrorKind::PermissionDenied)
            );
            assert_eq!(io.into_reason(), unknown);
        }
        let sql = AdmissionFailure::store(
            AdmissionStage::ReserveOwner,
            StoreError::Sql(rusqlite::Error::InvalidQuery),
        );
        assert_eq!(sql.cause, AdmissionCause::Sql(None));
        let local = AdmissionFailure::local(AdmissionStage::PostInput, "SOURCE_STALE");
        assert_eq!(local.cause, AdmissionCause::Local);
        assert_eq!(local.into_reason(), "SOURCE_STALE");
    }
}
