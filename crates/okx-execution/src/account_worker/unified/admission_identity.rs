//! Identities at the producer -> financial-owner boundary, not grants.
//! Correct identities enter the actual owner request fields.
//! ```
//! use okx_execution::account_worker::{AdmissionIntentId, AdmissionRunId, NativeReserveRequest};
//! fn bind(request: &mut NativeReserveRequest, intent: AdmissionIntentId, run: AdmissionRunId) {
//!     request.intent_id = intent;
//!     request.run_id = run;
//! }
//! ```
//! ```compile_fail,E0308
//! use okx_execution::account_worker::{AdmissionIntentId, AdmissionRunId, NativeReserveRequest};
//! fn bind(request: &mut NativeReserveRequest, intent: AdmissionIntentId, run: AdmissionRunId) {
//!     request.intent_id = run;
//!     request.run_id = intent;
//! }
//! ```
//! A reservation key cannot be replaced by an intent ID on BeginPost.
//! ```
//! use okx_execution::account_worker::{AdmissionReservationKey, NativePostRequest};
//! fn bind(request: &mut NativePostRequest, key: AdmissionReservationKey) {
//!     request.reservation_key = key;
//! }
//! ```
//! ```compile_fail,E0308
//! use okx_execution::account_worker::{AdmissionIntentId, NativePostRequest};
//! fn bind(request: &mut NativePostRequest, intent: AdmissionIntentId) {
//!     request.reservation_key = intent;
//! }
//! ```
use crate::{StoreError, StoreResult};

// Distinct concrete types prevent exchanging intent/run/reservation values.
// Shape checks do not replace affiliation and original-source checks in SQL.
macro_rules! identity {
    ($name:ident) => {
        #[derive(Debug, Clone, PartialEq, Eq)]
        pub struct $name(String);
        impl $name {
            pub fn new(value: String) -> StoreResult<Self> {
                if value.is_empty() {
                    return Err(StoreError::Invalid("NATIVE_ADMISSION_IDENTITY_REQUIRED"));
                }
                Ok(Self(value))
            }
            pub fn as_str(&self) -> &str {
                &self.0
            }
            pub fn into_string(self) -> String {
                self.0
            }
        }
        impl std::fmt::Display for $name {
            fn fmt(&self, out: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                self.0.fmt(out)
            }
        }
    };
}
identity!(AdmissionIntentId);
identity!(AdmissionRunId);
identity!(AdmissionReservationKey);

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn missing_identity_rejected_without_minting_an_owner_proof() {
        assert!(AdmissionIntentId::new(String::new()).is_err());
        assert!(AdmissionRunId::new(String::new()).is_err());
        assert!(AdmissionReservationKey::new(String::new()).is_err());
        assert_eq!(
            AdmissionIntentId::new("intent".into()).unwrap().as_str(),
            "intent"
        );
    }
}
