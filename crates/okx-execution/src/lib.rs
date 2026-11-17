//! Native account SQL, continuous finance and execution invariants.
pub mod account_store;
pub use account_store::*;
mod account_submission;
// A default shipping dependency must not be able to open a second SQL worker.
#[cfg_attr(
    not(feature = "legacy-account-worker-fixtures"),
    doc = "This historical writer is absent from the default public API.\n\n\u0060\u0060\u0060compile_fail,E0432\nuse okx_execution::AccountStoreHandle;\n\u0060\u0060\u0060"
)]
pub mod account_worker;
pub use account_submission::{CommittedVenueSend, IntentOutcome};
mod finance_owner_gate;
pub use account_worker::AccountStorageReadback;
// Historical test fixtures may opt in; the shipping library cannot construct a legacy SQL worker.
#[cfg(any(test, feature = "legacy-account-worker-fixtures"))]
pub use account_worker::AccountStoreHandle;
pub use finance_owner_gate::{LegacyAccountMaintenanceLease, hold_legacy_account_maintenance};
pub mod finance;
pub mod finance_snapshot;
pub mod risk;
