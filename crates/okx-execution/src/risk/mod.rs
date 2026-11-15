pub mod admission;
pub mod stops;
pub use admission::*;
pub use stops::*;

// #510 r4.1: the original AccountStore+RiskBook financial rules are
// callable only on the ONE installation-owned transaction. A verified
// cutover/activation source is still required before a live COMMIT.
mod book;
pub(crate) use book::{
    AccountReadback, EntrySettlementSource, PostFenceInput, ReservationReadback, ReserveInput,
    UnifiedRiskDecision,
    VerifiedCensus, begin_post_unified_native, read_unified_campaign_account_summary,
    read_unified_campaign_reservation, read_unified_campaign_summary,
    reconcile_outcome_unified_native, recover_posting_unified_native,
    release_unsent_unified_native, reserve_unified_native, settle_entry_unified_native,
    verify_legacy_import_source, verify_unified_import_campaign,
};

#[cfg(test)]
pub(crate) use book::test_stage_scoped_begin_post;
