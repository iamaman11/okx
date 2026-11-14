pub mod admission;
pub mod stops;
pub use admission::*;
pub use stops::*;

// WIP #510: private until its verified policy/legacy transfer boundary is wired.
mod book;
pub(crate) use book::{
    AccountReadback, ReservationReadback, read_unified_campaign_account_summary,
    read_unified_campaign_reservation, read_unified_campaign_summary, verify_legacy_import_source,
    verify_unified_import_campaign,
};

#[cfg(test)]
pub(crate) use book::test_stage_scoped_begin_post;
