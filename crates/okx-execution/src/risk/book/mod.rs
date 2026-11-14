//! Native SQLite campaign owner, not a second policy authority.
//! The module is private until authenticated initialization, legacy fencing,
//! full terminal rules and production callers are proved by #510.
//! No public opening or POST permission exists in this intermediate draft.
mod owner;
mod store;
pub(crate) use store::{
    read_unified_campaign_summary, verify_legacy_import_source, verify_unified_import_campaign,
};
#[cfg(test)]
mod tests;
