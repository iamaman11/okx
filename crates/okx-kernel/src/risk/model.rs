//! Verified numerical inputs for risk decisions; no persistent owner or execution capability.
use super::money::RiskAmount;

/// All values must be loaded/validated by the transaction owner at the same revision.
#[derive(Debug, Clone)]
pub struct CampaignCapacity {
    pub loss_limit_usd: RiskAmount,
    pub loss_consumed_usd: RiskAmount,
    pub loss_reserved_usd: RiskAmount,
    pub entry_limit_usd: RiskAmount,
    pub entry_consumed_usd: RiskAmount,
    pub entry_reserved_usd: RiskAmount,
    pub entry_bound_breach_usd: RiskAmount,
}

/// Source-attested occupancy + reservations across the entire authorized cohort.
#[derive(Debug, Clone)]
pub struct ScopeCapacity {
    pub global_limit_usdt: RiskAmount,
    pub global_occupied_usdt: RiskAmount,
    pub account_limit_usdt: RiskAmount,
    pub account_occupied_usdt: RiskAmount,
    pub strategy_limit_usdt: RiskAmount,
    pub strategy_occupied_usdt: RiskAmount,
    pub order_limit_usdt: RiskAmount,
}

/// A proposal alone is never an authorization to send an exchange order.
#[derive(Debug, Clone)]
pub struct RiskProposal {
    pub loss_usdt: RiskAmount,
    pub loss_usd: RiskAmount,
    pub entry_upper_usd: RiskAmount,
}
