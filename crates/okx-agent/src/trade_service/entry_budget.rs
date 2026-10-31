//! Exact fee-inclusive linear entry sizing. Pure arithmetic shared by manual, strategy and preview.
use super::*;
use rust_decimal::RoundingStrategy;

#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub(super) struct EntryQuote {
    pub(super) quantity: Decimal,
    pub(super) notional_usdt: Decimal,
    pub(super) fee_usdt: Decimal,
    pub(super) total_usdt: Decimal,
    pub(super) total_usd: Decimal,
    /// Observed conversion for display only; a separate maximum is mandatory for admission.
    pub(super) observed_usd_per_usdt: Decimal,
}

/// E1 cost evidence is explicit. An unavailable mandatory entry cost can
/// never be represented as zero or silently counted as NotApplicable.
// Staged E1: venue evidence constructors follow with the RiskBook binding.
// Only Unknown can be constructed by preview until authenticated proofs exist.
#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub(super) enum EntryCostEvidence {
    Proven {
        upper_usd: Decimal,
        basis: &'static str,
        source_digest: String,
    },
    NotApplicable {
        basis: &'static str,
        source_digest: String,
    },
    Unknown {
        reason: &'static str,
    },
}
/// Every classified zero and positive cost must identify a source. This
/// is necessary, not sufficient, for a venue attestation: the native source
/// owner must independently bind the digest to the actual captured fact.
fn verified_cost_source(basis: &str, digest: &str) -> Result<(), String> {
    if basis.is_empty() || digest.len() != 64 || !digest.bytes().all(|ch| ch.is_ascii_hexdigit()) {
        return Err("ENTRY_COST_SOURCE_UNVERIFIED".into());
    }
    Ok(())
}
impl EntryCostEvidence {
    fn upper(&self) -> Result<Option<Decimal>, String> {
        match self {
            Self::Proven {
                upper_usd,
                basis,
                source_digest,
            } => {
                if *upper_usd < Decimal::ZERO {
                    return Err("ENTRY_COST_NEGATIVE".into());
                }
                verified_cost_source(basis, source_digest)?;
                Ok(Some(*upper_usd))
            }
            Self::NotApplicable {
                basis,
                source_digest,
            } => {
                verified_cost_source(basis, source_digest)?;
                Ok(Some(Decimal::ZERO))
            }
            Self::Unknown { .. } => Ok(None),
        }
    }
    fn reason(&self) -> Option<&'static str> {
        match self {
            Self::Unknown { reason } => Some(*reason),
            _ => None,
        }
    }
}

/// Valuation is frozen at final admission, not a guaranteed future FX upper.
#[allow(dead_code)] // Source-backed proof producer is required before live admission.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub(super) enum BudgetValuation {
    AdmissionSnapshot {
        usd_per_usdt: Decimal,
        observed_at_ms: i64,
        source_digest: String,
        basis: &'static str,
    },
    Pending {
        reason: &'static str,
    },
}
impl BudgetValuation {
    fn rate(&self, observed: Decimal) -> Result<Option<Decimal>, String> {
        match self {
            Self::Pending { .. } => Ok(None),
            Self::AdmissionSnapshot {
                usd_per_usdt,
                observed_at_ms,
                source_digest,
                basis,
            } => {
                if *usd_per_usdt <= Decimal::ZERO
                    || observed <= Decimal::ZERO
                    || *observed_at_ms <= 0
                {
                    return Err("ENTRY_VALUATION_INVALID".into());
                }
                if *usd_per_usdt != observed {
                    return Err("ENTRY_VALUATION_RATE_MISMATCH".into());
                }
                if basis.is_empty()
                    || source_digest.len() != 64
                    || !source_digest.bytes().all(|b| b.is_ascii_hexdigit())
                {
                    return Err("ENTRY_VALUATION_SOURCE_INVALID".into());
                }
                Ok(Some(*usd_per_usdt))
            }
        }
    }
    fn reason(&self) -> Option<&'static str> {
        match self {
            Self::Pending { reason } => Some(*reason),
            _ => None,
        }
    }
    /// Bind a frozen rate to the exact independently authenticated SQL quote.
    fn bind(&self, receipt: &okx_execution::EntryQuoteEvidence) -> Result<(), String> {
        if let Self::AdmissionSnapshot {
            usd_per_usdt,
            observed_at_ms,
            source_digest,
            ..
        } = self
            && (*usd_per_usdt != receipt.usd_per_usdt
                || source_digest != &receipt.conversion_digest
                || *observed_at_ms > receipt.observed_at
                || receipt.observed_at - *observed_at_ms > 5_000)
        {
            return Err("ENTRY_VALUATION_RECEIPT_MISMATCH".into());
        }
        Ok(())
    }
}

/// A lower bound for immediately usable own collateral is not a cost upper.
#[allow(dead_code)] // Proven construction needs independently authenticated own-funds sources.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub(super) enum EntryAvailableCollateral {
    Proven {
        available_lower_usd: Decimal,
        basis: &'static str,
        source_digest: String,
    },
    Unknown {
        reason: &'static str,
    },
}
impl EntryAvailableCollateral {
    fn lower(&self) -> Result<Option<Decimal>, String> {
        match self {
            Self::Proven {
                available_lower_usd,
                basis,
                source_digest,
            } => {
                verified_cost_source(basis, source_digest)?;
                if *available_lower_usd < Decimal::ZERO {
                    return Err("ENTRY_AVAILABLE_COLLATERAL_NEGATIVE".into());
                }
                Ok(Some(*available_lower_usd))
            }
            Self::Unknown { .. } => Ok(None),
        }
    }

    fn reason(&self) -> Option<&'static str> {
        match self {
            Self::Unknown { reason } => Some(*reason),
            _ => None,
        }
    }
}

/// Entry turnover is opening notional plus fees at the source-bound admission
/// valuation. Collateral and future risk are independent dimensions.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub(super) struct EntryCostProof {
    pub(super) quote: EntryQuote,
    pub(super) extra_immediate_fees: EntryCostEvidence,
    pub(super) borrowing: EntryCostEvidence,
    /// Required margin/collateral is a separate funding requirement, never
    /// another charge against the entry-notional budget.
    pub(super) collateral_or_margin: EntryCostEvidence,
    /// Certified LOWER bound for available own collateral, in USD.
    pub(super) available_collateral: EntryAvailableCollateral,
    pub(super) valuation: BudgetValuation,
}
impl EntryCostProof {
    /// A missing venue cost or admission valuation remains Pending, including on a
    /// seemingly USD-pegged linear instrument. No synthetic source is implied.
    pub(super) fn unproven_linear_quote(quote: EntryQuote) -> Self {
        Self {
            quote,
            extra_immediate_fees: EntryCostEvidence::Unknown {
                reason: "ADDITIONAL_ENTRY_FEES_AND_FEE_CURRENCY_UNPROVEN",
            },
            borrowing: EntryCostEvidence::Unknown {
                reason: "BORROWING_APPLICABILITY_UNPROVEN",
            },
            collateral_or_margin: EntryCostEvidence::Unknown {
                reason: "MARGIN_REQUIREMENT_UNPROVEN",
            },
            available_collateral: EntryAvailableCollateral::Unknown {
                reason: "AVAILABLE_COLLATERAL_LOWER_UNPROVEN",
            },
            valuation: BudgetValuation::Pending {
                reason: "ADMISSION_FX_SNAPSHOT_UNPROVEN",
            },
        }
    }

    /// Candidate valuation shares the authenticated USDT->USD source with
    /// sizing/preview. Other economic dimensions stay Pending and MUST NOT
    /// grant a real reservation merely because the FX source is fresh.
    pub(super) fn source_valued_candidate(
        quote: EntryQuote,
        conversion: &UsdConversion,
        observed_at_ms: i64,
    ) -> Result<Self, String> {
        let source_at =
            i64::try_from(conversion.observed_at_ms).map_err(|_| "ENTRY_VALUATION_TIME_INVALID")?;
        if source_at <= 0 || source_at > observed_at_ms || observed_at_ms - source_at > 5_000 {
            return Err("ENTRY_VALUATION_SOURCE_STALE".into());
        }
        if conversion.source_digest.len() != 64
            || !conversion
                .source_digest
                .bytes()
                .all(|c| c.is_ascii_hexdigit())
        {
            return Err("ENTRY_VALUATION_SOURCE_INVALID".into());
        }
        let rate = dec(&conversion.usd_per_usdt)?;
        if rate != quote.observed_usd_per_usdt {
            return Err("ENTRY_VALUATION_RATE_MISMATCH".into());
        }
        let mut candidate = Self::unproven_linear_quote(quote);
        candidate.valuation = BudgetValuation::AdmissionSnapshot {
            usd_per_usdt: rate,
            observed_at_ms: source_at,
            source_digest: conversion.source_digest.clone(),
            basis: "account.usdt.equity_usd_div_equity:usdt_to_usd",
        };
        Ok(candidate)
    }

    /// A narrow source-backed first production lane: authenticated 2-level
    /// OKX linear USDT-settled swap, hedge account, no auto-borrow/liabilities,
    /// buy with an exchange-enforced maximum price. It never applies to
    /// short sells, market-without-price-cap, portfolio margin or other models.
    /// We require enough own *available* USDT for the full notional + fee;
    /// this deliberately over-collateralizes compared with leverage, rather
    /// than treating small margin as permission to open large turnover.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn attest_linear_usdt_sources(
        mut self,
        plan: &IntentPlan,
        order_type: &str,
        instrument: &str,
        rules: &okx_observation::InstrumentRulesSnapshot,
        fees: &okx_observation::FeeScheduleSnapshot,
        account: &okx_api::AccountConfig,
        snapshot: &okx_observation::AccountSnapshot,
        leverage: Decimal,
    ) -> Result<Self, String> {
        if !matches!(order_type, "limit" | "post_only")
            || plan.side != "buy"
            || rules.instrument.instrument_id != instrument
            || rules.instrument.state != "live"
            || rules.instrument.contract_type.as_deref() != Some("linear")
            || rules.instrument.settle_currency.as_deref() != Some("USDT")
            || rules.instrument.contract_value_currency.as_deref()
                != instrument.strip_suffix("-USDT-SWAP")
        {
            return Err("ENTRY_PRODUCT_OR_PRICE_CAP_UNSUPPORTED".into());
        }
        super::plan::verify_entry_rules_match(rules, instrument, plan)?;
        if account.account_level != "2"
            || !matches!(
                account.position_mode.as_str(),
                "long_short_mode" | "net_mode"
            )
            || snapshot.account_level != account.account_level
            || snapshot.position_mode != account.position_mode
            || snapshot.account_type != account.account_type
            || okx_api::account_uid_fingerprint(&account.uid) != snapshot.account_uid_fingerprint
        {
            return Err("ENTRY_ACCOUNT_MODE_SOURCE_UNSUPPORTED".into());
        }
        if account.auto_loan != Some(false) {
            return Err("ENTRY_AUTO_BORROW_UNVERIFIED_OR_ENABLED".into());
        }
        if leverage < Decimal::ONE {
            return Err("ENTRY_LEVERAGE_SOURCE_UNSUPPORTED".into());
        }
        // Fee generation, scope and age are independently verified against
        // the same instrument revision in the shared producer.
        let rate = verified_entry_fee_rate(fees, instrument, &rules.reference_generation, now())?;
        let rounded_fee = self
            .quote
            .notional_usdt
            .checked_mul(rate)
            .ok_or("MONEY_OVERFLOW")?
            .round_dp_with_strategy(6, RoundingStrategy::ToPositiveInfinity);
        if self.quote.fee_usdt != rounded_fee {
            return Err("ENTRY_FEE_SOURCE_CHANGED".into());
        }
        let available = snapshot
            .balance
            .details
            .iter()
            .find(|b| b.currency == "USDT")
            .ok_or("ENTRY_USDT_OWN_FUNDS_SOURCE_MISSING")?;
        // Absence of liabilities is not a certified zero for this mode.
        let liabilities = dec(available
            .liabilities
            .as_deref()
            .ok_or("ENTRY_USDT_LIABILITIES_UNPROVEN")?)?;
        if liabilities != Decimal::ZERO {
            return Err("ENTRY_EXISTING_BORROW_UNSUPPORTED".into());
        }
        let avail_eq = dec(available
            .available_equity
            .as_deref()
            .ok_or("ENTRY_AVAILABLE_EQUITY_UNPROVEN")?)?;
        let avail_bal = dec(available
            .available_balance
            .as_deref()
            .ok_or("ENTRY_AVAILABLE_BALANCE_UNPROVEN")?)?;
        if avail_eq < Decimal::ZERO || avail_bal < Decimal::ZERO {
            return Err("ENTRY_OWN_FUNDS_NEGATIVE".into());
        }
        // Taking the minimum of two independently reported available-fund
        // measures can only reduce the admissible size.
        let own_usd = avail_eq
            .min(avail_bal)
            .checked_mul(self.quote.observed_usd_per_usdt)
            .ok_or("MONEY_OVERFLOW")?;
        let conservative_margin_usd = self.quote.total_usd;
        let source_digest = digest(&(
            "okx.linear-usdt-swap-entry-sources/v1",
            instrument,
            &rules.reference_generation,
            &rules.instrument,
            &fees.fee_generation,
            &snapshot.account_generation,
            &snapshot.account_uid_fingerprint,
            account,
            available,
            leverage,
        ))?;
        self.extra_immediate_fees = EntryCostEvidence::NotApplicable {
            basis: "linear-usdt-swap:maker-taker-fee-covered-by-quote",
            source_digest: source_digest.clone(),
        };
        self.borrowing = EntryCostEvidence::NotApplicable {
            basis: "authenticated-auto-loan-disabled-and-usdt-liabilities-zero",
            source_digest: source_digest.clone(),
        };
        self.collateral_or_margin = EntryCostEvidence::Proven {
            upper_usd: conservative_margin_usd,
            basis: "cross-margin-at-most-full-notional-plus-entry-fee-for-leverage-ge-one",
            source_digest: source_digest.clone(),
        };
        self.available_collateral = EntryAvailableCollateral::Proven {
            available_lower_usd: own_usd,
            basis: "account-usdt-min-available-equity-and-balance",
            source_digest,
        };
        Ok(self)
    }

    /// Only opening notional plus immediate opening fees consume entry budget.
    /// Margin is verified against a LOWER bound on own available funds, not
    /// added to entry spend. Borrowing is unsupported in the first Real lane.
    /// UNKNOWN in any independent dimension denies admission.
    pub(super) fn verified_fx_and_entry_fees(&self) -> Result<Option<(Decimal, Decimal)>, String> {
        let Some(fx_upper) = self.valuation.rate(self.quote.observed_usd_per_usdt)? else {
            return Ok(None);
        };
        let Some(fees_upper) = self.extra_immediate_fees.upper()? else {
            return Ok(None);
        };
        let Some(borrowing_upper) = self.borrowing.upper()? else {
            return Ok(None);
        };
        let Some(margin_upper) = self.collateral_or_margin.upper()? else {
            return Ok(None);
        };
        let Some(available_lower) = self.available_collateral.lower()? else {
            return Ok(None);
        };
        if borrowing_upper > Decimal::ZERO {
            return Err("ENTRY_BORROWING_UNSUPPORTED".into());
        }
        if margin_upper > available_lower {
            return Err("ENTRY_COLLATERAL_INSUFFICIENT".into());
        }
        Ok(Some((fx_upper, fees_upper)))
    }

    /// One pure arithmetic path is shared by SQL admission and later actual
    /// fill/bill settlement. The rate is frozen at admission, not a future FX upper.
    pub(super) fn proven_upper_usd(&self) -> Result<Option<Decimal>, String> {
        let Some((fx_upper, fixed_upper)) = self.verified_fx_and_entry_fees()? else {
            return Ok(None);
        };
        if self.quote.total_usdt <= Decimal::ZERO {
            return Err("ENTRY_SOURCE_INVALID".into());
        }
        let upper = self
            .quote
            .total_usdt
            .checked_mul(fx_upper)
            .ok_or("MONEY_OVERFLOW")?
            .round_dp_with_strategy(9, RoundingStrategy::ToPositiveInfinity);
        if upper != self.quote.total_usd {
            return Err("ENTRY_VALUATION_RECOMPUTE_MISMATCH".into());
        }
        upper
            .checked_add(fixed_upper)
            .ok_or_else(|| "MONEY_OVERFLOW".into())
            .map(Some)
    }

    /// After HELD, run the same source-backed assessment again using fresh
    /// account, fee, product and FX reads. A new source generation is allowed
    /// only when the previously reserved cost upper is still exactly valid.
    /// The immutable admission is the authority; this cannot increase it.
    pub(super) fn verify_held_entry_admission(
        &self,
        quote: &okx_execution::EntryQuoteEvidence,
        admission: &okx_execution::EntryAdmissionEvidence,
    ) -> Result<(), String> {
        if admission.schema != "okx.entry-admission/v2"
            || admission.account_key != quote.account_key
            || admission.installation_id != quote.installation_id
            || admission.intent_id != quote.intent_id
            || admission.plan_hash != quote.plan_hash
            || self.quote.total_usdt != quote.total_usdt
            || self.quote.total_usd != quote.total_usd
        {
            return Err("POST_ENTRY_ADMISSION_IDENTITY_CHANGED".into());
        }
        let (rate, additional_fees) = self
            .verified_fx_and_entry_fees()?
            .ok_or("POST_ENTRY_FINANCIAL_SOURCES_UNPROVEN")?;
        let current_upper = self
            .proven_upper_usd()?
            .ok_or("POST_ENTRY_FINANCIAL_SOURCES_UNPROVEN")?;
        if rate != admission.admission_usd_per_usdt
            || additional_fees != admission.other_entry_costs_upper_usd
            || current_upper != admission.entry_upper_usd
        {
            return Err("POST_ENTRY_ADMISSION_COST_CHANGED".into());
        }
        Ok(())
    }

    pub(super) fn missing_reasons(&self) -> Vec<&'static str> {
        [
            &self.extra_immediate_fees,
            &self.borrowing,
            &self.collateral_or_margin,
        ]
        .into_iter()
        .filter_map(EntryCostEvidence::reason)
        .chain(self.available_collateral.reason())
        .chain(self.valuation.reason())
        .collect()
    }
}

/// The ONLY production source-backed assessor for a new linear USDT-SWAP
/// entry. All instrument-specific parameters come from the authenticated
/// venue specification. Preview, initial RESERVE and post-HELD checks call
/// this producer; none may invent a Proven financial dimension.
pub(super) struct EntryAssessmentSources<'a> {
    pub(super) instrument: &'a str,
    pub(super) order_type: &'a str,
    pub(super) rules: &'a okx_observation::InstrumentRulesSnapshot,
    pub(super) fees: &'a okx_observation::FeeScheduleSnapshot,
    pub(super) account: &'a okx_api::AccountConfig,
    pub(super) snapshot: &'a okx_observation::AccountSnapshot,
    pub(super) conversion: &'a UsdConversion,
    pub(super) leverage: Decimal,
    pub(super) verified_uid: &'a str,
    pub(super) observed_at_ms: i64,
}

pub(super) fn assess_order(
    plan: &IntentPlan,
    sources: EntryAssessmentSources<'_>,
) -> Result<(EntryCostProof, Decimal), String> {
    let (quote, fee_rate) = verified_known_entry_quote(
        plan,
        sources.instrument,
        sources.rules,
        sources.fees,
        sources.conversion,
        &sources.snapshot.account_uid_fingerprint,
        sources.verified_uid,
        sources.observed_at_ms,
    )?;
    let proof = EntryCostProof::source_valued_candidate(
        quote,
        sources.conversion,
        sources.observed_at_ms,
    )?
    .attest_linear_usdt_sources(
        plan,
        sources.order_type,
        sources.instrument,
        sources.rules,
        sources.fees,
        sources.account,
        sources.snapshot,
        sources.leverage,
    )?;
    Ok((proof, fee_rate))
}

/// E1.3: only a fully classified, independently sourced cost proof may
/// become a Worker RESERVE amount. The immutable SQL quote is the reference
/// for identity and exact arithmetic; a preview price or a hand-computed fee
/// must never be promoted into budget authority.
pub(super) fn reserve_entry_evidence(
    proof: &EntryCostProof,
    receipt: &okx_execution::EntryQuoteEvidence,
    account_cap: Decimal,
) -> Result<Option<(Decimal, String)>, String> {
    if account_cap <= Decimal::ZERO {
        return Err("ENTRY_BUDGET_INVALID".into());
    }
    let independently_recomputed = quote(
        receipt.quantity,
        receipt.contract_value,
        receipt.limit_price,
        receipt.fee_rate,
        receipt.usd_per_usdt,
    )?;
    if receipt.schema != "okx.entry-known-quote/v1"
        || receipt.account_key.is_empty()
        || receipt.installation_id.is_empty()
        || receipt.intent_id.is_empty()
        || receipt.plan_hash.len() != 64
        || !receipt.plan_hash.bytes().all(|b| b.is_ascii_hexdigit())
        || receipt.reference_generation.is_empty()
        || receipt.fee_generation.is_empty()
        || receipt.conversion_digest.len() != 64
        || !receipt
            .conversion_digest
            .bytes()
            .all(|b| b.is_ascii_hexdigit())
        || independently_recomputed != proof.quote
        || receipt.notional_usdt != independently_recomputed.notional_usdt
        || receipt.fee_usdt != independently_recomputed.fee_usdt
        || receipt.total_usdt != independently_recomputed.total_usdt
        || receipt.total_usd != independently_recomputed.total_usd
    {
        return Err("ENTRY_RESERVE_QUOTE_BINDING_MISMATCH".into());
    }
    proof.valuation.bind(receipt)?;
    let Some(upper) = proof.proven_upper_usd()? else {
        return Ok(None);
    };
    if upper <= Decimal::ZERO || upper < receipt.total_usd {
        return Err("ENTRY_RESERVE_BOUND_INVALID".into());
    }
    if upper > account_cap {
        return Err("ENTRY_RESERVE_ACCOUNT_CAP_EXCEEDED".into());
    }
    // The actual source evidence must be checked by its native owner BEFORE
    // creating Proven/NotApplicable variants. There is no such producer for
    // margin, borrowing, fee currency and frozen FX yet, so the production
    // path still yields None. Bind any future grant to the exact SQL quote,
    // its provenance and all classified dimensions in one canonical digest.
    let proof_digest = digest(&(
        "okx.entry-all-in-reserve/v1",
        &receipt.account_key,
        &receipt.installation_id,
        &receipt.intent_id,
        &receipt.plan_hash,
        receipt.policy_revision,
        &receipt.reference_generation,
        &receipt.fee_generation,
        &receipt.conversion_digest,
        &receipt.observed_at,
        &proof,
        exact(upper),
    ))?;
    Ok(Some((upper, proof_digest)))
}

/** Validate the native observer's instrument-specific fee generation and
 * receipt age. Both preview and execution must use this same source gate;
 * a correct maker/taker number from an old response is not current evidence.
 * This is only the known USDT quote fee, never the all-in debit authority.
 */
pub(super) fn verified_entry_fee_rate(
    fee: &okx_observation::FeeScheduleSnapshot,
    instrument: &str,
    reference_generation: &str,
    observed_at_ms: i64,
) -> Result<Decimal, String> {
    if fee.schema != okx_observation::FEE_SCHEDULE_SCHEMA_V1
        || fee.instrument_id != instrument
        || !fee.exact_for_instrument
        || fee.reference_generation != reference_generation
    {
        return Err("ENTRY_FEE_SOURCE_SCOPE_MISMATCH".into());
    }
    let source_time = chrono::DateTime::parse_from_rfc3339(&fee.source_received_at)
        .map_err(|_| "ENTRY_FEE_SOURCE_TIME_INVALID")?
        .timestamp_millis();
    if source_time <= 0
        || source_time > observed_at_ms
        || !crate::config::evidence_fresh(
            source_time,
            observed_at_ms,
            crate::config::ACCOUNT_MAX_AGE_MS,
        )
    {
        return Err("ENTRY_FEE_SOURCE_STALE".into());
    }
    let exchange_time = fee
        .exchange_timestamp_ms
        .parse::<i64>()
        .map_err(|_| "ENTRY_FEE_EXCHANGE_TIME_INVALID")?;
    if exchange_time <= 0 || exchange_time > observed_at_ms {
        return Err("ENTRY_FEE_EXCHANGE_TIME_INVALID".into());
    }
    if !crate::config::evidence_fresh(
        exchange_time,
        observed_at_ms,
        crate::config::ACCOUNT_MAX_AGE_MS,
    ) {
        return Err("ENTRY_FEE_EXCHANGE_TIME_STALE".into());
    }
    let canonical = serde_json::to_vec(&(
        &fee.instrument_id,
        &fee.reference_generation,
        &fee.level,
        &fee.maker_rate,
        &fee.taker_rate,
        fee.exact_for_instrument,
    ))
    .map_err(|_| "ENTRY_FEE_GENERATION_INVALID")?;
    let generation = format!("sha256:{:x}", Sha256::digest(canonical));
    if fee.fee_generation != generation {
        return Err("ENTRY_FEE_GENERATION_INVALID".into());
    }
    let maker = dec(&fee.maker_rate)?;
    let taker = dec(&fee.taker_rate)?;
    Ok(maker.abs().max(taker.abs()))
}

/// Shared source-bound known quote for preview and actual SQL planning.
/// Notional plus observed fee is NOT margin-account debit, and an equity
/// USD/USDT ratio is not a certified maximum FX conversion for a live order.
/// Consumers must retain EntryCostProof::Unknown until all additional
/// cost sources and the campaign opening balance are independently proven.
#[allow(clippy::too_many_arguments)] // One pure source gate; independent venue, fee, FX and identity facts.
pub(super) fn verified_known_entry_quote(
    plan: &IntentPlan,
    instrument: &str,
    rules: &okx_observation::InstrumentRulesSnapshot,
    fees: &okx_observation::FeeScheduleSnapshot,
    conversion: &UsdConversion,
    account_uid: &str,
    expected_uid: &str,
    observed_at_ms: i64,
) -> Result<(EntryQuote, Decimal), String> {
    plan::verify_entry_rules_match(rules, instrument, plan)?;
    let reference_time = chrono::DateTime::parse_from_rfc3339(&rules.source_received_at)
        .map_err(|_| "ENTRY_REFERENCE_SOURCE_TIME_INVALID")?
        .timestamp_millis();
    if reference_time <= 0
        || reference_time > observed_at_ms
        || !crate::config::evidence_fresh(
            reference_time,
            observed_at_ms,
            crate::config::ACCOUNT_MAX_AGE_MS,
        )
    {
        return Err("ENTRY_REFERENCE_SOURCE_STALE".into());
    }
    if account_uid != expected_uid {
        return Err("ENTRY_QUOTE_ACCOUNT_IDENTITY_MISMATCH".into());
    }
    let fee_rate = verified_entry_fee_rate(
        fees,
        instrument,
        &rules.reference_generation,
        observed_at_ms,
    )?;
    let fx_time =
        i64::try_from(conversion.observed_at_ms).map_err(|_| "ENTRY_FX_TIMESTAMP_INVALID")?;
    if fx_time <= 0
        || fx_time > observed_at_ms
        || !crate::config::evidence_fresh(
            fx_time,
            observed_at_ms,
            crate::config::ACCOUNT_MAX_AGE_MS,
        )
    {
        return Err("ENTRY_FX_SOURCE_STALE".into());
    }
    if conversion.source_digest.len() != 64
        || !conversion
            .source_digest
            .bytes()
            .all(|c| c.is_ascii_hexdigit())
    {
        return Err("ENTRY_FX_SOURCE_DIGEST_INVALID".into());
    }
    let fx = dec(&conversion.usd_per_usdt)?;
    if fx <= Decimal::ZERO {
        return Err("ENTRY_FX_SOURCE_INVALID".into());
    }
    Ok((
        quote(
            plan.quantity,
            plan.contract_value,
            plan.limit_price,
            fee_rate,
            fx,
        )?,
        fee_rate,
    ))
}

pub(super) fn quote(
    qty: Decimal,
    ct: Decimal,
    price: Decimal,
    fee_rate: Decimal,
    fx: Decimal,
) -> Result<EntryQuote, String> {
    if qty <= Decimal::ZERO
        || ct <= Decimal::ZERO
        || price <= Decimal::ZERO
        || fee_rate < Decimal::ZERO
        || fx <= Decimal::ZERO
    {
        return Err("ENTRY_SOURCE_INVALID".into());
    }
    let notional_usdt = qty
        .checked_mul(ct)
        .and_then(|v| v.checked_mul(price))
        .ok_or("MONEY_OVERFLOW")?;
    let fee_usdt = notional_usdt
        .checked_mul(fee_rate)
        .ok_or("MONEY_OVERFLOW")?
        .round_dp_with_strategy(6, RoundingStrategy::ToPositiveInfinity);
    let total_usdt = notional_usdt
        .checked_add(fee_usdt)
        .ok_or("MONEY_OVERFLOW")?;
    let total_usd = total_usdt
        .checked_mul(fx)
        .ok_or("MONEY_OVERFLOW")?
        .round_dp_with_strategy(9, RoundingStrategy::ToPositiveInfinity);
    Ok(EntryQuote {
        quantity: qty,
        notional_usdt,
        fee_usdt,
        total_usdt,
        total_usd,
        observed_usd_per_usdt: fx,
    })
}

/// The Real account policy owns the required ceiling. A command may only tighten it.
/// No cap is inferred from the independent campaign LOSS limit or USDT notional.
pub(super) fn enforced_entry_ceiling(
    increase: bool,
    real: bool,
    account_cap: Option<Decimal>,
    request_cap: Option<Decimal>,
) -> Result<Option<Decimal>, String> {
    if !increase {
        return Ok(None);
    }
    if real && account_cap.is_none() {
        return Err("ACCOUNT_ENTRY_CAP_UNCONFIGURED".into());
    }
    if account_cap.is_some_and(|cap| cap <= Decimal::ZERO)
        || request_cap.is_some_and(|cap| cap <= Decimal::ZERO)
    {
        return Err("ENTRY_BUDGET_INVALID".into());
    }
    Ok(match (account_cap, request_cap) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (Some(a), None) | (None, Some(a)) => Some(a),
        (None, None) => None,
    })
}
#[allow(clippy::too_many_arguments)] // One bounded, pure pricing calculation; no owner or IO.
pub(super) fn size_order(
    sizing: &Sizing,
    increase: bool,
    lot: Decimal,
    minimum: Decimal,
    ct: Decimal,
    price: Decimal,
    per_contract_loss: Decimal,
    fx: Decimal,
    fee_rate: Decimal,
    entry_ceiling_usd: Option<Decimal>,
) -> Result<EntryQuote, String> {
    if lot <= Decimal::ZERO
        || minimum <= Decimal::ZERO
        || ct <= Decimal::ZERO
        || price <= Decimal::ZERO
        || fx <= Decimal::ZERO
        || fee_rate < Decimal::ZERO
    {
        return Err("ENTRY_SOURCE_INVALID".into());
    }
    let quantity = match sizing {
        Sizing::Contracts { contracts } => dec(contracts)?,
        Sizing::MaxLossUsd { max_loss_usd } => {
            if !increase || per_contract_loss <= Decimal::ZERO {
                return Err("LOSS_SIZING_REQUIRES_PROTECTED_INCREASE".into());
            }
            dec(max_loss_usd)? / fx / per_contract_loss
        }
        Sizing::EntryBudgetUsd { entry_budget_usd } => {
            if !increase {
                return Err("ENTRY_BUDGET_REQUIRES_INCREASE".into());
            }
            let divisor = ct
                .checked_mul(price)
                .and_then(|v| v.checked_mul(Decimal::ONE + fee_rate))
                .and_then(|v| v.checked_mul(fx))
                .ok_or("MONEY_OVERFLOW")?;
            dec(entry_budget_usd)? / divisor
        }
    };
    let mut qty = if matches!(sizing, Sizing::Contracts { .. }) {
        if (quantity / lot).fract() != Decimal::ZERO {
            return Err("QUANTITY_NOT_ON_VENUE_LOT".into());
        }
        quantity
    } else {
        (quantity / lot).floor() * lot
    };
    if let Some(cap) = entry_ceiling_usd.filter(|_| increase) {
        if cap <= Decimal::ZERO {
            return Err("ENTRY_BUDGET_INVALID".into());
        }
        let divisor = ct
            .checked_mul(price)
            .and_then(|v| v.checked_mul(Decimal::ONE + fee_rate))
            .and_then(|v| v.checked_mul(fx))
            .ok_or("MONEY_OVERFLOW")?;
        let cap_qty = (cap / divisor / lot).floor() * lot;
        if matches!(sizing, Sizing::Contracts { .. }) && qty > cap_qty {
            return Err("ENTRY_BUDGET_EXCEEDED".into());
        }
        qty = qty.min(cap_qty);
    }
    if qty < minimum || qty <= Decimal::ZERO {
        return Err(
            if matches!(sizing, Sizing::EntryBudgetUsd { .. }) || entry_ceiling_usd.is_some() {
                "ENTRY_BUDGET_BELOW_VENUE_MINIMUM"
            } else {
                "MINIMUM_SIZE_EXCEEDS_BUDGET"
            }
            .into(),
        );
    }
    let mut value = quote(
        qty,
        ct,
        price,
        if increase { fee_rate } else { Decimal::ZERO },
        fx,
    )?;
    if increase {
        if let Some(cap) = entry_ceiling_usd {
            while value.total_usd > cap && qty >= minimum + lot {
                qty -= lot;
                value = quote(qty, ct, price, fee_rate, fx)?;
            }
            if value.total_usd > cap {
                return Err("ENTRY_BUDGET_BELOW_VENUE_MINIMUM".into());
            }
        }
        if let Sizing::EntryBudgetUsd { entry_budget_usd } = sizing {
            let cap = dec(entry_budget_usd)?;
            while value.total_usd > cap && qty >= minimum + lot {
                qty -= lot;
                value = quote(qty, ct, price, fee_rate, fx)?;
            }
            if value.total_usd > cap {
                return Err("ENTRY_BUDGET_BELOW_VENUE_MINIMUM".into());
            }
        }
    }
    Ok(value)
}

/** Remaining cap after verified price-bound notional and rounded entry fee.
 * This is headroom for UNKNOWN other costs, NOT proof those costs are zero.
 * In particular it never grants an exchange POST or a risk reservation.
 */
pub(super) fn unproven_entry_cost_headroom(
    known_entry_usd: Decimal,
    entry_ceiling_usd: Option<Decimal>,
) -> Result<Option<Decimal>, String> {
    let Some(cap) = entry_ceiling_usd else {
        return Ok(None);
    };
    if known_entry_usd <= Decimal::ZERO || cap <= Decimal::ZERO {
        return Err("ENTRY_SOURCE_INVALID".into());
    }
    let remaining = cap.checked_sub(known_entry_usd).ok_or("MONEY_OVERFLOW")?;
    if remaining < Decimal::ZERO {
        return Err("ENTRY_BUDGET_EXCEEDED".into());
    }
    Ok(Some(remaining))
}

#[cfg(test)]
mod tests {
    #[test]
    fn e1_fee_source_must_be_current_exact_and_content_bound() {
        let stamp = "2026-10-30T00:00:00Z";
        let now_ms = chrono::DateTime::parse_from_rfc3339(stamp)
            .unwrap()
            .timestamp_millis();
        let input = okx_observation::FeeScheduleInput {
            instrument_id: "BTC-USDT-SWAP".into(),
            reference_generation: "ref_generation".into(),
            source_received_at: stamp.into(),
            exchange_timestamp_ms: now_ms.to_string(),
            level: "Lv1".into(),
            maker_rate: "-0.0002".into(),
            taker_rate: "0.0005".into(),
            exact_for_instrument: true,
        };
        let fee = okx_observation::FeeScheduleSnapshot::from_input(input).unwrap();
        assert_eq!(
            verified_entry_fee_rate(&fee, "BTC-USDT-SWAP", "ref_generation", now_ms,),
            Ok(d("0.0005"))
        );
        assert_eq!(
            verified_entry_fee_rate(&fee, "ETH-USDT-SWAP", "ref_generation", now_ms,),
            Err("ENTRY_FEE_SOURCE_SCOPE_MISMATCH".into())
        );
        assert_eq!(
            verified_entry_fee_rate(&fee, "BTC-USDT-SWAP", "foreign_generation", now_ms,),
            Err("ENTRY_FEE_SOURCE_SCOPE_MISMATCH".into())
        );
        assert_eq!(
            verified_entry_fee_rate(
                &fee,
                "BTC-USDT-SWAP",
                "ref_generation",
                now_ms + crate::config::ACCOUNT_MAX_AGE_MS + 1,
            ),
            Err("ENTRY_FEE_SOURCE_STALE".into())
        );
        let mut changed = fee.clone();
        changed.taker_rate = "0.0001".into();
        assert_eq!(
            verified_entry_fee_rate(&changed, "BTC-USDT-SWAP", "ref_generation", now_ms,),
            Err("ENTRY_FEE_GENERATION_INVALID".into())
        );
        changed = fee.clone();
        changed.exchange_timestamp_ms = (now_ms + 1).to_string();
        assert_eq!(
            verified_entry_fee_rate(&changed, "BTC-USDT-SWAP", "ref_generation", now_ms,),
            Err("ENTRY_FEE_EXCHANGE_TIME_INVALID".into())
        );
        changed = fee.clone();
        changed.exchange_timestamp_ms =
            (now_ms - crate::config::ACCOUNT_MAX_AGE_MS - 1).to_string();
        assert_eq!(
            verified_entry_fee_rate(&changed, "BTC-USDT-SWAP", "ref_generation", now_ms,),
            Err("ENTRY_FEE_EXCHANGE_TIME_STALE".into())
        );
        changed = fee.clone();
        changed.source_received_at = "invalid".into();
        assert_eq!(
            verified_entry_fee_rate(&changed, "BTC-USDT-SWAP", "ref_generation", now_ms,),
            Err("ENTRY_FEE_SOURCE_TIME_INVALID".into())
        );
    }

    #[test]
    fn e1_known_quote_rejects_bad_fx_identity_and_stale_fee_equally() {
        let spec = okx_observation::InstrumentSpec {
            instrument_id: "BTC-USDT-SWAP".into(),
            instrument_type: okx_observation::InstrumentType::Swap,
            instrument_family: None,
            underlying: None,
            state: "live".into(),
            rule_type: None,
            funding_requirement: okx_observation::FundingRequirement::Unknown,
            base_currency: Some("BTC".into()),
            quote_currency: Some("USDT".into()),
            settle_currency: Some("USDT".into()),
            tick_size: "0.1".into(),
            lot_size: "0.01".into(),
            min_size: "0.01".into(),
            max_limit_size: Some("10".into()),
            max_market_size: None,
            max_limit_amount: None,
            max_market_amount: None,
            contract_type: Some("linear".into()),
            contract_value: Some("0.01".into()),
            contract_value_currency: Some("BTC".into()),
            fee_group_id: None,
            max_leverage: None,
            list_time_ms: None,
            expiry_time_ms: None,
            initial_price_limit_pct: None,
            floating_price_limit_pct: None,
            maximum_price_limit_pct: None,
            upcoming_rule_changes: Vec::new(),
        };
        let rules = okx_observation::InstrumentRulesSnapshot {
            reference_generation: "reference-1".into(),
            source_received_at: "2026-10-30T00:00:00Z".into(),
            instrument: spec,
        };
        let stamp = "2026-10-30T00:00:00Z";
        let t = chrono::DateTime::parse_from_rfc3339(stamp)
            .unwrap()
            .timestamp_millis();
        let fees =
            okx_observation::FeeScheduleSnapshot::from_input(okx_observation::FeeScheduleInput {
                instrument_id: "BTC-USDT-SWAP".into(),
                reference_generation: "reference-1".into(),
                source_received_at: stamp.into(),
                exchange_timestamp_ms: t.to_string(),
                level: "Lv1".into(),
                maker_rate: "-0.0002".into(),
                taker_rate: "0.0005".into(),
                exact_for_instrument: true,
            })
            .unwrap();
        let plan = IntentPlan {
            contract_value: dec("0.01").unwrap(),
            quantity: dec("0.02").unwrap(),
            limit_price: dec("50000.1").unwrap(),
            ..Default::default()
        };
        let fx = UsdConversion {
            usd_per_usdt: "1".into(),
            source_digest: "a".repeat(64),
            observed_at_ms: t as u64,
        };
        let verified = || {
            verified_known_entry_quote(&plan, "BTC-USDT-SWAP", &rules, &fees, &fx, "uid", "uid", t)
        };
        let (known, fee) = verified().unwrap();
        assert_eq!(fee, dec("0.0005").unwrap());
        assert_eq!(known.fee_usdt, dec("0.005001").unwrap());
        assert_eq!(known.total_usdt, dec("10.005021").unwrap());
        // Exercise the same production source classifier as live preview/execute.
        // No public MCP-provided Verified evidence or test-only RiskBook grant.
        let at = chrono::Utc::now();
        let mut live_fees = fees.clone();
        live_fees.source_received_at = at.to_rfc3339();
        live_fees.exchange_timestamp_ms = at.timestamp_millis().to_string();
        let config: okx_api::AccountConfig = serde_json::from_value(serde_json::json!({
            "acctLv":"2", "posMode":"long_short_mode", "uid":"real-fixture-uid",
            "type":"0", "autoLoan":false
        }))
        .unwrap();
        let snap: okx_observation::AccountSnapshot = serde_json::from_value(serde_json::json!({
            "schema":"okx.account-snapshot/v1",
            "source":"fixture-authenticated-account",
            "source_received_at":at.to_rfc3339(),
            "account_generation":"authenticated-generation",
            "quality_reason":"test-native-source",
            "private_ws_connected":true,
            "account_level":"2",
            "position_mode":"long_short_mode",
            "account_type":"0",
            "account_uid_fingerprint":okx_api::account_uid_fingerprint("real-fixture-uid"),
            "api_key_permissions":["trade"],
            "balance":{
                "total_equity_usd":"100",
                "details":[{
                    "currency":"USDT","equity":"100","equity_usd":"100",
                    "available_equity":"100","available_balance":"100",
                    "liabilities":"0"
                }]
            },
            "positions":[],"pending_orders":[]
        }))
        .unwrap();
        let mut open_plan = plan.clone();
        open_plan.side = "buy".into();
        open_plan.position_side = "long".into();
        let source_fx = UsdConversion {
            usd_per_usdt: "1".into(),
            source_digest: "a".repeat(64),
            observed_at_ms: t as u64,
        };
        let applicant =
            EntryCostProof::source_valued_candidate(known.clone(), &source_fx, t + 1).unwrap();
        let assessed = applicant
            .clone()
            .attest_linear_usdt_sources(
                &open_plan,
                "limit",
                "BTC-USDT-SWAP",
                &rules,
                &live_fees,
                &config,
                &snap,
                dec("2").unwrap(),
            )
            .unwrap();
        assert_eq!(assessed.proven_upper_usd().unwrap(), Some(known.total_usd));
        assert_eq!(assessed.missing_reasons(), Vec::<&'static str>::new());
        let mut auto = config.clone();
        auto.auto_loan = Some(true);
        assert_eq!(
            applicant
                .clone()
                .attest_linear_usdt_sources(
                    &open_plan,
                    "limit",
                    "BTC-USDT-SWAP",
                    &rules,
                    &live_fees,
                    &auto,
                    &snap,
                    dec("2").unwrap(),
                )
                .unwrap_err(),
            "ENTRY_AUTO_BORROW_UNVERIFIED_OR_ENABLED"
        );
        assert_eq!(
            applicant
                .clone()
                .attest_linear_usdt_sources(
                    &open_plan,
                    "market",
                    "BTC-USDT-SWAP",
                    &rules,
                    &live_fees,
                    &config,
                    &snap,
                    dec("2").unwrap(),
                )
                .unwrap_err(),
            "ENTRY_PRODUCT_OR_PRICE_CAP_UNSUPPORTED"
        );
        // Neither a missing field nor a net-mode account is silently
        // coerced to hedge mode or borrowing-disabled.
        auto.auto_loan = None;
        assert_eq!(
            applicant
                .clone()
                .attest_linear_usdt_sources(
                    &open_plan,
                    "limit",
                    "BTC-USDT-SWAP",
                    &rules,
                    &live_fees,
                    &auto,
                    &snap,
                    dec("2").unwrap(),
                )
                .unwrap_err(),
            "ENTRY_AUTO_BORROW_UNVERIFIED_OR_ENABLED"
        );
        let mut net = config.clone();
        net.position_mode = "net_mode".into();
        let mut net_snapshot = snap.clone();
        net_snapshot.position_mode = "net_mode".into();
        assert_eq!(
            applicant
                .clone()
                .attest_linear_usdt_sources(
                    &open_plan,
                    "limit",
                    "BTC-USDT-SWAP",
                    &rules,
                    &live_fees,
                    &net,
                    &net_snapshot,
                    dec("2").unwrap(),
                )
                .unwrap()
                .proven_upper_usd()
                .unwrap(),
            Some(known.total_usd)
        );
        let mut no_funds = snap.clone();
        no_funds.balance.details[0].available_balance = Some("1".into());
        assert_eq!(
            applicant
                .attest_linear_usdt_sources(
                    &open_plan,
                    "limit",
                    "BTC-USDT-SWAP",
                    &rules,
                    &live_fees,
                    &config,
                    &no_funds,
                    dec("2").unwrap(),
                )
                .unwrap()
                .proven_upper_usd()
                .unwrap_err(),
            "ENTRY_COLLATERAL_INSUFFICIENT"
        );
        assert_eq!(
            verified_known_entry_quote(
                &plan,
                "BTC-USDT-SWAP",
                &rules,
                &fees,
                &fx,
                "wrong",
                "uid",
                t,
            ),
            Err("ENTRY_QUOTE_ACCOUNT_IDENTITY_MISMATCH".into())
        );
        let stale = UsdConversion {
            observed_at_ms: (t - crate::config::ACCOUNT_MAX_AGE_MS - 1) as u64,
            ..fx.clone()
        };
        assert_eq!(
            verified_known_entry_quote(
                &plan,
                "BTC-USDT-SWAP",
                &rules,
                &fees,
                &stale,
                "uid",
                "uid",
                t
            ),
            Err("ENTRY_FX_SOURCE_STALE".into())
        );
        let mut stale_rules = rules.clone();
        stale_rules.source_received_at = "2026-10-29T23:00:00Z".into();
        assert_eq!(
            verified_known_entry_quote(
                &plan,
                "BTC-USDT-SWAP",
                &stale_rules,
                &fees,
                &fx,
                "uid",
                "uid",
                t
            ),
            Err("ENTRY_REFERENCE_SOURCE_STALE".into())
        );
        stale_rules.source_received_at = "not-a-date".into();
        assert_eq!(
            verified_known_entry_quote(
                &plan,
                "BTC-USDT-SWAP",
                &stale_rules,
                &fees,
                &fx,
                "uid",
                "uid",
                t
            ),
            Err("ENTRY_REFERENCE_SOURCE_TIME_INVALID".into())
        );
        let corrupted = UsdConversion {
            source_digest: "invalid".into(),
            ..fx.clone()
        };
        assert_eq!(
            verified_known_entry_quote(
                &plan,
                "BTC-USDT-SWAP",
                &rules,
                &fees,
                &corrupted,
                "uid",
                "uid",
                t
            ),
            Err("ENTRY_FX_SOURCE_DIGEST_INVALID".into())
        );
        let observed_late = t + crate::config::ACCOUNT_MAX_AGE_MS + 1;
        let mut current_rules = rules.clone();
        current_rules.source_received_at =
            chrono::DateTime::<chrono::Utc>::from_timestamp_millis(observed_late)
                .unwrap()
                .to_rfc3339();
        let current_fx = UsdConversion {
            observed_at_ms: observed_late as u64,
            ..fx.clone()
        };
        assert_eq!(
            verified_known_entry_quote(
                &plan,
                "BTC-USDT-SWAP",
                &current_rules,
                &fees,
                &current_fx,
                "uid",
                "uid",
                observed_late
            ),
            Err("ENTRY_FEE_SOURCE_STALE".into())
        );
    }

    #[test]
    fn e1_cost_proof_never_equates_margin_or_unknown_fees_with_zero() {
        let quote = quote(d("0.09"), d("1"), d("101"), d("0.0005"), d("1")).unwrap();
        assert_eq!(quote.total_usd, d("9.094545"));
        let mut p = EntryCostProof::unproven_linear_quote(quote);
        assert_eq!(p.proven_upper_usd().unwrap(), None);
        assert_eq!(p.missing_reasons().len(), 5);
        // Explicit venue non-applicability is evidence, not a guessed zero.
        p.extra_immediate_fees = EntryCostEvidence::NotApplicable {
            basis: "venue:no-other-immediate-fees",
            source_digest: "a".repeat(64),
        };
        p.borrowing = EntryCostEvidence::NotApplicable {
            basis: "venue:no-borrow",
            source_digest: "b".repeat(64),
        };
        p.collateral_or_margin = EntryCostEvidence::NotApplicable {
            basis: "conservative-notional-cover",
            source_digest: "c".repeat(64),
        };
        p.available_collateral = EntryAvailableCollateral::Proven {
            available_lower_usd: d("20"),
            basis: "authenticated-available-own-collateral",
            source_digest: "c".repeat(64),
        };
        // Fresh candidate source is not yet an admission.
        assert_eq!(p.proven_upper_usd().unwrap(), None);
        p.valuation = BudgetValuation::AdmissionSnapshot {
            usd_per_usdt: d("1"),
            observed_at_ms: 1,
            source_digest: "a".repeat(64),
            basis: "frozen-admission-source",
        };
        assert_eq!(p.proven_upper_usd().unwrap(), Some(d("9.094545")));
        p.borrowing = EntryCostEvidence::NotApplicable {
            basis: "venue:no-borrow",
            source_digest: "missing-source".into(),
        };
        assert_eq!(
            p.proven_upper_usd().unwrap_err(),
            "ENTRY_COST_SOURCE_UNVERIFIED"
        );
        p.borrowing = EntryCostEvidence::NotApplicable {
            basis: "venue:no-borrow",
            source_digest: "b".repeat(64),
        };
        p.collateral_or_margin = EntryCostEvidence::Proven {
            upper_usd: Decimal::ZERO,
            basis: "",
            source_digest: "c".repeat(64),
        };
        assert_eq!(
            p.proven_upper_usd().unwrap_err(),
            "ENTRY_COST_SOURCE_UNVERIFIED"
        );
        p.collateral_or_margin = EntryCostEvidence::NotApplicable {
            basis: "conservative-notional-cover",
            source_digest: "c".repeat(64),
        };
        p.extra_immediate_fees = EntryCostEvidence::Proven {
            upper_usd: d("0.1"),
            basis: "verified-third-currency-fee",
            source_digest: "d".repeat(64),
        };
        assert_eq!(p.proven_upper_usd().unwrap(), Some(d("9.194545")));
        // A positive margin requirement cannot be charged a second time.
        p.collateral_or_margin = EntryCostEvidence::Proven {
            upper_usd: d("4"),
            basis: "authenticated-margin-upper",
            source_digest: "e".repeat(64),
        };
        assert_eq!(p.proven_upper_usd().unwrap(), Some(d("9.194545")));
        p.available_collateral = EntryAvailableCollateral::Proven {
            available_lower_usd: d("3.99"),
            basis: "authenticated-available-own-collateral",
            source_digest: "f".repeat(64),
        };
        assert_eq!(
            p.proven_upper_usd().unwrap_err(),
            "ENTRY_COLLATERAL_INSUFFICIENT"
        );
        p.available_collateral = EntryAvailableCollateral::Proven {
            available_lower_usd: d("20"),
            basis: "authenticated-available-own-collateral",
            source_digest: "c".repeat(64),
        };
        p.borrowing = EntryCostEvidence::Proven {
            upper_usd: d("0.01"),
            basis: "authenticated-borrow-charge",
            source_digest: "b".repeat(64),
        };
        assert_eq!(
            p.proven_upper_usd().unwrap_err(),
            "ENTRY_BORROWING_UNSUPPORTED"
        );
        p.borrowing = EntryCostEvidence::NotApplicable {
            basis: "venue:no-borrow",
            source_digest: "b".repeat(64),
        };
        p.extra_immediate_fees = EntryCostEvidence::Proven {
            upper_usd: d("-0.01"),
            basis: "invalid-rebate",
            source_digest: "e".repeat(64),
        };
        assert_eq!(p.proven_upper_usd().unwrap_err(), "ENTRY_COST_NEGATIVE");
        p.extra_immediate_fees = EntryCostEvidence::NotApplicable {
            basis: "verified-no-additional-fee",
            source_digest: "f".repeat(64),
        };
        p.valuation = BudgetValuation::AdmissionSnapshot {
            usd_per_usdt: d("0.99"),
            observed_at_ms: 1,
            source_digest: "a".repeat(64),
            basis: "different-rate",
        };
        assert_eq!(
            p.proven_upper_usd().unwrap_err(),
            "ENTRY_VALUATION_RATE_MISMATCH"
        );
        p.valuation = BudgetValuation::AdmissionSnapshot {
            usd_per_usdt: d("1"),
            observed_at_ms: 1,
            source_digest: "bad".into(),
            basis: "invalid-source",
        };
        assert_eq!(
            p.proven_upper_usd().unwrap_err(),
            "ENTRY_VALUATION_SOURCE_INVALID"
        );
        p.valuation = BudgetValuation::Pending {
            reason: "ADMISSION_FX_SNAPSHOT_UNPROVEN",
        };
        assert_eq!(p.proven_upper_usd().unwrap(), None);
    }

    #[test]
    fn r4_frozen_admission_fx_valuates_turnover_not_margin_twice() {
        let quote = quote(d("8"), Decimal::ONE, Decimal::ONE, d("0.0005"), d("1.01")).unwrap();
        assert_eq!(quote.notional_usdt, d("8"));
        assert_eq!(quote.fee_usdt, d("0.004"));
        assert_eq!(quote.total_usdt, d("8.004"));
        assert_eq!(quote.total_usd, d("8.08404"));
        let mut proof = EntryCostProof::unproven_linear_quote(quote);
        proof.extra_immediate_fees = EntryCostEvidence::NotApplicable {
            basis: "no-extra-fee",
            source_digest: "a".repeat(64),
        };
        proof.borrowing = EntryCostEvidence::NotApplicable {
            basis: "no-borrow",
            source_digest: "b".repeat(64),
        };
        proof.collateral_or_margin = EntryCostEvidence::Proven {
            upper_usd: d("4.04"),
            basis: "margin-requirement",
            source_digest: "c".repeat(64),
        };
        proof.available_collateral = EntryAvailableCollateral::Proven {
            available_lower_usd: d("20"),
            basis: "own-collateral",
            source_digest: "d".repeat(64),
        };
        proof.valuation = BudgetValuation::AdmissionSnapshot {
            usd_per_usdt: d("1.01"),
            observed_at_ms: 1_000,
            source_digest: "e".repeat(64),
            basis: "frozen-admission",
        };
        assert_eq!(proof.proven_upper_usd().unwrap(), Some(d("8.08404")));
        // Later rates belong to current FX PnL, not historical budget charge.
        assert_eq!(proof.proven_upper_usd().unwrap(), Some(d("8.08404")));
        assert_ne!(
            proof.quote.total_usdt * d("0.97"),
            proof.proven_upper_usd().unwrap().unwrap()
        );
        assert_ne!(
            proof.quote.total_usdt * d("1.05"),
            proof.proven_upper_usd().unwrap().unwrap()
        );
    }

    #[test]
    fn r4_candidate_valuation_is_fresh_and_still_no_post_permission() {
        let cost = quote(d("8"), Decimal::ONE, Decimal::ONE, d("0.0005"), d("1.01")).unwrap();
        let fx = UsdConversion {
            usd_per_usdt: "1.01".into(),
            source_digest: "a".repeat(64),
            observed_at_ms: 1_000,
        };
        let candidate = EntryCostProof::source_valued_candidate(cost.clone(), &fx, 1_100).unwrap();
        assert_eq!(candidate.proven_upper_usd().unwrap(), None);
        assert_eq!(candidate.missing_reasons().len(), 4);
        assert_eq!(
            EntryCostProof::source_valued_candidate(cost.clone(), &fx, 6_001).unwrap_err(),
            "ENTRY_VALUATION_SOURCE_STALE"
        );
        let different = UsdConversion {
            usd_per_usdt: "1.02".into(),
            ..fx
        };
        assert_eq!(
            EntryCostProof::source_valued_candidate(cost, &different, 1_100).unwrap_err(),
            "ENTRY_VALUATION_RATE_MISMATCH"
        );
    }

    #[test]
    fn unknown_entry_cost_headroom_is_not_a_cost_proof() {
        let d = |s: &str| Decimal::from_str(s).unwrap();
        assert_eq!(
            unproven_entry_cost_headroom(d("8.2"), Some(d("10"))).unwrap(),
            Some(d("1.8"))
        );
        assert_eq!(unproven_entry_cost_headroom(d("8.2"), None).unwrap(), None);
        assert_eq!(
            unproven_entry_cost_headroom(d("10.01"), Some(d("10"))).unwrap_err(),
            "ENTRY_BUDGET_EXCEEDED"
        );
        assert_eq!(
            unproven_entry_cost_headroom(Decimal::ZERO, Some(d("10"))).unwrap_err(),
            "ENTRY_SOURCE_INVALID"
        );
    }

    use super::*;
    fn d(s: &str) -> Decimal {
        dec(s).unwrap()
    }
    #[test]
    fn policy_ceiling_is_mandatory_for_real_and_always_stricter() {
        assert_eq!(
            enforced_entry_ceiling(true, true, None, Some(d("10"))).unwrap_err(),
            "ACCOUNT_ENTRY_CAP_UNCONFIGURED"
        );
        assert_eq!(
            enforced_entry_ceiling(true, true, Some(d("10")), None).unwrap(),
            Some(d("10"))
        );
        assert_eq!(
            enforced_entry_ceiling(true, true, Some(d("10")), Some(d("8"))).unwrap(),
            Some(d("8"))
        );
        assert_eq!(
            enforced_entry_ceiling(true, true, Some(d("8")), Some(d("10"))).unwrap(),
            Some(d("8"))
        );
        assert_eq!(
            enforced_entry_ceiling(false, true, None, None).unwrap(),
            None
        );
        assert_eq!(
            enforced_entry_ceiling(true, false, None, Some(d("10"))).unwrap(),
            Some(d("10"))
        );
    }
    #[test]
    fn k6_v01_fee_inclusive_max_entry_and_minimum() {
        let size = Sizing::EntryBudgetUsd {
            entry_budget_usd: "10".into(),
        };
        let v = size_order(
            &size,
            true,
            d("0.01"),
            d("0.01"),
            d("1"),
            d("101"),
            d("1"),
            d("1"),
            d("0.0005"),
            Some(d("10")),
        )
        .unwrap();
        assert_eq!(v.quantity, d("0.09"));
        assert_eq!(v.notional_usdt, d("9.09"));
        assert_eq!(v.fee_usdt, d("0.004545"));
        assert_eq!(v.total_usd, d("9.094545"));
        assert_eq!(
            quote(d("0.10"), d("1"), d("101"), d("0.0005"), d("1"))
                .unwrap()
                .total_usd,
            d("10.10505")
        );
        assert_eq!(
            size_order(
                &size,
                true,
                d("0.01"),
                d("0.10"),
                d("1"),
                d("101"),
                d("1"),
                d("1"),
                d("0.0005"),
                Some(d("10"))
            )
            .unwrap_err(),
            "ENTRY_BUDGET_BELOW_VENUE_MINIMUM"
        );
    }
    #[test]
    fn cap_is_independent_of_modeled_loss_and_exact_qty_rejects_resize() {
        let loss = Sizing::MaxLossUsd {
            max_loss_usd: "1".into(),
        };
        let capped = size_order(
            &loss,
            true,
            d("0.01"),
            d("0.01"),
            d("0.01"),
            d("80000"),
            d("10"),
            d("1"),
            d("0.0005"),
            Some(d("10")),
        )
        .unwrap();
        assert_eq!(capped.quantity, d("0.01"));
        assert!(capped.total_usd <= d("10"));
        assert!(
            size_order(
                &Sizing::Contracts {
                    contracts: "0.02".into()
                },
                true,
                d("0.01"),
                d("0.01"),
                d("0.01"),
                d("80000"),
                d("10"),
                d("1"),
                d("0.0005"),
                Some(d("10"))
            )
            .is_err()
        );
        assert_eq!(
            size_order(
                &Sizing::Contracts {
                    contracts: "0.011".into()
                },
                true,
                d("0.01"),
                d("0.01"),
                d("0.01"),
                d("80000"),
                d("10"),
                d("1"),
                d("0.0005"),
                None
            )
            .unwrap_err(),
            "QUANTITY_NOT_ON_VENUE_LOT"
        );
    }
}
