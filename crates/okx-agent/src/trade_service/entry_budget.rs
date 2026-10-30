//! Exact fee-inclusive linear entry sizing. Pure arithmetic shared by manual, strategy and preview.
use super::*;
use rust_decimal::RoundingStrategy;

#[derive(Debug, Clone, PartialEq)]
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
#[derive(Debug, Clone, PartialEq)]
pub(super) enum EntryCostEvidence {
    Proven {
        upper_usd: Decimal,
        basis: &'static str,
    },
    NotApplicable {
        basis: &'static str,
    },
    Unknown {
        reason: &'static str,
    },
}
impl EntryCostEvidence {
    fn upper(&self) -> Result<Option<Decimal>, String> {
        match self {
            Self::Proven { upper_usd, .. } if *upper_usd >= Decimal::ZERO => Ok(Some(*upper_usd)),
            Self::NotApplicable { .. } => Ok(Some(Decimal::ZERO)),
            Self::Unknown { .. } => Ok(None),
            Self::Proven { .. } => Err("ENTRY_COST_NEGATIVE".into()),
        }
    }
    fn reason(&self) -> Option<&'static str> {
        match self {
            Self::Unknown { reason } => Some(*reason),
            _ => None,
        }
    }
}

/// USD/USDT conversion is a *rate*, not an additional charge in USD.
/// No NotApplicable state: even a USD-pegged quote requires a sourced bound.
#[allow(dead_code)] // Verified proof constructors are gated until native venue source binding.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum EntryFxUpperBound {
    Proven {
        usd_per_usdt_upper: Decimal,
        source_digest: String,
        basis: &'static str,
    },
    Unknown {
        reason: &'static str,
    },
}
impl EntryFxUpperBound {
    fn rate(&self, observed: Decimal) -> Result<Option<Decimal>, String> {
        match self {
            Self::Unknown { .. } => Ok(None),
            Self::Proven {
                usd_per_usdt_upper,
                source_digest,
                ..
            } => {
                if *usd_per_usdt_upper <= Decimal::ZERO || observed <= Decimal::ZERO {
                    return Err("ENTRY_FX_UPPER_BOUND_INVALID".into());
                }
                if *usd_per_usdt_upper < observed {
                    return Err("ENTRY_FX_BOUND_BELOW_OBSERVED".into());
                }
                if source_digest.len() != 64
                    || !source_digest.bytes().all(|b| b.is_ascii_hexdigit())
                {
                    return Err("ENTRY_FX_BOUND_SOURCE_INVALID".into());
                }
                Ok(Some(*usd_per_usdt_upper))
            }
        }
    }
    fn reason(&self) -> Option<&'static str> {
        match self {
            Self::Unknown { reason } => Some(*reason),
            _ => None,
        }
    }
}

/// Known quote total_usd uses an observed FX conversion; it is never an
/// admission upper bound. All fees/collateral/borrowing must be classified
/// separately by source. A proven FX maximum multiplies the FULL USDT
/// obligation before separately denominated USD charges are added.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct EntryCostProof {
    pub(super) quote: EntryQuote,
    pub(super) extra_immediate_fees: EntryCostEvidence,
    pub(super) borrowing: EntryCostEvidence,
    pub(super) collateral_or_margin: EntryCostEvidence,
    pub(super) fx_bound: EntryFxUpperBound,
}
impl EntryCostProof {
    /// A missing venue cost or FX maximum remains Unknown, including on a
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
                reason: "MARGIN_ENTRY_DEBIT_UNPROVEN",
            },
            fx_bound: EntryFxUpperBound::Unknown {
                reason: "FX_UPPER_BOUND_UNPROVEN",
            },
        }
    }

    /// The upper rate must be independently source-bound; observed FX is not
    /// permission to POST. Round the entire USDT obligation upward to 1e-9 USD
    /// before adding further proven USD-denominated costs. An Unknown in any
    /// mandatory dimension means not proven, not zero.
    pub(super) fn proven_upper_usd(&self) -> Result<Option<Decimal>, String> {
        let Some(fx_upper) = self.fx_bound.rate(self.quote.observed_usd_per_usdt)? else {
            return Ok(None);
        };
        if self.quote.total_usdt <= Decimal::ZERO {
            return Err("ENTRY_SOURCE_INVALID".into());
        }
        let mut upper = self
            .quote
            .total_usdt
            .checked_mul(fx_upper)
            .ok_or("MONEY_OVERFLOW")?
            .round_dp_with_strategy(9, RoundingStrategy::ToPositiveInfinity);
        if upper < self.quote.total_usd {
            return Err("ENTRY_FX_BOUND_BELOW_KNOWN_QUOTE".into());
        }
        for cost in [
            &self.extra_immediate_fees,
            &self.borrowing,
            &self.collateral_or_margin,
        ] {
            let Some(additional) = cost.upper()? else {
                return Ok(None);
            };
            upper = upper.checked_add(additional).ok_or("MONEY_OVERFLOW")?;
        }
        Ok(Some(upper))
    }

    pub(super) fn missing_reasons(&self) -> Vec<&'static str> {
        [
            &self.extra_immediate_fees,
            &self.borrowing,
            &self.collateral_or_margin,
        ]
        .into_iter()
        .filter_map(EntryCostEvidence::reason)
        .chain(self.fx_bound.reason())
        .collect()
    }
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
        assert_eq!(p.missing_reasons().len(), 4);
        // Explicit venue non-applicability is evidence, not a guessed zero.
        p.extra_immediate_fees = EntryCostEvidence::NotApplicable {
            basis: "venue:no-other-immediate-fees",
        };
        p.borrowing = EntryCostEvidence::NotApplicable {
            basis: "venue:no-borrow",
        };
        p.collateral_or_margin = EntryCostEvidence::NotApplicable {
            basis: "conservative-notional-cover",
        };
        // An observed rate is not necessarily a bounded FX rate.
        assert_eq!(p.proven_upper_usd().unwrap(), None);
        p.fx_bound = EntryFxUpperBound::Proven {
            usd_per_usdt_upper: d("1.02"),
            source_digest: "a".repeat(64),
            basis: "source-bound-maximum",
        };
        assert_eq!(p.proven_upper_usd().unwrap(), Some(d("9.2764359")));
        p.extra_immediate_fees = EntryCostEvidence::Proven {
            upper_usd: d("0.1"),
            basis: "verified-third-currency-fee",
        };
        assert_eq!(p.proven_upper_usd().unwrap(), Some(d("9.3764359")));
        p.extra_immediate_fees = EntryCostEvidence::Proven {
            upper_usd: d("-0.01"),
            basis: "invalid-rebate",
        };
        assert_eq!(p.proven_upper_usd().unwrap_err(), "ENTRY_COST_NEGATIVE");
        p.extra_immediate_fees = EntryCostEvidence::NotApplicable {
            basis: "verified-no-additional-fee",
        };
        p.fx_bound = EntryFxUpperBound::Proven {
            usd_per_usdt_upper: d("0.99"),
            source_digest: "a".repeat(64),
            basis: "invalid-lower-than-observed",
        };
        assert_eq!(
            p.proven_upper_usd().unwrap_err(),
            "ENTRY_FX_BOUND_BELOW_OBSERVED"
        );
        p.fx_bound = EntryFxUpperBound::Proven {
            usd_per_usdt_upper: d("1"),
            source_digest: "bad".into(),
            basis: "invalid-source",
        };
        assert_eq!(
            p.proven_upper_usd().unwrap_err(),
            "ENTRY_FX_BOUND_SOURCE_INVALID"
        );
        p.fx_bound = EntryFxUpperBound::Unknown {
            reason: "FX_UPPER_BOUND_UNPROVEN",
        };
        assert_eq!(p.proven_upper_usd().unwrap(), None);
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
