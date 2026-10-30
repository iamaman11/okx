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

/// A conservative bound for NOTIONAL+known USDT entry fee is not the actual
/// margin-account debit. All-in entry permission requires separately proved
/// venue fee currency, borrow/collateral applicability, and bounded FX.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct EntryCostProof {
    pub(super) quote: EntryQuote,
    pub(super) extra_immediate_fees: EntryCostEvidence,
    pub(super) borrowing: EntryCostEvidence,
    pub(super) collateral_or_margin: EntryCostEvidence,
    pub(super) fx_bound: EntryCostEvidence,
}
impl EntryCostProof {
    /// Preview/size-order produces an *incomplete* proof until the venue and
    /// central authority provide independent cost/applicability evidence.
    pub(super) fn unproven_linear_quote(quote: EntryQuote) -> Self {
        Self {
            quote,
            extra_immediate_fees: EntryCostEvidence::Unknown {
                reason: "ADDITIONAL_ENTRY_FEES_UNPROVEN",
            },
            borrowing: EntryCostEvidence::Unknown {
                reason: "BORROWING_APPLICABILITY_UNPROVEN",
            },
            collateral_or_margin: EntryCostEvidence::Unknown {
                reason: "MARGIN_ENTRY_DEBIT_UNPROVEN",
            },
            fx_bound: EntryCostEvidence::Unknown {
                reason: "FX_UPPER_BOUND_UNPROVEN",
            },
        }
    }

    /// No Unknown cost may become an authorization. The quote.total_usd is
    /// treated as a conservative notional+fee bound, not as exact margin debit.
    pub(super) fn proven_upper_usd(&self) -> Result<Option<Decimal>, String> {
        let mut additional = Decimal::ZERO;
        for cost in [
            &self.extra_immediate_fees,
            &self.borrowing,
            &self.collateral_or_margin,
            &self.fx_bound,
        ] {
            let Some(upper) = cost.upper()? else {
                return Ok(None);
            };
            additional = additional.checked_add(upper).ok_or("MONEY_OVERFLOW")?;
        }
        Ok(Some(
            self.quote
                .total_usd
                .checked_add(additional)
                .ok_or("MONEY_OVERFLOW")?,
        ))
    }

    pub(super) fn missing_reasons(&self) -> Vec<&'static str> {
        [
            &self.extra_immediate_fees,
            &self.borrowing,
            &self.collateral_or_margin,
            &self.fx_bound,
        ]
        .into_iter()
        .filter_map(EntryCostEvidence::reason)
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
    let exchange_time = fee.exchange_timestamp_ms.parse::<i64>()
        .map_err(|_| "ENTRY_FEE_EXCHANGE_TIME_INVALID")?;
    if exchange_time <= 0 || exchange_time > observed_at_ms {
        return Err("ENTRY_FEE_EXCHANGE_TIME_INVALID".into());
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
            .unwrap().timestamp_millis();
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
            verified_entry_fee_rate(
                &fee, "BTC-USDT-SWAP", "ref_generation", now_ms,
            ),
            Ok(d("0.0005"))
        );
        assert_eq!(
            verified_entry_fee_rate(
                &fee, "ETH-USDT-SWAP", "ref_generation", now_ms,
            ),
            Err("ENTRY_FEE_SOURCE_SCOPE_MISMATCH".into())
        );
        assert_eq!(
            verified_entry_fee_rate(
                &fee, "BTC-USDT-SWAP", "foreign_generation", now_ms,
            ),
            Err("ENTRY_FEE_SOURCE_SCOPE_MISMATCH".into())
        );
        assert_eq!(
            verified_entry_fee_rate(
                &fee, "BTC-USDT-SWAP", "ref_generation",
                now_ms + crate::config::ACCOUNT_MAX_AGE_MS + 1,
            ),
            Err("ENTRY_FEE_SOURCE_STALE".into())
        );
        let mut changed = fee.clone();
        changed.taker_rate = "0.0001".into();
        assert_eq!(
            verified_entry_fee_rate(
                &changed, "BTC-USDT-SWAP", "ref_generation", now_ms,
            ),
            Err("ENTRY_FEE_GENERATION_INVALID".into())
        );
        changed = fee.clone();
        changed.exchange_timestamp_ms = (now_ms + 1).to_string();
        assert_eq!(
            verified_entry_fee_rate(
                &changed, "BTC-USDT-SWAP", "ref_generation", now_ms,
            ),
            Err("ENTRY_FEE_EXCHANGE_TIME_INVALID".into())
        );
        changed = fee.clone();
        changed.source_received_at = "invalid".into();
        assert_eq!(
            verified_entry_fee_rate(
                &changed, "BTC-USDT-SWAP", "ref_generation", now_ms,
            ),
            Err("ENTRY_FEE_SOURCE_TIME_INVALID".into())
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
        p.fx_bound = EntryCostEvidence::Proven {
            upper_usd: Decimal::ZERO,
            basis: "bounded-fx-evidence",
        };
        assert_eq!(p.proven_upper_usd().unwrap(), Some(d("9.094545")));
        p.extra_immediate_fees = EntryCostEvidence::Proven {
            upper_usd: d("-0.01"),
            basis: "invalid-rebate",
        };
        assert_eq!(p.proven_upper_usd().unwrap_err(), "ENTRY_COST_NEGATIVE");
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
