//! Source-verified strategy valuation; shared production math, no independent owner.
use super::super::*;
impl AccountStore {
    pub(crate) fn reconcile_strategy_valuation_sql(
        db: &Connection,
        scope: Option<(&str, &str)>,
        evidence: &StrategyValuationEvidence,
    ) -> StoreResult<StrategyValuationReconciliation> {
        use rusqlite::{params_from_iter, types::Value};
        let metadata = Self::source_capture_state_sql(db, scope)?;
        let values = |mut original: Vec<Value>| {
            if let Some((installation, account)) = scope {
                original.push(installation.to_owned().into());
                original.push(account.to_owned().into());
            }
            original
        };
        if evidence.account_key != metadata.account_key
            || evidence.account_nav_source.is_empty()
            || evidence.conversion.source_digest.is_empty()
            || evidence.as_of != evidence.conversion.observed_at
            || evidence.venue_positions.len() > 256
        {
            return Err(StoreError::Invalid("STRATEGY_VALUATION_EVIDENCE_INVALID"));
        }
        let state = Self::capital_state_sql(
            db,
            scope,
            evidence.as_of,
            Self::latest_capital_assignment_time_sql(db, scope)?,
        )?;
        if state.coverage != "COMPLETE"
            || state.nav_source_as_of != evidence.as_of
            || state.nav_source_ref != evidence.account_nav_source
        {
            return Err(StoreError::Invalid("STRATEGY_NAV_ACCOUNT_SOURCE_MISMATCH"));
        }
        for (source, part) in [
            ("fills", "SPOT"),
            ("fills", "SWAP"),
            ("fills", "FUTURES"),
            ("fills", "OPTION"),
            ("bills", "account"),
            ("account_snapshot", "account"),
        ] {
            let found:Option<(i64,String,String)>=db.query_row(
                match scope {None=>"SELECT watermark,cursor,coverage_gaps FROM source_cursors WHERE source=?1 AND stream_partition=?2",Some(_)=>"SELECT watermark,cursor,coverage_gaps FROM source_cursors WHERE source=?1 AND stream_partition=?2 AND installation_id=?3 AND account_key=?4"},
                params_from_iter(values(vec![source.to_owned().into(),part.to_owned().into()])),|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))
            ).optional()?;
            if !found.is_some_and(|(t, c, g)| t >= evidence.as_of && c.is_empty() && g.is_empty()) {
                return Err(StoreError::Invalid("STRATEGY_NAV_HISTORY_INCOMPLETE"));
            }
        }
        let pending: bool = db.query_row(
            match scope {None=>"SELECT EXISTS(SELECT 1 FROM fill_projection_pending LIMIT 1) OR EXISTS(SELECT 1 FROM funding_projection_pending LIMIT 1)",Some(_)=>"SELECT EXISTS(SELECT 1 FROM fill_projection_pending WHERE installation_id=?1 AND account_key=?2 LIMIT 1) OR EXISTS(SELECT 1 FROM funding_projection_pending WHERE installation_id=?1 AND account_key=?2 LIMIT 1)"},
            params_from_iter(values(vec![])), |r| r.get(0)
        )?;
        if pending {
            return Err(StoreError::Invalid("STRATEGY_NAV_PROJECTION_PENDING"));
        }
        // F1: check an atomic, monotonic provenance proof, not all finance rows.
        // An old/foreign/unverified entry must never inherit a COMPLETE NAV.
        if !Self::finance_proof_sealed_sql(db, scope)? {
            return Err(StoreError::Invalid(
                "STRATEGY_NAV_ECONOMIC_SOURCE_UNVERIFIED",
            ));
        }
        let fx = &evidence.conversion.usd_per_currency;
        let convert = |currency: &str, value: Decimal| -> StoreResult<Decimal> {
            let rate = if currency == "USD" {
                Decimal::ONE
            } else {
                *fx.get(currency)
                    .ok_or(StoreError::Invalid("STRATEGY_NAV_FX_MISSING"))?
            };
            if rate <= Decimal::ZERO {
                return Err(StoreError::Invalid("STRATEGY_NAV_FX_INVALID"));
            }
            value
                .checked_mul(rate)
                .ok_or(StoreError::Invalid("MONEY_OVERFLOW"))
        };
        let mut result = state
            .strategy_principal_usd
            .into_iter()
            .map(|(id, capital)| {
                (
                    id.clone(),
                    StrategyValuation {
                        instance_id: id,
                        capital_usd: capital,
                        realized_usd: Decimal::ZERO,
                        unrealized_usd: Decimal::ZERO,
                        equity_usd: capital,
                    },
                )
            })
            .collect::<std::collections::BTreeMap<_, _>>();
        // Only economic components count. USD risk mirrors, deposits and
        // internal assignments are NEVER extra strategy profit.
        // Current NAV consumes the existing event-updated projection.
        // True historical as-of queries retain their immutable ledger fold.
        let latest_time: Option<i64> =
            db
                .query_row(match scope {None=>"SELECT MAX(time) FROM finance_entries",Some(_)=>"SELECT MAX(time) FROM finance_entries WHERE installation_id=?1 AND account_key=?2"}, params_from_iter(values(vec![])), |r| r.get(0))?;
        let attribution = if latest_time.is_some_and(|t| t > evidence.as_of) {
            Self::finance_attribution_filtered_sql(
                db,
                scope,
                metadata.initialized_at,
                evidence.as_of,
                &[],
                &[],
            )?
        } else {
            Self::current_strategy_finance_totals_sql(db, scope)?
        };
        for ((owner, currency, component), amount) in attribution {
            let Some(owner) = owner else { continue };
            if owner == "manual" {
                continue;
            }
            if component == "INTERNAL_CAPITAL_USD" {
                continue;
            }
            if !matches!(
                component.as_str(),
                "FEE" | "FUNDING" | "REALIZED_PNL" | "LIQUIDATION_COST" | "CAMPAIGN_LOSS_USD"
            ) {
                return Err(StoreError::Invalid("STRATEGY_NAV_COMPONENT_UNKNOWN"));
            }
            if component == "CAMPAIGN_LOSS_USD" {
                continue;
            }
            let row = result
                .get_mut(&owner)
                .ok_or(StoreError::Invalid("STRATEGY_NAV_CAPITAL_NOT_ASSIGNED"))?;
            row.realized_usd = row
                .realized_usd
                .checked_add(convert(&currency, amount)?)
                .ok_or(StoreError::Invalid("MONEY_OVERFLOW"))?;
        }
        let mut marks = std::collections::BTreeMap::new();
        for p in &evidence.venue_positions {
            if p.instrument_id.is_empty()
                || p.signed_contracts == Decimal::ZERO
                || p.mark_price <= Decimal::ZERO
                || marks.insert(p.instrument_id.clone(), p).is_some()
            {
                return Err(StoreError::Invalid("STRATEGY_NAV_POSITION_WITNESS_INVALID"));
            }
        }
        // One account may contain unattributed or manual positions. They
        // are included in net exposure reconciliation, not a second NAV.
        let mut venue_net = std::collections::BTreeMap::<String, (Decimal, Decimal)>::new();
        let mut lots=db.prepare(
            match scope {None=>"SELECT instance_id,intent_id,instrument_id,side,remaining_qty,cost FROM position_lots WHERE remaining_qty<>'0' ORDER BY instrument_id,lot_id LIMIT 258",Some(_)=>"SELECT instance_id,intent_id,instrument_id,side,remaining_qty,cost FROM position_lots WHERE installation_id=?1 AND account_key=?2 AND remaining_qty<>'0' ORDER BY instrument_id,lot_id LIMIT 258"}
        )?;
        let mut rows = lots.query(params_from_iter(values(vec![])))?;
        let mut count = 0_usize;
        while let Some(r) = rows.next()? {
            count += 1;
            if count > 256 {
                return Err(StoreError::Invalid("STRATEGY_NAV_POSITION_LIMIT"));
            }
            let owner: Option<String> = r.get(0)?;
            let intent: Option<String> = r.get(1)?;
            let instrument: String = r.get(2)?;
            let side: String = r.get(3)?;
            let qty = decimal(r.get(4)?)?;
            let entry = decimal(r.get(5)?)?;
            if qty <= Decimal::ZERO
                || entry <= Decimal::ZERO
                || !matches!(side.as_str(), "long" | "short")
            {
                return Err(StoreError::Invalid("STRATEGY_NAV_LOT_INVALID"));
            }
            let signed = if side == "long" { qty } else { -qty };
            let old = venue_net
                .get(&instrument)
                .copied()
                .unwrap_or((Decimal::ZERO, Decimal::ZERO));
            venue_net.insert(
                instrument.clone(),
                (
                    old.0
                        .checked_add(signed)
                        .ok_or(StoreError::Invalid("MONEY_OVERFLOW"))?,
                    old.1
                        .checked_add(qty)
                        .ok_or(StoreError::Invalid("MONEY_OVERFLOW"))?,
                ),
            );
            let Some(owner) = owner else { continue };
            if owner == "manual" {
                continue;
            }
            let mark = marks
                .get(&instrument)
                .ok_or(StoreError::Invalid("STRATEGY_NAV_MARK_MISSING"))?;
            let intent =
                intent.ok_or(StoreError::Invalid("STRATEGY_NAV_LOT_PROVENANCE_MISSING"))?;
            let plan = Self::plan_sql(db, scope, &intent)?;
            let direction = if side == "long" {
                Decimal::ONE
            } else {
                -Decimal::ONE
            };
            let native = crate::finance::position_pnl(
                qty,
                entry,
                mark.mark_price,
                plan.contract_value,
                plan.inverse,
                direction,
            )?;
            let unrealized = convert(&plan.settlement_currency, native)?;
            let row = result
                .get_mut(&owner)
                .ok_or(StoreError::Invalid("STRATEGY_NAV_CAPITAL_NOT_ASSIGNED"))?;
            row.unrealized_usd = row
                .unrealized_usd
                .checked_add(unrealized)
                .ok_or(StoreError::Invalid("MONEY_OVERFLOW"))?;
        }
        if marks.len() != venue_net.len() {
            return Err(StoreError::Invalid("STRATEGY_NAV_VENUE_POSITION_MISMATCH"));
        }
        for (instrument, (net, gross)) in venue_net {
            let Some(witness) = marks.get(&instrument) else {
                return Err(StoreError::Invalid("STRATEGY_NAV_VENUE_POSITION_MISMATCH"));
            };
            if net != witness.signed_contracts || gross != net.abs() {
                return Err(StoreError::Invalid("STRATEGY_NAV_VENUE_NETTING_UNRESOLVED"));
            }
        }
        let mut attributed = Decimal::ZERO;
        for row in result.values_mut() {
            row.equity_usd = row
                .capital_usd
                .checked_add(row.realized_usd)
                .and_then(|x| x.checked_add(row.unrealized_usd))
                .ok_or(StoreError::Invalid("MONEY_OVERFLOW"))?;
            attributed = attributed
                .checked_add(row.equity_usd)
                .ok_or(StoreError::Invalid("MONEY_OVERFLOW"))?;
        }
        let residual = state
            .account_nav_usd
            .checked_sub(attributed)
            .ok_or(StoreError::Invalid("MONEY_OVERFLOW"))?;
        if residual < Decimal::ZERO {
            return Err(StoreError::Invalid(
                "STRATEGY_NAV_ACCOUNT_RECONCILIATION_FAILED",
            ));
        }
        Ok(StrategyValuationReconciliation {
            account_nav_usd: state.account_nav_usd,
            attributed_usd: attributed,
            unattributed_usd: residual,
            as_of: evidence.as_of,
            source_ref: evidence.account_nav_source.clone(),
            strategies: result.into_values().collect(),
            coverage: "COMPLETE".into(),
        })
    }
}
