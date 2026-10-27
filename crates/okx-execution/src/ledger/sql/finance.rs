use super::super::*;
impl AccountStore {
    /// Account-backed principal, never PnL or independently priced strategy NAV.
    /// All returned balances are exact USD decimals within this AccountStore.
    pub fn capital_state(&self, now: i64) -> StoreResult<AccountCapitalState> {
        let (nav,nav_time,coverage,source):(String,i64,String,String)=self.connection.query_row(
            "SELECT equity,time,coverage,mark_source FROM nav_points WHERE scope_id='account' AND currency='USD' AND time<=?1 ORDER BY time DESC,source_version DESC LIMIT 1",
            [now],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))
        ).optional()?.ok_or(StoreError::Invalid("ACCOUNT_NAV_SOURCE_MISSING"))?;
        let recovering: i64 = self.connection.query_row(
            "SELECT requires_reconcile FROM storage_recovery WHERE singleton=1",
            [],
            |r| r.get(0),
        )?;
        if coverage != "COMPLETE" || now < nav_time || now - nav_time > 5000 || recovering != 0 {
            return Err(StoreError::Invalid(
                "ACCOUNT_CAPITAL_SOURCE_INCOMPLETE_OR_STALE",
            ));
        }
        let account_nav = decimal(nav)?;
        if account_nav < Decimal::ZERO {
            return Err(StoreError::Invalid("ACCOUNT_NAV_NEGATIVE"));
        }
        let mut query=self.connection.prepare(
            "SELECT instance_id,amount FROM instance_finance_totals WHERE currency='USD' AND component='INTERNAL_CAPITAL_USD' ORDER BY instance_id"
        )?;
        let mut rows = query.query([])?;
        let mut principals = std::collections::BTreeMap::new();
        let mut assigned = Decimal::ZERO;
        while let Some(row) = rows.next()? {
            let id: String = row.get(0)?;
            let amount = decimal(row.get(1)?)?;
            if id == "manual" || amount < Decimal::ZERO {
                return Err(StoreError::Invalid("INTERNAL_CAPITAL_CORRUPT"));
            }
            assigned = assigned
                .checked_add(amount)
                .ok_or(StoreError::Invalid("MONEY_OVERFLOW"))?;
            principals.insert(id, amount);
            if principals.len() > 64 {
                return Err(StoreError::Invalid("CAPITAL_SCOPE_LIMIT"));
            }
        }
        let residual = account_nav
            .checked_sub(assigned)
            .ok_or(StoreError::Invalid("MONEY_OVERFLOW"))?;
        // The residual includes economic returns until complete per-owner
        // valuation exists. It must never be presented as trading PnL.
        // Known venue losses can reduce equity below previously assigned
        // principal. Report the deficit, but allow safe deallocation to repair it.
        let under_collateralized = residual < Decimal::ZERO;
        let total:String=self.connection.query_row(
            "SELECT amount FROM finance_totals WHERE currency='USD' AND component='INTERNAL_CAPITAL_USD'",
            [],|r|r.get(0)
        ).optional()?.unwrap_or_else(||"0".into());
        if decimal(total)? != Decimal::ZERO {
            return Err(StoreError::Invalid("INTERNAL_CAPITAL_NOT_CONSERVED"));
        }
        Ok(AccountCapitalState {
            account_nav_usd: account_nav,
            assigned_usd: assigned,
            unattributed_residual_usd: residual,
            strategy_principal_usd: principals,
            nav_source_as_of: nav_time,
            nav_source_ref: source,
            coverage: if under_collateralized {
                "PARTIAL"
            } else {
                "COMPLETE"
            }
            .into(),
            reason_code: if under_collateralized {
                Some("ACCOUNT_CAPITAL_OVERASSIGNED".into())
            } else {
                None
            },
        })
    }

    /// Verify and price known strategy capital without creating a NAV point or
    /// unlocking risk. Only a converged exchange snapshot and five complete
    /// post-T0 history streams may supply a COMPLETE calculation.
    pub fn reconcile_strategy_valuation(
        &self,
        evidence: &StrategyValuationEvidence,
    ) -> StoreResult<StrategyValuationReconciliation> {
        if evidence.account_key != self.meta.account_key
            || evidence.account_nav_source.is_empty()
            || evidence.conversion.source_digest.is_empty()
            || evidence.as_of != evidence.conversion.observed_at
            || evidence.venue_positions.len() > 256
        {
            return Err(StoreError::Invalid("STRATEGY_VALUATION_EVIDENCE_INVALID"));
        }
        let state = self.capital_state(evidence.as_of)?;
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
            let found:Option<(i64,String,String)>=self.connection.query_row(
                "SELECT watermark,cursor,coverage_gaps FROM source_cursors WHERE source=?1 AND stream_partition=?2",
                params![source,part],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))
            ).optional()?;
            if !found.is_some_and(|(t, c, g)| t >= evidence.as_of && c.is_empty() && g.is_empty()) {
                return Err(StoreError::Invalid("STRATEGY_NAV_HISTORY_INCOMPLETE"));
            }
        }
        let pending:i64=self.connection.query_row(
            "SELECT (SELECT COUNT(*) FROM fill_projection_pending)+(SELECT COUNT(*) FROM funding_projection_pending)",
            [],|r|r.get(0)
        )?;
        if pending != 0 {
            return Err(StoreError::Invalid("STRATEGY_NAV_PROJECTION_PENDING"));
        }
        // A modeled economic component can be counted as strategy profit
        // only if its immutable originating fill or bill exists in this
        // account's ledger. Arbitrary finance ingestion is NOT source proof.
        let mut sources=self.connection.prepare(
            "SELECT instance_id,component,source_ref,economic_event_key,amount,currency FROM finance_entries WHERE time>=?1 AND time<=?2 AND instance_id IS NOT NULL ORDER BY time,entry_id"
        )?;
        let mut source_rows = sources.query(params![self.meta.initialized_at, evidence.as_of])?;
        while let Some(r) = source_rows.next()? {
            let instance: String = r.get(0)?;
            if instance == "manual" {
                continue;
            }
            let component: String = r.get(1)?;
            if !matches!(
                component.as_str(),
                "FEE" | "FUNDING" | "REALIZED_PNL" | "LIQUIDATION_COST"
            ) {
                if component == "INTERNAL_CAPITAL_USD" || component == "CAMPAIGN_LOSS_USD" {
                    continue;
                }
                return Err(StoreError::Invalid("STRATEGY_NAV_COMPONENT_UNKNOWN"));
            }
            let proof: String = r.get(2)?;
            let economic_key: String = r.get(3)?;
            let amount = decimal(r.get(4)?)?;
            let currency: String = r.get(5)?;
            let found = if let Some(id) = proof.strip_prefix("fill:") {
                let fill:Option<(String,String,String,String,String)>=self.connection.query_row(
                    "SELECT instrument_id,trade_id,exchange_order_id,fee,currency FROM fills WHERE fill_id=?1",
                    [id],|row|Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?,row.get(4)?))
                ).optional()?;
                if let Some((instrument, trade, order, fee, fee_currency)) = fill {
                    let canonical = format!("fill:{instrument}:{trade}:{order}");
                    // Venue-provided bill IDs are also valid economic keys,
                    // but they must be present in the same account ledger.
                    let linked_bill = if let Some(bill_id) = economic_key.strip_prefix("bill:") {
                        self.connection.query_row(
                            "SELECT EXISTS(SELECT 1 FROM bills WHERE bill_id=?1)",
                            [bill_id],
                            |row| row.get::<_, bool>(0),
                        )?
                    } else {
                        false
                    };
                    if component == "FEE" && (currency != fee_currency || amount != decimal(fee)?) {
                        return Err(StoreError::Invalid("STRATEGY_NAV_FEE_SOURCE_MISMATCH"));
                    }
                    economic_key == canonical || linked_bill
                } else {
                    false
                }
            } else if let Some(id) = proof.strip_prefix("bill:") {
                economic_key == proof
                    && self.connection.query_row(
                        "SELECT EXISTS(SELECT 1 FROM bills WHERE bill_id=?1)",
                        [id],
                        |row| row.get::<_, bool>(0),
                    )?
            } else {
                false
            };
            if !found {
                return Err(StoreError::Invalid(
                    "STRATEGY_NAV_ECONOMIC_SOURCE_UNVERIFIED",
                ));
            }
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
        for ((owner, currency, component), amount) in
            self.finance_attribution_filtered(self.meta.initialized_at, evidence.as_of, &[], &[])?
        {
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
        let mut lots=self.connection.prepare(
            "SELECT instance_id,intent_id,instrument_id,side,remaining_qty,cost FROM position_lots WHERE remaining_qty<>'0' ORDER BY instrument_id,lot_id LIMIT 258"
        )?;
        let mut rows = lots.query([])?;
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
            let plan = self.plan(&intent)?;
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

    /// Valuation needs a stronger fence than finance-history pagination:
    /// a zero-fee fill or a venue bill can change economic exposure without
    /// appending a finance entry. Only immutable source tables participate;
    /// risk-budget allocations and valuation/readback rows do not.
    pub fn strategy_valuation_revision(&self) -> StoreResult<String> {
        let (finance, fills, bills, lots, flows): (i64, i64, i64, i64, i64) =
            self.connection.query_row(
                "SELECT
                    (SELECT COALESCE(MAX(rowid),0) FROM finance_entries),
                    (SELECT COALESCE(MAX(rowid),0) FROM fills),
                    (SELECT COALESCE(MAX(rowid),0) FROM bills),
                    (SELECT COALESCE(MAX(rowid),0) FROM position_lots),
                    (SELECT COALESCE(MAX(rowid),0) FROM cash_flows)",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
            )?;
        Ok(format!("f{finance}:x{fills}:b{bills}:p{lots}:c{flows}"))
    }

    /// Atomically publish only a previously source-verified estimate to the
    /// existing NAV points and immutable operation receipts. A matching finance
    /// revision fences replay after a late fill, fee, bill or correction.
    pub fn commit_reconciled_strategy_nav(
        &mut self,
        evidence: &StrategyValuationEvidence,
    ) -> StoreResult<StrategyValuationReconciliation> {
        let summary = self.reconcile_strategy_valuation(evidence)?;
        if summary.strategies.is_empty() {
            return Ok(summary);
        }
        let revision = self.strategy_valuation_revision()?;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        for row in &summary.strategies {
            let scope = format!("strategy:{}", row.instance_id);
            let points = [
                (
                    self.meta.initialized_at,
                    Decimal::ZERO,
                    Decimal::ZERO,
                    Decimal::ZERO,
                    self.meta.opening_source.as_str(),
                    String::from("opening"),
                ),
                (
                    evidence.as_of,
                    row.equity_usd,
                    row.realized_usd
                        .checked_add(row.unrealized_usd)
                        .ok_or(StoreError::Invalid("MONEY_OVERFLOW"))?,
                    row.capital_usd,
                    evidence.account_nav_source.as_str(),
                    format!("revision:{revision}"),
                ),
            ];
            for (time, equity, pnl, flows, source, version) in points {
                // Same as-of can be revalued after an out-of-order fill, bill
                // or account correction. Keep both revisions immutable;
                // readback accepts only the currently verified source revision.
                let source_version = format!("strategy-reconciled-v1:{version}");
                let receipt_id = format!("strategy-nav:{scope}:{time}:{source_version}");
                let previous:Option<(String,String,String)>=tx.query_row(
                    "SELECT kind,request_hash,result_ref FROM operation_receipts WHERE operation_id=?1",
                    [&receipt_id],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))
                ).optional()?;
                if previous.is_some_and(|(kind, hash, refid)| {
                    kind != "STRATEGY_NAV_RECONCILED" || hash != version || refid != source
                }) {
                    return Err(StoreError::Invalid("STRATEGY_NAV_WITNESS_CONFLICT"));
                }
                tx.execute(
                    "INSERT INTO operation_receipts VALUES(?1,'STRATEGY_NAV_RECONCILED',?2,?3,?4) ON CONFLICT(operation_id) DO NOTHING",
                    params![receipt_id,version,source,time]
                )?;
                tx.execute(
                    "INSERT INTO nav_points VALUES(?1,?2,?3,?4,?5,?6,'USD','COMPLETE',?7) ON CONFLICT(scope_id,time,source_version) DO NOTHING",
                    params![scope,time,source_version,exact(equity),exact(pnl),exact(flows),source]
                )?;
                let actual:(String,String,String,String)=tx.query_row(
                    "SELECT equity,pnl,flows,mark_source FROM nav_points WHERE scope_id=?1 AND time=?2 AND source_version=?3",
                    params![scope,time,source_version],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))
                )?;
                if actual != (exact(equity), exact(pnl), exact(flows), source.to_owned()) {
                    return Err(StoreError::Invalid("STRATEGY_NAV_WITNESS_CONFLICT"));
                }
            }
            tx.execute(
                "INSERT INTO nav_scopes VALUES(?1,'USD') ON CONFLICT DO NOTHING",
                [&scope],
            )?;
            // No strategy high-water or risk admission from this read model:
            // strategy drawdown and authorization need independent acceptance.
        }
        tx.commit()?;
        Ok(summary)
    }

    /// Atomic two-leg, account-backed book assignment. The caller is the
    /// already-authorized Agent governance owner, not an arbitrary ingestion
    /// stream. No venue transfer, risk reservation, NAV point or highwater
    /// is created here. This cannot authorize new trading risk.
    pub fn commit_authorized_capital_assignment(
        &mut self,
        request: &AuthorizedCapitalAssignment,
    ) -> StoreResult<OperationReceipt> {
        if request.account_key != self.meta.account_key
            || request.installation_id != self.meta.installation_id
            || request.nav_source_ref.is_empty()
            || request.instance_id == "manual"
            || request.instance_id.is_empty()
            || request.operation_id.is_empty()
            || request.request_hash.is_empty()
            || request.authority_receipt.is_empty()
            || request.authority_revision <= 0
            || request.time < self.meta.initialized_at
            || request.delta_usd == Decimal::ZERO
        {
            return Err(StoreError::Invalid("INVALID_CAPITAL_ASSIGNMENT"));
        }
        if let Some(old) = self.operation(&request.operation_id)? {
            if old.kind != "CAPITAL_ASSIGNMENT" || old.request_hash != request.request_hash {
                return Err(StoreError::Invalid("IDEMPOTENCY_CONFLICT"));
            }
            return Ok(old);
        }
        let state = self.capital_state(request.time)?;
        if request.nav_source_ref != state.nav_source_ref {
            return Err(StoreError::Invalid("CAPITAL_NAV_SOURCE_MISMATCH"));
        }
        let existing = *state
            .strategy_principal_usd
            .get(&request.instance_id)
            .unwrap_or(&Decimal::ZERO);
        let next = existing
            .checked_add(request.delta_usd)
            .ok_or(StoreError::Invalid("MONEY_OVERFLOW"))?;
        if next < Decimal::ZERO
            || (request.delta_usd > Decimal::ZERO
                && request.delta_usd > state.unattributed_residual_usd)
        {
            return Err(StoreError::Invalid("CAPITAL_ASSIGNMENT_EXCEEDS_AVAILABLE"));
        }
        if self.instance(&request.instance_id)?.is_none() {
            return Err(StoreError::Invalid("STRATEGY_INSTANCE_NOT_FOUND"));
        }
        let (policy_revision,): (i64,) = self.connection.query_row(
            "SELECT revision FROM account_policy WHERE singleton=1",
            [],
            |r| Ok((r.get(0)?,)),
        )?;
        if policy_revision != request.authority_revision {
            return Err(StoreError::Invalid("CAPITAL_AUTHORITY_REVISION_MISMATCH"));
        }
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        // Book two opposite legs under one economic event. Neither leg is
        // income or expense. SQL immutability protects the event after commit.
        for (leg, owner, delta) in [
            (
                format!("strategy:{}", request.instance_id),
                Some(request.instance_id.as_str()),
                request.delta_usd,
            ),
            (
                String::from("unattributed"),
                None,
                Decimal::ZERO
                    .checked_sub(request.delta_usd)
                    .ok_or(StoreError::Invalid("MONEY_OVERFLOW"))?,
            ),
        ] {
            let entry_id = format!("capital:{}:{leg}", request.operation_id);
            tx.execute(
                "INSERT INTO finance_entries(entry_id,economic_event_key,component,allocation_leg,amount,currency,instance_id,source_ref,time,reverses_entry_id) VALUES(?1,?2,'INTERNAL_CAPITAL_USD',?3,?4,'USD',?5,?6,?7,NULL)",
                params![entry_id,format!("capital:{}",request.operation_id),leg,exact(delta),owner,request.authority_receipt,request.time]
            )?;
            tx.execute(
                "INSERT INTO finance_source_links(entry_id,source_ref) VALUES(?1,?2)",
                params![entry_id, request.authority_receipt],
            )?;
        }
        tx.execute(
            "INSERT INTO instance_finance_totals(instance_id,currency,component,amount) VALUES(?1,'USD','INTERNAL_CAPITAL_USD',?2) ON CONFLICT(instance_id,currency,component) DO UPDATE SET amount=excluded.amount",
            params![request.instance_id,exact(next)]
        )?;
        // A single account aggregate represents BOTH legs and remains zero.
        tx.execute(
            "INSERT INTO finance_totals(currency,component,amount) VALUES('USD','INTERNAL_CAPITAL_USD','0') ON CONFLICT(currency,component) DO NOTHING",
            []
        )?;
        let result = format!(
            "strategy:{}|principal_usd:{}|account_nav_as_of:{}",
            request.instance_id,
            exact(next),
            state.nav_source_as_of
        );
        tx.execute(
            "INSERT INTO operation_receipts VALUES(?1,'CAPITAL_ASSIGNMENT',?2,?3,?4)",
            params![
                request.operation_id,
                request.request_hash,
                result,
                request.time
            ],
        )?;
        tx.commit()?;
        self.operation(&request.operation_id)?
            .ok_or(StoreError::Invalid("CAPITAL_ASSIGNMENT_RECEIPT_MISSING"))
    }

    pub fn nav_summary(
        &self,
        from: i64,
        through: i64,
        instances: &[String],
        currencies: &[String],
    ) -> StoreResult<Vec<NavWindow>> {
        if from > through || instances.len() > 32 || currencies.len() > 32 {
            return Err(StoreError::Invalid("NAV_RANGE_FILTER_INVALID"));
        }
        let scopes = {
            let mut q = self
                .connection
                .prepare("SELECT scope_id,currency FROM nav_scopes UNION SELECT 'strategy:' || instance_id, 'USD' FROM strategy_instances ORDER BY scope_id,currency")?;
            q.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?
                .filter_map(|v| match v {
                    Ok((scope, currency))
                        if (instances.is_empty()
                            || instances.iter().any(|i| scope == format!("strategy:{i}")))
                            && (currencies.is_empty() || currencies.contains(&currency)) =>
                    {
                        Some(Ok((scope, currency)))
                    }
                    Err(e) => Some(Err(e)),
                    _ => None,
                })
                .take(65)
                .collect::<Result<Vec<_>, _>>()?
        };
        if scopes.len() > 64 {
            return Err(StoreError::Invalid("NAV_SCOPE_LIMIT"));
        }
        let mut result = Vec::new();
        let revision = self.strategy_valuation_revision()?;
        for (scope, currency) in scopes {
            // Only native reconciliation receipts authorize strategy NAV rows.
            // Arbitrary strategy nav_points, including old COMPLETE claims, are
            // ignored. A new financial event invalidates a prior revision.
            let manual = scope == "strategy:manual";
            let strategy = scope.starts_with("strategy:") && !manual;
            let endpoint = |time: i64| -> StoreResult<Option<(Decimal, Decimal, i64, String)>> {
                let row: Option<(String, String, i64, String)> = if strategy {
                    self.connection.query_row(
                        "SELECT n.equity,n.flows,n.time,n.coverage FROM nav_points n
                         JOIN operation_receipts o ON o.operation_id='strategy-nav:' || n.scope_id || ':' || n.time || ':' || n.source_version
                         WHERE n.scope_id=?1 AND n.time<=?2
                           AND n.source_version IN ('strategy-reconciled-v1:opening',?3)
                           AND o.kind='STRATEGY_NAV_RECONCILED' AND o.result_ref=n.mark_source
                           AND o.request_hash=substr(n.source_version,length('strategy-reconciled-v1:')+1)
                         ORDER BY n.time DESC, n.source_version DESC LIMIT 1",
                        params![scope,time,format!("strategy-reconciled-v1:revision:{revision}")],
                        |r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))
                    ).optional()?
                } else {
                    self.connection.query_row(
                        "SELECT equity,flows,time,coverage FROM nav_points WHERE scope_id=?1 AND time<=?2 ORDER BY time DESC,source_version DESC LIMIT 1",
                        params![scope,time],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))
                    ).optional()?
                };
                row.map(|(e, f, t, c)| Ok((decimal(e)?, decimal(f)?, t, c)))
                    .transpose()
            };
            let capital_known=!manual && (!scope.starts_with("strategy:") ||
                self.connection.query_row(
                    "SELECT EXISTS(SELECT 1 FROM nav_points n JOIN operation_receipts o ON
                     o.operation_id='strategy-nav:' || n.scope_id || ':' || n.time || ':' || n.source_version
                     WHERE n.scope_id=?1 AND n.time<=?2 AND n.source_version=?3
                       AND o.kind='STRATEGY_NAV_RECONCILED'
                       AND o.request_hash=substr(n.source_version,length('strategy-reconciled-v1:')+1)
                       AND o.result_ref=n.mark_source)",
                    params![scope,through,format!("strategy-reconciled-v1:revision:{revision}")],
                    |r|r.get::<_,bool>(0)
                )?);
            let opening = if capital_known { endpoint(from)? } else { None };
            let current = if capital_known {
                endpoint(through)?
            } else {
                None
            };
            let flows = opening
                .as_ref()
                .zip(current.as_ref())
                .map(|(o, c)| {
                    c.1.checked_sub(o.1)
                        .ok_or(StoreError::Invalid("MONEY_OVERFLOW"))
                })
                .transpose()?;
            let pnl = opening
                .as_ref()
                .zip(current.as_ref())
                .zip(flows)
                .map(|((o, c), f)| {
                    c.0.checked_sub(o.0)
                        .and_then(|v| v.checked_sub(f))
                        .ok_or(StoreError::Invalid("MONEY_OVERFLOW"))
                })
                .transpose()?;
            let valuation_gap = if strategy {
                let latest:Option<(i64,String)>=self.connection.query_row(
                    "SELECT watermark,coverage_gaps FROM source_cursors WHERE source='strategy_valuation' AND stream_partition='account'",
                    [],|r|Ok((r.get(0)?,r.get(1)?))
                ).optional()?;
                latest.is_some_and(|(t, g)| {
                    !g.is_empty() && t <= through && current.as_ref().is_none_or(|p| t >= p.2)
                })
            } else {
                false
            };
            // A newer account snapshot supersedes an old strategy witness
            // even when there is no fill, bill or finance entry (e.g. a pure
            // mark-to-market move). Historical reads are checked as-of through.
            let account_source_matches = if strategy {
                let account_latest:Option<(i64,String)>=self.connection.query_row(
                    "SELECT time,mark_source FROM nav_points WHERE scope_id='account'
                     AND currency='USD' AND time<=?1 ORDER BY time DESC,source_version DESC LIMIT 1",
                    [through],|r|Ok((r.get(0)?,r.get(1)?))
                ).optional()?;
                let strategy_source: Option<String> =
                    if let Some((_, _, time, _)) = current.as_ref() {
                        self.connection
                            .query_row(
                                "SELECT mark_source FROM nav_points WHERE scope_id=?1 AND time=?2
                         AND source_version='strategy-reconciled-v1'",
                                params![scope, time],
                                |r| r.get(0),
                            )
                            .optional()?
                    } else {
                        None
                    };
                account_latest
                    .zip(strategy_source)
                    .is_some_and(|((time, source), witness)| {
                        current
                            .as_ref()
                            .is_some_and(|row| row.2 == time && source == witness)
                    })
            } else {
                true
            };
            let complete = account_source_matches
                && !valuation_gap
                && opening
                    .as_ref()
                    .is_some_and(|v| v.3 == "COMPLETE" && from - v.2 <= 5000)
                && current
                    .as_ref()
                    .is_some_and(|v| v.3 == "COMPLETE" && through - v.2 <= 5000);
            result.push(NavWindow {
                scope_id: scope,
                currency,
                opening_equity: opening.as_ref().map(|v| v.0),
                current_equity: current.as_ref().map(|v| v.0),
                flows,
                pnl,
                opening_as_of: opening.as_ref().map(|v| v.2),
                current_as_of: current.as_ref().map(|v| v.2),
                coverage: if manual {
                    "NOT_APPLICABLE"
                } else if from < self.meta.initialized_at {
                    "UNAVAILABLE_BEFORE_T0"
                } else if complete && capital_known {
                    "COMPLETE"
                } else {
                    "PARTIAL"
                }
                .into(),
            });
        }
        Ok(result)
    }
    pub fn opening_equity_usd(&self) -> StoreResult<Decimal> {
        decimal(self.connection.query_row("SELECT equity FROM nav_points WHERE scope_id='account' AND time=?1 AND source_version='opening'",[self.meta.initialized_at],|r|r.get(0))?)
    }
    pub fn usd_component_total(&self, component: &str) -> StoreResult<Decimal> {
        let v: Option<String> = self
            .connection
            .query_row(
                "SELECT amount FROM finance_totals WHERE currency='USD' AND component=?1",
                [component],
                |r| r.get(0),
            )
            .optional()?;
        v.map(decimal)
            .transpose()
            .map(|v| v.unwrap_or(Decimal::ZERO))
    }
    pub fn financial_risk(&self, now: i64, instance: Option<&str>) -> StoreResult<FinancialRisk> {
        let scope = instance
            .map(|s| format!("strategy:{s}"))
            .unwrap_or_else(|| "account".into());
        let daily: Option<String> = self
            .connection
            .query_row(
                "SELECT loss_usd FROM daily_losses WHERE scope_id=?1 AND utc_day=?2",
                params![scope, crate::risk::utc_day(now)],
                |r| r.get(0),
            )
            .optional()?;
        let nav:Option<(String,String,i64,String)>=self.connection.query_row("SELECT highwater,adjusted_equity,time,coverage FROM nav_watermarks WHERE scope_id=?1",[&scope],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))).optional()?;
        let (drawdown, asof, complete) = if let Some((peak, equity, time, coverage)) = nav {
            let peak = decimal(peak)?;
            let equity = decimal(equity)?;
            let dd = if peak > Decimal::ZERO {
                peak.checked_sub(equity)
                    .and_then(|v| v.max(Decimal::ZERO).checked_div(peak))
                    .ok_or(StoreError::Invalid("MONEY_OVERFLOW"))?
            } else {
                Decimal::ZERO
            };
            (
                dd,
                time,
                coverage == "COMPLETE" && instance.is_none() && now >= time && now - time <= 5000,
            )
        } else {
            (Decimal::ZERO, 0, false)
        };
        Ok(FinancialRisk {
            daily_loss_usd: daily.map(decimal).transpose()?.unwrap_or(Decimal::ZERO),
            drawdown_ratio: drawdown,
            nav_complete: complete,
            source_as_of: asof,
        })
    }
    pub fn attribute_pending_funding(
        &mut self,
        fx: &UsdConversionEvidence,
        limit: usize,
    ) -> StoreResult<usize> {
        let rows = {
            let mut q=self.connection.prepare("SELECT event_id,instrument_id,time,amount,currency FROM funding_projection_pending ORDER BY time,event_id LIMIT ?1")?;
            q.query_map([limit.min(100) as i64], |r| {
                Ok((
                    FundingEvent {
                        event_id: r.get(0)?,
                        instrument_id: r.get(1)?,
                        time: r.get(2)?,
                        amount: Decimal::ZERO,
                        currency: r.get(4)?,
                    },
                    r.get::<_, String>(3)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?
        };
        let count = rows.len();
        for (mut event, amount) in rows {
            event.amount = decimal(amount)?;
            let mut exposures = std::collections::BTreeMap::<String, Decimal>::new();
            {
                let mut q=self.connection.prepare("SELECT d.instance_id,p.operation,p.position_side,f.side,f.qty FROM fills f LEFT JOIN intents i USING(intent_id) LEFT JOIN decisions d USING(decision_id) LEFT JOIN intent_plans p USING(intent_id) WHERE f.instrument_id=?1 AND f.time<=?2 ORDER BY f.time,f.fill_id")?;
                let mut rows = q.query(params![event.instrument_id, event.time])?;
                while let Some(r) = rows.next()? {
                    let owner = r
                        .get::<_, Option<String>>(0)?
                        .unwrap_or_else(|| "unattributed".into());
                    let operation = r.get::<_, Option<String>>(1)?;
                    let position = r.get::<_, Option<String>>(2)?;
                    let side: String = r.get(3)?;
                    let q = decimal(r.get(4)?)?;
                    let sign = match position.as_deref() {
                        Some("long") => {
                            if operation
                                .as_deref()
                                .is_some_and(|o| matches!(o, "close" | "reduce"))
                            {
                                -Decimal::ONE
                            } else {
                                Decimal::ONE
                            }
                        }
                        Some("short") => {
                            if operation
                                .as_deref()
                                .is_some_and(|o| matches!(o, "close" | "reduce"))
                            {
                                Decimal::ONE
                            } else {
                                -Decimal::ONE
                            }
                        }
                        _ => {
                            if side == "buy" {
                                Decimal::ONE
                            } else {
                                -Decimal::ONE
                            }
                        }
                    };
                    let old = *exposures.get(&owner).unwrap_or(&Decimal::ZERO);
                    exposures.insert(
                        owner,
                        old.checked_add(
                            q.checked_mul(sign)
                                .ok_or(StoreError::Invalid("MONEY_OVERFLOW"))?,
                        )
                        .ok_or(StoreError::Invalid("MONEY_OVERFLOW"))?,
                    );
                }
            }
            let exposure = exposures
                .into_iter()
                .filter(|(_, q)| *q != Decimal::ZERO)
                .collect::<Vec<_>>();
            let allocations = crate::finance::allocate_funding(event.amount, &exposure, 18)?;
            let unknown = allocations
                .iter()
                .any(|(owner, a)| owner == "unattributed" && *a != Decimal::ZERO);
            let key = format!("bill:{}", event.event_id);
            let mut page = IngestPage::default();
            for (owner, amount) in allocations {
                if amount == Decimal::ZERO && owner != "rounding_residual" {
                    continue;
                }
                let instance = if owner == "unattributed" || owner == "rounding_residual" {
                    None
                } else {
                    Some(owner.clone())
                };
                page.finance.push(FinanceEntry {
                    entry_id: format!("{key}:funding:{owner}"),
                    economic_event_key: key.clone(),
                    component: "FUNDING".into(),
                    allocation_leg: owner,
                    amount,
                    currency: event.currency.clone(),
                    instance_id: instance,
                    source_ref: key.clone(),
                    time: event.time,
                    reverses_entry_id: None,
                });
            }
            page.attributed_funding_ids.push(event.event_id);
            self.ingest_page_with_usd(&page, fx)?;
            if unknown {
                self.latch_stop(
                    "funding-attribution-incomplete",
                    "account",
                    "FUNDING_ATTRIBUTION_INCOMPLETE",
                    event.time,
                )?;
            }
        }
        Ok(count)
    }
    pub fn ingest_page_with_usd(
        &mut self,
        page: &IngestPage,
        fx: &UsdConversionEvidence,
    ) -> StoreResult<()> {
        if fx.source_digest.is_empty() || fx.observed_at <= 0 {
            return Err(StoreError::Invalid("USD_CONVERSION_SOURCE_UNAVAILABLE"));
        }
        let mut normalized = page.clone();
        for flow in &page.cash_flows {
            if flow.kind == "OPENING" {
                continue;
            }
            let exists:bool=self.connection.query_row("SELECT EXISTS(SELECT 1 FROM finance_entries WHERE economic_event_key=?1 AND component='EXTERNAL_FLOW_USD' AND allocation_leg='account')",[&flow.event_id],|r|r.get(0))?;
            if exists {
                continue;
            }
            let rate = if flow.currency == "USD" {
                Decimal::ONE
            } else {
                match fx.usd_per_currency.get(&flow.currency).copied() {
                    Some(v) => v,
                    None => {
                        self.latch_stop(
                            "usd-conversion-unavailable",
                            "account",
                            "USD_CONVERSION_SOURCE_UNAVAILABLE",
                            fx.observed_at,
                        )?;
                        return Err(StoreError::Invalid("USD_CONVERSION_SOURCE_UNAVAILABLE"));
                    }
                }
            };
            let amount = flow
                .amount
                .checked_mul(rate)
                .ok_or(StoreError::Invalid("MONEY_OVERFLOW"))?;
            normalized.finance.push(FinanceEntry {
                entry_id: format!("{}:usd-flow", flow.event_id),
                economic_event_key: flow.event_id.clone(),
                component: "EXTERNAL_FLOW_USD".into(),
                allocation_leg: "account".into(),
                amount,
                currency: "USD".into(),
                instance_id: None,
                source_ref: format!(
                    "{}|fx:{}|rate:{}|at:{}",
                    flow.source_ref,
                    fx.source_digest,
                    exact(rate),
                    fx.observed_at
                ),
                time: flow.time,
                reverses_entry_id: None,
            });
        }
        for e in &page.finance {
            if e.amount >= Decimal::ZERO
                || !matches!(
                    e.component.as_str(),
                    "FEE" | "REALIZED_PNL" | "FUNDING" | "LIQUIDATION_COST"
                )
            {
                continue;
            }
            let leg = format!("{}:{}", e.component, e.allocation_leg);
            let exists:bool=self.connection.query_row("SELECT EXISTS(SELECT 1 FROM finance_entries WHERE economic_event_key=?1 AND component='CAMPAIGN_LOSS_USD' AND allocation_leg=?2)",params![e.economic_event_key,leg],|r|r.get(0))?;
            if exists {
                continue;
            }
            let rate = if e.currency == "USD" {
                Decimal::ONE
            } else {
                match fx.usd_per_currency.get(&e.currency).copied() {
                    Some(v) => v,
                    None => {
                        self.latch_stop(
                            "usd-conversion-unavailable",
                            "account",
                            "USD_CONVERSION_SOURCE_UNAVAILABLE",
                            fx.observed_at,
                        )?;
                        return Err(StoreError::Invalid("USD_CONVERSION_SOURCE_UNAVAILABLE"));
                    }
                }
            };
            if rate <= Decimal::ZERO {
                self.latch_stop(
                    "usd-conversion-unavailable",
                    "account",
                    "USD_CONVERSION_SOURCE_UNAVAILABLE",
                    fx.observed_at,
                )?;
                return Err(StoreError::Invalid("USD_CONVERSION_SOURCE_UNAVAILABLE"));
            }
            let loss = (-e.amount)
                .checked_mul(rate)
                .ok_or(StoreError::Invalid("MONEY_OVERFLOW"))?
                .round_dp_with_strategy(9, rust_decimal::RoundingStrategy::AwayFromZero);
            normalized.finance.push(FinanceEntry {
                entry_id: format!("{}:usd-loss:{leg}", e.economic_event_key),
                economic_event_key: e.economic_event_key.clone(),
                component: "CAMPAIGN_LOSS_USD".into(),
                allocation_leg: leg,
                amount: loss,
                currency: "USD".into(),
                instance_id: e.instance_id.clone(),
                source_ref: format!(
                    "{}|fx:{}|rate:{}|at:{}",
                    e.source_ref,
                    fx.source_digest,
                    exact(rate),
                    fx.observed_at
                ),
                time: e.time,
                reverses_entry_id: None,
            });
        }
        self.ingest_page(&normalized)
    }
    #[allow(clippy::too_many_arguments)] // Public bounded pagination/filter facade.
    pub fn finance_history_filtered(
        &self,
        from: i64,
        through: i64,
        after_time: i64,
        after_id: &str,
        limit: usize,
        instances: &[String],
        currencies: &[String],
    ) -> StoreResult<Vec<FinanceEntry>> {
        let (filter, mut args) = finance_filter(instances, currencies, 5)?;
        let mut values = vec![
            from.into(),
            through.into(),
            after_time.into(),
            after_id.to_string().into(),
            (limit.min(1000) as i64).into(),
        ];
        values.append(&mut args);
        let query = format!(
            "SELECT entry_id,economic_event_key,component,allocation_leg,amount,currency,instance_id,source_ref,time,reverses_entry_id FROM finance_entries WHERE time>=?1 AND time<=?2 AND (time,entry_id)>(?3,?4){filter} ORDER BY time,entry_id LIMIT ?5"
        );
        let mut s = self.connection.prepare(&query)?;
        s.query_map(rusqlite::params_from_iter(values), |r| {
            Ok((
                FinanceEntry {
                    entry_id: r.get(0)?,
                    economic_event_key: r.get(1)?,
                    component: r.get(2)?,
                    allocation_leg: r.get(3)?,
                    amount: Decimal::ZERO,
                    currency: r.get(5)?,
                    instance_id: r.get(6)?,
                    source_ref: r.get(7)?,
                    time: r.get(8)?,
                    reverses_entry_id: r.get(9)?,
                },
                r.get::<_, String>(4)?,
            ))
        })?
        .map(|v| {
            let (mut e, a) = v?;
            e.amount = decimal(a)?;
            Ok(e)
        })
        .collect()
    }
    /// Cheap monotonic revision for the append-only, immutable finance log.
    /// Includes late event-time arrivals. MAX(rowid) uses the table btree,
    /// avoiding a full historical scan on every history page.
    pub fn finance_revision(&self) -> StoreResult<i64> {
        Ok(self.connection.query_row(
            "SELECT COALESCE(MAX(rowid),0) FROM finance_entries",
            [],
            |r| r.get(0),
        )?)
    }

    /// Exact per-instance economic facts from the canonical account ledger.
    /// None means the account's unattributed leg, not a second copy of money.
    pub fn finance_attribution_filtered(
        &self,
        from: i64,
        through: i64,
        instances: &[String],
        currencies: &[String],
    ) -> StoreResult<FinanceAttribution> {
        let (filter, mut args) = finance_filter(instances, currencies, 2)?;
        let mut values = vec![from.into(), through.into()];
        values.append(&mut args);
        let query = format!(
            "SELECT instance_id,currency,component,amount FROM finance_entries WHERE time>=?1 AND time<=?2{filter} ORDER BY time,entry_id"
        );
        let mut s = self.connection.prepare(&query)?;
        let mut rows = s.query(rusqlite::params_from_iter(values))?;
        let mut totals = std::collections::BTreeMap::new();
        while let Some(row) = rows.next()? {
            let key = (
                row.get::<_, Option<String>>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            );
            let amount = decimal(row.get(3)?)?;
            let old = *totals.get(&key).unwrap_or(&Decimal::ZERO);
            totals.insert(
                key,
                old.checked_add(amount)
                    .ok_or(StoreError::Invalid("MONEY_OVERFLOW"))?,
            );
            if totals.len() > 4096 {
                return Err(StoreError::Invalid("FINANCE_ATTRIBUTION_GROUP_LIMIT"));
            }
        }
        Ok(totals)
    }

    pub fn finance_totals_filtered(
        &self,
        from: i64,
        through: i64,
        instances: &[String],
        currencies: &[String],
    ) -> StoreResult<std::collections::BTreeMap<(String, String), Decimal>> {
        let (filter, mut args) = finance_filter(instances, currencies, 2)?;
        let mut values = vec![from.into(), through.into()];
        values.append(&mut args);
        let query = format!(
            "SELECT currency,component,amount FROM finance_entries WHERE time>=?1 AND time<=?2{filter} ORDER BY time,entry_id"
        );
        let mut s = self.connection.prepare(&query)?;
        let mut rows = s.query(rusqlite::params_from_iter(values))?;
        let mut totals = std::collections::BTreeMap::new();
        while let Some(row) = rows.next()? {
            let key = (row.get::<_, String>(0)?, row.get::<_, String>(1)?);
            let amount = decimal(row.get(2)?)?;
            let old = *totals.get(&key).unwrap_or(&Decimal::ZERO);
            totals.insert(
                key,
                old.checked_add(amount)
                    .ok_or(StoreError::Invalid("MONEY_OVERFLOW"))?,
            );
        }
        Ok(totals)
    }
    pub fn cash_flow_totals(
        &self,
    ) -> StoreResult<std::collections::BTreeMap<(String, String), Decimal>> {
        let mut s = self
            .connection
            .prepare("SELECT currency,kind,amount FROM cash_flows ORDER BY time,event_id")?;
        let mut rows = s.query([])?;
        let mut totals = std::collections::BTreeMap::new();
        while let Some(r) = rows.next()? {
            let key = (r.get::<_, String>(0)?, r.get::<_, String>(1)?);
            let old = *totals.get(&key).unwrap_or(&Decimal::ZERO);
            let v = decimal(r.get(2)?)?;
            totals.insert(
                key,
                old.checked_add(v)
                    .ok_or(StoreError::Invalid("MONEY_OVERFLOW"))?,
            );
        }
        Ok(totals)
    }
    pub fn project_pending_fills(&mut self, limit: usize) -> StoreResult<usize> {
        let rows = {
            let mut s=self.connection.prepare("SELECT f.fill_id,f.instrument_id,f.trade_id,f.exchange_order_id,f.intent_id,f.time,f.qty,f.price,f.fee,f.currency,p.side FROM fill_projection_pending p JOIN fills f USING(fill_id) ORDER BY f.time,f.fill_id LIMIT ?1")?;
            s.query_map([limit.min(1000) as i64], |r| {
                Ok((
                    Fill {
                        fill_id: r.get(0)?,
                        instrument_id: r.get(1)?,
                        trade_id: r.get(2)?,
                        exchange_order_id: r.get(3)?,
                        intent_id: r.get(4)?,
                        time: r.get(5)?,
                        qty: Decimal::ZERO,
                        price: Decimal::ZERO,
                        fee: Decimal::ZERO,
                        currency: r.get(9)?,
                        side: r.get(10)?,
                    },
                    r.get::<_, String>(6)?,
                    r.get::<_, String>(7)?,
                    r.get::<_, String>(8)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?
        };
        let count = rows.len();
        for (mut fill, q, p, fee) in rows {
            fill.qty = decimal(q)?;
            fill.price = decimal(p)?;
            fill.fee = decimal(fee)?;
            let plan = fill
                .intent_id
                .as_deref()
                .map(|id| self.plan(id))
                .transpose()?;
            let effect = if plan
                .as_ref()
                .is_some_and(|p| matches!(p.operation.as_str(), "close" | "reduce"))
            {
                FillEffect::Reduce
            } else {
                FillEffect::Open {
                    side: if fill.side == "buy" { "long" } else { "short" }.into(),
                }
            };
            self.ingest_attributed_fill(fill, effect, None)?;
        }
        Ok(count)
    }
    pub fn ingest_attributed_fill(
        &mut self,
        fill: Fill,
        effect: FillEffect,
        cursor: Option<SourceCursor>,
    ) -> StoreResult<()> {
        let existing:bool=self.connection.query_row("SELECT EXISTS(SELECT 1 FROM fills WHERE instrument_id=?1 AND trade_id=?2 AND exchange_order_id=?3)",params![fill.instrument_id,fill.trade_id,fill.exchange_order_id],|r|r.get(0))?;
        let pending: bool = self.connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM fill_projection_pending WHERE fill_id=?1)",
            [&fill.fill_id],
            |r| r.get(0),
        )?;
        if existing && !pending {
            return self.ingest_page(&IngestPage {
                fills: vec![fill],
                cursor,
                ..Default::default()
            });
        }
        let instance: Option<String> = if let Some(id) = &fill.intent_id {
            self.connection.query_row("SELECT instance_id FROM decisions JOIN intents USING(decision_id) WHERE intent_id=?1",[id],|r|r.get(0))?
        } else {
            None
        };
        let key = format!(
            "fill:{}:{}:{}",
            fill.instrument_id, fill.trade_id, fill.exchange_order_id
        );
        let mut page = IngestPage {
            cursor,
            ..Default::default()
        };
        // F7: a raw fill may have committed with fill_projection_pending
        // before its fee/position projection. Rebuild the fee on replay;
        // the canonical economic-event uniqueness prevents double charge.
        if fill.fee != Decimal::ZERO {
            page.finance.push(FinanceEntry {
                entry_id: format!("{key}:fee"),
                economic_event_key: key.clone(),
                component: "FEE".into(),
                allocation_leg: "account".into(),
                amount: fill.fee,
                currency: fill.currency.clone(),
                instance_id: instance.clone(),
                source_ref: format!("fill:{}", fill.fill_id),
                time: fill.time,
                reverses_entry_id: None,
            });
        }
        match effect {
            FillEffect::Open { side } => {
                if !matches!(side.as_str(), "long" | "short") {
                    return Err(StoreError::Invalid("INVALID_POSITION_SIDE"));
                }
                page.lots.push(StoredLot {
                    lot_id: format!("lot:{}", fill.fill_id),
                    instance_id: instance.clone(),
                    intent_id: fill.intent_id.clone(),
                    fill_id: Some(fill.fill_id.clone()),
                    instrument_id: fill.instrument_id.clone(),
                    side,
                    remaining_qty: fill.qty,
                    cost: fill.price,
                    opened_at: fill.time,
                });
            }
            FillEffect::Reduce => {
                let owner = instance.as_deref().ok_or(StoreError::Invalid(
                    "EXPLICIT_INSTANCE_REQUIRED_FOR_REDUCTION",
                ))?;
                let reduce_plan = self.plan(
                    fill.intent_id
                        .as_deref()
                        .ok_or(StoreError::Invalid("REDUCTION_INTENT_REQUIRED"))?,
                )?;
                let existing_lots = self
                    .active_lots(&fill.instrument_id)?
                    .into_iter()
                    .filter(|l| {
                        l.side == reduce_plan.position_side
                            && l.instance_id.as_deref() == Some(owner)
                    })
                    .collect::<Vec<_>>();
                let mut lots = existing_lots
                    .iter()
                    .map(|l| {
                        let p = self.plan(
                            l.intent_id
                                .as_deref()
                                .ok_or(StoreError::Invalid("LOT_MECHANICS_UNAVAILABLE"))?,
                        )?;
                        Ok(crate::finance::PositionLot {
                            contract_value: p.contract_value,
                            inverse: p.inverse,
                            lot_id: l.lot_id.clone(),
                            instance_id: l.instance_id.clone(),
                            instrument_id: l.instrument_id.clone(),
                            signed_qty: if l.side == "long" {
                                l.remaining_qty
                            } else {
                                -l.remaining_qty
                            },
                            unit_cost: l.cost,
                            opened_at: l.opened_at,
                        })
                    })
                    .collect::<StoreResult<Vec<_>>>()?;
                let reductions = crate::finance::reduce_fifo(
                    &mut lots,
                    owner,
                    &fill.instrument_id,
                    fill.qty,
                    fill.price,
                )?;
                let pnl = reductions.iter().try_fold(Decimal::ZERO, |a, r| {
                    a.checked_add(r.realized_pnl)
                        .ok_or(StoreError::Invalid("MONEY_OVERFLOW"))
                })?;
                for l in existing_lots {
                    if let Some(updated) = lots.iter().find(|v| v.lot_id == l.lot_id)
                        && updated.signed_qty.abs() != l.remaining_qty
                    {
                        page.lots.push(StoredLot {
                            remaining_qty: updated.signed_qty.abs(),
                            ..l
                        });
                    }
                }
                if !existing {
                    page.finance.push(FinanceEntry {
                        entry_id: format!("{key}:realized"),
                        economic_event_key: key,
                        component: "REALIZED_PNL".into(),
                        allocation_leg: owner.into(),
                        amount: pnl,
                        currency: fill.currency.clone(),
                        instance_id: instance.clone(),
                        source_ref: format!("fill:{}", fill.fill_id),
                        time: fill.time,
                        reverses_entry_id: None,
                    });
                }
            }
        }
        page.projected_fill_ids.push(fill.fill_id.clone());
        page.fills.push(fill);
        self.ingest_page(&page)
    }
    pub fn finance_totals(
        &self,
        from: i64,
        through: i64,
        instance: Option<&str>,
    ) -> StoreResult<std::collections::BTreeMap<(String, String), Decimal>> {
        let mut statement=self.connection.prepare("SELECT currency,component,amount FROM finance_entries WHERE time>=?1 AND time<=?2 AND (?3 IS NULL OR instance_id=?3) ORDER BY time,entry_id")?;
        let mut rows = statement.query(params![from, through, instance])?;
        let mut totals = std::collections::BTreeMap::new();
        while let Some(row) = rows.next()? {
            let key = (row.get::<_, String>(0)?, row.get::<_, String>(1)?);
            let amount = decimal(row.get(2)?)?;
            let old = *totals.get(&key).unwrap_or(&Decimal::ZERO);
            totals.insert(
                key,
                old.checked_add(amount)
                    .ok_or(StoreError::Invalid("MONEY_OVERFLOW"))?,
            );
        }
        Ok(totals)
    }
    pub fn ingest_page(&mut self, page: &IngestPage) -> StoreResult<()> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        for f in &page.fills {
            if f.qty <= Decimal::ZERO || f.price <= Decimal::ZERO || f.currency.is_empty() {
                return Err(StoreError::Invalid("INVALID_FILL"));
            }
            f.qty
                .checked_mul(f.price)
                .ok_or(StoreError::Invalid("MONEY_OVERFLOW"))?;
            let inserted=tx.execute("INSERT INTO fills VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11) ON CONFLICT(instrument_id,trade_id,exchange_order_id) DO NOTHING",params![f.fill_id,f.instrument_id,f.trade_id,f.exchange_order_id,f.intent_id,f.time,exact(f.qty),exact(f.price),exact(f.fee),f.currency,f.side])?;
            if inserted == 1 {
                tx.execute(
                    "INSERT INTO fill_projection_pending VALUES(?1,?2)",
                    params![f.fill_id, f.side],
                )?;
            }
            let old:(String,String,String,String)=tx.query_row("SELECT qty,price,fee,currency FROM fills WHERE instrument_id=?1 AND trade_id=?2 AND exchange_order_id=?3",params![f.instrument_id,f.trade_id,f.exchange_order_id],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?)))?;
            if old
                != (
                    exact(f.qty),
                    exact(f.price),
                    exact(f.fee),
                    f.currency.clone(),
                )
            {
                return Err(StoreError::Invalid("IMMUTABLE_FILL_CONFLICT"));
            }
        }
        for b in &page.bills {
            tx.execute(
                "INSERT INTO bills VALUES(?1,?2,?3,?4,?5,?6) ON CONFLICT(bill_id) DO NOTHING",
                params![
                    b.bill_id,
                    b.kind,
                    b.time,
                    exact(b.amount),
                    b.currency,
                    b.economic_event_key
                ],
            )?;
            let old: (String, String, String) = tx.query_row(
                "SELECT kind,amount,currency FROM bills WHERE bill_id=?1",
                [&b.bill_id],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )?;
            if old != (b.kind.clone(), exact(b.amount), b.currency.clone()) {
                return Err(StoreError::Invalid("IMMUTABLE_BILL_CONFLICT"));
            }
        }
        for event in &page.funding_events {
            let completed: bool = tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM operation_receipts WHERE operation_id=?1)",
                [format!("funding:{}", event.event_id)],
                |r| r.get(0),
            )?;
            if !completed {
                tx.execute("INSERT INTO funding_projection_pending VALUES(?1,?2,?3,?4,?5) ON CONFLICT(event_id) DO NOTHING",params![event.event_id,event.instrument_id,event.time,exact(event.amount),event.currency])?;
            }
        }
        for f in &page.cash_flows {
            if f.currency.is_empty() || f.source_ref.is_empty() {
                return Err(StoreError::Invalid("INVALID_CASH_FLOW"));
            }
            tx.execute(
                "INSERT INTO cash_flows VALUES(?1,?2,?3,?4,?5,?6) ON CONFLICT(event_id) DO NOTHING",
                params![
                    f.event_id,
                    f.kind,
                    f.currency,
                    exact(f.amount),
                    f.time,
                    f.source_ref
                ],
            )?;
            let old: (String, String, String) = tx.query_row(
                "SELECT kind,currency,amount FROM cash_flows WHERE event_id=?1",
                [&f.event_id],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )?;
            if old != (f.kind.clone(), f.currency.clone(), exact(f.amount)) {
                return Err(StoreError::Invalid("IMMUTABLE_CASH_FLOW_CONFLICT"));
            }
        }
        for a in &page.allocations {
            tx.execute(
                "INSERT INTO allocations VALUES(?1,?2,?3,?4,?5) ON CONFLICT(event_id) DO NOTHING",
                params![
                    a.event_id,
                    a.instance_id,
                    exact(a.delta),
                    a.currency,
                    a.time
                ],
            )?;
            let old: (String, String, String) = tx.query_row(
                "SELECT instance_id,delta,currency FROM allocations WHERE event_id=?1",
                [&a.event_id],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )?;
            if old != (a.instance_id.clone(), exact(a.delta), a.currency.clone()) {
                return Err(StoreError::Invalid("IMMUTABLE_ALLOCATION_CONFLICT"));
            }
        }
        let mut added_loss = Decimal::ZERO;
        for e in &page.finance {
            if e.component == "INTERNAL_CAPITAL_USD" {
                return Err(StoreError::Invalid(
                    "CAPITAL_WRITE_REQUIRES_AUTHORIZED_OPERATION",
                ));
            }
            if e.currency.is_empty() {
                return Err(StoreError::Invalid("CURRENCY_REQUIRED"));
            }
            let added=tx.execute("INSERT INTO finance_entries VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10) ON CONFLICT(economic_event_key,component,allocation_leg) DO NOTHING",params![e.entry_id,e.economic_event_key,e.component,e.allocation_leg,exact(e.amount),e.currency,e.instance_id,e.source_ref,e.time,e.reverses_entry_id])?;
            if added == 1
                && let Some(instance) = &e.instance_id
            {
                let old:Option<String>=tx.query_row("SELECT amount FROM instance_finance_totals WHERE instance_id=?1 AND currency=?2 AND component=?3",params![instance,e.currency,e.component],|r|r.get(0)).optional()?;
                let total = old
                    .map(decimal)
                    .transpose()?
                    .unwrap_or(Decimal::ZERO)
                    .checked_add(e.amount)
                    .ok_or(StoreError::Invalid("MONEY_OVERFLOW"))?;
                tx.execute("INSERT INTO instance_finance_totals VALUES(?1,?2,?3,?4) ON CONFLICT(instance_id,currency,component) DO UPDATE SET amount=excluded.amount",params![instance,e.currency,e.component,exact(total)])?;
            }
            if added == 1 {
                let prior: Option<String> = tx
                    .query_row(
                        "SELECT amount FROM finance_totals WHERE currency=?1 AND component=?2",
                        params![e.currency, e.component],
                        |r| r.get(0),
                    )
                    .optional()?;
                let total = prior
                    .map(decimal)
                    .transpose()?
                    .unwrap_or(Decimal::ZERO)
                    .checked_add(e.amount)
                    .ok_or(StoreError::Invalid("MONEY_OVERFLOW"))?;
                tx.execute("INSERT INTO finance_totals VALUES(?1,?2,?3) ON CONFLICT(currency,component) DO UPDATE SET amount=excluded.amount",params![e.currency,e.component,exact(total)])?;
            }
            let (entry_id,amount,currency,instance):(String,String,String,Option<String>)=tx.query_row("SELECT entry_id,amount,currency,instance_id FROM finance_entries WHERE economic_event_key=?1 AND component=?2 AND allocation_leg=?3",params![e.economic_event_key,e.component,e.allocation_leg],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?)))?;
            if amount != exact(e.amount) || currency != e.currency || instance != e.instance_id {
                return Err(StoreError::Invalid("CANONICAL_ECONOMIC_EVENT_CONFLICT"));
            }
            tx.execute(
                "INSERT INTO finance_source_links VALUES(?1,?2) ON CONFLICT DO NOTHING",
                params![entry_id, e.source_ref],
            )?;
            // Campaign accounting is conservative and never replenished by profits.
            // USD conversion must be supplied as its own attested component.
            if added == 1 && e.component == "CAMPAIGN_LOSS_USD" {
                if e.currency != "USD" || e.amount < Decimal::ZERO {
                    return Err(StoreError::Invalid("CAMPAIGN_USD_CONVERSION_REQUIRED"));
                }
                added_loss = added_loss
                    .checked_add(e.amount)
                    .ok_or(StoreError::Invalid("MONEY_OVERFLOW"))?;
                let mut scopes = vec!["account".to_string()];
                if let Some(instance) = &e.instance_id {
                    scopes.push(format!("strategy:{instance}"));
                }
                for scope in scopes {
                    let day = crate::risk::utc_day(e.time);
                    let prior: Option<String> = tx
                        .query_row(
                            "SELECT loss_usd FROM daily_losses WHERE scope_id=?1 AND utc_day=?2",
                            params![scope, day],
                            |r| r.get(0),
                        )
                        .optional()?;
                    let total = prior
                        .map(decimal)
                        .transpose()?
                        .unwrap_or(Decimal::ZERO)
                        .checked_add(e.amount)
                        .ok_or(StoreError::Invalid("MONEY_OVERFLOW"))?;
                    tx.execute("INSERT INTO daily_losses VALUES(?1,?2,?3) ON CONFLICT(scope_id,utc_day) DO UPDATE SET loss_usd=excluded.loss_usd",params![scope,day,exact(total)])?;
                }
            }
        }
        if added_loss > Decimal::ZERO {
            let old: String =
                tx.query_row("SELECT campaign_loss FROM account_policy", [], |r| r.get(0))?;
            let total = decimal(old)?
                .checked_add(added_loss)
                .ok_or(StoreError::Invalid("MONEY_OVERFLOW"))?;
            tx.execute("UPDATE account_policy SET campaign_loss=?1", [exact(total)])?;
        }
        for l in &page.lots {
            if l.remaining_qty < Decimal::ZERO
                || l.cost < Decimal::ZERO
                || !matches!(l.side.as_str(), "long" | "short")
            {
                return Err(StoreError::Invalid("INVALID_POSITION_LOT"));
            }
            if let Some(intent) = &l.intent_id {
                let owner:(Option<String>,String)=tx.query_row("SELECT instance_id,instrument_id FROM intents JOIN decisions USING(decision_id) WHERE intent_id=?1",[intent],|r|Ok((r.get(0)?,r.get(1)?)))?;
                if owner != (l.instance_id.clone(), l.instrument_id.clone()) {
                    return Err(StoreError::Invalid("LOT_OWNER_MISMATCH"));
                }
            } else if l.instance_id.is_some() {
                return Err(StoreError::Invalid("EXTERNAL_FILL_MUST_BE_UNATTRIBUTED"));
            }
            let changed=tx.execute("INSERT INTO position_lots VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9) ON CONFLICT(lot_id) DO UPDATE SET remaining_qty=excluded.remaining_qty,cost=excluded.cost WHERE instance_id IS excluded.instance_id AND instrument_id=excluded.instrument_id AND side=excluded.side AND intent_id IS excluded.intent_id AND fill_id IS excluded.fill_id",params![l.lot_id,l.instance_id,l.intent_id,l.fill_id,l.instrument_id,l.side,exact(l.remaining_qty),exact(l.cost),l.opened_at])?;
            if changed != 1 {
                return Err(StoreError::Invalid("LOT_OWNER_MISMATCH"));
            }
        }
        for n in &page.nav {
            if n.scope_id.starts_with("strategy:")
                && n.source_version.starts_with("strategy-reconciled-v1")
            {
                return Err(StoreError::Invalid("STRATEGY_NAV_REQUIRES_RECONCILIATION"));
            }
            tx.execute(
                "INSERT INTO nav_scopes VALUES(?1,?2) ON CONFLICT DO NOTHING",
                params![n.scope_id, n.currency],
            )?;
            if n.mark_source.is_empty() {
                return Err(StoreError::Invalid("NAV_MARK_SOURCE_REQUIRED"));
            }
            tx.execute("INSERT INTO nav_points VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9) ON CONFLICT(scope_id,time,source_version) DO NOTHING",params![n.scope_id,n.time,n.source_version,exact(n.equity),exact(n.pnl),exact(n.flows),n.currency,n.coverage,n.mark_source])?;
            if n.currency == "USD" {
                let adjusted = n
                    .equity
                    .checked_sub(n.flows)
                    .ok_or(StoreError::Invalid("MONEY_OVERFLOW"))?;
                let prior: Option<(String, i64)> = tx
                    .query_row(
                        "SELECT highwater,time FROM nav_watermarks WHERE scope_id=?1",
                        [&n.scope_id],
                        |r| Ok((r.get(0)?, r.get(1)?)),
                    )
                    .optional()?;
                if prior.as_ref().is_none_or(|(_, time)| n.time >= *time) {
                    let peak = prior
                        .map(|(v, _)| decimal(v))
                        .transpose()?
                        .unwrap_or(adjusted)
                        .max(adjusted);
                    tx.execute("INSERT INTO nav_watermarks VALUES(?1,?2,?3,?4,?5) ON CONFLICT(scope_id) DO UPDATE SET highwater=excluded.highwater,adjusted_equity=excluded.adjusted_equity,time=excluded.time,coverage=excluded.coverage",params![n.scope_id,exact(peak),exact(adjusted),n.time,n.coverage])?;
                }
            }
        }
        if let Some(c) = &page.cursor {
            let prior:Option<(i64,i64)>=tx.query_row("SELECT generation,watermark FROM source_cursors WHERE source=?1 AND stream_partition=?2",params![c.source,c.stream_partition],|r|Ok((r.get(0)?,r.get(1)?))).optional()?;
            if prior
                .is_some_and(|(g, w)| c.generation < g || (c.generation == g && c.watermark < w))
            {
                return Err(StoreError::Invalid("SOURCE_CURSOR_REGRESSION"));
            }
            tx.execute("INSERT INTO source_cursors VALUES(?1,?2,?3,?4,?5,?6) ON CONFLICT(source,stream_partition) DO UPDATE SET cursor=excluded.cursor,watermark=excluded.watermark,generation=excluded.generation,coverage_gaps=excluded.coverage_gaps",params![c.source,c.stream_partition,c.cursor,c.watermark,c.generation,c.coverage_gaps])?;
        }
        for id in &page.projected_fill_ids {
            tx.execute("DELETE FROM fill_projection_pending WHERE fill_id=?1", [id])?;
        }
        for id in &page.attributed_funding_ids {
            tx.execute(
                "DELETE FROM funding_projection_pending WHERE event_id=?1",
                [id],
            )?;
            tx.execute("INSERT INTO operation_receipts VALUES(?1,'FUNDING_ATTRIBUTED',?2,'COMMITTED',?3) ON CONFLICT(operation_id) DO NOTHING",params![format!("funding:{id}"),id,wall_time_ms()])?;
        }
        tx.commit()?;
        Ok(())
    }
    pub fn active_lots(&self, instrument: &str) -> StoreResult<Vec<StoredLot>> {
        let mut s=self.connection.prepare("SELECT lot_id,instance_id,intent_id,fill_id,instrument_id,side,remaining_qty,cost,opened_at FROM position_lots WHERE instrument_id=?1 AND remaining_qty<>'0' ORDER BY side,instance_id,opened_at,lot_id")?;
        s.query_map([instrument], |r| {
            Ok((
                StoredLot {
                    lot_id: r.get(0)?,
                    instance_id: r.get(1)?,
                    intent_id: r.get(2)?,
                    fill_id: r.get(3)?,
                    instrument_id: r.get(4)?,
                    side: r.get(5)?,
                    remaining_qty: Decimal::ZERO,
                    cost: Decimal::ZERO,
                    opened_at: r.get(8)?,
                },
                r.get::<_, String>(6)?,
                r.get::<_, String>(7)?,
            ))
        })?
        .map(|v| {
            let (mut l, q, c) = v?;
            l.remaining_qty = decimal(q)?;
            l.cost = decimal(c)?;
            Ok(l)
        })
        .collect()
    }
    pub fn finance_history(
        &self,
        from: i64,
        through: i64,
        after_time: i64,
        after_id: &str,
        limit: usize,
    ) -> StoreResult<Vec<FinanceEntry>> {
        let mut s=self.connection.prepare("SELECT entry_id,economic_event_key,component,allocation_leg,amount,currency,instance_id,source_ref,time,reverses_entry_id FROM finance_entries WHERE time>=?1 AND time<=?2 AND (time,entry_id)>(?3,?4) ORDER BY time,entry_id LIMIT ?5")?;
        s.query_map(
            params![from, through, after_time, after_id, limit.min(1000) as i64],
            |r| {
                Ok((
                    FinanceEntry {
                        entry_id: r.get(0)?,
                        economic_event_key: r.get(1)?,
                        component: r.get(2)?,
                        allocation_leg: r.get(3)?,
                        amount: Decimal::ZERO,
                        currency: r.get(5)?,
                        instance_id: r.get(6)?,
                        source_ref: r.get(7)?,
                        time: r.get(8)?,
                        reverses_entry_id: r.get(9)?,
                    },
                    r.get::<_, String>(4)?,
                ))
            },
        )?
        .map(|v| {
            let (mut e, a) = v?;
            e.amount = decimal(a)?;
            Ok(e)
        })
        .collect()
    }
}
fn finance_filter(
    instances: &[String],
    currencies: &[String],
    offset: usize,
) -> StoreResult<(String, Vec<rusqlite::types::Value>)> {
    if instances.len() > 32 || currencies.len() > 32 {
        return Err(StoreError::Invalid("FINANCE_FILTER_LIMIT"));
    }
    let mut filter = String::new();
    let mut args = Vec::new();
    for (column, items) in [("instance_id", instances), ("currency", currencies)] {
        if items.is_empty() {
            continue;
        }
        let mut placeholders = Vec::new();
        for item in items {
            if item.is_empty() {
                return Err(StoreError::Invalid("FINANCE_FILTER_INVALID"));
            }
            args.push(item.clone().into());
            placeholders.push(format!("?{}", offset + args.len()));
        }
        filter.push_str(&format!(" AND {column} IN({})", placeholders.join(",")));
    }
    Ok((filter, args))
}
