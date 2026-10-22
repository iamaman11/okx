use super::super::*;
impl AccountStore {
    pub fn ingest_page_with_usd(
        &mut self,
        page: &IngestPage,
        fx: &UsdConversionEvidence,
    ) -> StoreResult<()> {
        if fx.source_digest.is_empty() || fx.observed_at <= 0 {
            return Err(StoreError::Invalid("USD_CONVERSION_SOURCE_UNAVAILABLE"));
        }
        let mut normalized = page.clone();
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
        if !existing && fill.fee != Decimal::ZERO {
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
                let existing_lots = self.active_lots(&fill.instrument_id)?;
                let mut lots = existing_lots
                    .iter()
                    .map(|l| crate::finance::PositionLot {
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
                    .collect::<Vec<_>>();
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
                    if let Some(updated) = lots.iter().find(|v| v.lot_id == l.lot_id) {
                        if updated.signed_qty.abs() != l.remaining_qty {
                            page.lots.push(StoredLot {
                                remaining_qty: updated.signed_qty.abs(),
                                ..l
                            });
                        }
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
            if e.currency.is_empty() {
                return Err(StoreError::Invalid("CURRENCY_REQUIRED"));
            }
            let added=tx.execute("INSERT INTO finance_entries VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10) ON CONFLICT(economic_event_key,component,allocation_leg) DO NOTHING",params![e.entry_id,e.economic_event_key,e.component,e.allocation_leg,exact(e.amount),e.currency,e.instance_id,e.source_ref,e.time,e.reverses_entry_id])?;
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
            if n.mark_source.is_empty() {
                return Err(StoreError::Invalid("NAV_MARK_SOURCE_REQUIRED"));
            }
            tx.execute("INSERT INTO nav_points VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9) ON CONFLICT(scope_id,time,source_version) DO NOTHING",params![n.scope_id,n.time,n.source_version,exact(n.equity),exact(n.pnl),exact(n.flows),n.currency,n.coverage,n.mark_source])?;
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
