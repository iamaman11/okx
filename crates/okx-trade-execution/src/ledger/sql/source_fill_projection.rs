//! Original fill-to-lot/FIFO projection; all reads share the caller's SQL transaction.
use super::super::*;
impl AccountStore {
    pub(crate) fn project_attributed_fill_sql(
        db: &Connection,
        scope: Option<(&str, &str)>,
        fill: Fill,
        effect: FillEffect,
        cursor: Option<SourceCursor>,
    ) -> StoreResult<IngestPage> {
        use rusqlite::{params_from_iter, types::Value};
        if let Some((installation, account)) = scope {
            Self::verify_source_account_sql(db, installation, account)?;
        }
        let values = |mut original: Vec<Value>| {
            if let Some((installation, account)) = scope {
                original.push(installation.to_owned().into());
                original.push(account.to_owned().into());
            }
            original
        };
        let existing:bool=db.query_row(match scope { None => "SELECT EXISTS(SELECT 1 FROM fills WHERE instrument_id=?1 AND trade_id=?2 AND exchange_order_id=?3)", Some(_) => "SELECT EXISTS(SELECT 1 FROM fills WHERE instrument_id=?1 AND trade_id=?2 AND exchange_order_id=?3 AND installation_id=?4 AND account_key=?5)" },params_from_iter(values(vec![fill.instrument_id.clone().into(),fill.trade_id.clone().into(),fill.exchange_order_id.clone().into()])),|r|r.get(0))?;
        let pending: bool = db.query_row(
            match scope { None => "SELECT EXISTS(SELECT 1 FROM fill_projection_pending WHERE fill_id=?1)", Some(_) => "SELECT EXISTS(SELECT 1 FROM fill_projection_pending WHERE fill_id=?1 AND installation_id=?2 AND account_key=?3)" },
            params_from_iter(values(vec![fill.fill_id.clone().into()])),
            |r| r.get(0),
        )?;
        if existing && !pending {
            return Ok(IngestPage {
                fills: vec![fill],
                cursor,
                ..Default::default()
            });
        }
        let instance: Option<String> = if let Some(id) = &fill.intent_id {
            db.query_row(match scope { None => "SELECT instance_id FROM decisions JOIN intents USING(decision_id) WHERE intent_id=?1", Some(_) => "SELECT d.instance_id FROM decisions d JOIN intents i ON i.installation_id=d.installation_id AND i.account_key=d.account_key AND i.decision_id=d.decision_id WHERE i.intent_id=?1 AND i.installation_id=?2 AND i.account_key=?3" },params_from_iter(values(vec![id.clone().into()])),|r|r.get(0))?
        } else {
            None
        };
        let key = format!(
            "fill:{}:{}:{}",
            fill.instrument_id, fill.trade_id, fill.exchange_order_id
        );
        // Source capture already persisted the authenticated SWAP bill ID
        // before this pending-fill projection. Reuse its economic fee key:
        // projection is not a second fee (or a second owner of the key).
        // The legacy fill key remains for sources without a bill ID; those
        // are not final bill evidence and cannot authorize settlement.
        let fee_event_key = match db
            .query_row(
                match scope { None => "SELECT bill_id FROM swap_fill_sources WHERE fill_id=?1", Some(_) => "SELECT bill_id FROM swap_fill_sources WHERE fill_id=?1 AND installation_id=?2 AND account_key=?3" },
                params_from_iter(values(vec![fill.fill_id.clone().into()])),
                |row| row.get::<_, String>(0),
            )
            .optional()?
        {
            Some(id) if id.is_empty() => None,
            Some(id) => Some(format!("bill:{id}")),
            None => Some(key.clone()),
        };
        let mut page = IngestPage {
            cursor,
            ..Default::default()
        };
        // F7: a raw fill may have committed with fill_projection_pending
        // before its fee/position projection. Rebuild the fee on replay;
        // the canonical economic-event uniqueness prevents double charge.
        if fill.fee != Decimal::ZERO
            && let Some(fee_event_key) = fee_event_key
        {
            page.finance.push(FinanceEntry {
                entry_id: format!("{fee_event_key}:fee"),
                economic_event_key: fee_event_key,
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
                let reduce_plan = Self::plan_sql(
                    db,
                    scope,
                    fill.intent_id
                        .as_deref()
                        .ok_or(StoreError::Invalid("REDUCTION_INTENT_REQUIRED"))?,
                )?;
                let existing_lots = Self::active_lots_sql(db, scope, &fill.instrument_id)?
                    .into_iter()
                    .filter(|l| {
                        l.side == reduce_plan.position_side
                            && l.instance_id.as_deref() == Some(owner)
                    })
                    .collect::<Vec<_>>();
                let mut lots = existing_lots
                    .iter()
                    .map(|l| {
                        let p = Self::plan_sql(
                            db,
                            scope,
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
                // Raw source capture precedes projection. Its existence is
                // not proof that realized PnL was projected. Replays with no
                // pending marker returned above; the economic-event key also
                // prevents a second debit/credit during recovery.
                {
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
        Ok(page)
    }
    pub(crate) fn active_lots_sql(
        db: &Connection,
        scope: Option<(&str, &str)>,
        instrument: &str,
    ) -> StoreResult<Vec<StoredLot>> {
        if let Some((installation, account)) = scope {
            Self::verify_source_account_sql(db, installation, account)?;
        }
        let mut values = vec![rusqlite::types::Value::from(instrument.to_owned())];
        if let Some((installation, account)) = scope {
            values.push(installation.to_owned().into());
            values.push(account.to_owned().into());
        }
        let mut s=db.prepare(match scope {None=>"SELECT lot_id,instance_id,intent_id,fill_id,instrument_id,side,remaining_qty,cost,opened_at FROM position_lots WHERE instrument_id=?1 AND remaining_qty<>'0' ORDER BY side,instance_id,opened_at,lot_id",Some(_)=>"SELECT lot_id,instance_id,intent_id,fill_id,instrument_id,side,remaining_qty,cost,opened_at FROM position_lots WHERE instrument_id=?1 AND installation_id=?2 AND account_key=?3 AND remaining_qty<>'0' ORDER BY side,instance_id,opened_at,lot_id"})?;
        s.query_map(rusqlite::params_from_iter(values), |r| {
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
    pub(crate) fn pending_fill_sources_sql(
        db: &Connection,
        scope: Option<(&str, &str)>,
        limit: usize,
        observe: impl FnOnce(AccountSqlStatementStat),
    ) -> StoreResult<Vec<Fill>> {
        if let Some((installation, account)) = scope {
            Self::verify_source_account_sql(db, installation, account)?;
        }
        let mut values = vec![rusqlite::types::Value::from(limit.min(1000) as i64)];
        if let Some((installation, account)) = scope {
            values.push(installation.to_owned().into());
            values.push(account.to_owned().into());
        }
        let rows = {
            let mut s = db.prepare(match scope {None=>super::finance::PENDING_FILL_PROJECTION_SQL,Some(_)=>"SELECT f.fill_id,f.instrument_id,f.trade_id,f.exchange_order_id,f.intent_id,f.time,f.qty,f.price,f.fee,f.currency,p.side FROM fill_projection_pending p CROSS JOIN fills f ON f.installation_id=p.installation_id AND f.account_key=p.account_key AND f.fill_id=p.fill_id WHERE p.installation_id=?2 AND p.account_key=?3 ORDER BY f.time,f.fill_id LIMIT ?1"})?;
            let started = std::time::Instant::now();
            let result = s
                .query_map(rusqlite::params_from_iter(values), |r| {
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
                })
                .and_then(|items| items.collect::<Result<Vec<_>, _>>());
            observe(AccountSqlStatementStat {
                statement_id: "pending_fills_select",
                elapsed_us: u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX),
                rows: result.as_ref().map(|v| v.len() as u64).unwrap_or(0),
                vm_steps: s.get_status(rusqlite::StatementStatus::VmStep).max(0) as u64,
                fullscan_steps: s.get_status(rusqlite::StatementStatus::FullscanStep).max(0) as u64,
                sorts: s.get_status(rusqlite::StatementStatus::Sort).max(0) as u64,
                succeeded: result.is_ok(),
            });
            result?
        };
        rows.into_iter()
            .map(|(mut fill, q, p, fee)| {
                fill.qty = decimal(q)?;
                fill.price = decimal(p)?;
                fill.fee = decimal(fee)?;
                Ok(fill)
            })
            .collect()
    }
    pub(crate) fn pending_fill_effect_sql(
        db: &Connection,
        scope: Option<(&str, &str)>,
        fill: &Fill,
    ) -> StoreResult<FillEffect> {
        let plan = fill
            .intent_id
            .as_deref()
            .map(|id| Self::plan_sql(db, scope, id))
            .transpose()?;
        Ok(
            if plan
                .as_ref()
                .is_some_and(|p| matches!(p.operation.as_str(), "close" | "reduce"))
            {
                FillEffect::Reduce
            } else {
                FillEffect::Open {
                    side: if fill.side == "buy" { "long" } else { "short" }.into(),
                }
            },
        )
    }
}
