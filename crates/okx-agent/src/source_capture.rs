//! Incremental post-T0 exchange facts. The account worker commits each page with its cursor.
use crate::{
    AgentError, AgentResult, account_bootstrap::AccountBootstrapper,
    execution_runtime::ExecutionRuntime,
};
use okx_api::{AccountBill, FillHistory};
use okx_execution::{
    Bill, CashFlow, Fill, FinanceEntry, FundingEvent, IngestPage, NavPoint, SourceCursor,
    StoreError, StoreResult, UsdConversionEvidence, VenueMark,
};
use okx_runtime::PrivateWsHandle;
use rust_decimal::Decimal;
use sha2::Digest;
use std::{
    str::FromStr,
    time::{Duration, Instant},
};

fn invalid(reason: &str) -> AgentError {
    AgentError::FinancialCaptureTask(reason.into())
}
fn d(s: &str) -> StoreResult<Decimal> {
    Decimal::from_str(s).map_err(|_| StoreError::Invalid("SOURCE_DECIMAL_INVALID"))
}
fn optional_d(s: &str) -> StoreResult<Decimal> {
    if s.is_empty() {
        Ok(Decimal::ZERO)
    } else {
        d(s)
    }
}
fn ts(s: &str) -> StoreResult<i64> {
    s.parse()
        .map_err(|_| StoreError::Invalid("SOURCE_TIMESTAMP_INVALID"))
}
fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(5)
}
#[derive(Clone)]
struct Window {
    completed_through: i64,
    begin: i64,
    end: i64,
    after: String,
}
impl Window {
    fn resume(cursor: Option<&SourceCursor>, t0: i64, now: i64) -> AgentResult<Self> {
        if let Some(c) = cursor
            && !c.cursor.is_empty()
        {
            let p = c.cursor.split('|').collect::<Vec<_>>();
            if p.len() != 3 {
                return Err(invalid("SOURCE_CURSOR_INVALID"));
            }
            return Ok(Self {
                completed_through: c.watermark,
                begin: p[0].parse().map_err(|_| invalid("SOURCE_CURSOR_INVALID"))?,
                end: p[1].parse().map_err(|_| invalid("SOURCE_CURSOR_INVALID"))?,
                after: p[2].into(),
            });
        }
        // One millisecond overlap is intentional; immutable source keys deduplicate boundaries.
        Ok(Self {
            completed_through: cursor.map_or(t0, |c| c.watermark),
            begin: cursor
                .map_or(t0, |c| c.watermark.max(t0))
                .saturating_sub(1)
                .max(t0),
            end: now,
            after: String::new(),
        })
    }
    fn cursor(
        &self,
        source: &str,
        partition: &str,
        generation: i64,
        last_id: Option<&str>,
        complete: bool,
    ) -> StoreResult<SourceCursor> {
        let id = last_id.unwrap_or("");
        if !complete && (id.is_empty() || id == self.after) {
            return Err(StoreError::Invalid("SOURCE_PAGINATION_STALLED"));
        }
        Ok(SourceCursor {
            source: source.into(),
            stream_partition: partition.into(),
            cursor: if complete {
                String::new()
            } else {
                format!("{}|{}|{id}", self.begin, self.end)
            },
            watermark: if complete {
                self.end
            } else {
                self.completed_through
            },
            generation,
            coverage_gaps: if complete {
                String::new()
            } else {
                "BACKFILL_PENDING".into()
            },
        })
    }
}
pub async fn capture_account_sources(
    observer: &AccountBootstrapper,
    private_ws: &PrivateWsHandle,
    execution: &ExecutionRuntime,
) -> AgentResult<okx_observation::AccountSnapshot> {
    let initial = private_ws.status().await;
    if !initial.logged_in || initial.connection_generation == 0 {
        return Err(invalid("PRIVATE_SOURCE_NOT_READY"));
    }
    let snapshot = observer
        .snapshot()
        .await
        .map_err(|_| invalid("ACCOUNT_SOURCE_UNAVAILABLE"))?;
    if snapshot.account_uid_fingerprint != execution.uid()
        || !execution
            .verified_uid_matches(&snapshot.account_uid_fingerprint)
            .await?
    {
        return Err(invalid("ACCOUNT_IDENTITY_MISMATCH"));
    }
    let generation = i64::try_from(initial.connection_generation)
        .map_err(|_| invalid("SOURCE_GENERATION_OVERFLOW"))?;
    let now = chrono::Utc::now().timestamp_millis();
    let fx_source = format!("{:x}", sha2::Sha256::digest(serde_json::to_vec(&snapshot)?));
    let mut usd_per_currency = std::collections::BTreeMap::new();
    usd_per_currency.insert("USD".into(), Decimal::ONE);
    for b in &snapshot.balance.details {
        if let (Ok(equity), Some(usd)) = (Decimal::from_str(&b.equity), b.equity_usd.as_deref())
            && equity > Decimal::ZERO
            && let Ok(usd) = Decimal::from_str(usd)
            && let Some(rate) = usd.checked_div(equity)
            && rate > Decimal::ZERO
        {
            usd_per_currency.insert(b.currency.clone(), rate);
        }
    }
    let fx = UsdConversionEvidence {
        observed_at: now,
        source_digest: fx_source,
        usd_per_currency,
    };
    let t0 = execution
        .store
        .call(deadline(), |s| Ok(s.meta().initialized_at))
        .await?;
    // A bounded read per stream per turn; an unfinished window resumes next turn.
    let (spot, swap, futures, options, bills) = tokio::try_join!(
        capture_fills(execution, private_ws, "SPOT", t0, now, generation, &fx),
        capture_fills(execution, private_ws, "SWAP", t0, now, generation, &fx),
        capture_fills(execution, private_ws, "FUTURES", t0, now, generation, &fx),
        capture_fills(execution, private_ws, "OPTION", t0, now, generation, &fx),
        capture_bills(execution, private_ws, t0, now, generation, &fx)
    )?;
    if private_ws.status().await.connection_generation != initial.connection_generation {
        return Err(invalid("SOURCE_GENERATION_CHANGED"));
    }
    let complete = spot && swap && futures && options && bills;
    let balances = snapshot.balance.details.clone();
    let usd_equity = d(&snapshot.balance.total_equity_usd)?;
    // Validate all supplied marks even though no unproven strategy capital
    // is projected as NAV. Bad venue marks must not silently pass capture.
    let _marks = snapshot
        .positions
        .iter()
        .filter_map(|p| {
            p.mark_price.as_ref().map(|mark| {
                Ok(VenueMark {
                    instrument_id: p.instrument_id.clone(),
                    price: d(mark)?,
                    source_ref: snapshot.source_received_at.clone(),
                })
            })
        })
        .collect::<StoreResult<Vec<_>>>()?;
    let source = snapshot.source_received_at.clone();
    let version = format!("{generation}:{}", snapshot.account_generation);
    execution
        .store
        .call(deadline(), move |s| {
            if complete {
                s.project_pending_fills(1000)?;
                s.attribute_pending_funding(&fx, 100)?;
                s.resolve_stop("usd-conversion-unavailable", &fx.source_digest)?;
            }
            let flows = s.cash_flow_totals()?;
            let mut page = IngestPage::default();
            let external_usd = s.usd_component_total("EXTERNAL_FLOW_USD")?;
            let account_pnl = usd_equity
                .checked_sub(s.opening_equity_usd()?)
                .and_then(|v| v.checked_sub(external_usd))
                .ok_or(StoreError::Invalid("MONEY_OVERFLOW"))?;
            page.nav.push(NavPoint {
                scope_id: "account".into(),
                time: now,
                source_version: version.clone(),
                equity: usd_equity,
                pnl: account_pnl,
                flows: external_usd,
                currency: "USD".into(),
                coverage: if complete { "COMPLETE" } else { "PARTIAL" }.into(),
                mark_source: source.clone(),
            });
            for balance in balances {
                let currency = balance.currency;
                let equity = d(&balance.equity)?;
                let opening = *flows
                    .get(&(currency.clone(), "OPENING".into()))
                    .unwrap_or(&Decimal::ZERO);
                let external = flows
                    .iter()
                    .filter(|((c, k), _)| c == &currency && k != "OPENING")
                    .try_fold(Decimal::ZERO, |a, (_, v)| {
                        a.checked_add(*v)
                            .ok_or(StoreError::Invalid("MONEY_OVERFLOW"))
                    })?;
                let pnl = equity
                    .checked_sub(opening)
                    .and_then(|v| v.checked_sub(external))
                    .ok_or(StoreError::Invalid("MONEY_OVERFLOW"))?;
                page.nav.push(NavPoint {
                    scope_id: format!("account:{currency}"),
                    time: now,
                    source_version: version.clone(),
                    equity,
                    pnl,
                    flows: external,
                    currency,
                    coverage: if complete { "COMPLETE" } else { "PARTIAL" }.into(),
                    mark_source: source.clone(),
                });
            }
            page.cursor = Some(SourceCursor {
                source: "account_snapshot".into(),
                stream_partition: "account".into(),
                cursor: String::new(),
                watermark: now,
                generation,
                coverage_gaps: if complete {
                    String::new()
                } else {
                    "FINANCIAL_BACKFILL_PENDING".into()
                },
            });
            s.ingest_page(&page)?;
            Ok(())
        })
        .await?;
    if !complete {
        return Err(invalid("FINANCIAL_BACKFILL_PENDING"));
    }
    Ok(snapshot)
}
async fn capture_fills(
    execution: &ExecutionRuntime,
    ws: &PrivateWsHandle,
    partition: &str,
    t0: i64,
    now: i64,
    generation: i64,
    fx: &UsdConversionEvidence,
) -> AgentResult<bool> {
    let fx = fx.clone();
    let part = partition.to_string();
    let cursor = execution
        .store
        .call(deadline(), move |s| s.cursor("fills", &part))
        .await?;
    let window = Window::resume(cursor.as_ref(), t0, now)?;
    let rows: Vec<FillHistory> = execution
        .history
        .fills_history_page(
            partition,
            window.begin,
            window.end,
            if window.after.is_empty() {
                None
            } else {
                Some(&window.after)
            },
        )
        .await
        .map_err(|_| {
            invalid(match partition {
                "SPOT" => "SPOT_FILLS_SOURCE_UNAVAILABLE",
                "SWAP" => "SWAP_FILLS_SOURCE_UNAVAILABLE",
                "FUTURES" => "FUTURES_FILLS_SOURCE_UNAVAILABLE",
                "OPTION" => "OPTION_FILLS_SOURCE_UNAVAILABLE",
                _ => "FILL_HISTORY_SOURCE_UNAVAILABLE",
            })
        })?;
    if ws.status().await.connection_generation != generation as u64 {
        return Err(invalid("SOURCE_GENERATION_CHANGED"));
    }
    let complete = rows.len() < 100;
    let next = window.cursor(
        "fills",
        partition,
        generation,
        rows.last().map(|r| r.bill_id.as_str()),
        complete,
    )?;
    execution
        .store
        .call(deadline(), move |s| {
            let mut page = IngestPage {
                cursor: Some(next),
                ..Default::default()
            };
            for r in rows {
                let time = ts(if r.fill_time_ms.is_empty() {
                    &r.timestamp_ms
                } else {
                    &r.fill_time_ms
                })?;
                if time < t0 {
                    continue;
                }
                let owner = s.by_client_order_id(&r.client_order_id)?;
                let instance = owner.as_ref().map(|i| i.plan.strategy_instance.clone());
                let fee = optional_d(&r.fee)?;
                let key = if r.bill_id.is_empty() {
                    format!("fill:{}:{}:{}", r.instrument_id, r.trade_id, r.order_id)
                } else {
                    format!("bill:{}", r.bill_id)
                };
                let id = format!("{}:{}:{}", r.instrument_id, r.trade_id, r.order_id);
                if fee != Decimal::ZERO {
                    page.finance.push(FinanceEntry {
                        entry_id: format!("{key}:fee"),
                        economic_event_key: key,
                        component: "FEE".into(),
                        allocation_leg: "account".into(),
                        amount: fee,
                        currency: r.fee_currency.clone(),
                        instance_id: instance,
                        source_ref: format!("fill:{id}"),
                        time,
                        reverses_entry_id: None,
                    });
                }
                page.fills.push(Fill {
                    fill_id: id,
                    instrument_id: r.instrument_id,
                    trade_id: r.trade_id,
                    exchange_order_id: r.order_id,
                    intent_id: owner.map(|i| i.intent_id),
                    time,
                    qty: d(&r.fill_size)?,
                    price: d(&r.fill_price)?,
                    fee,
                    currency: if r.fee_currency.is_empty() {
                        return Err(StoreError::Invalid("FILL_FEE_CURRENCY_UNAVAILABLE"));
                    } else {
                        r.fee_currency
                    },
                    side: r.side,
                });
            }
            s.ingest_page_with_usd(&page, &fx)
        })
        .await?;
    Ok(complete)
}
async fn capture_bills(
    execution: &ExecutionRuntime,
    ws: &PrivateWsHandle,
    t0: i64,
    now: i64,
    generation: i64,
    fx: &UsdConversionEvidence,
) -> AgentResult<bool> {
    let fx = fx.clone();
    let cursor = execution
        .store
        .call(deadline(), |s| s.cursor("bills", "account"))
        .await?;
    let window = Window::resume(cursor.as_ref(), t0, now)?;
    let rows: Vec<AccountBill> = execution
        .history
        .bills_history_page(
            window.begin,
            window.end,
            if window.after.is_empty() {
                None
            } else {
                Some(&window.after)
            },
        )
        .await
        .map_err(|_| invalid("BILLS_SOURCE_UNAVAILABLE"))?;
    if ws.status().await.connection_generation != generation as u64 {
        return Err(invalid("SOURCE_GENERATION_CHANGED"));
    }
    let complete = rows.len() < 100;
    let next = window.cursor(
        "bills",
        "account",
        generation,
        rows.last().map(|r| r.bill_id.as_str()),
        complete,
    )?;
    execution
        .store
        .call(deadline(), move |s| {
            let mut page = IngestPage {
                cursor: Some(next),
                ..Default::default()
            };
            for b in rows {
                let time = ts(&b.timestamp_ms)?;
                if time < t0 {
                    continue;
                }
                let key = format!("bill:{}", b.bill_id);
                let owner = s
                    .by_client_order_id(&b.client_order_id)?
                    .or(s.by_exchange_order_id(&b.order_id)?);
                let instance = owner.map(|i| i.plan.strategy_instance);
                let amount = optional_d(&b.balance_change)?;
                page.bills.push(Bill {
                    bill_id: b.bill_id.clone(),
                    kind: format!("{}:{}", b.bill_type, b.bill_sub_type),
                    time,
                    amount,
                    currency: b.ccy.clone(),
                    economic_event_key: key.clone(),
                });
                let transfer =
                    b.bill_type == "1" && matches!(b.bill_sub_type.as_str(), "11" | "12");
                if transfer {
                    if !b.instrument_id.is_empty() || !b.order_id.is_empty() && b.order_id != "0" {
                        return Err(StoreError::Invalid("CASH_FLOW_LINKAGE_INVALID"));
                    }
                    page.cash_flows.push(CashFlow {
                        event_id: key.clone(),
                        kind: if amount >= Decimal::ZERO {
                            "TRANSFER_IN"
                        } else {
                            "TRANSFER_OUT"
                        }
                        .into(),
                        currency: b.ccy.clone(),
                        amount,
                        time,
                        source_ref: key.clone(),
                    });
                }
                for (component, value) in [
                    ("FEE", optional_d(&b.fee)?),
                    (
                        if matches!(b.bill_sub_type.as_str(), "173" | "174") {
                            "FUNDING"
                        } else {
                            "REALIZED_PNL"
                        },
                        optional_d(&b.pnl)?,
                    ),
                ] {
                    if value == Decimal::ZERO {
                        continue;
                    }
                    if component == "FUNDING" {
                        page.funding_events.push(FundingEvent {
                            event_id: b.bill_id.clone(),
                            instrument_id: b.instrument_id.clone(),
                            time,
                            amount: value,
                            currency: b.ccy.clone(),
                        });
                        continue;
                    }
                    page.finance.push(FinanceEntry {
                        entry_id: format!("{key}:{component}"),
                        economic_event_key: key.clone(),
                        component: component.into(),
                        allocation_leg: "account".into(),
                        amount: value,
                        currency: b.ccy.clone(),
                        instance_id: instance.clone(),
                        source_ref: key.clone(),
                        time,
                        reverses_entry_id: None,
                    });
                }
                if !transfer && b.pnl.is_empty() && b.fee.is_empty() && amount != Decimal::ZERO {
                    page.finance.push(FinanceEntry {
                        entry_id: format!("{key}:unattributed"),
                        economic_event_key: key.clone(),
                        component: "UNATTRIBUTED".into(),
                        allocation_leg: "account".into(),
                        amount,
                        currency: b.ccy,
                        instance_id: None,
                        source_ref: key,
                        time,
                        reverses_entry_id: None,
                    });
                }
            }
            s.ingest_page_with_usd(&page, &fx)
        })
        .await?;
    Ok(complete)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn source_window_retry_preserves_range_and_overlap() {
        let w = Window {
            completed_through: 10,
            begin: 10,
            end: 100,
            after: String::new(),
        };
        let c = w.cursor("fills", "SWAP", 1, Some("last"), false).unwrap();
        let r = Window::resume(Some(&c), 1, 200).unwrap();
        assert_eq!((r.begin, r.end, r.after.as_str()), (10, 100, "last"));
        let c = r.cursor("fills", "SWAP", 1, None, true).unwrap();
        let next = Window::resume(Some(&c), 1, 200).unwrap();
        assert_eq!(next.begin, 99);
        assert_eq!(next.end, 200);
        let pending = next.cursor("fills", "SWAP", 1, Some("123"), false).unwrap();
        assert_eq!(pending.watermark, 100);
    }
    #[test]
    fn unknown_currency_and_decimal_do_not_become_zero() {
        assert!(d("NaN").is_err());
        assert!(ts("unknown").is_err());
    }
}
