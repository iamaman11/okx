//! Incremental post-T0 exchange facts. The account worker commits each page with its cursor.
use crate::{
    AgentError, AgentResult, account_bootstrap::AccountBootstrapper,
    execution_runtime::ExecutionRuntime,
};
use okx_api::{AccountBill, FillHistory};
use okx_execution::{
    Bill, CashFlow, Fill, FinanceEntry, FundingEvent, IngestPage, NavPoint, SourceCursor,
    StoreError, StoreResult, StrategyValuationEvidence, UsdConversionEvidence, ValuationPosition,
    VenueMark,
};
use okx_runtime::PrivateWsHandle;
use rust_decimal::Decimal;
use sha2::Digest;
use std::{
    str::FromStr,
    sync::Arc,
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
// An absent amount on a real fill or a funding bill is UNKNOWN, not a zero
// commission or zero cashflow. Other bill types explicitly permit empty PnL.
fn required_d(s: &str, reason: &'static str) -> StoreResult<Decimal> {
    if s.is_empty() {
        Err(StoreError::Invalid(reason))
    } else {
        d(s)
    }
}
fn bill_pnl(sub_type: &str, value: &str) -> StoreResult<Decimal> {
    if matches!(sub_type, "173" | "174") {
        required_d(value, "FUNDING_AMOUNT_SOURCE_UNAVAILABLE")
    } else {
        optional_d(value)
    }
}
// OKX bill type 7 is interest. Never infer it from an unrelated cash
// movement or silently classify an ambiguous interest bill as a fee/PnL.
fn residual_bill_component(
    bill_type: &str,
    pnl: &str,
    fee: &str,
) -> StoreResult<Option<&'static str>> {
    if bill_type == "7" {
        // Venue absent fields may be encoded as either "" or "0".
        // Only a *nonzero* separate fee/PnL would double-count interest.
        if optional_d(pnl)? != Decimal::ZERO || optional_d(fee)? != Decimal::ZERO {
            return Err(StoreError::Invalid("INTEREST_BILL_AMBIGUOUS"));
        }
        Ok(Some("INTEREST"))
    } else if pnl.is_empty() && fee.is_empty() {
        Ok(Some("UNATTRIBUTED"))
    } else {
        Ok(None)
    }
}
fn ts(s: &str) -> StoreResult<i64> {
    s.parse()
        .map_err(|_| StoreError::Invalid("SOURCE_TIMESTAMP_INVALID"))
}
fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(5)
}
/// Single-cycle phase timings; source and ledger timestamps stay independent.
#[derive(Clone, Debug, Default)]
pub struct CapturePhaseTimings {
    pub observer_and_cursors_ms: u64,
    pub history_and_ingest_ms: u64,
    pub nav_reconciliation_ms: u64,
    /// Time this NAV job waited in the existing FIFO SQL queue.
    pub nav_sql_queue_wait_ms: u64,
    /// Actual on-owner execution of this NAV job, excluding queue wait.
    pub nav_sql_owner_ms: u64,
}

/// Returned only after the full financial capture succeeds.
pub struct CaptureCompleted {
    pub snapshot: okx_observation::AccountSnapshot,
    pub phases: CapturePhaseTimings,
}

struct CaptureCursors {
    spot: Option<SourceCursor>,
    swap: Option<SourceCursor>,
    futures: Option<SourceCursor>,
    options: Option<SourceCursor>,
    bills: Option<SourceCursor>,
}

/// Preserve the source partition and its own durable cursor as one argument.
struct FillHistorySource {
    partition: &'static str,
    cursor: Option<SourceCursor>,
}

fn phase_elapsed_ms(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX)
}

// Fixed vocabulary: a slow or failed operation identifies its owning phase
// without storing any finance facts, arbitrary SQL text or secret labels.
#[derive(Clone, Copy)]
enum NavPhase {
    FillProjection,
    FundingAttribution,
    StopResolution,
    CashFlows,
    AccountNavBuild,
    AccountNavCommit,
    Capital,
    StrategyValuation,
    CursorRead,
    CursorCommit,
}
impl NavPhase {
    fn code(self) -> &'static str {
        match self {
            Self::FillProjection => "fill_projection",
            Self::FundingAttribution => "funding_attribution",
            Self::StopResolution => "stop_resolution",
            Self::CashFlows => "cash_flows",
            Self::AccountNavBuild => "account_nav_build",
            Self::AccountNavCommit => "account_nav_commit",
            Self::Capital => "capital",
            Self::StrategyValuation => "strategy_valuation",
            Self::CursorRead => "cursor_read",
            Self::CursorCommit => "cursor_commit",
        }
    }
}
fn measured_nav_phase<T>(
    trace: &Option<(Arc<crate::finance_trace::FinanceTrace>,u64)>,
    phase: NavPhase,
    operation: impl FnOnce() -> StoreResult<T>,
) -> StoreResult<T> {
    if let Some((store,id))=trace {store.phase_started(*id,phase.code());}
    let started = Instant::now();
    let result = operation();
    if let Some((store,id))=trace {
        store.phase_finished(*id,phase.code(),
            u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX),result.is_ok());
    }
    let elapsed_ms = phase_elapsed_ms(started);
    if elapsed_ms >= 250 || result.is_err() {
        eprintln!(
            "OKX_NAV_PHASE_V1 phase={} elapsed_ms={} outcome={}",
            phase.code(),
            elapsed_ms,
            if result.is_ok() { "ok" } else { "error" }
        );
    }
    result
}

// With no allocated strategy capital, a prior gap-free cursor for the same
// source generation already proves NOT_APPLICABLE. Rewriting its watermark
// every NAV cycle forces an unnecessary second synchronous SQLite commit.
// A new generation, missing cursor or previous gap must still be persisted.
fn strategy_cursor_refresh_needed(
    no_strategy_capital: bool,
    gap: &str,
    previous: Option<&SourceCursor>,
    generation: i64,
) -> bool {
    !no_strategy_capital
        || !gap.is_empty()
        || previous.is_none_or(|c| c.generation != generation || !c.coverage_gaps.is_empty())
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
/// Stable account-snapshot contract also used by trade recovery.
pub async fn capture_account_sources(
    observer: &AccountBootstrapper,
    private_ws: &PrivateWsHandle,
    execution: &ExecutionRuntime,
) -> AgentResult<okx_observation::AccountSnapshot> {
    let completed = capture_account_sources_measured(observer, private_ws, execution).await?;
    Ok(completed.snapshot)
}

/// Source-loop-only measured readback; no change to the recovery contract.
pub async fn capture_account_sources_measured(
    observer: &AccountBootstrapper,
    private_ws: &PrivateWsHandle,
    execution: &ExecutionRuntime,
) -> AgentResult<CaptureCompleted> {
    capture_account_sources_traced(observer,private_ws,execution,None).await
}
pub(crate) async fn capture_account_sources_traced(
    observer: &AccountBootstrapper,
    private_ws: &PrivateWsHandle,
    execution: &ExecutionRuntime,
    trace: Option<(Arc<crate::finance_trace::FinanceTrace>,u64)>,
) -> AgentResult<CaptureCompleted> {
    let initial = private_ws.status().await;
    if !initial.logged_in || initial.connection_generation == 0 {
        return Err(invalid("PRIVATE_SOURCE_NOT_READY"));
    }
    let observer_and_cursors_started = Instant::now();
    // Read all five history cursors and the opening time in ONE existing
    // AccountStore job, concurrently with the authenticated venue snapshot.
    // No cursor is advanced here; the individual page ingests still own
    // the durable source coverage and failure/replay semantics.
    let (snapshot, (t0, cursors)) = tokio::try_join!(
        async {
            observer
                .snapshot()
                .await
                .map_err(|_| invalid("ACCOUNT_SOURCE_UNAVAILABLE"))
        },
        async {
            execution
                .store
                .call(deadline(), |s| {
                    Ok((
                        s.meta().initialized_at,
                        CaptureCursors {
                            spot: s.cursor("fills", "SPOT")?,
                            swap: s.cursor("fills", "SWAP")?,
                            futures: s.cursor("fills", "FUTURES")?,
                            options: s.cursor("fills", "OPTION")?,
                            bills: s.cursor("bills", "account")?,
                        },
                    ))
                })
                .await
                .map_err(AgentError::from)
        },
    )?;
    let observer_and_cursors_ms = phase_elapsed_ms(observer_and_cursors_started);
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
    // A bounded read per stream per turn; an unfinished window resumes next
    // turn. Venue fetches still run concurrently, and all five durable
    // cursor writes retain their previous ordered AccountStore owner.
    let history_started = Instant::now();
    let (spot, swap, futures, options, bills) = tokio::try_join!(
        capture_fills(
            execution,
            private_ws,
            FillHistorySource {
                partition: "SPOT",
                cursor: cursors.spot,
            },
            t0,
            now,
            generation,
            &fx
        ),
        capture_fills(
            execution,
            private_ws,
            FillHistorySource {
                partition: "SWAP",
                cursor: cursors.swap,
            },
            t0,
            now,
            generation,
            &fx
        ),
        capture_fills(
            execution,
            private_ws,
            FillHistorySource {
                partition: "FUTURES",
                cursor: cursors.futures,
            },
            t0,
            now,
            generation,
            &fx
        ),
        capture_fills(
            execution,
            private_ws,
            FillHistorySource {
                partition: "OPTION",
                cursor: cursors.options,
            },
            t0,
            now,
            generation,
            &fx
        ),
        capture_bills(
            execution,
            private_ws,
            cursors.bills,
            t0,
            now,
            generation,
            &fx
        )
    )?;
    let history_and_ingest_ms = phase_elapsed_ms(history_started);
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
    // Strategy valuation uses the same account-identity-verified snapshot.
    // Missing marks affect strategy coverage, not account/manual NAV.
    let valuation_positions: StoreResult<Vec<ValuationPosition>> = snapshot
        .positions
        .iter()
        .filter_map(|p| match d(&p.position) {
            Ok(n) if n == Decimal::ZERO => None,
            Ok(n) => Some((|| {
                let signed = match p.position_side.as_str() {
                    "net" => n,
                    "long" => n.abs(),
                    "short" => -n.abs(),
                    _ => {
                        return Err(StoreError::Invalid(
                            "STRATEGY_NAV_POSITION_MODE_UNSUPPORTED",
                        ));
                    }
                };
                let mark = p
                    .mark_price
                    .as_deref()
                    .ok_or(StoreError::Invalid("STRATEGY_NAV_MARK_MISSING"))?;
                Ok(ValuationPosition {
                    instrument_id: p.instrument_id.clone(),
                    signed_contracts: signed,
                    mark_price: d(mark)?,
                })
            })()),
            Err(e) => Some(Err(e)),
        })
        .collect();
    let source = snapshot.source_received_at.clone();
    let version = format!("{generation}:{}", snapshot.account_generation);
    let nav_started = Instant::now();
    let nav_trace=trace.clone();
    let owner_trace=trace;
    let ((), nav_sql_timing) = execution
        .store
        .call_with_timing_observed(deadline(), move |s| {
            if complete {
                measured_nav_phase(&nav_trace,NavPhase::FillProjection, || s.project_pending_fills(1000))?;
                measured_nav_phase(&nav_trace,NavPhase::FundingAttribution, || {
                    s.attribute_pending_funding(&fx, 100)
                })?;
                measured_nav_phase(&nav_trace,NavPhase::StopResolution, || {
                    s.resolve_stop("usd-conversion-unavailable", &fx.source_digest)
                })?;
            }
            let flows = measured_nav_phase(&nav_trace,NavPhase::CashFlows, || s.cash_flow_totals())?;
            let page = measured_nav_phase(&nav_trace,NavPhase::AccountNavBuild, || {
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
                Ok(page)
            })?;
            measured_nav_phase(&nav_trace,NavPhase::AccountNavCommit, || s.ingest_page(&page))?;
            // One AccountStore, one canonical NAV owner. Strategy failures
            // are recorded in the existing coverage cursor; manual trading
            // does not inherit a missing standalone strategy NAV.
            let strategy_cursor = SourceCursor {
                source: "strategy_valuation".into(),
                stream_partition: "account".into(),
                cursor: String::new(),
                watermark: now,
                generation,
                coverage_gaps: String::new(),
            };
            let mut strategy_cursor_committed = false;
            let mut no_strategy_capital = false;
            let strategy_result = if !complete {
                Err(StoreError::Invalid("FINANCIAL_BACKFILL_PENDING"))
            } else {
                match measured_nav_phase(&nav_trace,NavPhase::Capital, || s.capital_state(now)) {
                    Ok(capital) if capital.strategy_principal_usd.is_empty() => {
                        no_strategy_capital = true;
                        Ok(())
                    }
                    Ok(_) => valuation_positions.and_then(|venue_positions| {
                        measured_nav_phase(&nav_trace,NavPhase::StrategyValuation, || {
                            s.commit_reconciled_strategy_nav_with_cursor(
                                &StrategyValuationEvidence {
                                    account_key: s.meta().account_key.clone(),
                                    account_nav_source: source.clone(),
                                    as_of: now,
                                    conversion: fx.clone(),
                                    venue_positions,
                                },
                                &strategy_cursor,
                            )
                        })?;
                        strategy_cursor_committed = true;
                        Ok(())
                    }),
                    Err(e) => Err(e),
                }
            };
            let gap = match strategy_result {
                Ok(()) => String::new(),
                Err(StoreError::Invalid(reason)) => reason.into(),
                Err(e) => return Err(e),
            };
            // On success strategy NAV and its cursor are already atomic.
            // On incomplete evidence record only the actual coverage gap.
            if !strategy_cursor_committed {
                let previous = if no_strategy_capital && gap.is_empty() {
                    measured_nav_phase(&nav_trace,NavPhase::CursorRead, || {
                        s.cursor("strategy_valuation", "account")
                    })?
                } else {
                    None
                };
                if strategy_cursor_refresh_needed(
                    no_strategy_capital,
                    &gap,
                    previous.as_ref(),
                    generation,
                ) {
                    measured_nav_phase(&nav_trace,NavPhase::CursorCommit, || {
                        s.ingest_page(&IngestPage {
                            cursor: Some(SourceCursor {
                                coverage_gaps: gap,
                                ..strategy_cursor
                            }),
                            ..Default::default()
                        })
                    })?;
                }
            }
            Ok(())
        },move |timing,_ok|{
            if let Some((store,id))=owner_trace.as_ref(){store.owner_finished(*id,timing.queue_wait_ms,timing.sql_owner_ms);}
        })
        .await?;
    let nav_reconciliation_ms = phase_elapsed_ms(nav_started);
    if !complete {
        return Err(invalid("FINANCIAL_BACKFILL_PENDING"));
    }
    Ok(CaptureCompleted {
        snapshot,
        phases: CapturePhaseTimings {
            observer_and_cursors_ms,
            history_and_ingest_ms,
            nav_reconciliation_ms,
            nav_sql_queue_wait_ms: nav_sql_timing.queue_wait_ms,
            nav_sql_owner_ms: nav_sql_timing.sql_owner_ms,
        },
    })
}
async fn capture_fills(
    execution: &ExecutionRuntime,
    ws: &PrivateWsHandle,
    source: FillHistorySource,
    t0: i64,
    now: i64,
    generation: i64,
    fx: &UsdConversionEvidence,
) -> AgentResult<bool> {
    let fx = fx.clone();
    let partition = source.partition;
    let window = Window::resume(source.cursor.as_ref(), t0, now)?;
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
                let fee = required_d(&r.fee, "FILL_FEE_SOURCE_UNAVAILABLE")?;
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
    cursor: Option<SourceCursor>,
    t0: i64,
    now: i64,
    generation: i64,
    fx: &UsdConversionEvidence,
) -> AgentResult<bool> {
    let fx = fx.clone();
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
                let residual_component = residual_bill_component(&b.bill_type, &b.pnl, &b.fee)?;
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
                        bill_pnl(&b.bill_sub_type, &b.pnl)?,
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
                if !transfer
                    && amount != Decimal::ZERO
                    && let Some(component) = residual_component
                {
                    page.finance.push(FinanceEntry {
                        entry_id: format!("{key}:unattributed"),
                        economic_event_key: key.clone(),
                        component: component.into(),
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
    fn five_independent_cursors_keep_unfinished_pages_and_reject_corruption() {
        let earlier = Window {
            completed_through: 55,
            begin: 55,
            end: 100,
            after: String::new(),
        };
        let future_page = earlier
            .cursor("fills", "FUTURES", 7, Some("fill-page"), false)
            .unwrap();
        let bill_page = Window {
            completed_through: 88,
            begin: 88,
            end: 100,
            after: String::new(),
        }
        .cursor("bills", "account", 7, Some("bill-page"), false)
        .unwrap();
        let cursors = CaptureCursors {
            spot: None,
            swap: None,
            futures: Some(future_page),
            options: None,
            bills: Some(bill_page),
        };
        let spot = Window::resume(cursors.spot.as_ref(), 1, 200).unwrap();
        let futures = Window::resume(cursors.futures.as_ref(), 1, 200).unwrap();
        let bills = Window::resume(cursors.bills.as_ref(), 1, 200).unwrap();
        assert_eq!(spot.begin, 1);
        assert_eq!(
            (futures.begin, futures.end, futures.after.as_str()),
            (55, 100, "fill-page")
        );
        assert_eq!(
            (bills.begin, bills.end, bills.after.as_str()),
            (88, 100, "bill-page")
        );
        assert!(earlier.cursor("fills", "FUTURES", 7, None, false).is_err());
        let corrupt = SourceCursor {
            source: "fills".into(),
            stream_partition: "FUTURES".into(),
            cursor: "not-a-window".into(),
            watermark: 55,
            generation: 7,
            coverage_gaps: "BACKFILL_PENDING".into(),
        };
        assert!(Window::resume(Some(&corrupt), 1, 200).is_err());
    }

    #[test]
    fn unallocated_strategy_valuation_avoids_repeated_durable_cursor_commits() {
        let current = SourceCursor {
            source: "strategy_valuation".into(),
            stream_partition: "account".into(),
            cursor: String::new(),
            watermark: 100,
            generation: 7,
            coverage_gaps: String::new(),
        };
        // After the first accepted no-capital cycle, no second write is
        // needed for subsequent account-only NAV snapshots.
        assert!(!strategy_cursor_refresh_needed(true, "", Some(&current), 7));
        // A new generation, initial state or prior gap must be recorded.
        assert!(strategy_cursor_refresh_needed(true, "", None, 7));
        assert!(strategy_cursor_refresh_needed(true, "", Some(&current), 8));
        let with_gap = SourceCursor {
            coverage_gaps: "STRATEGY_NAV_MARK_MISSING".into(),
            ..current.clone()
        };
        assert!(strategy_cursor_refresh_needed(true, "", Some(&with_gap), 7));
        // Incomplete source and genuine strategy capital never skip their
        // fail-closed cursor; strategy NAV reconciliation remains unchanged.
        assert!(strategy_cursor_refresh_needed(
            true,
            "FINANCIAL_BACKFILL_PENDING",
            Some(&current),
            7
        ));
        assert!(strategy_cursor_refresh_needed(false, "", Some(&current), 7));
    }

    #[test]
    fn unknown_currency_and_decimal_do_not_become_zero() {
        assert!(d("NaN").is_err());
        assert!(ts("unknown").is_err());
    }

    #[test]
    fn interest_bill_has_one_explicit_component_and_ambiguous_sources_fail_closed() {
        assert_eq!(
            residual_bill_component("7", "", "").unwrap(),
            Some("INTEREST")
        );
        assert_eq!(
            residual_bill_component("7", "0", "0").unwrap(),
            Some("INTEREST")
        );
        assert!(matches!(
            residual_bill_component("7", "", "-0.01"),
            Err(StoreError::Invalid("INTEREST_BILL_AMBIGUOUS"))
        ));
        assert!(matches!(
            residual_bill_component("7", "0.01", ""),
            Err(StoreError::Invalid("INTEREST_BILL_AMBIGUOUS"))
        ));
        assert_eq!(
            residual_bill_component("1", "", "").unwrap(),
            Some("UNATTRIBUTED")
        );
        assert_eq!(residual_bill_component("8", "0", "0").unwrap(), None);
    }

    #[test]
    fn trade_fee_and_funding_are_required_economic_facts() {
        assert!(matches!(
            required_d("", "FILL_FEE_SOURCE_UNAVAILABLE"),
            Err(StoreError::Invalid("FILL_FEE_SOURCE_UNAVAILABLE"))
        ));
        assert_eq!(
            required_d("0", "FILL_FEE_SOURCE_UNAVAILABLE").unwrap(),
            Decimal::ZERO
        );
        assert_eq!(
            required_d("-0.001", "FILL_FEE_SOURCE_UNAVAILABLE").unwrap(),
            Decimal::new(-1, 3)
        );
        for subtype in ["173", "174"] {
            assert!(matches!(
                bill_pnl(subtype, ""),
                Err(StoreError::Invalid("FUNDING_AMOUNT_SOURCE_UNAVAILABLE"))
            ));
            assert_eq!(bill_pnl(subtype, "0").unwrap(), Decimal::ZERO);
            assert_eq!(bill_pnl(subtype, "-0.05").unwrap(), Decimal::new(-5, 2));
        }
        // A transfer or non-funding bill may legitimately omit PnL.
        assert_eq!(bill_pnl("11", "").unwrap(), Decimal::ZERO);
        assert_eq!(bill_pnl("12", "").unwrap(), Decimal::ZERO);
    }
}
