//! Pure, fail-closed strategy financial projection from the SINGLE persisted execution ledger.
//! Never assign shared account equity, foreign fills or ambiguous funding to a run.
use std::{
    collections::{BTreeMap, BTreeSet},
    str::FromStr,
};

use okx_observation::AccountLedgerFacts;
use rust_decimal::Decimal;
use serde::Serialize;
use serde_json::Value;

use crate::{
    DurableExchangeEvidence, ExecutionAction, ExecutionLedgerEntry, ExecutionLedgerError,
    StrategyCapitalReservation,
    account_cashflow::{AccountBillClass, AccountBillSource, classify_native_account_bill},
};

pub const STRATEGY_FINANCIAL_PERIOD_SCHEMA_V1: &str = "okx.strategy-financial-period/v1";

/// Production-public WS mark supplied by the single observation owner.
/// Demo/private account marks and UPL must NEVER be used as strategy signal NAV.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProductionMarkEvidence {
    pub price: String,
    pub exchange_timestamp_ms: u64,
    pub received_at_ms: u64,
}

/// The ONE exact daily cash-debit calculation used by native SQLite and
/// sealed v14. Callers independently prove bill-to-intent ownership; this
/// pure accumulator handles currency, timestamps, Decimal, completeness and
/// the FLAT capital-to-NAV lower bound identically on both storage paths.
#[derive(Default)]
pub(crate) struct DailyStrategyLossAccumulator {
    linked: usize,
    fees: Decimal,
    realized: Decimal,
    gross_loss: Decimal,
    source_ahead: bool,
}

impl DailyStrategyLossAccumulator {
    pub(crate) fn accept(
        &mut self,
        payload: &Value,
        run: &StrategyCapitalReservation,
        day: u64,
        as_of: u64,
    ) -> Result<(), ExecutionLedgerError> {
        if payload["ccy"] != "USDT" {
            return Err(ExecutionLedgerError::ExchangeSourceIncomplete);
        }
        let fee = payload["fee"]
            .as_str()
            .and_then(|v| Decimal::from_str(v).ok())
            .ok_or(ExecutionLedgerError::ExchangeSourceIncomplete)?;
        let pnl = payload["pnl"]
            .as_str()
            .and_then(|v| Decimal::from_str(v).ok())
            .ok_or(ExecutionLedgerError::ExchangeSourceIncomplete)?;
        let ts = payload["ts"]
            .as_u64()
            .ok_or(ExecutionLedgerError::ExchangeSourceIncomplete)?;
        if ts < run.started_at_ms {
            return Err(ExecutionLedgerError::ExchangeSourceIncomplete);
        }
        // Out-of-order REST windows cannot certify a day before the
        // immutable source's latest related bill was actually received.
        if ts > as_of {
            self.source_ahead = true;
            return Ok(());
        }
        self.linked = self
            .linked
            .checked_add(1)
            .ok_or(ExecutionLedgerError::ExchangeEvidenceCapacity)?;
        self.fees += fee;
        self.realized += pnl;
        if ts >= day {
            if fee < Decimal::ZERO {
                self.gross_loss -= fee;
            }
            if pnl < Decimal::ZERO {
                self.gross_loss -= pnl;
            }
        }
        Ok(())
    }

    pub(crate) fn finish(
        self,
        run: &StrategyCapitalReservation,
        report: &StrategyFinancialPeriod,
        day: u64,
    ) -> Result<Option<Decimal>, ExecutionLedgerError> {
        if self.source_ahead {
            return Ok(None);
        }
        if self.linked != report.linked_bills
            || Decimal::from_str(&report.fees_usdt).ok() != Some(self.fees)
            || Decimal::from_str(&report.realized_usdt).ok() != Some(self.realized)
        {
            return Err(ExecutionLedgerError::ExchangeSourceIncomplete);
        }
        if run.started_at_ms >= day && report.state == "FLAT_RECONCILED" {
            let allocation = Decimal::from_str(&run.allocated_usdt)
                .map_err(|_| ExecutionLedgerError::CapitalReservationUnattested)?;
            let nav = Decimal::from_str(
                report
                    .nav_usdt
                    .as_deref()
                    .ok_or(ExecutionLedgerError::ExchangeSourceIncomplete)?,
            )
            .map_err(|_| ExecutionLedgerError::ExchangeSourceIncomplete)?;
            if self.gross_loss < (allocation - nav).max(Decimal::ZERO) {
                return Err(ExecutionLedgerError::ExchangeSourceIncomplete);
            }
        }
        Ok(Some(self.gross_loss))
    }
}

const MAX_STRATEGY_MARK_AGE_MS: u64 = 30_000;
const MAX_STRATEGY_MARK_CLOCK_SKEW_MS: u64 = 2_000;

fn attested_production_mark(
    mark: &ProductionMarkEvidence,
    account_as_of_ms: u64,
) -> Option<Decimal> {
    if mark.exchange_timestamp_ms == 0
        || mark.received_at_ms == 0
        || mark.exchange_timestamp_ms
            > mark
                .received_at_ms
                .saturating_add(MAX_STRATEGY_MARK_CLOCK_SKEW_MS)
        || mark
            .received_at_ms
            .saturating_sub(mark.exchange_timestamp_ms)
            > MAX_STRATEGY_MARK_AGE_MS
        || account_as_of_ms.abs_diff(mark.received_at_ms) > MAX_STRATEGY_MARK_AGE_MS
        // The 30s budget is end-to-end. Two individually fresh hops
        // (exchange -> Windows -> account) must not compose into a
        // nearly 60-second-old strategy mark.
        || account_as_of_ms.abs_diff(mark.exchange_timestamp_ms) > MAX_STRATEGY_MARK_AGE_MS
    {
        return None;
    }
    Decimal::from_str(&mark.price)
        .ok()
        .filter(|price| *price > Decimal::ZERO)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RunOwnedPosition {
    pub instrument_id: String,
    pub position_side: String,
    pub contracts: String,
    pub mark_price: Option<String>,
    pub unrealized_usdt: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct StrategyFinancialPeriod {
    pub schema: &'static str,
    pub run_id: String,
    pub state: &'static str,
    pub evidence_sha256: String,
    pub allocation_usdt: String,
    pub linked_fills: usize,
    pub linked_bills: usize,
    pub realized_usdt: String,
    pub fees_usdt: String,
    pub funding_usdt: Option<String>,
    pub unrealized_usdt: Option<String>,
    pub nav_usdt: Option<String>,
    pub positions: Vec<RunOwnedPosition>,
    pub usable_for_new_risk: bool,
    pub reasons: Vec<&'static str>,
}

/// Single financial source read contract for authenticated legacy evidence and SQLite.
/// Proof of a sorted, authenticated source timeline. Retains only the
/// first/last receipt and largest gap, not the entire historical receipt list.
/// The same rule is used for in-memory test evidence and native SQL cursors.
#[derive(Debug, Clone, Default)]
pub(crate) struct SourceReceiptCoverage {
    first: Option<u64>,
    last: Option<u64>,
    largest_gap_ms: u64,
    out_of_order: bool,
}

impl SourceReceiptCoverage {
    pub(crate) fn observe(&mut self, receipt: u64) {
        if let Some(last) = self.last {
            if receipt < last {
                self.out_of_order = true;
            } else {
                self.largest_gap_ms = self.largest_gap_ms.max(receipt - last);
            }
        } else {
            self.first = Some(receipt);
        }
        self.last = Some(receipt);
    }

    pub(crate) fn latest(&self) -> Option<u64> {
        self.last
    }

    pub(crate) fn attests(&self, run_started_at_ms: u64, current_as_of: Option<u64>) -> bool {
        let (Some(first), Some(last), Some(current)) =
            (self.first, self.last, current_as_of)
        else {
            return false;
        };
        let window = crate::exchange_evidence::MAX_SOURCE_COVERAGE_GAP_MS;
        !self.out_of_order
            && first >= run_started_at_ms
            && first - run_started_at_ms <= window
            && self.largest_gap_ms <= window
            && current >= last
            && current - last <= window
    }
}

#[derive(Debug, Clone)]
pub(crate) struct FinanceSourceAudit {
    pub uid: String,
    pub run_id: String,
    pub digest: String,
    pub coverage: SourceReceiptCoverage,
    pub truncated: bool,
}
pub(crate) trait FinanceSourceReader {
    fn audit(&self) -> Result<FinanceSourceAudit, ExecutionLedgerError>;
    fn visit_fills(
        &self,
        visit: &mut dyn FnMut(
            &crate::exchange_evidence::StoredExchangeEvent,
        ) -> Result<(), ExecutionLedgerError>,
    ) -> Result<(), ExecutionLedgerError>;
    fn bill(
        &self,
        key: &str,
    ) -> Result<Option<crate::exchange_evidence::StoredExchangeEvent>, ExecutionLedgerError>;
    fn visit_bills(
        &self,
        visitor: &mut dyn FnMut(
            &str,
            &crate::exchange_evidence::StoredExchangeEvent,
        ) -> Result<(), ExecutionLedgerError>,
    ) -> Result<(), ExecutionLedgerError>;
    fn entry(
        &self,
        id: &str,
        entries: &[ExecutionLedgerEntry],
    ) -> Result<Option<ExecutionLedgerEntry>, ExecutionLedgerError>;
    fn exact_order(
        &self,
        order: &str,
        client: &str,
        inst: &str,
        intent: &str,
    ) -> Result<bool, ExecutionLedgerError>;
    fn validate_entry(
        &self,
        entry: &ExecutionLedgerEntry,
        since: u64,
        reasons: &mut BTreeSet<&'static str>,
    ) -> Result<(), ExecutionLedgerError>;
}
struct LegacyFinanceReader<'a> {
    source: &'a DurableExchangeEvidence,
    index: &'a SourceOrderIndex<'a>,
}
impl FinanceSourceReader for LegacyFinanceReader<'_> {
    fn audit(&self) -> Result<FinanceSourceAudit, ExecutionLedgerError> {
        self.source.validate()?;
        let receipts = self
            .source
            .cursors
            .values()
            .map(|c| {
                crate::exchange_evidence::source_receipt_ms(&c.source_received_at)
                    .ok_or(ExecutionLedgerError::ExchangeSourceIncomplete)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let mut receipts = receipts;
        receipts.sort_unstable();
        let mut coverage = SourceReceiptCoverage::default();
        for receipt in receipts {
            coverage.observe(receipt);
        }
        Ok(FinanceSourceAudit {
            uid: self.source.account_uid.clone(),
            run_id: self.source.run_id.clone(),
            digest: self.source.checksum_sha256.clone(),
            coverage,
            truncated: self
                .source
                .cursors
                .values()
                .any(|c| !c.order_complete || !c.fill_complete || !c.bill_complete),
        })
    }
    fn visit_fills(
        &self,
        visit: &mut dyn FnMut(
            &crate::exchange_evidence::StoredExchangeEvent,
        ) -> Result<(), ExecutionLedgerError>,
    ) -> Result<(), ExecutionLedgerError> {
        let mut fills = self
            .source
            .events
            .values()
            .filter(|e| e.payload["source"] == "OKX_REST_FILL")
            .collect::<Vec<_>>();
        fills.sort_by(|a, b| {
            timestamp(&a.payload, "ts")
                .cmp(&timestamp(&b.payload, "ts"))
                .then(string(&a.payload, "instId").cmp(&string(&b.payload, "instId")))
                .then(string(&a.payload, "tradeId").cmp(&string(&b.payload, "tradeId")))
        });
        for fill in fills {
            visit(fill)?;
        }
        Ok(())
    }
    fn bill(
        &self,
        key: &str,
    ) -> Result<Option<crate::exchange_evidence::StoredExchangeEvent>, ExecutionLedgerError> {
        Ok(self.source.events.get(key).cloned())
    }
    fn visit_bills(
        &self,
        visitor: &mut dyn FnMut(
            &str,
            &crate::exchange_evidence::StoredExchangeEvent,
        ) -> Result<(), ExecutionLedgerError>,
    ) -> Result<(), ExecutionLedgerError> {
        for (key, event) in &self.source.events {
            if key.starts_with("bill:") {
                visitor(key, event)?;
            }
        }
        Ok(())
    }
    fn entry(
        &self,
        id: &str,
        entries: &[ExecutionLedgerEntry],
    ) -> Result<Option<ExecutionLedgerEntry>, ExecutionLedgerError> {
        Ok(entries
            .iter()
            .find(|entry| entry.record.plan.intent_id == id)
            .cloned())
    }
    fn exact_order(
        &self,
        order: &str,
        client: &str,
        inst: &str,
        intent: &str,
    ) -> Result<bool, ExecutionLedgerError> {
        Ok(self.index.contains_exact_order(order, client, inst, intent))
    }
    fn validate_entry(
        &self,
        entry: &ExecutionLedgerEntry,
        since: u64,
        reasons: &mut BTreeSet<&'static str>,
    ) -> Result<(), ExecutionLedgerError> {
        self.index.validate_entry(entry, since, reasons);
        Ok(())
    }
}
#[derive(Clone)]
struct Lot {
    contracts: Decimal,
    price: Decimal,
    contract_value: Decimal,
}

struct FilledLeg<'a> {
    source: &'a crate::exchange_evidence::StoredExchangeEvent,
    ts: u64,
    inst: String,
    pos: String,
    side: String,
    ord: String,
    client: String,
    trade: String,
    bill: String,
    size: Decimal,
    price: Decimal,
}

fn string<'a>(payload: &'a Value, key: &str) -> Option<&'a str> {
    payload.get(key)?.as_str().filter(|v| !v.is_empty())
}

fn amount(payload: &Value, key: &str) -> Option<Decimal> {
    Decimal::from_str(string(payload, key)?).ok()
}

fn timestamp(payload: &Value, key: &str) -> Option<u64> {
    payload.get(key)?.as_u64()
}

fn terminal_native_coverage(
    state: &str,
    expected: Decimal,
    native_size: Decimal,
    accumulated: Decimal,
    fills: &[Option<Decimal>],
    amended: bool,
) -> bool {
    !amended
        && native_size > Decimal::ZERO
        && expected == native_size
        && accumulated >= Decimal::ZERO
        && accumulated <= native_size
        && fills.iter().all(|f| f.is_some_and(|v| v > Decimal::ZERO))
        && fills.iter().flatten().copied().sum::<Decimal>() == accumulated
        && match state {
            "filled" => accumulated == native_size,
            "canceled" | "mmp_canceled" => true,
            _ => false,
        }
}

/// The last order update must contain one stable native quantity pair.
fn native_terminal_quantities(
    orders: &[&crate::exchange_evidence::StoredExchangeEvent],
) -> Option<(Decimal, Decimal)> {
    let latest = orders
        .iter()
        .filter_map(|e| string(&e.payload, "uTime").and_then(|v| v.parse::<u64>().ok()))
        .max()?;
    let mut quantities: Option<(Decimal, Decimal)> = None;
    for order in orders {
        if string(&order.payload, "uTime")?.parse::<u64>().ok()? == latest {
            let pair = (
                amount(&order.payload, "sz")?,
                amount(&order.payload, "accFillSz")?,
            );
            if quantities.is_some_and(|earlier| earlier != pair) {
                return None;
            }
            quantities = Some(pair);
        }
    }
    quantities
}

/// A terminal exchange order is never financial proof by itself. For FULL fills,
/// source IDs and the sum of native fills must cover exactly the submitted size.
/// Amend/cancel and unresolved exchange states cannot claim exact period closure
/// from a source order row lacking native accFillSz.
/// Use the last exact timestamped venue state. Older LIVE transitions are
/// normal evidence and must not invalidate a later FILLED acknowledgement.
fn latest_order_state(orders: &[&crate::exchange_evidence::StoredExchangeEvent]) -> Option<String> {
    let mut latest: Option<(u64, String)> = None;
    for order in orders {
        let time = string(&order.payload, "uTime")?.parse::<u64>().ok()?;
        let state = string(&order.payload, "state")?.to_owned();
        match &latest {
            Some((previous, _)) if time < *previous => {}
            Some((previous, prior_state)) if time == *previous => {
                if state != *prior_state {
                    return None;
                }
            }
            _ => latest = Some((time, state)),
        }
    }
    latest.map(|(_, state)| state)
}

/// One source-bounded lookup index for original orders and fills. This
/// replaces a quadratic search during million-intent SQL attestation.
pub(crate) struct SourceOrderIndex<'a> {
    orders: BTreeMap<&'a str, Vec<&'a crate::exchange_evidence::StoredExchangeEvent>>,
    fills: BTreeMap<&'a str, Vec<&'a crate::exchange_evidence::StoredExchangeEvent>>,
}

impl<'a> SourceOrderIndex<'a> {
    pub(crate) fn new(evidence: &'a DurableExchangeEvidence) -> Self {
        let mut orders = BTreeMap::new();
        let mut fills = BTreeMap::new();
        for event in evidence.events.values() {
            let Some(id) = string(&event.payload, "ordId") else {
                continue;
            };
            match string(&event.payload, "source") {
                Some("OKX_REST_ORDER") => {
                    orders.entry(id).or_insert_with(Vec::new).push(event);
                }
                Some("OKX_REST_FILL") => {
                    fills.entry(id).or_insert_with(Vec::new).push(event);
                }
                _ => {}
            }
        }
        Self { orders, fills }
    }

    /// Exact read from the same source index used for terminal native order
    /// coverage. Previous finance performed a full event-map scan for each
    /// fill; both lookups now share one immutable index.
    fn contains_exact_order(
        &self,
        order_id: &str,
        client_id: &str,
        instrument_id: &str,
        intent_id: &str,
    ) -> bool {
        self.orders.get(order_id).is_some_and(|history| {
            history.iter().any(|event| {
                string(&event.payload, "clOrdId") == Some(client_id)
                    && string(&event.payload, "instId") == Some(instrument_id)
                    && event.source_intent_id.as_deref() == Some(intent_id)
            })
        })
    }

    pub(crate) fn validate_entry(
        &self,
        entry: &ExecutionLedgerEntry,
        run_started_at_ms: u64,
        reasons: &mut BTreeSet<&'static str>,
    ) {
        if entry.created_at_ms <= run_started_at_ms {
            return;
        }
        let record = &entry.record;
        let Some(order_id) = record.order_id.as_deref() else {
            return;
        };
        let orders = self
            .orders
            .get(order_id)
            .into_iter()
            .flatten()
            .copied()
            .filter(|event| {
                string(&event.payload, "clOrdId") == Some(record.plan.client_order_id.as_str())
                    && string(&event.payload, "instId") == Some(record.plan.instrument_id.as_str())
                    && event.source_intent_id.as_deref() == Some(record.plan.intent_id.as_str())
            })
            .collect::<Vec<_>>();
        if orders.is_empty() {
            reasons.insert("managed_exchange_order_missing");
            return;
        }
        let Some(last_state) = latest_order_state(&orders) else {
            reasons.insert("managed_order_terminal_state_unproven");
            return;
        };
        if !matches!(last_state.as_str(), "filled" | "canceled" | "mmp_canceled")
            || !record.state.is_terminal()
        {
            reasons.insert("managed_order_terminal_state_unproven");
            return;
        }
        let fills = self
            .fills
            .get(order_id)
            .into_iter()
            .flatten()
            .copied()
            .filter(|event| {
                string(&event.payload, "clOrdId") == Some(record.plan.client_order_id.as_str())
                    && string(&event.payload, "instId") == Some(record.plan.instrument_id.as_str())
                    && event.source_intent_id.as_deref() == Some(record.plan.intent_id.as_str())
            })
            .collect::<Vec<_>>();
        let Some(expected) = Decimal::from_str(&record.plan.size).ok() else {
            reasons.insert("invalid_managed_order_size");
            return;
        };
        let Some((exchange_size, accumulated)) = native_terminal_quantities(&orders) else {
            reasons.insert("native_order_accumulated_fill_missing_or_conflicting");
            return;
        };
        let parts = fills
            .iter()
            .map(|f| amount(&f.payload, "fillSz"))
            .collect::<Vec<_>>();
        // Native final accFillSz proves zero or partial fills before cancel.
        // An amendment still requires independently proved final size lineage.
        if !terminal_native_coverage(
            &last_state,
            expected,
            exchange_size,
            accumulated,
            &parts,
            !record.mutations.is_empty(),
        ) {
            reasons.insert("terminal_order_native_fill_coverage_not_proven");
        }
    }
}

struct LotChange<'a> {
    inst: &'a str,
    pos: &'a str,
    side: &'a str,
    action: ExecutionAction,
    size: Decimal,
    price: Decimal,
    ct: Decimal,
}

fn put_lot(
    lots: &mut BTreeMap<(String, String), Vec<Lot>>,
    change: LotChange<'_>,
) -> Result<(), &'static str> {
    let LotChange {
        inst,
        pos,
        side,
        action,
        size,
        price,
        ct,
    } = change;
    if size <= Decimal::ZERO || price <= Decimal::ZERO || ct <= Decimal::ZERO {
        return Err("invalid_lot_financial_terms");
    }
    let increase = match (pos, side) {
        ("long", "buy") | ("short", "sell") => true,
        ("long", "sell") | ("short", "buy") => false,
        _ => return Err("invalid_hedge_position_side"),
    };
    if increase != action.is_risk_increasing() {
        return Err("fill_action_side_mismatch");
    }
    let book = lots.entry((inst.to_owned(), pos.to_owned())).or_default();
    if increase {
        if book.iter().any(|l| l.contract_value != ct) {
            return Err("conflicting_contract_mechanics");
        }
        book.push(Lot {
            contracts: size,
            price,
            contract_value: ct,
        });
        return Ok(());
    }
    let available = book.iter().map(|l| l.contracts).sum::<Decimal>();
    if available < size {
        return Err("exit_exceeds_run_owned_lot");
    }
    // The reducer may ONLY consume historical owned lots; deterministic FIFO.
    let mut remaining = size;
    for lot in book.iter_mut() {
        let used = remaining.min(lot.contracts);
        lot.contracts -= used;
        remaining -= used;
        if remaining == Decimal::ZERO {
            break;
        }
    }
    book.retain(|l| l.contracts > Decimal::ZERO);
    Ok(())
}

/// Every interval from the run opening THROUGH the current authenticated
/// account snapshot needs durable source-complete checkpoints. Merely having
/// adjacent persisted cursors is not enough: if the latest is too old, a
/// later short REST window cannot recover vanished canceled orders.
///
/// current_as_of is read from the same coherent private account snapshot
/// used for this assessment, never inferred from the workstation clock.
/// A fresh complete REST observation is evidence only if it closes the last
/// <=2h interval; it cannot bridge a longer outage after restart.
#[cfg(test)]
fn source_continuity_attested(
    sorted_cursor_times: &[u64],
    run_started_at_ms: u64,
    current_as_of_ms: Option<u64>,
) -> bool {
    let mut coverage = SourceReceiptCoverage::default();
    for &receipt in sorted_cursor_times {
        coverage.observe(receipt);
    }
    coverage.attests(run_started_at_ms, current_as_of_ms)
}

/// Conservative default used by settlement/finalization paths. It never
/// values an OPEN lot from the Demo-private position mark.
pub fn assess_persisted_financial_period(
    run: &StrategyCapitalReservation,
    evidence: Option<&DurableExchangeEvidence>,
    entries: &[ExecutionLedgerEntry],
    facts: &AccountLedgerFacts,
    coherent: bool,
) -> StrategyFinancialPeriod {
    assess_persisted_financial_period_with_production_marks(
        run,
        evidence,
        entries,
        facts,
        coherent,
        &BTreeMap::new(),
    )
}

/// Production-public mark input is observation-owned and NOT supplied by
/// an operator, private Demo account or trade request.
pub fn assess_persisted_financial_period_with_production_marks(
    run: &StrategyCapitalReservation,
    evidence: Option<&DurableExchangeEvidence>,
    entries: &[ExecutionLedgerEntry],
    facts: &AccountLedgerFacts,
    coherent: bool,
    production_marks: &BTreeMap<String, ProductionMarkEvidence>,
) -> StrategyFinancialPeriod {
    assess_persisted_financial_period_with_verified_source(
        run,
        evidence,
        entries,
        facts,
        coherent,
        production_marks,
        false,
    )
}

/// A complete, coherent private REST/WS account read has just been captured
/// by the SINGLE ledger writer. New native events are durably appended; an
/// unchanged quiet REST window may legitimately coalesce its *cursor*.
/// Do not confuse an old persisted cursor with stale current account facts.
#[allow(clippy::too_many_arguments)]
pub fn assess_persisted_financial_period_with_verified_source(
    run: &StrategyCapitalReservation,
    evidence: Option<&DurableExchangeEvidence>,
    entries: &[ExecutionLedgerEntry],
    facts: &AccountLedgerFacts,
    coherent: bool,
    production_marks: &BTreeMap<String, ProductionMarkEvidence>,
    current_source_captured: bool,
) -> StrategyFinancialPeriod {
    let source_index = evidence.map(SourceOrderIndex::new);
    assess_persisted_financial_period_with_index(
        run,
        evidence,
        entries,
        facts,
        coherent,
        production_marks,
        current_source_captured,
        source_index.as_ref(),
    )
}

/// Use the identical immutable source index already required by native
/// terminal-order reconciliation. Only one proof/index is built per financial
/// calculation, never one for each linked fill or risk candidate.
#[allow(clippy::too_many_arguments)]
pub(crate) fn assess_persisted_financial_period_with_index(
    run: &StrategyCapitalReservation,
    evidence: Option<&DurableExchangeEvidence>,
    entries: &[ExecutionLedgerEntry],
    facts: &AccountLedgerFacts,
    coherent: bool,
    production_marks: &BTreeMap<String, ProductionMarkEvidence>,
    current_source_captured: bool,
    source_index: Option<&SourceOrderIndex<'_>>,
) -> StrategyFinancialPeriod {
    let (Some(evidence), Some(index)) = (evidence, source_index) else {
        let mut reasons = BTreeSet::new();
        reasons.insert(if evidence.is_none() {
            "durable_exchange_evidence_absent"
        } else {
            "source_order_index_unavailable"
        });
        return incomplete(
            run,
            Decimal::from_str(&run.allocated_usdt).unwrap_or(Decimal::ZERO),
            evidence.map_or(String::new(), |e| e.checksum_sha256.clone()),
            0,
            0,
            Decimal::ZERO,
            Decimal::ZERO,
            Vec::new(),
            reasons,
        );
    };
    let reader = LegacyFinanceReader {
        source: evidence,
        index,
    };
    assess_persisted_financial_period_from_reader(
        run,
        &reader,
        entries,
        facts,
        coherent,
        production_marks,
        current_source_captured,
    )
    .unwrap_or_else(|_| {
        let mut reasons = BTreeSet::new();
        reasons.insert("invalid_persisted_source_integrity");
        incomplete(
            run,
            Decimal::from_str(&run.allocated_usdt).unwrap_or(Decimal::ZERO),
            evidence.checksum_sha256.clone(),
            0,
            0,
            Decimal::ZERO,
            Decimal::ZERO,
            Vec::new(),
            reasons,
        )
    })
}
/// The ONE financial policy, not separate v14 and native FIFO/PnL engines.
#[allow(clippy::too_many_arguments)]
pub(crate) fn assess_persisted_financial_period_from_reader(
    run: &StrategyCapitalReservation,
    reader: &dyn FinanceSourceReader,
    entries: &[ExecutionLedgerEntry],
    facts: &AccountLedgerFacts,
    coherent: bool,
    production_marks: &BTreeMap<String, ProductionMarkEvidence>,
    current_source_captured: bool,
) -> Result<StrategyFinancialPeriod, ExecutionLedgerError> {
    let mut reasons = BTreeSet::new();
    let mut realized = Decimal::ZERO;
    let mut fees = Decimal::ZERO;
    let mut lots = BTreeMap::<(String, String), Vec<Lot>>::new();
    let mut linked_bills = BTreeSet::<String>::new();
    let mut processed = 0usize;
    let allocation = Decimal::from_str(&run.allocated_usdt).unwrap_or(Decimal::ZERO);
    let audit = reader.audit()?;
    let evidence_sha256 = audit.digest.clone();
    if !coherent {
        reasons.insert("account_snapshot_not_coherent");
    }
    if facts.summary.authority.account_uid_fingerprint != run.account_uid_fingerprint {
        reasons.insert("financial_account_uid_mismatch");
    }
    if audit.uid != run.account_uid_fingerprint || audit.run_id != run.allocation_id {
        reasons.insert("invalid_persisted_source_integrity");
        return Ok(incomplete(
            run,
            allocation,
            evidence_sha256,
            0,
            0,
            realized,
            fees,
            Vec::new(),
            reasons,
        ));
    }
    if audit.coverage.latest().is_none() {
        reasons.insert("source_coverage_missing");
    }
    // Source coverage is measured by authenticated receipt times, NOT the
    // venue balance/position last-change uTime (which can be days old).
    let current_as_of =
        crate::exchange_evidence::source_receipt_ms(&facts.summary.source_received_at);
    let newest = audit.coverage.latest();
    let coalesced_source_attested = current_source_captured
        && coherent
        && DurableExchangeEvidence::complete_source_checkpoint(facts);
    if !coalesced_source_attested
        && current_as_of
            .zip(newest)
            .is_none_or(|(current, last)| current < last || current - last > 30_000)
    {
        reasons.insert("financial_source_asof_not_current");
    }
    if !audit.coverage.attests(run.started_at_ms, current_as_of) {
        reasons.insert("source_continuity_not_proven");
    }
    if audit.truncated {
        reasons.insert("persisted_source_window_truncated");
    }
    if !facts.exchange_orders_history_complete
        || !facts.exchange_fills_history_complete
        || !facts.bills_history_complete
    {
        reasons.insert("current_source_window_truncated");
    }

    for entry in entries {
        reader.validate_entry(entry, run.started_at_ms, &mut reasons)?;
    }
    // One ordered stream; no historical fill collection is retained.
    let mut previous_fill: Option<(u64, String, String, String)> = None;
    reader.visit_fills(&mut |item| {
        let p = &item.payload;
        let (
            Some(ts),
            Some(inst),
            Some(pos),
            Some(side),
            Some(ord),
            Some(client),
            Some(trade),
            Some(bill),
            Some(size),
            Some(price),
        ) = (
            timestamp(p, "ts"),
            string(p, "instId"),
            string(p, "posSide"),
            string(p, "side"),
            string(p, "ordId"),
            string(p, "clOrdId"),
            string(p, "tradeId"),
            string(p, "billId"),
            amount(p, "fillSz"),
            amount(p, "fillPx"),
        )
        else {
            reasons.insert("missing_source_fill_identity");
            return Ok(());
        };
        if ts < run.started_at_ms || size <= Decimal::ZERO || price <= Decimal::ZERO {
            reasons.insert("missing_source_fill_identity");
            return Ok(());
        }
        let f = FilledLeg {
            source: item,
            ts,
            inst: inst.to_owned(),
            pos: pos.to_owned(),
            side: side.to_owned(),
            ord: ord.to_owned(),
            client: client.to_owned(),
            trade: trade.to_owned(),
            bill: bill.to_owned(),
            size,
            price,
        };
        if previous_fill
            .as_ref()
            .is_some_and(|(prior_ts, prior_inst, prior_pos, prior_side)| {
                *prior_ts == f.ts
                    && prior_inst == &f.inst
                    && prior_pos == &f.pos
                    && prior_side != &f.side
            })
        {
            reasons.insert("same_timestamp_opposite_fill_sequence");
        }
        previous_fill = Some((f.ts, f.inst.clone(), f.pos.clone(), f.side.clone()));
        let item = f.source;
        let Some(intent) = item.source_intent_id.as_deref() else {
            reasons.insert("unattributed_account_fill");
            return Ok(());
        };
        let Some(entry) = reader.entry(intent, entries)? else {
            reasons.insert("source_intent_not_present");
            return Ok(());
        };
        let plan = &entry.record.plan;
        if entry.record.order_id.as_deref() != Some(f.ord.as_str())
            || plan.client_order_id != f.client
            || plan.instrument_id != f.inst
            || plan.side.as_str() != f.side
            || plan.position_side.as_str() != f.pos
            || plan.account_uid_fingerprint != run.account_uid_fingerprint
            || entry.created_at_ms <= run.started_at_ms
        {
            reasons.insert("source_order_intent_identity_mismatch");
            return Ok(());
        }
        if plan.action.is_risk_increasing()
            && (item.strategy_run_id.as_deref() != Some(run.allocation_id.as_str())
                || entry.record.strategy_run_id.as_deref() != Some(run.allocation_id.as_str())
                || entry.record.lineage.as_ref().is_none_or(|l| {
                    l.origin_schema != run.origin_schema || l.origin_version != run.origin_version
                })
                || plan
                    .risk_binding
                    .as_ref()
                    .is_none_or(|b| b.mandate.version != run.mandate_version))
        {
            reasons.insert("risk_increase_not_owned_by_run");
            return Ok(());
        }
        if plan.action.is_risk_reducing()
            && (item.strategy_run_id.is_some() || entry.record.strategy_run_id.is_some())
        {
            reasons.insert("unsupported_reducer_run_binding");
            return Ok(());
        }
        // Only exact originating order identity and native fill source can
        // establish this leg. A shared instrument is not a sufficient link.
        let matching_order = reader.exact_order(&f.ord, &f.client, &f.inst, intent)?;
        if !matching_order {
            reasons.insert("exact_source_order_missing");
            return Ok(());
        }
        let mechanics = entry
            .record
            .lineage
            .as_ref()
            .and_then(|l| l.tca_mechanics.as_ref());
        let ct = if let Some(m) = mechanics {
            if m.settle_currency == "USDT" && m.contract_type.eq_ignore_ascii_case("linear") {
                Decimal::from_str(&m.contract_value).ok()
            } else {
                None
            }
        } else {
            None
        };
        // Reduction can reuse the original owned lot's mechanics; the exit
        // must not invent a contract value from its own missing lineage.
        let ct = ct.or_else(|| {
            lots.get(&(f.inst.clone(), f.pos.clone()))
                .and_then(|v| v.first().map(|l| l.contract_value))
        });
        let Some(ct) = ct else {
            reasons.insert("linear_contract_mechanics_unattested");
            return Ok(());
        };
        let bill_key = format!("bill:{}", f.bill);
        let Some(b) = reader.bill(&bill_key)? else {
            reasons.insert("native_fill_bill_missing");
            return Ok(());
        };
        let bp = &b.payload;
        if bp["source"] != "OKX_REST_BILL"
            || string(bp, "billId") != Some(f.bill.as_str())
            || string(bp, "instId") != Some(f.inst.as_str())
            || string(bp, "tradeId") != Some(f.trade.as_str())
            || string(bp, "ordId").is_some_and(|v| v != f.ord)
            || string(bp, "clOrdId").is_some_and(|v| v != f.client)
            || string(bp, "ccy") != Some("USDT")
            || string(&item.payload, "feeCcy") != Some("USDT")
            || matches!(string(bp, "subType"), Some("173" | "174"))
            || b.source_intent_id.as_deref() != Some(intent)
        {
            reasons.insert("fill_bill_provenance_or_currency_mismatch");
            return Ok(());
        }
        let (Some(native_fee), Some(bill_fee), Some(native_pnl), Some(bill_pnl)) = (
            amount(&item.payload, "fee"),
            amount(bp, "fee"),
            amount(&item.payload, "fillPnl"),
            amount(bp, "pnl"),
        ) else {
            reasons.insert("missing_native_fee_or_pnl");
            return Ok(());
        };
        if native_fee != bill_fee || native_pnl != bill_pnl {
            reasons.insert("native_bill_fee_or_pnl_mismatch");
            return Ok(());
        }
        if linked_bills.contains(&f.bill) {
            reasons.insert("duplicate_bill_consumption");
            return Ok(());
        }
        if let Err(reason) = put_lot(
            &mut lots,
            LotChange {
                inst: &f.inst,
                pos: &f.pos,
                side: &f.side,
                action: plan.action,
                size: f.size,
                price: f.price,
                ct,
            },
        ) {
            reasons.insert(reason);
            return Ok(());
        }
        linked_bills.insert(f.bill.clone());
        realized += native_pnl;
        fees += native_fee;
        processed += 1;
        Ok(())
    })?;

    // A separate bill, even for the same symbol, is not strategy-owned.
    // Funding subTypes carry no exact strategy/side link; never allocate them.
    reader.visit_bills(&mut |key, bill| {
        if matches!(string(&bill.payload, "subType"), Some("173" | "174")) {
            reasons.insert("funding_unallocated");
        } else if !linked_bills.contains(key.trim_start_matches("bill:")) {
            let payload = &bill.payload;
            let kind = classify_native_account_bill(AccountBillSource {
                bill_type: string(payload, "type").unwrap_or(""),
                sub_type: string(payload, "subType").unwrap_or(""),
                currency: string(payload, "ccy").unwrap_or(""),
                balance_change: string(payload, "balChg"),
                instrument_id: string(payload, "instId").unwrap_or(""),
                order_id: string(payload, "ordId"),
                trade_id: string(payload, "tradeId"),
                client_order_id: string(payload, "clOrdId"),
                fee: string(payload, "fee"),
                pnl: string(payload, "pnl"),
            });
            reasons.insert(match kind {
                AccountBillClass::TransferIn(_) | AccountBillClass::TransferOut(_) => {
                    "account_cashflow_not_allocated_to_run"
                }
                AccountBillClass::UnverifiedTransfer => "account_cashflow_source_unverified",
                AccountBillClass::Other => "unattributed_account_bill",
            });
        }
        Ok(())
    })?;
    let mut open_positions = Vec::new();
    let mut unrealized = Decimal::ZERO;
    for ((inst, pos), book) in &lots {
        let qty = book.iter().map(|l| l.contracts).sum::<Decimal>();
        if qty == Decimal::ZERO {
            continue;
        }
        let matches = facts
            .authoritative_positions
            .iter()
            .filter(|p| p.instrument_id == *inst && p.position_side == *pos)
            .collect::<Vec<_>>();
        let mut mark = None;
        let mut upl = None;
        if matches.len() == 1
            && Decimal::from_str(&matches[0].position).is_ok_and(|v| v.abs() == qty)
        {
            // The private OKX position belongs to the selected execution
            // venue (Demo here). Only its exact identity and contract size
            // attest lot ownership. Its mark and reported UPL are *not* the
            // Production-public market's valuation of this position.
            if let Some(px) = current_as_of.and_then(|as_of| {
                production_marks
                    .get(inst)
                    .and_then(|quote| attested_production_mark(quote, as_of))
            }) {
                let computed = book
                    .iter()
                    .map(|l| {
                        (px - l.price)
                            * l.contracts
                            * l.contract_value
                            * if pos == "long" {
                                Decimal::ONE
                            } else {
                                -Decimal::ONE
                            }
                    })
                    .sum::<Decimal>();
                unrealized += computed;
                mark = Some(px.normalize().to_string());
                upl = Some(computed.normalize().to_string());
            } else {
                reasons.insert("production_public_mark_unattested");
            }
        } else {
            reasons.insert("run_position_not_exclusive_on_exchange");
        }
        open_positions.push(RunOwnedPosition {
            instrument_id: inst.clone(),
            position_side: pos.clone(),
            contracts: qty.normalize().to_string(),
            mark_price: mark,
            unrealized_usdt: upl,
        });
    }
    // Detect an external outstanding position, even when no run-owned lot exists.
    if facts.authoritative_positions.iter().any(|p| {
        Decimal::from_str(&p.position).is_ok_and(|q| q != Decimal::ZERO)
            && !lots.contains_key(&(p.instrument_id.clone(), p.position_side.clone()))
    }) {
        reasons.insert("external_exchange_position");
    }
    if facts.summary.pending_orders != 0 {
        reasons.insert("pending_exchange_orders");
    }
    if entries
        .iter()
        .any(|entry| entry.created_at_ms > run.started_at_ms && !entry.record.state.is_terminal())
    {
        reasons.insert("unresolved_managed_execution");
    }
    let complete = reasons.is_empty();
    let nav = allocation + realized + fees + unrealized;
    Ok(StrategyFinancialPeriod {
        schema: STRATEGY_FINANCIAL_PERIOD_SCHEMA_V1,
        run_id: run.allocation_id.clone(),
        state: if complete && open_positions.is_empty() {
            "FLAT_RECONCILED"
        } else if complete {
            "ACTIVE_VALUED"
        } else {
            "INCOMPLETE"
        },
        evidence_sha256,
        allocation_usdt: allocation.normalize().to_string(),
        linked_fills: processed,
        linked_bills: linked_bills.len(),
        realized_usdt: realized.normalize().to_string(),
        fees_usdt: fees.normalize().to_string(),
        // Zero funding is knowable only when the entire run source is
        // complete and no funding bill was seen. Any funding bill remains
        // unallocated and makes the period INCOMPLETE instead of inventing
        // a position-side attribution.
        funding_usdt: complete.then(|| "0".to_owned()),
        unrealized_usdt: complete.then(|| unrealized.normalize().to_string()),
        nav_usdt: complete.then(|| nav.normalize().to_string()),
        positions: open_positions,
        // Peak/day-loss/high-water durability not yet attested. Do not grant
        // any new-risk permission from this period projection alone.
        usable_for_new_risk: false,
        reasons: reasons.into_iter().collect(),
    })
}

#[allow(clippy::too_many_arguments)]
fn incomplete(
    run: &StrategyCapitalReservation,
    allocation: Decimal,
    evidence_sha256: String,
    linked_fills: usize,
    linked_bills: usize,
    realized: Decimal,
    fees: Decimal,
    positions: Vec<RunOwnedPosition>,
    reasons: BTreeSet<&'static str>,
) -> StrategyFinancialPeriod {
    StrategyFinancialPeriod {
        schema: STRATEGY_FINANCIAL_PERIOD_SCHEMA_V1,
        run_id: run.allocation_id.clone(),
        state: "INCOMPLETE",
        evidence_sha256,
        allocation_usdt: allocation.normalize().to_string(),
        linked_fills,
        linked_bills,
        realized_usdt: realized.normalize().to_string(),
        fees_usdt: fees.normalize().to_string(),
        funding_usdt: None,
        unrealized_usdt: None,
        nav_usdt: None,
        positions,
        usable_for_new_risk: false,
        reasons: reasons.into_iter().collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_fresh_production_public_mark_can_value_owned_lots() {
        let mark = ProductionMarkEvidence {
            price: "83636.8".to_owned(),
            exchange_timestamp_ms: 100_000,
            received_at_ms: 100_200,
        };
        assert_eq!(
            attested_production_mark(&mark, 100_300),
            Some(Decimal::new(836368, 1))
        );
        let mut bad = mark.clone();
        bad.price = "0".to_owned();
        assert_eq!(attested_production_mark(&bad, 100_300), None);
        bad = mark.clone();
        bad.exchange_timestamp_ms = 101_500;
        // Native exchange time may be marginally ahead of the host UTC clock.
        assert_eq!(
            attested_production_mark(&bad, 100_300),
            Some(Decimal::new(836368, 1))
        );
        bad.exchange_timestamp_ms = 130_000;
        assert_eq!(attested_production_mark(&bad, 100_300), None);
        bad = mark.clone();
        bad.received_at_ms = 160_201;
        assert_eq!(attested_production_mark(&bad, 100_300), None);
        bad = mark.clone();
        bad.exchange_timestamp_ms = 50_000;
        assert_eq!(attested_production_mark(&bad, 100_300), None);
        assert_eq!(attested_production_mark(&mark, 130_201), None);
        // Prior code accepted both 30s hops independently, causing a
        // 60s-old mark to be counted as fresh strategy NAV.
        let composed_stale = ProductionMarkEvidence {
            price: "83636.8".to_owned(),
            exchange_timestamp_ms: 100_000,
            received_at_ms: 130_000,
        };
        assert_eq!(attested_production_mark(&composed_stale, 160_000), None);
        // Exact end-to-end boundary is accepted; one millisecond older fails.
        assert_eq!(
            attested_production_mark(&composed_stale, 130_000),
            Some(Decimal::new(836368, 1))
        );
        assert_eq!(attested_production_mark(&composed_stale, 130_001), None);
    }

    #[test]
    fn real_receipts_bound_the_run_even_when_account_last_change_is_old() {
        let hour = 3_600_000_u64;
        let run_start = 1_000_000_u64;
        // Immutable source receipts are hourly and pass financial continuity.
        // The account last-change timestamp is intentionally irrelevant here:
        // it may predate the strategy run by a whole day.
        let unchanged_account_last_change = run_start - 900_000_u64;
        let receipts = [
            run_start + hour / 2,
            run_start + hour + hour / 2,
            run_start + 2 * hour + hour / 2,
            run_start + 3 * hour + hour / 2,
        ];
        assert!(unchanged_account_last_change < run_start);
        assert!(source_continuity_attested(
            &receipts,
            run_start,
            Some(receipts[3] + hour),
        ));
        // An actual >2h missing receipt is NOT bridged by a later complete
        // read, even if that read itself is fresh.
        let missed = [receipts[0], receipts[0] + 2 * hour + 1];
        assert!(!source_continuity_attested(
            &missed,
            run_start,
            Some(missed[1]),
        ));
        assert!(!source_continuity_attested(
            &receipts,
            run_start,
            Some(receipts[3] + 2 * hour + 1),
        ));
    }

    #[test]
    fn source_coverage_streaming_matches_full_vector_for_long_history() {
        let start = 1_000_000_u64;
        let step = 1_000_u64;
        let mut stream = SourceReceiptCoverage::default();
        let mut receipt_times = Vec::new();
        for i in 0..20_000_u64 {
            let t = start + i * step;
            stream.observe(t);
            receipt_times.push(t);
        }
        let current = Some(start + 20_000 * step);
        assert_eq!(
            stream.attests(start, current),
            source_continuity_attested(&receipt_times, start, current)
        );
        assert!(stream.attests(start, current));
        stream.observe(start + 20_000 * step + crate::exchange_evidence::MAX_SOURCE_COVERAGE_GAP_MS + 1);
        assert!(!stream.attests(start, stream.latest()));
        let mut out_of_order = SourceReceiptCoverage::default();
        out_of_order.observe(start + step);
        out_of_order.observe(start);
        assert!(!out_of_order.attests(start, Some(start + step)));
    }

    #[test]
    fn missing_two_hour_venue_window_stays_incomplete_after_restart() {
        let hour = 3_600_000_u64;
        let start = 1_000_000_u64;
        let max_gap = crate::exchange_evidence::MAX_SOURCE_COVERAGE_GAP_MS;

        // Neither an empty source ledger nor a missing current exchange
        // timestamp can prove continuity.
        assert!(!source_continuity_attested(&[], start, Some(start)));
        assert!(!source_continuity_attested(&[start], start, None));

        let saved = [start, start + hour, start + 2 * hour];
        assert!(source_continuity_attested(
            &saved,
            start,
            Some(start + 2 * hour + max_gap),
        ));
        // The final checkpoint-to-account interval is part of the proof.
        // Formerly this returned true regardless of current snapshot age.
        assert!(!source_continuity_attested(
            &saved,
            start,
            Some(start + 2 * hour + max_gap + 1),
        ));
        assert!(!source_continuity_attested(
            &saved,
            start,
            Some(start + 2 * hour - 1),
        ));
        // Checkpoints survive restart. Replaying a fresh later REST snapshot
        // without intermediate immutable cursors must not fill the hole.
        let reopened: Vec<u64> = serde_json::from_slice(
            &serde_json::to_vec(&saved).expect("persisted cursor timestamps"),
        )
        .expect("read-only reopen");
        assert!(!source_continuity_attested(
            &reopened,
            start,
            Some(start + 5 * hour),
        ));
        // A later independent complete checkpoint within the window restores
        // *future* continuity only when all earlier intervals were covered.
        let continuous = [start, start + hour, start + 2 * hour, start + 3 * hour];
        assert!(source_continuity_attested(
            &continuous,
            start,
            Some(start + 4 * hour),
        ));
        assert!(!source_continuity_attested(
            &[start, start + 2 * hour + 1],
            start,
            Some(start + 2 * hour + 1),
        ));
        assert!(!source_continuity_attested(
            &[start + 2 * hour + 1],
            start,
            Some(start + 2 * hour + 1),
        ));
        assert!(!source_continuity_attested(
            &[start + hour, start],
            start,
            Some(start + 2 * hour),
        ));
        // Neither old pre-run evidence nor a future-dated cursor is trusted.
        assert!(!source_continuity_attested(
            &[start - 1],
            start,
            Some(start),
        ));
        assert!(!source_continuity_attested(
            &[start + 1],
            start,
            Some(start),
        ));
    }

    #[test]
    fn fifo_partial_fills_and_cross_side_cannot_double_spend() {
        let mut lots = BTreeMap::new();
        put_lot(
            &mut lots,
            LotChange {
                inst: "BTC-USDT-SWAP",
                pos: "long",
                side: "buy",
                action: ExecutionAction::Open,
                size: Decimal::new(3, 0),
                price: Decimal::new(100, 0),
                ct: Decimal::new(1, 2),
            },
        )
        .unwrap();
        put_lot(
            &mut lots,
            LotChange {
                inst: "BTC-USDT-SWAP",
                pos: "long",
                side: "buy",
                action: ExecutionAction::Add,
                size: Decimal::new(2, 0),
                price: Decimal::new(105, 0),
                ct: Decimal::new(1, 2),
            },
        )
        .unwrap();
        put_lot(
            &mut lots,
            LotChange {
                inst: "BTC-USDT-SWAP",
                pos: "long",
                side: "sell",
                action: ExecutionAction::Reduce,
                size: Decimal::new(4, 0),
                price: Decimal::new(110, 0),
                ct: Decimal::new(1, 2),
            },
        )
        .unwrap();
        assert_eq!(
            lots[&("BTC-USDT-SWAP".into(), "long".into())][0].contracts,
            Decimal::ONE
        );
        assert_eq!(
            lots[&("BTC-USDT-SWAP".into(), "long".into())][0].price,
            Decimal::new(105, 0)
        );
        assert_eq!(
            put_lot(
                &mut lots,
                LotChange {
                    inst: "BTC-USDT-SWAP",
                    pos: "long",
                    side: "sell",
                    action: ExecutionAction::Close,
                    size: Decimal::new(2, 0),
                    price: Decimal::new(110, 0),
                    ct: Decimal::new(1, 2),
                },
            ),
            Err("exit_exceeds_run_owned_lot")
        );
        assert_eq!(
            put_lot(
                &mut lots,
                LotChange {
                    inst: "BTC-USDT-SWAP",
                    pos: "short",
                    side: "buy",
                    action: ExecutionAction::Close,
                    size: Decimal::ONE,
                    price: Decimal::new(110, 0),
                    ct: Decimal::new(1, 2),
                },
            ),
            Err("exit_exceeds_run_owned_lot")
        );
        assert_eq!(
            put_lot(
                &mut lots,
                LotChange {
                    inst: "BTC-USDT-SWAP",
                    pos: "long",
                    side: "buy",
                    action: ExecutionAction::Close,
                    size: Decimal::ONE,
                    price: Decimal::new(110, 0),
                    ct: Decimal::new(1, 2),
                },
            ),
            Err("fill_action_side_mismatch")
        );
    }

    #[test]
    fn filled_order_without_complete_native_fill_fails_closed() {
        use crate::{EXCHANGE_EVIDENCE_SCHEMA_V1, exchange_evidence::StoredExchangeEvent};
        use serde_json::json;
        // Source-exact terminal state with no fills cannot be treated as
        // an accounted zero-result strategy period.
        let mut evidence = DurableExchangeEvidence {
            schema: EXCHANGE_EVIDENCE_SCHEMA_V1.to_owned(),
            account_uid: "demo".into(),
            run_id: "run".into(),
            events: BTreeMap::new(),
            cursors: BTreeMap::new(),
            checksum_sha256: String::new(),
        };
        evidence.events.insert(
            "order:SWAP:123:1".into(),
            StoredExchangeEvent {
                payload: json!({"source":"OKX_REST_ORDER","ordId":"123",
                "clOrdId":"cl_1","instId":"BTC-USDT-SWAP","state":"filled"}),
                source_intent_id: None,
                strategy_run_id: None,
                allocation: "UNALLOCATED".into(),
            },
        );
        let no_entries: Vec<ExecutionLedgerEntry> = vec![];
        let mut reasons = BTreeSet::new();
        let index = SourceOrderIndex::new(&evidence);
        for entry in &no_entries {
            index.validate_entry(entry, 1, &mut reasons);
        }
        assert!(
            reasons.is_empty(),
            "no invented owner from account-only history"
        );
    }

    #[test]
    fn native_terminal_quantity_accepts_only_exact_native_canceled_coverage() {
        let d = |n: i64| Some(Decimal::new(n, 0));
        let ok = |state, qty, acc, fills: Vec<Option<Decimal>>, amended| {
            terminal_native_coverage(
                state,
                Decimal::new(5, 0),
                Decimal::new(qty, 0),
                Decimal::new(acc, 0),
                &fills,
                amended,
            )
        };
        assert!(ok("filled", 5, 5, vec![d(2), d(3)], false));
        assert!(ok("canceled", 5, 0, vec![], false));
        assert!(ok("canceled", 5, 3, vec![d(1), d(2)], false));
        assert!(!ok("canceled", 5, 3, vec![d(1)], false));
        assert!(!ok("canceled", 5, 3, vec![d(1), None], false));
        assert!(!ok("filled", 5, 3, vec![d(3)], false));
        assert!(!ok("canceled", 5, 6, vec![d(6)], false));
        assert!(!ok("canceled", 6, 3, vec![d(3)], false));
        assert!(!ok("filled", 5, 5, vec![d(5)], true));
        assert!(!ok("live", 5, 0, vec![], false));
    }

    #[test]
    fn latest_order_history_allows_live_then_filled_but_rejects_same_timestamp_conflict() {
        use crate::exchange_evidence::StoredExchangeEvent;
        use serde_json::json;
        let row = |state: &str, ts: &str| StoredExchangeEvent {
            payload: json!({"source":"OKX_REST_ORDER","ordId":"1","state":state,"uTime":ts}),
            source_intent_id: Some("intent".into()),
            strategy_run_id: None,
            allocation: "UNALLOCATED".into(),
        };
        let live = row("live", "100");
        let filled = row("filled", "200");
        assert_eq!(
            latest_order_state(&[&live, &filled]).as_deref(),
            Some("filled")
        );
        assert_eq!(
            latest_order_state(&[&filled, &live]).as_deref(),
            Some("filled")
        );
        let contradictory = row("canceled", "200");
        assert_eq!(latest_order_state(&[&filled, &contradictory]), None);
        let invalid = row("filled", "no-timestamp");
        assert_eq!(latest_order_state(&[&invalid]), None);
    }
    #[test]
    fn cross_currency_and_wrong_hedge_never_create_owned_lots() {
        let mut lots = BTreeMap::new();
        assert_eq!(
            put_lot(
                &mut lots,
                LotChange {
                    inst: "ETH-USDT-SWAP",
                    pos: "short",
                    side: "buy",
                    action: ExecutionAction::Open,
                    size: Decimal::ONE,
                    price: Decimal::new(3000, 0),
                    ct: Decimal::ONE,
                },
            ),
            Err("fill_action_side_mismatch")
        );
        assert_eq!(
            put_lot(
                &mut lots,
                LotChange {
                    inst: "ETH-USDT-SWAP",
                    pos: "net",
                    side: "buy",
                    action: ExecutionAction::Open,
                    size: Decimal::ONE,
                    price: Decimal::new(3000, 0),
                    ct: Decimal::ONE,
                },
            ),
            Err("invalid_hedge_position_side")
        );
    }
}
