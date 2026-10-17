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
    DurableExchangeEvidence, ExecutionAction, ExecutionLedgerEntry, StrategyCapitalReservation,
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

fn validate_terminal_order_coverage(
    evidence: &DurableExchangeEvidence,
    entries: &[ExecutionLedgerEntry],
    run_started_at_ms: u64,
    reasons: &mut BTreeSet<&'static str>,
) {
    for entry in entries
        .iter()
        .filter(|entry| entry.created_at_ms > run_started_at_ms)
    {
        let record = &entry.record;
        let Some(order_id) = record.order_id.as_deref() else {
            continue;
        };
        let orders = evidence
            .events
            .values()
            .filter(|event| {
                event.payload["source"] == "OKX_REST_ORDER"
                    && string(&event.payload, "ordId") == Some(order_id)
                    && string(&event.payload, "clOrdId")
                        == Some(record.plan.client_order_id.as_str())
                    && string(&event.payload, "instId") == Some(record.plan.instrument_id.as_str())
                    && event.source_intent_id.as_deref() == Some(record.plan.intent_id.as_str())
            })
            .collect::<Vec<_>>();
        if orders.is_empty() {
            reasons.insert("managed_exchange_order_missing");
            continue;
        }
        let Some(last_state) = latest_order_state(&orders) else {
            reasons.insert("managed_order_terminal_state_unproven");
            continue;
        };
        if !matches!(last_state.as_str(), "filled" | "canceled" | "mmp_canceled")
            || !record.state.is_terminal()
        {
            reasons.insert("managed_order_terminal_state_unproven");
            continue;
        }
        let fills = evidence
            .events
            .values()
            .filter(|event| {
                event.payload["source"] == "OKX_REST_FILL"
                    && string(&event.payload, "ordId") == Some(order_id)
                    && string(&event.payload, "clOrdId")
                        == Some(record.plan.client_order_id.as_str())
                    && string(&event.payload, "instId") == Some(record.plan.instrument_id.as_str())
                    && event.source_intent_id.as_deref() == Some(record.plan.intent_id.as_str())
            })
            .collect::<Vec<_>>();
        let Some(expected) = Decimal::from_str(&record.plan.size).ok() else {
            reasons.insert("invalid_managed_order_size");
            continue;
        };
        let Some((exchange_size, accumulated)) = native_terminal_quantities(&orders) else {
            reasons.insert("native_order_accumulated_fill_missing_or_conflicting");
            continue;
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
fn source_continuity_attested(
    sorted_cursor_times: &[u64],
    run_started_at_ms: u64,
    current_as_of_ms: Option<u64>,
) -> bool {
    let Some(current) = current_as_of_ms else {
        return false;
    };
    let Some(first) = sorted_cursor_times.first().copied() else {
        return false;
    };
    let Some(last) = sorted_cursor_times.last().copied() else {
        return false;
    };
    let window = crate::exchange_evidence::MAX_SOURCE_COVERAGE_GAP_MS;
    first >= run_started_at_ms
        && first - run_started_at_ms <= window
        && sorted_cursor_times
            .windows(2)
            .all(|pair| pair[1] >= pair[0] && pair[1] - pair[0] <= window)
        && current >= last
        && current - last <= window
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
    let mut reasons = BTreeSet::new();
    let mut realized = Decimal::ZERO;
    let mut fees = Decimal::ZERO;
    let mut lots = BTreeMap::<(String, String), Vec<Lot>>::new();
    let mut linked_bills = BTreeSet::<String>::new();
    let mut processed = 0usize;
    let allocation = Decimal::from_str(&run.allocated_usdt).unwrap_or(Decimal::ZERO);
    let evidence_sha256 = evidence
        .map(|e| e.checksum_sha256.clone())
        .unwrap_or_default();
    if !coherent {
        reasons.insert("account_snapshot_not_coherent");
    }
    if facts.summary.authority.account_uid_fingerprint != run.account_uid_fingerprint {
        reasons.insert("financial_account_uid_mismatch");
    }
    let Some(e) = evidence else {
        reasons.insert("durable_exchange_evidence_absent");
        return incomplete(
            run,
            allocation,
            evidence_sha256,
            0,
            0,
            realized,
            fees,
            Vec::new(),
            reasons,
        );
    };
    if e.validate().is_err()
        || e.run_id != run.allocation_id
        || e.account_uid != run.account_uid_fingerprint
    {
        reasons.insert("invalid_persisted_source_integrity");
        return incomplete(
            run,
            allocation,
            evidence_sha256,
            0,
            0,
            realized,
            fees,
            Vec::new(),
            reasons,
        );
    }
    if e.cursors.is_empty() {
        reasons.insert("source_coverage_missing");
    }
    let current_as_of = facts
        .summary
        .current_account_as_of_ms
        .as_deref()
        .and_then(|s| s.parse::<u64>().ok());
    let newest = e.cursors.values().map(|c| c.account_as_of_ms).max();
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
    let mut cursor_times = e
        .cursors
        .values()
        .map(|c| c.account_as_of_ms)
        .collect::<Vec<_>>();
    cursor_times.sort_unstable();
    cursor_times.dedup();
    // Canceled unfilled exchange orders may vanish after 2 hours. Gaps cannot
    // be silently bridged by a later historical query or an empty response.
    if !source_continuity_attested(&cursor_times, run.started_at_ms, current_as_of) {
        reasons.insert("source_continuity_not_proven");
    }
    if e.cursors
        .values()
        .any(|c| !c.order_complete || !c.fill_complete || !c.bill_complete)
    {
        reasons.insert("persisted_source_window_truncated");
    }
    if !facts.exchange_orders_history_complete
        || !facts.exchange_fills_history_complete
        || !facts.bills_history_complete
    {
        reasons.insert("current_source_window_truncated");
    }

    let by_intent = entries
        .iter()
        .map(|x| (x.record.plan.intent_id.as_str(), x))
        .collect::<BTreeMap<_, _>>();
    validate_terminal_order_coverage(e, entries, run.started_at_ms, &mut reasons);
    let mut fills = Vec::<FilledLeg>::new();
    for item in e
        .events
        .values()
        .filter(|v| v.payload["source"] == "OKX_REST_FILL")
    {
        let p = &item.payload;
        match (
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
        ) {
            (
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
            ) if ts >= run.started_at_ms && size > Decimal::ZERO && price > Decimal::ZERO => {
                fills.push(FilledLeg {
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
                });
            }
            _ => {
                reasons.insert("missing_source_fill_identity");
            }
        }
    }
    fills.sort_by(|a, b| {
        a.ts.cmp(&b.ts)
            .then(a.inst.cmp(&b.inst))
            .then(a.trade.cmp(&b.trade))
    });
    for pair in fills.windows(2) {
        if pair[0].ts == pair[1].ts
            && pair[0].inst == pair[1].inst
            && pair[0].pos == pair[1].pos
            && pair[0].side != pair[1].side
        {
            reasons.insert("same_timestamp_opposite_fill_sequence");
        }
    }
    for f in fills {
        let item = f.source;
        let Some(intent) = item.source_intent_id.as_deref() else {
            reasons.insert("unattributed_account_fill");
            continue;
        };
        let Some(entry) = by_intent.get(intent) else {
            reasons.insert("source_intent_not_present");
            continue;
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
            continue;
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
            continue;
        }
        if plan.action.is_risk_reducing()
            && (item.strategy_run_id.is_some() || entry.record.strategy_run_id.is_some())
        {
            reasons.insert("unsupported_reducer_run_binding");
            continue;
        }
        // Only exact originating order identity and native fill source can
        // establish this leg. A shared instrument is not a sufficient link.
        let matching_order = e.events.values().any(|x| {
            x.payload["source"] == "OKX_REST_ORDER"
                && string(&x.payload, "ordId") == Some(f.ord.as_str())
                && string(&x.payload, "clOrdId") == Some(f.client.as_str())
                && string(&x.payload, "instId") == Some(f.inst.as_str())
                && x.source_intent_id.as_deref() == Some(intent)
        });
        if !matching_order {
            reasons.insert("exact_source_order_missing");
            continue;
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
            continue;
        };
        let bill_key = format!("bill:{}", f.bill);
        let Some(b) = e.events.get(&bill_key) else {
            reasons.insert("native_fill_bill_missing");
            continue;
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
            continue;
        }
        let (Some(native_fee), Some(bill_fee), Some(native_pnl), Some(bill_pnl)) = (
            amount(&item.payload, "fee"),
            amount(bp, "fee"),
            amount(&item.payload, "fillPnl"),
            amount(bp, "pnl"),
        ) else {
            reasons.insert("missing_native_fee_or_pnl");
            continue;
        };
        if native_fee != bill_fee || native_pnl != bill_pnl {
            reasons.insert("native_bill_fee_or_pnl_mismatch");
            continue;
        }
        if linked_bills.contains(&f.bill) {
            reasons.insert("duplicate_bill_consumption");
            continue;
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
            continue;
        }
        linked_bills.insert(f.bill.clone());
        realized += native_pnl;
        fees += native_fee;
        processed += 1;
    }

    // A separate bill, even for the same symbol, is not strategy-owned.
    // Funding subTypes carry no exact strategy/side link; never allocate them.
    for (key, bill) in &e.events {
        if !key.starts_with("bill:") {
            continue;
        }
        if matches!(string(&bill.payload, "subType"), Some("173" | "174")) {
            reasons.insert("funding_unallocated");
        } else if !linked_bills.contains(key.trim_start_matches("bill:")) {
            reasons.insert("unattributed_account_bill");
        }
    }

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
    StrategyFinancialPeriod {
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
        funding_usdt: None,
        unrealized_usdt: complete.then(|| unrealized.normalize().to_string()),
        nav_usdt: complete.then(|| nav.normalize().to_string()),
        positions: open_positions,
        // Peak/day-loss/high-water durability not yet attested. Do not grant
        // any new-risk permission from this period projection alone.
        usable_for_new_risk: false,
        reasons: reasons.into_iter().collect(),
    }
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
        validate_terminal_order_coverage(&evidence, &no_entries, 1, &mut reasons);
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
