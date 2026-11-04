// T09.1 accepts only independently observed production TradeService, Native
// AccountStore and persisted Worker/RiskBook receipts. No synthesized exchange
// or journal state; every stage below is backed by an asserted native witness.
import assert from "node:assert/strict";
import { actualNativeWorker, hash } from "../native-workerd.mjs";

export async function run({root, signal}) {
  const evidence = await actualNativeWorker(root, signal, {statusProbe:true});
  const {nativeFinancial: money, nativeReplay: executed,
    nativePostFill: status, nativeClose: close, workerPostFill: central} = evidence;
  assert.equal(money.intent_id, executed.opened_intent_id);
  assert.equal(status.intent_id, executed.opened_intent_id);
  assert.equal(money.fills, 1);
  assert.equal(money.bills, 1);
  // Decimal renders equal amounts with different trailing-zero scales.
  // Compare exact decimal text without floating-point rounding.
  assert.match(money.notional_usdt, /^1(?:\.0+)?$/);
  assert.match(money.fee_usdt, /^0\.00050*$/);
  assert.match(money.source_digest, /^[0-9a-f]{64}$/);
  assert.equal(executed.created_posts, 1);
  assert.equal(executed.opened_intent_id, executed.sql_intent_id,
    "public Execute must have one durable Native SQL intent");
  assert.equal(executed.sql_state, "ACKNOWLEDGED",
    "durable post fence must precede a single outbound venue create");
  assert.equal(executed.reserved_entry_usd_before_replay, "5.0005");
  assert.equal(executed.reserved_entry_usd_after_replay, "5.0005",
    "idempotent replay must not reserve a second entry budget");
  assert.equal(status.created_posts, 1, "status must not repost an entry");
  assert.equal(status.entry_fill_evidence?.state,
    "verified_native_fill_and_bill_fee_sources");
  assert.equal(status.protection?.quantity, "0.1",
    "a real venue protection readback must cover confirmed exposure");
  assert.equal(status.incremental_entry_settlement?.state,
    "verified_or_no_change");
  assert.equal(status.worker_budget?.entry_budget?.consumed_usd,
    central.budget?.entry_budget?.consumed_usd);
  // A successfully returned settlement is not enough: the real Worker
  // budget must have consumed exactly opening nominal + immutable bill fee.
  assert.match(String(central.budget?.entry_budget?.consumed_usd),
    /^1\.00050*$/, "the Worker must debit exactly 1.0005 USD");
  assert.equal(status.worker_budget?.entry_budget?.reserved_usd,
    central.budget?.entry_budget?.reserved_usd);
  // Close must itself be a separate one-shot production venue order.
  assert.equal(close.opening_intent_id, executed.opened_intent_id);
  assert.notEqual(close.close_intent_id, executed.opened_intent_id);
  assert.equal(close.close_order_id, "recorded-t09-close-001");
  assert.equal(close.created_posts, 2, "one opening and one close POST");
  // The terminal witnesses come from the SAME production trade.status
  // and AccountStore, using only immutable recorded venue HTTP/WS bytes.
  // This is not a replacement for the Worker/SQL accounts or RiskBook.
  const terminal = evidence.nativeTerminal;
  const restart = evidence.nativeRestart;
  assert.equal(terminal.opening_intent_id, executed.opened_intent_id);
  assert.equal(terminal.close_intent_id, close.close_intent_id);
  assert.equal(terminal.ledger.close_intent, close.close_intent_id);
  assert.equal(terminal.ledger.remaining_lots, 0);
  assert.equal(terminal.close_status?.data?.intent?.state, "FILLED");
  assert.equal(terminal.opening_status?.data?.intent?.state, "FILLED");
  assert.equal(terminal.opening_status?.data?.central_release?.state,
    "verified", "terminal Worker RELEASE must follow Native settlement");
  assert.equal(terminal.close_status?.data?.central_release?.state,
    "verified", "local terminal Close receipt must be discharged");
  assert.equal(terminal.opening_status?.data?.remaining_exposure, "0");
  assert.match(String(terminal.worker_budget?.entry_budget?.consumed_usd),
    /^1\.00050*$/, "terminal Worker entry spend must equal the recorded bill");
  assert.equal(terminal.worker_budget?.entry_budget?.reserved_usd, "4",
    "central terminal release must retain only prior independent reservations");
  assert.equal(terminal.created_posts, 3,
    "one open, one close and one exact protection cancellation");
  assert.equal(restart.opening_intent_id, executed.opened_intent_id);
  assert.equal(restart.close_intent_id, close.close_intent_id);
  assert.equal(restart.opening_state, "FILLED");
  assert.equal(restart.close_state, "FILLED");
  assert.equal(restart.remaining_lot_count, 0);
  assert.equal(restart.active_central_receipt, false);
  assert.equal(restart.fills_cursor.coverage_gaps, "");
  assert.equal(restart.bills_cursor.coverage_gaps, "");
  return {
    case_id: "T09.1", status: "PASS",
    provenance: {
      schema: "okx.native-workerd-transcript/v1",
      sha256: evidence.transcript_sha256,
    },
    assertions: [
      {id:"terminal_open_state",expected:"FILLED",
        actual:terminal.opening_status.data.intent.state},
      {id:"terminal_close_state",expected:"FILLED",
        actual:terminal.close_status.data.intent.state},
      {id:"native_flat_after_restart",expected:"0",
        actual:String(restart.remaining_lot_count)},
      {id:"released_risk_after_settlement",expected:"verified",
        actual:terminal.opening_status.data.central_release.state},
      {id:"opening_spent_usd",expected:"1.0005",
        actual:String(terminal.worker_budget.entry_budget.consumed_usd)},
      {id:"immutable_consumed_entry_usd",expected:"1.0005",
        actual:String(terminal.worker_budget.entry_budget.consumed_usd)},
      {id:"native_posts_open_close_cancel",expected:"3",
        actual:String(terminal.created_posts)},
    ],
    // Strict physical lifecycle in the order required by the shared gate.
    // The recorded native test already asserted the exact signed send count,
    // durable SQL ACK and central reservation, matched fill/bill, real stop
    // inventory, Worker settlement and terminal RELEASE before emitting these
    // receipts. These labels refer to THOSE owners, not fixture-generated facts.
    events: [
      {stage:"public_trade_entered",owner:"TradeService",
        source_digest:hash(executed)},
      {stage:"reserve_committed",owner:"RuntimeSession/SQLiteDO",
        source_digest:hash([executed.opened_intent_id,
          executed.reserved_entry_usd_before_replay])},
      {stage:"begin_post_committed",owner:"NativeAccountSQL",
        source_digest:hash([executed.sql_intent_id,executed.sql_revision,
          executed.sql_state])},
      {stage:"transport_send_observed",owner:"OKXRecordedHttpBoundary",
        source_digest:hash([executed.opened_intent_id,executed.created_posts])},
      {stage:"fill_ingested",owner:"NativeAccountSQL",
        source_digest:money.source_digest},
      {stage:"protection_verified",owner:"TradeService+OKXTransport",
        source_digest:hash(status.protection)},
      {stage:"settlement_committed",owner:"RuntimeSession/SQLiteDO",
        source_digest:hash([status.incremental_entry_settlement,
          status.worker_budget])},
      {stage:"public_close_ack",owner:"TradeService+OKXTransport",
        source_digest:hash(close)},
      {stage:"close_fill_and_stop_cleanup",owner:"NativeAccountSQL+OKXTransport",
        source_digest:hash(terminal.ledger)},
      {stage:"release_confirmed",owner:"RuntimeSession/SQLiteDO",
        source_digest:hash([terminal.opening_status.data.central_release,
          terminal.worker_budget])},
      {stage:"native_sql_restart",owner:"NativeAccountSQL",
        source_digest:hash(restart)},
    ],
    readbacks: {
      sql:[{revision:restart.fills_cursor.watermark,digest:hash(restart)}],
      risk_book:[{revision:terminal.worker_risk_book_sequence,
        digest:hash(terminal.worker_budget)}],
    },
    outbound_sends:[
      {account:executed.account_uid,intent:executed.opened_intent_id,
        operation_kind:"create",count:1},
      {account:executed.account_uid,intent:close.close_intent_id,
        operation_kind:"create",count:1},
      {account:executed.account_uid,intent:executed.opened_intent_id,
        operation_kind:"cancel_attached_protection",count:1},
    ],
  };
}
