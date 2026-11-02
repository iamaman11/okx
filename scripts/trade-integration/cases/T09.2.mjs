// T09.2: second independently addressed 0.30-contract public entry.
// One production TradeService / Native SQL / persisted Worker RiskBook;
// recorded raw venue bytes are allowed only at the signed transport boundary.
import assert from "node:assert/strict";
import { actualNativeWorker, hash } from "../native-workerd.mjs";

export async function run({ root, signal }) {
  const evidence = await actualNativeWorker(root, signal, {
    statusProbe: true, secondAdmission: true,
  });
  const admission = evidence.nativeSecondAdmission;
  const worker = evidence.workerSecondAdmission;
  assert.ok(admission && worker, "T09.2 requires both authentic Native and Worker receipts");
  assert.match(admission.intent_id, /^.+$/);
  assert.match(admission.account_uid, /^[a-f0-9]{64}$/);
  assert.match(admission.client_order_id, /^.+$/);
  assert.match(admission.central_reservation_key, /^.+$/);
  assert.match(admission.central_receipt_hash, /^.+$/);
  assert.match(admission.plan_hash, /^[0-9a-f]{64}$/);
  assert.equal(admission.state, "ACKNOWLEDGED");
  assert.equal(admission.sql_exchange_order_id, "recorded-t09-second-001");
  assert.equal(admission.sql_post_reservation_state, "OPEN",
    "ACK advances the durably fenced risk receipt from POSTED to OPEN");
  assert.ok(Number.isSafeInteger(admission.sql_begin_post_at_ms)
    && admission.sql_begin_post_at_ms > 0,
    "BEGIN_POST must have its own durable SQL timestamp");
  assert.ok(Number.isSafeInteger(admission.sql_revision) && admission.sql_revision > 0);
  assert.equal(admission.quote_entry_usd, "3.0015");
  assert.equal(admission.preview_reserved_entry_usd, "4");
  assert.equal(admission.create_posts_total, 4,
    "T09.1 open, close and cancel plus exactly one NEW venue create");
  assert.equal(admission.post_ack_financial_complete, false,
    "venue ACK without a fill/bill is never financial settlement");
  assert.equal(admission.after_worker_budget.entry_budget.reserved_usd, "7.0015");
  assert.equal(admission.after_worker_budget.entry_budget.consumed_usd, "1.0005");
  assert.deepEqual(worker.budget, admission.after_worker_budget,
    "Native and separate persisted Worker WSS readbacks must agree");
  assert.ok(Number.isSafeInteger(worker.connection_generation)
    && worker.connection_generation > 0);
  assert.ok(Number.isSafeInteger(worker.risk_book_sequence)
    && worker.risk_book_sequence >= admission.after_risk_book_sequence);

  return {
    case_id: "T09.2", status: "PASS",
    provenance: {
      schema: "okx.native-workerd-transcript/v1",
      sha256: evidence.transcript_sha256,
    },
    assertions: [
      { id: "preview_no_reserve", expected: "4",
        actual: String(admission.preview_reserved_entry_usd) },
      { id: "separate_entry_upper_usd", expected: "3.0015",
        actual: String(admission.quote_entry_usd) },
      { id: "durable_post_fence_after_ack", expected: "OPEN",
        actual: String(admission.sql_post_reservation_state) },
      { id: "second_venue_order_id", expected: "recorded-t09-second-001",
        actual: String(admission.sql_exchange_order_id) },
      { id: "total_exact_outbound_mutations", expected: "4",
        actual: String(admission.create_posts_total) },
      { id: "worker_reserved_entry_usd", expected: "7.0015",
        actual: String(worker.budget.entry_budget.reserved_usd) },
      { id: "financial_unsettled_until_fills", expected: "false",
        actual: String(admission.post_ack_financial_complete) },
    ],
    events: [
      { stage: "public_trade_entered", owner: "TradeService",
        source_digest: hash([admission.intent_id, admission.client_order_id]) },
      { stage: "reserve_committed", owner: "NativeAccountSQL+RuntimeSession/SQLiteDO",
        source_digest: hash([admission.central_reservation_key,
          admission.central_receipt_hash, admission.after_worker_budget]) },
      { stage: "begin_post_committed", owner: "NativeAccountSQL",
        source_digest: hash([admission.intent_id,
          admission.sql_begin_post_at_ms, admission.sql_post_reservation_state]) },
      { stage: "transport_send_observed", owner: "OKXRecordedHttpBoundary",
        source_digest: hash([admission.sql_exchange_order_id,
          admission.create_posts_total, admission.client_order_id]) },
    ],
    readbacks: {
      sql: [{ revision: admission.sql_revision,
        digest: hash([admission.sql_begin_post_at_ms, admission.sql_post_reservation_state,
          admission.sql_exchange_order_id, admission.plan_hash]) }],
      risk_book: [{ revision: worker.risk_book_sequence,
        digest: hash(worker.budget) }],
    },
    outbound_sends: [{
      account: admission.account_uid,
      intent: admission.intent_id,
      operation_kind: "create",
      count: 1,
    }],
  };
}
