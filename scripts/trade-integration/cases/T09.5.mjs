// Replays the same Public TradeService idempotency key on the shared
// production Native SQL + SQLite-backed Worker RiskBook assembly.
import assert from "node:assert/strict";
import { actualNativeWorker, hash } from "../native-workerd.mjs";

export async function run({ caseId, root, signal }) {
  const { nativeReplay: native, workerReplay: worker } =
    await actualNativeWorker(root, signal);
  assert.equal(native.opened_intent_id, native.replayed_intent_id);
  assert.equal(native.opened_intent_id, native.sql_intent_id);
  assert.equal(native.sql_state, "ACKNOWLEDGED");
  assert.ok(Number.isSafeInteger(native.sql_revision) && native.sql_revision > 0);
  assert.equal(native.created_posts, 1);
  assert.equal(native.reserved_entry_usd_before_replay, "5.0005");
  assert.equal(native.reserved_entry_usd_after_replay,
    native.reserved_entry_usd_before_replay);
  assert.equal(worker.budget.entry_budget.reserved_usd,
    native.reserved_entry_usd_after_replay);
  assert.equal(worker.budget.entry_budget.consumed_usd, "0",
    "unsettled ACK must never become an invented financial debit");
  assert.ok(Number.isSafeInteger(worker.connection_generation)
    && worker.connection_generation > 0);
  return {
    case_id:caseId, status:"PASS",
    assertions:[
      { id:"identical_intent_from_independent_sql_readback",
        expected:native.opened_intent_id, actual:native.sql_intent_id },
      { id:"identical_replay_intent",
        expected:native.opened_intent_id, actual:native.replayed_intent_id },
      { id:"one_possible_create_post", expected:"1", actual:String(native.created_posts) },
      { id:"no_second_risk_reserve", expected:"5.0005",
        actual:native.reserved_entry_usd_after_replay },
      { id:"worker_cross_owner_reserved_entry", expected:"5.0005",
        actual:worker.budget.entry_budget.reserved_usd },
    ],
    events:[
      { stage:"public_trade_entered", owner:"TradeService+AccountStore",
        source_digest:hash({intent:native.opened_intent_id,sql_revision:native.sql_revision}) },
      { stage:"idempotent_replay_verified", owner:"TradeService+RuntimeSession",
        source_digest:hash({native,worker}) },
    ],
    readbacks:{
      sql:[{revision:native.sql_revision,digest:hash(native)}],
      risk_book:[{revision:worker.connection_generation,digest:hash(worker.budget)}],
    },
    outbound_sends:[{
      account:native.account_uid,intent:native.opened_intent_id,
      operation_kind:"create",count:native.created_posts,
    }],
  };
}
