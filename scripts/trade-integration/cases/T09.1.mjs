// T09.1 full acceptance is blocked until the same production owners have
// proven protected exposure, close, settlement, release and restart.
import assert from "node:assert/strict";
import { actualNativeWorker } from "../native-workerd.mjs";

export async function run({root, signal}) {
  const {nativeFinancial: money, nativeReplay: executed,
    nativePostFill: status, workerPostFill: central} =
    await actualNativeWorker(root, signal, {statusProbe:true});
  assert.equal(money.intent_id, executed.opened_intent_id);
  assert.equal(status.intent_id, executed.opened_intent_id);
  assert.equal(money.fills, 1);
  assert.equal(money.bills, 1);
  assert.equal(money.notional_usdt, "1");
  assert.equal(money.fee_usdt, "0.0005");
  assert.match(money.source_digest, /^[0-9a-f]{64}$/);
  assert.equal(executed.created_posts, 1);
  assert.equal(status.created_posts, 1, "status must not repost an entry");
  assert.equal(status.entry_fill_evidence?.state,
    "verified_native_fill_and_bill_fee_sources");
  assert.equal(status.protection?.quantity, "0.1",
    "a real venue protection readback must cover confirmed exposure");
  assert.equal(status.incremental_entry_settlement?.state,
    "verified_or_no_change");
  assert.equal(status.worker_budget?.entry_budget?.consumed_usd,
    central.budget?.entry_budget?.consumed_usd);
  assert.equal(status.worker_budget?.entry_budget?.reserved_usd,
    central.budget?.entry_budget?.reserved_usd);
  // The test must not claim a complete T09.1 while a live position has not
  // been closed, its protection removed and central risk RELEASED.
  throw Error("T09_1_CLOSE_RELEASE_RESTART_UNPROVEN");
}
