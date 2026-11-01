// T09.1 full acceptance is blocked until the same production owners have
// proven protected exposure, close, settlement, release and restart.
import assert from "node:assert/strict";
import { actualNativeWorker } from "../native-workerd.mjs";

export async function run({root, signal}) {
  const {nativeFinancial: money, nativeReplay: executed,
    nativePostFill: status, nativeClose: close, workerPostFill: central} =
    await actualNativeWorker(root, signal, {statusProbe:true});
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
  // A Close ACK without its independent fills, flat venue position, stop
  // cleanup and central RELEASE is NOT a completed terminal lifecycle.
  throw Error("T09_1_CLOSE_FILL_PROTECTION_RELEASE_RESTART_UNPROVEN");
}
