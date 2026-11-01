// T09.1 is deliberately fail-closed until the SAME production Native/Worker
// assembly has proven protection, close, settlement, release and restart.
// Its entry money readback is genuine; no partial source can stand in for PASS.
import assert from "node:assert/strict";
import { actualNativeWorker } from "../native-workerd.mjs";

export async function run({root, signal}) {
  const {nativeFinancial: money, nativeReplay: executed, workerReplay: central} =
    await actualNativeWorker(root, signal);
  assert.equal(money.intent_id, executed.opened_intent_id,
    "financial sources must belong to the exact public Execute intent");
  assert.equal(money.fills, 1);
  assert.equal(money.bills, 1);
  assert.equal(money.notional_usdt, "1");
  assert.equal(money.fee_usdt, "0.0005");
  assert.match(money.source_digest, /^[0-9a-f]{64}$/);
  assert.equal(executed.created_posts, 1);
  assert.equal(central.budget.entry_budget.reserved_usd, "5.0005");
  assert.equal(central.budget.entry_budget.consumed_usd, "0");
  assert.equal(money.state, "FILL_AND_BILL_VERIFIED_CLOSE_RELEASE_PENDING");
  // No fabricated SQL protection, terminal close or Worker RELEASED receipt.
  // Replacing this error with PASS requires actual production readbacks for
  // the remaining mandatory T09.1 transitions, not JS-generated events.
  throw Error("T09_1_PROTECTION_CLOSE_SETTLEMENT_RELEASE_RESTART_UNPROVEN");
}
