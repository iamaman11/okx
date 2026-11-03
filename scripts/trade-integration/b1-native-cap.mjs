#!/usr/bin/env node
// #449 B1 partial Native/Worker evidence, never a required T09-MA/T10-07 PASS.
// Public Native TradeService handles separate verified UIDs against the SAME
// real persisted RuntimeSession/SQLiteDO and records an independently
// source-priced B admission that is denied by the central campaign cap.
// No test-only risk or SQL mutation and no venue emulator.
import assert from "node:assert/strict";
import { mkdir, writeFile } from "node:fs/promises";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { actualNativeWorker } from "./native-workerd.mjs";

const root = fileURLToPath(new URL("../../", import.meta.url));
process.env.OKX_B1_NATIVE_CAP_PROBE = "1";
const transcript = await actualNativeWorker(root, AbortSignal.timeout(260_000), {
  statusProbe: true, secondAdmission: true,
});
const b = transcript.nativeB1Cap;
const a = transcript.nativeSecondAdmission;
assert.ok(b && a, "real Native B and A account receipts required");
assert.match(b.account_a_uid, /^[a-f0-9]{64}$/);
assert.match(b.account_b_uid, /^[a-f0-9]{64}$/);
assert.notEqual(b.account_a_uid, b.account_b_uid);
assert.equal(b.account_a_uid, a.account_uid);
assert.equal(b.same_idempotency_key, true);
assert.notEqual(b.a_intent_id, b.b_intent_id);
assert.equal(b.a_intent_id, a.intent_id);
assert.equal(b.b_sql_state, "REJECTED");
assert.equal(b.b_sql_reserve_held, false);
assert.equal(b.b_sql_quote_upper_usd, "3.0015");
assert.equal(b.b_sql_admission_upper_usd, "3.0015");
assert.match(b.b_signed_fee_ts, /^[0-9]{12,}$/);
assert.equal(b.denial_origin, "CENTRAL");
assert.equal(b.denial_reason, "CAMPAIGN_ENTRY_BUDGET_EXHAUSTED");
assert.equal(b.send_state, "NOT_STARTED");
assert.equal(b.outbound_posts_before, 4);
assert.equal(b.outbound_posts_after, 4);
assert.equal(b.before_budget.campaign_limit_usd, "10");
for (const budget of [b.before_budget, b.after_budget]) {
  assert.equal(budget.entry_budget.limit_usd, "10");
  assert.equal(budget.entry_budget.reserved_usd, "7.0015");
  assert.equal(budget.entry_budget.consumed_usd, "1.0005");
  assert.equal(budget.entry_budget.remaining_usd, "1.998");
}
assert.equal(b.after_budget.reserved_loss_usd,
  b.before_budget.reserved_loss_usd);
assert.ok(Number.isSafeInteger(b.before_sequence) &&
  Number.isSafeInteger(b.after_sequence) &&
  b.after_sequence >= b.before_sequence);
const result = {
  schema: "okx.b1-native-shared-cap-prerequisite/v1",
  status: "PASS", exact_requirement: "#449:T09-MA/T10-07:PARTIAL",
  required_case_accepted: false, native_public_trade_exercised: true,
  worker_persisted_sqlite: true, native_account_sql_independent: true,
  negative_b_venue_post_count: 0,
  limitations: [
    "No two positive Native Execute attempts with two distinct accounts",
    "No three-by-four Native admission in same campaign",
    "No Native-source-proven release one held reservation 8->4",
  ],
  evidence: b,
  a_evidence: { intent_id: a.intent_id, account_uid: a.account_uid },
  transcript_sha256: transcript.transcript_sha256,
};
const arg = process.argv.indexOf("--artifacts");
if (arg >= 0) {
  const dir = process.argv[arg + 1];
  assert.ok(dir, "artifacts directory required");
  await mkdir(dir, { recursive: true });
  await writeFile(resolve(dir, "b1-native-shared-cap.json"),
    JSON.stringify(result, null, 2) + "\n");
}
process.stdout.write(JSON.stringify({
  schema: result.schema, status: result.status,
  native_public_trade_exercised: true,
  required_case_accepted: false, a_uid: b.account_a_uid,
  b_uid: b.account_b_uid, central_denial: b.denial_reason,
  venue_posts_added: 0,
}) + "\n");
