#!/usr/bin/env node
// #449 B1 partial Native/Worker evidence, never a required T09-MA/T10-07 PASS.
// Public Native TradeService handles separate verified UIDs against the SAME
// real persisted RuntimeSession/SQLiteDO and records an independently
// source-priced B admission that is denied by the central campaign cap.
// No test-only risk or SQL mutation and no venue emulator.
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { mkdir, writeFile } from "node:fs/promises";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { actualNativeWorker } from "./native-workerd.mjs";

const root = fileURLToPath(new URL("../../", import.meta.url));
process.env.OKX_B1_NATIVE_CAP_PROBE = "1";
const artifactArg = process.argv.indexOf("--artifacts");
const artifactDir = artifactArg >= 0 ? process.argv[artifactArg + 1] : null;
if (artifactArg >= 0) {
  assert.ok(artifactDir, "artifacts directory required");
  await mkdir(artifactDir, { recursive: true });
}
let transcript;
try {
  transcript = await actualNativeWorker(root, AbortSignal.timeout(260_000), {
    statusProbe: true, secondAdmission: true,
  });
} catch (error) {
  // Retain a typed, bounded failure even when the nested Rust child exits.
  // Never persist raw HTTP bytes, authentication or an unredacted stack.
  if (artifactDir) {
    const message = String(error?.message ?? "");
    const detail = error?.b1Diagnostic ?? {};
    const reasonCode = detail.reason_code ??
      message.match(/TRANSCRIPT_[A-Z_]+/)?.[0] ??
      (message.startsWith("B1_POSITIVE_REFUSED:") ? "B1_POSITIVE_REFUSED" : "CHILD_FAILED");
    const failure = {
      schema: "okx.b1-native-failure/v1", status: "FAIL",
      phase: "native_worker", reason_code: reasonCode,
      source_actor: detail.source_actor ?? null,
      source_route: detail.source_route ?? null,
      a_algo_success_reads_total: detail.a_algo_success_reads_total ?? null,
      a_algo_success_reads_since_positive: detail.a_algo_success_reads_since_positive ?? null,
      a_algo_scopes: detail.a_algo_scopes ?? null,
      outbound_posts: detail.outbound_posts ?? null,
      message_sha256: createHash("sha256").update(message).digest("hex"),
      commit_sha: process.env.GITHUB_SHA ?? null,
    };
    await writeFile(resolve(artifactDir, "b1-native-failure.json"),
      JSON.stringify(failure, null, 2) + "\n");
  }
  throw error;
}
const b = transcript.nativeB1Cap;
const a = transcript.nativeSecondAdmission;
const positiveMode = process.env.OKX_B1_SECOND_POSITIVE_PROBE === "1";
const bPositive = positiveMode ? transcript.nativeB1Positive : null;
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
if (positiveMode) {
  assert.ok(bPositive,"source-backed B positive Native/Worker receipt required");
  assert.equal(bPositive.account_a_uid,b.account_a_uid);
  assert.equal(bPositive.account_b_uid,b.account_b_uid);
  assert.notEqual(bPositive.b_intent_id,b.b_intent_id);
  assert.notEqual(bPositive.b_intent_id,a.intent_id);
  assert.equal(bPositive.b_sql_state,"ACKNOWLEDGED");
  assert.equal(bPositive.b_sql_reserve_held,true);
  assert.equal(bPositive.b_entry_quote_usd,"1.0005");
  assert.equal(bPositive.b_exchange_order_id,"recorded-b1-b-001");
  assert.equal(bPositive.outbound_posts_before,4);
  assert.equal(bPositive.outbound_posts_after,5);
  assert.equal(bPositive.before_budget.entry_budget.reserved_usd,"7.0015");
  assert.equal(bPositive.after_budget.entry_budget.reserved_usd,"8.002");
  assert.equal(bPositive.after_budget.entry_budget.remaining_usd,"0.9975");
  assert.equal(bPositive.after_budget.entry_budget.consumed_usd,"1.0005");
  assert.equal(bPositive.after_budget.campaign_limit_usd,"10");
}
const result = {
  schema: "okx.b1-native-shared-cap-prerequisite/v1",
  status: "PASS", exact_requirement: "#449:T09-MA/T10-07:PARTIAL",
  required_case_accepted: false, native_public_trade_exercised: true,
  worker_persisted_sqlite: true, native_account_sql_independent: true,
  negative_b_venue_post_count: 0,
  positive_b_venue_post_count: positiveMode ? 1 : 0,
  positive_b_accepted: positiveMode,
  positive_b_evidence: bPositive,
  limitations: [
    ...(positiveMode ? [] :
      ["No two positive Native Execute attempts with two distinct accounts"]),
    "Native A/B positive sends not yet executed concurrently",
    "No three-by-four Native admission in same campaign",
    "No Native-source-proven release one held reservation 8->4",
  ],
  evidence: b,
  a_evidence: { intent_id: a.intent_id, account_uid: a.account_uid },
  transcript_sha256: transcript.transcript_sha256,
};
if (artifactDir) {
  await writeFile(resolve(artifactDir, "b1-native-shared-cap.json"),
    JSON.stringify(result, null, 2) + "\n");
}
process.stdout.write(JSON.stringify({
  schema: result.schema, status: result.status,
  native_public_trade_exercised: true,
  required_case_accepted: false, a_uid: b.account_a_uid,
  b_uid: b.account_b_uid, central_denial: b.denial_reason,
  venue_posts_added: positiveMode ? 1 : 0,
}) + "\n");
