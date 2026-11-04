// T10-07: fail-closed integration of the existing TWO real prerequisite
// cohorts. This file must NOT report mandatory PASS until the actual Native
// account SQL and source-backed 8->4 RELEASE share the same Worker campaign.
// Worker-only reservations are not Native orders and have no fake SQL owner.
import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { readFile, mkdir, writeFile } from "node:fs/promises";
import { resolve } from "node:path";
import { actualNativeWorker } from "../native-workerd.mjs";

const exec = promisify(execFile);

export async function run({ caseId, root, signal, artifactsDir }) {
  assert.equal(caseId, "T10-07");
  assert.ok(artifactsDir, "T10-07 requires retained stage artifacts");
  const dir = resolve(artifactsDir, "T10-07");
  await mkdir(dir, { recursive: true });

  // Component test: actual production RuntimeSession/SQLite DO receives
  // three competing 4 USD requests and reopens the SAME persisted storage.
  // Its proof has no venue send authority and cannot constitute RELEASE.
  const { stdout } = await exec(process.execPath, [
    resolve(root, "scripts/trade-integration/worker-shared-cap.mjs"),
    "--concurrent", "--artifacts", dir,
  ], { cwd: root, signal, timeout: 120_000,
    maxBuffer: 4 * 1024 * 1024, windowsHide: true });
  const output = stdout.trim().split(/\r?\n/).filter(Boolean);
  assert.equal(output.length, 1, "one Worker prerequisite terminal record");
  const terminal = JSON.parse(output[0]);
  assert.equal(terminal.full_t10_07_accepted, false);
  assert.equal(terminal.concurrent_three_way, "PASS");
  const component = JSON.parse(await readFile(
    resolve(dir, "b1-worker-shared-cap.json"), "utf8"));
  const three = component.concurrent_worker;
  assert.equal(component.full_t10_07_accepted, false);
  assert.equal(three.schema, "okx.t10-worker-concurrent-cap/v1");
  assert.deepEqual(three.statuses, ["HELD", "HELD", "DENIED"]);
  assert.equal(three.held_count, 2);
  assert.equal(three.denied_count, 1);
  assert.equal(three.denied_reason, "CAMPAIGN_ENTRY_BUDGET_EXHAUSTED");
  assert.equal(three.limit_usd, "10");
  assert.equal(three.reserved_usd, "8");
  assert.equal(three.remaining_usd, "2");
  assert.equal(three.consumed_usd, "0");
  assert.equal(three.persisted_generation_advanced, true);
  await writeFile(resolve(dir, "worker-component-confirmed.json"),
    JSON.stringify({
      schema: "okx.t10-07-component-checkpoint/v1",
      status: "WORKER_COMPONENT_ONLY", same_campaign_native_release: false,
      command_count: three.command_count, statuses: three.statuses,
      denied_reason: three.denied_reason, reserved_usd: three.reserved_usd,
      remaining_usd: three.remaining_usd,
      persisted_sequence: three.persisted_sequence,
    }, null, 2) + "\n");

  // Different, independent Native public TradeService cohort. Two distinct
  // verified account SQL owners share one Worker campaign. B's over-cap
  // request is denied centrally with zero extra venue POST; a smaller B
  // request can then use the remaining shared capacity.
  process.env.OKX_T10_07_NATIVE_PROBE = "1";
  process.env.OKX_B1_NATIVE_CAP_PROBE = "1";
  process.env.OKX_B1_SECOND_POSITIVE_PROBE = "1";
  process.env.OKX_T09_MA_CONCURRENT_PROBE = "1";
  const t = await actualNativeWorker(root, signal, {
    statusProbe: true, secondAdmission: true,
  });
  const negative = t.nativeB1Cap, positive = t.nativeB1Positive;
  const both = t.nativeMAConcurrent, persisted = t.workerSecondAdmission;
  assert.ok(negative && positive && both && persisted,
    "public Native A/B account SQL and real Worker budget required");
  assert.equal(t.cohort_execution.reused, false,
    "T10-07 needs an independent Native execution, not T09-MA's cached run");
  assert.equal(negative.denial_origin, "CENTRAL");
  assert.equal(negative.denial_reason, "CAMPAIGN_ENTRY_BUDGET_EXHAUSTED");
  assert.equal(negative.send_state, "NOT_STARTED");
  assert.equal(negative.outbound_posts_after, negative.outbound_posts_before);
  assert.notEqual(both.a_account_uid, both.b_account_uid);
  assert.notEqual(both.a_intent_id, both.b_intent_id);
  assert.equal(both.a_sql_state, "ACKNOWLEDGED");
  assert.equal(both.b_sql_state, "ACKNOWLEDGED");
  assert.equal(both.worker_shared_cap_usd, "10");
  assert.equal(both.worker_reserved_usd, "8.002");
  assert.equal(both.worker_consumed_usd, "1.0005");
  assert.equal(positive.after_budget.entry_budget.remaining_usd, "0.9975");
  assert.equal(persisted.budget.entry_budget.reserved_usd, "8.002");
  assert.equal(positive.outbound_posts_after, 5);
  assert.ok(Number.isSafeInteger(both.worker_sequence) &&
    both.worker_sequence > 0);
  await writeFile(resolve(dir, "native-prerequisite-confirmed.json"),
    JSON.stringify({
      schema: "okx.t10-07-native-checkpoint/v1",
      status: "NATIVE_SHARED_CAP_ONLY", source_backed_release: false,
      same_campaign_as_worker_three_by_four: false,
      cohort_transcript_sha256: t.transcript_sha256,
      a_intent_id: both.a_intent_id, b_intent_id: both.b_intent_id,
      a_account_uid: both.a_account_uid, b_account_uid: both.b_account_uid,
      native_worker_sequence: both.worker_sequence,
      native_reserved_usd: both.worker_reserved_usd,
      native_consumed_usd: both.worker_consumed_usd,
      negative_reason: negative.denial_reason,
      negative_extra_posts: negative.outbound_posts_after -
        negative.outbound_posts_before,
    }, null, 2) + "\n");

  // The two components intentionally use different campaigns. They do NOT
  // prove that a real Native SQL RELEASE of either 4 USD reservation occurred.
  // Once the public Native owner implements that SAME-campaign witness, wire
  // it here, assert 8->4/remaining 6/zero extra POST and return ordered stages,
  // SQL+Worker revisions, and source digests. Never manufacture a PASS now.
  throw Error("T10_07_SAME_CAMPAIGN_NATIVE_SOURCE_BACKED_RELEASE_NOT_PROVEN");
}
