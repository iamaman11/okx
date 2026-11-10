#!/usr/bin/env node
// B1 / #449 T10-07: independently exercised WORKER component prerequisite.
// This is NOT a T10-07 required-case PASS: Native public Execute, SQL owner,
// venue send/no-send and source-backed RELEASE still need the same campaign.
// The only monetary authority used here is production RuntimeSession/SQLite DO.
import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { fileURLToPath } from "node:url";
import { resolve } from "node:path";
import { writeFile, mkdir } from "node:fs/promises";

const exec = promisify(execFile);
const repo = fileURLToPath(new URL("../../", import.meta.url));
const test = resolve(repo, "cloudflare/okx-mcp/scripts/test-runtime-do.mjs");
const marker = "T10_7_WORKER_SHARED_CAP_V1:";
const concurrentMarker = "T10_7_WORKER_CONCURRENT_CAP_V1:";
const terminalMarker = '{"suite":"runtime-session-sqlite-do","status":"PASS"';

function singleLine(stdout, prefix) {
  const matches = stdout.split(/\r?\n/).filter(s => s.startsWith(prefix));
  assert.equal(matches.length, 1, "one authenticated worker receipt required: " + prefix);
  return JSON.parse(matches[0].slice(prefix.length));
}
function prove(e) {
  assert.equal(e.schema, "okx.t10-shared-worker-cap/v1");
  assert.match(e.account_a, /^okx:real:[a-f0-9]{64}$/);
  assert.match(e.account_b, /^okx:real:[a-f0-9]{64}$/);
  assert.notEqual(e.account_a, e.account_b);
  assert.equal(e.instrument_id, "BTC-USDT-SWAP");
  assert.equal(e.a_strategy, "native_alpha");
  assert.equal(e.b_strategy, "native_beta");
  assert.notEqual(e.a_strategy, e.b_strategy);
  assert.equal(e.saved_strategy_ids[e.account_a], e.a_strategy);
  assert.equal(e.saved_strategy_ids[e.account_b], e.b_strategy);
  assert.ok(Number.isSafeInteger(e.policy_revision) && e.policy_revision > 2);
  assert.equal(e.same_intent_id_cross_account, true,
    "same intent ID must remain valid across two different verified accounts");
  assert.equal(e.independent_account_scope, true);
  assert.equal(e.distinct_reservation_keys, true,
    "central RiskBook must never reuse a different account's reservation");
  assert.equal(e.first_status, "HELD");
  assert.equal(e.second_status, "HELD");
  assert.equal(e.third_status, "DENIED");
  assert.equal(e.third_reason, "CAMPAIGN_ENTRY_BUDGET_EXHAUSTED");
  assert.equal(e.foreign_strategy_status, "DENIED");
  assert.equal(e.foreign_strategy_reason, "STRATEGY_NOT_ENABLED_FOR_ACCOUNT");
  assert.equal(e.foreign_strategy_no_budget_reserved, true);
  assert.equal(e.replay_status, "HELD");
  assert.equal(e.replay_reservation_identical, true);
  assert.equal(e.without_native_sql_proof_status,"DENIED");
  assert.equal(e.without_native_sql_proof_reason,"INVALID_UNSENT_RELEASE_COMMAND");
  assert.equal(e.foreign_sql_owner_release_status,"DENIED");
  assert.ok(["INVALID_UNSENT_SQL_PROOF",
    "UNSENT_RELEASE_IDENTITY_OR_STATE_MISMATCH"].includes(
      e.foreign_sql_owner_release_reason));
  assert.equal(e.denied_release_sequence_unchanged,true);
  assert.equal(e.persisted_a_state, "HELD");
  assert.equal(e.persisted_b_state, "HELD");
  assert.equal(e.before_budget.campaign_limit_usd, "10");
  assert.equal(e.before_budget.entry_budget.limit_usd, "10");
  assert.equal(e.after_budget.entry_budget.limit_usd, "10");
  for (const b of [e.before_budget, e.after_budget]) {
    assert.equal(b.entry_budget.consumed_usd, "0");
    assert.equal(b.entry_budget.reserved_usd, "8");
    assert.equal(b.entry_budget.remaining_usd, "2");
    assert.equal(b.reserved_loss_usd, "2");
    assert.equal(b.remaining_loss_usd, "8");
  }
  assert.ok(Number.isSafeInteger(e.before_sequence) && e.before_sequence > 0);
  assert.ok(Number.isSafeInteger(e.after_sequence) && e.after_sequence >= e.before_sequence);
  assert.ok(Number.isSafeInteger(e.connection_generation_before));
  assert.ok(e.connection_generation_after > e.connection_generation_before);
}
function proveConcurrent(e) {
  assert.equal(e.schema,"okx.t10-worker-concurrent-cap/v1");
  assert.equal(e.actual_worker_sqlite_do,true);
  assert.equal(e.command_count,3);
  assert.equal(new Set(e.command_ids).size,3);
  assert.deepEqual(e.statuses,["HELD","HELD","DENIED"]);
  assert.equal(e.held_count,2);
  assert.equal(e.denied_count,1);
  assert.equal(e.denied_reason,"CAMPAIGN_ENTRY_BUDGET_EXHAUSTED");
  assert.equal(e.two_distinct_verified_accounts,true);
  assert.equal(e.held_reservations_distinct,true);
  assert.equal(e.limit_usd,"10");
  assert.equal(e.reserved_usd,"8");
  assert.equal(e.remaining_usd,"2");
  assert.equal(e.consumed_usd,"0");
  assert.ok(Number.isSafeInteger(e.before_sequence) && e.before_sequence>0);
  assert.ok(e.persisted_sequence>=e.before_sequence);
  assert.equal(e.persisted_generation_advanced,true);
}
// Self-test MUST reject an incorrect budget; never substitute synthetic
// evidence for the actual Worker run.
if (process.argv.includes("--self-test")) {
  const bad = { schema: "okx.t10-shared-worker-cap/v1", before_budget: {
    campaign_limit_usd: "10", entry_budget: { reserved_usd: "12" } } };
  assert.throws(() => prove(bad));
  assert.throws(() => proveConcurrent({
    schema:"okx.t10-worker-concurrent-cap/v1",actual_worker_sqlite_do:true,
    command_count:3,command_ids:["a","b","c"],statuses:["HELD","HELD","HELD"],
    held_count:3,denied_count:0,denied_reason:null,
    two_distinct_verified_accounts:true,held_reservations_distinct:true,
    limit_usd:"10",reserved_usd:"12",remaining_usd:"0",consumed_usd:"0",
    before_sequence:1,persisted_sequence:2,persisted_generation_advanced:true,
  }));
  process.stdout.write("B1_WORKER_ORACLE_SELF_TEST_PASS\n");
} else {
  const concurrentMode=process.argv.includes("--concurrent");
  const { stdout, stderr } = await exec(process.execPath, [test], {
    cwd: resolve(repo, "cloudflare/okx-mcp"),
    env: { ...process.env, OKX_T10_7_SHARED_CAP_PROBE: "1",
      OKX_T10_7_CONCURRENT_CAP_PROBE: concurrentMode ? "1" : "0",
      OKX_T10_03_LOST_RESERVE_REPLY_PROBE: "0",
      OKX_NATIVE_WORKER_INTEGRATION: "0", OKX_T09_4_MANAGED_ADD_PROBE: "0" },
    timeout: 120_000, maxBuffer: 4 * 1024 * 1024,
  });
  const result = singleLine(stdout, marker);
  prove(result);
  const concurrent = concurrentMode ? singleLine(stdout, concurrentMarker) : null;
  if (concurrentMode) proveConcurrent(concurrent);
  const terminalLines = stdout.split(/\r?\n/).filter(x => x.startsWith(terminalMarker));
  assert.equal(terminalLines.length, 1,
    "production RuntimeSession terminal assertion is mandatory");
  const terminal = JSON.parse(terminalLines[0]);
  assert.equal(terminal.real_durable_object, true);
  assert.equal(terminal.persisted_generation, true);
  assert.equal(terminal.authentic_reserve, true);
  assert.equal(terminal.authentic_begin_post, false,
    "isolated cap probe must never claim a Native BeginPost");
  assert.equal(terminal.reserved_loss_usd, "2");
  assert.equal(terminal.reserved_entry_usd, "8");
  assert.equal(terminal.risk_monetary_post_count, 0);
  const evidence = {
    schema: "okx.b1-worker-cap-component/v1",
    status: "PASS", requirement: "#449:T10-07:WORKER_ONLY",
    full_t10_07_accepted: false, native_trade_exercised: false,
    source: "RuntimeSession/SQLiteDO",
    worker: result,
    concurrent_worker: concurrent,
    limitations: ["Native public Execute + SQL owner not exercised",
      "Malformed and foreign SQL release commands were denied, not source-backed release",
      "No source-backed Native RELEASE proof: reservations stay HELD"],
  };
  const outputIndex = process.argv.indexOf("--artifacts");
  if (outputIndex !== -1) {
    const target = process.argv[outputIndex + 1];
    assert.ok(target, "--artifacts requires an output directory");
    await mkdir(target, { recursive: true });
    await writeFile(resolve(target, "b1-worker-shared-cap.json"),
      JSON.stringify(evidence, null, 2) + "\n");
  }
  process.stdout.write(JSON.stringify({
    schema: evidence.schema, status: "PASS", full_t10_07_accepted: false,
    entry_reserved_usd: result.after_budget.entry_budget.reserved_usd,
    entry_remaining_usd: result.after_budget.entry_budget.remaining_usd,
    concurrent_three_way: concurrentMode ? "PASS" : "NOT_RUN",
    first: result.first_status, second: result.second_status,
    third: result.third_reason, worker_restart: "PASS",
  }) + "\n");
  if (stderr.trim()) process.stderr.write(stderr.slice(-3000) + "\n");
}
