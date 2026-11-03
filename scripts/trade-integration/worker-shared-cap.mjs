#!/usr/bin/env node
// #449 T10-07 central-only regression: actual RuntimeSession + persisted
// SQLite DO with two authenticated account identities competing on one
// campaign. Does NOT certify public TradeService, native venue sources,
// signed SQL release receipts, or the full mandatory T09-MA/T10-07 case.
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";

const run = promisify(execFile);
const root = fileURLToPath(new URL("../../", import.meta.url));
let stdout;
try {
  ({ stdout } = await run(process.execPath, [
    resolve(root, "cloudflare", "okx-mcp", "scripts", "test-runtime-do.mjs"),
  ], {
    cwd: root,
    env: { ...process.env,
      OKX_NATIVE_WORKER_INTEGRATION: "0",
      OKX_T09_4_MANAGED_ADD_PROBE: "0",
      OKX_T10_7_SHARED_CAP_PROBE: "1",
    },
    timeout: 120_000,
    maxBuffer: 8 * 1024 * 1024,
    windowsHide: true,
  }));
} catch (error) {
  const cause = String(error.stderr ?? error.message ?? "SHARED_CAP_CHILD_FAILED");
  throw Error("T10_7_WORKER_PROBE_FAILED:" + cause.slice(-900));
}
const marker = "T10_7_WORKER_SHARED_CAP_V1:";
const matches = stdout.split(/\r?\n/).filter(line=>line.startsWith(marker));
assert.equal(matches.length, 1, "exactly one authentic Workerd cap receipt required");
assert.match(stdout, /"suite":"runtime-session-sqlite-do","status":"PASS"/);
const evidence = JSON.parse(matches[0].slice(marker.length));
assert.equal(evidence.schema, "okx.t10-shared-worker-cap/v1");
assert.match(evidence.account_a, /^okx:real:[a-f0-9]{64}$/);
assert.match(evidence.account_b, /^okx:real:[a-f0-9]{64}$/);
assert.notEqual(evidence.account_a, evidence.account_b);
assert.equal(evidence.instrument_id, "BTC-USDT-SWAP");
assert.equal(evidence.a_strategy,"native_alpha");
assert.equal(evidence.b_strategy,"native_beta");
assert.notEqual(evidence.a_strategy,evidence.b_strategy);
assert.ok(Number.isSafeInteger(evidence.policy_revision) && evidence.policy_revision > 2);
assert.equal(evidence.saved_strategy_ids[evidence.account_a],evidence.a_strategy);
assert.equal(evidence.saved_strategy_ids[evidence.account_b],evidence.b_strategy);
assert.equal(evidence.first_status, "HELD");
assert.equal(evidence.second_status, "HELD");
assert.equal(evidence.third_status, "DENIED");
assert.equal(evidence.third_reason, "CAMPAIGN_ENTRY_BUDGET_EXHAUSTED");
assert.equal(evidence.replay_status, "HELD");
assert.equal(evidence.replay_reservation_identical, true);
assert.equal(evidence.persisted_a_state, "HELD");
assert.equal(evidence.persisted_b_state, "HELD");
for (const budget of [evidence.before_budget, evidence.after_budget]) {
  assert.equal(budget.entry_budget.limit_usd, "10");
  assert.equal(budget.entry_budget.reserved_usd, "8");
  assert.equal(budget.entry_budget.remaining_usd, "2");
  assert.equal(budget.entry_budget.consumed_usd, "0");
  assert.equal(budget.reserved_loss_usd, "2");
  assert.equal(budget.remaining_loss_usd, "8");
}
assert.ok(evidence.after_sequence >= evidence.before_sequence);
assert.ok(evidence.connection_generation_after > evidence.connection_generation_before);
console.log("T10_7_WORKER_SHARED_CAP_COMPONENT_PASS:" + JSON.stringify({
  schema: evidence.schema, subject: "real_sqlite_do_concurrent_reserve_only",
  account_count: 2, simultaneous_held: 2,
  third_denied_reason: evidence.third_reason,
  reserved_usd: evidence.after_budget.entry_budget.reserved_usd,
  remaining_usd: evidence.after_budget.entry_budget.remaining_usd,
  retained_on_restart: true, native_trade_acceptance: "NOT_RUN",
  transcript_sha256: createHash("sha256").update(stdout).digest("hex"),
}));
