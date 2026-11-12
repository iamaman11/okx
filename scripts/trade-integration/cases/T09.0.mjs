// Production bootstrap and source-capture readbacks: #449-r6 T09.0.
// Uses two actual AccountStore SQL owners and one SQLite-backed Worker DO.
import assert from "node:assert/strict";
import { actualNativeWorker, hash } from "../native-workerd.mjs";

export async function run({ caseId, root, signal }) {
  const { nativeBootstrap: sql, workerBootstrap: worker, transcript_sha256 } =
    await actualNativeWorker(root, signal);
  assert.equal(sql.accounts.length, 2);
  assert.equal(sql.sqlite_files.length, 2);
  assert.notEqual(sql.sqlite_files[0], sql.sqlite_files[1]);
  assert.ok(sql.sqlite_files.every(path => path.endsWith("account.sqlite")));
  assert.deepEqual(sql.accounts.map(a => a.alias).sort(), ["fixture-a", "fixture-b"]);
  assert.notEqual(sql.accounts[0].verified_uid, sql.accounts[1].verified_uid);
  assert.ok(sql.accounts.every(a =>
    Number.isSafeInteger(a.private_generation) && a.private_generation > 0
    && Number.isSafeInteger(a.source_capture_ms) && a.source_capture_ms > 0
    && a.active_intent_count === 0));
  assert.deepEqual(
    sql.accounts.map(a => "okx:real:" + a.verified_uid).sort(),
    worker.account_keys,
    "native account identities must match Worker's actual enrolled accounts",
  );
  assert.equal(worker.before_budget.entry_budget, null);
  assert.equal(worker.opening_budget.entry_budget.consumed_usd, "0");
  assert.equal(worker.opening_budget.entry_budget.opening_source_digest,
    worker.opening_source_digest);
  assert.ok(Number.isSafeInteger(worker.connection_generation)
    && worker.connection_generation > 0);
  assert.ok(Number.isSafeInteger(worker.risk_book_sequence)
    && worker.risk_book_sequence >= 0,
    "risk-book revision must come from Worker SUMMARY, not WSS generation");

  const assertions = [
    { id: "distinct_native_sql_accounts", expected: "2",
      actual: String(new Set(sql.sqlite_files).size) },
    { id: "verified_accounts_in_central_riskbook", expected: "2",
      actual: String(worker.account_keys.length) },
    { id: "initial_active_intents", expected: "0",
      actual: String(sql.accounts.reduce((sum, a) => sum + a.active_intent_count, 0)) },
    { id: "shared_campaign_cap_usd", expected: "10",
      actual: String(worker.opening_budget.campaign_limit_usd) },
    { id: "initial_consumed_entry_usd", expected: "0",
      actual: worker.opening_budget.entry_budget.consumed_usd },
  ];
  return {
    case_id: caseId, status: "PASS", assertions,
    provenance: { schema:"okx.native-workerd-transcript/v1", sha256:transcript_sha256 },
    events: [
      { stage: "native_sql_bootstrap_verified", owner: "NativeAccountSQL",
        source_digest: hash(sql.sqlite_files) },
      { stage: "private_source_captured", owner: "PrivateWsCoordinator+AccountStore",
        source_digest: hash(sql.accounts) },
      { stage: "worker_campaign_opening_verified", owner: "RuntimeSession/SQLiteDO",
        source_digest: hash(worker) },
    ],
    readbacks: {
      sql: sql.accounts.map(a => ({
        revision: a.private_generation, digest: hash(a),
      })),
      risk_book: [{
        revision: worker.risk_book_sequence, digest: hash(worker.opening_budget),
      }],
    },
    outbound_sends: [],
  };
}
