#!/usr/bin/env node
// The sole T09/T10 acceptance entrypoint. A missing scenario is a failure,
// never a skip or a copied component-test PASS. Cases invoke production owners
// with recorded raw HTTP/WS inputs and must return independently checked facts.
import { createHash } from "node:crypto";
import { execFileSync } from "node:child_process";
import { existsSync, readFileSync } from "node:fs";
import { mkdir, writeFile } from "node:fs/promises";
import { basename, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import os from "node:os";
import { nativeCohortSnapshot } from "./trade-integration/native-workerd.mjs";

const here = fileURLToPath(new URL(".", import.meta.url));
const root = resolve(here, "..");
const t09 = [...Array.from({ length: 9 }, (_, i) => `T09.${i}`), "T09-MA", "T09-SPEC"];
const t10 = Array.from({ length: 15 }, (_, i) => `T10-${String(i + 1).padStart(2, "0")}`);
const required = Object.freeze([...t09, ...t10]);
const mandatoryStages = Object.freeze([
  "public_trade_entered", "reserve_committed", "begin_post_committed",
  "transport_send_observed", "fill_ingested", "protection_verified",
  "settlement_committed", "release_confirmed",
]);
const failures = {
  missing: "MISSING_REQUIRED_CASE",
  timeout: "CASE_DEADLINE_EXCEEDED",
  invalid: "UNVERIFIED_CASE_EVIDENCE",
};
// Exact per-case *minimum* observed stages; T09.0 is bootstrap without a POST,
// and denial cases must never invent a venue send or settlement.
const requiredStagesByCase = new Map([
  ["T09.0", ["native_sql_bootstrap_verified", "private_source_captured", "worker_campaign_opening_verified"]],
  ["T09.1", mandatoryStages],
  ["T09.2", mandatoryStages.slice(0, 4)],
  ["T09.3", ["public_trade_entered", "fill_ingested", "protection_verified"]],
  ["T09.4", mandatoryStages.slice(0, 6)],
  ["T09.5", ["public_trade_entered", "idempotent_replay_verified"]],
  ["T09.6", ["public_trade_entered", "fill_ingested", "protection_verified", "settlement_committed", "release_confirmed"]],
  ["T09.7", ["public_trade_entered", "settlement_committed", "release_confirmed"]],
  ["T09.8", ["public_trade_entered"]],
  ["T09-MA", [
    "public_trade_entered", "a_and_b_sql_distinct",
    "concurrent_public_owner_routing", "shared_risk_book_verified",
    "cross_account_mutation_denied",
  ]],
  ["T09-SPEC", mandatoryStages.slice(0, 4)],
  ["T10-06", [
    "public_trade_entered", "reserve_committed", "begin_post_committed",
    "transport_send_observed", "concurrent_idempotency_reconciled",
  ]],
  // These are mandatory per-case causal witnesses, not credit for merely
  // having a JS case file. The future Native/Worker cases must report their
  // own owner/readback/send evidence in this order; missing stages stay RED.
  ["T10-01", [
    "public_trade_entered", "stale_source_before_admission_denied",
    "stale_source_after_held_denied", "source_denials_no_venue_send",
    "fresh_source_recovery_verified",
  ]],
  ["T10-02", [
    "managed_exposure_positive_admitted", "external_or_unowned_exposure_observed",
    "unknown_exposure_denied", "sql_worker_exposure_unchanged",
  ]],
  ["T10-03", [
    "public_trade_entered", "worker_reply_lost_after_durable_commit",
    "same_reservation_inspected", "no_duplicate_grant_or_send",
    "pending_risk_reconciled",
  ]],
  ["T10-04", [
    "public_trade_entered", "foreign_worker_reply_injected",
    "binding_error_observed", "sql_did_not_authorize_post",
    "worker_held_not_fabricated_release", "zero_venue_create_verified",
  ]],
  ["T10-05", [
    "public_trade_entered", "venue_send_observed",
    "venue_ack_lost_or_unbound", "unknown_without_resubmit",
    "same_client_order_readback_verified", "explicit_venue_reject_distinguished",
  ]],
  ["T10-07", [
    "public_trade_entered", "two_independent_held_reserves",
    "third_central_denial_without_post", "one_reservation_released",
    "shared_worker_cap_reconciled",
  ]],
  ["T10-08", [
    "partial_fill_ingested", "protection_failure_source_observed",
    "new_risk_fenced", "bounded_protective_attempt_observed",
    "late_fill_financial_reconciled",
  ]],
  ["T10-09", [
    "cancel_ack_observed", "risk_not_released_on_ack",
    "late_fill_ingested", "partial_close_recorded",
    "remaining_position_and_protection_verified",
  ]],
  ["T10-10", [
    "duplicate_fill_and_bill_replayed", "single_economic_debit_verified",
    "late_fee_or_rebate_ingested", "settlement_revision_replayed",
    "fee_upper_breach_or_pending_verified",
  ]],
  ["T10-11", [
    "native_crash_f0_through_f8_repeated", "native_process_restarts_verified",
    "durable_sql_worker_readbacks", "no_duplicate_post_or_finance",
    "unresolved_outcome_not_fabricated",
  ]],
  ["T10-12", [
    "worker_committed_before_kill", "worker_process_terminated",
    "worker_new_generation_recovered", "stale_reply_denied",
    "single_risk_mutation_verified",
  ]],
  ["T10-13", [
    "fill_ingested", "stop_after_fill_applied",
    "post_stop_new_claims_denied", "stale_reduction_blocker_verified",
    "safe_reduce_or_cancel_attempted",
  ]],
  ["T10-15", [
    "preview_source_snapshot", "held_source_revision_changed",
    "post_held_proof_rejected", "fee_change_prevents_stale_post",
    "fresh_plan_revalidated",
  ]],
  ["T10-14", [
    "public_trade_entered", "a_sql_owner_held", "b_public_status_read",
    "b_exact_venue_cancel_observed", "a_sql_owner_rejoined",
    "worker_budget_after_cancel_verified",
  ]],
]);
const entryGolden = new Map([["T09.1", "1.0005"]]);
// Independent immutable acceptance constants from #449 §4, never calculated
// from the same production finance function under test.
const concurrentGolden = new Map([["T10-06", new Map([
  ["concurrent_requests","100"],
  ["same_intent_or_proven_no_send","100"],
  ["one_venue_create","1"],
  ["changed_payload_rejected","IDEMPOTENCY_CONFLICT"],
  ["no_spurious_worker_debit","0"],
])]]);
// T10-07 is one SQL-owned campaign, not a sum of unrelated Worker and
// Native checkpoints. The runner independently pins the required economics;
// individual cases must also validate actual owner/readback/source evidence.
const sharedCapGolden = new Map([["T10-07", new Map([
  ["concurrent_requests", "3"],
  ["reserve_decisions", "HELD,HELD,DENIED"],
  ["reserved_before_release_usd", "8"],
  ["remaining_before_release_usd", "2"],
  ["reserved_after_release_usd", "4"],
  ["remaining_after_release_usd", "6"],
  ["native_sql_source_backed_release", "true"],
  ["same_worker_campaign", "true"],
  ["third_denial_extra_posts", "0"],
  ["release_extra_posts", "0"],
])]]);
const terminalGolden = new Map([
  ["T09.6", new Map([
    ["remaining_exposure", "0"], ["orphan_algo_orders", "0"],
    ["outstanding_reserve_usd", "0"], ["campaign_consumed_usd", "6.003"],
    ["entry_fees_usdt", "-0.003"], ["exit_fees_usdt", "-0.003"],
    ["verified_fee_bill_count", "7"],
  ])],
  ["T09.7", new Map([
    ["equity_a_usd", "99.994"], ["campaign_consumed_usd", "6.003"],
    ["duplicate_posts", "0"],
  ])],
  ["T09.8", new Map([
    ["campaign_consumed_usd", "6.003"], ["remaining_entry_budget_usd", "3.997"],
    ["denial_reason", "CAMPAIGN_ENTRY_BUDGET_EXHAUSTED"],
    ["requested_entry_upper_usd", "4.002"], ["new_exchange_posts", "0"],
  ])],
]);
function usage() {
  throw Error("USAGE: --list | --suite t09,t10 --artifacts DIR | --case T10-05 --artifacts DIR");
}
function args(input) {
  if (input.length === 1 && input[0] === "--list") return { list: true };
  if (input.length === 1 && input[0] === "--self-test") return { selfTest: true };
  const config = {};
  for (let i = 0; i < input.length; i += 2) {
    const key = input[i], value = input[i + 1];
    if (!["--suite", "--case", "--artifacts"].includes(key) || !value || key in config)
      usage();
    config[key] = value;
  }
  if (config["--suite"] === "t09,t10" && !config["--case"] && config["--artifacts"])
    return { selected: required, artifacts: config["--artifacts"] };
  if (required.includes(config["--case"]) && !config["--suite"] && config["--artifacts"])
    return { selected: [config["--case"]], artifacts: config["--artifacts"] };
  usage();
}
function hash(data) { return createHash("sha256").update(data).digest("hex"); }
function hashFile(path) { return existsSync(path) ? hash(readFileSync(path)) : null; }
function git(...cmd) {
  try { return execFileSync("git", cmd, { cwd: root, encoding: "utf8" }).trim(); }
  catch { return null; }
}
function verified(output, caseId) {
  // A PASS requires stage records from physical owners, independent expected
  // and actual financial assertions, and measured outbound send counts.
  if (!output || output.case_id !== caseId || output.status !== "PASS"
      || !Array.isArray(output.assertions) || output.assertions.length === 0
      || !Array.isArray(output.events) || output.events.length === 0
      || !output.readbacks || !Array.isArray(output.readbacks.sql)
      || !Array.isArray(output.readbacks.risk_book)
      || !Array.isArray(output.outbound_sends)
      || output.readbacks.sql.length === 0 || output.readbacks.risk_book.length === 0)
    throw Error(failures.invalid);
  for (const a of output.assertions) {
    if (typeof a.id !== "string" || a.id.length === 0
        || typeof a.expected !== "string"
        || typeof a.actual !== "string" || a.expected !== a.actual)
      throw Error(failures.invalid);
  }
  for (const e of output.events) {
    if (typeof e.stage !== "string" || e.stage.length === 0
        || typeof e.owner !== "string" || e.owner.length === 0
        || typeof e.source_digest !== "string" || !/^[a-f0-9]{64}$/.test(e.source_digest))
      throw Error(failures.invalid);
  }
  // Missing or reordered execution stages must not produce a PASS just
  // because the case returned a short set of syntactically valid events.
  const stages = output.events.map(e => e.stage);
  const requiredStages = requiredStagesByCase.get(caseId) ?? [];
  let previous = -1;
  for (const stage of requiredStages) {
    const next = stages.findIndex((candidate, index) => index > previous && candidate === stage);
    if (next < 0) throw Error(`${failures.invalid}:STAGE_MISSING_OR_OUT_OF_ORDER:${stage}`);
    previous = next;
  }
  const golden = entryGolden.get(caseId);
  if (golden && !output.assertions.some(a => a.id === "opening_spent_usd"
      && a.expected === golden && a.actual === golden))
    throw Error(`${failures.invalid}:INDEPENDENT_ENTRY_GOLDEN_MISSING`);
  const concurrent = concurrentGolden.get(caseId);
  if (concurrent) {
    for (const [id, expected] of concurrent) {
      if (!output.assertions.some(a => a.id === id
          && a.expected === expected && a.actual === expected))
        throw Error(`${failures.invalid}:INDEPENDENT_CONCURRENT_GOLDEN_MISSING:${id}`);
    }
  }
  const cap = sharedCapGolden.get(caseId);
  if (cap) {
    for (const [id, expected] of cap) {
      if (!output.assertions.some(a => a.id === id
          && a.expected === expected && a.actual === expected))
        throw Error(`${failures.invalid}:INDEPENDENT_SHARED_CAP_GOLDEN_MISSING:${id}`);
    }
  }
  const terminal = terminalGolden.get(caseId);
  if (terminal) {
    for (const [id, expected] of terminal) {
      if (!output.assertions.some(a => a.id === id
          && a.expected === expected && a.actual === expected))
        throw Error(`${failures.invalid}:INDEPENDENT_TERMINAL_GOLDEN_MISSING:${id}`);
    }
  }
  if (["T09.0", "T09.5"].includes(caseId)
      && (output.provenance?.schema !== "okx.native-workerd-transcript/v1"
      || !/^[a-f0-9]{64}$/.test(output.provenance?.sha256 ?? "")))
    throw Error(`${failures.invalid}:PRODUCTION_TRANSCRIPT_PROVENANCE_MISSING`);
  for (const r of [...output.readbacks.sql, ...output.readbacks.risk_book]) {
    if (!Number.isSafeInteger(r.revision) || r.revision < 0
        || typeof r.digest !== "string" || !/^[a-f0-9]{64}$/.test(r.digest))
      throw Error(failures.invalid);
  }
  const uniqueSends = new Set();
  for (const send of output.outbound_sends) {
    if (typeof send.account !== "string" || send.account.length === 0
        || typeof send.intent !== "string" || send.intent.length === 0
        || typeof send.operation_kind !== "string" || send.operation_kind.length === 0
        || !Number.isSafeInteger(send.count) || send.count < 0 || send.count > 1)
      throw Error(failures.invalid);
    const key = JSON.stringify([send.account, send.intent, send.operation_kind]);
    if (uniqueSends.has(key)) throw Error(`${failures.invalid}:DUPLICATE_SEND_KEY`);
    uniqueSends.add(key);
  }
  const createCount = output.outbound_sends
    .filter(send => send.operation_kind === "create")
    .reduce((count, send) => count + send.count, 0);
  // This depends on a validated, unique send ledger. Computing it in the
  // golden-assertions loop above would access createCount before initialization
  // and turn an otherwise verified T10-06 into a false failure.
  if (caseId === "T10-06" && createCount !== 1)
    throw Error(`${failures.invalid}:T10_06_SINGLE_SEND_UNVERIFIED`);
  if (["T09.1", "T09.2", "T09.4", "T09-SPEC"].includes(caseId) && createCount === 0)
    throw Error(`${failures.invalid}:VENUE_CREATE_NOT_OBSERVED`);
  // The two separately owned lots need two real, distinct, reduce-only
  // close operations; accounting readback alone is not a venue Close proof.
  if (caseId === "T09.6" && createCount !== 2)
    throw Error(`${failures.invalid}:MANAGED_CLOSE_SENDS_MISSING`);
  // Recovery is not a license to replay an already-sent order.
  if (caseId === "T09.7" && createCount !== 0)
    throw Error(`${failures.invalid}:RESTART_REPOST_OBSERVED`);
  // T10-01 must prove zero sends during stale/HELD denials, but may later
  // admit a freshly revalidated request; a blanket zero-send check would
  // incorrectly forbid the required positive recovery half.
  if (["T09.8", "T10-03", "T10-04"].includes(caseId) && createCount !== 0)
    throw Error(`${failures.invalid}:FORBIDDEN_VENUE_CREATE`);
  return output;
}
function selfTest() {
  const digest = "a".repeat(64);
  const full = {
    case_id: "T09.1", status: "PASS",
    assertions: [{ id: "opening_spent_usd", expected: "1.0005", actual: "1.0005" }],
    events: mandatoryStages.map(stage => ({ stage, owner: "test_owner", source_digest: digest })),
    readbacks: { sql: [{ revision: 1, digest }], risk_book: [{ revision: 1, digest }] },
    outbound_sends: [{ account: "A", intent: "I", operation_kind: "create", count: 1 }],
  };
  verified(full, "T09.1");
  let negatives = 0;
  function mustReject(change, label) {
    const output = structuredClone(full);
    change(output);
    let rejected = false;
    try { verified(output, "T09.1"); } catch { rejected = true; }
    if (!rejected) throw Error(`SELF_TEST_FALSE_PASS:${label}`);
    negatives++;
  }
  mustReject(o => { o.events.splice(5, 1); }, "missing_protection");
  mustReject(o => { [o.events[3], o.events[4]] = [o.events[4], o.events[3]]; }, "out_of_order");
  mustReject(o => { o.outbound_sends.push({ ...o.outbound_sends[0] }); }, "duplicate_create_tuple");
  mustReject(o => { o.outbound_sends[0].count = 2; }, "double_create");
  mustReject(o => { o.outbound_sends = []; }, "missing_create");
  mustReject(o => { o.assertions[0].actual = "0"; }, "false_money");
  mustReject(o => { o.assertions[0].id = "other"; }, "missing_goldens");
  mustReject(o => { o.readbacks.risk_book = []; }, "missing_worker");
  mustReject(o => { o.events[0].source_digest = "unverified"; }, "missing_digest");
  const rejected = structuredClone(full);
  rejected.case_id = "T09.8";
  rejected.assertions = [{ id: "denial", expected: "denied", actual: "denied" }];
  let forbidden = false;
  try { verified(rejected, "T09.8"); } catch { forbidden = true; }
  if (!forbidden) throw Error("SELF_TEST_FALSE_PASS:denial_with_create");
  // The next three scenarios cannot pass with a generic "all green"
  // object or with only one of the two managed closes performed.
  const terminalCase = structuredClone(full);
  terminalCase.case_id = "T09.6";
  terminalCase.assertions = [...terminalGolden.get("T09.6")].map(
    ([id, value]) => ({ id, expected: value, actual: value }));
  terminalCase.outbound_sends = [
    { account: "A", intent: "close-030", operation_kind: "create", count: 1 },
    { account: "A", intent: "close-020", operation_kind: "create", count: 1 },
  ];
  verified(terminalCase, "T09.6");
  function rejectTerminal(candidate, id, reason) {
    let blocked = false;
    try { verified(candidate, id); } catch { blocked = true; }
    if (!blocked) throw Error("SELF_TEST_FALSE_PASS:" + reason);
  }
  const missingClose = structuredClone(terminalCase);
  missingClose.outbound_sends.pop();
  rejectTerminal(missingClose, "T09.6", "missing_close");
  const missingMoney = structuredClone(terminalCase);
  missingMoney.assertions.pop();
  rejectTerminal(missingMoney, "T09.6", "missing_money");

  // Regression: the 100-way public Execute case must be accepted with
  // exactly one create and must reject a missing or doubled send witness.
  const concurrentCase = structuredClone(full);
  concurrentCase.case_id = "T10-06";
  concurrentCase.assertions = [...concurrentGolden.get("T10-06")].map(
    ([id, value]) => ({ id, expected: value, actual: value }));
  concurrentCase.events = [
    ...mandatoryStages.slice(0, 4).map(stage =>
      ({ stage, owner: "test_owner", source_digest: digest })),
    { stage: "concurrent_idempotency_reconciled",
      owner: "test_owner", source_digest: digest },
  ];
  verified(concurrentCase, "T10-06");
  const noConcurrentSend = structuredClone(concurrentCase);
  noConcurrentSend.outbound_sends = [];
  rejectTerminal(noConcurrentSend, "T10-06", "concurrent_missing_create");
  const doubleConcurrentSend = structuredClone(concurrentCase);
  doubleConcurrentSend.outbound_sends[0].count = 2;
  rejectTerminal(doubleConcurrentSend, "T10-06", "concurrent_double_create");
  const incorrectConcurrentProof = structuredClone(concurrentCase);
  incorrectConcurrentProof.assertions[1].actual = "99";
  rejectTerminal(incorrectConcurrentProof, "T10-06", "concurrent_incorrect_count");
  const missingConcurrentStage = structuredClone(concurrentCase);
  missingConcurrentStage.events.pop();
  rejectTerminal(missingConcurrentStage, "T10-06", "concurrent_missing_stage");

  // A plausible-looking pair of independent component checkpoints is not
  // a full source-backed 8 -> 4 release. Wrong money, missing campaign
  // identity and missing no-send evidence must each fail the top-level gate.
  const capCase = structuredClone(full);
  capCase.case_id = "T10-07";
  capCase.assertions = [...sharedCapGolden.get("T10-07")].map(
    ([id, value]) => ({ id, expected: value, actual: value }));
  capCase.events = requiredStagesByCase.get("T10-07").map(stage =>
    ({ stage, owner: "test_owner", source_digest: digest }));
  capCase.outbound_sends = [];
  verified(capCase, "T10-07");
  const wrongSharedMoney = structuredClone(capCase);
  wrongSharedMoney.assertions.find(a => a.id === "reserved_after_release_usd").actual = "8";
  rejectTerminal(wrongSharedMoney, "T10-07", "shared_cap_release_not_performed");
  const differentCampaigns = structuredClone(capCase);
  differentCampaigns.assertions.find(a => a.id === "same_worker_campaign").actual = "false";
  rejectTerminal(differentCampaigns, "T10-07", "separate_worker_campaigns");
  const missingNativeRelease = structuredClone(capCase);
  missingNativeRelease.assertions = missingNativeRelease.assertions.filter(
    a => a.id !== "native_sql_source_backed_release");
  rejectTerminal(missingNativeRelease, "T10-07", "missing_native_sql_release");
  const repostOnRelease = structuredClone(capCase);
  repostOnRelease.assertions.find(a => a.id === "release_extra_posts").actual = "1";
  rejectTerminal(repostOnRelease, "T10-07", "release_caused_post");

  const restart = structuredClone(terminalCase);
  restart.case_id = "T09.7";
  restart.assertions = [...terminalGolden.get("T09.7")].map(
    ([id, value]) => ({ id, expected: value, actual: value }));
  restart.outbound_sends = [];
  verified(restart, "T09.7");
  restart.outbound_sends = terminalCase.outbound_sends;
  rejectTerminal(restart, "T09.7", "restart_repost");
  const budget = structuredClone(terminalCase);
  budget.case_id = "T09.8";
  budget.assertions = [...terminalGolden.get("T09.8")].map(
    ([id, value]) => ({ id, expected: value, actual: value }));
  budget.outbound_sends = [];
  verified(budget, "T09.8");
  budget.assertions[2].actual = "STALE_SOURCE";
  rejectTerminal(budget, "T09.8", "wrong_denial");
  process.stdout.write(`TRADE_INTEGRATION_RUNNER_SELF_TEST_PASS negatives=${negatives + 13}\n`);
}
const cli = args(process.argv.slice(2));
if (cli.selfTest) { selfTest(); process.exit(0); }
if (cli.list) {
  process.stdout.write(JSON.stringify({
    schema: "okx.trade-integration-manifest/v1", spec_revision: "#449-r8.3",
    required, mandatory_stages: mandatoryStages
  }, null, 2) + "\n");
  process.exit(0);
}
const dir = resolve(cli.artifacts);
if (basename(dir) === "." || dir === root) throw Error("ARTIFACTS_MUST_BE_SEPARATE_DIRECTORY");
await mkdir(dir, { recursive: true });
const events = [];
const caseResults = [];
const assertions = [];
const caseReadbacks = [];
// Cases execute in one Node process; never let an opt-in fault probe from
// a previous case silently change the next Native/Worker financial cohort.
// Preserve deliberate caller-provided environment (e.g. targeted S2-05)
// while restoring every per-case override before advancing the matrix.
const scopedProbeKeys = Object.freeze([
  "OKX_T09_MA_CONCURRENT_PROBE",
  "OKX_T10_06_CONCURRENT_PROBE",
  "OKX_T10_07_NATIVE_PROBE",
  "OKX_T10_02_EXTERNAL_EXPOSURE_PROBE",
  "OKX_T10_04_FOREIGN_WORKER_REPLY_PROBE",
  "OKX_T09_SPEC_SECOND_LINEAR_PROBE",
  "OKX_T10_05_LOST_ACK_PROBE",
  "OKX_T10_05_EXPLICIT_REJECT_PROBE",
  "OKX_T10_15_POST_HELD_SOURCE_PROBE",
  "OKX_T10_03_POST_AUTH_REPLY_LOST_PROBE",
  "OKX_T10_03_LOST_RESERVE_REPLY_PROBE",
  "OKX_T10_01_PARTIAL_SOURCE_RECOVERY_PROBE",
  "OKX_T10_13_STOP_PROBE",
  "OKX_T10_10_FINANCE_REPLAY_PROBE",
  "OKX_T10_14_NATIVE_PROBE",
  "OKX_T10_09_LATE_FILL_PROBE",
  "OKX_B1_NATIVE_CAP_PROBE",
  "OKX_B1_SECOND_POSITIVE_PROBE",
  "OKX_S2_05_POLICY_CAS_PROBE",
]);
const initialProbes = new Map(scopedProbeKeys.map(k => [k, process.env[k]]));

const started = Date.now();
for (const caseId of cli.selected) {
  const script = resolve(root, "scripts", "trade-integration", "cases", caseId + ".mjs");
  const runStarted = Date.now();
  let status = "FAIL", reason = null;
  try {
    if (!existsSync(script)) throw Error(failures.missing);
    const { run } = await import(pathToFileURL(script).href);
    if (typeof run !== "function") throw Error(failures.invalid);
    const controller = new AbortController();
    // Managed close + native OS process and persisted DO restart are the
    // same bounded full financial replay, not a short isolated unit case.
    const deadline = ["T10-05", "T10-07", "T10-10", "T10-11", "T10-12"].includes(caseId) ? 300_000
      : ["T09.6", "T09.7", "T09.8"].includes(caseId) ? 260_000 : 120_000;
    let timer;
    try {
      const output = await Promise.race([
        run({ caseId, root, signal: controller.signal, artifactsDir: dir }),
        new Promise((_, reject) => {
          timer = setTimeout(() => {
            controller.abort();
            reject(Error(failures.timeout));
          }, deadline);
        }),
      ]);
      const accepted = verified(output, caseId);
      status = "PASS";
      caseReadbacks.push({
        case_id:caseId,
        provenance:accepted.provenance ?? null,
        sql:accepted.readbacks.sql,
        risk_book:accepted.readbacks.risk_book,
        outbound_sends:accepted.outbound_sends,
      });
      for (const a of accepted.assertions) assertions.push({ case_id: caseId, ...a });
      for (const e of accepted.events) events.push({ case_id: caseId, ...e });
    } finally { clearTimeout(timer); }
  } catch (error) {
    reason = (error instanceof Error ? error.message : String(error)).slice(0, 200);
    events.push({ case_id: caseId, stage: "case_failed", owner: "integration_runner", reason });
  } finally {
    for (const [key, original] of initialProbes) {
      if (original === undefined) delete process.env[key];
      else process.env[key] = original;
    }
  }
  caseResults.push({ id: caseId, status, reason, elapsed_ms: Date.now() - runStarted });
}
const successful = caseResults.every(c => c.status === "PASS");
const summary = {
  schema: "okx.trade-integration-summary/v1", spec_revision: "#449-r8.3",
  exact_commit: git("rev-parse", "HEAD"), exact_tree: git("rev-parse", "HEAD^{tree}"),
  driver_sha256: hashFile(fileURLToPath(import.meta.url)),
  cargo_lock_sha256: hashFile(resolve(root, "Cargo.lock")),
  worker_lock_sha256: hashFile(resolve(root, "cloudflare", "okx-mcp", "package-lock.json")),
  worker_bundle_sha256: null,
  os: { platform: os.platform(), release: os.release(), arch: os.arch() },
  runtime: process.version, seed: process.env.OKX_INTEGRATION_SEED ?? null,
  required, selected: cli.selected,
  outcomes: caseResults,
  executed_pass: caseResults.filter(c => c.status === "PASS").length,
  executed_failures: caseResults.filter(c => c.status !== "PASS" && c.reason !== failures.missing).map(c => c.id),
  missing_required: caseResults.filter(c => c.reason === failures.missing).map(c => c.id),
  cohort_usages: nativeCohortSnapshot(),
  elapsed_ms: Date.now() - started,
  gate: successful ? "PASS" : "BLOCKED",
  rerun: cli.selected.length === 1
    ? `node scripts/run-trade-integration.mjs --case ${cli.selected[0]} --artifacts <run-dir>`
    : "node scripts/run-trade-integration.mjs --suite t09,t10 --artifacts <run-dir>",
};
await Promise.all([
  writeFile(resolve(dir, "summary.json"), JSON.stringify(summary, null, 2) + "\n"),
  writeFile(resolve(dir, "assertions.json"), JSON.stringify({
    schema:"okx.trade-assertions/v1", assertions
  }, null, 2) + "\n"),
  writeFile(resolve(dir, "events.jsonl"), events.slice(0, 4096).map(x => JSON.stringify(x)).join("\n") + "\n"),
  writeFile(resolve(dir, "readbacks.json"), JSON.stringify({
    schema:"okx.trade-integration-readbacks/v1",cases:caseReadbacks,
  }, null, 2) + "\n"),
]);
process.stdout.write(JSON.stringify({
  schema: summary.schema, gate: summary.gate, pass: caseResults.filter(x=>x.status==="PASS").length,
  required: cli.selected.length, artifacts: dir, failures: caseResults.filter(x=>x.status!=="PASS")
}) + "\n");
if (!successful) process.exitCode = 1;
