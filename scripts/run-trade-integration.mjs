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
  ["T09-MA", mandatoryStages.slice(0, 4)],
  ["T09-SPEC", mandatoryStages.slice(0, 4)],
]);
const entryGolden = new Map([["T09.1", "1.0005"]]);
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
  if (["T09.1", "T09.2", "T09.4", "T09-SPEC"].includes(caseId) && createCount === 0)
    throw Error(`${failures.invalid}:VENUE_CREATE_NOT_OBSERVED`);
  if (["T09.8", "T10-01", "T10-04"].includes(caseId) && createCount !== 0)
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
  process.stdout.write(`TRADE_INTEGRATION_RUNNER_SELF_TEST_PASS negatives=${negatives + 1}\n`);
}
const cli = args(process.argv.slice(2));
if (cli.selfTest) { selfTest(); process.exit(0); }
if (cli.list) {
  process.stdout.write(JSON.stringify({
    schema: "okx.trade-integration-manifest/v1", spec_revision: "#449-r6",
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
    const deadline = 120_000;
    let timer;
    try {
      const output = await Promise.race([
        run({ caseId, root, signal: controller.signal }),
        new Promise((_, reject) => {
          timer = setTimeout(() => {
            controller.abort();
            reject(Error(failures.timeout));
          }, deadline);
        }),
      ]);
      const accepted = verified(output, caseId);
      status = "PASS";
      for (const a of accepted.assertions) assertions.push({ case_id: caseId, ...a });
      for (const e of accepted.events) events.push({ case_id: caseId, ...e });
    } finally { clearTimeout(timer); }
  } catch (error) {
    reason = (error instanceof Error ? error.message : String(error)).slice(0, 200);
    events.push({ case_id: caseId, stage: "case_failed", owner: "integration_runner", reason });
  }
  caseResults.push({ id: caseId, status, reason, elapsed_ms: Date.now() - runStarted });
}
const successful = caseResults.every(c => c.status === "PASS");
const summary = {
  schema: "okx.trade-integration-summary/v1", spec_revision: "#449-r6",
  exact_commit: git("rev-parse", "HEAD"), exact_tree: git("rev-parse", "HEAD^{tree}"),
  driver_sha256: hashFile(fileURLToPath(import.meta.url)),
  cargo_lock_sha256: hashFile(resolve(root, "Cargo.lock")),
  worker_lock_sha256: hashFile(resolve(root, "cloudflare", "okx-mcp", "package-lock.json")),
  worker_bundle_sha256: null,
  os: { platform: os.platform(), release: os.release(), arch: os.arch() },
  runtime: process.version, seed: process.env.OKX_INTEGRATION_SEED ?? null,
  required, selected: cli.selected,
  outcomes: caseResults, elapsed_ms: Date.now() - started,
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
]);
process.stdout.write(JSON.stringify({
  schema: summary.schema, gate: summary.gate, pass: caseResults.filter(x=>x.status==="PASS").length,
  required: cli.selected.length, artifacts: dir, failures: caseResults.filter(x=>x.status!=="PASS")
}) + "\n");
if (!successful) process.exitCode = 1;
