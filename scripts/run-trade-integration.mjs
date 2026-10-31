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
function usage() {
  throw Error("USAGE: --list | --suite t09,t10 --artifacts DIR | --case T10-05 --artifacts DIR");
}
function args(input) {
  if (input.length === 1 && input[0] === "--list") return { list: true };
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
    if (typeof a.id !== "string" || typeof a.expected !== "string"
        || typeof a.actual !== "string" || a.expected !== a.actual)
      throw Error(failures.invalid);
  }
  for (const e of output.events) {
    if (!mandatoryStages.includes(e.stage) || typeof e.owner !== "string"
        || typeof e.source_digest !== "string" || !/^[a-f0-9]{64}$/.test(e.source_digest))
      throw Error(failures.invalid);
  }
  for (const r of [...output.readbacks.sql, ...output.readbacks.risk_book]) {
    if (!Number.isSafeInteger(r.revision) || r.revision < 0
        || typeof r.digest !== "string" || !/^[a-f0-9]{64}$/.test(r.digest))
      throw Error(failures.invalid);
  }
  for (const send of output.outbound_sends) {
    if (typeof send.account !== "string" || typeof send.intent !== "string"
        || !Number.isSafeInteger(send.count) || send.count < 0 || send.count > 1)
      throw Error(failures.invalid);
  }
  return output;
}
const cli = args(process.argv.slice(2));
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
