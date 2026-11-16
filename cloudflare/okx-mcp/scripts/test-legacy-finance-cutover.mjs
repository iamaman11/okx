// Bounded physical RB09 boundary: real authorization, RuntimeSession and
// SQLite-backed workerd. This does NOT activate Rust or prove an empty ledger.
import assert from "node:assert/strict";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";
import { performance } from "node:perf_hooks";
import { createHash } from "node:crypto";
import { build } from "esbuild";
import { Miniflare } from "miniflare";

const start = performance.now();
const root = await mkdtemp(join(tmpdir(), "okx-cutover-workerd-"));
const outfile = join(root, "cutover.mjs");
// Deliberately public test input, NEVER a saved user credential.
const secret = "isolated-cutover-test-owner";
const configuration = {
  schema: "okx.governance/v3", revision: 0,
  installation_id: "installation_test", campaign_id: "sha256:" + "c".repeat(64),
  campaign_loss_limit_usd: "10", campaign_entry_limit_usd: "10",
  max_total_modelled_loss_usdt: "5", accounts: [],
};
const disable = {
  schema: "okx.legacy-finance-disable/v1", installation_id: "installation_test",
  operation_id: "disable_finance_physical_01", source_tree: "b".repeat(40),
};
let mf;
async function request(path, method = "GET", body, authorized = true) {
  const response = await mf.dispatchFetch("http://localhost" + path, {
    method, headers: {
      ...(authorized ? {authorization: "Bearer " + secret} : {}),
      ...(body === undefined ? {} : {"content-type": "application/json"}),
    }, ...(body === undefined ? {} : {body: JSON.stringify(body)}),
  });
  const text = await response.text();
  return {status: response.status, body: (() => {try {return JSON.parse(text);} catch {return text;}})()};
}
async function open(script) {
  mf = new Miniflare({
    resourcePersistencePath: join(root, "sqlite-do"),
    workers: [{config: {
      name: "okx-cutover-test", compatibilityDate: "2026-09-30",
      compatibilityFlags: ["global_fetch_strictly_public"],
      manifest: {mainModule: "cutover.mjs", modulesRoot: root,
        modules: {"cutover.mjs": {type: "esm", contents: script}}},
      env: {
        RUNTIME: {type: "durable-object", worker: "okx-cutover-test", exportName: "RuntimeSession"},
        MCP_OWNER_SECRET: {type: "text", value: secret},
        RELEASE_SOURCE_TREE: {type: "text", value: disable.source_tree},
        RUNTIME_TOKEN_SHA256: {type: "text", value: createHash("sha256").update(secret).digest("hex")},
      }, exports: {RuntimeSession: {type: "durable-object", storage: "sqlite"}},
    }}],
  });
}
async function deniedNativeCommands() {
  const response = await mf.dispatchFetch("http://localhost/runtime", {
    headers: {upgrade: "websocket", authorization: "Bearer " + secret},
  });
  assert.equal(response.status, 101, "retirement must retain authenticated transport");
  const ws = response.webSocket;
  const received = [], waiting = [];
  ws.addEventListener("message", event => {
    const frame = JSON.parse(event.data);
    const i = waiting.findIndex(item => item.match(frame));
    if (i < 0) received.push(frame);
    else {const item = waiting.splice(i, 1)[0]; clearTimeout(item.timer); item.resolve(frame);}
  });
  function next(match) {
    const i = received.findIndex(match);
    if (i >= 0) return Promise.resolve(received.splice(i, 1)[0]);
    return new Promise((resolve, reject) => {
      const item = {match, resolve, timer: setTimeout(() => reject(Error("CUTOVER_FRAME_DEADLINE")), 3000)};
      waiting.push(item);
    });
  }
  ws.accept();
  try {
    ws.send(JSON.stringify({type: "hello", schema: "okx.direct-transport.frame/v2",
      runtime_id: "windows-primary", connection_id: "native_" + "a".repeat(32),
      installation_id: configuration.installation_id, contract: "okx.product/v1",
      runtime_profile: "engine", agent_pid: 12345, agent_binary_sha256: "a".repeat(64),
      agent_incarnation: "ctl_" + "a".repeat(32)}));
    const hello = await next(frame => frame.type === "hello_ack");
    for (const action of ["SUMMARY", "INSTALL_ENTRY_OPENING", "ACCOUNT_SUMMARY", "INSPECT",
      "RESERVE", "SETTLE_ENTRY", "RELEASE_UNSENT", "BEGIN_POST", "RESOLVE"]) {
      const id = "denied_cutover_" + action;
      ws.send(JSON.stringify({type: "risk_control", schema: "okx.direct-transport.frame/v2",
        session_id: hello.session_id, connection_generation: hello.connection_generation,
        command: {schema: "okx.risk-command/v1", command_id: id,
          installation_id: configuration.installation_id, action: {action}}}));
      const frame = await next(frame => frame.type === "risk_control_result" && frame.result?.command_id === id);
      assert.equal(frame.result.status, "DENIED", action);
      assert.equal(frame.result.reason, "LEGACY_FINANCE_SERVER_DISABLED", action);
    }
  } finally {ws.close(1000, "bounded cutover witness");}
}
try {
  await build({entryPoints: [fileURLToPath(new URL("../src/integration/finance_cutover_entry.ts", import.meta.url))],
    outfile, bundle: true, format: "esm", platform: "browser", target: "es2022", logLevel: "warning"});
  const script = await readFile(outfile, "utf8");
  await open(script);
  const initialized = await request("/installation/initialize", "POST", {
    contract: "okx.direct-transport.frame/v2", release_sha256: "a".repeat(64),
    operation_id: "installation_physical_01", configuration,
  });
  assert.equal(initialized.status, 200, JSON.stringify(initialized));
  assert.equal((await request("/installation/disable-legacy-finance", "POST", disable, false)).status, 401);
  assert.equal((await request("/installation/legacy-finance-readback", "GET", undefined, false)).status, 401);
  assert.equal((await request("/installation/disable-legacy-finance", "POST", {
    ...disable, source_tree: "c".repeat(40),
  })).status, 409);
  assert.equal((await request("/installation/disable-legacy-finance", "POST", {
    ...disable, installation_id: "wrong_installation",
  })).status, 409);
  const disabled = await request("/installation/disable-legacy-finance", "POST", disable);
  assert.equal(disabled.status, 200, JSON.stringify(disabled));
  const snapshot = await request("/installation/legacy-finance-readback");
  assert.equal(snapshot.status, 200, JSON.stringify(snapshot));
  assert.equal(snapshot.body.inventory_complete, true);
  assert.equal(snapshot.body.state, "DISABLED");
  assert.ok(snapshot.body.sources.risk_meta_v2, "original metadata must be exported");
  assert.deepEqual(snapshot.body.sources.account_access_v1, configuration);
  assert.equal(snapshot.body.sources.risk_meta_v2.campaign_limit_usd, "10");
  assert.equal(snapshot.body.sources.risk_meta_v2.occupied_usd, "0");
  assert.equal(snapshot.body.sources.risk_meta_v2.consumed_loss_usd, "0");
  const canonical = JSON.stringify(snapshot.body.sources, (_key, value) =>
    value && typeof value === "object" && !Array.isArray(value)
      ? Object.fromEntries(Object.entries(value).sort(([a], [b]) => a < b ? -1 : a > b ? 1 : 0)) : value);
  assert.equal(snapshot.body.receipt.inventory_sha256,
    createHash("sha256").update(canonical).digest("hex"), "independent exported-content hash");
  assert.equal(snapshot.body.receipt.inventory_records, Object.keys(snapshot.body.sources).length);
  assert.match(snapshot.body.receipt.inventory_sha256, /^[0-9a-f]{64}$/);
  await deniedNativeCommands();
  const mutation = {expected_revision: 0, campaign_loss_limit_usd: "9", idempotency_key: "lower_after_disable_01"};
  assert.equal((await request("/account-governance/campaign-limit", "POST", mutation)).status, 409);
  assert.deepEqual(await request("/installation/legacy-finance-readback"), snapshot,
    "denied mutation must leave original financial snapshot unchanged");
  assert.deepEqual(await request("/installation/disable-legacy-finance", "POST", disable), disabled);
  assert.equal((await request("/installation/disable-legacy-finance", "POST", {
    ...disable, operation_id: "conflicting_disable_02",
  })).status, 409);
  await mf.dispose(); mf = undefined;
  await open(script); // New workerd instance opens the ORIGINAL persisted DO SQLite.
  assert.deepEqual(await request("/installation/legacy-finance-readback"), snapshot,
    "durable disable and exact original snapshot must survive cold reopen");
  await deniedNativeCommands();
  assert.equal((await request("/account-governance/campaign-limit", "POST", mutation)).status, 409);
  assert.deepEqual(await request("/installation/disable-legacy-finance", "POST", disable), disabled);
  assert.equal((await request("/health")).status, 200, "transport health remains available");
  assert.deepEqual(await request("/installation/legacy-finance-readback"), snapshot);
  console.log(JSON.stringify({schema: "okx.rb09-server-disable-witness/v1", result: "PASS",
    physical: "workerd-sqlite-cold-reopen", runtime_ms: Math.round(performance.now() - start),
    denied_native_commands: 18, inventory_sha256: snapshot.body.receipt.inventory_sha256,
    rust_activation: "NOT_PROVEN", legacy_pre_guard_rollback: "UNSUPPORTED"}));
} finally {
  if (mf) await mf.dispose();
  const absolute = resolve(root);
  assert.ok(absolute.startsWith(resolve(tmpdir()) + sep) && absolute.includes("okx-cutover-workerd-"));
  await rm(absolute, {recursive: true, force: true});
}
