#!/usr/bin/env node
// Workerd/Miniflare executes the ACTUAL RuntimeSession with SQLite-backed DO
// transactions. Only the incoming WSS frames are scripted. No Map storage,
// alternate RiskBook, exchange server, endpoint in production, or financial stub.
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { promisify } from "node:util";
import { execFile } from "node:child_process";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { build } from "esbuild";
import { Miniflare } from "miniflare";

const root = await mkdtemp(join(tmpdir(), "okx-workerd-risk-"));
const outfile = join(root, "runtime-test.mjs");
const FRAME_SCHEMA = "okx.direct-transport.frame/v2";
const configuration = {
  schema: "okx.governance/v3",
  revision: 0,
  installation_id: "installation_test",
  campaign_id: "sha256:" + "c".repeat(64),
  campaign_loss_limit_usd: "10",
  campaign_entry_limit_usd: "10",
  max_total_modelled_loss_usdt: "5",
  accounts: [],
};
const token = "integration-only-ephemeral-token";
const tokenHash = createHash("sha256").update(token).digest("hex");
// Miniflare 5 uses the build-output worker manifest and typed bindings.
const makeOptions = (bundleContents) => ({
  resourcePersistencePath: join(root, "sqlite-do"),
  workers: [{
    config: {
      name: "okx-test-runtime",
      compatibilityDate: "2026-09-30",
      compatibilityFlags: ["global_fetch_strictly_public"],
      manifest: {
        mainModule: "runtime-test.mjs",
        modulesRoot: root,
        modules: {
          "runtime-test.mjs": { type: "esm", contents: bundleContents },
        },
      },
      env: {
        RUNTIME: {
          type: "durable-object",
          worker: "okx-test-runtime",
          exportName: "RuntimeSession",
        },
        RUNTIME_TOKEN_SHA256: { type: "text", value: tokenHash },
      },
      exports: {
        RuntimeSession: { type: "durable-object", storage: "sqlite" },
      },
    },
  }],
});
const installation = {
  contract: FRAME_SCHEMA,
  release_sha256: "f".repeat(64),
  operation_id: "install_integration_001",
  configuration,
};

async function request(mf, pathname, method = "GET", body) {
  const response = await mf.dispatchFetch("http://localhost" + pathname, {
    method,
    ...(body === undefined ? {} : {
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    }),
  });
  return { status: response.status, body: await response.json() };
}
function frames(ws) {
  const received = [];
  const waiters = [];
  const onMessage = (event) => {
    const frame = JSON.parse(event.data);
    const pending = waiters.find((w) => w.match(frame));
    if (pending) {
      waiters.splice(waiters.indexOf(pending), 1);
      clearTimeout(pending.timer);
      pending.resolve(frame);
    } else {
      received.push(frame);
    }
  };
  ws.addEventListener("message", onMessage);
  ws.accept();
  return {
    send(frame) { ws.send(JSON.stringify(frame)); },
    next(match) {
      const idx = received.findIndex(match);
      if (idx >= 0) return Promise.resolve(received.splice(idx, 1)[0]);
      return new Promise((resolve, reject) => {
        const waiter = { match, resolve, timer: null };
        waiter.timer = setTimeout(() => {
          const pos = waiters.indexOf(waiter);
          if (pos >= 0) waiters.splice(pos, 1);
          reject(new Error("WORKER_FRAME_TIMEOUT"));
        }, 5000);
        waiters.push(waiter);
      });
    },
    close() { ws.close(1000, "integration complete"); },
  };
}
async function session(mf) {
  const reply = await mf.dispatchFetch("http://localhost/runtime", {
    headers: { upgrade: "websocket", authorization: "Bearer " + token },
  });
  assert.equal(reply.status, 101, "auth-bound real runtime WSS handshake");
  assert.ok(reply.webSocket, "WebSocketPair exists only through production RuntimeSession");
  const ws = frames(reply.webSocket);
  ws.send({
    type: "hello",
    schema: FRAME_SCHEMA,
    runtime_id: "windows-primary",
    connection_id: "native_" + "a".repeat(32),
    installation_id: configuration.installation_id,
    contract: "okx.product/v1",
    runtime_profile: "engine",
    agent_pid: 12345,
    agent_binary_sha256: "b".repeat(64),
    agent_incarnation: "ctl_" + "d".repeat(32),
  });
  const hello = await ws.next((f) => f.type === "hello_ack");
  assert.ok(hello.connection_generation > 0);
  assert.ok(hello.session_id.startsWith("session_"));
  const policy = await ws.next((f) => f.type === "policy_snapshot");
  assert.equal(policy.connection_generation, hello.connection_generation);
  assert.equal(policy.snapshot.state.installation_id, configuration.installation_id);
  return {ws,hello};
}
// The only test-controlled input to the production WSS parser is a raw frame.
// RuntimeSession handles all authority, revision and financial state transitions.
async function risk(session, command_id, action) {
  const {ws,hello} = session;
  ws.send({
    type: "risk_control",
    schema: FRAME_SCHEMA,
    session_id: hello.session_id,
    connection_generation: hello.connection_generation,
    command: {
      schema: "okx.risk-command/v1",
      command_id,
      installation_id: configuration.installation_id,
      action,
    },
  });
  const frame = await ws.next((f) =>
    f.type === "risk_control_result" && f.result?.command_id === command_id);
  assert.equal(frame.session_id, hello.session_id);
  assert.equal(frame.connection_generation, hello.connection_generation);
  return frame.result;
}
async function summary(session, commandId) {
  const result = await risk(session, commandId, {action:"SUMMARY"});
  assert.equal(result.status, "OBSERVED");
  assert.equal(result.budget?.campaign_limit_usd, "10");
  return result;
}
function fixtureOpening(campaignStartedAt, asOf, accountKeys) {
  const source = {
    campaign_id: configuration.campaign_id,
    limit_usd: "10",
    consumed_usd: "0",
    source_digest: "",
    as_of_ms: asOf,
    campaign_started_at_ms: campaignStartedAt,
    accounts: accountKeys.map((account_key) => ({
      account_key,
      history_from_ms: campaignStartedAt,
      history_through_ms: asOf,
      consumed_entry_usd: "0",
      order_history_complete: true,
      fill_history_complete: true,
      bill_history_complete: true,
      finance_provenance_sealed: true,
      source_digest: "a".repeat(64),
    })),
  };
  const canonical = JSON.stringify([
    "okx.campaign-entry-opening/v1",source.campaign_id,
    source.limit_usd,source.consumed_usd,
    source.campaign_started_at_ms,source.as_of_ms,source.accounts
  ]);
  source.source_digest = createHash("sha256").update(canonical).digest("hex");
  return source;
}
let first, second;
try {
  await build({
    entryPoints: [fileURLToPath(new URL("../src/integration/runtime_entry.ts", import.meta.url))],
    outfile, bundle: true, write: true, format: "esm",
    platform: "browser", target: "es2022", logLevel: "warning",
  });
  const opts = makeOptions(await readFile(outfile, "utf8"));
  first = new Miniflare(opts);
  const install = await request(first, "/installation/initialize", "POST", installation);
  assert.equal(install.status, 200, "real governance/RiskBook created in one DO transaction");
  // Enrol one test account via the actual DO governance transaction.
  // UID and signed-source facts are scripted here; NOT real-exchange acceptance.
  const uid = createHash("sha256").update("account-001").digest("hex");
  const accountKey = "okx:real:" + uid;
  const limits = {
    max_modelled_loss_usdt:"5",max_order_loss_usdt:"1",
    max_notional_usdt:"100",max_entry_usd:"10",max_leverage:"3",
    max_daily_realized_loss_usdt:"2",max_drawdown_ratio:"0.1",
    allowed_instruments:["BTC-USDT-SWAP"],
  };
  const enrollment = await request(first,"/account-governance/enroll","POST",{
    uid_fingerprint:uid,venue:"okx",environment:"real",
    label:"Recorded account",alias:"recordedA",enabled:true,
    limits,credential_ref:"fixture-credentials-a",
    idempotency_key:"enroll_recorded_account_001",
  });
  assert.equal(enrollment.status,200,"actual governance enrolment must commit");
  // T09.0: the central owner must know BOTH verified accounts before the
  // campaign source, census and reservation can be accepted. The two native
  // SQL owners below are opened independently by the same production bootstrap.
  const uidB = createHash("sha256").update("account-002").digest("hex");
  const accountKeyB = "okx:real:" + uidB;
  const enrollmentB = await request(first,"/account-governance/enroll","POST",{
    uid_fingerprint:uidB,venue:"okx",environment:"real",
    label:"Recorded account B",alias:"recordedB",enabled:true,
    limits,credential_ref:"fixture-credentials-b",
    idempotency_key:"enroll_recorded_account_002",
  });
  assert.equal(enrollmentB.status,200,"second account must be enrolled in the same Worker");
  const accountKeys = [accountKey,accountKeyB].sort();
  assert.equal(new Set(accountKeys).size,2,"two distinct verified account identities");
  const enrolled = await request(first,"/account-governance");
  assert.equal(enrolled.status,200);
  assert.deepEqual(enrolled.body.accounts.map((a) => a.account_key).sort(),accountKeys);
  const socket1 = await session(first);
  const before = await summary(socket1,"risk_workerd_first_0001");
  assert.equal(before.budget?.entry_budget,null);
  const opening = fixtureOpening(
    before.budget.campaign_origin.started_at_ms,Date.now(),accountKeys
  );
  const opened = await risk(socket1,"risk_opening_recorded_0001",{
    action:"INSTALL_ENTRY_OPENING",source:opening,
  });
  assert.equal(opened.status,"OBSERVED","native WSS campaign source installs exactly once");
  assert.equal(opened.budget?.entry_budget?.consumed_usd,"0");
  assert.equal(opened.budget.entry_budget.opening_source_digest,opening.source_digest);
  const observedAt = Date.now()-1000;
  const census = {
    complete:true,source_digest:"d".repeat(64),content_digest:"a".repeat(64),
    generated_at_ms:observedAt,source_read_floor_ms:observedAt,
    account_keys:accountKeys,
    occupied:accountKeys.map((account_key) => ({
      account_key,occupied_usdt:"0",cumulative_loss_usd:"0"
    })),
    native_account_evidence_complete:true,
    native_order_evidence_complete:true,
    native_ledger_evidence_complete:true,
  };
  const reserveAction = {
    action:"RESERVE",venue:"okx",account_uid:uid,
    run_id:"run_workerd_recorded_001",intent_id:"intent_workerd_recorded_001",
    strategy_id:"manual",instrument_id:"BTC-USDT-SWAP",
    plan_sha256:"b".repeat(64),
    requested_loss_usdt:"1",requested_loss_usd:"1",
    entry_upper_usd:"2",entry_proof_digest:"d".repeat(64),
    usd_conversion:{usd_per_usdt:"1",source_digest:"d".repeat(64),observed_at_ms:observedAt},
    policy_revision:2,census,
  };
  const held = await risk(socket1,"reserve_recorded_0001",reserveAction);
  assert.equal(held.status,"HELD","real RiskBook reservation consumes shared capacity");
  assert.ok(held.reservation_key);
  const reheld = await risk(socket1,"reserve_recorded_replay_0001",reserveAction);
  assert.equal(reheld.status,"HELD");
  assert.equal(reheld.reservation_key,held.reservation_key,
    "same source-bound intent does not allocate another reservation");
  const afterHeld = Date.now();
  const begin = await risk(socket1,"post_recorded_0001",{
    action:"BEGIN_POST",
    reservation_key:held.reservation_key,
    plan_sha256:reserveAction.plan_sha256,
    policy_revision:2,
    expected_risk_book_sequence:held.risk_book_sequence,
    source_proof_sha256:"f".repeat(64),
    census:{
      ...census,
      source_digest:"e".repeat(64),
      generated_at_ms:Math.max(afterHeld,observedAt+1),
      source_read_floor_ms:Math.max(afterHeld,observedAt+1),
    },
    causal_proof:{
      reserve_anchor:held.reservation_key,
      native_incarnation:"a".repeat(32),
      elapsed_to_proof_ms:30,
      account_reads:accountKeys.map((account_key) => ({
        account_key,private_generation:7,
        source_read_id:"b".repeat(64),content_digest:census.content_digest,
        started_after_held_ms:5,completed_after_held_ms:20,
      })),
    },
  });
  assert.equal(begin.status,"POST_AUTHORIZED",
    "only the actual Worker CAS may fence the one possible venue POST");
  const after = await summary(socket1,"risk_before_restart_001");
  assert.equal(after.budget?.reserved_loss_usd,"1");
  assert.equal(after.budget.entry_budget.reserved_usd,"2");
  assert.equal(after.budget.entry_budget.consumed_usd,"0");
  socket1.ws.close();
  await first.dispose();
  first = null;
  second = new Miniflare(opts);
  const replay = await request(second, "/installation/initialize", "POST", installation);
  assert.equal(replay.status, 200, "persisted installation receipt is idempotent after restart");
  const persisted = await request(second, "/account-governance");
  assert.equal(persisted.status, 200);
  assert.equal(persisted.body.installation_id, configuration.installation_id);
  assert.deepEqual(persisted.body.accounts.map((a) => a.account_key).sort(),accountKeys,
    "both real account enrollments persist across Worker restart");
  const socket2 = await session(second);
  assert.ok(socket2.hello.connection_generation > socket1.hello.connection_generation,
    "restarted DO retains connection generation");
  const restored = await summary(socket2, "risk_workerd_second_0002");
  assert.equal(restored.budget?.reserved_loss_usd,"1");
  assert.equal(restored.budget?.entry_budget?.reserved_usd,"2");
  assert.equal(restored.budget?.entry_budget?.consumed_usd,"0");
  const exactReplay = await risk(socket2,"reserve_recorded_later_0001",reserveAction);
  assert.equal(exactReplay.status,"DENIED");
  const repeated = await summary(socket2,"risk_after_replay_0001");
  assert.equal(repeated.budget?.reserved_loss_usd,"1");
  assert.equal(repeated.budget?.entry_budget?.reserved_usd,"2");
  socket2.ws.close();
  if (process.env.OKX_NATIVE_WORKER_INTEGRATION === "1") {
    // Native cargo test is launched only by linux-rust, which already owns
    // the Rust toolchain and dependencies. Workerd remains alive throughout.
    const url = await second.ready;
    assert.equal(url.hostname,"127.0.0.1","CI worker must be loopback-only");
    const wsUrl = new URL("/runtime",url);
    wsUrl.protocol = "ws:";
    const { stdout, stderr } = await promisify(execFile)(
      "cargo",
      [
        "test","--locked","-p","okx-agent","--lib",
        "native_to_workerd_risk_summary_over_actual_websocket",
        "--","--nocapture",
      ],
      {
        cwd: fileURLToPath(new URL("../../../",import.meta.url)),
        env: {
          ...process.env,
          OKX_INTEGRATION_WORKER_WS_URL: wsUrl.toString(),
          OKX_INTEGRATION_WORKER_TOKEN: token,
          OKX_INTEGRATION_RESERVATION: held.reservation_key,
        },
        maxBuffer: 8 * 1024 * 1024,
        timeout: 180_000,
      },
    );
    assert.match(stdout,/native_to_persisted_workerd_riskbook PASS/,
      "real Rust client must prove Worker SQL summary and reservation");
    process.stdout.write(stdout);
    if (stderr) process.stderr.write(stderr);
    const afterNativeSocket = await session(second);
    const afterNative = await summary(afterNativeSocket, "risk_after_native_real_wss_0001");
    assert.ok(Number(afterNative.budget?.reserved_loss_usd) > 2,
      "the public TradeService Execute must reserve its own risk in central Worker");
    assert.equal(afterNative.budget?.entry_budget?.reserved_usd, "5.0005",
      "Native component reservations (4) plus public Execute (1.0005) must share one RiskBook");
    assert.equal(afterNative.budget?.entry_budget?.consumed_usd, "0",
      "POST authorization is not a fabricated cash debit");
    afterNativeSocket.ws.close();
  }
  console.log(JSON.stringify({
    suite:"runtime-session-sqlite-do",
    status:"PASS",
    real_durable_object:true,
    persisted_generation:true,
    risk_book_source:"RuntimeSession.webSocketMessage",
    authentic_reserve:true,
    authentic_begin_post:true,
    reserved_loss_usd:"1",
    reserved_entry_usd:"2",
    risk_monetary_post_count:0,
  }));
} finally {
  if (first) await first.dispose();
  if (second) await second.dispose();
  await rm(root, {recursive: true, force: true});
}
