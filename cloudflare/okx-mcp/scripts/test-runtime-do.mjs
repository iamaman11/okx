#!/usr/bin/env node
// Workerd/Miniflare executes the ACTUAL RuntimeSession with SQLite-backed DO
// transactions. Only the incoming WSS frames are scripted. No Map storage,
// alternate RiskBook, exchange server, endpoint in production, or financial stub.
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { promisify } from "node:util";
import { execFile } from "node:child_process";
import { mkdtemp, readFile, rm, stat, writeFile } from "node:fs/promises";
import { setTimeout as delay } from "node:timers/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { build } from "esbuild";
import { Miniflare } from "miniflare";

const root = await mkdtemp(join(tmpdir(), "okx-workerd-risk-"));
const outfile = join(root, "runtime-test.mjs");
const handoffPath = join(root, "native-handoff.json");
// Only a uniquely emitted production Rust receipt can certify terminal
// settlement across the child-process boundary. Never fabricate a receipt
// from the scripted test input or a missing marker.
function receipt(stdout, prefix) {
  const marker = prefix + ":";
  const matches = stdout.split(/\r?\n/)
    .filter(line => line.startsWith(marker));
  assert.equal(matches.length, 1, prefix + " must have one authentic Native readback");
  return JSON.parse(matches[0].slice(marker.length));
}
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
const addMode=process.env.OKX_T09_4_MANAGED_ADD_PROBE==="1";
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
  // Native must initialize the REAL account SQL owners before the actual
  // Worker campaign is created. A fixture origin before initialized_at
  // would correctly fail with BASELINE_RECONCILIATION_REQUIRED.
  let nativeProcess = null;
  if (process.env.OKX_NATIVE_WORKER_INTEGRATION === "1") {
    nativeProcess = promisify(execFile)(
      "cargo",
      [
        "test", "--locked", "-p", "okx-agent", "--lib",
        "native_to_workerd_risk_summary_over_actual_websocket", "--", "--nocapture",
      ],
      {
        cwd: fileURLToPath(new URL("../../../", import.meta.url)),
        // This subprocess runs the single large T09.0..8 native async
        // acceptance future on a Windows Rust test-harness thread. The default
        // 2 MiB test stack overflows (0xC00000FD) BEFORE T09.2; no venue
        // operation is implicated. Bound only the CHILD's test-thread stack.
        // Production Agent/Worker settings and per-request limits are unchanged.
        env: {
          ...process.env,
          RUST_MIN_STACK: "16777216",
          OKX_INTEGRATION_HANDOFF_PATH: handoffPath,
        },
        maxBuffer: 8 * 1024 * 1024,
        timeout: process.env.OKX_T09_6_CLOSE_PROBE === "1" ? 220_000 : 180_000,
      },
    );
    nativeProcess.catch(() => {});
    const bootstrapReady = async () => {
      const deadline = Date.now() + 120_000;
      while (Date.now() < deadline) {
        try {
          await stat(handoffPath + ".native-ready");
          return;
        } catch (error) {
          if (error?.code !== "ENOENT") throw error;
        }
        await delay(25);
      }
      throw new Error("NATIVE_SQL_BOOTSTRAP_DEADLINE");
    };
    await Promise.race([
      bootstrapReady(),
      nativeProcess.then(
        () => { throw new Error("NATIVE_EXITED_BEFORE_WORKER_CAMPAIGN"); },
        (error) => { throw error; },
      ),
    ]);
  }
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
  // Structured evidence is copied from actual SQL-backed Worker readbacks,
  // never from a simulated RiskBook or a test-owned financial projection.
  console.log("T09_0_WORKER_EVIDENCE_V1:" + JSON.stringify({
    account_keys:enrolled.body.accounts.map(a => a.account_key).sort(),
    connection_generation:socket1.hello.connection_generation,
    risk_book_sequence:opened.risk_book_sequence,
    policy_revision:opened.policy_revision,
    before_budget:before.budget,
    opening_budget:opened.budget,
    opening_source_digest:opening.source_digest,
  }));
  let held;
  if (!addMode) {
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
  held = await risk(socket1,"reserve_recorded_0001",reserveAction);
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
  } else {
    // Independent T09.4 campaign: do not create component-test reserves.
    // Real DO SQLite persistence, accounts, authority and 10 USD cap remain.
    socket1.ws.close();
    await first.dispose();
    first=null;
    second=new Miniflare(opts);
    const replay=await request(second,"/installation/initialize","POST",installation);
    assert.equal(replay.status,200);
    const saved=await request(second,"/account-governance");
    assert.deepEqual(saved.body.accounts.map(a=>a.account_key).sort(),accountKeys);
    const socket=await session(second);
    const clean=await summary(socket,"risk_t09_4_pristine");
    assert.equal(clean.budget.entry_budget.reserved_usd,"0");
    assert.equal(clean.budget.entry_budget.consumed_usd,"0");
    socket.ws.close();
  }
  if (process.env.OKX_NATIVE_WORKER_INTEGRATION === "1") {
    // The already-running Native process opened both real SQL accounts
    // BEFORE this Worker campaign was created. Supply only the actual
    // workerd connection metadata, never a synthetic SQL source.
    const url = await second.ready;
    assert.equal(url.hostname,"127.0.0.1","CI worker must be loopback-only");
    const wsUrl = new URL("/runtime",url);
    wsUrl.protocol = "ws:";
    await writeFile(handoffPath, JSON.stringify({
      ws_url:wsUrl.toString(),token,reservation_key:addMode ? null : held.reservation_key,
    }), {mode:0o600});
    let stdout, stderr;
    try {
      ({stdout,stderr} = await nativeProcess);
    } catch (error) {
      // cargo test --nocapture holds the original Native failure in stdout.
      // Emit only the bounded structured production denial; the outer test
      // runner's 200-char reason must never hide it behind a Node stack.
      const recorded = String(error.stdout ?? "");
      const matches = [...recorded.matchAll(/T09_1_NATIVE_CLOSE_FAILURE_V1:(\{[^\r\n]+\})/g)];
      if (matches.length) {
        process.stderr.write("T09_1_CLOSE_ERROR_FROM_RUST_V1:" +
          matches.at(-1)[1] + "\n");
      } else {
        // Rust's test harness can put a final panic in stdout, while cargo
        // reports only "test failed" on stderr. Inspect BOTH channels, then
        // forward the precise, bounded assertion instead of a generic child
        // process failure or the complete (potentially sensitive) transcript.
        const lines = [String(error.stdout ?? ""), String(error.stderr ?? "")]
          .flatMap(channel => channel.split(/\r?\n/))
          .map(line => line.replace(/\x1b\[[0-9;]*m/g, "").trim());
        const panic = lines.findLastIndex(line =>
          /thread '.+' panicked at|panicked at .*:[0-9]+:[0-9]+:/.test(line));
        const detail = panic >= 0
          ? lines.slice(panic, panic + 7)
          : lines.filter(line => /assertion|error: test failed|FAILED/.test(line)).slice(-5);
        const cause = detail.map(line => line.slice(0, 800)).join(" | ");
        const exitCode = String(error.code ?? "UNKNOWN").slice(0, 40);
        const phases = [...[String(error.stdout ?? ""), String(error.stderr ?? "")]
          .flatMap(channel => [...channel.matchAll(/T09_2_PHASE_V1:([A-Z_]+)/g)])
          .map(match => match[1])];
        process.stderr.write("T09_1_NATIVE_CHILD_FAILURE_V2:" + JSON.stringify({
          schema:"okx.t09-native-child-failure/v2",
          exit_code:exitCode,
          windows_stack_overflow:exitCode === "3221225725"
            || lines.some(line => /stack overflow|has overflowed its stack/i.test(line)),
          t09_2_last_phase:phases.at(-1) ?? null,
          t09_2_phases:phases.slice(-10),
          rust_assertion:cause.slice(0, 1900) || "RUST_CHILD_EXIT_WITHOUT_ASSERTION",
          maintenance_errors:lines.filter(line => line.includes("OKX_ENTRY_BASELINE_V1")).slice(-12).map(line => line.slice(0, 240)),
          managed_census_denials:lines.filter(line =>
            line.includes("MANAGED_AGGREGATE_CENSUS_DENIAL_V1:")).slice(-5).map(line => line.slice(0, 500)),
          restart_reached:recorded.includes("T09_1_NATIVE_SQL_RESTART_V1:"),
          terminal_reached:recorded.includes("T09_1_NATIVE_TERMINAL_CLOSE_V1:"),
        }) + "\n");
      }
      throw error;
    }
    assert.match(stdout,/native_to_persisted_workerd_riskbook PASS/,
      "real Rust client must prove Worker SQL summary and reservation");
    process.stdout.write(stdout);
    if (stderr) process.stderr.write(stderr);
    const afterNativeSocket = await session(second);
    const afterNative = await summary(afterNativeSocket, "risk_after_native_real_wss_0001");
    if (process.env.OKX_T09_1_STATUS_PROBE === "1") {
      const secondAdmission = process.env.OKX_T09_2_ADMISSION_PROBE === "1";
      const partialFills = process.env.OKX_T09_3_PARTIAL_PROBE === "1";
      const managedAdd = process.env.OKX_T09_4_MANAGED_ADD_PROBE === "1";
      const managedClose = process.env.OKX_T09_6_CLOSE_PROBE === "1";
      // T09.1 has already closed the exact public order and released its
      // reservation. Requiring reserved_loss > 2 here would incorrectly
      // demand that a settled RELEASE remain HELD. Compare two independent
      // actual WSS RiskBook reads across the Native process termination,
      // never a synthetic test-owned SETTLE_ENTRY or RELEASE.
      const nativeTerminal = receipt(stdout, "T09_1_NATIVE_TERMINAL_CLOSE_V1");
      const nativeRestart = receipt(stdout, "T09_1_NATIVE_SQL_RESTART_V1");
      assert.equal(nativeTerminal.opening_status?.data?.central_release?.state,
        "verified", "Native must prove opening central RELEASE");
      assert.equal(nativeTerminal.close_status?.data?.central_release?.state,
        "verified", "Native must prove terminal local Close");
      assert.equal(nativeTerminal.created_posts, 3,
        "one opening POST, one closing POST and one attached-protection cancel");
      assert.equal(nativeRestart.active_central_receipt, false,
        "Native SQL restart must not resurrect a released central reservation");
      assert.equal(nativeRestart.remaining_lot_count, managedClose ? 0 : managedAdd ? 3 : partialFills ? 2 : 0,
        "Native SQL restart must retain first close and optional second exposure");
      if (managedClose) {
        const terminal=receipt(stdout,"T09_6_NATIVE_TERMINAL_V1");
        assert.equal(terminal.remaining_lots,0);
        assert.equal(terminal.orphan_algos,0);
        assert.equal(terminal.worker_budget.entry_budget.consumed_usd,"6.003");
        assert.equal(terminal.worker_budget.entry_budget.reserved_usd,"0");
        assert.equal(terminal.worker_budget.reserved_loss_usd,"0");
        assert.equal(terminal.outbound_posts,9);
        // The fresh persisted Worker WSS read must match the final Native
        // close+release proof, not the earlier T09.4 managed-Add snapshot.
        // Never accept a stale/partially reserved budget as fully released.
        assert.deepEqual(afterNative.budget, terminal.worker_budget,
          "T09.6 persisted Worker budget must equal terminal Native close/release");
        assert.ok(afterNative.risk_book_sequence>=terminal.worker_risk_book_sequence,
          "T09.6 persisted Worker cannot roll back Native close/release revision");
        console.log("T09_6_WORKER_CLOSE_RECONCILED_V1:"+JSON.stringify({
          native_close_proof:terminal.worker_risk_book_sequence,
          worker_after_native_sequence:afterNative.risk_book_sequence,
          native_budget:terminal.worker_budget,
          worker_budget:afterNative.budget,
          connection_generation:afterNativeSocket.hello.connection_generation,
        }));
      }
      if (secondAdmission) {
        // This separate mode makes a new production Execute after terminal
        // release. Match the new post-ACK Worker budget, never the earlier
        // T09.1 terminal budget. The fresh read comes from persisted DO WSS.
        const admitted = receipt(stdout, "T09_2_NATIVE_ADMISSION_V1");
        assert.equal(admitted.state, "ACKNOWLEDGED");
        assert.equal(admitted.post_ack_financial_complete, false);
        assert.equal(admitted.create_posts_total, 4);
        assert.equal(admitted.quote_entry_usd, "3.0015");
        assert.equal(admitted.preview_reserved_entry_usd, managedAdd ? "0" : "4",
          "clean managed-Add acceptance cohort excludes T09.0 component reservations");
        assert.equal(afterNative.budget?.entry_budget?.reserved_usd,managedAdd?"0":partialFills?"4":"7.0015");
        assert.equal(afterNative.budget?.entry_budget?.consumed_usd,managedAdd?"6.003":partialFills?"4.002":"1.0005");
        assert.deepEqual(afterNative.budget,
          // T09.6 may include T09.2/3/4 earlier in its historical cohort,
          // but the authoritative readback is the *last* terminal state.
          managedClose ? receipt(stdout,"T09_6_NATIVE_TERMINAL_V1").worker_budget
          : managedAdd ? receipt(stdout,"T09_4_NATIVE_MANAGED_ADD_V1").worker_budget
          : partialFills ? receipt(stdout,"T09_3_NATIVE_PARTIAL_TERMINAL_V1").worker_budget
          : admitted.after_worker_budget,
          "T09.2/T09.6 persisted RiskBook must match latest Native production proof");
        console.log("T09_2_WORKER_AFTER_RECONNECT_V1:" + JSON.stringify({
          connection_generation:afterNativeSocket.hello.connection_generation,
          budget:afterNative.budget,
          risk_book_sequence:afterNative.risk_book_sequence,
        }));
      } else {
        assert.deepEqual(afterNative.budget, nativeTerminal.worker_budget,
          "reconnected persisted RiskBook must match terminal Native WSS budget");
        console.log("T09_1_WORKER_POST_FILL_EVIDENCE_V1:" + JSON.stringify({
          connection_generation:afterNativeSocket.hello.connection_generation,
          budget:afterNative.budget,
          risk_book_sequence:afterNative.risk_book_sequence,
        }));
      }
    } else {
      assert.ok(Number(afterNative.budget?.reserved_loss_usd) > 2,
        "T09.5 public Execute must reserve its own risk in central Worker");
      assert.equal(afterNative.budget?.entry_budget?.reserved_usd, "5.0005",
        "Native component reservations (4) plus public Execute (1.0005) must share one RiskBook");
      assert.equal(afterNative.budget?.entry_budget?.consumed_usd, "0",
        "POST authorization is not a fabricated cash debit");
      console.log("T09_5_WORKER_IDEMPOTENCY_EVIDENCE_V1:" + JSON.stringify({
        connection_generation:afterNativeSocket.hello.connection_generation,
        budget:afterNative.budget,
        risk_book_sequence:afterNative.risk_book_sequence,
      }));
    }
    afterNativeSocket.ws.close();
    if (process.env.OKX_T09_6_CLOSE_PROBE === "1") {
      // Actual persisted DO is destroyed and reconstructed AFTER the Native
      // close/release and Native SQL-owner reopen. No test-owned budget is
      // installed during recovery; the owner must read its same SQLite.
      const firstGeneration=afterNativeSocket.hello.connection_generation;
      const terminalBudget=afterNative.budget;
      const terminalSequence=afterNative.risk_book_sequence;
      await second.dispose();
      second=null;
      second=new Miniflare(opts);
      const initialization=await request(second,"/installation/initialize","POST",installation);
      assert.equal(initialization.status,200,
        "T09.7 persisted DO installation is idempotent after terminal release");
      const persistedSocket=await session(second);
      assert.ok(persistedSocket.hello.connection_generation>firstGeneration,
        "T09.7 DO connection generation must advance after real Worker restart");
      const persisted=await summary(persistedSocket,"risk_t09_7_after_terminal_restart");
      assert.deepEqual(persisted.budget,terminalBudget,
        "T09.7 Worker must reload exact source-backed settled budget");
      assert.ok(persisted.risk_book_sequence>=terminalSequence,
        "T09.7 must not roll back central RiskBook sequence");
      assert.equal(persisted.budget.entry_budget.consumed_usd,"6.003");
      assert.equal(persisted.budget.entry_budget.reserved_usd,"0");
      assert.equal(persisted.budget.reserved_loss_usd,"0");
      const sqlReopen=receipt(stdout,"T09_1_NATIVE_SQL_RESTART_V1");
      assert.equal(sqlReopen.remaining_lot_quantity,"0");
      assert.equal(sqlReopen.managed_add.active_central_receipt,false);
      assert.equal(sqlReopen.second_active_central_receipt,false);
      console.log("T09_7_WORKER_TERMINAL_RESTART_V1:"+JSON.stringify({
        worker_budget:persisted.budget,
        worker_risk_book_sequence:persisted.risk_book_sequence,
        previous_risk_book_sequence:terminalSequence,
        prior_connection_generation:firstGeneration,
        connection_generation:persistedSocket.hello.connection_generation,
        native_sql_reopened:true,
        native_remaining_lot_quantity:sqlReopen.remaining_lot_quantity,
        native_second_risk_held:sqlReopen.second_active_central_receipt,
        native_add_risk_held:sqlReopen.managed_add.active_central_receipt,
      }));
      persistedSocket.ws.close();
    }
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
