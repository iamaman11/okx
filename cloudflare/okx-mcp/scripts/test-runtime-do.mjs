#!/usr/bin/env node
// Workerd/Miniflare executes the ACTUAL RuntimeSession with SQLite-backed DO
// transactions. Only the incoming WSS frames are scripted. No Map storage,
// alternate RiskBook, exchange server, endpoint in production, or financial stub.
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { promisify } from "node:util";
import { execFile } from "node:child_process";
import { mkdtemp, readFile, rename, rm, stat, writeFile } from "node:fs/promises";
import { setTimeout as delay } from "node:timers/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { build } from "esbuild";
import { Miniflare } from "miniflare";
import { assertBudgetEconomicsUnchanged } from "../../../scripts/trade-integration/budget-evidence.mjs";

const crashChild=process.env.OKX_T10_12_WORKER_CRASH_CHILD==="1";
const crashRecovery=process.env.OKX_T10_12_WORKER_CRASH_RECOVERY==="1";
if(crashChild&&crashRecovery)throw Error("T10_12_CONFLICTING_TEST_MODES");
const recoveryRoot=process.env.OKX_T10_12_PERSISTED_ROOT;
if(crashRecovery&&(!recoveryRoot||
  !resolve(recoveryRoot).includes("okx-workerd-risk-")))
  throw Error("T10_12_REQUIRES_PRIOR_PERSISTED_STORAGE");
const root=crashRecovery ? resolve(recoveryRoot)
  :await mkdtemp(join(tmpdir(),"okx-workerd-risk-"));
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
const sharedCapMode=process.env.OKX_T10_7_SHARED_CAP_PROBE==="1";
const concurrentCapMode=process.env.OKX_T10_7_CONCURRENT_CAP_PROBE==="1";
const reserveReplyLossMode=process.env.OKX_T10_03_LOST_RESERVE_REPLY_PROBE==="1";
const specMode=process.env.OKX_T09_SPEC_SECOND_LINEAR_PROBE==="1";
const t10PostOutcomeMode=process.env.OKX_T10_05_LOST_ACK_PROBE==="1"
  ||process.env.OKX_T10_05_EXPLICIT_REJECT_PROBE==="1";
const t10SourceChangedMode=process.env.OKX_T10_15_POST_HELD_SOURCE_PROBE==="1";
const t10PostReplyLostMode=process.env.OKX_T10_03_POST_AUTH_REPLY_LOST_PROBE==="1";
const t10SourceRecoveryMode=process.env.OKX_T10_01_PARTIAL_SOURCE_RECOVERY_PROBE==="1";
const t10StopMode=process.env.OKX_T10_13_STOP_PROBE==="1";
if ((crashChild||crashRecovery)&&!sharedCapMode)
  throw Error("T10_12_REQUIRES_EXISTING_WORKER_SHARED_CAP_FIXTURE");
if(crashChild&&!reserveReplyLossMode)
  throw Error("T10_12_REQUIRES_UNREAD_ORIGINAL_RESERVE_REPLY");
if (reserveReplyLossMode && !sharedCapMode)
  throw Error("T10_03_REQUIRES_REAL_PERSISTED_SHARED_CAP_WORKER");
if (concurrentCapMode && (!sharedCapMode || reserveReplyLossMode))
  throw Error("T10_07_CONCURRENT_REQUIRES_ISOLATED_SHARED_CAP_WITHOUT_REPLY_LOSS");
if (sharedCapMode && (addMode || process.env.OKX_NATIVE_WORKER_INTEGRATION==="1"))
  throw Error("SHARED_CAP_WORKER_PROBE_MUST_BE_ISOLATED_FROM_NATIVE_FINANCIAL_SCENARIO");
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
    closed(){
      return new Promise((resolve,reject)=>{
        const timeout=setTimeout(()=>reject(Error("T10_12_STALE_FRAME_CLOSE_TIMEOUT")),5000);
        ws.addEventListener("close",e=>{clearTimeout(timeout);resolve(e.code)},{once:true});
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
async function runRuntimeFixture(){
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
  if(crashRecovery){
    const proof=JSON.parse(await readFile(join(root,"t10-12-handoff.json"),"utf8"));
    assert.equal(proof.schema,"okx.t10-12-worker-pre-kill/v1");
    const installed=await request(first,"/installation/initialize","POST",installation);
    assert.equal(installed.status,200);
    const recovered=await session(first);
    assert.ok(recovered.hello.connection_generation>proof.connection_generation,
      "independent new process must advance persisted Durable Object generation");
    const before=await summary(recovered,"t10_12_recovered_risk");
    assert.ok(before.risk_book_sequence>=proof.risk_book_sequence);
    for(const [field,value] of Object.entries({
      limit_usd:"10",reserved_usd:"8",remaining_usd:"2",consumed_usd:"0"
    })){
      assert.equal(before.budget.entry_budget[field],value,
        "no financial reset after Worker process kill: "+field);
    }
    const original=await risk(recovered,"t10_12_original_held_inspect",{
      action:"INSPECT",reservation_key:proof.reservation_key,
      plan_sha256:proof.plan_sha256,
    });
    assert.equal(original.status,"OBSERVED");
    assert.equal(original.reservation_state,"HELD");
    const closed=recovered.ws.closed();
    recovered.ws.send({
      type:"risk_control",schema:FRAME_SCHEMA,
      session_id:proof.session_id,
      connection_generation:proof.connection_generation,
      command:{
        schema:"okx.risk-command/v1",
        command_id:"t10_12_old_session_summary_rejected",
        installation_id:configuration.installation_id,
        action:{action:"SUMMARY"},
      },
    });
    const staleCloseCode=await closed;
    assert.equal(staleCloseCode,1008,
      "old generation may not send a risk command on new session");
    const current=await session(first);
    const after=await summary(current,"t10_12_after_stale_session_denial");
    assert.equal(after.risk_book_sequence,before.risk_book_sequence);
    assert.deepEqual(after.budget.entry_budget,before.budget.entry_budget);
    console.log("T10_12_ACTUAL_WORKER_PROCESS_RECOVERY_V1:"+JSON.stringify({
      schema:"okx.t10-12-worker-process-recovery/v1",
      persisted_do_reopened:true,
      generation_before:proof.connection_generation,
      generation_after:current.hello.connection_generation,
      sequence_before:proof.risk_book_sequence,
      sequence_after:after.risk_book_sequence,
      consumed_usd:after.budget.entry_budget.consumed_usd,
      reserved_usd:after.budget.entry_budget.reserved_usd,
      remaining_usd:after.budget.entry_budget.remaining_usd,
      original_state:original.reservation_state,
      original_reply_unread:true,
      stale_frame_close_code:staleCloseCode,
      native_sql_and_settlement_crash_proven:false,
    }));
    current.ws.close();
    return;
  }
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
    // A remains BTC-only even if public metadata discovers ETH. B's ETH
    // permission is an explicit governed account choice, not a side effect
    // of registry discovery or a hidden ticker whitelist.
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
    limits:specMode?{
      ...limits,allowed_instruments:["BTC-USDT-SWAP","ETH-USDT-SWAP"]
    }:limits,credential_ref:"fixture-credentials-b",
    idempotency_key:"enroll_recorded_account_002",
  });
  assert.equal(enrollmentB.status,200,"second account must be enrolled in the same Worker");
  const accountKeys = [accountKey,accountKeyB].sort();
  assert.equal(new Set(accountKeys).size,2,"two distinct verified account identities");
  const enrolled = await request(first,"/account-governance");
  assert.equal(enrolled.status,200);
  assert.deepEqual(enrolled.body.accounts.map((a) => a.account_key).sort(),accountKeys);
  let sharedStrategyGovernance = null;
  if (sharedCapMode) {
    // Use the ordinary authenticated governance CAS; no test-only strategy
    // whitelist or separate RiskBook. Real account revisions advance to 3.
    const activeStrategies = {
      [accountKey]: "native_alpha",
      [accountKeyB]: "native_beta",
    };
    const update = {
      expected_revision: enrolled.body.revision,
      idempotency_key: "governance_two_account_strategies_0001",
      max_total_modelled_loss_usdt: enrolled.body.max_total_modelled_loss_usdt,
      accounts: enrolled.body.accounts.map((entry) => ({
        id: entry.id,
        label: entry.label,
        enabled: entry.enabled,
        explicit_pause: entry.explicit_pause,
        limits: entry.limits,
        strategies: [{
          id: activeStrategies[entry.account_key],
          enabled: true,
          allocated_capital_usdt: "1",
          max_modelled_loss_usdt: "1",
          max_daily_realized_loss_usdt: "2",
          max_drawdown_ratio: "0.1",
        }],
      })),
    };
    const changed = await request(first,"/account-governance","POST",update);
    assert.equal(changed.status,200,"real account-strategy governance CAS must commit");
    const saved = await request(first,"/account-governance");
    assert.equal(saved.status,200);
    assert.equal(saved.body.revision,enrolled.body.revision+1);
    for (const entry of saved.body.accounts) {
      assert.equal(entry.policy_revision,saved.body.revision);
      assert.deepEqual(entry.strategies.map(s=>s.id),[activeStrategies[entry.account_key]]);
      assert.equal(entry.strategies[0].enabled,true);
    }
    sharedStrategyGovernance = {
      revision: saved.body.revision,
      by_account: Object.fromEntries(saved.body.accounts.map(entry=>[
        entry.account_key,entry.strategies[0].id
      ])),
    };
  }
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
  if (sharedCapMode) {
    // Dedicated component of #449 T10-07 on the ACTUAL persisted
    // RuntimeSession/SQLite DO, not a JS RiskBook replica. A full T10-07
    // still additionally requires public Native Execute and SQL/venue proof.
    const observedAt=Date.now()-500;
    const census={
      complete:true,source_digest:"d".repeat(64),
      content_digest:"a".repeat(64),
      generated_at_ms:observedAt,source_read_floor_ms:observedAt,
      account_keys:accountKeys,
      occupied:accountKeys.map(account_key=>({
        account_key,occupied_usdt:"0",cumulative_loss_usd:"0"
      })),
      native_account_evidence_complete:true,
      native_order_evidence_complete:true,
      native_ledger_evidence_complete:true,
    };
    const requested=(accountUid,strategy,intentSuffix,planDigest)=>({
      action:"RESERVE",venue:"okx",account_uid:accountUid,
      run_id:"run_central_cap_0001",
      intent_id:"intent_central_cap_"+intentSuffix,
      strategy_id:strategy,instrument_id:"BTC-USDT-SWAP",
      plan_sha256:planDigest.repeat(64),
      requested_loss_usdt:"1",requested_loss_usd:"1",
      entry_upper_usd:"4",entry_proof_digest:"e".repeat(64),
      usd_conversion:{usd_per_usdt:"1",
        source_digest:"e".repeat(64),observed_at_ms:observedAt},
      policy_revision:sharedStrategyGovernance.revision,census
    });
    assert.ok(sharedStrategyGovernance,"strategy policy must originate from actual Worker CAS");
    // Same externally visible intent key is legal in TWO distinct verified
    // account scopes. Identity must be account-qualified even under concurrent
    // RESERVE; the plans differ deliberately to detect cross-account reuse.
    const firstRequest=requested(uid,"native_alpha","same_0001","a");
    const secondRequest=requested(uidB,"native_beta","same_0001","b");
    const thirdRequest=requested(uid,"native_alpha","a_0002","c");
    assert.equal(firstRequest.intent_id, secondRequest.intent_id);
    assert.notEqual(firstRequest.account_uid, secondRequest.account_uid);
    // A valid UID and authentic policy generation must not let B borrow A's
    // exclusively registered strategy. This must be denied before any risk
    // can be held, independent of remaining shared campaign headroom.
    const foreignStrategy = requested(uidB,"native_alpha","foreign_a_0001","d");
    const foreignDecision = await risk(
      socket1,"reserve_foreign_strategy_0001",foreignStrategy
    );
    assert.equal(foreignDecision.status,"DENIED",
      "foreign strategy must never be authorized by account-level fallback");
    assert.equal(foreignDecision.reason,"STRATEGY_NOT_ENABLED_FOR_ACCOUNT");
    const afterForeign=await summary(socket1,"summary_foreign_strategy_denied_0001");
    assert.equal(afterForeign.budget.entry_budget.reserved_usd,"0");
    assert.equal(afterForeign.budget.reserved_loss_usd,"0");
    // Both accounts genuinely contend over one SQLite RiskBook owner.
    // Distinct command IDs mean concurrent WSS results cannot be confused.
    let firstReserve,secondReserve,concurrentThirdDecision;
    if (reserveReplyLossMode) {
      // T10-03 causal fault: the real Worker commits RESERVE A, but the
      // client deliberately does NOT consume A's risk_control_result.
      // The same socket submits B and queries the authoritative Worker
      // balance; A's orphaned reply stays unread in the receive queue.
      // This is a delivery fault, never a fake Worker decision or SQL edit.
      socket1.ws.send({
        type:"risk_control",schema:FRAME_SCHEMA,
        session_id:socket1.hello.session_id,
        connection_generation:socket1.hello.connection_generation,
        command:{
          schema:"okx.risk-command/v1",
          command_id:"reserve_lost_reply_a_0001",
          installation_id:configuration.installation_id,
          action:firstRequest,
        },
      });
      secondReserve=await risk(socket1,"reserve_concurrent_b_0001",secondRequest);
      const authoritative=await summary(socket1,"summary_t10_03_after_lost_reply");
      assert.equal(authoritative.budget.entry_budget.reserved_usd,"8",
        "both committed real Worker reservations survive unread A reply");
      assert.equal(authoritative.budget.entry_budget.consumed_usd,"0");
      // Replay is an identity-bound INSPECT-like idempotent return, not a
      // fresh reserve or venue POST. The first reply is never read by caller.
      firstReserve=await risk(socket1,"reserve_reinspect_a_0001",firstRequest);
      assert.equal(firstReserve.status,"HELD");
      const afterReplay=await summary(socket1,"summary_t10_03_after_same_identity");
      assert.equal(afterReplay.budget.entry_budget.reserved_usd,"8");
      assert.equal(afterReplay.budget.reserved_loss_usd,
        authoritative.budget.reserved_loss_usd);
      console.log("T10_03_WORKER_LOST_RESERVE_REPLY_V1:"+JSON.stringify({
        schema:"okx.t10-03-worker-lost-reserve-reply/v1",
        real_sqlite_do:true,
        account_a:accountKey,account_b:accountKeyB,
        original_command_id:"reserve_lost_reply_a_0001",
        original_reply_consumed:false,
        same_intent_id:firstRequest.intent_id,
        replay_command_id:"reserve_reinspect_a_0001",
        replay_status:firstReserve.status,
        durable_reservation_key:firstReserve.reservation_key,
        worker_sequence_after_commit:authoritative.risk_book_sequence,
        worker_sequence_after_replay:afterReplay.risk_book_sequence,
        budget_after_commit:authoritative.budget,
        budget_after_replay:afterReplay.budget,
      }));
    } else if (concurrentCapMode) {
      // Three independent commands enter the SAME authenticated WSS session
      // before awaiting any reply. The real SQLite DO, not this test,
      // serializes the campaign-cap decisions. WSS command order is A/B/A2.
      [firstReserve,secondReserve,concurrentThirdDecision]=await Promise.all([
        risk(socket1,"reserve_three_way_a_0001",firstRequest),
        risk(socket1,"reserve_three_way_b_0001",secondRequest),
        risk(socket1,"reserve_three_way_a_0002",thirdRequest),
      ]);
    } else {
      [firstReserve,secondReserve]=await Promise.all([
        risk(socket1,"reserve_concurrent_a_0001",firstRequest),
        risk(socket1,"reserve_concurrent_b_0001",secondRequest),
      ]);
    }
    assert.equal(firstReserve.status,"HELD","account A must receive real held risk");
    assert.equal(secondReserve.status,"HELD","account B must receive real held risk");
    assert.notEqual(firstReserve.reservation_key,secondReserve.reservation_key);
    const beforeDenied=await summary(socket1,"summary_before_third_cap_0001");
    assert.equal(beforeDenied.budget.entry_budget.reserved_usd,"8");
    assert.equal(beforeDenied.budget.entry_budget.remaining_usd,"2");
    const denied=concurrentCapMode ? concurrentThirdDecision
      : await risk(socket1,"reserve_third_over_cap_0001",thirdRequest);
    assert.equal(denied.status,"DENIED");
    assert.equal(denied.reason,"CAMPAIGN_ENTRY_BUDGET_EXHAUSTED");
    const repeated=await risk(socket1,"reserve_duplicate_a_0001",firstRequest);
    assert.equal(repeated.status,"HELD");
    assert.equal(repeated.reservation_key,firstReserve.reservation_key,
      "replaying A's same intent never creates a third reservation");
    const budgetBeforeRestart=await summary(socket1,"summary_after_denied_cap_0001");
    assert.equal(budgetBeforeRestart.budget.entry_budget.limit_usd,"10");
    assert.equal(budgetBeforeRestart.budget.entry_budget.consumed_usd,"0");
    assert.equal(budgetBeforeRestart.budget.entry_budget.reserved_usd,"8");
    assert.equal(budgetBeforeRestart.budget.entry_budget.remaining_usd,"2");
    assert.equal(budgetBeforeRestart.budget.reserved_loss_usd,"2");
    assert.equal(budgetBeforeRestart.budget.remaining_loss_usd,"8");
    // A scripted WSS command is NOT the persisted NativeAccountSQL owner's
    // unsent-release proof. Missing or foreign proof must never free budget.
    // These are DENIAL inputs only; no test mutates SQL/Worker financial state.
    const withoutSqlProof=await risk(
      socket1,"release_without_native_sql_proof_0001",{
        action:"RELEASE_UNSENT",
        reservation_key:firstReserve.reservation_key,
        plan_sha256:firstRequest.plan_sha256,
        expected_risk_book_sequence:budgetBeforeRestart.risk_book_sequence,
      }
    );
    assert.equal(withoutSqlProof.status,"DENIED");
    assert.equal(withoutSqlProof.reason,"INVALID_UNSENT_RELEASE_COMMAND");
    const wrongAccountProof=await risk(
      socket1,"release_wrong_sql_owner_0001",{
        action:"RELEASE_UNSENT",
        reservation_key:firstReserve.reservation_key,
        plan_sha256:firstRequest.plan_sha256,
        expected_risk_book_sequence:budgetBeforeRestart.risk_book_sequence,
        proof:{
          reservation_key:firstReserve.reservation_key,
          account_key:accountKeyB,
          intent_id:firstRequest.intent_id,
          plan_hash:firstRequest.plan_sha256,
          request_hash:"a".repeat(64),
          reason:"REJECTED_UNSENT",
          record_digest:"b".repeat(64),
        },
      }
    );
    assert.equal(wrongAccountProof.status,"DENIED");
    assert.ok(["INVALID_UNSENT_SQL_PROOF",
      "UNSENT_RELEASE_IDENTITY_OR_STATE_MISMATCH"].includes(wrongAccountProof.reason),
      "a foreign account cannot free A's central reservation");
    const afterInvalidRelease=await summary(
      socket1,"summary_after_unproven_release_denials_0001");
    assert.equal(afterInvalidRelease.risk_book_sequence,
      budgetBeforeRestart.risk_book_sequence,
      "invalid release cannot advance Worker money/CAS sequence");
    assert.deepEqual(afterInvalidRelease.budget.entry_budget,
      budgetBeforeRestart.budget.entry_budget);
    if(crashChild){
      const handoff={
        schema:"okx.t10-12-worker-pre-kill/v1",
        session_id:socket1.hello.session_id,
        connection_generation:socket1.hello.connection_generation,
        risk_book_sequence:budgetBeforeRestart.risk_book_sequence,
        reservation_key:firstReserve.reservation_key,
        plan_sha256:firstRequest.plan_sha256,
        entry_reserved_usd:budgetBeforeRestart.budget.entry_budget.reserved_usd,
        original_reply_unread:true,
      };
      assert.equal(reserveReplyLossMode,true);
      await writeFile(join(root,"t10-12-handoff.json"),JSON.stringify(handoff)+"\n");
      process.stdout.write("T10_12_WORKER_COMMIT_READY_V1:"+
        JSON.stringify({root,...handoff})+"\n");
      // The OS parent terminates this process tree with the real SQLite DO
      // open. No return, fake Err, in-process dispose or Rust struct reset.
      while(true)await delay(30_000);
    }
    socket1.ws.close();
    await first.dispose();
    first=null;
    second=new Miniflare(opts);
    const installationReplay=await request(second,"/installation/initialize","POST",installation);
    assert.equal(installationReplay.status,200);
    const restarted=await session(second);
    const persisted=await summary(restarted,"summary_shared_cap_restart_0001");
    assert.ok(restarted.hello.connection_generation>socket1.hello.connection_generation);
    assert.equal(persisted.budget.entry_budget.consumed_usd,"0");
    assert.equal(persisted.budget.entry_budget.reserved_usd,"8");
    assert.equal(persisted.budget.entry_budget.remaining_usd,"2");
    assert.equal(persisted.budget.reserved_loss_usd,"2");
    assert.equal(persisted.budget.remaining_loss_usd,"8");
    assert.ok(persisted.risk_book_sequence>=budgetBeforeRestart.risk_book_sequence);
    const inspectA=await risk(restarted,"inspect_shared_cap_a_0001",{
      action:"INSPECT",reservation_key:firstReserve.reservation_key,
      plan_sha256:firstRequest.plan_sha256
    });
    const inspectB=await risk(restarted,"inspect_shared_cap_b_0001",{
      action:"INSPECT",reservation_key:secondReserve.reservation_key,
      plan_sha256:secondRequest.plan_sha256
    });
    assert.equal(inspectA.status,"OBSERVED");
    assert.equal(inspectB.status,"OBSERVED");
    assert.equal(inspectA.reservation_state,"HELD");
    assert.equal(inspectB.reservation_state,"HELD");
    // A three-way command burst is checked separately from the original
    // two-way/reply-loss component probe. No simulated venue POST or release.
    if (concurrentCapMode) {
      const decisions=[firstReserve,secondReserve,denied];
      assert.deepEqual(decisions.map(d=>d.status),["HELD","HELD","DENIED"]);
      assert.equal(persisted.budget.entry_budget.reserved_usd,"8");
      assert.equal(persisted.budget.entry_budget.remaining_usd,"2");
      console.log("T10_7_WORKER_CONCURRENT_CAP_V1:"+JSON.stringify({
        schema:"okx.t10-worker-concurrent-cap/v1",
        actual_worker_sqlite_do:true,
        command_ids:["reserve_three_way_a_0001","reserve_three_way_b_0001",
          "reserve_three_way_a_0002"],
        command_count:3,
        statuses:decisions.map(d=>d.status),
        held_count:decisions.filter(d=>d.status==="HELD").length,
        denied_count:decisions.filter(d=>d.status==="DENIED").length,
        denied_reason:denied.reason,
        two_distinct_verified_accounts:firstRequest.account_uid!==secondRequest.account_uid,
        held_reservations_distinct:firstReserve.reservation_key!==secondReserve.reservation_key,
        limit_usd:persisted.budget.entry_budget.limit_usd,
        reserved_usd:persisted.budget.entry_budget.reserved_usd,
        remaining_usd:persisted.budget.entry_budget.remaining_usd,
        consumed_usd:persisted.budget.entry_budget.consumed_usd,
        before_sequence:budgetBeforeRestart.risk_book_sequence,
        persisted_sequence:persisted.risk_book_sequence,
        persisted_generation_advanced:
          restarted.hello.connection_generation>socket1.hello.connection_generation,
      }));
    }
    // No fabricated SQL REJECTED_UNSENT receipt is supplied to release money.
    console.log("T10_7_WORKER_SHARED_CAP_V1:"+JSON.stringify({
      schema:"okx.t10-shared-worker-cap/v1",
      account_a:accountKey,account_b:accountKeyB,
      a_strategy:firstRequest.strategy_id,
      b_strategy:secondRequest.strategy_id,
      policy_revision:sharedStrategyGovernance.revision,
      saved_strategy_ids:sharedStrategyGovernance.by_account,
      instrument_id:firstRequest.instrument_id,
      same_intent_id_cross_account:firstRequest.intent_id===secondRequest.intent_id,
      independent_account_scope:firstRequest.account_uid!==secondRequest.account_uid,
      distinct_reservation_keys:firstReserve.reservation_key!==secondReserve.reservation_key,
      first_status:firstReserve.status,
      second_status:secondReserve.status,
      third_status:denied.status,
      third_reason:denied.reason,
      foreign_strategy_status:foreignDecision.status,
      foreign_strategy_reason:foreignDecision.reason,
      foreign_strategy_no_budget_reserved:
        afterForeign.budget.entry_budget.reserved_usd==="0" &&
        afterForeign.budget.reserved_loss_usd==="0",
      replay_status:repeated.status,
      replay_reservation_identical:repeated.reservation_key===firstReserve.reservation_key,
      without_native_sql_proof_status:withoutSqlProof.status,
      without_native_sql_proof_reason:withoutSqlProof.reason,
      foreign_sql_owner_release_status:wrongAccountProof.status,
      foreign_sql_owner_release_reason:wrongAccountProof.reason,
      denied_release_sequence_unchanged:
        afterInvalidRelease.risk_book_sequence===budgetBeforeRestart.risk_book_sequence,
      persisted_a_state:inspectA.reservation_state,
      persisted_b_state:inspectB.reservation_state,
      before_budget:budgetBeforeRestart.budget,
      after_budget:persisted.budget,
      before_sequence:budgetBeforeRestart.risk_book_sequence,
      after_sequence:persisted.risk_book_sequence,
      connection_generation_before:socket1.hello.connection_generation,
      connection_generation_after:restarted.hello.connection_generation
    }));
    restarted.ws.close();
  } else if (!addMode) {
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
    // Publish only a COMPLETE handoff. The Rust test waits for path
    // existence, so writing directly to that name lets it read an empty
    // or truncated JSON file on Windows before writeFile finishes.
    // Same-directory rename is the sole readiness barrier; no new service,
    // test exchange, financial receipt or timeout is involved.
    const stagedHandoff = handoffPath + ".pending";
    await writeFile(stagedHandoff, JSON.stringify({
      ws_url:wsUrl.toString(),token,reservation_key:addMode ? null : held.reservation_key,
    }), {mode:0o600});
    await rename(stagedHandoff, handoffPath);
    // S2-05: alter REAL persisted Worker governance only after Native has
    // committed and replayed an immutable public order. The authenticated
    // RuntimeSession broadcasts the new PolicySnapshot on its existing WSS.
    // Files are bounded synchronization markers, not financial/source facts.
    const policyCas = process.env.OKX_S2_05_POLICY_CAS_PROBE === "1"
      ? (async () => {
        const ready = handoffPath + ".s2-policy-cas-ready";
        const until = Date.now() + 90_000;
        let present = false;
        while (Date.now() < until) {
          try { await stat(ready); present = true; break; }
          catch (error) { if (error?.code !== "ENOENT") throw error; }
          await delay(25);
        }
        assert.ok(present, "S2-05 Native ACK+replay causality marker missing");
        const prior = await request(second, "/account-governance");
        assert.equal(prior.status, 200);
        const changed = await request(second, "/account-governance/pause", "POST", {
          account: accountKey, explicit_pause: true,
          expected_revision: prior.body.revision,
          idempotency_key: "s2_05_worker_policy_pause_after_ack_0001",
        });
        assert.equal(changed.status, 200, "S2-05 actual Worker governance CAS");
        const persisted = await request(second, "/account-governance");
        assert.equal(persisted.status, 200);
        assert.equal(persisted.body.revision, prior.body.revision + 1);
        assert.equal(persisted.body.accounts.find(a =>
          a.account_key === accountKey).explicit_pause, true);
        await writeFile(
          handoffPath + ".s2-policy-cas-committed",
          String(persisted.body.revision), {mode:0o600},
        );
        return {prior_revision:prior.body.revision,
          new_revision:persisted.body.revision,
          account_paused:true, persisted_worker_durable_object:true};
      })().then(value => ({ok:true,value}), error => ({ok:false,error}))
      : Promise.resolve({ok:true,value:null});
    // T10-13 STOP occurs after authenticated fill+bill/status but before
    // the same Native account submits its required real protective Close.
    // The Worker is the only governance writer, and propagates a normal
    // PolicySnapshot on the existing WSS session.
    const stopCas=t10StopMode
      ? (async()=>{
          const ready=handoffPath+".t10-stop-after-fill-ready";
          const until=Date.now()+90_000;
          let present=false;
          while(Date.now()<until){
            try{await stat(ready);present=true;break;}
            catch(error){if(error?.code!=="ENOENT")throw error;}
            await delay(25);
          }
          assert.ok(present,"T10-13 actual Native filled source handoff missing");
          const prior=await request(second,"/account-governance");
          assert.equal(prior.status,200);
          const changed=await request(second,"/account-governance/pause","POST",{
            account:accountKey,explicit_pause:true,
            expected_revision:prior.body.revision,
            idempotency_key:"t10_13_stop_after_real_fill_0001",
          });
          assert.equal(changed.status,200,"T10-13 real Worker STOP CAS");
          const saved=await request(second,"/account-governance");
          assert.equal(saved.status,200);
          assert.equal(saved.body.revision,prior.body.revision+1);
          assert.equal(saved.body.accounts.find(a=>
            a.account_key===accountKey).explicit_pause,true);
          return {
            schema:"okx.t10-13-worker-stop/v1",
            account_key:accountKey,
            before_revision:prior.body.revision,
            stop_revision:saved.body.revision,
            persisted_worker_durable_object:true,
            explicit_pause:true,
          };
        })().then(value=>({ok:true,value}),error=>({ok:false,error}))
      : Promise.resolve({ok:true,value:null});
    let stdout, stderr;
    try {
      ({stdout,stderr} = await nativeProcess);
      const rotated = await policyCas;
      if (!rotated.ok) throw rotated.error;
      if (rotated.value) console.log("S2_05_WORKER_POLICY_CAS_V1:" +
        JSON.stringify(rotated.value));
      const stopped=await stopCas;
      if (!stopped.ok) throw stopped.error;
      if (stopped.value) console.log("T10_13_WORKER_STOP_V1:"+
        JSON.stringify(stopped.value));
    } catch (error) {
      // cargo test --nocapture holds the original Native failure in stdout.
      // Emit only the bounded structured production denial; the outer test
      // runner's 200-char reason must never hide it behind a Node stack.
      const recorded = String(error.stdout ?? "");
      // Keep the FIRST typed Native B positive refusal visible to CI.
      // Never expose raw authenticated HTTP bytes or credentials.
      const rawChildErrorChannels = recorded + "\n" + String(error.stderr ?? "");

      // Preserve only bounded, redacted provenance from first failed census.
      for (const [marker,schema] of [
        ["B_CENSUS_FIRST_FAILURE_V1","okx.b-census-first-failure/v1"],
        ["B_CENSUS_FINANCIAL_GAP_V1","okx.b-census-financial-gap/v1"],
      ]) {
        const samples=[...rawChildErrorChannels.matchAll(
          new RegExp(marker+":(\\{[^\\r\\n]+\\})","g")
        )];
        for(const sample of samples.slice(0,8)){
          try {
            const data=JSON.parse(sample[1]);
            if(data.schema===schema)
              process.stderr.write(marker+":"+JSON.stringify(data)+"\n");
          }catch { /* Diagnostic must not affect the test oracle. */ }
        }
      }
      // Forward a bounded, source-redacted classification emitted by
      // TradeService at the FIRST failed order readback. The outer Node
      // adapter otherwise surfaces only an uninformative UNKNOWN status.
      for (const marker of [
        "T09_6_ORDER_READBACK_CAUSE_V1",
        "T09_6_CLOSE_READBACK_TRANSCRIPT_V1",
      ]) {
        const samples=[...rawChildErrorChannels.matchAll(
          new RegExp(marker + ":(\\{[^\\r\\n]+\\})", "g")
        )];
        for (const sample of samples.slice(-4)) {
          const fact=JSON.parse(sample[1]);
          if (typeof fact.schema==="string" &&
              fact.schema.startsWith("okx.t09-6-")) {
            process.stderr.write(marker + ":" + JSON.stringify(fact) + "\n");
          }
        }
      }
      const b1Prepost = [...rawChildErrorChannels.matchAll(
        /B1_B_PREPOST_CAUSE_V1:(\{[^\r\n]+\})/g
      )];
      if (b1Prepost.length) {
        process.stderr.write("B1_B_PREPOST_CAUSE_V1:" +
          b1Prepost.at(-1)[1] + "\n");
      }
      const b1Transport = [...rawChildErrorChannels.matchAll(
        /B1_POSITIVE_TRANSPORT_FAILURE_V1:(\{[^\r\n]+\})/g
      )];
      if (b1Transport.length) {
        const source = JSON.parse(b1Transport.at(-1)[1]);
        const scopes = Object.fromEntries(Object.entries(source.a_algo_scopes ?? {})
          .filter(([scope, count]) =>
            /^(?:SWAP|FUTURES):(?:conditional|oco|trigger|move_order_stop|chase|iceberg|twap|smart_iceberg)$/.test(scope) &&
            Number.isSafeInteger(count) && count >= 0));
        process.stderr.write("B1_POSITIVE_TRANSPORT_FAILURE_V1:" + JSON.stringify({
          schema: "okx.b1-transport-cause/v1",
          phase: "B_POSITIVE_AFTER_A_ACK",
          reason_code: [
            "TRANSCRIPT_SOURCE_PAGE_UNEXPECTED_OR_EXHAUSTED",
            "TRANSCRIPT_REPEAT_EXCEEDED", "LOCAL_RATE_DEFER",
            "OTHER_B1_EXECUTE_FAILURE",
          ].includes(source.reason_code) ? source.reason_code : "UNRECOGNIZED",
          source_actor: "fixture-observer-a",
          source_route: "/api/v5/trade/orders-algo-pending",
          a_algo_success_reads_total: source.a_algo_success_reads_total,
          a_algo_success_reads_since_positive: source.a_algo_success_reads_since_positive,
          a_algo_scopes: scopes,
          outbound_posts: source.outbound_posts,
        }) + "\n");
      }
      const b1Refusal = [...recorded.matchAll(
        /B1_POSITIVE_DIAGNOSTIC_V1:(\{[^\r\n]+\})/g
      )];
      if (b1Refusal.length) {
        const source = JSON.parse(b1Refusal.at(-1)[1]);
        process.stderr.write("B1_POSITIVE_DENIAL_EVIDENCE_V1:" +
          JSON.stringify({
            status:source.status ?? null,
            state:source.state ?? null,
            reason:String(source.primary_reason ?? "NOT_REPORTED").slice(0,170),
            origin:source.rejection_origin ?? null,
            send_state:source.send_state ?? null,
            posts:source.outbound_posts ?? null,
            balance_phase_ms:source.recorded_balance_phase_ms ?? null,
            denied_routes:(source.denied_routes ?? []).slice(0,5).map(x=>{
              const match=String(x).match(
                /^(TRANSCRIPT_[A-Z_]+):(fixture-(?:observer|executor)-[ab]):(\/api\/v5\/[a-z0-9/-]+)$/
              );
              return match ? {code:match[1],fixture:match[2],route:match[3]}
                : {code:String(x).split(":")[0].slice(0,100)};
            }),
          }) + "\n");
      }
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
        // A causal T10-09 Native SQL timeout is lost when cargo stdout is
        // wrapped by the nested worker child. Forward only typed financial
        // counters and stable source errors, never signed payloads or keys.
        const bTimeoutLine = lines.findLast(line =>
          line.startsWith("T10_09_B_CAPTURE_TIMEOUT_V1:"));
        let bTimeout = null;
        if (bTimeoutLine) {
          try {
            const data = JSON.parse(bTimeoutLine.slice(
              "T10_09_B_CAPTURE_TIMEOUT_V1:".length));
            if (data.schema === "okx.t10-09-signed-b-capture-timeout/v1") {
              const sql = data.last_sql ?? {};
              const health = data.source_health ?? {};
              const reads = data.signed_source_reads ?? {};
              const decimal = value => typeof value === "string" &&
                /^(?:0|[1-9][0-9]{0,11})(?:\.[0-9]{1,9})?$/.test(value)
                ? value : null;
              const count = value => Number.isSafeInteger(value) && value>=0 ? value : null;
              bTimeout = {
                sql: {
                  quantity:decimal(sql.quantity),
                  fill_count:count(sql.fill_count),
                  bill_count:count(sql.bill_count),
                  verified_fee_usdt:decimal(sql.verified_fee_usdt),
                  fill_watermark:count(sql.fill_watermark),
                  bill_watermark:count(sql.bill_watermark),
                  fill_gaps_empty:sql.fill_gaps==="",
                  bill_gaps_empty:sql.bill_gaps==="",
                  active_central_receipt:sql.active_central_receipt===true,
                  sql_readback_error:typeof sql.sql_readback_error==="string"
                    ?sql.sql_readback_error.slice(0,120):null,
                },
                source_health: {
                  reason_code:/^[A-Z0-9_]{1,96}$/.test(health.reason_code??"")
                    ?health.reason_code:null,
                  last_attempt_ms:count(health.last_attempt_ms),
                  last_success_ms:count(health.last_success_ms),
                  last_failure_ms:count(health.last_failure_ms),
                },
                signed_source_reads: {
                  fills:count(reads.fills),bills:count(reads.bills),
                  positions:count(reads.positions),balance:count(reads.balance),
                  order:count(reads.order),
                },
                denied_route_count:count(data.denied_route_count),
              };
            }
          } catch { bTimeout={error:"INVALID_BOUNDED_CAPTURE_DIAGNOSTIC"}; }
        }
        process.stderr.write("T09_1_NATIVE_CHILD_FAILURE_V2:" + JSON.stringify({
          schema:"okx.t09-native-child-failure/v2",
          exit_code:exitCode,
          windows_stack_overflow:exitCode === "3221225725"
            || lines.some(line => /stack overflow|has overflowed its stack/i.test(line)),
          t09_2_last_phase:phases.at(-1) ?? null,
          t09_2_phases:phases.slice(-10),
          // Bounded counters from the actual recorded HTTP transport.
          // Never forward arbitrary request URLs, bodies or credentials.
          t09_3_source_reads:lines.filter(line =>
            line.startsWith("T09_3_SOURCE_READS_V1 ")).slice(-6)
            .map(line=>line.slice(0, 360)),
          rust_assertion:cause.slice(0, 1900) || "RUST_CHILD_EXIT_WITHOUT_ASSERTION",
          t10_09_capture:bTimeout,
          maintenance_errors:lines.filter(line => line.includes("OKX_ENTRY_BASELINE_V1")).slice(-12).map(line => line.slice(0, 240)),
          managed_census_denials:lines.filter(line =>
            line.includes("MANAGED_AGGREGATE_CENSUS_DENIAL_V1:")).slice(-5).map(line => line.slice(0, 500)),
          t10_01_maintenance_shutdown:lines.filter(line =>
            line.startsWith("T10_01_OWNER_SHUTDOWN_V1:")).slice(-20),
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
        // T09.8 may refresh authenticated RiskBook source_as_of_ms AFTER
        // the terminal close. A newer observation is not a money mutation.
        // Preserve full campaign/loss/entry equality and time monotonicity.
        assertBudgetEconomicsUnchanged(terminal.worker_budget,afterNative.budget,
          "T09.6 persisted Worker budget versus Native terminal close/release");
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
        assert.equal(admitted.create_posts_total,
          process.env.OKX_T10_14_NATIVE_PROBE === "1" ? 6
          : process.env.OKX_B1_SECOND_POSITIVE_PROBE === "1" ? 5 : 4);
        assert.equal(admitted.quote_entry_usd, "3.0015");
        assert.equal(admitted.preview_reserved_entry_usd, managedAdd ? "0" : "4",
          "clean managed-Add acceptance cohort excludes T09.0 component reservations");
        // The signed B late fill transfers 0.4002 from HELD to consumed:
        // 0.04 contracts × 0.01 BTC × 1000 USD + 0.0002 signed fee.
        // The campaign limit/remaining does not change on Cancel ACK.
        const t10LateFillMode=process.env.OKX_T10_09_LATE_FILL_PROBE === "1";
        if (t10LateFillMode) {
          assert.equal(process.env.OKX_B1_NATIVE_CAP_PROBE,"1",
            "T10-09 must use the same real A/B Native campaign");
          assert.equal(process.env.OKX_B1_SECOND_POSITIVE_PROBE,"1",
            "T10-09 must source B's accepted Native SQL intent");
        }
        const expectedReserved=managedAdd?"0":partialFills?"4":
          t10LateFillMode?"7.6018":
          process.env.OKX_T10_04_FOREIGN_WORKER_REPLY_PROBE === "1"?"8.002":
          t10PostReplyLostMode?"8.002":
          t10SourceRecoveryMode?"8.002":
          specMode?"8.002":
          t10PostOutcomeMode?(
            process.env.OKX_T10_05_LOST_ACK_PROBE==="1"?"8.002":
            afterNative.budget?.entry_budget?.reserved_usd
          ):
          t10SourceChangedMode?(afterNative.budget?.entry_budget?.reserved_usd):
          process.env.OKX_B1_SECOND_POSITIVE_PROBE === "1"?"8.002":"7.0015";
        const expectedSpent=t10LateFillMode?"1.4007":
          managedAdd?"6.003":partialFills?"4.002":"1.0005";
        if(afterNative.budget?.entry_budget?.reserved_usd!==expectedReserved
          ||afterNative.budget?.entry_budget?.consumed_usd!==expectedSpent){
          // Only numeric Worker/Native receipts. Never dump WSS frames,
          // signed source bodies or account credentials on assertion failure.
          const nativePartial=partialFills
            ?receipt(stdout,"T09_3_NATIVE_PARTIAL_TERMINAL_V1"):null;
          process.stderr.write("T09_2_POST_NATIVE_BUDGET_MISMATCH_V1:"+JSON.stringify({
            expected_reserved_usd:expectedReserved,
            actual_reserved_usd:afterNative.budget?.entry_budget?.reserved_usd??null,
            expected_spent_usd:expectedSpent,
            actual_spent_usd:afterNative.budget?.entry_budget?.consumed_usd??null,
            worker_risk_book_sequence:afterNative.risk_book_sequence,
            native_risk_book_sequence:nativePartial?.risk_book_sequence??null,
            native_worker_budget:nativePartial?.worker_budget??admitted.after_worker_budget,
            native_read_counts:nativePartial?.phases?.map(p=>p.source_read_counts)??null
          })+"\n");
        }
        if (t10PostOutcomeMode && process.env.OKX_T10_05_EXPLICIT_REJECT_PROBE==="1") {
          assert.ok(["7.0015","8.002"].includes(expectedReserved),
            "a signed venue reject may release with proof or retain unresolved risk");
        }
        if (t10SourceChangedMode) {
          // Only two legal exact central states after this no-send fault:
          // the original HELD remains, or it was released using verified
          // Native REJECTED_UNSENT proof. Never infer a financial reset from
          // an absent response or a process timeout.
          assert.ok(["7.0015","8.002"].includes(expectedReserved),
            "T10-15 central reserve must be either retained or source-proven released");
        }
        assert.equal(afterNative.budget?.entry_budget?.reserved_usd,expectedReserved);
        assert.equal(afterNative.budget?.entry_budget?.consumed_usd,expectedSpent);
        // Select the latest source-backed checkpoint, never T09.4 once
        // the two managed Close releases have happened. A follow-up T09.8
        // can advance source time, but every economic field must match.
        const latestNativeBudget =
          managedClose ? receipt(stdout,"T09_6_NATIVE_TERMINAL_V1").worker_budget
          : managedAdd ? receipt(stdout,"T09_4_NATIVE_MANAGED_ADD_V1").worker_budget
          : partialFills ? receipt(stdout,"T09_3_NATIVE_PARTIAL_TERMINAL_V1").worker_budget
          : admitted.after_worker_budget;
        if (managedClose) {
          assertBudgetEconomicsUnchanged(latestNativeBudget,afterNative.budget,
            "T09.2/T09.6 persisted Worker versus latest Native terminal budget");
        } else if (process.env.OKX_B1_NATIVE_CAP_PROBE === "1") {
          // B's independently authenticated public Execute is rejected by
          // the shared cap but may advance the COMPLETE account census.
          // Preserve every economic field while allowing ONLY a newer
          // source_as_of_ms. Ordinary T09.2 retains exact object equality.
          const b1 = receipt(stdout,"B1_NATIVE_SHARED_CAP_DENIAL_V1");
          assert.equal(b1.denial_reason,"CAMPAIGN_ENTRY_BUDGET_EXHAUSTED");
          assert.equal(b1.outbound_posts_before,b1.outbound_posts_after);
          assertBudgetEconomicsUnchanged(b1.before_budget,b1.after_budget,
            "B1 rejected second account cannot change shared campaign money");
          if (process.env.OKX_B1_SECOND_POSITIVE_PROBE === "1") {
            const acceptedB = receipt(stdout,"B1_NATIVE_SECOND_ACCOUNT_ACCEPTED_V1");
            assert.equal(acceptedB.b_sql_state,"ACKNOWLEDGED");
            assert.equal(acceptedB.b_sql_reserve_held,true);
            assert.equal(acceptedB.outbound_posts_after,5);
            assert.equal(acceptedB.after_budget.entry_budget.reserved_usd,"8.002");
            if (t10LateFillMode) {
              const late=receipt(stdout,"T10_09_NATIVE_LATE_B_FILL_V1");
              assert.equal(late.schema,"okx.t10-09-late-signed-b-fill/v1");
              assert.equal(late.after_cancel_ack,true);
              assert.equal(late.signed_fill_and_bill_ingested_by_native_sql,true);
              assert.equal(late.sql.quantity,"0.04");
              assert.equal(late.sql.fill_count,1);
              assert.equal(late.sql.bill_count,1);
              assert.equal(late.sql.verified_fee_usdt,"0.0002");
              assert.equal(late.sql.active_central_receipt,true);
              assert.equal(late.posts_before,6);
              assert.equal(late.posts_after,6);
              const before=acceptedB.after_budget.entry_budget;
              const after=afterNative.budget.entry_budget;
              assert.equal(before.consumed_usd,"1.0005");
              assert.equal(before.reserved_usd,"8.002");
              assert.equal(after.consumed_usd,"1.4007");
              assert.equal(after.reserved_usd,"7.6018");
              assert.equal(after.remaining_usd,before.remaining_usd,
                "Cancel ACK plus late fill cannot replenish campaign capacity");
              assert.equal(after.limit_usd,before.limit_usd);
              assertBudgetEconomicsUnchanged(late.worker_budget,afterNative.budget,
                "T10-09 signed B fill must match persisted Worker money");
            } else {
              assertBudgetEconomicsUnchanged(acceptedB.after_budget,afterNative.budget,
                "B1 A+B public accepted Native money equals persisted Worker");
            }
          } else {
            assertBudgetEconomicsUnchanged(latestNativeBudget,afterNative.budget,
              "B1 resumed Worker must match Native money after source refresh");
          }
        } else if (t10SourceRecoveryMode) {
          const native=receipt(stdout,"T10_01_NATIVE_SOURCE_RECOVERY_V1");
          assert.equal(native.posts_before,4);
          assert.equal(native.posts_on_incomplete,4);
          assert.equal(native.posts_after_recovery,5);
          assert.equal(native.fresh_sql_state,"ACKNOWLEDGED");
          assert.equal(afterNative.budget.entry_budget.reserved_usd,"8.002");
          assert.equal(afterNative.budget.entry_budget.consumed_usd,"1.0005");
          assertBudgetEconomicsUnchanged(native.worker_budget_after,afterNative.budget,
            "T10-01 valid recovery must persist exactly one independent B reserve");
          console.log("T10_01_WORKER_SOURCE_RECOVERY_V1:"+JSON.stringify({
            schema:"okx.t10-01-worker-source-recovery/v1",
            budget:afterNative.budget,
            risk_book_sequence:afterNative.risk_book_sequence,
            no_post_on_incomplete:native.posts_on_incomplete===native.posts_before,
            one_post_on_recovered:native.posts_after_recovery-native.posts_before===1,
          }));
        } else if (t10PostReplyLostMode) {
          const native=receipt(stdout,"T10_03_NATIVE_POST_AUTH_REPLY_LOST_V1");
          assert.equal(native.lost_post_authorization,true);
          assert.equal(native.central_state,"POSTED");
          assert.equal(native.posts_before,4);
          assert.equal(native.posts_after,4);
          assert.equal(native.active_sql_receipt,true);
          assert.equal(afterNative.budget.entry_budget.consumed_usd,"1.0005");
          assert.equal(afterNative.budget.entry_budget.reserved_usd,"8.002",
            "a committed POSTED risk must not be released as unsent on lost response");
          assertBudgetEconomicsUnchanged(native.worker_budget,afterNative.budget,
            "T10-03 lost post fence cannot change money on worker reconnect");
          console.log("T10_03_WORKER_POST_AUTH_RECONCILED_V1:"+JSON.stringify({
            schema:"okx.t10-03-post-auth-lost-worker/v1",
            after_budget:afterNative.budget,
            after_sequence:afterNative.risk_book_sequence,
            central_state:native.central_state,
            new_exchange_posts:native.posts_after-native.posts_before,
          }));
        } else if (t10SourceChangedMode) {
          const witnessed=receipt(stdout,"T10_15_NATIVE_CAUSAL_SOURCE_V1");
          const sameNativeCampaign=process.env.OKX_T10_07_NATIVE_PROBE==="1";
          const expectedPosts=sameNativeCampaign?5:4;
          const expectedQuote=sameNativeCampaign?"0.5003":"1.0006";
          const expectedHeld=sameNativeCampaign?"8.002":"7.0015";
          const expectedRemaining=sameNativeCampaign?"0.9975":"1.998";
          assert.equal(witnessed.same_campaign_as_native_b1,sameNativeCampaign);
          assert.equal(witnessed.release_state,"RELEASED");
          assert.equal(witnessed.worker_release_verified,true);
          assert.equal(witnessed.sql_unsent_receipt_verified,true);
          assert.equal(witnessed.posts_before,expectedPosts);
          assert.equal(witnessed.posts_after,expectedPosts);
          assert.equal(witnessed.new_quote_usd,expectedQuote);
          assert.equal(afterNative.budget.entry_budget.consumed_usd,"1.0005");
          assert.equal(afterNative.budget.entry_budget.reserved_usd,expectedHeld,
            "source-proven Native release must restore pre-reserve headroom");
          assert.equal(afterNative.budget.entry_budget.remaining_usd,expectedRemaining);
          console.log("T10_15_WORKER_AFTER_NATIVE_V1:"+JSON.stringify({
            schema:"okx.t10-15-worker-source-change/v1",
            budget:afterNative.budget,
            risk_book_sequence:afterNative.risk_book_sequence,
            posts_for_changed_plan:0,
          }));
        } else if (t10PostOutcomeMode) {
          const witness=receipt(stdout,"T10_05_NATIVE_POST_OUTCOME_V1");
          assert.equal(witness.posts_before,4);
          assert.equal(witness.posts_after,5);
          assert.ok(
            (witness.mode==="lost_ack" && afterNative.budget.entry_budget.reserved_usd==="8.002")
            ||(witness.mode==="venue_rejected"
              &&["7.0015","8.002"].includes(afterNative.budget.entry_budget.reserved_usd)),
            "UNKNOWN cannot release risk, explicit venue rejection needs independent terminal proof"
          );
          assert.equal(afterNative.budget.entry_budget.consumed_usd,"1.0005");
          assertBudgetEconomicsUnchanged(witness.worker_budget_after,afterNative.budget,
            "T10-05 post-send ACK outcome cannot double-debit or silently release");
          console.log("T10_05_WORKER_AFTER_NATIVE_V1:"+JSON.stringify({
            schema:"okx.t10-05-worker-post-outcome/v1",
            mode:witness.mode,budget:afterNative.budget,
            risk_book_sequence:afterNative.risk_book_sequence,
            no_extra_posts:witness.posts_after-witness.posts_before===1,
          }));
        } else if (specMode) {
          const spec=receipt(stdout,"T09_SPEC_NATIVE_SECOND_LINEAR_V1");
          assert.equal(spec.intent_state,"ACKNOWLEDGED");
          assert.equal(spec.sql_risk_held,true);
          assert.equal(spec.posts_after-spec.posts_before,1);
          assert.equal(afterNative.budget.entry_budget.reserved_usd,"8.002");
          assert.equal(afterNative.budget.entry_budget.consumed_usd,"1.0005");
          console.log("T09_SPEC_WORKER_SECOND_LINEAR_V1:"+JSON.stringify({
            schema:"okx.t09-spec-worker-risk/v1",
            budget:afterNative.budget,
            risk_book_sequence:afterNative.risk_book_sequence,
            account_b:spec.account_uid,
            venue_new_posts:spec.posts_after-spec.posts_before,
          }));
        } else if (t10LateFillMode) {
          throw Error("T10_09_UNSCOPED_NATIVE_B_CAMPAIGN");
        } else if (process.env.OKX_T10_04_FOREIGN_WORKER_REPLY_PROBE === "1") {
          const blocked=receipt(stdout,"T10_04_NATIVE_FOREIGN_WORKER_REPLY_V1");
          assert.equal(blocked.posts_before,4);
          assert.equal(blocked.posts_after,4);
          assert.equal(blocked.worker_budget_before.entry_budget.reserved_usd,"7.0015");
          assert.equal(afterNative.budget.entry_budget.reserved_usd,"8.002",
            "real Worker HELD survives corrupt delivery, Native did not POST");
          assert.equal(afterNative.budget.entry_budget.consumed_usd,"1.0005");
          // Compare central monetary reservations as exact fixed-scale
          // integers. A mismatched Native reply cannot release previously
          // held risk or hide the new authoritative Worker HELD.
          const riskUnits = (value) => {
            assert.match(value,/^(0|[1-9][0-9]*)(?:\.[0-9]{1,9})?$/);
            const [whole,fraction=""] = value.split(".");
            return BigInt(whole)*1_000_000_000n+
              BigInt(fraction.padEnd(9,"0"));
          };
          assert.ok(
            riskUnits(afterNative.budget.reserved_loss_usd) >=
              riskUnits(blocked.worker_budget_before.reserved_loss_usd),
            "Native must not report a fabricated release after foreign Worker command"
          );
          console.log("T10_04_WORKER_FOREIGN_REPLY_V1:"+JSON.stringify({
            schema:"okx.t10-04-worker-post-commit-binding/v1",
            committed_worker_budget:afterNative.budget,
            committed_worker_sequence:afterNative.risk_book_sequence,
            native_sql_state:blocked.sql_state,
            posts_for_denied_command:blocked.posts_after-blocked.posts_before,
            original_source_before:blocked.worker_budget_before,
            transport_error_expected:"RISK_RESULT_COMMAND_UNBOUND",
          }));
        } else if (process.env.OKX_T10_02_EXTERNAL_EXPOSURE_PROBE === "1") {
          // T10-02's negative public Execute deliberately observes a newer
          // external position. That can advance signed source timestamps
          // without changing the one authoritative amount of held/spent risk.
          assertBudgetEconomicsUnchanged(latestNativeBudget,afterNative.budget,
            "T10-02 foreign exposure must not mutate accepted managed money");
        } else {
          assert.deepEqual(afterNative.budget,latestNativeBudget,
            "T09.2 persisted Worker must equal Native pre-close budget");
        }
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
      assertBudgetEconomicsUnchanged(terminalBudget,persisted.budget,
        "T09.7 persisted Worker restart must preserve settled campaign money");
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
    // The isolated shared-cap probe performs no BEGIN_POST and intentionally
    // retains both HELD reservations. Do not certify a Native/venue lifecycle.
    authentic_begin_post:!sharedCapMode,
    reserved_loss_usd:sharedCapMode ? "2" : "1",
    reserved_entry_usd:sharedCapMode ? "8" : "2",
    risk_monetary_post_count:0,
  }));
} finally {
  if (first) await first.dispose();
  if (second) await second.dispose();
  await rm(root, {recursive: true, force: true});
}
}
await runRuntimeFixture();
