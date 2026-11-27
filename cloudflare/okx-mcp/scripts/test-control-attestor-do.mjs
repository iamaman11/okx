#!/usr/bin/env node
// Actual workerd/SQLite Durable Object with two independently authenticated
// WSS owners, exercised exclusively through Miniflare's local dispatchFetch.
// All bearer values are fresh per process; this never contacts production.
import assert from "node:assert/strict";
import {createHash, randomBytes} from "node:crypto";
import {mkdtemp, readFile, rm} from "node:fs/promises";
import {tmpdir} from "node:os";
import {join} from "node:path";
import {fileURLToPath} from "node:url";
import {build} from "esbuild";
import {Miniflare} from "miniflare";

const FRAME = "okx.direct-transport.frame/v2";
const CONTROL_FRAME = "okx.control-attestor.frame/v1";
const SHA = "b".repeat(64); // Synthetic binary digest, never a workstation value.
const PID = 17001; // Synthetic process identity, never a workstation value.
const bearer = () => randomBytes(32).toString("hex");
const fingerprint = (secret) => createHash("sha256").update(secret).digest("hex");
const tokens = {engine: bearer(), research: bearer(), control: bearer(), rotated: bearer()};
const hashes = Object.fromEntries(Object.entries(tokens).map(([owner, token]) => [owner, fingerprint(token)]));
const root = await mkdtemp(join(tmpdir(), "okx-control-attestor-workerd-"));
const outfile = join(root, "attestor-test.mjs");
const baseUrl = "http://localhost";

function options(bundle, controlHash) {
  return {
    resourcePersistencePath: join(root, "sqlite-do"),
    workers: [{
      config: {
        name: "okx-attestor-hermetic",
        compatibilityDate: "2026-09-30",
        compatibilityFlags: ["global_fetch_strictly_public"],
        manifest: {
          mainModule: "attestor-test.mjs", modulesRoot: root,
          modules: {"attestor-test.mjs": {type: "esm", contents: bundle}},
        },
        env: {
          CONTROL_ATTEST_E2E_LOCAL_ONLY: {type: "text", value: "1"},
          RUNTIME: {
            type: "durable-object", worker: "okx-attestor-hermetic",
            exportName: "RuntimeSession",
          },
          RUNTIME_TOKEN_SHA256: {type: "text", value: hashes.engine},
          RESEARCH_RUNTIME_TOKEN_SHA256: {type: "text", value: hashes.research},
          CONTROL_ATTEST_TOKEN_SHA256: {type: "text", value: controlHash},
        },
        exports: {RuntimeSession: {type: "durable-object", storage: "sqlite"}},
      },
    }],
  };
}

function channel(webSocket) {
  assert.ok(webSocket, "real Worker WSS upgrade must create a WebSocketPair");
  const queued = [];
  const waiters = [];
  let closeCode = null;
  const closeWaiters = [];
  webSocket.addEventListener("message", (event) => {
    const frame = JSON.parse(event.data);
    // A live Research native must answer Edge freshness checks independently
    // from CONTROL. Never fabricate its control/observation response.
    if (frame.type === "ping" && frame.schema === FRAME) {
      webSocket.send(JSON.stringify({...frame, type: "pong"}));
      return;
    }
    const index = waiters.findIndex((waiter) => waiter.match(frame));
    if (index < 0) queued.push(frame);
    else {
      const [waiter] = waiters.splice(index, 1);
      clearTimeout(waiter.timer);
      waiter.resolve(frame);
    }
  });
  webSocket.addEventListener("close", (event) => {
    closeCode = event.code;
    for (const done of closeWaiters.splice(0)) done(closeCode);
  });
  webSocket.accept();
  return {
    send(frame) { webSocket.send(JSON.stringify(frame)); },
    next(match, label) {
      const index = queued.findIndex(match);
      if (index >= 0) return Promise.resolve(queued.splice(index, 1)[0]);
      assert.equal(closeCode, null, `${label}: WSS already closed`);
      return new Promise((resolve, reject) => {
        const waiter = {match, resolve, timer: null};
        waiter.timer = setTimeout(() => {
          const i = waiters.indexOf(waiter);
          if (i >= 0) waiters.splice(i, 1);
          reject(new Error(`LOCAL_WORKER_FRAME_TIMEOUT: ${label}`));
        }, 5000);
        waiters.push(waiter);
      });
    },
    closed(label = "WSS") {
      if (closeCode !== null) return Promise.resolve(closeCode);
      return new Promise((resolve, reject) => {
        const timer = setTimeout(() => reject(Error("LOCAL_WORKER_CLOSE_TIMEOUT: " + label)), 5000);
        closeWaiters.push((code) => {clearTimeout(timer); resolve(code);});
      });
    },
    close() {if (closeCode === null) webSocket.close(1000, "test complete");},
  };
}

async function dispatch(mf, path, init) {
  assert.ok(path.startsWith("/"), "all requests must remain localhost-relative");
  return mf.dispatchFetch(baseUrl + path, init);
}
async function attestation(mf) {
  const response = await dispatch(mf, "/attestation/research");
  assert.equal(response.status, 200);
  return response.json();
}
async function upgrade(mf, path, token) {
  const response = await dispatch(mf, path, {
    headers: {upgrade: "websocket", authorization: "Bearer " + token},
  });
  assert.equal(response.status, 101, `${path} must admit only valid owner bearer`);
  return channel(response.webSocket);
}
async function researchSession(mf) {
  const socket = await upgrade(mf, "/runtime", tokens.research);
  socket.send({
    schema: FRAME, type: "hello_research", runtime_id: "windows-primary",
    connection_id: "native_" + randomBytes(16).toString("hex"),
    contract: "okx.product/v1", host_scope: "a".repeat(64),
    runtime_profile: "research", agent_pid: PID, agent_binary_sha256: SHA,
    agent_incarnation: "ctl_" + randomBytes(16).toString("hex"),
  });
  const ack = await socket.next((f) => f.type === "hello_ack", "Research ACK");
  assert.equal(ack.schema, FRAME);
  assert.ok(ack.session_id.startsWith("session_"));
  assert.ok(ack.connection_generation > 0);
  return {socket, ack};
}
async function controlSession(mf, token = tokens.control) {
  const socket = await upgrade(mf, "/attestor", token);
  socket.send({
    schema: CONTROL_FRAME, type: "hello_control",
    connection_id: "control_" + randomBytes(16).toString("hex"),
  });
  const ack = await socket.next((f) => f.type === "hello_ack", "CONTROL ACK");
  assert.equal(ack.schema, CONTROL_FRAME);
  assert.ok(ack.session_id.startsWith("control_"));
  assert.ok(ack.connection_generation > 0);
  return {socket, ack};
}
function status(overrides = {}) {
  return {
    schema: "okx.host-control.research-runtime/v1", desired_running: true,
    process_generation: 7, pid: PID, active_worker_sha256: SHA,
    installed_worker_sha256: SHA, phase: "RUNNING_UNVERIFIED",
    catalog_mode: "HOST_INDEPENDENT", retry_in_ms: null, last_error: null,
    ...overrides,
  };
}
function observation(challenge, processStatus = status()) {
  return {
    schema: CONTROL_FRAME, type: "research_observation",
    request_id: challenge.request_id, nonce: challenge.nonce,
    session_id: challenge.session_id,
    connection_generation: challenge.connection_generation,
    research_session_id: challenge.research_session_id,
    research_connection_generation: challenge.research_connection_generation,
    observed_at_ms: Date.now(), status: processStatus,
  };
}
async function begin(mf, control) {
  const pending = attestation(mf);
  const challenge = await control.socket.next(
    (f) => f.type === "observe_research", "fresh CONTROL challenge");
  assert.equal(challenge.schema, CONTROL_FRAME);
  assert.ok(challenge.nonce.startsWith("nonce_"));
  assert.ok(challenge.deadline_ms > Date.now() - 1000);
  return {pending, challenge};
}
async function verify(mf, control, processStatus = status()) {
  const {pending, challenge} = await begin(mf, control);
  const reply = observation(challenge, processStatus);
  control.socket.send(reply);
  return {result: await pending, challenge, reply};
}
function failClosed(result, reason) {
  assert.equal(result.status, "UNVERIFIED", reason);
  assert.equal(result.reason_code, reason);
  assert.equal(result.pid, null);
  assert.equal(result.active_worker_sha256, null);
  assert.equal(result.installed_worker_sha256, null);
  assert.equal(result.process_generation, null);
}

let mf;
try {
  const built = await build({
    entryPoints: [fileURLToPath(new URL("../src/integration/control_attestor_entry.ts", import.meta.url))],
    outfile, bundle: true, write: true, format: "esm", platform: "browser",
    target: "es2022", metafile: true, logLevel: "warning",
  });
  assert.ok(Object.keys(built.metafile.inputs).some((path) => path.endsWith("src/auth.ts")),
    "test entry must include actual production Worker authorization");
  assert.ok(Object.keys(built.metafile.inputs).some((path) => path.endsWith("src/runtime_session.ts")),
    "test entry must include actual production Durable Object class");
  assert.ok(!Object.keys(built.metafile.inputs).some((path) => path.endsWith("src/index.ts")),
    "hermetic test entry must not bundle shipped OAuth Worker entry");
  const bundle = await readFile(outfile, "utf8");
  mf = new Miniflare(options(bundle, hashes.control));

  // The public Worker prefilter rejects wrong owner before WSS admission.
  for (const path of ["/attestor", "/runtime"]) {
    const owner = path === "/attestor" ? tokens.control : tokens.research;
    for (const invalid of [undefined, tokens.engine, path === "/attestor" ? tokens.research : tokens.control]) {
      const response = await dispatch(mf, path, {headers: {
        upgrade: "websocket", ...(invalid ? {authorization: "Bearer " + invalid} : {}),
      }});
      if (path === "/runtime" && invalid === tokens.engine) {
        assert.equal(response.status, 101, "Engine WSS bearer is independent");
        response.webSocket.accept();
        response.webSocket.close(1000, "no Engine hello");
      } else {
        assert.equal(response.status, 401, "cross-owner/missing bearer is denied");
      }
    }
    const plain = await dispatch(mf, path, {headers: {authorization: "Bearer " + owner}});
    assert.equal(plain.status, 426, "non-upgrade request denied");
  }
  assert.equal((await dispatch(mf, "/dispatch")).status, 404,
    "test Worker exposes no finance/mutation endpoint");

  let research = await researchSession(mf);
  failClosed(await attestation(mf), "CONTROL_ATTEST_OFFLINE");

  // A forged initial hello is denied by the real DO, not just the in-memory mock.
  const forged = await upgrade(mf, "/attestor", tokens.control);
  const forgedClosed = forged.closed("forged hello");
  forged.send({schema: CONTROL_FRAME, type: "hello_control",
    connection_id: "invalid", owner: "research"});
  assert.equal(await forgedClosed, 1008);
  failClosed(await attestation(mf), "CONTROL_ATTEST_OFFLINE");

  let control = await controlSession(mf);
  const first = await verify(mf, control);
  assert.equal(first.result.status, "PASS");
  assert.equal(first.result.reason_code, null);
  assert.equal(first.result.pid, PID);
  assert.equal(first.result.active_worker_sha256, SHA);
  assert.equal(first.result.installed_worker_sha256, SHA);
  assert.equal(first.result.process_generation, 7);
  assert.equal(first.result.research_connection_generation, research.ack.connection_generation);

  for (const [name, changed] of [
    ["PID", {pid: PID + 1}],
    ["installed SHA", {installed_worker_sha256: "c".repeat(64)}],
    ["not running", {phase: "STOPPED"}],
  ]) {
    const failed = await verify(mf, control, status(changed));
    failClosed(failed.result, "CONTROL_RESEARCH_PID_SHA_MISMATCH");
    assert.equal(failed.result.status, "UNVERIFIED", name);
  }

  // A completed challenge cannot be replayed, even after a previous PASS.
  const replayClosed = control.socket.closed("replay");
  control.socket.send(first.reply);
  assert.equal(await replayClosed, 1008);
  failClosed(await attestation(mf), "CONTROL_ATTEST_OFFLINE");
  control = await controlSession(mf);

  // Research generation change during an in-flight challenge must invalidate
  // the observation, even when the old PID/SHA happen to be identical.
  const changedResearch = await begin(mf, control);
  const oldResearchClosed = research.socket.closed("Research reconnect");
  const nextResearch = await researchSession(mf);
  assert.equal(await oldResearchClosed, 1012);
  control.socket.send(observation(changedResearch.challenge));
  failClosed(await changedResearch.pending, "RESEARCH_EDGE_GENERATION_CHANGED");
  research = nextResearch;
  assert.equal((await verify(mf, control)).result.status, "PASS",
    "fresh Research generation can pass only a new challenge");

  // A CONTROL reconnect evicts only CONTROL, settles the old challenge, and
  // leaves the independent Research native socket available for a fresh PASS.
  const oldControl = control;
  const changedControl = await begin(mf, oldControl);
  const oldControlClosed = oldControl.socket.closed("CONTROL reconnect");
  control = await controlSession(mf);
  assert.equal(await oldControlClosed, 1012);
  failClosed(await changedControl.pending, "CONTROL_ATTEST_SOCKET_STALE");
  assert.equal((await verify(mf, control)).result.status, "PASS");

  // Bounded read-only admission is fail-closed under a stalled CONTROL owner.
  const pending = await Promise.all([begin(mf, control), begin(mf, control), begin(mf, control)]);
  const busy = await attestation(mf);
  failClosed(busy, "CONTROL_ATTEST_BUSY");
  const disconnected = control.socket.closed("invalid CONTROL frame under load");
  control.socket.send({schema: CONTROL_FRAME, type: "unexpected"});
  assert.equal(await disconnected, 1008);
  for (const waiting of pending)
    failClosed(await waiting.pending, "CONTROL_ATTEST_FRAME_INVALID");
  failClosed(await attestation(mf), "CONTROL_ATTEST_OFFLINE");
  control = await controlSession(mf);
  assert.equal((await verify(mf, control)).result.status, "PASS");

  // A silent CONTROL socket expires in the real DO's 2.5-second deadline.
  const timeout = await begin(mf, control);
  failClosed(await timeout.pending, "CONTROL_ATTEST_TIMEOUT");
  assert.equal((await verify(mf, control)).result.status, "PASS",
    "timeout must not persist a positive or poison a later fresh challenge");
  const priorControlGeneration = control.ack.connection_generation;
  const priorResearchGeneration = research.ack.connection_generation;

  // Cold restart with the same SQLite DO but disabled CONTROL fingerprint:
  // previously admitted sockets/pending challenges cannot grant authority.
  await mf.dispose(); mf = null;
  mf = new Miniflare(options(bundle, "disabled"));
  const revoked = await dispatch(mf, "/attestor", {
    headers: {upgrade: "websocket", authorization: "Bearer " + tokens.control},
  });
  assert.equal(revoked.status, 503);
  failClosed(await attestation(mf), "CONTROL_ATTEST_NOT_CONFIGURED");
  research = await researchSession(mf);
  assert.ok(research.ack.connection_generation > priorResearchGeneration,
    "SQLite Research generation persisted across actual workerd restart");
  failClosed(await attestation(mf), "CONTROL_ATTEST_NOT_CONFIGURED");

  // Cold rotation is a separate sealed test binding. The previous bearer
  // is rejected and only a newly generated independent bearer is admitted.
  await mf.dispose(); mf = null;
  mf = new Miniflare(options(bundle, hashes.rotated));
  const stale = await dispatch(mf, "/attestor", {
    headers: {upgrade: "websocket", authorization: "Bearer " + tokens.control},
  });
  assert.equal(stale.status, 401);
  research = await researchSession(mf);
  failClosed(await attestation(mf), "CONTROL_ATTEST_OFFLINE");
  control = await controlSession(mf, tokens.rotated);
  assert.ok(control.ack.connection_generation > priorControlGeneration,
    "SQLite CONTROL generation persisted across workerd restarts");
  assert.equal((await verify(mf, control)).result.status, "PASS",
    "rotated owner needs a new handshake and challenge");
  control.socket.close();
  research.socket.close();
  console.log("actual_workerd_control_attestor_negative PASS auth, separate WSS, real DO/SQLite, forged hello, PID/SHA/mode, replay, reconnects, busy, offline, timeout, cold revoke/rotate/restart; physical negatives still pending");
} finally {
  if (mf) await mf.dispose();
  await rm(root, {recursive: true, force: true});
}
