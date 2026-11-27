import {RuntimeSession} from "./runtime_session.js";
import {defaultHandler} from "./auth.js";
import {CONTROL_ATTEST_FRAME_SCHEMA, FRAME_SCHEMA} from "./shared.js";

function check(condition: boolean, message: string): void {
  if (!condition) throw new Error(message);
}

const runtimeHash = "e".repeat(64);
const researchHash = "d".repeat(64);
const controlHash = "c".repeat(64);
const workerSha = "b".repeat(64);
const controlStatus = () => ({
  schema: "okx.host-control.research-runtime/v1", desired_running: true,
  process_generation: 7, pid: 332, active_worker_sha256: workerSha,
  installed_worker_sha256: workerSha, phase: "RUNNING_UNVERIFIED",
  catalog_mode: "HOST_INDEPENDENT", retry_in_ms: null, last_error: null,
});
type Fault = "none" | "wrong_nonce" | "wrong_research_session" | "pid" | "sha" |
  "extra_status" | "extra_frame" | "future" | "expired" | "generation_changed" |
  "invalid_process_generation" | "not_running" | "drop" | "close" | "rotate_inflight";

async function run(fault: Fault = "none") {
  let attestorAttachment: any = {role: "control-attestor", admittedCredentialFingerprint: controlHash,
    sessionId: "control_session_1", generation: 51, hello: false, connectedAtMs: Date.now()};
  const researchAttachment: any = {runtimeId: "windows-primary", runtimeProfile: "research",
    sessionMode: "research_only", credentialOwner: "research", sessionId: "research_session_4",
    generation: 4, hello: true, agentPid: 332, agentBinarySha256: workerSha,
    connectedAtMs: Date.now(), lastPongAtMs: Date.now()};
  let runtime: RuntimeSession;
  let challenge: any = null;
  let lastResponse: any = null;
  const nativeFrames: any[] = [];
  const researchWs: any = {
    deserializeAttachment: () => ({...researchAttachment}),
    serializeAttachment: (attachment: any) => Object.assign(researchAttachment, attachment),
    close: () => {throw Error("attestor closed Research socket");},
    send: (raw: string) => {
      const frame = JSON.parse(raw); nativeFrames.push(frame);
      if (frame.type !== "ping") throw Error("attestor sent a non-ping to Research");
      queueMicrotask(() => void runtime.webSocketMessage(researchWs, JSON.stringify({
        schema: FRAME_SCHEMA, type: "pong", nonce: frame.nonce,
        session_id: researchAttachment.sessionId, connection_generation: researchAttachment.generation,
      })));
    },
  };
  let controlClosed: string | null = null;
  const controlFrames: any[] = [];
  const controlWs: any = {
    deserializeAttachment: () => ({...attestorAttachment}),
    serializeAttachment: (attachment: any) => {attestorAttachment = {...attachment};},
    close: (_code: number, reason: string) => {controlClosed = reason;},
    send: (raw: string) => {
      const frame = JSON.parse(raw); controlFrames.push(frame);
      if (frame.type !== "observe_research") return;
      challenge = frame;
      if (fault === "drop") return;
      if (fault === "close") {
        queueMicrotask(() => void runtime.webSocketClose(controlWs)); return;
      }
      const status: any = controlStatus();
      if (fault === "pid") status.pid = 999;
      if (fault === "sha") status.installed_worker_sha256 = "a".repeat(64);
      if (fault === "extra_status") status.unknown = "injected";
      if (fault === "invalid_process_generation") status.process_generation = -1;
      if (fault === "not_running") status.phase = "STOPPED";
      const response: any = {schema: CONTROL_ATTEST_FRAME_SCHEMA, type: "research_observation",
        request_id: frame.request_id, nonce: fault === "wrong_nonce" ? "bogus" : frame.nonce,
        session_id: frame.session_id, connection_generation: frame.connection_generation,
        research_session_id: fault === "wrong_research_session" ? "foreign" : frame.research_session_id,
        research_connection_generation: frame.research_connection_generation,
        observed_at_ms: fault === "future" ? Date.now() + 60000 :
          fault === "expired" ? Date.now() - 60000 : Date.now(), status};
      if (fault === "extra_frame") response.proof = "none";
      lastResponse = response;
      queueMicrotask(() => {
        if (fault === "generation_changed") researchAttachment.generation++;
        // The response was created under the old credential, but Edge's
        // binding changes before the old CONTROL socket delivers it.
        if (fault === "rotate_inflight") env.CONTROL_ATTEST_TOKEN_SHA256 = "a".repeat(64);
        void runtime.webSocketMessage(controlWs, JSON.stringify(response));
      });
    },
  };
  const state: any = {
    storage: {get: async () => {throw Error("attestation read finance/storage");}},
    getWebSockets: (tag: string) => tag === "runtime" ? [researchWs] :
      tag === "attestor" ? [controlWs] : (() => {throw Error("unknown DO tag");})(),
  };
  const env: any = {CONTROL_ATTEST_TOKEN_SHA256: controlHash,
    RUNTIME_TOKEN_SHA256: runtimeHash, RESEARCH_RUNTIME_TOKEN_SHA256: researchHash};
  runtime = new RuntimeSession(state, env);
  await runtime.webSocketMessage(controlWs, JSON.stringify({schema: CONTROL_ATTEST_FRAME_SCHEMA,
    type: "hello_control", connection_id: "control_0123456789abcdef"}));
  check(attestorAttachment.hello && controlFrames[0]?.type === "hello_ack" &&
    controlFrames[0].session_id === attestorAttachment.sessionId &&
    controlFrames[0].connection_generation === 51, "exact CONTROL hello/ack handshake");
  const observed: any = await (await runtime.fetch(new Request("https://internal/attestation/research"))).json();
  check(challenge?.type === "observe_research" && challenge.deadline_ms > Date.now() - 5000 &&
    challenge.deadline_ms <= Date.now() + 3000 && challenge.nonce.startsWith("nonce_") &&
    challenge.research_session_id === "research_session_4" &&
    challenge.research_connection_generation === 4 &&
    nativeFrames.length === 1 && nativeFrames[0].type === "ping", "bounded read-only live challenge");
  check((runtime as any).attestorPending.size === 0 &&
    (runtime as any).pending.size === 0 &&
    (runtime as any).admissions.engine.inflight === 0 &&
    (runtime as any).admissions.research.inflight === 0, "attestor released its own and runtime request slots");
  return {observed, challenge, lastResponse, runtime, controlWs, researchWs,
    getControlClosed: () => controlClosed, env};
}

// Forged first hello frames must not establish an authenticated CONTROL
// attestor generation, emit hello_ack, or touch runtime/finance state.
for (const [caseName, forgedHello] of [
  ["short id", {schema: CONTROL_ATTEST_FRAME_SCHEMA, type: "hello_control", connection_id: "short"}],
  ["owner claim", {schema: CONTROL_ATTEST_FRAME_SCHEMA, type: "hello_control",
    connection_id: "control_0123456789abcdef", owner: "engine"}],
  ["foreign schema", {schema: FRAME_SCHEMA, type: "hello_control",
    connection_id: "control_0123456789abcdef"}],
  ["foreign type", {schema: CONTROL_ATTEST_FRAME_SCHEMA, type: "hello_research",
    connection_id: "control_0123456789abcdef"}],
] as const) {
  let attachment: any = {role: "control-attestor", admittedCredentialFingerprint: controlHash,
    sessionId: "control_session_forged", generation: 90, hello: false, connectedAtMs: Date.now()};
  const sent: string[] = [];
  const closed: Array<{code: number; reason: string}> = [];
  const socket: any = {
    deserializeAttachment: () => ({...attachment}),
    serializeAttachment: (value: any) => {attachment = {...value};},
    send: (raw: string) => {sent.push(raw);},
    close: (code: number, reason: string) => {closed.push({code, reason});},
  };
  const state: any = {
    storage: {get: async () => {throw Error("forged CONTROL hello touched DO storage");}},
    getWebSockets: (tag: string) => tag === "attestor" ? [socket] : [],
  };
  const runtime = new RuntimeSession(state, {
    CONTROL_ATTEST_TOKEN_SHA256: controlHash, RUNTIME_TOKEN_SHA256: runtimeHash,
    RESEARCH_RUNTIME_TOKEN_SHA256: researchHash,
  } as any);
  await runtime.webSocketMessage(socket, JSON.stringify(forgedHello));
  check(closed.length === 1 && closed[0].code === 1008 && sent.length === 0 &&
    attachment.hello === false, `forged initial CONTROL hello rejected: ${caseName}`);
}

const ok = await run();
check(ok.observed.status === "PASS" && ok.observed.reason_code === null &&
  ok.observed.pid === 332 && ok.observed.active_worker_sha256 === workerSha &&
  ok.observed.installed_worker_sha256 === workerSha && ok.observed.process_generation === 7 &&
  ok.observed.research_connection_generation === 4, "bound PID/SHA/process generation passes");

for (const fault of ["wrong_nonce", "wrong_research_session", "pid", "sha",
  "extra_status", "extra_frame", "future", "expired", "generation_changed",
  "invalid_process_generation", "not_running", "close", "drop"] as Fault[]) {
  const fixture = await run(fault);
  check(fixture.observed.status === "UNVERIFIED" && !!fixture.observed.reason_code &&
    fixture.observed.pid === null && fixture.observed.process_generation === null,
    `${fault}: malformed/stale CONTROL observation must not be promoted`);
}

// A late answer after an in-place credential rotation must be discarded,
// settle the active challenge with an explicit revoked reason, and close the
// old CONTROL WSS. A new attestation must never reuse a prior PASS.
{
  const revoked = await run("rotate_inflight");
  check(revoked.observed.status === "UNVERIFIED" &&
    revoked.observed.reason_code === "CONTROL_ATTEST_CREDENTIAL_REVOKED" &&
    revoked.observed.pid === null && revoked.observed.process_generation === null,
    "in-flight credential rotation must report revoked rather than stale socket");
  check(revoked.getControlClosed() === "attestor credential revoked" &&
    (revoked.runtime as any).attestorPending.size === 0,
    "old CONTROL WSS closed and in-flight challenge cleaned after revocation");
  const stillRevoked: any = await (await revoked.runtime.fetch(new Request(
    "https://internal/attestation/research"))).json();
  check(stillRevoked.status === "UNVERIFIED" &&
    stillRevoked.reason_code === "CONTROL_ATTEST_OFFLINE" &&
    stillRevoked.pid === null,
    "revoked generation cannot cache or replay a prior PASS");
  check((await run()).observed.status === "PASS",
    "new independently admitted attestor session can still PASS");
}

// Replayed observations have no pending challenge. A replay cannot silently
// resurrect any previously valid verification after the HTTP read completed.
await ok.runtime.webSocketMessage(ok.controlWs, JSON.stringify(ok.lastResponse));
check(ok.getControlClosed() === "unmatched observation" &&
  (ok.runtime as any).attestorPending.size === 0, "settled challenge cannot be replayed");

// A CONTROL close/error must never touch Research and Engine response slots.
for (const event of ["webSocketClose", "webSocketError"] as const) {
  const fixture = await run();
  (fixture.runtime as any).rejectGeneration = () => {throw Error("CONTROL socket rejected native generation");};
  await fixture.runtime[event](fixture.controlWs);
}

// Rotation after admission invalidates an existing attestor without waiting
// for a reconnect; no credential bytes are exposed in any protocol frame.
{
  const fixture = await run();
  fixture.env.CONTROL_ATTEST_TOKEN_SHA256 = "a".repeat(64);
  const rotated: any = await (await fixture.runtime.fetch(new Request("https://internal/attestation/research"))).json();
  check(rotated.status === "UNVERIFIED" && rotated.reason_code === "CONTROL_ATTEST_OFFLINE",
    "rotated bearer must invalidate an old live attestor socket");
  check(!JSON.stringify(fixture.challenge).includes(controlHash), "challenge omits credential fingerprint");
}

// Missing or explicitly invalid configuration remains dormant, even when
// an earlier out-of-band Worker secret used this same binding name.
for (const env of [{}, {CONTROL_ATTEST_TOKEN_SHA256: "disabled"}]) {
  const runtime = new RuntimeSession({getWebSockets: () => {throw Error("disabled attestor looked at sockets");}} as any, env as any);
  const result: any = await (await runtime.fetch(new Request("https://internal/attestation/research"))).json();
  check(result.reason_code === "CONTROL_ATTEST_NOT_CONFIGURED", "invalid or absent credential remains dormant");
}

// Worker must refuse unauthenticated /attestor before constructing a DO stub.
{
  let calls = 0;
  const env: any = {CONTROL_ATTEST_TOKEN_SHA256: controlHash,
    RUNTIME_TOKEN_SHA256: runtimeHash, RESEARCH_RUNTIME_TOKEN_SHA256: researchHash,
    RUNTIME: {idFromName: () => {calls++; return 1;}, get: () => {throw Error("unauthorized DO use");}}};
  for (const headers of [{}, {upgrade: "websocket"},
    {upgrade: "websocket", authorization: "Bearer wrong"}] as Record<string,string>[]) {
    const result = await defaultHandler.fetch(new Request("https://fixture/attestor", {headers}), env);
    check([401,426].includes(result.status), "unauthorized attestor rejected at Worker");
  }
  check(calls === 0, "unauthenticated attestor never bills Durable Object");
  const disabled = await defaultHandler.fetch(new Request("https://fixture/attestor", {
    headers: {upgrade: "websocket", authorization: "Bearer wrong"},
  }), {...env, CONTROL_ATTEST_TOKEN_SHA256: "disabled"});
  check(disabled.status === 503 && calls === 0,
    "explicit disabled sentinel blocks stale Worker secret before Durable Object");
  const collided = await defaultHandler.fetch(new Request("https://fixture/attestor", {
    headers: {upgrade: "websocket", authorization: "Bearer wrong"},
  }), {...env, CONTROL_ATTEST_TOKEN_SHA256: researchHash});
  check(collided.status === 503 && calls === 0, "Research credential cannot double as CONTROL attestor");
}
console.log("control_attestor_read_only_session_binding_and_negative PASS");
