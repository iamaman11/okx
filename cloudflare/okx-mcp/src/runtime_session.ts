import { executionProfilePermitsOperation, isExecutionMutation, isResearchNativeOperation } from "./execution_tools.js";
import { initializeGovernance, setAccountLimits, governanceRequest, lowerCampaignLimit, enrollObservedAccount, stopNewRealRisk, setAccountPause, GOVERNANCE_STORAGE_KEY } from "./account_governance_store.js";
import {validGovernance,accessEnabled,validLimits} from "./account_governance.js";
import { handleNativeRiskCommand } from "./risk_authority.js";
import {
  ACK_DEADLINE_MS,
  CONTROL_ATTEST_DEADLINE_MS,
  CONTROL_ATTEST_FRAME_SCHEMA,
  type ControlAttestorAttachment,
  type Env,
  FRAME_SCHEMA,
  type Json,
  isObject,
  MAX_BODY_BYTES,
  MAX_INFLIGHT,
  MAX_READ_INFLIGHT,
  MAX_READ_WAITERS,
  READ_QUEUE_WAIT_MS,
  type Pending,
  PONG_DEADLINE_MS,
  RESPONSE_DEADLINE_MS,
  RUNTIME_NAME,
  decodeRuntimeProfile,
  sameHex,
  sha256Hex,
  type SocketAttachment,
  transportFailure,
} from "./shared.js";

// The scope is an authenticated host's namespace claim, not proof of a key.
// Classify ownership separately from admission: a forged account selector on a
// Research operation must be rejected by Research, not reclassified as Engine.
function researchOwnedOperation(request: any): boolean {
  return isResearchNativeOperation(request?.operation);
}

function researchOnlyPermits(request: any): boolean {
  return request?.account == null && request?.operation?.request?.account == null &&
    researchOwnedOperation(request);
}

function credentialFingerprint(value: unknown): string | null {
  return typeof value === "string" && /^[0-9a-f]{64}$/.test(value) ? value : null;
}

type RuntimeOwner = "engine" | "research";
type AttestationResult = {
  schema: "okx.research.attestation/v1";
  status: "PASS" | "UNVERIFIED";
  reason_code: string | null;
  pid: number | null;
  active_worker_sha256: string | null;
  installed_worker_sha256: string | null;
  process_generation: number | null;
  observed_at_ms: number | null;
  research_connection_generation: number | null;
};
type AttestorPending = {
  requestId: string;
  nonce: string;
  generation: number;
  sessionId: string;
  research: { sessionId: string; generation: number; pid: number; sha256: string };
  issuedAtMs: number;
  deadlineMs: number;
  resolve: (value: AttestationResult) => void;
  timer: ReturnType<typeof setTimeout>;
};
const ATTEST_PENDING_LIMIT = 3;
const ATTEST_FRAME_LIMIT_BYTES = 8 * 1024;
const CONTROL_STATUS_FIELDS = ["active_worker_sha256", "catalog_mode", "desired_running", "installed_worker_sha256", "last_error", "phase", "pid", "process_generation", "retry_in_ms", "schema"];
const CONTROL_REPLY_FIELDS = ["connection_generation", "nonce", "observed_at_ms", "request_id", "research_connection_generation", "research_session_id", "schema", "session_id", "status", "type"];
function exactFields(value: Record<string, unknown>, names: string[]): boolean {
  return Object.keys(value).sort().join(",") === names.join(",");
}
function validUnsigned(value: unknown): value is number {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= 0;
}
function validResearchStatus(value: unknown): value is Record<string, unknown> {
  if (!isObject(value) || !exactFields(value, CONTROL_STATUS_FIELDS)) return false;
  const sha = (v: unknown) => v === null || (typeof v === "string" && /^[0-9a-f]{64}$/.test(v));
  const pid = value.pid;
  return value.schema === "okx.host-control.research-runtime/v1" &&
    typeof value.desired_running === "boolean" && validUnsigned(value.process_generation) &&
    (pid === null || (validUnsigned(pid) && pid > 0 && pid <= 4_294_967_295)) &&
    sha(value.active_worker_sha256) && sha(value.installed_worker_sha256) &&
    ["STOPPED", "STARTING", "RUNNING_UNVERIFIED", "RESTART_WAIT", "INVALID_PROVENANCE"].includes(String(value.phase)) &&
    ["HOST_INDEPENDENT", "EXISTING_SCOPED"].includes(String(value.catalog_mode)) &&
    (value.retry_in_ms === null || (validUnsigned(value.retry_in_ms) && value.retry_in_ms <= 600_000)) &&
    (value.last_error === null || (typeof value.last_error === "string" && /^[A-Z0-9_]{1,128}$/.test(value.last_error))) &&
    (pid === null || (Number(value.process_generation) > 0 && value.active_worker_sha256 !== null)) &&
    (value.phase !== "RUNNING_UNVERIFIED" || pid !== null);
}
function unverified(reason_code: string, research_connection_generation: number | null = null): AttestationResult {
  return {schema: "okx.research.attestation/v1", status: "UNVERIFIED", reason_code,
    pid: null, active_worker_sha256: null, installed_worker_sha256: null,
    process_generation: null, observed_at_ms: null, research_connection_generation};
}
function ownerOfflineReason(owner: RuntimeOwner): string {
  return owner === "research" ? "RESEARCH_RUNTIME_OFFLINE" : "RUNTIME_OFFLINE";
}
type ReadWaiter = {
  resolve: (admitted: boolean) => void;
  timer: ReturnType<typeof setTimeout>;
};

class OwnerAdmission {
  inflight = 0;
  readonlyInflight = 0;
  normalInflight = 0;
  readonlyWaiters: ReadWaiter[] = [];
  activeRequests = new Set<string>();
}

function pendingKey(generation: number, key: string): string {
  return `${generation}:${key}`;
}

function socketOwner(attachment: SocketAttachment | null): RuntimeOwner | null {
  if (!attachment?.hello) return null;
  if (attachment.sessionMode === "research_only") {
    if (attachment.runtimeProfile === "research" && attachment.credentialOwner === "research")
      return "research";
    return null;
  }
  if (attachment.runtimeProfile === "engine" &&
      (attachment.credentialOwner === "engine" || attachment.credentialOwner === undefined))
    return "engine";
  return null;
}

export class RuntimeSession {
  private state: any;
  private env: Env;
  private pending = new Map<string, Pending>();
  // Ephemeral, read-only challenges. Hibernation drops the map and therefore
  // fails closed: a late/replayed response cannot satisfy a new request.
  private attestorPending = new Map<string, AttestorPending>();
  private admissions = { engine: new OwnerAdmission(), research: new OwnerAdmission() };

  constructor(state: any, env: Env) {
    this.state = state;
    this.env = env;
  }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);

    if(url.pathname==="/installation/inspect"&&request.method==="GET"){
      // Owner-authorized readback of the existing DO; never return account names, keys or amounts.
      const governance:unknown=await this.state.storage.get(GOVERNANCE_STORAGE_KEY);
      const storedRelease:unknown=await this.state.storage.get("installation_release_v1");
      const release=storedRelease!==null&&typeof storedRelease==="object"?storedRelease as Record<string,unknown>:null;
      const valid=validGovernance(governance);
      const bound=valid&&release?.installation_id===governance.installation_id;
      return Response.json({
        schema:"okx.installation-inspection/v1",
        state:bound?"BOUND":valid?"INCONSISTENT":"MISSING_OR_INVALID",
        installation_id:valid?governance.installation_id:null,
        accounts_total:valid?governance.accounts.length:null,
        // Owner-readonly account policy metadata. No UID, credential, position,
        // order, financial amount, or strategy/limits payload leaves the DO.
        account_access:valid?governance.accounts.map(a=>({
          id:a.id,environment:a.environment,identity_verified:a.identity_verified,
          enabled:a.enabled,explicit_pause:a.explicit_pause,policy_revision:a.policy_revision,
          strategies_enabled:a.strategies.filter(s=>s.enabled).length
        })):null,
        real_accounts_enabled:valid?governance.accounts.filter(a=>
          a.environment==="real"&&accessEnabled(a)).length:null,
        all_new_real_risk_paused:valid?governance.accounts.every(a=>
          a.environment!=="real"||!accessEnabled(a)):null,
        revision:valid?governance.revision:null,
        release_present:release!==null,
        finance_authority:"native"
      },{headers:{"cache-control":"no-store"}});
    }
    if(url.pathname==="/installation/initialize"&&request.method==="POST"){
      let raw:any;try{raw=await request.json();}catch{return Response.json({reason:"INVALID_INSTALLATION"},{status:400});}
      if(raw?.contract!==FRAME_SCHEMA||typeof raw.release_sha256!=="string"||!/^[0-9a-f]{64}$/.test(raw.release_sha256)||typeof raw.operation_id!=="string"||!/^[A-Za-z0-9_-]{8,128}$/.test(raw.operation_id)||raw.configuration?.campaign_loss_limit_usd!=="10")return Response.json({reason:"INVALID_INSTALLATION"},{status:400});
      const result=await initializeGovernance(this.state.storage,raw.configuration,{operation_id:raw.operation_id,release_sha256:raw.release_sha256,contract:raw.contract});
      if(result.status===200)await this.publishPolicy();
      return Response.json(result.body,{status:result.status});
    }
    if(url.pathname==="/account-governance/campaign-limit"&&request.method==="POST"){
      let raw:unknown;try{raw=await request.json();}catch{return Response.json({reason:"INVALID_REQUEST"},{status:400});}
      const result=await lowerCampaignLimit(this.state.storage,raw);if(result.status===200)await this.publishPolicy();return Response.json(result.body,{status:result.status,headers:{"cache-control":"no-store"}});
    }
    if(url.pathname==="/account-governance/limits"&&request.method==="POST"){
      let raw:unknown;try{raw=await request.json();}catch{return Response.json({reason:"INVALID_REQUEST"},{status:400});}
      const result=await setAccountLimits(this.state.storage,raw);if(result.status===200)await this.publishPolicy();return Response.json(result.body,{status:result.status});
    }
    if (url.pathname === "/runtime") {
      return this.acceptRuntime(request);
    }
    if (url.pathname === "/attestor") {
      return this.acceptAttestor(request);
    }
    if (url.pathname === "/attestation/research" && request.method === "GET") {
      return Response.json(await this.attestResearch(), {headers: {"cache-control": "no-store"}});
    }
    if (url.pathname === "/status") {
      const observed=await this.status(url.searchParams.get("probe") === "1",
        url.searchParams.get("owner")==="research"?"research":"engine");
      return Response.json(observed);
    }
    if(url.pathname==="/account-governance/stop"&&request.method==="POST"){
      let raw:any;try{raw=await request.json();}catch{return Response.json({reason:"INVALID_STOP_REQUEST"},{status:400});}
      const value=await stopNewRealRisk(this.state.storage,raw?.idempotency_key);
      if(value.status===200)await this.publishPolicy();
      return Response.json(value.body,{status:value.status,headers:{"cache-control":"no-store"}});
    }
    if(url.pathname==="/account-governance/pause"&&request.method==="POST"){
      const text=await request.text();
      if(text.length>1024)return Response.json({reason:"REQUEST_TOO_LARGE"},{status:413});
      let body:unknown;try{body=JSON.parse(text);}catch{return Response.json({reason:"INVALID_REQUEST"},{status:400});}
      const result=await setAccountPause(this.state.storage,body);
      if(result.status===200)await this.publishPolicy();
      return Response.json(result.body,{status:result.status});
    }
    if(url.pathname==="/account-governance/enroll"&&request.method==="POST"){
      // Internal DO endpoint; Worker alone constructs the observation from
      // an authenticated native trader_snapshot. It is never an MCP UID field.
      const body=await request.text();
      if(body.length>8192)return Response.json({status:"FAILED",reason:"ENROLLMENT_TOO_LARGE"},{status:413});
      let raw:unknown;
      try{raw=JSON.parse(body);}catch{
        return Response.json({status:"FAILED",reason:"INVALID_ENROLLMENT"},{status:400});
      }
      const r=raw as Record<string,unknown>;
      if(!r||typeof r.uid_fingerprint!=="string"||r.venue!=="okx"
        ||!["real","demo"].includes(String(r.environment))
        ||typeof r.label!=="string"||typeof r.alias!=="string"||typeof r.enabled!=="boolean"
        ||typeof r.idempotency_key!=="string"||typeof r.credential_ref!=="string"||!validLimits(r.limits))
        return Response.json({status:"FAILED",reason:"INVALID_ENROLLMENT"},{status:400});
      const out=await enrollObservedAccount(this.state.storage,{
        uid_fingerprint:r.uid_fingerprint,venue:"okx",
        environment:r.environment as "real"|"demo",label:r.label,alias:r.alias,enabled:r.enabled,limits:r.limits,idempotency_key:r.idempotency_key,credential_ref:r.credential_ref,
      });
      if(out.status===200)await this.publishPolicy();
      return Response.json(out.body,{status:out.status,headers:{"cache-control":"no-store"}});
    }
    if (url.pathname === "/account-governance") {
      // Same RuntimeSession owner, no additional DO instance or service.
      let update:unknown;
      if(request.method==="POST"){
        const body=await request.text();
        if(body.length>32768)return Response.json({status:"FAILED",reason:"GOVERNANCE_TOO_LARGE"},{status:413});
        try{update=JSON.parse(body);}
        catch{return Response.json({status:"FAILED",reason:"INVALID_GOVERNANCE_REQUEST"},{status:400});}
      }
      const answer=await governanceRequest(this.state.storage,request.method,update);
      if(request.method==="POST"&&answer.status===200)await this.publishPolicy();
      return Response.json(answer.body,{status:answer.status,headers:{"cache-control":"no-store"}});
    }
    if (url.pathname === "/dispatch" && request.method === "POST") {
      const body = await request.json() as any;
      return Response.json(await this.dispatch(body));
    }
    return new Response("not found", { status: 404 });
  }

  private async publishPolicy():Promise<void>{
    const state=await this.state.storage.get(GOVERNANCE_STORAGE_KEY);if(!validGovernance(state))return;
    for(const ws of this.state.getWebSockets?.("runtime")??[]){const a=ws.deserializeAttachment() as SocketAttachment;if(!a.hello||a.runtimeProfile!=="engine"||a.sessionMode==="research_only")continue;
      try{ws.send(JSON.stringify({type:"policy_snapshot",schema:FRAME_SCHEMA,session_id:a.sessionId,connection_generation:a.generation,snapshot:{state,issued_at_ms:Date.now(),session_generation:a.generation}}));}catch{try{ws.close(1011,"policy delivery failed");}catch{}}
    }
  }

  private async acceptRuntime(request: Request): Promise<Response> {
    if (request.headers.get("upgrade")?.toLowerCase() !== "websocket") {
      return new Response("websocket required", { status: 426 });
    }
    const engineHash = credentialFingerprint(this.env.RUNTIME_TOKEN_SHA256);
    const researchHash = credentialFingerprint(this.env.RESEARCH_RUNTIME_TOKEN_SHA256);
    // An absent or invalid credential for one application cannot take down
    // the other. A duplicated fingerprint authorizes the Engine alone.
    if (engineHash === null && researchHash === null) {
      return new Response("runtime credential fingerprint invalid", { status: 503 });
    }
    const authorization = request.headers.get("authorization") ?? "";
    if (!authorization.startsWith("Bearer ")) {
      return new Response("unauthorized", { status: 401 });
    }
    const presentedHash = await sha256Hex(authorization.slice("Bearer ".length));
    const credentialOwner = engineHash !== null && sameHex(engineHash, presentedHash) ? "engine" :
      researchHash !== null && sameHex(researchHash, presentedHash) ? "research" : null;
    if (!credentialOwner) {
      return new Response("unauthorized", { status: 401 });
    }

    // An upgrade is not yet identified as Engine or Research. Do not evict
    // either owner until a fully authenticated and validated hello succeeds.
    const generation = Number(await this.state.storage.get("generation") ?? 0) + 1;
    await this.state.storage.put("generation", generation);
    // A WSS generation is not a second owner of Windows installation proof.

    const pair = new (globalThis as any).WebSocketPair();
    const client = pair[0] as WebSocket;
    const server = pair[1] as any;
    const attachment: SocketAttachment = {
      runtimeId: RUNTIME_NAME,
      sessionId: `session_${crypto.randomUUID()}`,
      generation,
      hello: false,
      credentialOwner,
      connectedAtMs: Date.now(),
      lastPongAtMs: null,
    };
    server.serializeAttachment(attachment);
    this.state.acceptWebSocket(server, ["runtime"]);
    return new Response(null, { status: 101, webSocket: client } as any);
  }

  private async acceptAttestor(request: Request): Promise<Response> {
    if (request.headers.get("upgrade")?.toLowerCase() !== "websocket")
      return new Response("websocket required", {status: 426});
    const control = credentialFingerprint(this.env.CONTROL_ATTEST_TOKEN_SHA256);
    const engine = credentialFingerprint(this.env.RUNTIME_TOKEN_SHA256);
    const research = credentialFingerprint(this.env.RESEARCH_RUNTIME_TOKEN_SHA256);
    if (!control || control === engine || control === research)
      return new Response("attestor disabled", {status: 503});
    const auth = request.headers.get("authorization") ?? "";
    if (!auth.startsWith("Bearer ") || auth.length > 512)
      return new Response("unauthorized", {status: 401});
    if (!sameHex(control, await sha256Hex(auth.slice(7))))
      return new Response("unauthorized", {status: 401});

    // An attestor connection is never counted as a runtime generation and
    // can never receive /dispatch, policy snapshots or risk-control frames.
    const generation = Number(await this.state.storage.get("control_attestor_generation") ?? 0) + 1;
    await this.state.storage.put("control_attestor_generation", generation);
    const pair = new (globalThis as any).WebSocketPair();
    const client = pair[0] as WebSocket;
    const server = pair[1] as any;
    const attachment: ControlAttestorAttachment = {role: "control-attestor",
      admittedCredentialFingerprint: control, sessionId: `control_${crypto.randomUUID()}`,
      generation, hello: false, connectedAtMs: Date.now()};
    server.serializeAttachment(attachment);
    this.state.acceptWebSocket(server, ["attestor"]);
    return new Response(null, {status: 101, webSocket: client} as any);
  }

  private activeAttestor(): {ws: any; attachment: ControlAttestorAttachment} | null {
    const currentFingerprint = credentialFingerprint(this.env.CONTROL_ATTEST_TOKEN_SHA256);
    let selected: {ws: any; attachment: ControlAttestorAttachment} | null = null;
    for (const ws of this.state.getWebSockets("attestor") as any[]) {
      const attachment = ws.deserializeAttachment() as ControlAttestorAttachment | null;
      if (attachment?.role !== "control-attestor" || attachment.hello !== true ||
          !currentFingerprint || attachment.admittedCredentialFingerprint !== currentFingerprint) continue;
      if (!selected || attachment.generation > selected.attachment.generation)
        selected = {ws, attachment};
    }
    return selected;
  }

  private settleAttestor(requestId: string, value: AttestationResult): void {
    const pending = this.attestorPending.get(requestId);
    if (!pending) return;
    this.attestorPending.delete(requestId);
    clearTimeout(pending.timer);
    pending.resolve(value);
  }

  private rejectAttestorGeneration(generation: number, reason: string): void {
    for (const [requestId, pending] of this.attestorPending) {
      if (pending.generation === generation)
        this.settleAttestor(requestId, unverified(reason, pending.research.generation));
    }
  }

  private async receiveAttestor(ws: any, frame: unknown, bytes: number): Promise<void> {
    const a = ws.deserializeAttachment() as ControlAttestorAttachment;
    const currentFingerprint = credentialFingerprint(this.env.CONTROL_ATTEST_TOKEN_SHA256);
    if (!currentFingerprint || a.admittedCredentialFingerprint !== currentFingerprint ||
        currentFingerprint === credentialFingerprint(this.env.RUNTIME_TOKEN_SHA256) ||
        currentFingerprint === credentialFingerprint(this.env.RESEARCH_RUNTIME_TOKEN_SHA256)) {
      this.rejectAttestorGeneration(a.generation, "CONTROL_ATTEST_CREDENTIAL_REVOKED");
      ws.close(1008, "attestor credential revoked"); return;
    }
    if (bytes > ATTEST_FRAME_LIMIT_BYTES || !isObject(frame) || frame.schema !== CONTROL_ATTEST_FRAME_SCHEMA) {
      this.rejectAttestorGeneration(a.generation, "CONTROL_ATTEST_FRAME_INVALID");
      ws.close(bytes > ATTEST_FRAME_LIMIT_BYTES ? 1009 : 1008, "invalid control frame");
      return;
    }
    if (frame.type === "hello_control") {
      if (a.hello || !exactFields(frame, ["connection_id", "schema", "type"]) ||
          typeof frame.connection_id !== "string" || !/^[A-Za-z0-9_-]{16,128}$/.test(frame.connection_id)) {
        ws.close(1008, "invalid control hello"); return;
      }
      a.hello = true;
      ws.serializeAttachment(a);
      // Attestor rotation is independent of both native runtime owners.
      for (const old of this.state.getWebSockets("attestor") as any[]) {
        if (old === ws) continue;
        const prior = old.deserializeAttachment() as ControlAttestorAttachment | null;
        if (prior?.role === "control-attestor" && prior.hello === true) {
          this.rejectAttestorGeneration(prior.generation, "CONTROL_ATTEST_SOCKET_STALE");
          try { old.close(1012, "attestor new generation"); } catch {}
        }
      }
      ws.send(JSON.stringify({schema: CONTROL_ATTEST_FRAME_SCHEMA, type: "hello_ack",
        session_id: a.sessionId, connection_generation: a.generation}));
      return;
    }
    if (!a.hello || frame.type !== "research_observation" || !exactFields(frame, CONTROL_REPLY_FIELDS)) {
      this.rejectAttestorGeneration(a.generation, "CONTROL_ATTEST_FRAME_INVALID");
      ws.close(1008, "unexpected control frame"); return;
    }
    const pending = typeof frame.request_id === "string" ? this.attestorPending.get(frame.request_id) : undefined;
    if (!pending || pending.generation !== a.generation || pending.sessionId !== a.sessionId ||
        frame.session_id !== a.sessionId || frame.connection_generation !== a.generation ||
        frame.nonce !== pending.nonce ||
        frame.research_session_id !== pending.research.sessionId ||
        frame.research_connection_generation !== pending.research.generation) {
      this.rejectAttestorGeneration(a.generation, "CONTROL_ATTEST_RESPONSE_MISMATCH");
      ws.close(1008, "unmatched observation"); return;
    }
    const now = Date.now();
    // CONTROL has a host clock; allow bounded clock skew, but never a
    // response outside the Edge-issued challenge's own elapsed TTL.
    if (now > pending.deadlineMs || now < pending.issuedAtMs ||
        !validUnsigned(frame.observed_at_ms) ||
        frame.observed_at_ms < pending.issuedAtMs - 3_000 ||
        frame.observed_at_ms > now + 3_000 || !validResearchStatus(frame.status)) {
      this.settleAttestor(pending.requestId, unverified("CONTROL_ATTEST_RESPONSE_INVALID", pending.research.generation));
      ws.close(1008, "invalid observation"); return;
    }
    const observed = frame.status;
    if (observed.desired_running !== true || observed.phase !== "RUNNING_UNVERIFIED" ||
        observed.pid !== pending.research.pid || observed.process_generation === 0 ||
        observed.active_worker_sha256 !== pending.research.sha256 ||
        observed.installed_worker_sha256 !== pending.research.sha256) {
      this.settleAttestor(pending.requestId, unverified("CONTROL_RESEARCH_PID_SHA_MISMATCH", pending.research.generation));
      return;
    }
    this.settleAttestor(pending.requestId, {
      schema: "okx.research.attestation/v1", status: "PASS", reason_code: null,
      pid: observed.pid as number, active_worker_sha256: observed.active_worker_sha256 as string,
      installed_worker_sha256: observed.installed_worker_sha256 as string,
      process_generation: observed.process_generation as number,
      observed_at_ms: frame.observed_at_ms, research_connection_generation: pending.research.generation,
    });
  }

  private async attestResearch(): Promise<AttestationResult> {
    const fingerprint = credentialFingerprint(this.env.CONTROL_ATTEST_TOKEN_SHA256);
    if (!fingerprint || fingerprint === credentialFingerprint(this.env.RUNTIME_TOKEN_SHA256) ||
        fingerprint === credentialFingerprint(this.env.RESEARCH_RUNTIME_TOKEN_SHA256))
      return unverified("CONTROL_ATTEST_NOT_CONFIGURED");
    const research = this.activeSocket("research");
    if (!research || !research.attachment.agentPid || !research.attachment.agentBinarySha256)
      return unverified("RESEARCH_EDGE_SESSION_UNAVAILABLE");
    const attestor = this.activeAttestor();
    if (!attestor) return unverified("CONTROL_ATTEST_OFFLINE", research.attachment.generation);
    if (this.attestorPending.size >= ATTEST_PENDING_LIMIT)
      return unverified("CONTROL_ATTEST_BUSY", research.attachment.generation);
    const sameResearch = () => {
      const current = this.activeSocket("research");
      return current !== null && current.ws === research.ws && current.attachment.sessionId === research.attachment.sessionId &&
        current.attachment.generation === research.attachment.generation &&
        current.attachment.agentPid === research.attachment.agentPid &&
        current.attachment.agentBinarySha256 === research.attachment.agentBinarySha256;
    };
    try { await this.proveFresh(research); }
    catch { return unverified("RESEARCH_EDGE_SESSION_NOT_FRESH", research.attachment.generation); }
    if (!sameResearch()) return unverified("RESEARCH_EDGE_GENERATION_CHANGED", research.attachment.generation);
    if (this.activeAttestor()?.ws !== attestor.ws)
      return unverified("CONTROL_ATTEST_SOCKET_STALE", research.attachment.generation);
    // Multiple HTTP diagnostics can finish their Research pings together.
    // Re-check admission AFTER that await to keep the challenge map bounded.
    if (this.attestorPending.size >= ATTEST_PENDING_LIMIT)
      return unverified("CONTROL_ATTEST_BUSY", research.attachment.generation);
    const requestId = `att_${crypto.randomUUID()}`;
    const nonce = `nonce_${crypto.randomUUID()}`;
    const issuedAtMs = Date.now();
    const deadlineMs = issuedAtMs + CONTROL_ATTEST_DEADLINE_MS;
    const result = new Promise<AttestationResult>((resolve) => {
      const timer = setTimeout(() => this.settleAttestor(requestId,
        unverified("CONTROL_ATTEST_TIMEOUT", research.attachment.generation)), CONTROL_ATTEST_DEADLINE_MS);
      this.attestorPending.set(requestId, {requestId, nonce, generation: attestor.attachment.generation,
        sessionId: attestor.attachment.sessionId, research: {
          sessionId: research.attachment.sessionId, generation: research.attachment.generation,
          pid: research.attachment.agentPid!, sha256: research.attachment.agentBinarySha256!},
        issuedAtMs, deadlineMs, resolve, timer});
    });
    try {
      attestor.ws.send(JSON.stringify({schema: CONTROL_ATTEST_FRAME_SCHEMA, type: "observe_research",
        request_id: requestId, nonce, session_id: attestor.attachment.sessionId,
        connection_generation: attestor.attachment.generation,
        research_session_id: research.attachment.sessionId,
        research_connection_generation: research.attachment.generation, deadline_ms: deadlineMs}));
    } catch {
      this.settleAttestor(requestId, unverified("CONTROL_ATTEST_SEND_FAILED", research.attachment.generation));
    }
    const observed = await result;
    // A valid process observation from CONTROL still cannot be promoted when
    // Research reconnects during the challenge or CONTROL rotates meanwhile.
    if (!sameResearch()) return unverified("RESEARCH_EDGE_GENERATION_CHANGED", research.attachment.generation);
    if (this.activeAttestor()?.ws !== attestor.ws)
      return unverified("CONTROL_ATTEST_SOCKET_STALE", research.attachment.generation);
    return observed;
  }

  async webSocketMessage(ws: any, message: string | ArrayBuffer): Promise<void> {
    if (typeof message !== "string") {
      ws.close(1003, "text frames only");
      return;
    }
    // JavaScript string.length counts UTF-16 code units. Native WSS caps the
    // serialized UTF-8 frame at 64 KiB, including multibyte JSON strings.
    if (message.length > MAX_BODY_BYTES ||
        new TextEncoder().encode(message).byteLength > MAX_BODY_BYTES) {
      ws.close(1009, "frame too large");
      return;
    }
    const bytes = new TextEncoder().encode(message).byteLength;
    const kind = ws.deserializeAttachment?.();
    if (kind?.role === "control-attestor" && bytes > ATTEST_FRAME_LIMIT_BYTES) {
      this.rejectAttestorGeneration(kind.generation, "CONTROL_ATTEST_FRAME_INVALID");
      ws.close(1009, "control frame too large");
      return;
    }

    let frame: any;
    try { frame = JSON.parse(message); } catch {
      if (kind?.role === "control-attestor") this.rejectAttestorGeneration(kind.generation, "CONTROL_ATTEST_FRAME_INVALID");
      ws.close(1007, "invalid json");
      return;
    }
    if (kind?.role === "control-attestor") {
      await this.receiveAttestor(ws, frame, bytes);
      return;
    }
    const attachment = ws.deserializeAttachment() as SocketAttachment;
    if (!frame || frame.schema !== FRAME_SCHEMA) {
      ws.close(1008, "invalid schema");
      return;
    }

    if (frame.type === "hello" || frame.type === "hello_research") {
      if (attachment.hello) { ws.close(1008, "hello already accepted"); return; }
      if (frame.runtime_id !== RUNTIME_NAME || typeof frame.connection_id !== "string"
        || !Number.isSafeInteger(frame.agent_pid) || frame.agent_pid <= 0
        || typeof frame.agent_binary_sha256 !== "string"
        || !/^[0-9a-f]{64}$/.test(frame.agent_binary_sha256)
        || typeof frame.agent_incarnation !== "string"
        || !/^ctl_[0-9a-f]{32}$/.test(frame.agent_incarnation)) {
        ws.close(1008, "invalid hello");
        return;
      }
      const runtimeProfile = decodeRuntimeProfile(frame.runtime_profile);
      if (!runtimeProfile) {
        ws.close(1008, "invalid runtime profile");
        return;
      }
      if (frame.type === "hello_research") {
        const fields = ["agent_binary_sha256","agent_incarnation","agent_pid","connection_id","contract","host_scope","runtime_id","runtime_profile","schema","type"];
        if (Object.keys(frame).sort().join(",") !== fields.sort().join(",") || !/^[A-Za-z0-9_-]{16,128}$/.test(frame.connection_id) || typeof frame.host_scope !== "string" || !/^[0-9a-f]{64}$/.test(frame.host_scope) || frame.contract !== "okx.product/v1" || runtimeProfile !== "research" || attachment.credentialOwner !== "research") {
          ws.close(1008, "invalid research hello"); return;
        }
        attachment.sessionMode = "research_only";
        attachment.hostScope = frame.host_scope;
        delete attachment.installationId;
      } else {
        if ("host_scope" in frame) { ws.close(1008, "mixed hello fields"); return; }
        const configuration=await this.state.storage.get(GOVERNANCE_STORAGE_KEY);
        if(!validGovernance(configuration)||frame.installation_id!==configuration.installation_id||frame.contract!=="okx.product/v1"||runtimeProfile!=="engine"||(attachment.credentialOwner??"engine")!=="engine"){
          ws.close(1008,"installation or contract mismatch");return;
        }
        attachment.sessionMode = "finance";
        attachment.installationId = frame.installation_id;
        delete attachment.hostScope;
      }
      attachment.runtimeProfile = runtimeProfile;
      attachment.agentPid = frame.agent_pid;
      attachment.agentBinarySha256 = frame.agent_binary_sha256;
      attachment.agentIncarnation = frame.agent_incarnation;
      attachment.hello = true;
      const acceptedOwner = socketOwner(attachment);
      if (!acceptedOwner) { ws.close(1008,"invalid owner credentials"); return; }
      // Exactly one live generation per owner, never a global socket owner.
      // Research reconnects must not interrupt financial protection, and
      // Engine reconnects must not invalidate Research's independent lane.
      for (const existing of this.state.getWebSockets("runtime") as any[]) {
        if (existing === ws) continue;
        const prior = existing.deserializeAttachment() as SocketAttachment | null;
        if (socketOwner(prior) !== acceptedOwner) continue;
        try { existing.close(1012, "owner new generation"); } catch {}
      }
      ws.serializeAttachment(attachment);
      ws.send(JSON.stringify({
        type: "hello_ack",
        schema: FRAME_SCHEMA,
        session_id: attachment.sessionId,
        connection_generation: attachment.generation,
      }));
      if (attachment.sessionMode !== "research_only") await this.publishPolicy();
      return;
    }

    if (
      !socketOwner(attachment) ||
      frame.session_id !== attachment.sessionId ||
      frame.connection_generation !== attachment.generation
    ) {
      ws.close(1008, "stale generation");
      return;
    }

    if (frame.type === "ping" && typeof frame.nonce === "string") {
      ws.send(JSON.stringify({
        type: "pong",
        schema: FRAME_SCHEMA,
        session_id: attachment.sessionId,
        connection_generation: attachment.generation,
        nonce: frame.nonce,
      }));
      return;
    }
    if (frame.type === "pong" && typeof frame.nonce === "string") {
      attachment.lastPongAtMs = Date.now();
      ws.serializeAttachment(attachment);
      this.resolvePending(`pong:${frame.nonce}`, attachment.generation, frame);
      return;
    }
    if(frame.type==="risk_control") {
      if (attachment.sessionMode === "research_only") { ws.close(1008, "research finance forbidden"); return; }
      const result=await handleNativeRiskCommand(this.state.storage,{
        connection_generation:attachment.generation,
        runtime_profile:attachment.runtimeProfile??"unverified",
        hello:attachment.hello
      },frame.command,Date.now());
      ws.send(JSON.stringify({
        type:"risk_control_result",schema:FRAME_SCHEMA,
        session_id:attachment.sessionId,
        connection_generation:attachment.generation,
        result
      }));
      return;
    }
    if (frame.type === "delivery_ack" && typeof frame.request_id === "string") {
      this.resolvePending(`ack:${frame.request_id}`, attachment.generation, frame);
      return;
    }
    if (frame.type === "response" && frame.response?.request_id) {
      this.resolvePending(`response:${frame.response.request_id}`, attachment.generation, frame.response);
      return;
    }

    ws.close(1008, "unexpected frame");
  }

  async webSocketClose(ws: any): Promise<void> {
    const attachment = ws.deserializeAttachment() as (SocketAttachment | ControlAttestorAttachment) | null;
    if (attachment && "role" in attachment && attachment.role === "control-attestor") {
      this.rejectAttestorGeneration(attachment.generation, "CONTROL_ATTEST_OFFLINE");
      return;
    }
    const native = attachment as SocketAttachment | null;
    if (native?.generation) {
      this.rejectGeneration(native.generation, ownerOfflineReason(socketOwner(native) ?? "engine"));
    }
  }

  async webSocketError(ws: any): Promise<void> {
    const attachment = ws.deserializeAttachment() as (SocketAttachment | ControlAttestorAttachment) | null;
    if (attachment && "role" in attachment && attachment.role === "control-attestor") {
      this.rejectAttestorGeneration(attachment.generation, "CONTROL_ATTEST_OFFLINE");
      return;
    }
    const native = attachment as SocketAttachment | null;
    if (native?.generation) {
      this.rejectGeneration(native.generation, ownerOfflineReason(socketOwner(native) ?? "engine"));
    }
  }

  private activeSocket(owner: RuntimeOwner = "engine"): { ws: any; attachment: SocketAttachment } | null {
    const sockets = this.state.getWebSockets("runtime") as any[];
    let selected: { ws: any; attachment: SocketAttachment } | null = null;
    for (const ws of sockets) {
      const attachment = ws.deserializeAttachment() as SocketAttachment;
      if (socketOwner(attachment) !== owner) continue;
      if (!selected || attachment.generation > selected.attachment.generation) {
        selected = { ws, attachment };
      }
    }
    return selected;
  }

  private async proveFresh(active: { ws: any; attachment: SocketAttachment }): Promise<string> {
    const nonce = `ping_${crypto.randomUUID()}`;
    const wait = this.waitFor(
      `pong:${nonce}`,
      active.attachment.generation,
      PONG_DEADLINE_MS,
    );
    try {
      active.ws.send(JSON.stringify({
        type: "ping",
        schema: FRAME_SCHEMA,
        session_id: active.attachment.sessionId,
        connection_generation: active.attachment.generation,
        nonce,
      }));
    } catch {
      this.rejectGeneration(active.attachment.generation,
        ownerOfflineReason(socketOwner(active.attachment) ?? "engine"));
    }
    await wait;
    return nonce;
  }

  private grantWaitingReads(admission: OwnerAdmission): void {
    // A reserved WSS slot is never consumed by ordinary read-only work.
    while(admission.readonlyWaiters.length && admission.normalInflight<MAX_READ_INFLIGHT &&
          admission.inflight<MAX_INFLIGHT){
      const next=admission.readonlyWaiters.shift()!;
      clearTimeout(next.timer);
      admission.readonlyInflight++;
      admission.normalInflight++;
      admission.inflight++;
      next.resolve(true);
    }
  }

  private admitReadOnly(admission: OwnerAdmission): Promise<boolean> {
    if(admission.readonlyWaiters.length===0 && admission.normalInflight<MAX_READ_INFLIGHT &&
       admission.inflight<MAX_INFLIGHT){
      admission.readonlyInflight++;admission.normalInflight++;admission.inflight++;
      return Promise.resolve(true);
    }
    if(admission.readonlyWaiters.length>=MAX_READ_WAITERS)return Promise.resolve(false);
    return new Promise<boolean>(resolve=>{
      const waiter={resolve,timer:null as unknown as ReturnType<typeof setTimeout>};
      waiter.timer=setTimeout(()=>{
        const index=admission.readonlyWaiters.indexOf(waiter);
        if(index>=0){admission.readonlyWaiters.splice(index,1);resolve(false);}
      },READ_QUEUE_WAIT_MS);
      admission.readonlyWaiters.push(waiter);
    });
  }

  private async dispatch(request: any): Promise<Json> {
    if (!request || request.schema !== "okx.agent.request/v1" || typeof request.request_id !== "string") {
      return transportFailure("INVALID_REQUEST", false);
    }
    if (request.target_owner !== undefined && request.target_owner !== "research" &&
        request.target_owner !== "engine") return transportFailure("INVALID_OWNER", false);
    // Ownership follows the operation, independently of credential configuration.
    if (request.target_owner === "engine" && researchOwnedOperation(request))
      return transportFailure("RESEARCH_OWNER_REQUIRED", false);
    const owner: RuntimeOwner = request.target_owner === "research" || researchOwnedOperation(request)
      ? "research" : "engine";
    if (owner !== "engine" && !researchOnlyPermits(request))
      return transportFailure("RESEARCH_OPERATION_FORBIDDEN", false);
    const active = this.activeSocket(owner);
    if (!active) return transportFailure(ownerOfflineReason(owner));
    // OAuth/profile only authorizes delivery of a narrowly typed action.
    // The native Engine alone evaluates current account admission and
    // owns the actual exchange POST admission under the same ledger.
    if (!executionProfilePermitsOperation(
      active.attachment.runtimeProfile,
      request.operation,
    )) {
      return transportFailure("EXECUTION_PROFILE_OPERATION_UNSUPPORTED", false);
    }

    if (active.attachment.sessionMode === "research_only" && !researchOnlyPermits(request)) {
      return transportFailure("RESEARCH_OPERATION_FORBIDDEN", false);
    }

    const admission = this.admissions[owner === "research" ? "research" : "engine"];
    if(admission.activeRequests.has(request.request_id))
      return transportFailure("REQUEST_ALREADY_IN_FLIGHT",false);
    // Keep protective trading commands and exact-intent status above both
    // read-only traffic AND ordinary research/strategy/new-risk mutations.
    // Ordinary mutations NEVER wait: BUSY is returned before native delivery.
    const type=request.operation?.type;
    const action=request.operation?.request?.action;
    const protective=(type==="trade" && (["status","cancel","close"].includes(String(action)) ||
      (action==="execute"&&["reduce","close"].includes(String(request.operation?.request?.operation))))) ||
      (type==="strategies"&&action==="stop") ||
      (type==="research"&&action==="cancel");
    const queuedRead=!protective&&!isExecutionMutation(type,action);
    admission.activeRequests.add(request.request_id);
    let admitted=false;
    try {
      if(queuedRead){
        admitted=await this.admitReadOnly(admission);
        if(!admitted)return transportFailure("TRANSPORT_BUSY");
      }else if(protective){
        if(admission.inflight>=MAX_INFLIGHT)return transportFailure("TRANSPORT_BUSY");
        admission.inflight++;admitted=true;
      }else{
        if(admission.normalInflight>=MAX_READ_INFLIGHT||admission.inflight>=MAX_INFLIGHT)
          return transportFailure("TRANSPORT_BUSY");
        admission.normalInflight++;admission.inflight++;admitted=true;
      }
      // A waiting reader must never cross a native session generation.
      const current=this.activeSocket(owner);
      if(!current||current.attachment.generation!==active.attachment.generation||
         current.attachment.sessionId!==active.attachment.sessionId)
        return transportFailure(ownerOfflineReason(owner));
      const deliveryChallenge = await this.proveFresh(active);

      // Production writes and exact-intent terminal status both need the
      // current native governance configuration for its own finance decisions.
      const tradeMutation = isExecutionMutation(request.operation?.type,request.operation?.request?.action)&&request.operation?.type!=="connect_account";
      const terminalStatus = request.operation?.type === "trade";
      let operator_governance: Record<string, unknown> | undefined;
      if ((tradeMutation || terminalStatus) &&
          active.attachment.runtimeProfile === "engine" &&
          active.attachment.sessionMode !== "research_only") {
        const current:unknown=await this.state.storage.get(GOVERNANCE_STORAGE_KEY);
        if(current!==undefined){
          if(!validGovernance(current))
            return transportFailure("CENTRAL_GOVERNANCE_CORRUPT",false);
          if(request.operation?.type==="trade"&&request.operation?.request?.action==="execute" && ["open","add","replace"].includes(request.operation?.request?.operation) &&
             !current.accounts.some(a=>accessEnabled(a)&&(a.id===request.account||a.account_key===request.account)))
            return transportFailure("CENTRAL_NEW_RISK_STOPPED",false);
          // Typed fresh request-scoped delivery proof. This is NOT the POST
          // permit; the native executor enforces its own durable finance reservation.
          operator_governance={
            state:current,issued_at_ms:Date.now(),
            session_generation:active.attachment.generation,
            // Native Agent consumed this exact ping just before delivery.
            // No inter-machine wall clock dependency for a risk-controlled write.
            delivery_challenge:deliveryChallenge
          };
        }else if(tradeMutation){
          return transportFailure("INSTALLATION_ACCESS_MISSING_OR_CORRUPT",false);
        }
      }
      const ack = this.waitFor(
        `ack:${request.request_id}`,
        active.attachment.generation,
        ACK_DEADLINE_MS,
      );
      const response = this.waitFor(
        `response:${request.request_id}`,
        active.attachment.generation,
        RESPONSE_DEADLINE_MS,
      );
      const completion=Promise.all([ack,response]);
      // Install a rejection observer before send: synchronous WS failure and
      // disconnect can reject both stages before the await is reached.
      void completion.catch(()=>undefined);
      // target_owner is a Worker/DO-only routing instruction, not part of the
      // native AgentRequest contract. Rust rejects unknown fields on both the
      // request and the enclosing DirectTransportFrame. Never leak this
      // internal routing metadata to either native owner, and do not mutate
      // the original dispatch request while the response remains in flight.
      const nativeRequest = {...request};
      delete nativeRequest.target_owner;
      try {
        active.ws.send(JSON.stringify({
          type: "request",
          schema: FRAME_SCHEMA,
          session_id: active.attachment.sessionId,
          connection_generation: active.attachment.generation,
          request: nativeRequest,
          ...(operator_governance === undefined ? {} : { operator_governance }),
        }));
      } catch {
        // A synchronous WSS send failure is owner-specific. Settle both
        // delivery stages with the same typed reason as a close/error event.
        this.rejectGeneration(active.attachment.generation, ownerOfflineReason(owner));
        throw new Error(ownerOfflineReason(owner));
      }
      const [,reply]=await completion;
      return reply as Json;
    } catch (error) {
      // An exchange request may already have reached the one Rust owner
      // even when this HTTP/WS envelope loses ACK or response. Returning a
      // retryable failure would tempt blind duplicate sends. Require an exact
      // read-only intent/ledger reconciliation, never auto-retry the mutation.
      if (request.operation?.type === "research" &&
          isExecutionMutation("research", request.operation?.request?.action)) {
        // Backtest/grid/benchmark create durable jobs; cancel/record_decision
        // can also commit before the transport drops. Their recovery owner is
        // the Research job/idempotency receipt, not a financial trade intent.
        return { schema: "okx.direct-transport.result/v1", status: "UNKNOWN",
          reason: "RESEARCH_MUTATION_OUTCOME_UNKNOWN_CHECK_JOB", scope: "research",
          retryable: false, next_action: "inspect_research_job_or_idempotency_receipt" };
      }
      if (isExecutionMutation(request.operation?.type,request.operation?.request?.action)) {
        return transportFailure("MUTATION_OUTCOME_UNKNOWN_RECONCILE_EXACT_INTENT", false);
      }
      return transportFailure(error instanceof Error ? error.message : "TRANSPORT_FAILURE");
    } finally {
      if(admitted){
        admission.inflight--;
        if(!protective)admission.normalInflight--;
        if(queuedRead)admission.readonlyInflight--;
        this.grantWaitingReads(admission);
      }
      admission.activeRequests.delete(request.request_id);
      this.clearPending(`ack:${request.request_id}`,active.attachment.generation);
      this.clearPending(`response:${request.request_id}`,active.attachment.generation);
    }
  }

  private async status(probe: boolean, owner: "engine" | "research" = "engine"): Promise<Record<string,Json>> {
    const active = this.activeSocket(owner);
    const admission = this.admissions[owner];
    if (!active) {
      return {
        schema: "okx.direct-transport.status/v1",
        status: "OFFLINE",
        runtime_id: RUNTIME_NAME,
        owner,
        connected: false,
        session_fresh: false,
        inflight: admission.inflight,
        readonly_inflight: admission.readonlyInflight,
        normal_inflight: admission.normalInflight,
        readonly_queue_depth: admission.readonlyWaiters.length,
        readonly_queue_capacity: MAX_READ_WAITERS,
      };
    }

    let fresh = false;
    if (probe) {
      try {
        await this.proveFresh(active);
        fresh = true;
      } catch {
        fresh = false;
      }
    } else {
      fresh = active.attachment.lastPongAtMs !== null &&
        Date.now() - active.attachment.lastPongAtMs < 30_000;
    }

    return {
      schema: "okx.direct-transport.status/v1",
      status: fresh ? "PASS" : "STALE",
      runtime_id: active.attachment.runtimeId,
      owner,
      runtime_profile: active.attachment.runtimeProfile ?? "unverified",
      connected: true,
      session_fresh: fresh,
      connection_generation: active.attachment.generation,
      inflight: admission.inflight,
      readonly_inflight: admission.readonlyInflight,
      normal_inflight: admission.normalInflight,
      readonly_queue_depth: admission.readonlyWaiters.length,
      readonly_queue_capacity: MAX_READ_WAITERS,
      connected_at_ms: active.attachment.connectedAtMs,
      installation_id: active.attachment.installationId ?? null,
      agent_pid: active.attachment.agentPid ?? null,
      agent_binary_sha256: active.attachment.agentBinarySha256 ?? null,
      agent_incarnation: active.attachment.agentIncarnation ?? null,
    };
  }

  private waitFor(key: string, generation: number, timeoutMs: number): Promise<any> {
    const handle = pendingKey(generation, key);
    if(this.pending.has(handle))return Promise.reject(new Error("REQUEST_ALREADY_IN_FLIGHT"));
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        const current = this.pending.get(handle);
        if (current?.generation === generation) {
          this.pending.delete(handle);
        }
        reject(new Error(key.startsWith("pong:") ? "SESSION_STALE" : key.startsWith("ack:") ? "DELIVERY_FAILED" : "RESPONSE_TIMEOUT"));
      }, timeoutMs);
      this.pending.set(handle, { generation, resolve, reject, timer });
    });
  }

  private resolvePending(key: string, generation: number, value: any): void {
    const handle = pendingKey(generation, key);
    const pending = this.pending.get(handle);
    if (!pending) return;
    clearTimeout(pending.timer);
    this.pending.delete(handle);
    pending.resolve(value);
  }

  private clearPending(key: string,generation:number): void {
    const handle = pendingKey(generation, key);
    const pending = this.pending.get(handle);
    if (!pending) return;
    clearTimeout(pending.timer);
    this.pending.delete(handle);
    pending.reject(new Error("REQUEST_FINISHED"));
  }

  private rejectGeneration(generation: number, reason: string): void {
    for (const [key, pending] of this.pending) {
      if (pending.generation !== generation) continue;
      clearTimeout(pending.timer);
      pending.reject(new Error(reason));
      this.pending.delete(key);
    }
  }
}
