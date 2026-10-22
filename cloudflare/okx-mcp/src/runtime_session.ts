import { executionProfilePermitsOperation, isExecutionMutation } from "./execution_tools.js";
import { governanceRequest, enrollObservedAccount, stopNewRealRisk, setAccountPause, GOVERNANCE_STORAGE_KEY } from "./account_governance_store.js";
import {validGovernance,accessEnabled,validLimits} from "./account_governance.js";
import { handleNativeRiskCommand } from "./risk_authority.js";
import {
  ACK_DEADLINE_MS,
  type Env,
  FRAME_SCHEMA,
  type Json,
  MAX_BODY_BYTES,
  MAX_INFLIGHT,
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

export class RuntimeSession {
  private state: any;
  private env: Env;
  private pending = new Map<string, Pending>();
  private inflight = 0;
  private activeRequests=new Set<string>();

  constructor(state: any, env: Env) {
    this.state = state;
    this.env = env;
  }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);

    if (url.pathname === "/runtime") {
      return this.acceptRuntime(request);
    }
    if (url.pathname === "/status") {
      return Response.json(await this.status(url.searchParams.get("probe") === "1"));
    }
    if(url.pathname==="/account-governance/stop"&&request.method==="POST"){
      let raw:any;try{raw=await request.json();}catch{return Response.json({reason:"INVALID_STOP_REQUEST"},{status:400});}
      const value=await stopNewRealRisk(this.state.storage,raw?.idempotency_key);
      return Response.json(value.body,{status:value.status,headers:{"cache-control":"no-store"}});
    }
    if(url.pathname==="/account-governance/pause"&&request.method==="POST"){
      const text=await request.text();
      if(text.length>1024)return Response.json({reason:"REQUEST_TOO_LARGE"},{status:413});
      let body:unknown;try{body=JSON.parse(text);}catch{return Response.json({reason:"INVALID_REQUEST"},{status:400});}
      const result=await setAccountPause(this.state.storage,body);
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
        ||typeof r.idempotency_key!=="string"||!validLimits(r.limits))
        return Response.json({status:"FAILED",reason:"INVALID_ENROLLMENT"},{status:400});
      const out=await enrollObservedAccount(this.state.storage,{
        uid_fingerprint:r.uid_fingerprint,venue:"okx",
        environment:r.environment as "real"|"demo",label:r.label,alias:r.alias,enabled:r.enabled,limits:r.limits,idempotency_key:r.idempotency_key,
      });
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
      return Response.json(answer.body,{status:answer.status,headers:{"cache-control":"no-store"}});
    }
    if (url.pathname === "/dispatch" && request.method === "POST") {
      const body = await request.json() as any;
      return Response.json(await this.dispatch(body));
    }
    return new Response("not found", { status: 404 });
  }

  private async acceptRuntime(request: Request): Promise<Response> {
    if (request.headers.get("upgrade")?.toLowerCase() !== "websocket") {
      return new Response("websocket required", { status: 426 });
    }
    const configuredHash = this.env.RUNTIME_TOKEN_SHA256;
    if (!/^[0-9a-f]{64}$/.test(configuredHash)) {
      return new Response("runtime credential fingerprint invalid", { status: 503 });
    }
    const authorization = request.headers.get("authorization") ?? "";
    if (!authorization.startsWith("Bearer ")) {
      return new Response("unauthorized", { status: 401 });
    }
    const presentedHash = await sha256Hex(authorization.slice("Bearer ".length));
    if (!sameHex(configuredHash, presentedHash)) {
      return new Response("unauthorized", { status: 401 });
    }

    for (const existing of this.state.getWebSockets("runtime") as WebSocket[]) {
      try { existing.close(1012, "new generation"); } catch {}
    }

    const generation = Number(await this.state.storage.get("generation") ?? 0) + 1;
    await this.state.storage.put("generation", generation);

    const pair = new (globalThis as any).WebSocketPair();
    const client = pair[0] as WebSocket;
    const server = pair[1] as any;
    const attachment: SocketAttachment = {
      runtimeId: RUNTIME_NAME,
      sessionId: `session_${crypto.randomUUID()}`,
      generation,
      hello: false,
      connectedAtMs: Date.now(),
      lastPongAtMs: null,
    };
    server.serializeAttachment(attachment);
    this.state.acceptWebSocket(server, ["runtime"]);
    return new Response(null, { status: 101, webSocket: client } as any);
  }

  async webSocketMessage(ws: any, message: string | ArrayBuffer): Promise<void> {
    if (typeof message !== "string" || message.length > MAX_BODY_BYTES) {
      ws.close(1003, "text frames only");
      return;
    }

    let frame: any;
    try { frame = JSON.parse(message); } catch {
      ws.close(1007, "invalid json");
      return;
    }
    const attachment = ws.deserializeAttachment() as SocketAttachment;
    if (!frame || frame.schema !== FRAME_SCHEMA) {
      ws.close(1008, "invalid schema");
      return;
    }

    if (frame.type === "hello") {
      if (frame.runtime_id !== RUNTIME_NAME || typeof frame.connection_id !== "string") {
        ws.close(1008, "invalid hello");
        return;
      }
      const runtimeProfile = decodeRuntimeProfile(frame.runtime_profile);
      if (!runtimeProfile) {
        ws.close(1008, "invalid runtime profile");
        return;
      }
      attachment.runtimeProfile = runtimeProfile;
      attachment.hello = true;
      ws.serializeAttachment(attachment);
      ws.send(JSON.stringify({
        type: "hello_ack",
        schema: FRAME_SCHEMA,
        session_id: attachment.sessionId,
        connection_generation: attachment.generation,
      }));
      return;
    }

    if (
      !attachment.hello ||
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
    const attachment = ws.deserializeAttachment() as SocketAttachment | null;
    if (attachment?.generation) {
      this.rejectGeneration(attachment.generation, "RUNTIME_OFFLINE");
    }
  }

  async webSocketError(ws: any): Promise<void> {
    const attachment = ws.deserializeAttachment() as SocketAttachment | null;
    if (attachment?.generation) {
      this.rejectGeneration(attachment.generation, "RUNTIME_OFFLINE");
    }
  }

  private activeSocket(): { ws: any; attachment: SocketAttachment } | null {
    const sockets = this.state.getWebSockets("runtime") as any[];
    let selected: { ws: any; attachment: SocketAttachment } | null = null;
    for (const ws of sockets) {
      const attachment = ws.deserializeAttachment() as SocketAttachment;
      if (!attachment?.hello) continue;
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
      this.rejectGeneration(active.attachment.generation,"RUNTIME_OFFLINE");
    }
    await wait;
    return nonce;
  }

  private async dispatch(request: any): Promise<Json> {
    if (this.inflight >= MAX_INFLIGHT) return transportFailure("TRANSPORT_BUSY");
    if (!request || request.schema !== "okx.agent.request/v1" || typeof request.request_id !== "string") {
      return transportFailure("INVALID_REQUEST", false);
    }
    const active = this.activeSocket();
    if (!active) return transportFailure("RUNTIME_OFFLINE");
    // OAuth/profile only authorizes delivery of a narrowly typed action.
    // Rust CONTROL/executor alone evaluates the durable live policy and
    // owns the actual exchange POST admission under the same ledger.
    if (!executionProfilePermitsOperation(
      active.attachment.runtimeProfile,
      request.operation?.type,
    )) {
      return transportFailure("EXECUTION_PROFILE_OPERATION_UNSUPPORTED", false);
    }

    if(this.activeRequests.has(request.request_id))
      return transportFailure("REQUEST_ALREADY_IN_FLIGHT",false);
    this.activeRequests.add(request.request_id);
    this.inflight += 1;
    try {
      const deliveryChallenge = await this.proveFresh(active);

      // Production writes and exact-intent terminal status both need the
      // current native governance cohort. The latter is ONLY for a proven
      // terminal RELEASE, not permission to introduce new exposure.
      const tradeMutation = isExecutionMutation(request.operation?.type);
      const terminalStatus = request.operation?.type === "execution_status";
      let operator_governance: Record<string, unknown> | undefined;
      if ((tradeMutation || terminalStatus) &&
          active.attachment.runtimeProfile === "production") {
        const current:unknown=await this.state.storage.get(GOVERNANCE_STORAGE_KEY);
        if(current!==undefined){
          if(!validGovernance(current))
            return transportFailure("CENTRAL_GOVERNANCE_CORRUPT",false);
          if(tradeMutation &&
             !current.accounts.some(accessEnabled))
            return transportFailure("CENTRAL_NEW_RISK_STOPPED",false);
          // Typed fresh request-scoped delivery proof. This is NOT the POST
          // permit; the native executor still needs an exact DO reservation.
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
      active.ws.send(JSON.stringify({
        type: "request",
        schema: FRAME_SCHEMA,
        session_id: active.attachment.sessionId,
        connection_generation: active.attachment.generation,
        request,
        ...(operator_governance === undefined ? {} : { operator_governance }),
      }));
      const [,reply]=await completion;
      return reply as Json;
    } catch (error) {
      // An exchange request may already have reached the one Rust owner
      // even when this HTTP/WS envelope loses ACK or response. Returning a
      // retryable failure would tempt blind duplicate sends. Require an exact
      // read-only intent/ledger reconciliation, never auto-retry the mutation.
      if (request.operation?.type === "prepare_execution") {
        // A lost delivery/response may leave local PREPARED state, but this
        // action never sends a venue order. It must be checked by intent ID,
        // not retried and not reported as an unknown exchange fill.
        return transportFailure("EXECUTION_PREPARE_OUTCOME_UNKNOWN_READ_EXACT_INTENT", false);
      }
      if (isExecutionMutation(request.operation?.type)) {
        return transportFailure("MUTATION_OUTCOME_UNKNOWN_RECONCILE_EXACT_INTENT", false);
      }
      return transportFailure(error instanceof Error ? error.message : "TRANSPORT_FAILURE");
    } finally {
      this.inflight -= 1;
      this.activeRequests.delete(request.request_id);
      this.clearPending(`ack:${request.request_id}`,active.attachment.generation);
      this.clearPending(`response:${request.request_id}`,active.attachment.generation);
    }
  }

  private async status(probe: boolean): Promise<Json> {
    const active = this.activeSocket();
    if (!active) {
      return {
        schema: "okx.direct-transport.status/v1",
        status: "OFFLINE",
        runtime_id: RUNTIME_NAME,
        connected: false,
        session_fresh: false,
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
      runtime_profile: active.attachment.runtimeProfile ?? "unverified",
      connected: true,
      session_fresh: fresh,
      connection_generation: active.attachment.generation,
    };
  }

  private waitFor(key: string, generation: number, timeoutMs: number): Promise<any> {
    if(this.pending.has(key))return Promise.reject(new Error("REQUEST_ALREADY_IN_FLIGHT"));
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        const current = this.pending.get(key);
        if (current?.generation === generation) {
          this.pending.delete(key);
        }
        reject(new Error(key.startsWith("pong:") ? "SESSION_STALE" : key.startsWith("ack:") ? "DELIVERY_FAILED" : "RESPONSE_TIMEOUT"));
      }, timeoutMs);
      this.pending.set(key, { generation, resolve, reject, timer });
    });
  }

  private resolvePending(key: string, generation: number, value: any): void {
    const pending = this.pending.get(key);
    if (!pending || pending.generation !== generation) return;
    clearTimeout(pending.timer);
    this.pending.delete(key);
    pending.resolve(value);
  }

  private clearPending(key: string,generation:number): void {
    const pending = this.pending.get(key);
    if (!pending||pending.generation!==generation) return;
    clearTimeout(pending.timer);
    this.pending.delete(key);
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
