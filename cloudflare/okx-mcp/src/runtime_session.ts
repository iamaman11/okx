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
} from "./shared";

export class RuntimeSession {
  private state: any;
  private env: Env;
  private pending = new Map<string, Pending>();
  private inflight = 0;

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

  private async proveFresh(active: { ws: any; attachment: SocketAttachment }): Promise<void> {
    const nonce = `ping_${crypto.randomUUID()}`;
    const wait = this.waitFor(
      `pong:${nonce}`,
      active.attachment.generation,
      PONG_DEADLINE_MS,
    );
    active.ws.send(JSON.stringify({
      type: "ping",
      schema: FRAME_SCHEMA,
      session_id: active.attachment.sessionId,
      connection_generation: active.attachment.generation,
      nonce,
    }));
    await wait;
  }

  private async dispatch(request: any): Promise<Json> {
    if (this.inflight >= MAX_INFLIGHT) return transportFailure("TRANSPORT_BUSY");
    if (!request || request.schema !== "okx.agent.request/v1" || typeof request.request_id !== "string") {
      return transportFailure("INVALID_REQUEST", false);
    }
    const active = this.activeSocket();
    if (!active) return transportFailure("RUNTIME_OFFLINE");
    // OAuth alone never authorizes a trading command; profile identity
    // comes from the single authenticated Windows Hello/generation.
    const executionMutation = [
      "prepare_execution", "submit_prepared_execution",
      "mutate_execution", "abort_reverse_execution",
    ].includes(String(request.operation?.type));
    if (executionMutation && active.attachment.runtimeProfile !== "demo_acceptance") {
      return transportFailure("DEMO_EXECUTION_PROFILE_REQUIRED", false);
    }

    this.inflight += 1;
    try {
      await this.proveFresh(active);

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
      active.ws.send(JSON.stringify({
        type: "request",
        schema: FRAME_SCHEMA,
        session_id: active.attachment.sessionId,
        connection_generation: active.attachment.generation,
        request,
      }));
      await ack;
      return await response as Json;
    } catch (error) {
      return transportFailure(error instanceof Error ? error.message : "TRANSPORT_FAILURE");
    } finally {
      this.inflight -= 1;
      this.clearPending(`ack:${request.request_id}`);
      this.clearPending(`response:${request.request_id}`);
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
    this.clearPending(key);
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

  private clearPending(key: string): void {
    const pending = this.pending.get(key);
    if (!pending) return;
    clearTimeout(pending.timer);
    this.pending.delete(key);
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
