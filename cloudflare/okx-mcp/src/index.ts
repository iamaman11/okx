import OAuthProvider, { type OAuthHelpers } from "@cloudflare/workers-oauth-provider";

const FRAME_SCHEMA = "okx.direct-transport.frame/v1";
const MCP_PROTOCOL_VERSION = "2025-06-18";
const MAX_BODY_BYTES = 64 * 1024;
const MAX_INFLIGHT = 8;
const PONG_DEADLINE_MS = 2_000;
const ACK_DEADLINE_MS = 2_000;
const RESPONSE_DEADLINE_MS = 20_000;
const AUTH_CSRF_TTL_MS = 5 * 60 * 1000;
const AUTH_APPROVAL_TTL_MS = 2 * 60 * 1000;
const RUNTIME_NAME = "windows-primary";
const PUBLIC_ORIGIN = "https://okx-cloudflare-mcp.okx-794.workers.dev";

type Json = null | boolean | number | string | Json[] | { [key: string]: Json };

interface Env {
  OAUTH_KV: any;
  RUNTIME: any;
  MCP_OWNER_SECRET: string;
  RUNTIME_TOKEN_SHA256: string;
  OAUTH_PROVIDER: OAuthHelpers;
}

interface SocketAttachment {
  runtimeId: string;
  sessionId: string;
  generation: number;
  hello: boolean;
  connectedAtMs: number;
  lastPongAtMs: number | null;
}

interface Pending {
  generation: number;
  resolve: (value: any) => void;
  reject: (error: Error) => void;
  timer: ReturnType<typeof setTimeout>;
}

function jsonRpc(id: Json, result: Json): Response {
  return Response.json({ jsonrpc: "2.0", id, result });
}

function jsonRpcError(id: Json, code: number, message: string): Response {
  return Response.json({ jsonrpc: "2.0", id, error: { code, message } });
}

function toolResult(value: Json): Json {
  return {
    content: [{ type: "text", text: JSON.stringify(value) }],
    structuredContent: value,
  };
}

function transportFailure(reason: string, retryable = true): Json {
  return {
    schema: "okx.direct-transport.result/v1",
    status: "FAILED",
    reason,
    retryable,
  };
}

function requestId(): string {
  return `req_mcp_${crypto.randomUUID()}`;
}

function isObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function validCode(value: unknown, min = 2, max = 64): value is string {
  return (
    typeof value === "string" &&
    value.length >= min &&
    value.length <= max &&
    /^[A-Z0-9_-]+$/.test(value)
  );
}

function validInstrument(value: unknown): value is string {
  return (
    typeof value === "string" &&
    value.length >= 3 &&
    value.length <= 64 &&
    /^[A-Z0-9_-]+$/.test(value)
  );
}

async function sha256Hex(value: string): Promise<string> {
  const bytes = new TextEncoder().encode(value);
  const digest = await crypto.subtle.digest("SHA-256", bytes);
  return [...new Uint8Array(digest)].map((byte) => byte.toString(16).padStart(2, "0")).join("");
}

async function hmacSha256Hex(secret: string, value: string): Promise<string> {
  const key = await crypto.subtle.importKey(
    "raw",
    new TextEncoder().encode(secret),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"],
  );
  const signature = await crypto.subtle.sign("HMAC", key, new TextEncoder().encode(value));
  return [...new Uint8Array(signature)].map((byte) => byte.toString(16).padStart(2, "0")).join("");
}

function authorizationRequestUrl(url: URL): URL {
  const clean = new URL(url.toString());
  clean.searchParams.delete("approval");
  clean.hash = "";
  return clean;
}

function canonicalAuthorizationBinding(url: URL): string {
  const canonical = authorizationRequestUrl(url);
  canonical.searchParams.sort();
  return `${canonical.origin}${canonical.pathname}?${canonical.searchParams.toString()}`;
}

function authorizationTokenMessage(kind: "csrf" | "approval", url: URL, expiresAtMs: number): string {
  return [
    `okx.oauth.${kind}/v1`,
    String(expiresAtMs),
    canonicalAuthorizationBinding(url),
  ].join("\n");
}

async function issueAuthorizationToken(
  env: Env,
  kind: "csrf" | "approval",
  url: URL,
  ttlMs: number,
): Promise<string> {
  const expiresAtMs = Date.now() + ttlMs;
  const signature = await hmacSha256Hex(
    env.MCP_OWNER_SECRET,
    authorizationTokenMessage(kind, url, expiresAtMs),
  );
  return `${expiresAtMs}.${signature}`;
}

async function verifyAuthorizationToken(
  env: Env,
  kind: "csrf" | "approval",
  url: URL,
  token: string,
  ttlMs: number,
): Promise<boolean> {
  const separator = token.indexOf(".");
  if (separator <= 0) return false;
  const expiresText = token.slice(0, separator);
  const suppliedSignature = token.slice(separator + 1);
  if (!/^\d{13}$/.test(expiresText) || !/^[0-9a-f]{64}$/.test(suppliedSignature)) return false;

  const expiresAtMs = Number(expiresText);
  const now = Date.now();
  if (!Number.isSafeInteger(expiresAtMs) || expiresAtMs < now || expiresAtMs - now > ttlMs) {
    return false;
  }

  const expectedSignature = await hmacSha256Hex(
    env.MCP_OWNER_SECRET,
    authorizationTokenMessage(kind, url, expiresAtMs),
  );
  return sameHex(suppliedSignature, expectedSignature);
}

async function issueAuthorizationCsrf(env: Env, url: URL): Promise<string> {
  return issueAuthorizationToken(env, "csrf", url, AUTH_CSRF_TTL_MS);
}

async function verifyAuthorizationCsrf(env: Env, url: URL, token: string): Promise<boolean> {
  return verifyAuthorizationToken(env, "csrf", url, token, AUTH_CSRF_TTL_MS);
}

async function issueAuthorizationApproval(env: Env, url: URL): Promise<string> {
  return issueAuthorizationToken(env, "approval", url, AUTH_APPROVAL_TTL_MS);
}

async function verifyAuthorizationApproval(env: Env, url: URL, token: string): Promise<boolean> {
  return verifyAuthorizationToken(env, "approval", url, token, AUTH_APPROVAL_TTL_MS);
}

async function completeOwnerAuthorization(env: Env, authRequest: any): Promise<Response> {
  const { redirectTo } = await env.OAUTH_PROVIDER.completeAuthorization({
    request: authRequest,
    userId: "okx-owner",
    metadata: {},
    scope: authRequest.scope,
    props: { user: "okx-owner" },
    revokeExistingGrants: false,
  });
  return Response.redirect(redirectTo, 302);
}

function sameHex(left: string, right: string): boolean {
  if (left.length !== right.length) return false;
  let mismatch = 0;
  for (let i = 0; i < left.length; i += 1) {
    mismatch |= left.charCodeAt(i) ^ right.charCodeAt(i);
  }
  return mismatch === 0;
}

async function runtimeStub(env: Env): Promise<any> {
  const id = env.RUNTIME.idFromName("primary");
  return env.RUNTIME.get(id);
}

async function runtimeFetch(env: Env, path: string, init?: RequestInit): Promise<any> {
  const stub = await runtimeStub(env);
  const response = await stub.fetch(`https://runtime.internal${path}`, init);
  return response.json();
}

const mcpApi = {
  async fetch(request: Request, env: Env, ctx: any): Promise<Response> {
    if (!ctx?.auth?.scope?.includes("mcp:use")) {
      return new Response("insufficient scope", { status: 403 });
    }
    if (request.method !== "POST") {
      return new Response("method not allowed", { status: 405 });
    }

    const declaredLength = Number(request.headers.get("content-length") ?? "0");
    if (declaredLength > MAX_BODY_BYTES) {
      return new Response("request too large", { status: 413 });
    }
    const raw = await request.text();
    if (raw.length > MAX_BODY_BYTES) {
      return new Response("request too large", { status: 413 });
    }

    let rpc: any;
    try {
      rpc = JSON.parse(raw);
    } catch {
      return jsonRpcError(null, -32700, "parse error");
    }

    const id = (rpc.id ?? null) as Json;
    if (rpc.method === "notifications/initialized") {
      return new Response(null, { status: 202 });
    }
    if (rpc.method === "initialize") {
      return jsonRpc(id, {
        protocolVersion: MCP_PROTOCOL_VERSION,
        capabilities: { tools: {} },
        serverInfo: { name: "okx-cloudflare-mcp", version: "0.1.0" },
      });
    }
    if (rpc.method === "ping") {
      return jsonRpc(id, {});
    }
    if (rpc.method === "tools/list") {
      return jsonRpc(id, {
        tools: [
          {
            name: "runtime_status",
            description: "Check the authenticated Windows direct-transport session and freshness.",
            inputSchema: { type: "object", properties: {}, additionalProperties: false },
          },
          {
            name: "find_instruments",
            description: "Find OKX instruments through the Windows product runtime.",
            inputSchema: {
              type: "object",
              properties: {
                asset: { type: "string", minLength: 2, maxLength: 16 },
                settle_currency: { type: "string", minLength: 2, maxLength: 16 },
                instrument_type: { type: "string", enum: ["SWAP", "FUTURES"] },
              },
              required: ["asset"],
              additionalProperties: false,
            },
          },
          {
            name: "market_overview",
            description: "Get a bounded current market overview through the Windows product runtime.",
            inputSchema: {
              type: "object",
              properties: { instrument: { type: "string", minLength: 3, maxLength: 64 } },
              required: ["instrument"],
              additionalProperties: false,
            },
          },
          {
            name: "trading_capabilities",
            description: "Get read-only authenticated OKX account and trading capabilities for one instrument through the Windows product runtime.",
            inputSchema: {
              type: "object",
              properties: {
                instrument: { type: "string", minLength: 3, maxLength: 64 },
                margin_mode: { type: "string", enum: ["cross", "isolated"] },
              },
              required: ["instrument", "margin_mode"],
              additionalProperties: false,
            },
          },
        ],
      });
    }
    if (rpc.method !== "tools/call" || !isObject(rpc.params)) {
      return jsonRpcError(id, -32601, "method not found");
    }

    const name = rpc.params.name;
    const args = isObject(rpc.params.arguments) ? rpc.params.arguments : {};
    try {
      if (name === "runtime_status") {
        return jsonRpc(id, toolResult(await runtimeFetch(env, "/status?probe=1")));
      }
      if (name === "find_instruments") {
        if (!validCode(args.asset)) return jsonRpcError(id, -32602, "invalid asset");
        if (args.settle_currency !== undefined && !validCode(args.settle_currency)) {
          return jsonRpcError(id, -32602, "invalid settle_currency");
        }
        if (args.instrument_type !== undefined && !["SWAP", "FUTURES"].includes(String(args.instrument_type))) {
          return jsonRpcError(id, -32602, "invalid instrument_type");
        }
        const agentRequest = {
          schema: "okx.agent.request/v1",
          request_id: requestId(),
          operation: {
            type: "find_instruments",
            asset: args.asset,
            settle_currency: args.settle_currency ?? null,
            instrument_type: args.instrument_type ?? null,
          },
        };
        return jsonRpc(id, toolResult(await dispatchRuntime(env, agentRequest)));
      }
      if (name === "market_overview") {
        if (!validInstrument(args.instrument)) {
          return jsonRpcError(id, -32602, "invalid instrument");
        }
        const agentRequest = {
          schema: "okx.agent.request/v1",
          request_id: requestId(),
          operation: { type: "market_overview", instrument: args.instrument },
        };
        return jsonRpc(id, toolResult(await dispatchRuntime(env, agentRequest)));
      }
      if (name === "trading_capabilities") {
        if (!validInstrument(args.instrument)) {
          return jsonRpcError(id, -32602, "invalid instrument");
        }
        if (!["cross", "isolated"].includes(String(args.margin_mode))) {
          return jsonRpcError(id, -32602, "invalid margin_mode");
        }
        const agentRequest = {
          schema: "okx.agent.request/v1",
          request_id: requestId(),
          operation: {
            type: "trading_capabilities",
            instrument: args.instrument,
            margin_mode: args.margin_mode,
          },
        };
        return jsonRpc(id, toolResult(await dispatchRuntime(env, agentRequest)));
      }
      return jsonRpcError(id, -32602, "unknown tool");
    } catch (error) {
      const message = error instanceof Error ? error.message : "internal transport failure";
      return jsonRpc(id, toolResult(transportFailure(message)));
    }
  },
};

async function dispatchRuntime(env: Env, request: Record<string, unknown>): Promise<Json> {
  return runtimeFetch(env, "/dispatch", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(request),
  });
}

const defaultHandler = {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);

    if (url.pathname === "/health") {
      return Response.json({ status: "ok", service: "okx-cloudflare-mcp" });
    }

    if (url.pathname === "/runtime") {
      const stub = await runtimeStub(env);
      return stub.fetch(request);
    }

    if (url.pathname !== "/authorize") {
      return new Response("not found", { status: 404 });
    }

    const oauthUrl = authorizationRequestUrl(url);
    let authRequest: any;
    try {
      authRequest = await env.OAUTH_PROVIDER.parseAuthRequest(new Request(oauthUrl.toString()));
    } catch {
      return new Response("invalid authorization request", { status: 400 });
    }

    if (request.method === "POST") {
      const wantsJson = request.headers.get("accept")?.includes("application/json") ?? false;
      const form = await request.formData();
      const csrf = String(form.get("csrf") ?? "");
      if (!(await verifyAuthorizationCsrf(env, oauthUrl, csrf))) {
        if (wantsJson) {
          return Response.json(
            { status: "FAILED", reason: "INVALID_CSRF" },
            { status: 400, headers: { "cache-control": "no-store" } },
          );
        }
        return new Response("invalid authorization csrf", {
          status: 400,
          headers: { "cache-control": "no-store" },
        });
      }

      const supplied = String(form.get("secret") ?? "");
      const [suppliedHash, expectedHash] = await Promise.all([
        sha256Hex(supplied),
        sha256Hex(env.MCP_OWNER_SECRET),
      ]);
      if (!sameHex(suppliedHash, expectedHash)) {
        if (wantsJson) {
          return Response.json(
            { status: "FAILED", reason: "INVALID_OWNER_SECRET" },
            { headers: { "cache-control": "no-store" } },
          );
        }
        return authorizationPage(oauthUrl, authRequest, true, env);
      }

      const approval = await issueAuthorizationApproval(env, oauthUrl);
      const continueUrl = new URL(oauthUrl.toString());
      continueUrl.searchParams.set("approval", approval);
      if (wantsJson) {
        return Response.json(
          { status: "PASS", continue_url: continueUrl.pathname + continueUrl.search },
          { headers: { "cache-control": "no-store" } },
        );
      }
      return Response.redirect(continueUrl.toString(), 303);
    }

    if (request.method === "GET") {
      const approval = url.searchParams.get("approval");
      if (approval !== null) {
        if (!(await verifyAuthorizationApproval(env, oauthUrl, approval))) {
          return new Response("invalid or expired authorization approval", {
            status: 400,
            headers: { "cache-control": "no-store" },
          });
        }
        return completeOwnerAuthorization(env, authRequest);
      }
      return authorizationPage(oauthUrl, authRequest, false, env);
    }
    return new Response("method not allowed", { status: 405 });
  },
};

async function authorizationPage(url: URL, authRequest: any, invalid: boolean, env: Env): Promise<Response> {
  const action = escapeHtml(url.pathname + url.search);
  const csrf = escapeHtml(await issueAuthorizationCsrf(env, url));
  const scopes = Array.isArray(authRequest.scope) ? authRequest.scope.join(" ") : "";
  const scriptNonce = crypto.randomUUID().replaceAll("-", "");
  const body = `<!doctype html>
<meta charset="utf-8">
<title>Authorize OKX MCP</title>
<h1>Authorize OKX Cloudflare MCP</h1>
<p>Client requests access to the Windows OKX read transport.</p>
<p>Requested scope: <code>${escapeHtml(scopes)}</code></p>
<p>The owner secret is sent only in an HTTPS POST body. OAuth completion then continues with a short-lived signed GET approval, matching the proven ChatGPT flow.</p>
${invalid ? '<p id="auth-status">Invalid owner secret.</p>' : '<p id="auth-status" aria-live="polite"></p>'}
<form id="owner-auth" method="post" action="${action}">
<input type="hidden" name="csrf" value="${csrf}">
<label>Owner secret <input type="password" name="secret" autocomplete="current-password" required></label>
<button id="authorize-button" type="submit">Authorize</button>
</form>
<script nonce="${scriptNonce}">
(() => {
  const form = document.getElementById("owner-auth");
  const button = document.getElementById("authorize-button");
  const status = document.getElementById("auth-status");
  if (!(form instanceof HTMLFormElement) || !(button instanceof HTMLButtonElement) || !(status instanceof HTMLElement)) return;

  form.addEventListener("submit", async (event) => {
    event.preventDefault();
    button.disabled = true;
    status.textContent = "Authorizing…";
    try {
      const response = await fetch(form.action, {
        method: "POST",
        body: new FormData(form),
        headers: { "accept": "application/json" },
        credentials: "same-origin",
        cache: "no-store",
      });
      const result = await response.json();
      if (response.ok && result?.status === "PASS" && typeof result.continue_url === "string") {
        const link = document.createElement("a");
        link.href = result.continue_url;
        link.textContent = "Authorize and continue";
        status.replaceChildren(link);
        link.click();
        return;
      }
      status.textContent = result?.reason === "INVALID_OWNER_SECRET"
        ? "Invalid owner secret."
        : "Authorization request failed. Try again.";
    } catch {
      status.textContent = "Embedded authorization transport failed; retrying with browser navigation…";
      HTMLFormElement.prototype.submit.call(form);
      return;
    } finally {
      button.disabled = false;
    }
  });
})();
</script>`;
  return new Response(body, {
    headers: {
      "content-type": "text/html; charset=utf-8",
      "cache-control": "no-store",
      "referrer-policy": "no-referrer",
      "content-security-policy": `default-src 'none'; script-src 'nonce-${scriptNonce}'; connect-src 'self'; style-src 'unsafe-inline'; form-action 'self'; base-uri 'none'; frame-ancestors 'none'`,
    },
  });
}

function escapeHtml(value: string): string {
  return value.replace(/[&<>"']/g, (char) => ({
    "&": "&amp;",
    "<": "&lt;",
    ">": "&gt;",
    '"': "&quot;",
    "'": "&#39;",
  })[char] ?? char);
}

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

export default new OAuthProvider<Env>({
  apiRoute: "/mcp",
  apiHandler: mcpApi,
  defaultHandler,
  authorizeEndpoint: "/authorize",
  tokenEndpoint: "/token",
  clientRegistrationEndpoint: "/register",
  scopesSupported: ["mcp:use"],
  resourceMetadata: {
    resource: `${PUBLIC_ORIGIN}/mcp`,
    authorization_servers: [PUBLIC_ORIGIN],
  },
  requiredScopes: ["mcp:use"],
});
