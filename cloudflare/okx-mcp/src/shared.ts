import type { OAuthHelpers } from "@cloudflare/workers-oauth-provider";

export const FRAME_SCHEMA = "okx.direct-transport.frame/v1";
export const MCP_PROTOCOL_VERSION = "2025-06-18";
export const MCP_SERVER_VERSION = "0.6.0";
export const TOOL_CONTRACT_VERSION = "okx.mcp.tools/2026-10-05.6";
export const MAX_BODY_BYTES = 64 * 1024;
export const MAX_INFLIGHT = 8;
export const PONG_DEADLINE_MS = 2_000;
export const ACK_DEADLINE_MS = 2_000;
export const RESPONSE_DEADLINE_MS = 20_000;
export const RUNTIME_NAME = "windows-primary";
export const PUBLIC_ORIGIN = "https://okx-cloudflare-mcp.okx-794.workers.dev";

export type Json = null | boolean | number | string | Json[] | { [key: string]: Json };

export interface Env {
  OAUTH_KV: any;
  RUNTIME: any;
  MCP_OWNER_SECRET: string;
  RUNTIME_TOKEN_SHA256: string;
  OAUTH_PROVIDER: OAuthHelpers;
}

export interface SocketAttachment {
  runtimeId: string;
  sessionId: string;
  generation: number;
  hello: boolean;
  connectedAtMs: number;
  lastPongAtMs: number | null;
}

export interface Pending {
  generation: number;
  resolve: (value: any) => void;
  reject: (error: Error) => void;
  timer: ReturnType<typeof setTimeout>;
}

export function jsonRpc(id: Json, result: Json): Response {
  return Response.json({ jsonrpc: "2.0", id, result });
}

export function jsonRpcError(id: Json, code: number, message: string): Response {
  return Response.json({ jsonrpc: "2.0", id, error: { code, message } });
}

export function toolResult(value: Json): Json {
  return {
    content: [{ type: "text", text: JSON.stringify(value) }],
    structuredContent: value,
  };
}

export function transportFailure(reason: string, retryable = true): Json {
  return {
    schema: "okx.direct-transport.result/v1",
    status: "FAILED",
    reason,
    retryable,
  };
}

export function requestId(): string {
  return `req_mcp_${crypto.randomUUID()}`;
}

export function isObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function normalizeToken(
  value: unknown,
  min: number,
  max: number,
): string | null {
  if (typeof value !== "string") return null;
  const normalized = value.trim().toUpperCase();
  if (
    normalized.length < min ||
    normalized.length > max ||
    !/^[A-Z0-9_-]+$/.test(normalized)
  ) {
    return null;
  }
  return normalized;
}

export function normalizeCode(value: unknown, min = 2, max = 64): string | null {
  return normalizeToken(value, min, max);
}

export function normalizeInstrument(value: unknown): string | null {
  return normalizeToken(value, 3, 64);
}

export async function sha256Hex(value: string): Promise<string> {
  const bytes = new TextEncoder().encode(value);
  const digest = await crypto.subtle.digest("SHA-256", bytes);
  return [...new Uint8Array(digest)].map((byte) => byte.toString(16).padStart(2, "0")).join("");
}

export function sameHex(left: string, right: string): boolean {
  if (left.length !== right.length) return false;
  let mismatch = 0;
  for (let i = 0; i < left.length; i += 1) {
    mismatch |= left.charCodeAt(i) ^ right.charCodeAt(i);
  }
  return mismatch === 0;
}

export async function runtimeStub(env: Env): Promise<any> {
  const id = env.RUNTIME.idFromName("primary");
  return env.RUNTIME.get(id);
}

export async function runtimeFetch(env: Env, path: string, init?: RequestInit): Promise<any> {
  const stub = await runtimeStub(env);
  const response = await stub.fetch(`https://runtime.internal${path}`, init);
  return response.json();
}

export async function dispatchRuntime(
  env: Env,
  request: Record<string, unknown>,
): Promise<Json> {
  return runtimeFetch(env, "/dispatch", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(request),
  });
}
