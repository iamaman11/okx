import type { OAuthHelpers } from "@cloudflare/workers-oauth-provider";

export const FRAME_SCHEMA = "okx.direct-transport.frame/v1";
export const MCP_PROTOCOL_VERSION = "2025-06-18";
export const MCP_SERVER_VERSION = "0.2.0";
export const TOOL_CONTRACT_VERSION = "okx.mcp.tools/v2";
export const MAX_BODY_BYTES = 64 * 1024;
export const MAX_INFLIGHT = 8;
export const PONG_DEADLINE_MS = 2_000;
export const ACK_DEADLINE_MS = 2_000;
export const RESPONSE_DEADLINE_MS = 20_000;
export const AUTH_CSRF_TTL_MS = 5 * 60 * 1000;
export const AUTH_APPROVAL_TTL_MS = 2 * 60 * 1000;
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

async function sha256Hex(value: string): Promise<string> {
  const bytes = new TextEncoder().encode(value);
  const digest = await crypto.subtle.digest("SHA-256", bytes);
  return [...new Uint8Array(digest)].map((byte) => byte.toString(16).padStart(2, "0")).join("");
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

export function transportFailure(reason: string, retryable = true): Json {
  return {
    schema: "okx.direct-transport.result/v1",
    status: "FAILED",
    reason,
    retryable,
  };
}
