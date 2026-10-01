import {
  MAX_BODY_BYTES,
  MCP_PROTOCOL_VERSION,
  MCP_SERVER_VERSION,
  TOOL_CONTRACT_VERSION,
  type Env,
  type Json,
  runtimeFetch,
  transportFailure,
} from "./shared";

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

function normalizeUpper(value: unknown): string | null {
  return typeof value === "string" ? value.trim().toUpperCase() : null;
}

function normalizeLower(value: unknown): string | null {
  return typeof value === "string" ? value.trim().toLowerCase() : null;
}

export const mcpApi = {
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
        serverInfo: { name: "okx-cloudflare-mcp", version: MCP_SERVER_VERSION },
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
            description: "Find OKX instruments through the Windows product runtime. Asset/currency/type codes are case-insensitive at the MCP boundary.",
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
            description: "Get a bounded current market overview through the Windows product runtime. Instrument codes are normalized to uppercase.",
            inputSchema: {
              type: "object",
              properties: { instrument: { type: "string", minLength: 3, maxLength: 64 } },
              required: ["instrument"],
              additionalProperties: false,
            },
          },
          {
            name: "market_research",
            description: "Get one bounded multi-instrument market research result computed by the Windows runtime for 2 to 8 instruments. Instrument codes are normalized to uppercase.",
            inputSchema: {
              type: "object",
              properties: {
                instruments: {
                  type: "array",
                  minItems: 2,
                  maxItems: 8,
                  uniqueItems: true,
                  items: { type: "string", minLength: 3, maxLength: 64 },
                },
                bar: {
                  type: "string",
                  enum: ["1s","1m","3m","5m","15m","30m","1H","2H","4H","6H","12H","1D","2D","3D","1W","1M","3M","6Hutc","12Hutc","1Dutc","2Dutc","3Dutc","1Wutc","1Mutc","3Mutc"],
                },
                limit: { type: "integer", minimum: 1, maximum: 100 },
              },
              required: ["instruments", "bar"],
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
        const status = await runtimeFetch(env, "/status?probe=1");
        return jsonRpc(id, toolResult({
          ...status,
          mcp_server_version: MCP_SERVER_VERSION,
          tool_contract: TOOL_CONTRACT_VERSION,
        }));
      }
      if (name === "find_instruments") {
        const asset = normalizeUpper(args.asset);
        const settleCurrency = args.settle_currency === undefined ? undefined : normalizeUpper(args.settle_currency);
        const instrumentType = args.instrument_type === undefined ? undefined : normalizeUpper(args.instrument_type);
        if (!validCode(asset)) return jsonRpcError(id, -32602, "invalid asset");
        if (settleCurrency !== undefined && !validCode(settleCurrency)) {
          return jsonRpcError(id, -32602, "invalid settle_currency");
        }
        if (instrumentType !== undefined && !["SWAP", "FUTURES"].includes(instrumentType)) {
          return jsonRpcError(id, -32602, "invalid instrument_type");
        }
        const agentRequest = {
          schema: "okx.agent.request/v1",
          request_id: requestId(),
          operation: {
            type: "find_instruments",
            asset,
            settle_currency: settleCurrency ?? null,
            instrument_type: instrumentType ?? null,
          },
        };
        return jsonRpc(id, toolResult(await dispatchRuntime(env, agentRequest)));
      }
      if (name === "market_overview") {
        const instrument = normalizeUpper(args.instrument);
        if (!validInstrument(instrument)) {
          return jsonRpcError(id, -32602, "invalid instrument");
        }
        const agentRequest = {
          schema: "okx.agent.request/v1",
          request_id: requestId(),
          operation: { type: "market_overview", instrument },
        };
        return jsonRpc(id, toolResult(await dispatchRuntime(env, agentRequest)));
      }
      if (name === "market_research") {
        if (!Array.isArray(args.instruments) || args.instruments.length < 2 || args.instruments.length > 8) {
          return jsonRpcError(id, -32602, "invalid instruments");
        }
        const instruments = (args.instruments as unknown[]).map(normalizeUpper);
        if (instruments.some((value) => !validInstrument(value))) {
          return jsonRpcError(id, -32602, "invalid instruments");
        }
        const normalizedInstruments = instruments as string[];
        if (new Set(normalizedInstruments).size !== normalizedInstruments.length) {
          return jsonRpcError(id, -32602, "invalid instruments");
        }
        const bar = typeof args.bar === "string" ? args.bar.trim() : "";
        const validBars = ["1s","1m","3m","5m","15m","30m","1H","2H","4H","6H","12H","1D","2D","3D","1W","1M","3M","6Hutc","12Hutc","1Dutc","2Dutc","3Dutc","1Wutc","1Mutc","3Mutc"];
        if (!validBars.includes(bar)) {
          return jsonRpcError(id, -32602, "invalid bar");
        }
        if (args.limit !== undefined && (!Number.isInteger(args.limit) || Number(args.limit) < 1 || Number(args.limit) > 100)) {
          return jsonRpcError(id, -32602, "invalid limit");
        }
        const agentRequest = {
          schema: "okx.agent.request/v1",
          request_id: requestId(),
          operation: {
            type: "market_research",
            instruments: normalizedInstruments,
            bar,
            limit: args.limit ?? null,
          },
        };
        return jsonRpc(id, toolResult(await dispatchRuntime(env, agentRequest)));
      }
      if (name === "trading_capabilities") {
        const instrument = normalizeUpper(args.instrument);
        const marginMode = normalizeLower(args.margin_mode);
        if (!validInstrument(instrument)) {
          return jsonRpcError(id, -32602, "invalid instrument");
        }
        if (!["cross", "isolated"].includes(marginMode ?? "")) {
          return jsonRpcError(id, -32602, "invalid margin_mode");
        }
        const agentRequest = {
          schema: "okx.agent.request/v1",
          request_id: requestId(),
          operation: {
            type: "trading_capabilities",
            instrument,
            margin_mode: marginMode,
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
