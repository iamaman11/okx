import {
  dispatchRuntime,
  type Env,
  isObject,
  type Json,
  jsonRpc,
  jsonRpcError,
  MAX_BODY_BYTES,
  MCP_PROTOCOL_VERSION,
  MCP_SERVER_VERSION,
  normalizeCode,
  normalizeInstrument,
  requestId,
  runtimeFetch,
  TOOL_CONTRACT_VERSION,
  toolResult,
  transportFailure,
} from "./shared";

const VALID_BARS = new Set([
  "1s","1m","3m","5m","15m","30m","1H","2H","4H","6H","12H","1D","2D","3D","1W","1M","3M",
  "6Hutc","12Hutc","1Dutc","2Dutc","3Dutc","1Wutc","1Mutc","3Mutc",
]);

const CODE_PATTERN = "^[A-Za-z0-9_-]+$";

function contractStatus(value: unknown): Json {
  if (!isObject(value)) return transportFailure("INVALID_RUNTIME_STATUS", false);
  return {
    ...(value as Record<string, Json>),
    tool_contract_version: TOOL_CONTRACT_VERSION,
  };
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
        serverInfo: {
          name: "okx-cloudflare-mcp",
          version: MCP_SERVER_VERSION,
          toolContractVersion: TOOL_CONTRACT_VERSION,
        },
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
            description: "Check the authenticated Windows direct-transport session, freshness, and MCP tool-contract version.",
            inputSchema: { type: "object", properties: {}, additionalProperties: false },
          },
          {
            name: "find_instruments",
            description: "Find OKX derivatives through the Windows ReferenceRegistry. AgentResponse.quality describes reference-readiness, not live market freshness; use market_overview for market freshness.",
            inputSchema: {
              type: "object",
              properties: {
                asset: { type: "string", minLength: 2, maxLength: 16, pattern: CODE_PATTERN },
                settle_currency: { type: "string", minLength: 2, maxLength: 16, pattern: CODE_PATTERN },
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
              properties: {
                instrument: { type: "string", minLength: 3, maxLength: 64, pattern: CODE_PATTERN },
              },
              required: ["instrument"],
              additionalProperties: false,
            },
          },
          {
            name: "market_research",
            description: "Get one compact multi-instrument market research result computed by the Windows runtime for 2 to 8 instruments.",
            inputSchema: {
              type: "object",
              properties: {
                instruments: {
                  type: "array",
                  minItems: 2,
                  maxItems: 8,
                  uniqueItems: true,
                  items: { type: "string", minLength: 3, maxLength: 64, pattern: CODE_PATTERN },
                },
                bar: { type: "string", enum: [...VALID_BARS] },
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
                instrument: { type: "string", minLength: 3, maxLength: 64, pattern: CODE_PATTERN },
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
        return jsonRpc(id, toolResult(contractStatus(await runtimeFetch(env, "/status?probe=1"))));
      }
      if (name === "find_instruments") {
        const asset = normalizeCode(args.asset, 2, 16);
        if (!asset) return jsonRpcError(id, -32602, "invalid asset");

        const settleCurrency = args.settle_currency === undefined
          ? null
          : normalizeCode(args.settle_currency, 2, 16);
        if (args.settle_currency !== undefined && !settleCurrency) {
          return jsonRpcError(id, -32602, "invalid settle_currency");
        }

        const instrumentType = args.instrument_type === undefined
          ? null
          : String(args.instrument_type).toUpperCase();
        if (instrumentType !== null && !["SWAP", "FUTURES"].includes(instrumentType)) {
          return jsonRpcError(id, -32602, "invalid instrument_type");
        }

        const agentRequest = {
          schema: "okx.agent.request/v1",
          request_id: requestId(),
          operation: {
            type: "find_instruments",
            asset,
            settle_currency: settleCurrency,
            instrument_type: instrumentType,
          },
        };
        return jsonRpc(id, toolResult(await dispatchRuntime(env, agentRequest)));
      }
      if (name === "market_overview") {
        const instrument = normalizeInstrument(args.instrument);
        if (!instrument) return jsonRpcError(id, -32602, "invalid instrument");

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
        const instruments = (args.instruments as unknown[]).map(normalizeInstrument);
        if (
          instruments.some((value) => value === null) ||
          new Set(instruments).size !== instruments.length
        ) {
          return jsonRpcError(id, -32602, "invalid instruments");
        }
        const normalizedInstruments = instruments as string[];
        if (!VALID_BARS.has(String(args.bar))) {
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
            bar: args.bar,
            limit: args.limit ?? null,
          },
        };
        return jsonRpc(id, toolResult(await dispatchRuntime(env, agentRequest)));
      }
      if (name === "trading_capabilities") {
        const instrument = normalizeInstrument(args.instrument);
        if (!instrument) return jsonRpcError(id, -32602, "invalid instrument");
        const marginMode = String(args.margin_mode).toLowerCase();
        if (!["cross", "isolated"].includes(marginMode)) {
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
