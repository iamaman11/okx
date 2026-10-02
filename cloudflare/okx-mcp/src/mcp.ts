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
const QUERY_CATALOG_PATTERN = /^[A-Za-z0-9._\/-]{1,64}$/;
const QUERY_FIELDS = new Set([
  "instrument_id",
  "instrument_type",
  "settle_currency",
  "state",
  "last",
  "open_24h",
  "volume_24h",
  "volume_currency_24h",
  "exchange_timestamp_ms",
  "return_24h_pct",
]);

function hasOnlyKeys(value: Record<string, unknown>, allowed: readonly string[]): boolean {
  const allowedKeys = new Set(allowed);
  return Object.keys(value).every((key) => allowedKeys.has(key));
}

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
            name: "query_capabilities",
            description: "Get the versioned bounded analytical query catalog, supported fields/metrics/operators, and hard limits.",
            inputSchema: { type: "object", properties: {}, additionalProperties: false },
          },
          {
            name: "query",
            description: "Execute one bounded read-only market analytical plan through the Windows runtime. This is not SQL, code execution, or a raw OKX RPC.",
            inputSchema: {
              type: "object",
              properties: {
                plan: {
                  type: "object",
                  properties: {
                    catalog_version: { type: "string", minLength: 1, maxLength: 64 },
                    universe: {
                      type: "object",
                      properties: {
                        instrument_types: {
                          type: "array",
                          minItems: 1,
                          maxItems: 2,
                          uniqueItems: true,
                          items: { type: "string", enum: ["SWAP", "FUTURES"] },
                        },
                        settle_currency: { type: "string", minLength: 2, maxLength: 16, pattern: CODE_PATTERN },
                        state: { type: "string", enum: ["live"] },
                      },
                      required: ["instrument_types"],
                      additionalProperties: false,
                    },
                    select: {
                      type: "array",
                      minItems: 1,
                      maxItems: 10,
                      uniqueItems: true,
                      items: { type: "string", enum: [...QUERY_FIELDS] },
                    },
                    metric: { type: "string", enum: ["return_24h_pct"] },
                    sort: {
                      type: "object",
                      properties: {
                        key: { type: "string", enum: ["return_24h_pct"] },
                        direction: { type: "string", enum: ["asc", "desc"] },
                      },
                      required: ["key", "direction"],
                      additionalProperties: false,
                    },
                    limit: { type: "integer", minimum: 1, maximum: 25 },
                  },
                  required: ["catalog_version", "universe", "select", "limit"],
                  additionalProperties: false,
                },
              },
              required: ["plan"],
              additionalProperties: false,
            },
          },
          {
            name: "account_summary",
            description: "Get a bounded read-only OKX account and ledger truth summary, including history coverage and durable execution-ledger reconciliation.",
            inputSchema: { type: "object", properties: {}, additionalProperties: false },
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
      if (name === "query_capabilities") {
        const agentRequest = {
          schema: "okx.agent.request/v1",
          request_id: requestId(),
          operation: { type: "query_capabilities" },
        };
        return jsonRpc(id, toolResult(await dispatchRuntime(env, agentRequest)));
      }
      if (name === "query") {
        if (!hasOnlyKeys(args, ["plan"]) || !isObject(args.plan)) {
          return jsonRpcError(id, -32602, "invalid query plan");
        }
        const plan = args.plan;
        if (!hasOnlyKeys(plan, ["catalog_version", "universe", "select", "metric", "sort", "limit"])) {
          return jsonRpcError(id, -32602, "unsupported query plan field");
        }
        if (typeof plan.catalog_version !== "string" || !QUERY_CATALOG_PATTERN.test(plan.catalog_version)) {
          return jsonRpcError(id, -32602, "invalid catalog_version");
        }
        if (!isObject(plan.universe)) {
          return jsonRpcError(id, -32602, "invalid universe");
        }
        const universe = plan.universe;
        if (!hasOnlyKeys(universe, ["instrument_types", "settle_currency", "state"])) {
          return jsonRpcError(id, -32602, "unsupported universe field");
        }
        if (
          !Array.isArray(universe.instrument_types) ||
          universe.instrument_types.length < 1 ||
          universe.instrument_types.length > 2
        ) {
          return jsonRpcError(id, -32602, "invalid instrument_types");
        }
        const instrumentTypes = universe.instrument_types.map((value: unknown) => String(value).toUpperCase());
        if (
          instrumentTypes.some((value: string) => !["SWAP", "FUTURES"].includes(value)) ||
          new Set(instrumentTypes).size !== instrumentTypes.length
        ) {
          return jsonRpcError(id, -32602, "invalid instrument_types");
        }
        const settleCurrency = universe.settle_currency === undefined
          ? null
          : normalizeCode(universe.settle_currency, 2, 16);
        if (universe.settle_currency !== undefined && !settleCurrency) {
          return jsonRpcError(id, -32602, "invalid settle_currency");
        }
        const state = universe.state === undefined ? null : String(universe.state).toLowerCase();
        if (state !== null && state !== "live") {
          return jsonRpcError(id, -32602, "invalid state");
        }
        if (
          !Array.isArray(plan.select) ||
          plan.select.length < 1 ||
          plan.select.length > 10 ||
          plan.select.some((field: unknown) => typeof field !== "string" || !QUERY_FIELDS.has(field)) ||
          new Set(plan.select).size !== plan.select.length
        ) {
          return jsonRpcError(id, -32602, "invalid select");
        }
        const metric = plan.metric === undefined ? null : String(plan.metric);
        if (metric !== null && metric !== "return_24h_pct") {
          return jsonRpcError(id, -32602, "invalid metric");
        }
        let sort: { key: string; direction: string } | null = null;
        if (plan.sort !== undefined) {
          if (!isObject(plan.sort) || !hasOnlyKeys(plan.sort, ["key", "direction"])) {
            return jsonRpcError(id, -32602, "invalid sort");
          }
          const key = String(plan.sort.key);
          const direction = String(plan.sort.direction);
          if (key !== "return_24h_pct" || !["asc", "desc"].includes(direction)) {
            return jsonRpcError(id, -32602, "invalid sort");
          }
          sort = { key, direction };
        }
        if (!Number.isInteger(plan.limit) || Number(plan.limit) < 1 || Number(plan.limit) > 25) {
          return jsonRpcError(id, -32602, "invalid limit");
        }
        if (
          ((plan.select as unknown[]).includes("return_24h_pct") || sort !== null) &&
          metric !== "return_24h_pct"
        ) {
          return jsonRpcError(id, -32602, "return_24h_pct metric must be declared");
        }

        const agentRequest = {
          schema: "okx.agent.request/v1",
          request_id: requestId(),
          operation: {
            type: "query",
            plan: {
              catalog_version: plan.catalog_version,
              universe: {
                instrument_types: instrumentTypes,
                settle_currency: settleCurrency,
                state,
              },
              select: plan.select,
              metric,
              sort,
              limit: plan.limit,
            },
          },
        };
        return jsonRpc(id, toolResult(await dispatchRuntime(env, agentRequest)));
      }
      if (name === "account_summary") {
        const agentRequest = {
          schema: "okx.agent.request/v1",
          request_id: requestId(),
          operation: { type: "account_summary" },
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
