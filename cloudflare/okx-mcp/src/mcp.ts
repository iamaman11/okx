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

const STATISTICAL_BARS = new Set([
  "1m","3m","5m","15m","30m","1H","2H","4H","6H","12H","1D","2D","3D","1W",
  "6Hutc","12Hutc","1Dutc","2Dutc","3Dutc","1Wutc",
]);

const CODE_PATTERN = "^[A-Za-z0-9_-]+$";
const DECIMAL_PATTERN = "^[0-9]+(?:\\.[0-9]+)?$";
const VERSION_PATTERN = "^[A-Za-z0-9._/-]+$";
const QUERY_CATALOG_PATTERN = /^[A-Za-z0-9._\/-]{1,64}$/;
const QUERY_FIELDS = new Set([
  "instrument_id",
  "instrument_type",
  "settle_currency",
  "state",
  "last",
  "best_bid",
  "best_ask",
  "open_24h",
  "volume_24h",
  "volume_currency_24h",
  "exchange_timestamp_ms",
  "return_24h_pct",
  "spread_bps",
]);

function hasOnlyKeys(value: Record<string, unknown>, allowed: readonly string[]): boolean {
  const allowedKeys = new Set(allowed);
  return Object.keys(value).every((key) => allowedKeys.has(key));
}

function decimalText(value: unknown, positive: boolean): string | null {
  if (
    typeof value !== "string" ||
    value.length < 1 ||
    value.length > 64 ||
    !/^[0-9]+(?:\.[0-9]+)?$/.test(value)
  ) {
    return null;
  }
  if (positive && Number(value) <= 0) return null;
  return value;
}

function signedDecimalText(value: unknown): string | null {
  if (
    typeof value !== "string" ||
    value.length < 1 ||
    value.length > 64 ||
    !/^-?[0-9]+(?:\.[0-9]+)?$/.test(value)
  ) {
    return null;
  }
  return value;
}

function versionText(value: unknown): string | null {
  if (
    typeof value !== "string" ||
    value.length < 1 ||
    value.length > 64 ||
    !/^[A-Za-z0-9._\/-]+$/.test(value)
  ) {
    return null;
  }
  return value;
}

function instrumentList(value: unknown, max: number): string[] | null {
  if (!Array.isArray(value) || value.length > max) return null;
  const normalized = value.map(normalizeInstrument);
  if (normalized.some((item) => item === null) || new Set(normalized).size !== normalized.length) {
    return null;
  }
  return normalized as string[];
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
            name: "market_intelligence",
            description: "Get FRESH sequence-contiguous market microstructure and derivatives evidence for one instrument, including spread, depth, basis and modelled book-sweep impact.",
            inputSchema: {
              type: "object",
              properties: {
                instrument: { type: "string", minLength: 3, maxLength: 64, pattern: CODE_PATTERN },
                impact_contracts: { type: "string", minLength: 1, maxLength: 64, pattern: "^[0-9]+(?:\\.[0-9]+)?$" },
                depth_levels: { type: "integer", minimum: 1, maximum: 50 },
              },
              required: ["instrument", "impact_contracts", "depth_levels"],
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
                    metric: { type: "string", enum: ["return_24h_pct", "spread_bps"] },
                    sort: {
                      type: "object",
                      properties: {
                        key: { type: "string", enum: ["return_24h_pct", "spread_bps"] },
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
            name: "research_capabilities",
            description: "Get the versioned Stage-3 research catalog, bounded data scope, provenance guarantees, and transport budgets.",
            inputSchema: { type: "object", properties: {}, additionalProperties: false },
          },
          {
            name: "research",
            description: "Run one bounded Stage-3 research operation in the Windows runtime. Data inspection, deterministic replay, and Stage-3C checkpointed validation-dataset preparation share this coarse tool; bulk history and replay traces are never returned.",
            inputSchema: {
              type: "object",
              properties: {
                request: {
                  type: "object",
                  properties: {
                    action: { type: "string", enum: ["inspect_tier_a", "inspect_tier_b", "run_replay", "prepare_validation_dataset", "prepare_validation_split", "evaluate_validation_evidence", "evaluate_validation_robustness", "consume_final_oos"] },
                    catalog_version: { type: "string", const: "okx.research.catalog/2026-10-05.8" },
                    instrument: {
                      type: "string",
                      enum: ["BTC-USDT-SWAP", "ETH-USDT-SWAP", "DOGE-USDT-SWAP"],
                    },
                    bar: { type: "string", const: "1H" },
                    candle_limit: { type: "integer", minimum: 2, maximum: 100 },
                    funding_limit: { type: "integer", minimum: 1, maximum: 400 },
                    trade_limit: { type: "integer", minimum: 2, maximum: 100 },
                    replay_dataset_artifact_id: {
                      type: "string",
                      pattern: "^sha256:[0-9a-f]{64}$",
                    },
                    strategy: { type: "string", enum: ["no_trade", "close_momentum", "two_bar_momentum"] },
                    mechanics_provenance: {
                      type: "string",
                      enum: ["declared_counterfactual", "historical_observed"],
                    },
                    target_candle_count: { type: "integer", minimum: 240, maximum: 2400 },
                    checkpoint_artifact_id: {
                      type: "string",
                      pattern: "^sha256:[0-9a-f]{64}$",
                    },
                    parent_replay_dataset_artifact_id: {
                      type: "string",
                      pattern: "^sha256:[0-9a-f]{64}$",
                    },
                    train_candle_count: { type: "integer", minimum: 4, maximum: 2400 },
                    validation_candle_count: { type: "integer", minimum: 4, maximum: 2400 },
                    final_oos_candle_count: { type: "integer", minimum: 4, maximum: 2400 },
                    validation_spec_artifact_id: {
                      type: "string",
                      pattern: "^sha256:[0-9a-f]{64}$",
                    },
                    train_replay_dataset_artifact_id: {
                      type: "string",
                      pattern: "^sha256:[0-9a-f]{64}$",
                    },
                    train_experiment_result_artifact_id: {
                      type: "string",
                      pattern: "^sha256:[0-9a-f]{64}$",
                    },
                    validation_replay_dataset_artifact_id: {
                      type: "string",
                      pattern: "^sha256:[0-9a-f]{64}$",
                    },
                    validation_experiment_result_artifact_id: {
                      type: "string",
                      pattern: "^sha256:[0-9a-f]{64}$",
                    },
                    pre_holdout_evidence_artifact_id: {
                      type: "string",
                      pattern: "^sha256:[0-9a-f]{64}$",
                    },
                    validation_robustness_artifact_id: {
                      type: "string",
                      pattern: "^sha256:[0-9a-f]{64}$",
                    },
                  },
                  required: ["action", "catalog_version", "instrument"],
                  additionalProperties: false,
                },
              },
              required: ["request"],
              additionalProperties: false,
            },
          },
          {
            name: "account_summary",
            description: "Get a bounded read-only OKX account and ledger truth summary, including history coverage and durable execution-ledger reconciliation.",
            inputSchema: { type: "object", properties: {}, additionalProperties: false },
          },
          {
            name: "portfolio_risk",
            description: "Evaluate coherent read-only portfolio risk against an explicit versioned mandate and hard-risk policy, with OKX account-position-risk oracle comparison.",
            inputSchema: {
              type: "object",
              properties: {
                mandate: {
                  type: "object",
                  properties: {
                    version: { type: "string", minLength: 1, maxLength: 64, pattern: VERSION_PATTERN },
                    capital_base_usd: { type: "string", minLength: 1, maxLength: 64, pattern: DECIMAL_PATTERN },
                    decision_horizon_hours: { type: "integer", minimum: 1, maximum: 8760 },
                    benchmark: { type: "string", minLength: 1, maxLength: 64, pattern: VERSION_PATTERN },
                    allowed_instruments: {
                      type: "array", maxItems: 32, uniqueItems: true,
                      items: { type: "string", minLength: 3, maxLength: 64, pattern: CODE_PATTERN },
                    },
                    max_drawdown_ratio: { type: "string", minLength: 1, maxLength: 64, pattern: DECIMAL_PATTERN },
                    leverage_ceiling: { type: "string", minLength: 1, maxLength: 64, pattern: DECIMAL_PATTERN },
                    minimum_liquidity_notional_usd: { type: "string", minLength: 1, maxLength: 64, pattern: DECIMAL_PATTERN },
                    max_turnover_ratio: { type: "string", minLength: 1, maxLength: 64, pattern: DECIMAL_PATTERN },
                  },
                  required: [
                    "version","capital_base_usd","decision_horizon_hours","allowed_instruments",
                    "max_drawdown_ratio","leverage_ceiling","minimum_liquidity_notional_usd","max_turnover_ratio",
                  ],
                  additionalProperties: false,
                },
                policy: {
                  type: "object",
                  properties: {
                    version: { type: "string", minLength: 1, maxLength: 64, pattern: VERSION_PATTERN },
                    max_account_gross_notional_usd: { type: "string", minLength: 1, maxLength: 64, pattern: DECIMAL_PATTERN },
                    max_instrument_gross_notional_usd: { type: "string", minLength: 1, maxLength: 64, pattern: DECIMAL_PATTERN },
                    max_margin_utilization_ratio: { type: "string", minLength: 1, maxLength: 64, pattern: DECIMAL_PATTERN },
                    max_loss_per_trade_usd: { type: "string", minLength: 1, maxLength: 64, pattern: DECIMAL_PATTERN },
                    max_daily_realized_loss_usd: { type: "string", minLength: 1, maxLength: 64, pattern: DECIMAL_PATTERN },
                    max_drawdown_ratio: { type: "string", minLength: 1, maxLength: 64, pattern: DECIMAL_PATTERN },
                    max_leverage: { type: "string", minLength: 1, maxLength: 64, pattern: DECIMAL_PATTERN },
                    allowed_instruments: {
                      type: "array", maxItems: 32, uniqueItems: true,
                      items: { type: "string", minLength: 3, maxLength: 64, pattern: CODE_PATTERN },
                    },
                    minimum_quality: { type: "string", enum: ["fresh", "degraded"] },
                    degraded_mode: { type: "string", enum: ["reject", "allow_read_only"] },
                    correlated_clusters: {
                      type: "array", maxItems: 16,
                      items: {
                        type: "object",
                        properties: {
                          id: { type: "string", minLength: 1, maxLength: 64, pattern: VERSION_PATTERN },
                          instruments: {
                            type: "array", maxItems: 16, uniqueItems: true,
                            items: { type: "string", minLength: 3, maxLength: 64, pattern: CODE_PATTERN },
                          },
                          max_gross_notional_usd: { type: "string", minLength: 1, maxLength: 64, pattern: DECIMAL_PATTERN },
                        },
                        required: ["id","instruments","max_gross_notional_usd"],
                        additionalProperties: false,
                      },
                    },
                  },
                  required: [
                    "version","max_account_gross_notional_usd","max_instrument_gross_notional_usd",
                    "max_margin_utilization_ratio","max_loss_per_trade_usd","max_daily_realized_loss_usd",
                    "max_drawdown_ratio","max_leverage","allowed_instruments","minimum_quality",
                    "degraded_mode","correlated_clusters",
                  ],
                  additionalProperties: false,
                },
                candidate: {
                  type: "object",
                  properties: {
                    instrument: { type: "string", minLength: 3, maxLength: 64, pattern: CODE_PATTERN },
                    side: { type: "string", enum: ["long", "short"] },
                    notional_usd: { type: "string", minLength: 1, maxLength: 64, pattern: DECIMAL_PATTERN },
                    worst_case_loss_usd: { type: "string", minLength: 1, maxLength: 64, pattern: DECIMAL_PATTERN },
                    leverage: { type: "string", minLength: 1, maxLength: 64, pattern: DECIMAL_PATTERN },
                  },
                  required: ["instrument","side","notional_usd","worst_case_loss_usd","leverage"],
                  additionalProperties: false,
                },
                statistics: {
                  type: "object",
                  properties: {
                    bar: { type: "string", enum: [...STATISTICAL_BARS] },
                    limit: { type: "integer", minimum: 3, maximum: 100 },
                    parallel_scenario_move_ratio: {
                      type: "string",
                      minLength: 1,
                      maxLength: 64,
                      pattern: "^-?[0-9]+(?:\\.[0-9]+)?$",
                    },
                  },
                  required: ["bar","limit"],
                  additionalProperties: false,
                },
                virtual_portfolio: {
                  type: "object",
                  properties: {
                    collateral_usdt: {
                      type: "string", minLength: 1, maxLength: 64, pattern: DECIMAL_PATTERN,
                    },
                    positions: {
                      type: "array", minItems: 2, maxItems: 8,
                      items: {
                        type: "object",
                        properties: {
                          instrument: { type: "string", minLength: 3, maxLength: 64, pattern: CODE_PATTERN },
                          contracts: {
                            type: "string", minLength: 1, maxLength: 64, pattern: "^-?[0-9]+(?:\\.[0-9]+)?$",
                          },
                          average_price: {
                            type: "string", minLength: 1, maxLength: 64, pattern: DECIMAL_PATTERN,
                          },
                          leverage: {
                            type: "string", minLength: 1, maxLength: 64, pattern: DECIMAL_PATTERN,
                          },
                        },
                        required: ["instrument","contracts","average_price","leverage"],
                        additionalProperties: false,
                      },
                    },
                  },
                  required: ["collateral_usdt","positions"],
                  additionalProperties: false,
                },
              },
              required: ["mandate","policy"],
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
      if (name === "market_intelligence") {
        const instrument = normalizeInstrument(args.instrument);
        if (!instrument) return jsonRpcError(id, -32602, "invalid instrument");
        if (
          typeof args.impact_contracts !== "string" ||
          args.impact_contracts.length < 1 ||
          args.impact_contracts.length > 64 ||
          !/^[0-9]+(?:\.[0-9]+)?$/.test(args.impact_contracts) ||
          Number(args.impact_contracts) <= 0
        ) {
          return jsonRpcError(id, -32602, "invalid impact_contracts");
        }
        if (!Number.isInteger(args.depth_levels) || Number(args.depth_levels) < 1 || Number(args.depth_levels) > 50) {
          return jsonRpcError(id, -32602, "invalid depth_levels");
        }

        const agentRequest = {
          schema: "okx.agent.request/v1",
          request_id: requestId(),
          operation: {
            type: "market_intelligence",
            instrument,
            impact_contracts: args.impact_contracts,
            depth_levels: args.depth_levels,
          },
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
      if (name === "research_capabilities") {
        const agentRequest = {
          schema: "okx.agent.request/v1",
          request_id: requestId(),
          operation: { type: "research_capabilities" },
        };
        return jsonRpc(id, toolResult(await dispatchRuntime(env, agentRequest)));
      }
      if (name === "research") {
        if (!hasOnlyKeys(args, ["request"]) || !isObject(args.request)) {
          return jsonRpcError(id, -32602, "invalid research request");
        }
        const research = args.request;
        if (
          !hasOnlyKeys(research, [
            "action",
            "catalog_version",
            "instrument",
            "bar",
            "candle_limit",
            "funding_limit",
            "trade_limit",
            "replay_dataset_artifact_id",
            "strategy",
            "mechanics_provenance",
            "target_candle_count",
            "checkpoint_artifact_id",
            "parent_replay_dataset_artifact_id",
            "train_candle_count",
            "validation_candle_count",
            "final_oos_candle_count",
            "validation_spec_artifact_id",
            "train_replay_dataset_artifact_id",
            "train_experiment_result_artifact_id",
            "validation_replay_dataset_artifact_id",
            "validation_experiment_result_artifact_id",
            "pre_holdout_evidence_artifact_id",
            "validation_robustness_artifact_id",
          ])
        ) {
          return jsonRpcError(id, -32602, "unsupported research request field");
        }
        if (!["inspect_tier_a", "inspect_tier_b", "run_replay", "prepare_validation_dataset", "prepare_validation_split", "evaluate_validation_evidence", "evaluate_validation_robustness", "consume_final_oos"].includes(String(research.action))) {
          return jsonRpcError(id, -32602, "invalid research action");
        }
        if (research.catalog_version !== "okx.research.catalog/2026-10-05.8") {
          return jsonRpcError(id, -32602, "invalid research catalog_version");
        }
        const instrument = normalizeInstrument(research.instrument);
        if (
          instrument === null ||
          !["BTC-USDT-SWAP", "ETH-USDT-SWAP", "DOGE-USDT-SWAP"].includes(instrument)
        ) {
          return jsonRpcError(id, -32602, "invalid Stage-3 instrument");
        }

        if (research.action === "inspect_tier_a") {
          if (
            research.trade_limit !== undefined ||
            research.replay_dataset_artifact_id !== undefined ||
            research.strategy !== undefined ||
            research.mechanics_provenance !== undefined ||
            research.target_candle_count !== undefined ||
            research.checkpoint_artifact_id !== undefined ||
            research.parent_replay_dataset_artifact_id !== undefined ||
            research.train_candle_count !== undefined ||
            research.validation_candle_count !== undefined ||
            research.final_oos_candle_count !== undefined ||
            research.validation_spec_artifact_id !== undefined ||
            research.train_replay_dataset_artifact_id !== undefined ||
            research.train_experiment_result_artifact_id !== undefined ||
            research.validation_replay_dataset_artifact_id !== undefined ||
            research.validation_experiment_result_artifact_id !== undefined ||
          research.pre_holdout_evidence_artifact_id !== undefined ||
            research.validation_robustness_artifact_id !== undefined ||
            research.bar !== "1H" ||
            !Number.isInteger(research.candle_limit) ||
            Number(research.candle_limit) < 2 ||
            Number(research.candle_limit) > 100 ||
            !Number.isInteger(research.funding_limit) ||
            Number(research.funding_limit) < 1 ||
            Number(research.funding_limit) > 400
          ) {
            return jsonRpcError(id, -32602, "invalid Tier-A research request");
          }
          const agentRequest = {
            schema: "okx.agent.request/v1",
            request_id: requestId(),
            operation: {
              type: "research",
              request: {
                action: "inspect_tier_a",
                catalog_version: research.catalog_version,
                instrument,
                bar: "1H",
                candle_limit: research.candle_limit,
                funding_limit: research.funding_limit,
              },
            },
          };
          return jsonRpc(id, toolResult(await dispatchRuntime(env, agentRequest)));
        }

        if (research.action === "inspect_tier_b") {
          if (
            instrument !== "BTC-USDT-SWAP" ||
            research.bar !== undefined ||
            research.candle_limit !== undefined ||
            research.funding_limit !== undefined ||
            research.replay_dataset_artifact_id !== undefined ||
            research.strategy !== undefined ||
            research.mechanics_provenance !== undefined ||
            research.target_candle_count !== undefined ||
            research.checkpoint_artifact_id !== undefined ||
            research.parent_replay_dataset_artifact_id !== undefined ||
            research.train_candle_count !== undefined ||
            research.validation_candle_count !== undefined ||
            research.final_oos_candle_count !== undefined ||
            research.validation_spec_artifact_id !== undefined ||
            research.train_replay_dataset_artifact_id !== undefined ||
            research.train_experiment_result_artifact_id !== undefined ||
            research.validation_replay_dataset_artifact_id !== undefined ||
            research.validation_experiment_result_artifact_id !== undefined ||
            research.pre_holdout_evidence_artifact_id !== undefined ||
            research.validation_robustness_artifact_id !== undefined ||
            !Number.isInteger(research.trade_limit) ||
            Number(research.trade_limit) < 2 ||
            Number(research.trade_limit) > 100
          ) {
            return jsonRpcError(id, -32602, "invalid Tier-B research request");
          }
          const agentRequest = {
            schema: "okx.agent.request/v1",
            request_id: requestId(),
            operation: {
              type: "research",
              request: {
                action: "inspect_tier_b",
                catalog_version: research.catalog_version,
                instrument,
                trade_limit: research.trade_limit,
              },
            },
          };
          return jsonRpc(id, toolResult(await dispatchRuntime(env, agentRequest)));
        }

        if (research.action === "prepare_validation_dataset") {
          if (
            instrument !== "BTC-USDT-SWAP" ||
            research.bar !== "1H" ||
            research.candle_limit !== undefined ||
            research.funding_limit !== undefined ||
            research.trade_limit !== undefined ||
            research.replay_dataset_artifact_id !== undefined ||
            research.strategy !== undefined ||
            research.mechanics_provenance !== undefined ||
            research.parent_replay_dataset_artifact_id !== undefined ||
            research.train_candle_count !== undefined ||
            research.validation_candle_count !== undefined ||
            research.final_oos_candle_count !== undefined ||
            research.validation_spec_artifact_id !== undefined ||
            research.train_replay_dataset_artifact_id !== undefined ||
            research.train_experiment_result_artifact_id !== undefined ||
            research.validation_replay_dataset_artifact_id !== undefined ||
            research.validation_experiment_result_artifact_id !== undefined ||
            research.pre_holdout_evidence_artifact_id !== undefined ||
            research.validation_robustness_artifact_id !== undefined ||
            !Number.isInteger(research.target_candle_count) ||
            Number(research.target_candle_count) < 240 ||
            Number(research.target_candle_count) > 2400 ||
            (research.checkpoint_artifact_id !== undefined &&
              (typeof research.checkpoint_artifact_id !== "string" ||
                !/^sha256:[0-9a-f]{64}$/.test(research.checkpoint_artifact_id)))
          ) {
            return jsonRpcError(id, -32602, "invalid Stage-3C validation dataset request");
          }
          const agentRequest = {
            schema: "okx.agent.request/v1",
            request_id: requestId(),
            operation: {
              type: "research",
              request: {
                action: "prepare_validation_dataset",
                catalog_version: research.catalog_version,
                instrument,
                bar: "1H",
                target_candle_count: research.target_candle_count,
                checkpoint_artifact_id: research.checkpoint_artifact_id,
              },
            },
          };
          return jsonRpc(id, toolResult(await dispatchRuntime(env, agentRequest)));
        }

        if (research.action === "prepare_validation_split") {
          const splitCounts = [
            research.train_candle_count,
            research.validation_candle_count,
            research.final_oos_candle_count,
          ];
          if (
            instrument !== "BTC-USDT-SWAP" ||
            research.bar !== undefined ||
            research.candle_limit !== undefined ||
            research.funding_limit !== undefined ||
            research.trade_limit !== undefined ||
            research.replay_dataset_artifact_id !== undefined ||
            research.mechanics_provenance !== undefined ||
            research.target_candle_count !== undefined ||
            research.checkpoint_artifact_id !== undefined ||
            typeof research.parent_replay_dataset_artifact_id !== "string" ||
            !/^sha256:[0-9a-f]{64}$/.test(research.parent_replay_dataset_artifact_id) ||
            !["no_trade", "close_momentum", "two_bar_momentum"].includes(String(research.strategy)) ||
            splitCounts.some(
              (value) => !Number.isInteger(value) || Number(value) < 4 || Number(value) > 2400,
            ) ||
            splitCounts.reduce((sum, value) => sum + Number(value), 0) < 240 ||
            splitCounts.reduce((sum, value) => sum + Number(value), 0) > 2400 ||
            research.validation_spec_artifact_id !== undefined ||
            research.train_replay_dataset_artifact_id !== undefined ||
            research.train_experiment_result_artifact_id !== undefined ||
            research.validation_replay_dataset_artifact_id !== undefined ||
            research.validation_experiment_result_artifact_id !== undefined ||
            research.pre_holdout_evidence_artifact_id !== undefined
          ) {
            return jsonRpcError(id, -32602, "invalid Stage-3C validation split request");
          }
          const agentRequest = {
            schema: "okx.agent.request/v1",
            request_id: requestId(),
            operation: {
              type: "research",
              request: {
                action: "prepare_validation_split",
                catalog_version: research.catalog_version,
                instrument,
                parent_replay_dataset_artifact_id: research.parent_replay_dataset_artifact_id,
                strategy: research.strategy,
                train_candle_count: research.train_candle_count,
                validation_candle_count: research.validation_candle_count,
                final_oos_candle_count: research.final_oos_candle_count,
              },
            },
          };
          return jsonRpc(id, toolResult(await dispatchRuntime(env, agentRequest)));
        }

        if (research.action === "evaluate_validation_evidence") {
          const artifactFields = [
            research.validation_spec_artifact_id,
            research.train_replay_dataset_artifact_id,
            research.train_experiment_result_artifact_id,
            research.validation_replay_dataset_artifact_id,
            research.validation_experiment_result_artifact_id,
          ];
          if (
            instrument !== "BTC-USDT-SWAP" ||
            research.bar !== undefined ||
            research.candle_limit !== undefined ||
            research.funding_limit !== undefined ||
            research.trade_limit !== undefined ||
            research.replay_dataset_artifact_id !== undefined ||
            research.strategy !== undefined ||
            research.mechanics_provenance !== undefined ||
            research.target_candle_count !== undefined ||
            research.checkpoint_artifact_id !== undefined ||
            research.parent_replay_dataset_artifact_id !== undefined ||
            research.train_candle_count !== undefined ||
            research.validation_candle_count !== undefined ||
            research.final_oos_candle_count !== undefined ||
            research.pre_holdout_evidence_artifact_id !== undefined ||
            research.validation_robustness_artifact_id !== undefined ||
            artifactFields.some(
              (value) => typeof value !== "string" || !/^sha256:[0-9a-f]{64}$/.test(value),
            )
          ) {
            return jsonRpcError(id, -32602, "invalid Stage-3C validation evidence request");
          }
          const agentRequest = {
            schema: "okx.agent.request/v1",
            request_id: requestId(),
            operation: {
              type: "research",
              request: {
                action: "evaluate_validation_evidence",
                catalog_version: research.catalog_version,
                instrument,
                validation_spec_artifact_id: research.validation_spec_artifact_id,
                train_replay_dataset_artifact_id: research.train_replay_dataset_artifact_id,
                train_experiment_result_artifact_id: research.train_experiment_result_artifact_id,
                validation_replay_dataset_artifact_id: research.validation_replay_dataset_artifact_id,
                validation_experiment_result_artifact_id:
                  research.validation_experiment_result_artifact_id,
              },
            },
          };
          return jsonRpc(id, toolResult(await dispatchRuntime(env, agentRequest)));
        }

        if (research.action === "evaluate_validation_robustness") {
          const artifactFields = [
            research.validation_spec_artifact_id,
            research.pre_holdout_evidence_artifact_id,
            research.train_replay_dataset_artifact_id,
            research.validation_replay_dataset_artifact_id,
            research.validation_experiment_result_artifact_id,
          ];
          if (
            instrument !== "BTC-USDT-SWAP" ||
            research.bar !== undefined ||
            research.candle_limit !== undefined ||
            research.funding_limit !== undefined ||
            research.trade_limit !== undefined ||
            research.replay_dataset_artifact_id !== undefined ||
            research.strategy !== undefined ||
            research.mechanics_provenance !== undefined ||
            research.target_candle_count !== undefined ||
            research.checkpoint_artifact_id !== undefined ||
            research.parent_replay_dataset_artifact_id !== undefined ||
            research.train_candle_count !== undefined ||
            research.validation_candle_count !== undefined ||
            research.final_oos_candle_count !== undefined ||
            research.train_experiment_result_artifact_id !== undefined ||
            artifactFields.some(
              (value) => typeof value !== "string" || !/^sha256:[0-9a-f]{64}$/.test(value),
            )
          ) {
            return jsonRpcError(id, -32602, "invalid Stage-3C validation robustness request");
          }
          const agentRequest = {
            schema: "okx.agent.request/v1",
            request_id: requestId(),
            operation: {
              type: "research",
              request: {
                action: "evaluate_validation_robustness",
                catalog_version: research.catalog_version,
                instrument,
                validation_spec_artifact_id: research.validation_spec_artifact_id,
                pre_holdout_evidence_artifact_id: research.pre_holdout_evidence_artifact_id,
                train_replay_dataset_artifact_id: research.train_replay_dataset_artifact_id,
                validation_replay_dataset_artifact_id:
                  research.validation_replay_dataset_artifact_id,
                validation_experiment_result_artifact_id:
                  research.validation_experiment_result_artifact_id,
              },
            },
          };
          return jsonRpc(id, toolResult(await dispatchRuntime(env, agentRequest)));
        }

        if (research.action === "consume_final_oos") {
          const artifactFields = [
            research.validation_spec_artifact_id,
            research.validation_robustness_artifact_id,
          ];
          if (
            instrument !== "BTC-USDT-SWAP" ||
            research.bar !== undefined ||
            research.candle_limit !== undefined ||
            research.funding_limit !== undefined ||
            research.trade_limit !== undefined ||
            research.replay_dataset_artifact_id !== undefined ||
            research.strategy !== undefined ||
            research.mechanics_provenance !== undefined ||
            research.target_candle_count !== undefined ||
            research.checkpoint_artifact_id !== undefined ||
            research.parent_replay_dataset_artifact_id !== undefined ||
            research.train_candle_count !== undefined ||
            research.validation_candle_count !== undefined ||
            research.final_oos_candle_count !== undefined ||
            research.train_replay_dataset_artifact_id !== undefined ||
            research.train_experiment_result_artifact_id !== undefined ||
            research.validation_replay_dataset_artifact_id !== undefined ||
            research.validation_experiment_result_artifact_id !== undefined ||
            research.pre_holdout_evidence_artifact_id !== undefined ||
            artifactFields.some(
              (value) => typeof value !== "string" || !/^sha256:[0-9a-f]{64}$/.test(value),
            )
          ) {
            return jsonRpcError(id, -32602, "invalid Stage-3C final OOS consumption request");
          }
          const agentRequest = {
            schema: "okx.agent.request/v1",
            request_id: requestId(),
            operation: {
              type: "research",
              request: {
                action: "consume_final_oos",
                catalog_version: research.catalog_version,
                instrument,
                validation_spec_artifact_id: research.validation_spec_artifact_id,
                validation_robustness_artifact_id: research.validation_robustness_artifact_id,
              },
            },
          };
          return jsonRpc(id, toolResult(await dispatchRuntime(env, agentRequest)));
        }

        if (
          instrument !== "BTC-USDT-SWAP" ||
          research.bar !== undefined ||
          research.candle_limit !== undefined ||
          research.funding_limit !== undefined ||
          research.trade_limit !== undefined ||
          research.target_candle_count !== undefined ||
          research.checkpoint_artifact_id !== undefined ||
          research.parent_replay_dataset_artifact_id !== undefined ||
          research.train_candle_count !== undefined ||
          research.validation_candle_count !== undefined ||
          research.final_oos_candle_count !== undefined ||
          research.validation_spec_artifact_id !== undefined ||
          research.train_replay_dataset_artifact_id !== undefined ||
          research.train_experiment_result_artifact_id !== undefined ||
          research.validation_replay_dataset_artifact_id !== undefined ||
          research.validation_experiment_result_artifact_id !== undefined ||
            research.pre_holdout_evidence_artifact_id !== undefined ||
            research.validation_robustness_artifact_id !== undefined ||
          typeof research.replay_dataset_artifact_id !== "string" ||
          !/^sha256:[0-9a-f]{64}$/.test(research.replay_dataset_artifact_id) ||
          !["no_trade", "close_momentum", "two_bar_momentum"].includes(String(research.strategy)) ||
          !["declared_counterfactual", "historical_observed"].includes(
            String(research.mechanics_provenance),
          )
        ) {
          return jsonRpcError(id, -32602, "invalid Stage-3B replay request");
        }
        const agentRequest = {
          schema: "okx.agent.request/v1",
          request_id: requestId(),
          operation: {
            type: "research",
            request: {
              action: "run_replay",
              catalog_version: research.catalog_version,
              instrument,
              replay_dataset_artifact_id: research.replay_dataset_artifact_id,
              strategy: research.strategy,
              mechanics_provenance: research.mechanics_provenance,
            },
          },
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
        if (metric !== null && !["return_24h_pct", "spread_bps"].includes(metric)) {
          return jsonRpcError(id, -32602, "invalid metric");
        }
        let sort: { key: string; direction: string } | null = null;
        if (plan.sort !== undefined) {
          if (!isObject(plan.sort) || !hasOnlyKeys(plan.sort, ["key", "direction"])) {
            return jsonRpcError(id, -32602, "invalid sort");
          }
          const key = String(plan.sort.key);
          const direction = String(plan.sort.direction);
          if (!["return_24h_pct", "spread_bps"].includes(key) || !["asc", "desc"].includes(direction)) {
            return jsonRpcError(id, -32602, "invalid sort");
          }
          sort = { key, direction };
        }
        if (!Number.isInteger(plan.limit) || Number(plan.limit) < 1 || Number(plan.limit) > 25) {
          return jsonRpcError(id, -32602, "invalid limit");
        }
        const selectedDerived = (plan.select as unknown[]).filter(
          (field) => field === "return_24h_pct" || field === "spread_bps",
        ) as string[];
        if (selectedDerived.length > 1) {
          return jsonRpcError(id, -32602, "one derived metric per query plan");
        }
        const sortMetric = sort?.key ?? null;
        if (selectedDerived.length === 1 && sortMetric !== null && selectedDerived[0] !== sortMetric) {
          return jsonRpcError(id, -32602, "selected and sorted metrics must match");
        }
        const requiredMetric = selectedDerived[0] ?? sortMetric;
        if ((requiredMetric ?? null) !== metric) {
          return jsonRpcError(id, -32602, "declared metric must exactly match derived field/sort key");
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
      if (name === "portfolio_risk") {
        if (!hasOnlyKeys(args, ["mandate", "policy", "candidate", "statistics", "virtual_portfolio"]) || !isObject(args.mandate) || !isObject(args.policy)) {
          return jsonRpcError(id, -32602, "invalid portfolio_risk request");
        }
        const mandate = args.mandate;
        const policy = args.policy;
        if (!hasOnlyKeys(mandate, [
          "version","capital_base_usd","decision_horizon_hours","benchmark","allowed_instruments",
          "max_drawdown_ratio","leverage_ceiling","minimum_liquidity_notional_usd","max_turnover_ratio",
        ])) {
          return jsonRpcError(id, -32602, "invalid mandate");
        }
        const mandateVersion = versionText(mandate.version);
        const capitalBase = decimalText(mandate.capital_base_usd, true);
        const mandateAllowed = instrumentList(mandate.allowed_instruments, 32);
        const maxDrawdown = decimalText(mandate.max_drawdown_ratio, false);
        const leverageCeiling = decimalText(mandate.leverage_ceiling, true);
        const minLiquidity = decimalText(mandate.minimum_liquidity_notional_usd, false);
        const maxTurnover = decimalText(mandate.max_turnover_ratio, false);
        const benchmark = mandate.benchmark === undefined ? null : versionText(mandate.benchmark);
        if (
          !mandateVersion || !capitalBase || !mandateAllowed || !maxDrawdown || !leverageCeiling ||
          !minLiquidity || !maxTurnover ||
          (mandate.benchmark !== undefined && !benchmark) ||
          !Number.isInteger(mandate.decision_horizon_hours) ||
          Number(mandate.decision_horizon_hours) < 1 ||
          Number(mandate.decision_horizon_hours) > 8760
        ) {
          return jsonRpcError(id, -32602, "invalid mandate");
        }

        if (!hasOnlyKeys(policy, [
          "version","max_account_gross_notional_usd","max_instrument_gross_notional_usd",
          "max_margin_utilization_ratio","max_loss_per_trade_usd","max_daily_realized_loss_usd",
          "max_drawdown_ratio","max_leverage","allowed_instruments","minimum_quality",
          "degraded_mode","correlated_clusters",
        ])) {
          return jsonRpcError(id, -32602, "invalid policy");
        }
        const policyVersion = versionText(policy.version);
        const policyAllowed = instrumentList(policy.allowed_instruments, 32);
        const policyNumbers = [
          decimalText(policy.max_account_gross_notional_usd, false),
          decimalText(policy.max_instrument_gross_notional_usd, false),
          decimalText(policy.max_margin_utilization_ratio, false),
          decimalText(policy.max_loss_per_trade_usd, false),
          decimalText(policy.max_daily_realized_loss_usd, false),
          decimalText(policy.max_drawdown_ratio, false),
        ];
        const maxLeverage = decimalText(policy.max_leverage, true);
        const minimumQuality = String(policy.minimum_quality);
        const degradedMode = String(policy.degraded_mode);
        if (
          !policyVersion || !policyAllowed || policyNumbers.some((value) => value === null) ||
          !maxLeverage || !["fresh", "degraded"].includes(minimumQuality) ||
          !["reject", "allow_read_only"].includes(degradedMode) ||
          !Array.isArray(policy.correlated_clusters) || policy.correlated_clusters.length > 16
        ) {
          return jsonRpcError(id, -32602, "invalid policy");
        }
        const clusters: Array<{id:string; instruments:string[]; max_gross_notional_usd:string}> = [];
        for (const rawCluster of policy.correlated_clusters) {
          if (!isObject(rawCluster) || !hasOnlyKeys(rawCluster, ["id","instruments","max_gross_notional_usd"])) {
            return jsonRpcError(id, -32602, "invalid correlated cluster");
          }
          const clusterId = versionText(rawCluster.id);
          const instruments = instrumentList(rawCluster.instruments, 16);
          const maxGross = decimalText(rawCluster.max_gross_notional_usd, false);
          if (!clusterId || !instruments || !maxGross) {
            return jsonRpcError(id, -32602, "invalid correlated cluster");
          }
          clusters.push({ id: clusterId, instruments, max_gross_notional_usd: maxGross });
        }

        let candidate: Record<string, unknown> | null = null;
        if (args.candidate !== undefined) {
          if (!isObject(args.candidate) || !hasOnlyKeys(args.candidate, [
            "instrument","side","notional_usd","worst_case_loss_usd","leverage",
          ])) {
            return jsonRpcError(id, -32602, "invalid candidate");
          }
          const instrument = normalizeInstrument(args.candidate.instrument);
          const side = String(args.candidate.side);
          const notional = decimalText(args.candidate.notional_usd, true);
          const worstCaseLoss = decimalText(args.candidate.worst_case_loss_usd, false);
          const leverage = decimalText(args.candidate.leverage, true);
          if (!instrument || !["long","short"].includes(side) || !notional || !worstCaseLoss || !leverage) {
            return jsonRpcError(id, -32602, "invalid candidate");
          }
          candidate = {
            instrument,
            side,
            notional_usd: notional,
            worst_case_loss_usd: worstCaseLoss,
            leverage,
          };
        }

        let statistics: Record<string, unknown> | null = null;
        if (args.statistics !== undefined) {
          if (!isObject(args.statistics) || !hasOnlyKeys(args.statistics, [
            "bar","limit","parallel_scenario_move_ratio",
          ])) {
            return jsonRpcError(id, -32602, "invalid statistics");
          }
          const bar = String(args.statistics.bar);
          const limit = Number(args.statistics.limit);
          const moveRatio = args.statistics.parallel_scenario_move_ratio === undefined
            ? null
            : signedDecimalText(args.statistics.parallel_scenario_move_ratio);
          if (
            !STATISTICAL_BARS.has(bar) ||
            !Number.isInteger(limit) ||
            limit < 3 ||
            limit > 100 ||
            (args.statistics.parallel_scenario_move_ratio !== undefined && moveRatio === null) ||
            (moveRatio !== null && Number(moveRatio) <= -1)
          ) {
            return jsonRpcError(id, -32602, "invalid statistics");
          }
          statistics = {
            bar,
            limit,
            parallel_scenario_move_ratio: moveRatio,
          };
        }

        let virtualPortfolio: Record<string, unknown> | null = null;
        if (args.virtual_portfolio !== undefined) {
          if (
            candidate !== null ||
            !isObject(args.virtual_portfolio) ||
            !hasOnlyKeys(args.virtual_portfolio, ["collateral_usdt","positions"])
          ) {
            return jsonRpcError(id, -32602, "invalid virtual_portfolio");
          }
          const collateral = decimalText(args.virtual_portfolio.collateral_usdt, true);
          if (
            !collateral ||
            !Array.isArray(args.virtual_portfolio.positions) ||
            args.virtual_portfolio.positions.length < 2 ||
            args.virtual_portfolio.positions.length > 8
          ) {
            return jsonRpcError(id, -32602, "invalid virtual_portfolio");
          }
          const positions: Array<Record<string, string>> = [];
          const seen = new Set<string>();
          for (const rawPosition of args.virtual_portfolio.positions) {
            if (!isObject(rawPosition) || !hasOnlyKeys(rawPosition, [
              "instrument","contracts","average_price","leverage",
            ])) {
              return jsonRpcError(id, -32602, "invalid virtual portfolio position");
            }
            const instrument = normalizeInstrument(rawPosition.instrument);
            const contracts = signedDecimalText(rawPosition.contracts);
            const averagePrice = decimalText(rawPosition.average_price, true);
            const leverage = decimalText(rawPosition.leverage, true);
            if (
              !instrument || !contracts || Number(contracts) === 0 || !averagePrice || !leverage ||
              seen.has(instrument)
            ) {
              return jsonRpcError(id, -32602, "invalid virtual portfolio position");
            }
            seen.add(instrument);
            positions.push({
              instrument,
              contracts,
              average_price: averagePrice,
              leverage,
            });
          }
          virtualPortfolio = {
            collateral_usdt: collateral,
            positions,
          };
        }

        const agentRequest = {
          schema: "okx.agent.request/v1",
          request_id: requestId(),
          operation: {
            type: "portfolio_risk",
            mandate: {
              version: mandateVersion,
              capital_base_usd: capitalBase,
              decision_horizon_hours: mandate.decision_horizon_hours,
              benchmark,
              allowed_instruments: mandateAllowed,
              max_drawdown_ratio: maxDrawdown,
              leverage_ceiling: leverageCeiling,
              minimum_liquidity_notional_usd: minLiquidity,
              max_turnover_ratio: maxTurnover,
            },
            policy: {
              version: policyVersion,
              max_account_gross_notional_usd: policyNumbers[0],
              max_instrument_gross_notional_usd: policyNumbers[1],
              max_margin_utilization_ratio: policyNumbers[2],
              max_loss_per_trade_usd: policyNumbers[3],
              max_daily_realized_loss_usd: policyNumbers[4],
              max_drawdown_ratio: policyNumbers[5],
              max_leverage: maxLeverage,
              allowed_instruments: policyAllowed,
              minimum_quality: minimumQuality,
              degraded_mode: degradedMode,
              correlated_clusters: clusters,
            },
            candidate,
            statistics,
            virtual_portfolio: virtualPortfolio,
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
