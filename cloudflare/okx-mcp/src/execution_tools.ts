import { isObject, type Json } from "./shared.js";

// This is an explicit, finite product execution contract, not an RPC tunnel.
// All real order/UID/policy/risk/reconciliation authority stays in okx-execution.
const ID = { type: "string", minLength: 16, maxLength: 128, pattern: "^[A-Za-z0-9_-]+$" };
const MUTATION_ID = { type: "string", minLength: 8, maxLength: 128, pattern: "^[A-Za-z0-9_-]+$" };
const DECIMAL = { type: "string", minLength: 1, maxLength: 64, pattern: "^[0-9]+(?:\\.[0-9]+)?$" };
const POSITIVE = DECIMAL;
const INSTRUMENT = { type: "string", minLength: 3, maxLength: 64, pattern: "^[A-Za-z0-9_-]+$" };
const VERSION = { type: "string", minLength: 1, maxLength: 64, pattern: "^[A-Za-z0-9._/-]+$" };
const instrumentSet = { type: "array", maxItems: 32, uniqueItems: true, items: INSTRUMENT };
const mandate = {
  type: "object", additionalProperties: false,
  properties: {
    version: VERSION, capital_base_usd: POSITIVE, decision_horizon_hours: { type: "integer", minimum: 1, maximum: 8760 },
    benchmark: VERSION, allowed_instruments: instrumentSet,
    max_drawdown_ratio: DECIMAL, leverage_ceiling: POSITIVE,
    minimum_liquidity_notional_usd: DECIMAL, max_turnover_ratio: DECIMAL,
  },
  required: ["version","capital_base_usd","decision_horizon_hours","allowed_instruments","max_drawdown_ratio","leverage_ceiling","minimum_liquidity_notional_usd","max_turnover_ratio"],
};
const correlatedCluster = {
  type: "object", additionalProperties: false,
  properties: { id: VERSION, instruments: instrumentSet, max_gross_notional_usd: DECIMAL },
  required: ["id","instruments","max_gross_notional_usd"],
};
const hardPolicy = {
  type: "object", additionalProperties: false,
  properties: {
    version: VERSION, max_account_gross_notional_usd: POSITIVE,
    max_instrument_gross_notional_usd: POSITIVE, max_margin_utilization_ratio: POSITIVE,
    max_loss_per_trade_usd: POSITIVE, max_daily_realized_loss_usd: POSITIVE,
    max_drawdown_ratio: DECIMAL, max_leverage: POSITIVE, allowed_instruments: instrumentSet,
    minimum_quality: { type: "string", enum: ["fresh","degraded"] },
    degraded_mode: { type: "string", enum: ["reject","allow_read_only"] },
    correlated_clusters: { type: "array", maxItems: 16, items: correlatedCluster },
  },
  required: ["version","max_account_gross_notional_usd","max_instrument_gross_notional_usd","max_margin_utilization_ratio","max_loss_per_trade_usd","max_daily_realized_loss_usd","max_drawdown_ratio","max_leverage","allowed_instruments","minimum_quality","degraded_mode","correlated_clusters"],
};
const entry = {
  type: "object", additionalProperties: false,
  properties: {
    entry_price: POSITIVE, stop_price: POSITIVE, max_settle_notional: POSITIVE,
    max_loss_settle: POSITIVE, target_rr: POSITIVE,
    entry_liquidity_role: { type: "string", enum: ["maker","taker"] },
    exit_liquidity_role: { type: "string", enum: ["maker","taker"] },
  },
  required: ["entry_price","stop_price","max_settle_notional","max_loss_settle","target_rr","entry_liquidity_role","exit_liquidity_role"],
};
const spec = {
  type: "object", additionalProperties: false,
  properties: {
    action: { type: "string", enum: ["open","add","hedge","reduce","close","reverse","continue_reverse"] },
    position_side: { type: "string", enum: ["long","short"] },
    size: POSITIVE, price: POSITIVE, entry,
  },
  required: ["action"],
};
const risk = {
  type: "object", additionalProperties: false,
  properties: { mandate, policy: hardPolicy },
  required: ["mandate","policy"],
};
const lineage = {
  type: "object", additionalProperties: false,
  properties: {
    origin_evidence_id: { type: "string", minLength: 1, maxLength: 128 },
    origin_schema: VERSION, origin_version: VERSION,
    authority_evidence_id: { type: "string", minLength: 1, maxLength: 128 },
    decision_reference: {
      type: "object", additionalProperties: false,
      properties: {
        decision_time_ms: { type: "integer", minimum: 1 },
        price: POSITIVE, price_basis: { type: "string", enum: ["decision_price","arrival_mid","mark","index","last","limit_price"] },
        price_policy_version: VERSION,
      },
      required: ["decision_time_ms","price","price_basis","price_policy_version"],
    },
  },
  required: ["origin_evidence_id","origin_schema","origin_version","decision_reference"],
};
export const EXECUTION_TOOLS: Json[] = [
  {
    name: "executor_preflight",
    description: "READ ONLY: verify the exact active account/environment, Demo Trade executor credential, clock and admission readiness; no order created.",
    inputSchema: { type: "object", properties: {}, additionalProperties: false },
  },
  {
    name: "execution_action",
    description: "WRITE: explicit, typed and single-intent trading operation. Only an authenticated active Demo runtime is presently authorized; production is ALWAYS read-only. Prepare persists risk/intent without exchange send; submit/mutate can send EXACTLY ONCE and require independent read-only reconciliation. Do not retry uncertain results or use as a batch/raw RPC.",
    inputSchema: {
      type: "object", additionalProperties: false,
      properties: {
        action: { type: "string", enum: ["prepare","submit","amend","cancel","cancel_protection","abort_reverse","abandon_prepared"] },
        intent_id: ID, instrument: INSTRUMENT,
        trade_mode: { type: "string", enum: ["cross","isolated"] },
        order_type: { type: "string", enum: ["limit","post_only","fok","ioc"] },
        spec, risk, lineage, mutation_id: MUTATION_ID, new_size: POSITIVE, new_price: POSITIVE,
      },
      required: ["action","intent_id"],
    },
  },
];
function exactKeys(v: Record<string, unknown>, keys: readonly string[]): boolean {
  return Object.keys(v).every(k => keys.includes(k));
}
function token(v: unknown, min: number, max: number): v is string {
  return typeof v === "string" && v.length >= min && v.length <= max && /^[A-Za-z0-9_-]+$/.test(v);
}
function decimal(v: unknown): v is string {
  return typeof v === "string" && v.length <= 64 && /^[0-9]+(?:\.[0-9]+)?$/.test(v) && Number(v) > 0;
}
function entryInput(v: unknown): boolean {
  return isObject(v) && exactKeys(v, [
    "entry_price","stop_price","max_settle_notional","max_loss_settle","target_rr",
    "entry_liquidity_role","exit_liquidity_role",
  ]) && ["entry_price","stop_price","max_settle_notional","max_loss_settle","target_rr"].every(k => decimal(v[k]))
    && ["maker","taker"].includes(String(v.entry_liquidity_role))
    && ["maker","taker"].includes(String(v.exit_liquidity_role));
}
function preparedSpec(v: unknown): boolean {
  if (!isObject(v) || typeof v.action !== "string") return false;
  const action = v.action;
  if (["open","add","hedge"].includes(action)) {
    return exactKeys(v,["action","position_side","entry"])
      && ["long","short"].includes(String(v.position_side)) && entryInput(v.entry);
  }
  if (["reduce","close","reverse"].includes(action)) {
    return exactKeys(v,["action","position_side","size","price"])
      && ["long","short"].includes(String(v.position_side)) && decimal(v.size) && decimal(v.price);
  }
  return action === "continue_reverse" && exactKeys(v,["action","entry"]) && entryInput(v.entry);
}
function riskInput(v: unknown): boolean {
  if (!isObject(v) || !exactKeys(v,["mandate","policy"]) || !isObject(v.mandate) || !isObject(v.policy)) return false;
  const m=v.mandate,p=v.policy;
  if (!exactKeys(m,Object.keys(mandate.properties)) || !exactKeys(p,Object.keys(hardPolicy.properties))) return false;
  const version=(x:unknown)=>typeof x==="string"&&x.length>0&&x.length<=64&&/^[A-Za-z0-9._/-]+$/.test(x);
  const number=(x:unknown)=>typeof x==="string"&&x.length>0&&x.length<=64&&/^[0-9]+(?:\.[0-9]+)?$/.test(x);
  const instruments=(x:unknown)=>Array.isArray(x)&&x.length<=32
    && x.every(v=>token(v,3,64))&&new Set(x).size===x.length;
  if (!version(m.version)||!version(p.version)
    || !Number.isInteger(m.decision_horizon_hours)||Number(m.decision_horizon_hours)<1||Number(m.decision_horizon_hours)>8760
    || (m.benchmark!==undefined&&!version(m.benchmark))
    || !instruments(m.allowed_instruments)||!instruments(p.allowed_instruments)) return false;
  for (const key of ["capital_base_usd","max_drawdown_ratio","leverage_ceiling","minimum_liquidity_notional_usd","max_turnover_ratio"]) {
    if (!number(m[key])) return false;
  }
  for (const key of ["max_account_gross_notional_usd","max_instrument_gross_notional_usd",
    "max_margin_utilization_ratio","max_loss_per_trade_usd","max_daily_realized_loss_usd",
    "max_drawdown_ratio","max_leverage"]) {
    if (!number(p[key])) return false;
  }
  if (!["fresh","degraded"].includes(String(p.minimum_quality))
    || !["reject","allow_read_only"].includes(String(p.degraded_mode))
    || !Array.isArray(p.correlated_clusters)||p.correlated_clusters.length>16) return false;
  for (const raw of p.correlated_clusters) {
    if (!isObject(raw)||!exactKeys(raw,["id","instruments","max_gross_notional_usd"])
      || !version(raw.id)||!instruments(raw.instruments)||!number(raw.max_gross_notional_usd)) return false;
  }
  // Native Rust revalidates immutable policy semantics, account UID,
  // generation, positions, available margin and hard-risk limits again.
  return true;
}
function lineageInput(v: unknown): boolean {
  if (!isObject(v) || !exactKeys(v,["origin_evidence_id","origin_schema","origin_version","authority_evidence_id","decision_reference"]) || !isObject(v.decision_reference)) return false;
  return typeof v.origin_evidence_id === "string" && typeof v.origin_schema === "string"
    && typeof v.origin_version === "string"
    && exactKeys(v.decision_reference,["decision_time_ms","price","price_basis","price_policy_version"])
    && typeof v.decision_reference.price === "string";
}
export function isExecutionMutation(operationType: unknown): boolean {
  return [
    "prepare_execution", "submit_prepared_execution",
    "mutate_execution", "abort_reverse_execution", "abandon_prepared_execution",
  ].includes(String(operationType));
}

export function executionProfilePermitsOperation(profile: string | undefined, operationType: unknown): boolean {
  // The profile is authenticated from a single Windows Hello, not MCP input.
  return !isExecutionMutation(operationType) || profile === "demo_acceptance";
}

/** Return exactly one whitelisted native Rust AgentOperation, never arbitrary operation JSON. */
export function buildExecutionOperation(args: Record<string, unknown>): Record<string, unknown> | null {
  if (!token(args.intent_id,16,128) || typeof args.action !== "string") return null;
  const intent_id=args.intent_id;
  switch(args.action){
    case "prepare": {
      if (!exactKeys(args,["action","intent_id","instrument","trade_mode","order_type","spec","risk","lineage"])
        || !token(args.instrument,3,64) || !["cross","isolated"].includes(String(args.trade_mode))
        || !["limit","post_only","fok","ioc"].includes(String(args.order_type))
        || !preparedSpec(args.spec) || !riskInput(args.risk)
        || (args.lineage!==undefined && !lineageInput(args.lineage))) return null;
      return {
        type:"prepare_execution", intent_id, instrument:args.instrument, trade_mode:args.trade_mode,
        order_type:args.order_type, spec:args.spec, risk:args.risk, lineage:args.lineage??null,
      };
    }
    case "submit":
    case "abort_reverse":
    case "abandon_prepared":
      if (!exactKeys(args,["action","intent_id"])) return null;
      return {type:args.action==="submit"?"submit_prepared_execution"
        :args.action==="abort_reverse"?"abort_reverse_execution":"abandon_prepared_execution",intent_id};
    case "amend":
      if (!exactKeys(args,["action","intent_id","mutation_id","new_size","new_price"])
        || !token(args.mutation_id,8,128)
        || (args.new_size===undefined && args.new_price===undefined)
        || (args.new_size!==undefined && !decimal(args.new_size))
        || (args.new_price!==undefined && !decimal(args.new_price))) return null;
      return {type:"mutate_execution",intent_id,mutation:{
        type:"amend",mutation_id:args.mutation_id,new_size:args.new_size??null,new_price:args.new_price??null,
      }};
    case "cancel":
    case "cancel_protection":
      if (!exactKeys(args,["action","intent_id","mutation_id"]) || !token(args.mutation_id,8,128)) return null;
      return {type:"mutate_execution",intent_id,mutation:{type:args.action,mutation_id:args.mutation_id}};
    default:
      return null;
  }
}
