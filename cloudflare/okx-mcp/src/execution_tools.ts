import { isObject, type Json } from "./shared.js";

// This is an explicit, finite product execution contract, not an RPC tunnel.
// All real order/UID/policy/risk/reconciliation authority stays in okx-execution.
const ID = { type: "string", minLength: 16, maxLength: 128, pattern: "^[A-Za-z0-9_-]+$" };
const MUTATION_ID = { type: "string", minLength: 8, maxLength: 128, pattern: "^[A-Za-z0-9_-]+$" };
const EVIDENCE_ID = { type: "string", minLength: 71, maxLength: 71, pattern: "^sha256:[0-9a-f]{64}$" };
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
    origin_evidence_id: EVIDENCE_ID,
    origin_schema: VERSION, origin_version: VERSION,
    authority_evidence_id: EVIDENCE_ID,
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
    description: "READ ONLY: verify the exact authenticated account/environment, Trade credential, clock and native executor admission; no order created.",
    inputSchema: { type: "object", properties: {}, additionalProperties: false },
  },
  {
    name: "execution_action",
    description: "WRITE: typed single-intent action through the ONE Rust executor. Authenticated Demo supports its accepted mutation set; authenticated Production may dispatch only begin_strategy_run, prepare_execution, submit_prepared_execution, finalize_strategy_run and quarantine_unverified_strategy_run. Production exchange POST additionally requires native CONTROL ENABLED, exact UID/run/instrument, STOP/UNKNOWN, fresh source and risk checks. Profile alone never grants a trade. Never retry an uncertain result or use as a batch/raw RPC.",
    inputSchema: {
      type: "object", additionalProperties: false,
      properties: {
        action: { type: "string", enum: ["prepare","submit","amend","cancel","cancel_protection","confirm_protection_size","shrink_protection_size","abort_reverse","abandon_prepared","begin_strategy_run","finalize_strategy_run","quarantine_unverified_strategy_run","set_leverage","migrate_finalized_ledger_storage","reconcile_unsent_prepared"] },
        intent_id: ID, instrument: INSTRUMENT,
        trade_mode: { type: "string", enum: ["cross","isolated"] },
        order_type: { type: "string", enum: ["limit","post_only","fok","ioc"] },
        spec, risk, lineage, mutation_id: MUTATION_ID, new_size: POSITIVE, new_price: POSITIVE,
        expected_manifest_sha256: { type: "string", pattern: "^[0-9a-f]{64}$" },
        account_uid_fingerprint: { type: "string", pattern: "^[0-9a-f]{64}$" },
        intent_ids: { type: "array", minItems: 1, maxItems: 32, uniqueItems: true, items: ID },
        expected_finalized_checksum_sha256: { type: "string", pattern: "^[0-9a-f]{64}$" },
        expected_stopped_at_ms: { type: "integer", minimum: 1 },
        origin_schema: VERSION, origin_version: VERSION, mandate_version: VERSION,
        allocated_usdt: POSITIVE, operator_authority_id: EVIDENCE_ID, leverage: POSITIVE,
        rearm: { type: "object", additionalProperties: false, properties: {
          expected_stopped_at_ms: { type: "integer", minimum: 1 },
          prior_finalization_sha256: { type: "string", pattern: "^[0-9a-f]{64}$" }
        }, required: ["expected_stopped_at_ms","prior_finalization_sha256"] },
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
  const d = v.decision_reference;
  const version = (x: unknown) => typeof x === "string" && x.length > 0
    && x.length <= 128 && /^[A-Za-z0-9._/-]+$/.test(x);
  const evidence = (x: unknown) => typeof x === "string" && /^sha256:[0-9a-f]{64}$/.test(x);
  return evidence(v.origin_evidence_id)
    && (v.authority_evidence_id === undefined || evidence(v.authority_evidence_id))
    && version(v.origin_schema) && version(v.origin_version)
    && exactKeys(d,["decision_time_ms","price","price_basis","price_policy_version"])
    && Number.isSafeInteger(d.decision_time_ms) && Number(d.decision_time_ms) > 0
    && decimal(d.price)
    && ["decision_price","arrival_mid","mark","index","last","limit_price"].includes(String(d.price_basis))
    && version(d.price_policy_version);
}
export function isExecutionMutation(operationType: unknown): boolean {
  return [
    "prepare_execution", "submit_prepared_execution",
    "mutate_execution", "abort_reverse_execution", "abandon_prepared_execution",
    "begin_strategy_run", "finalize_strategy_run", "quarantine_unverified_strategy_run", "set_leverage",
    "migrate_finalized_ledger_storage", "reconcile_unsent_prepared",
  ].includes(String(operationType));
}

// Transport admission is deliberately narrower than the native executor.
 // Only the existing Rust owner can authorize exchange POST and determine
 // whether a real Production policy is ENABLED or STOPPED.
const PRODUCTION_NATIVE_ACTIONS = new Set([
  "begin_strategy_run",
  "prepare_execution",
  "submit_prepared_execution",
  "finalize_strategy_run",
  "quarantine_unverified_strategy_run",
  "reconcile_unsent_prepared",
]);

export function executionProfilePermitsOperation(profile: string | undefined, operationType: unknown): boolean {
  if (!isExecutionMutation(operationType)) return true;
  // Profile comes from the authenticated single-owner Windows Hello, not
  // from user args or OAuth identity. Unverified always fails closed.
  if (profile === "demo_acceptance") return true;
  return profile === "production" && PRODUCTION_NATIVE_ACTIONS.has(String(operationType));
}

/** Return exactly one whitelisted native Rust AgentOperation, never arbitrary operation JSON. */
export function buildExecutionOperation(args: Record<string, unknown>): Record<string, unknown> | null {
  if (!token(args.intent_id,16,128) || typeof args.action !== "string") return null;
  const intent_id=args.intent_id;
  switch(args.action){
    case "reconcile_unsent_prepared": {
      // Exact whole-inventory maintenance: not a trade, not a dry-run signal
      // and never operator-supplied evidence about OKX orders/fills.
      const ids=args.intent_ids;
      if (!exactKeys(args, [
          "action","intent_id","expected_manifest_sha256",
          "account_uid_fingerprint","intent_ids"
        ])
        || typeof args.expected_manifest_sha256 !== "string"
        || !/^[0-9a-f]{64}$/.test(args.expected_manifest_sha256)
        || typeof args.account_uid_fingerprint !== "string"
        || !/^[0-9a-f]{64}$/.test(args.account_uid_fingerprint)
        || !Array.isArray(ids) || ids.length===0 || ids.length>32
        || !ids.every(v=>token(v,16,128))
        || !ids.every((v,i)=>i===0 || String(ids[i-1])<String(v))) return null;
      return {
        type:"reconcile_unsent_prepared",
        expected_manifest_sha256:args.expected_manifest_sha256,
        account_uid_fingerprint:args.account_uid_fingerprint,
        intent_ids:ids,
      };
    }
    case "migrate_finalized_ledger_storage": {
      if (!exactKeys(args,[
        "action","intent_id","expected_manifest_sha256",
        "expected_finalized_checksum_sha256","expected_stopped_at_ms"
      ]) || typeof args.expected_manifest_sha256 !== "string"
        || !/^[0-9a-f]{64}$/.test(args.expected_manifest_sha256)
        || typeof args.expected_finalized_checksum_sha256 !== "string"
        || !/^[0-9a-f]{64}$/.test(args.expected_finalized_checksum_sha256)
        || !Number.isSafeInteger(args.expected_stopped_at_ms)
        || Number(args.expected_stopped_at_ms) <= 0) return null;
      return {
        type:"migrate_finalized_ledger_storage",
        allocation_id:intent_id,
        expected_manifest_sha256:args.expected_manifest_sha256,
        expected_finalized_checksum_sha256:args.expected_finalized_checksum_sha256,
        expected_stopped_at_ms:args.expected_stopped_at_ms,
      };
    }
    case "set_leverage": {
      if (!exactKeys(args,["action","intent_id","instrument","trade_mode","leverage"])
        || !token(args.instrument,3,64)
        || !String(args.instrument).endsWith("-USDT-SWAP")
        || args.trade_mode!=="cross" || !decimal(args.leverage)) return null;
      return {type:"set_leverage",mutation_id:intent_id,
        instrument:args.instrument,margin_mode:"cross",leverage:args.leverage};
    }
    case "finalize_strategy_run": {
      if (!exactKeys(args,["action","intent_id"])) return null;
      return {type:"finalize_strategy_run",allocation_id:intent_id};
    }
    case "quarantine_unverified_strategy_run": {
      if (!exactKeys(args,["action","intent_id"])) return null;
      return {type:"quarantine_unverified_strategy_run",allocation_id:intent_id};
    }
    case "begin_strategy_run": {
      if (!exactKeys(args,["action","intent_id","origin_schema","origin_version","mandate_version","allocated_usdt","operator_authority_id","rearm"])
        || ![args.origin_schema,args.origin_version,args.mandate_version].every(v =>
          typeof v==="string" && v.length>0 && v.length<=64 && /^[A-Za-z0-9._/-]+$/.test(v))
        || !decimal(args.allocated_usdt)
        || typeof args.operator_authority_id!=="string"
        || !/^sha256:[0-9a-f]{64}$/.test(args.operator_authority_id)
        || (args.rearm !== undefined
          && (!isObject(args.rearm)
            || !exactKeys(args.rearm,["expected_stopped_at_ms","prior_finalization_sha256"])
            || !Number.isSafeInteger(args.rearm.expected_stopped_at_ms)
            || Number(args.rearm.expected_stopped_at_ms) <= 0
            || !/^[0-9a-f]{64}$/.test(String(args.rearm.prior_finalization_sha256))))) return null;
      return {type:"begin_strategy_run",allocation_id:intent_id,
        origin_schema:args.origin_schema,origin_version:args.origin_version,
        mandate_version:args.mandate_version,allocated_usdt:args.allocated_usdt,
        operator_authority_id:args.operator_authority_id,
        ...(args.rearm===undefined?{}:{rearm:args.rearm})};
    }
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
    case "shrink_protection_size":
      if (!exactKeys(args,["action","intent_id","mutation_id","new_size"])
        || !token(args.mutation_id,8,128) || !decimal(args.new_size)) return null;
      return {type:"mutate_execution",intent_id,mutation:{
        type:"shrink_protection",mutation_id:args.mutation_id,new_size:args.new_size,
      }};
    case "cancel":
    case "cancel_protection":
    case "confirm_protection_size":
      if (!exactKeys(args,["action","intent_id","mutation_id"]) || !token(args.mutation_id,8,128)) return null;
      return {type:"mutate_execution",intent_id,mutation:{type:args.action,mutation_id:args.mutation_id}};
    default:
      return null;
  }
}
