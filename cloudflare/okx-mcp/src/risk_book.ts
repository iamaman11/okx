import {validGovernance,projectRiskPolicy} from "./account_governance.js";

/**
 * Durable Object risk ledger primitive. This is NOT an exchange admission endpoint.
 * Only the authenticated Windows execution owner may eventually bind a native,
 * source-complete census to this primitive; never accept one from public MCP args.
 */
export const RISK_BOOK_SCHEMA = "okx.risk-book/v1" as const;
export type RiskEntryState = "HELD" | "POSTED" | "UNKNOWN" | "OPEN" | "RELEASED";

export interface RiskEntry {
  key: string;
  venue: string;
  environment: "real";
  uid: string;
  run_id: string;
  strategy_id: string;
  intent_id: string;
  plan_hash: string;
  policy_revision: number;
  loss_usdt: string;
  state: RiskEntryState;
  created_at_ms: number;
  transition_proof: string | null;
}
export interface RiskBook {
  schema: typeof RISK_BOOK_SCHEMA;
  sequence: number;
  entries: RiskEntry[];
}
export interface RiskPolicy {
  revision: number;
  enabled: boolean;
  authorized_ceiling_usdt: string;
  global_limit_usdt: string;
  account_limits_usdt: Record<string, string>;
}
export interface AccountRiskWitness {
  account_key: string;
  occupied_usdt: string;
}
/**
 * Must come from a complete, authenticated, coherent native all-managed-account
 * read; a snapshot of only the selected UID or a zero derived from FLAT is not
 * sufficient. Do not expose a caller-writable census via MCP.
 */
export interface CompleteRiskCensus {
  account_keys: string[];
  occupied: AccountRiskWitness[];
  generated_at_ms: number;
  source_digest: string;
  complete: true;
}
export interface RiskReservation {
  venue: string;
  environment: "real";
  uid: string;
  run_id: string;
  intent_id: string;
  plan_hash: string;
  loss_usdt: string;
  policy_revision: number;
  now_ms: number;
}
/** Created only inside an authenticated, current-generation native WS frame handler.
 * Never expose this object through public MCP arguments.
 */
export interface NativeSessionRiskProof {
  connection_generation:number;
  source_digest:string;
  native_account_evidence_complete:boolean;
  native_order_evidence_complete:boolean;
  native_ledger_evidence_complete:boolean;
}

export type RiskDecision =
  | { status: "HELD"; entry: RiskEntry; book_sequence: number; occupied_global_usdt: string }
  | { status: "DENIED"; reason: string };

const U = 1_000_000_000n;
const HEX = /^[0-9a-f]{64}$/;
const TOKEN = /^[A-Za-z0-9_-]{8,128}$/;
const AMOUNT = /^(?:0|[1-9][0-9]{0,14})(?:\.[0-9]{1,9})?$/;
const KEY = "risk_book_v1";
const MAX_ENTRIES = 50_000;
const MAX_CENSUS_AGE_MS = 5_000;

export function units(amount: unknown): bigint | null {
  if (typeof amount !== "string" || !AMOUNT.test(amount)) return null;
  const [whole, fraction = ""] = amount.split(".");
  return BigInt(whole) * U + BigInt(fraction.padEnd(9, "0") || "0");
}
export function amount(value: bigint): string {
  if (value < 0n) throw Error("negative risk");
  const whole = value / U;
  const fraction = (value % U).toString().padStart(9, "0").replace(/0+$/, "");
  return fraction ? whole.toString() + "." + fraction : whole.toString();
}
function validEntry(entry: RiskEntry): boolean {
  return entry.environment === "real" && /^[a-z][a-z0-9_-]{1,31}$/.test(entry.venue)
    && HEX.test(entry.uid) && HEX.test(entry.plan_hash)
    && TOKEN.test(entry.run_id) && /^[A-Za-z][A-Za-z0-9_-]{1,63}$/.test(entry.strategy_id)
    && TOKEN.test(entry.intent_id)
    && Number.isSafeInteger(entry.policy_revision) && entry.policy_revision > 0
    && Number.isSafeInteger(entry.created_at_ms) && entry.created_at_ms > 0
    && units(entry.loss_usdt) !== null && units(entry.loss_usdt)! > 0n
    && ["HELD","POSTED","UNKNOWN","OPEN","RELEASED"].includes(entry.state)
    && (entry.transition_proof === null || HEX.test(entry.transition_proof))
    && entry.key === riskKey(entry);
}
export function accountKey(entry: Pick<RiskEntry, "venue"|"environment"|"uid">): string {
  return `${entry.venue}:${entry.environment}:${entry.uid}`;
}
function riskKey(entry: Pick<RiskEntry, "venue"|"environment"|"uid"|"run_id"|"intent_id">): string {
  // Length-delimited canonical components; no separator collision.
  return [entry.venue, entry.environment, entry.uid, entry.run_id, entry.intent_id]
    .map(x => x.length + ":" + x).join("");
}
function validBook(value: unknown): value is RiskBook {
  if (typeof value !== "object" || value === null || Array.isArray(value)) return false;
  const v = value as Record<string, unknown>;
  if (v.schema !== RISK_BOOK_SCHEMA || !Number.isSafeInteger(v.sequence) || (v.sequence as number) < 0
    || !Array.isArray(v.entries) || v.entries.length > MAX_ENTRIES) return false;
  const keys = new Set<string>();
  for (const e of v.entries) {
    if (!e || typeof e !== "object" || Array.isArray(e)) return false;
    const entry = e as RiskEntry;
    if (!validEntry(entry) || keys.has(entry.key)) return false;
    keys.add(entry.key);
  }
  return true;
}
function emptyBook(): RiskBook { return { schema: RISK_BOOK_SCHEMA, sequence: 0, entries: [] }; }
function censusRisk(census: CompleteRiskCensus, policy: RiskPolicy, now: number): Map<string,bigint> | null {
  if (census.complete !== true || !HEX.test(census.source_digest)
    || census.generated_at_ms > now || now - census.generated_at_ms > MAX_CENSUS_AGE_MS
    || !Number.isSafeInteger(now) || !Number.isSafeInteger(census.generated_at_ms)) return null;
  const required = Object.keys(policy.account_limits_usdt).sort();
  if (!Array.isArray(census.account_keys)
    || required.join(",") !== [...census.account_keys].sort().join(",")
    || new Set(census.account_keys).size !== required.length
    || !Array.isArray(census.occupied) || census.occupied.length !== required.length) return null;
  const result = new Map<string,bigint>();
  for (const position of census.occupied) {
    const value = units(position.occupied_usdt);
    if (!required.includes(position.account_key) || result.has(position.account_key) || value === null) return null;
    result.set(position.account_key, value);
  }
  return result;
}
function budget(policy: RiskPolicy): { global: bigint; authorized: bigint; accounts: Map<string,bigint> } | null {
  const global = units(policy.global_limit_usdt), authorized = units(policy.authorized_ceiling_usdt);
  if (!Number.isSafeInteger(policy.revision) || policy.revision < 1
      || global === null || authorized === null || global <= 0n
      || authorized <= 0n || global > authorized) return null;
  const accounts = new Map<string,bigint>();
  for (const [uid, text] of Object.entries(policy.account_limits_usdt)) {
    const value = units(text);
    if (!/^[a-z][a-z0-9_-]{1,31}:real:[0-9a-f]{64}$/.test(uid)
      || value === null || value <= 0n || value > authorized) return null;
    accounts.set(uid, value);
  }
  return accounts.size ? {global, authorized, accounts} : null;
}
function replayed(existing: RiskEntry, input: RiskReservation): boolean {
  return existing.plan_hash === input.plan_hash
    && existing.loss_usdt === input.loss_usdt
    && existing.policy_revision === input.policy_revision
    && existing.strategy_id === input.strategy_id;
}

/**
 * One transaction per reservation. Includes risk of all HELD / POSTED /
 * UNKNOWN / OPEN entries plus pessimistic source-attested external occupancy.
 * Double counting is conservative; never understate when source overlap is
 * not independently proven. Released tombstones disallow intent reuse.
 */
export async function reserveRisk(
  storage: any,
  policy: RiskPolicy,
  census: CompleteRiskCensus,
  input: RiskReservation,
  native: NativeSessionRiskProof | null = null,
): Promise<RiskDecision> {
  const bounds = budget(policy);
  const requested = units(input.loss_usdt);
  if(!native || !native.native_account_evidence_complete ||
    !native.native_order_evidence_complete || !native.native_ledger_evidence_complete ||
    !Number.isSafeInteger(native.connection_generation) || native.connection_generation < 1 ||
    native.source_digest !== census.source_digest)
      return {status:"DENIED",reason:"NATIVE_COMPLETE_RISK_SOURCE_UNPROVEN"};
  if (!policy.enabled || bounds === null || input.environment !== "real"
    || input.policy_revision !== policy.revision
    || requested === null || requested <= 0n
    || !HEX.test(input.uid) || !HEX.test(input.plan_hash)
    || !TOKEN.test(input.run_id) || !TOKEN.test(input.intent_id)
    || !/^[A-Za-z][A-Za-z0-9_-]{1,63}$/.test(input.strategy_id)
    || !/^[a-z][a-z0-9_-]{1,31}$/.test(input.venue)
    || !Number.isSafeInteger(input.now_ms) || input.now_ms <= 0
    || !bounds.accounts.has(accountKey(input))) return {status:"DENIED", reason:"INVALID_SCOPE_OR_POLICY"};
  const externallyOccupied = censusRisk(census, policy, input.now_ms);
  if (externallyOccupied === null) return {status:"DENIED", reason:"COMPLETE_ACCOUNT_CENSUS_UNPROVEN"};
  return storage.transaction(async (tx: any): Promise<RiskDecision> => {
    // One transaction snapshots BOTH central policy and risk occupancy.
    // A caller-supplied policy (or a snapshot from a prior CAS revision)
    // can never mint risk capacity independently of the DO's persisted grant.
    const governance:unknown=await tx.get("account_governance_v2");
    if(!validGovernance(governance))
      return {status:"DENIED",reason:"AUTHORITATIVE_GOVERNANCE_UNAVAILABLE"};
    const authority=projectRiskPolicy(governance);
    if(authority===null||!authority.enabled||authority.revision!==policy.revision
       ||authority.global_limit_usdt!==policy.global_limit_usdt
       ||authority.authorized_ceiling_usdt!==policy.authorized_ceiling_usdt
       ||Object.keys(authority.account_limits_usdt).sort().join("|")
         !==Object.keys(policy.account_limits_usdt).sort().join("|")
       ||Object.entries(authority.account_limits_usdt).some(
           ([k,v])=>policy.account_limits_usdt[k]!==v
       ))
      return {status:"DENIED",reason:"POLICY_SNAPSHOT_NOT_AUTHORITATIVE"};
    // Current authenticated generation is authoritative for the transaction.
    // No persisted intermediate attestation / second control journal.
    const generation:unknown=await tx.get("generation");
    if(generation!==native.connection_generation)
      return {status:"DENIED",reason:"STALE_RUNTIME_GENERATION"};
    const stored: unknown = await tx.get(KEY);
    if (stored !== undefined && !validBook(stored)) return {status:"DENIED", reason:"RISK_BOOK_CORRUPT"};
    const book = (stored ?? emptyBook()) as RiskBook;
    const key = riskKey(input);
    const prior = book.entries.find(e => e.key === key);
    if (prior) {
      if (prior.state !== "RELEASED" && replayed(prior, input))
        return {status:"HELD", entry:prior, book_sequence:book.sequence, occupied_global_usdt:"UNKNOWN_IDEMPOTENT"};
      return {status:"DENIED", reason:"IMMUTABLE_INTENT_REUSE"};
    }
    if (book.entries.length >= MAX_ENTRIES) return {status:"DENIED", reason:"RISK_BOOK_CAPACITY"};
    const account=governance.accounts.find(a=>
      a.venue===input.venue && a.environment==="real" && a.uid_fingerprint===input.uid &&
      a.identity_verified && a.enabled);
    const strategy=account?.strategies.find(s=>s.id===input.strategy_id && s.enabled);
    if(!account||!strategy)return {status:"DENIED",reason:"ACCOUNT_OR_STRATEGY_NOT_ACTIVE"};
    const maxOrder=units(account.limits.max_order_loss_usdt);
    const maxStrategy=units(strategy.max_modelled_loss_usdt);
    if(maxOrder===null||maxStrategy===null||requested>maxOrder||requested>maxStrategy)
      return {status:"DENIED",reason:"ORDER_OR_STRATEGY_RISK_LIMIT"};
    let strategyOutstanding=0n;
    for(const entry of book.entries) {
      if(entry.state!=="RELEASED" && entry.uid===input.uid &&
        entry.venue===input.venue && entry.strategy_id===input.strategy_id) {
        strategyOutstanding+=units(entry.loss_usdt)!;
      }
    }
    if(strategyOutstanding+requested>maxStrategy)
      return {status:"DENIED",reason:"STRATEGY_AGGREGATE_RISK_LIMIT"};
    const occupied = new Map(externallyOccupied);
    for (const e of book.entries) {
      if (e.state === "RELEASED") continue;
      const owner = accountKey(e);
      if (!occupied.has(owner)) return {status:"DENIED", reason:"EXPOSURE_UNREGISTERED"};
      occupied.set(owner, occupied.get(owner)! + units(e.loss_usdt)!);
    }
    let total = 0n;
    for (const [uid,value] of occupied) {
      if (value > bounds.accounts.get(uid)!) return {status:"DENIED", reason:"ACCOUNT_ALREADY_OVER_LIMIT"};
      total += value;
    }
    if (total + requested > bounds.global
      || occupied.get(accountKey(input))! + requested > bounds.accounts.get(accountKey(input))!)
      return {status:"DENIED", reason:"INSUFFICIENT_AGGREGATE_OR_ACCOUNT_RISK"};
    const entry: RiskEntry = {
      key, venue:input.venue, environment:"real", uid:input.uid,
      run_id:input.run_id, strategy_id:input.strategy_id, intent_id:input.intent_id, plan_hash:input.plan_hash,
      policy_revision:input.policy_revision, loss_usdt:input.loss_usdt,
      state:"HELD", created_at_ms:input.now_ms, transition_proof:null,
    };
    const next: RiskBook = {schema:RISK_BOOK_SCHEMA, sequence:book.sequence+1, entries:[...book.entries,entry]};
    await tx.put(KEY,next);
    return {status:"HELD",entry,book_sequence:next.sequence,occupied_global_usdt:amount(total+requested)};
  });
}

/** Never release because of a timeout. Native source/ledger exact reconciliation
 * must provide the proof hash; all transition origins remain private to DO.
 */
export async function transitionRisk(
  storage: any,
  key: string,
  plan_hash: string,
  to: RiskEntryState,
  exact_native_proof_digest: string,
  native: {connection_generation:number;expected_book_sequence:number}|null = null,
): Promise<{status:"UPDATED"|"DENIED";reason?:string}> {
  if (!HEX.test(plan_hash) || !HEX.test(exact_native_proof_digest) ||
      !native || !Number.isSafeInteger(native.connection_generation) ||
      native.connection_generation<1 || !Number.isSafeInteger(native.expected_book_sequence) ||
      native.expected_book_sequence<1)
    return {status:"DENIED",reason:"MISSING_NATIVE_PROOF"};
  const transitions: Record<RiskEntryState,RiskEntryState[]> = {
    HELD:["POSTED","UNKNOWN","RELEASED"],
    POSTED:["UNKNOWN","OPEN","RELEASED"],
    UNKNOWN:["OPEN","RELEASED"],
    OPEN:["RELEASED"],
    RELEASED:[],
  };
  return storage.transaction(async (tx:any) => {
    const value: unknown = await tx.get(KEY);
    if (!validBook(value)) return {status:"DENIED" as const,reason:"RISK_BOOK_MISSING_OR_CORRUPT"};
    const book = value as RiskBook;
    const generation:unknown=await tx.get("generation");
    if(generation!==native.connection_generation ||
      book.sequence!==native.expected_book_sequence)
      return {status:"DENIED" as const,reason:"STALE_SESSION_OR_BOOK_SEQUENCE"};
    const index = book.entries.findIndex(e=>e.key===key && e.plan_hash===plan_hash);
    if (index<0 || !transitions[book.entries[index].state].includes(to))
      return {status:"DENIED" as const,reason:"INVALID_RISK_TRANSITION"};
    const entries = [...book.entries];
    entries[index] = {...entries[index],state:to,transition_proof:exact_native_proof_digest};
    await tx.put(KEY,{schema:RISK_BOOK_SCHEMA,sequence:book.sequence+1,entries});
    return {status:"UPDATED" as const};
  });
}

/** Pre-POST fence. A previous HELD reservation is *not* exchange permission.
 * This transaction checks current centralized policy+session and durably
 * marks the only possible send POSTED; replay/timeout never authorizes POST.
 */
export async function beginRiskPost(
  storage:any,
  key:string,
  plan_hash:string,
  policy_revision:number,
  proof_digest:string,
  native:{connection_generation:number;expected_book_sequence:number}|null,
):Promise<{status:"POST_AUTHORIZED"|"DENIED";reason?:string;book_sequence?:number}>{
  if(!native||!HEX.test(plan_hash)||!HEX.test(proof_digest)||
    !Number.isSafeInteger(native.connection_generation)||native.connection_generation<1||
    !Number.isSafeInteger(native.expected_book_sequence)||native.expected_book_sequence<1||
    !Number.isSafeInteger(policy_revision)||policy_revision<1)
    return {status:"DENIED",reason:"INVALID_NATIVE_POST_FENCE"};
  return storage.transaction(async(tx:any)=>{
    const generation:unknown=await tx.get("generation");
    if(generation!==native.connection_generation)
      return {status:"DENIED" as const,reason:"STALE_RUNTIME_GENERATION"};
    const governance:unknown=await tx.get("account_governance_v2");
    if(!validGovernance(governance)||!governance.trading_requested||
      governance.revision!==policy_revision)
      return {status:"DENIED" as const,reason:"CENTRAL_REVOCATION_OR_POLICY_CHANGE"};
    const authority=projectRiskPolicy(governance);
    if(!authority?.enabled)
      return {status:"DENIED" as const,reason:"CENTRAL_OPERATOR_GRANT_INACTIVE"};
    const stored:unknown=await tx.get(KEY);
    if(!validBook(stored))
      return {status:"DENIED" as const,reason:"RISK_BOOK_MISSING_OR_CORRUPT"};
    if(stored.sequence!==native.expected_book_sequence)
      return {status:"DENIED" as const,reason:"STALE_RISK_BOOK_SEQUENCE"};
    const index=stored.entries.findIndex(e=>e.key===key&&e.plan_hash===plan_hash);
    if(index<0||stored.entries[index].state!=="HELD"||
      stored.entries[index].policy_revision!==policy_revision)
      return {status:"DENIED" as const,reason:"INTENT_NOT_HELD_EXACTLY_ONCE"};
    const entry=stored.entries[index];
    if(!Object.hasOwn(authority.account_limits_usdt,accountKey(entry)))
      return {status:"DENIED" as const,reason:"ACCOUNT_REVOKED"};
    const entries=[...stored.entries];
    entries[index]={...entry,state:"POSTED" as const,transition_proof:proof_digest};
    await tx.put(KEY,{schema:RISK_BOOK_SCHEMA,sequence:stored.sequence+1,entries});
    return {status:"POST_AUTHORIZED" as const,book_sequence:stored.sequence+1};
  });
}

export async function readRiskBook(storage:any):Promise<RiskBook|null> {
  const value:unknown=await storage.get(KEY);
  return value===undefined?emptyBook():validBook(value)?value:null;
}
