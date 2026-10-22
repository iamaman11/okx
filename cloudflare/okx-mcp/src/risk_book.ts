import {validGovernance,projectRiskPolicy,accessEnabled,GOVERNANCE_STORAGE_KEY} from "./account_governance.js";

/**
 * Durable Object risk ledger primitive. This is NOT an exchange admission endpoint.
 * Only the authenticated Windows execution owner may eventually bind a native,
 * source-complete census to this primitive; never accept one from public MCP args.
 */
export const RISK_BOOK_SCHEMA = "okx.risk-book/v2" as const;
export type RiskEntryState = "HELD" | "POSTED" | "UNKNOWN" | "OPEN" | "RELEASED";

export interface RiskEntry {
  key: string;
  venue: string;
  environment: "real";
  uid: string;
  run_id: string;
  strategy_id: string;
  instrument_id: string;
  intent_id: string;
  plan_hash: string;
  policy_revision: number;
  loss_usdt: string;
  loss_usd:string;
  usd_conversion:{usd_per_usdt:string;source_digest:string;observed_at_ms:number};
  state: RiskEntryState;
  created_at_ms: number;
  /** Native source digest bound to this reservation. */
  reserve_source_digest?: string;
  reserve_source_read_floor_ms?: number;
  /** Durable BEGIN_POST timestamp; timeout never releases the reservation. */
  posted_at_ms?: number;
  transition_proof: string | null;
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
  cumulative_loss_usd:string;
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
  content_digest: string;
  source_read_floor_ms: number;
  complete: true;
}
export interface RiskReservation {
  venue: string;
  environment: "real";
  uid: string;
  run_id: string;
  strategy_id: string;
  instrument_id: string;
  intent_id: string;
  plan_hash: string;
  loss_usdt: string;
  loss_usd:string;
  usd_conversion:{usd_per_usdt:string;source_digest:string;observed_at_ms:number};
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
const KEY = "risk_meta_v2";

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
    && /^[A-Z0-9_-]{3,64}$/.test(entry.instrument_id)
    && TOKEN.test(entry.intent_id)
    && Number.isSafeInteger(entry.policy_revision) && entry.policy_revision > 0
    && Number.isSafeInteger(entry.created_at_ms) && entry.created_at_ms > 0
    && (entry.reserve_source_digest === undefined || HEX.test(entry.reserve_source_digest))
    && (entry.reserve_source_read_floor_ms === undefined ||
        Number.isSafeInteger(entry.reserve_source_read_floor_ms) && entry.reserve_source_read_floor_ms > 0)
    && (entry.posted_at_ms === undefined || Number.isSafeInteger(entry.posted_at_ms)
      && entry.posted_at_ms > entry.created_at_ms)
    && units(entry.loss_usd)!==null && !!entry.usd_conversion && units(entry.usd_conversion.usd_per_usdt)!==null && HEX.test(entry.usd_conversion.source_digest)
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
export interface RiskMeta { source_as_of_ms:number; schema:typeof RISK_BOOK_SCHEMA; sequence:number; installation_id:string; occupied_usdt:string; occupied_usd:string; consumed_loss_usd:string; campaign_limit_usd:string; accounts:Record<string,string>; }
function validMeta(v:any):v is RiskMeta {return !!v&&v.schema===RISK_BOOK_SCHEMA&&Number.isSafeInteger(v.source_as_of_ms)&&v.source_as_of_ms>=0&&Number.isSafeInteger(v.sequence)&&v.sequence>=0&&typeof v.installation_id==="string"&&units(v.occupied_usdt)!==null&&units(v.occupied_usd)!==null&&units(v.consumed_loss_usd)!==null&&v.campaign_limit_usd==="20"&&!!v.accounts&&Object.keys(v.accounts).length<=32&&Object.values(v.accounts).every(x=>units(x)!==null)&&Object.values(v.accounts).reduce<bigint>((sum,x)=>sum+units(x)!,0n)===units(v.occupied_usdt);}
export async function initializeRiskBook(tx:any,installation_id:string):Promise<void>{
 if(await tx.get(KEY)!==undefined)throw Error("RISK_ALREADY_INITIALIZED");
 await tx.put(KEY,{schema:RISK_BOOK_SCHEMA,source_as_of_ms:0,sequence:0,installation_id,occupied_usdt:"0",occupied_usd:"0",consumed_loss_usd:"0",campaign_limit_usd:"20",accounts:{}} satisfies RiskMeta);
}
const entryKey=(key:string)=>"risk_entry_v2:"+key;
const strategyKey=(entry:Pick<RiskEntry,"venue"|"environment"|"uid"|"strategy_id">)=>accountKey(entry)+":"+entry.strategy_id;
export async function inspectRisk(storage:any,key:string):Promise<{meta:RiskMeta;entry:RiskEntry}|null>{
 return storage.transaction(async(tx:any)=>{const meta=await tx.get(KEY),entry=await tx.get(entryKey(key));return validMeta(meta)&&entry&&validEntry(entry)?{meta,entry}:null;});
}
async function saveEntry(tx:any,meta:RiskMeta,entry:RiskEntry,delta:bigint){
 const a=accountKey(entry),s="risk_strategy_v2:"+strategyKey(entry);
 const strategy=units(await tx.get(s)??"0");if(strategy===null)throw Error("RISK_STRATEGY_AGGREGATE_CORRUPT");
 const next={...meta,sequence:meta.sequence+1,occupied_usdt:amount(units(meta.occupied_usdt)!+delta),occupied_usd:amount(units(meta.occupied_usd)!+(delta===0n?0n:delta>0n?units(entry.loss_usd)!:-units(entry.loss_usd)!)),accounts:{...meta.accounts,[a]:amount(units(meta.accounts[a]??"0")!+delta)}};
 await tx.put(s,amount(strategy+delta));
 await tx.put(entryKey(entry.key),entry);await tx.put(KEY,next);return next;
}
async function applyCensusLoss(tx:any,meta:RiskMeta,census:CompleteRiskCensus):Promise<boolean>{
 let added=0n;const updates:[string,string][]=[];
 for(const row of census.occupied){const key="risk_loss_v2:"+row.account_key;const previous=units(await tx.get(key)??"0"),current=units(row.cumulative_loss_usd);if(previous===null||current===null||current<previous)return false;added+=current-previous;if(current!==previous)updates.push([key,amount(current)]);}
 meta.source_as_of_ms=census.source_read_floor_ms;
 if(added>0n){meta.consumed_loss_usd=amount(units(meta.consumed_loss_usd)!+added);meta.sequence++;for(const[k,v]of updates)await tx.put(k,v);await tx.put(KEY,meta);}else{await tx.put(KEY,meta);}return true;
}
function censusRisk(census: CompleteRiskCensus, policy: RiskPolicy, now: number): Map<string,bigint> | null {
  if (census.complete !== true || !HEX.test(census.source_digest)
    || !HEX.test(census.content_digest)
    || !Number.isSafeInteger(census.source_read_floor_ms)
    || census.source_read_floor_ms <= 0
    || census.source_read_floor_ms > census.generated_at_ms
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
    if (!required.includes(position.account_key) || result.has(position.account_key) || value === null || units(position.cumulative_loss_usd)===null) return null;
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
  const rate=units(input.usd_conversion?.usd_per_usdt),usd=units(input.loss_usd);
  if(rate===null||rate<=0n||usd===null||requested===null||usd!==(requested*rate+U-1n)/U||!HEX.test(input.usd_conversion?.source_digest??"")||!Number.isSafeInteger(input.usd_conversion?.observed_at_ms)||input.usd_conversion.observed_at_ms>input.now_ms||input.now_ms-input.usd_conversion.observed_at_ms>5000)return {status:"DENIED",reason:"USD_CONVERSION_UNPROVEN"};
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
    || !/^[A-Z0-9_-]{3,64}$/.test(input.instrument_id)
    || !/^[a-z][a-z0-9_-]{1,31}$/.test(input.venue)
    || !Number.isSafeInteger(input.now_ms) || input.now_ms <= 0
    || !bounds.accounts.has(accountKey(input))) return {status:"DENIED", reason:"INVALID_SCOPE_OR_POLICY"};
  const externallyOccupied = censusRisk(census, policy, input.now_ms);
  if (externallyOccupied === null) return {status:"DENIED", reason:"COMPLETE_ACCOUNT_CENSUS_UNPROVEN"};
  return storage.transaction(async (tx: any): Promise<RiskDecision> => {
    // One transaction snapshots BOTH central policy and risk occupancy.
    // A caller-supplied policy (or a snapshot from a prior CAS revision)
    // can never mint risk capacity independently of the DO's persisted grant.
    const governance:unknown=await tx.get(GOVERNANCE_STORAGE_KEY);
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
    const meta:unknown=await tx.get(KEY);
    if(!validMeta(meta)||meta.installation_id!==governance.installation_id)return {status:"DENIED",reason:"RISK_BOOK_MISSING_OR_CORRUPT"};
    if(!await applyCensusLoss(tx,meta,census))return {status:"DENIED",reason:"CUMULATIVE_LOSS_SOURCE_REGRESSION"};
    const key=riskKey(input),prior:RiskEntry|undefined=await tx.get(entryKey(key));
    if(prior){
      if(!validEntry(prior)||prior.plan_hash!==input.plan_hash||prior.loss_usdt!==input.loss_usdt||prior.loss_usd!==input.loss_usd||JSON.stringify(prior.usd_conversion)!==JSON.stringify(input.usd_conversion)||prior.policy_revision!==input.policy_revision||prior.strategy_id!==input.strategy_id||prior.instrument_id!==input.instrument_id)return {status:"DENIED",reason:"IDEMPOTENCY_CONFLICT"};
      return prior.state==="HELD"?{status:"HELD",entry:prior,book_sequence:meta.sequence,occupied_global_usdt:meta.occupied_usdt}:{status:"DENIED",reason:"INTENT_ALREADY_POSTED_OR_RESOLVED"};
    }
    if(units(meta.consumed_loss_usd)!+units(meta.occupied_usd)!+usd>units(meta.campaign_limit_usd)!)return {status:"DENIED",reason:"CAMPAIGN_LOSS_BUDGET_EXHAUSTED"};
    const account=governance.accounts.find(a=>
      a.venue===input.venue && a.environment==="real" && a.uid_fingerprint===input.uid &&
      accessEnabled(a));
    const strategy=account?.strategies.find(s=>s.id===input.strategy_id && s.enabled);
    if(!account)return {status:"DENIED",reason:"ACCOUNT_NOT_ACTIVE"};
    if(!account.limits.allowed_instruments.includes(input.instrument_id))
      return {status:"DENIED",reason:"INSTRUMENT_NOT_ALLOWED_BY_ACCOUNT"};
    const maxOrder=units(account.limits.max_order_loss_usdt);
    const maxStrategy=units(strategy?.max_modelled_loss_usdt??account.limits.max_modelled_loss_usdt);
    if(maxOrder===null||maxStrategy===null||requested>maxOrder||requested>maxStrategy)
      return {status:"DENIED",reason:"ORDER_OR_STRATEGY_RISK_LIMIT"};
    const strategyOutstanding=units(await tx.get("risk_strategy_v2:"+strategyKey(input))??"0")!;
    if(strategyOutstanding+requested>maxStrategy)return {status:"DENIED",reason:"STRATEGY_AGGREGATE_RISK_LIMIT"};
    const occupied=new Map(externallyOccupied);
    for(const [owner,value] of Object.entries(meta.accounts)){
      if(!occupied.has(owner)&&units(value)!>0n)return {status:"DENIED",reason:"EXPOSURE_UNREGISTERED"};
      occupied.set(owner,(occupied.get(owner)??0n)+units(value)!);
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
      run_id:input.run_id, strategy_id:input.strategy_id, instrument_id:input.instrument_id,
      intent_id:input.intent_id, plan_hash:input.plan_hash,
      policy_revision:input.policy_revision, loss_usdt:input.loss_usdt,loss_usd:input.loss_usd,usd_conversion:input.usd_conversion,
      state:"HELD", created_at_ms:input.now_ms,
      reserve_source_digest:census.source_digest,
      reserve_source_read_floor_ms:census.source_read_floor_ms,
      transition_proof:null,
    };
    const next=await saveEntry(tx,meta,entry,requested);
    return {status:"HELD",entry,book_sequence:next.sequence,occupied_global_usdt:amount(total+requested)};
  });
}

/** Never release because of a timeout. Native source/ledger exact reconciliation
 * must provide the proof hash; all transition origins remain private to DO.
 */
export interface TerminalRiskLedgerProof {
  account_uid:string;run_id:string;intent_id:string;
  state:"FILLED"|"CANCELED"|"REJECTED";record_digest:string;cumulative_loss_usd:string;remaining_exposure_usdt:string;
}
export interface TerminalRiskReleaseProof {
  census:CompleteRiskCensus;source_proof:NativeSessionRiskProof;
  ledger:TerminalRiskLedgerProof;now_ms:number;
}

export async function transitionRisk(
  storage: any,
  key: string,
  plan_hash: string,
  to: RiskEntryState,
  exact_native_proof_digest: string,
  native: {connection_generation:number;expected_book_sequence:number;
    terminal?:TerminalRiskReleaseProof}|null = null,
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
    const meta=await tx.get(KEY);
    if(!validMeta(meta))return {status:"DENIED" as const,reason:"RISK_BOOK_MISSING_OR_CORRUPT"};
    if(await tx.get("generation")!==native.connection_generation||meta.sequence!==native.expected_book_sequence)return {status:"DENIED" as const,reason:"STALE_SESSION_OR_BOOK_SEQUENCE"};
    const entry:RiskEntry|undefined=await tx.get(entryKey(key));
    if(!entry||!validEntry(entry)||entry.plan_hash!==plan_hash||!transitions[entry.state].includes(to))return {status:"DENIED" as const,reason:"INVALID_RISK_TRANSITION"};
    if(to==="RELEASED"){
      const proof=native.terminal;
      if(!proof||(!entry.posted_at_ms&&(entry.state!=="HELD"||proof.ledger.state!=="REJECTED"))||!Number.isSafeInteger(proof.now_ms)
        ||proof.now_ms<=(entry.posted_at_ms??entry.created_at_ms)
        ||proof.ledger.account_uid!==entry.uid
        ||proof.ledger.run_id!==entry.run_id
        ||proof.ledger.intent_id!==entry.intent_id
        ||!["FILLED","CANCELED","REJECTED"].includes(proof.ledger.state)
        ||!HEX.test(proof.ledger.record_digest)||units(proof.ledger.cumulative_loss_usd)===null||units(proof.ledger.remaining_exposure_usdt)!==0n
        ||proof.source_proof.connection_generation!==native.connection_generation
        ||proof.source_proof.source_digest!==proof.census.source_digest
        ||!proof.source_proof.native_account_evidence_complete
        ||!proof.source_proof.native_order_evidence_complete
        ||!proof.source_proof.native_ledger_evidence_complete
        ||proof.census.source_read_floor_ms<=(entry.posted_at_ms??entry.created_at_ms))
        return {status:"DENIED" as const,reason:"TERMINAL_NATIVE_PROOF_UNAVAILABLE"};
      const governance:unknown=await tx.get(GOVERNANCE_STORAGE_KEY);
      const policy=validGovernance(governance)?projectRiskPolicy(governance):null;
      const occupied=policy?censusRisk(proof.census,policy,proof.now_ms):null;
      if(!occupied||!await applyCensusLoss(tx,meta,proof.census))
        return {status:"DENIED" as const,reason:"TERMINAL_ACCOUNT_EXPOSURE_NOT_FLAT"};
    }
    if(to==="RELEASED"){
      const seenKey="risk_loss_v2:"+accountKey(entry),previous=units(await tx.get(seenKey)??"0")!,current=units(native.terminal!.ledger.cumulative_loss_usd)!;
      if(current<previous)return {status:"DENIED" as const,reason:"CUMULATIVE_LOSS_REGRESSION"};
      meta.consumed_loss_usd=amount(units(meta.consumed_loss_usd)!+current-previous);await tx.put(seenKey,amount(current));
    }
    await saveEntry(tx,meta,{...entry,state:to,transition_proof:exact_native_proof_digest},to==="RELEASED"?-units(entry.loss_usdt)!:0n);
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
  native:{
    connection_generation:number;expected_book_sequence:number;
    census:CompleteRiskCensus;source_proof:NativeSessionRiskProof;now_ms:number;
  }|null,
):Promise<{status:"POST_AUTHORIZED"|"DENIED";reason?:string;book_sequence?:number;existing_modelled_loss_usdt?:string}>{
  if(!native||!HEX.test(plan_hash)||!HEX.test(proof_digest)||
    !Number.isSafeInteger(native.connection_generation)||native.connection_generation<1||
    !Number.isSafeInteger(native.expected_book_sequence)||native.expected_book_sequence<1||
    !Number.isSafeInteger(policy_revision)||policy_revision<1||
    native.source_proof.connection_generation!==native.connection_generation||
    native.source_proof.source_digest!==native.census.source_digest||
    !native.source_proof.native_account_evidence_complete||
    !native.source_proof.native_order_evidence_complete||
    !native.source_proof.native_ledger_evidence_complete||
    !Number.isSafeInteger(native.now_ms)||native.now_ms<=0)
    return {status:"DENIED",reason:"INVALID_NATIVE_POST_FENCE"};
  return storage.transaction(async(tx:any)=>{
    const generation:unknown=await tx.get("generation");
    if(generation!==native.connection_generation)
      return {status:"DENIED" as const,reason:"STALE_RUNTIME_GENERATION"};
    const governance:unknown=await tx.get(GOVERNANCE_STORAGE_KEY);
    if(!validGovernance(governance)||governance.revision!==policy_revision)
      return {status:"DENIED" as const,reason:"CENTRAL_REVOCATION_OR_POLICY_CHANGE"};
    const authority=projectRiskPolicy(governance);
    if(!authority?.enabled)
      return {status:"DENIED" as const,reason:"CENTRAL_OPERATOR_GRANT_INACTIVE"};
    // A fresh native inventory is mandatory AGAIN immediately before POST.
    // Positions, unmanaged orders and another account can change after RESERVE.
    const occupied=censusRisk(native.census,authority,native.now_ms);
    if(occupied===null)
      return {status:"DENIED" as const,reason:"POST_CENSUS_UNPROVEN_OR_STALE"};
    const stored=await tx.get(KEY);
    if(!validMeta(stored))return {status:"DENIED" as const,reason:"RISK_BOOK_MISSING_OR_CORRUPT"};
    if(!await applyCensusLoss(tx,stored,native.census))return {status:"DENIED" as const,reason:"CUMULATIVE_LOSS_SOURCE_REGRESSION"};
    if(stored.sequence!==native.expected_book_sequence)return {status:"DENIED" as const,reason:"STALE_RISK_BOOK_SEQUENCE"};
    const entry:RiskEntry|undefined=await tx.get(entryKey(key));
    if(!entry||!validEntry(entry)||entry.plan_hash!==plan_hash||entry.state!=="HELD"||entry.policy_revision!==policy_revision)return {status:"DENIED" as const,reason:"INTENT_NOT_HELD_EXACTLY_ONCE"};
    if(native.now_ms-entry.usd_conversion.observed_at_ms>5000)return {status:"DENIED" as const,reason:"USD_CONVERSION_STALE"};
    // Reusing a recently sampled RESERVE census is not a new source read.
    if(native.census.generated_at_ms<=entry.created_at_ms ||
       native.census.source_read_floor_ms<=entry.created_at_ms)
      return {status:"DENIED" as const,reason:"POST_CENSUS_NOT_AFTER_RESERVE"};
    // Every registered Real UID must have been read again. Identical
    // exchange content is valid; advancing only a command clock is not.
    if (!entry.reserve_source_digest ||
        !entry.reserve_source_read_floor_ms ||
        native.census.source_read_floor_ms <= entry.reserve_source_read_floor_ms ||
        native.census.source_digest===entry.reserve_source_digest)
      return {status:"DENIED" as const,reason:"POST_CENSUS_SOURCE_REUSED_OR_UNKNOWN"};
    const account=governance.accounts.find(a=>
      a.venue===entry.venue&&a.environment==="real"&&a.uid_fingerprint===entry.uid
      &&accessEnabled(a));
    if(!account||!account.limits.allowed_instruments.includes(entry.instrument_id))
      return {status:"DENIED" as const,reason:"ACCOUNT_OR_INSTRUMENT_REVOKED"};
    for(const [owner,value] of Object.entries(stored.accounts)){
      if(!occupied.has(owner)&&units(value)!>0n)return {status:"DENIED" as const,reason:"POST_OCCUPANCY_COHORT_MISMATCH"};
      occupied.set(owner,(occupied.get(owner)??0n)+units(value)!);
    }
    let total=0n;
    for(const [accountKey,value] of occupied){
      const limit=units(authority.account_limits_usdt[accountKey]);
      if(limit===null||value>limit)
        return {status:"DENIED" as const,reason:"POST_ACCOUNT_RISK_EXCEEDED"};
      total+=value;
    }
    const global=units(authority.global_limit_usdt);
    if(global===null||total>global)
      return {status:"DENIED" as const,reason:"POST_PORTFOLIO_RISK_EXCEEDED"};
    await saveEntry(tx,stored,{...entry,state:"POSTED",posted_at_ms:native.now_ms,transition_proof:proof_digest},0n);
    // Occupancy already includes the just-fenced reservation.
    // Executor will add its requested loss, so exclude only that candidate.
    return {status:"POST_AUTHORIZED" as const,book_sequence:stored.sequence+1,
      existing_modelled_loss_usdt:amount(total-units(entry.loss_usdt)!)};
  });
}

export async function readRiskBook(storage:any):Promise<RiskMeta|null>{const v=await storage.get(KEY);return validMeta(v)?v:null;}

