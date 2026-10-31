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
  /** Campaign entry costs are independent of modelled loss. */
  entry_upper_usd?:string;entry_proof_digest?:string;
  entry_settled_usd?:string;entry_settlement_revision?:number;entry_settlement_digest?:string;
  /** Cumulative verified expense above the original reservation, never a refund. */
  entry_bound_breach_usd?:string;
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
  entry_upper_usd?:string;entry_proof_digest?:string;
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

/** Evidence produced by the same authenticated native process after HELD.
 * Worker verifies the reserve window using its own clock, never native wall time.
 */
export interface CausalPostProof {
  reserve_anchor:string;
  native_incarnation:string;
  elapsed_to_proof_ms:number;
  account_reads:{
    account_key:string;private_generation:number;source_read_id:string;
    content_digest:string;started_after_held_ms:number;completed_after_held_ms:number;
  }[];
}
function validPostProof(proof:CausalPostProof,census:CompleteRiskCensus,key:string):boolean {
  return !!proof&&proof.reserve_anchor===key&&key.length<=512&&
    /^[0-9a-f]{32}$/.test(proof.native_incarnation)&&
    Number.isSafeInteger(proof.elapsed_to_proof_ms)&&
    proof.elapsed_to_proof_ms>=0&&proof.elapsed_to_proof_ms<=5000&&
    Array.isArray(proof.account_reads)&&proof.account_reads.length===census.account_keys.length&&
    proof.account_reads.every((read,i)=>read.account_key===census.account_keys[i]&&
      Number.isSafeInteger(read.private_generation)&&read.private_generation>0&&
      HEX.test(read.source_read_id)&&HEX.test(read.content_digest)&&
      Number.isSafeInteger(read.started_after_held_ms)&&
      Number.isSafeInteger(read.completed_after_held_ms)&&
      read.started_after_held_ms>=0&&
      read.started_after_held_ms<=read.completed_after_held_ms&&
      read.completed_after_held_ms<=proof.elapsed_to_proof_ms);
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
    && (entry.entry_upper_usd===undefined ||
        units(entry.entry_upper_usd)!>0n && units(entry.entry_settled_usd)!==null &&
        (entry.entry_bound_breach_usd===undefined
          ?units(entry.entry_settled_usd)!<=units(entry.entry_upper_usd)!
          :units(entry.entry_bound_breach_usd)!==null &&
           units(entry.entry_bound_breach_usd)===
           (units(entry.entry_settled_usd)!>units(entry.entry_upper_usd)!
             ?units(entry.entry_settled_usd)!-units(entry.entry_upper_usd)!:0n)) &&
        HEX.test(entry.entry_proof_digest??"") &&
        Number.isSafeInteger(entry.entry_settlement_revision) &&
        entry.entry_settlement_revision!>=0 &&
        (entry.entry_settlement_revision===0 ||
          HEX.test(entry.entry_settlement_digest??"")))
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
/** Immutable campaign inception, created only in the authenticated
 * installation transaction. It is an authority timestamp, NOT a claim
 * that older real-account activity has been reconciled. Legacy books
 * without this receipt remain NOT_PROVEN for entry opening.
 */
export interface CampaignOrigin {
  campaign_id:string;
  installation_id:string;
  started_at_ms:number;
}
function validCampaignOrigin(v:any,installation_id:string):v is CampaignOrigin {
  return !!v&&/^sha256:[0-9a-f]{64}$/.test(v.campaign_id)&&
    v.installation_id===installation_id&&
    Number.isSafeInteger(v.started_at_ms)&&v.started_at_ms>0;
}
export interface CampaignEntryBudget {
  campaign_id:string;limit_usd:string;consumed_usd:string;reserved_usd:string;
  /** Monotonic native over-reserve debit, not reusable budget headroom. */
  bound_breach_usd?:string;
  opening_source_digest:string;opening_as_of_ms:number;
  /** Immutable all-managed-real-account opening scope. New enrolled accounts
   * cannot silently inherit a historical campaign balance. */
  opening_account_keys:string[];
}
function validEntryBudget(v:any):v is CampaignEntryBudget{
  return !!v&&/^sha256:[0-9a-f]{64}$/.test(v.campaign_id)&&
    units(v.limit_usd)!>0n&&units(v.consumed_usd)!==null&&
    units(v.reserved_usd)!==null&&units(v.bound_breach_usd??"0")!==null&&
    units(v.consumed_usd)!+units(v.reserved_usd)!
      <=units(v.limit_usd)!+units(v.bound_breach_usd??"0")!&&
    HEX.test(v.opening_source_digest)&&
    Array.isArray(v.opening_account_keys)&&v.opening_account_keys.length>0&&
    v.opening_account_keys.length<=32&&
    v.opening_account_keys.every((k:string,i:number)=>
      /^okx:real:[0-9a-f]{64}$/.test(k)&&
      (i===0||v.opening_account_keys[i-1]<k))&&
    Number.isSafeInteger(v.opening_as_of_ms)&&v.opening_as_of_ms>0;
}
function openingCoversManagedCohort(
  budget:CampaignEntryBudget,accounts:{account_key:string;environment:string}[]
):boolean{
  const registered=accounts.filter(a=>a.environment==="real")
    .map(a=>a.account_key).sort();
  return registered.length===budget.opening_account_keys.length&&
    registered.every((key,i)=>key===budget.opening_account_keys[i]);
}
/** E1.1 history is a single full-cohort fact, not an unexplained
 * initial zero. Each managed real account must supply immutable native
 * finance provenance, complete order/fill/bill history through the same
 * cut-off, and an explicit cost total. An empty list is never zero spend. */
export interface CampaignEntryOpeningAccount {
  account_key:string;
  history_from_ms:number;
  history_through_ms:number;
  consumed_entry_usd:string;
  order_history_complete:true;
  fill_history_complete:true;
  bill_history_complete:true;
  finance_provenance_sealed:true;
  source_digest:string;
}
export interface CampaignEntryOpeningSource {
  campaign_id:string;limit_usd:string;consumed_usd:string;
  source_digest:string;as_of_ms:number;campaign_started_at_ms:number;
  accounts:CampaignEntryOpeningAccount[];
}
export async function openingDigest(source:CampaignEntryOpeningSource):Promise<string>{
  const payload=JSON.stringify([
    "okx.campaign-entry-opening/v1",
    source.campaign_id,source.limit_usd,source.consumed_usd,
    source.campaign_started_at_ms,source.as_of_ms,source.accounts
  ]);
  const bytes=new TextEncoder().encode(payload);
  const hash=await crypto.subtle.digest("SHA-256",bytes);
  return Array.from(new Uint8Array(hash),v=>v.toString(16).padStart(2,"0")).join("");
}
/** No public, MCP or WSS route. The future native importer must bind each
 * account row to independently authenticated venue history and the actual
 * campaign inception. A syntactically valid hash by itself is NOT such an
 * attestation. No production importer is connected until that exists.
 */
export async function installCampaignEntryProof(tx:any,source:CampaignEntryOpeningSource):Promise<boolean>{
  const meta:RiskMeta=await tx.get(KEY);
  const governance:unknown=await tx.get(GOVERNANCE_STORAGE_KEY);
  if(!validMeta(meta)||!validGovernance(governance)||
     meta.entry_budget!==undefined||meta.installation_id!==governance.installation_id||
     !validCampaignOrigin(meta.campaign_origin,meta.installation_id)||
     meta.campaign_origin.campaign_id!==governance.campaign_id||
     source.campaign_started_at_ms!==meta.campaign_origin.started_at_ms||
     meta.occupied_usdt!=="0"||meta.occupied_usd!=="0"||
     source.campaign_id!==governance.campaign_id||
     units(governance.campaign_entry_limit_usd)===null||
     units(governance.campaign_entry_limit_usd)!<=0n||
     units(source.limit_usd)!==units(governance.campaign_entry_limit_usd)||
     !HEX.test(source.source_digest)||!Number.isSafeInteger(source.as_of_ms)||
     source.as_of_ms<=0||!Number.isSafeInteger(source.campaign_started_at_ms)||
     source.campaign_started_at_ms<=0||source.campaign_started_at_ms>source.as_of_ms||
     units(source.limit_usd)!<=0n||units(source.consumed_usd)===null||
     units(source.consumed_usd)!>units(source.limit_usd)!||
     !Array.isArray(source.accounts))return false;
  const managed=governance.accounts
    .filter(a=>a.environment==="real")
    .map(a=>a.account_key).sort();
  if(managed.length===0||managed.length!==source.accounts.length)return false;
  let witnessed=0n;
  for(let i=0;i<managed.length;i++){
    const row=source.accounts[i];
    const cost=units(row?.consumed_entry_usd);
    if(!row||row.account_key!==managed[i]||
       !HEX.test(row.source_digest)||
       row.order_history_complete!==true||
       row.fill_history_complete!==true||
       row.bill_history_complete!==true||
       row.finance_provenance_sealed!==true||
       !Number.isSafeInteger(row.history_from_ms)||
       row.history_from_ms!==source.campaign_started_at_ms||
       !Number.isSafeInteger(row.history_through_ms)||
       row.history_through_ms!==source.as_of_ms||
       cost===null)return false;
    witnessed+=cost;
  }
  if(witnessed!==units(source.consumed_usd)||
     await openingDigest(source)!==source.source_digest)return false;
  await tx.put(KEY,{...meta,entry_budget:{
    campaign_id:source.campaign_id,limit_usd:source.limit_usd,
    consumed_usd:source.consumed_usd,reserved_usd:"0",bound_breach_usd:"0",
    opening_source_digest:source.source_digest,opening_as_of_ms:source.as_of_ms,
    opening_account_keys:managed,
  },sequence:meta.sequence+1});
  return true;
}
export interface RiskMeta { entry_budget?:CampaignEntryBudget; campaign_origin?:CampaignOrigin; source_as_of_ms:number; schema:typeof RISK_BOOK_SCHEMA; sequence:number; installation_id:string; occupied_usdt:string; occupied_usd:string; consumed_loss_usd:string; campaign_limit_usd:string; accounts:Record<string,string>; }
function validMeta(v:any):v is RiskMeta {return !!v&&v.schema===RISK_BOOK_SCHEMA&&Number.isSafeInteger(v.source_as_of_ms)&&v.source_as_of_ms>=0&&Number.isSafeInteger(v.sequence)&&v.sequence>=0&&typeof v.installation_id==="string"&&(v.entry_budget===undefined||validEntryBudget(v.entry_budget))&&(v.campaign_origin===undefined||validCampaignOrigin(v.campaign_origin,v.installation_id))&&(v.entry_budget===undefined||validCampaignOrigin(v.campaign_origin,v.installation_id)&&v.entry_budget.campaign_id===v.campaign_origin.campaign_id&&v.entry_budget.opening_as_of_ms>=v.campaign_origin.started_at_ms)&&units(v.occupied_usdt)!==null&&units(v.occupied_usd)!==null&&units(v.consumed_loss_usd)!==null&&units(v.campaign_limit_usd)!==null&&units(v.campaign_limit_usd)!>0n&&!!v.accounts&&Object.keys(v.accounts).length<=32&&Object.values(v.accounts).every(x=>units(x)!==null)&&Object.values(v.accounts).reduce<bigint>((sum,x)=>sum+units(x)!,0n)===units(v.occupied_usdt);}
export async function initializeRiskBook(
 tx:any,installation_id:string,campaign_origin?:CampaignOrigin
):Promise<void>{
 if(await tx.get(KEY)!==undefined)throw Error("RISK_ALREADY_INITIALIZED");
 if(campaign_origin!==undefined&&!validCampaignOrigin(campaign_origin,installation_id))
   throw Error("CAMPAIGN_ORIGIN_INVALID");
 await tx.put(KEY,{schema:RISK_BOOK_SCHEMA,source_as_of_ms:0,sequence:0,installation_id,
   ...(campaign_origin===undefined?{}:{campaign_origin}),
   occupied_usdt:"0",occupied_usd:"0",consumed_loss_usd:"0",
   campaign_limit_usd:"10",accounts:{}} satisfies RiskMeta);
}
/** A campaign ceiling may only be lowered. Existing loss and held risk remain charged. */
export async function lowerRiskBookLimit(tx:any,limit:string):Promise<boolean>{
 const meta=await tx.get(KEY),next=units(limit);
 if(!validMeta(meta)||next===null||next<=0n||next>=units(meta.campaign_limit_usd)!||next<units(meta.consumed_loss_usd)!+units(meta.occupied_usd)!)return false;
 await tx.put(KEY,{...meta,campaign_limit_usd:limit,sequence:meta.sequence+1});return true;
}
/** Same owner / same transaction as governance CAS. A lowered campaign
 * entry cap never refunds existing consumed spend or drops HELD reservations.
 * A missing imported opening balance remains missing, not zero.
 */
export async function lowerCampaignEntryLimit(
  tx:any,previousCap:string|undefined,nextCap:string|undefined
):Promise<boolean>{
  const meta:RiskMeta=await tx.get(KEY);
  if(!validMeta(meta)||!nextCap||units(nextCap)===null||units(nextCap)!<=0n||
     (previousCap!==undefined &&
       (units(previousCap)===null||units(nextCap)!>units(previousCap)!)))
    return false;
  const budget=meta.entry_budget;
  if(!budget)return true; // Proof is still uninstalled, hence NO E1 admissions.
  if(previousCap===undefined||units(budget.limit_usd)!==units(previousCap))
    return false;
  if(units(nextCap)!<units(budget.consumed_usd)!+units(budget.reserved_usd)!)
    return false;
  if(units(nextCap)===units(budget.limit_usd))return true;
  await tx.put(KEY,{...meta,entry_budget:{...budget,limit_usd:nextCap},sequence:meta.sequence+1});
  return true;
}
const entryKey=(key:string)=>"risk_entry_v2:"+key;
const noReserveKey=(key:string)=>"risk_no_reserve_v1:"+key;
/** Permanent negative fence in the existing Worker RiskBook, never occupancy. */
interface NoReserveFence {
  account_key:string;intent_id:string;plan_hash:string;
  record_digest:string;policy_revision:number;
}
function validNoReserveAddress(key:string,account:string,intent:string):boolean {
  const pieces=account.split(":");
  if(pieces.length!==3||pieces[0]!=="okx"||pieces[1]!=="real"||!HEX.test(pieces[2])||
    !TOKEN.test(intent))return false;
  const prefix=pieces.map(v=>v.length+":"+v).join("");
  const suffix=intent.length+":"+intent;
  if(!key.startsWith(prefix)||!key.endsWith(suffix))return false;
  const run=key.slice(prefix.length,key.length-suffix.length);
  const m=/^([1-9][0-9]{0,2}):(.+)$/.exec(run);
  return !!m&&m[2].length===Number(m[1])&&TOKEN.test(m[2]);
}
export async function inspectNoReserveFence(storage:any,key:string,plan:string):
  Promise<{sequence:number;policy_revision:number}|null>{
  return storage.transaction(async(tx:any)=>{
    const meta=await tx.get(KEY),f:NoReserveFence|undefined=await tx.get(noReserveKey(key));
    return validMeta(meta)&&f&&f.plan_hash===plan&&HEX.test(f.record_digest)&&
      validNoReserveAddress(key,f.account_key,f.intent_id)&&
      Number.isSafeInteger(f.policy_revision)&&f.policy_revision>0
      ?{sequence:meta.sequence,policy_revision:f.policy_revision}:null;
  });
}

const strategyKey=(entry:Pick<RiskEntry,"venue"|"environment"|"uid"|"strategy_id">)=>accountKey(entry)+":"+entry.strategy_id;
export async function inspectRisk(storage:any,key:string):Promise<{meta:RiskMeta;entry:RiskEntry}|null>{
 return storage.transaction(async(tx:any)=>{const meta=await tx.get(KEY),entry=await tx.get(entryKey(key));return validMeta(meta)&&entry&&validEntry(entry)?{meta,entry}:null;});
}
async function saveEntry(tx:any,meta:RiskMeta,entry:RiskEntry,delta:bigint){
 const a=accountKey(entry),s="risk_strategy_v2:"+strategyKey(entry);
 const strategy=units(await tx.get(s)??"0");if(strategy===null)throw Error("RISK_STRATEGY_AGGREGATE_CORRUPT");
 const aggregateKey="risk_account_v2:"+a;
 const prior=await tx.get(aggregateKey);
 if(prior===undefined && meta.accounts[a]!==undefined)throw Error("RISK_ACCOUNT_AGGREGATE_MISSING");
 const aggregate=prior??{active_reservations:0,occupied_usd:"0",occupied_usdt:"0",latest_activity_ms:0};
 if(aggregate.occupied_usdt!==(meta.accounts[a]??"0"))throw Error("RISK_ACCOUNT_AGGREGATE_CORRUPT");
 if(!Number.isSafeInteger(aggregate.active_reservations)||aggregate.active_reservations<0||units(aggregate.occupied_usd)===null)throw Error("RISK_ACCOUNT_AGGREGATE_CORRUPT");
 const count=aggregate.active_reservations+(delta>0n?1:delta<0n?-1:0);
 const usd=units(aggregate.occupied_usd)!+(delta===0n?0n:delta>0n?units(entry.loss_usd)!:-units(entry.loss_usd)!);
 if(count<0||usd<0n)throw Error("RISK_ACCOUNT_AGGREGATE_CORRUPT");
 await tx.put(aggregateKey,{active_reservations:count,occupied_usd:amount(usd),occupied_usdt:amount(units(aggregate.occupied_usdt)!+delta),latest_activity_ms:Date.now()});
 if(delta<0n && entry.entry_upper_usd!==undefined){
   const ceiling=units(entry.entry_upper_usd),settled=units(entry.entry_settled_usd);
   const reserved=units(meta.entry_budget?.reserved_usd);
   const refundable=ceiling!==null&&settled!==null&&ceiling>settled
     ?ceiling-settled:0n;
   if(!meta.entry_budget||ceiling===null||settled===null||reserved===null||
      reserved<refundable)
     throw Error("ENTRY_BUDGET_SETTLEMENT_CORRUPT");
   meta.entry_budget={...meta.entry_budget,reserved_usd:amount(reserved-refundable)};
 }
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
function censusRisk(census: CompleteRiskCensus, policy: RiskPolicy, now: number,
  postHeldCausal=false): Map<string,bigint> | null {
  if (census.complete !== true || !HEX.test(census.source_digest)
    || !HEX.test(census.content_digest)
    || !Number.isSafeInteger(census.source_read_floor_ms)
    || census.source_read_floor_ms <= 0
    || census.source_read_floor_ms > census.generated_at_ms
    || (!postHeldCausal &&
      (census.generated_at_ms > now || now - census.generated_at_ms > MAX_CENSUS_AGE_MS))
    || (postHeldCausal && census.generated_at_ms-census.source_read_floor_ms>MAX_CENSUS_AGE_MS)
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
    const key=riskKey(input);
    // A late RESERVE after a durable native no-send release cannot mint risk.
    if(await tx.get(noReserveKey(key))!==undefined)
      return {status:"DENIED",reason:"RESERVE_FENCED_NO_SEND"};
    if(!await applyCensusLoss(tx,meta,census))return {status:"DENIED",reason:"CUMULATIVE_LOSS_SOURCE_REGRESSION"};
    const prior:RiskEntry|undefined=await tx.get(entryKey(key));
    if(prior){
      if(!meta.entry_budget||prior.entry_upper_usd===undefined)
        return {status:"DENIED",reason:"ENTRY_RESERVATION_NOT_PROVEN"};
      if(!validEntry(prior)||prior.plan_hash!==input.plan_hash||prior.loss_usdt!==input.loss_usdt||prior.loss_usd!==input.loss_usd||prior.entry_upper_usd!==input.entry_upper_usd||prior.entry_proof_digest!==input.entry_proof_digest||JSON.stringify(prior.usd_conversion)!==JSON.stringify(input.usd_conversion)||prior.policy_revision!==input.policy_revision||prior.strategy_id!==input.strategy_id||prior.instrument_id!==input.instrument_id)return {status:"DENIED",reason:"IDEMPOTENCY_CONFLICT"};
      return prior.state==="HELD"?{status:"HELD",entry:prior,book_sequence:meta.sequence,occupied_global_usdt:meta.occupied_usdt}:{status:"DENIED",reason:"INTENT_ALREADY_POSTED_OR_RESOLVED"};
    }
    // E1: source-unproven opening spend is NEVER zero; the same transaction
    // must reserve the independent entry cost and the existing loss budget.
    const entryBudget=meta.entry_budget;
    const proposed=units(input.entry_upper_usd);
    if(!entryBudget || entryBudget.campaign_id!==governance.campaign_id||
       units(entryBudget.limit_usd)!==units(governance.campaign_entry_limit_usd))
      return {status:"DENIED",reason:"CAMPAIGN_ENTRY_BASELINE_NOT_PROVEN"};
    if(!openingCoversManagedCohort(entryBudget,governance.accounts))
      return {status:"DENIED",reason:"CAMPAIGN_OPENING_COHORT_CHANGED"};
    if(proposed===null||proposed<=0n||!HEX.test(input.entry_proof_digest??""))
      return {status:"DENIED",reason:"ENTRY_COST_PROOF_UNAVAILABLE"};
    if(units(entryBudget.bound_breach_usd??"0")!>0n)
      return {status:"DENIED",reason:"BUDGET_BOUND_BREACH"};
    if(units(entryBudget.consumed_usd)!+units(entryBudget.reserved_usd)!+proposed>
       units(entryBudget.limit_usd)!)
      return {status:"DENIED",reason:"CAMPAIGN_ENTRY_BUDGET_EXHAUSTED"};
    if(units(meta.consumed_loss_usd)!+units(meta.occupied_usd)!+usd>units(meta.campaign_limit_usd)!)return {status:"DENIED",reason:"CAMPAIGN_LOSS_BUDGET_EXHAUSTED"};
    const account=governance.accounts.find(a=>
      a.venue===input.venue && a.environment==="real" && a.uid_fingerprint===input.uid &&
      accessEnabled(a));
    const strategy=account?.strategies.find(s=>s.id===input.strategy_id && s.enabled);
    if(!account)return {status:"DENIED",reason:"ACCOUNT_NOT_ACTIVE"};
    if(!account.limits.allowed_instruments.includes(input.instrument_id))
      return {status:"DENIED",reason:"INSTRUMENT_NOT_ALLOWED_BY_ACCOUNT"};
    // An account-level entry cap is independent of the campaign entry
    // ceiling and the loss model. Missing saved cap is NOT unlimited.
    const accountEntryCap=units(account.limits.max_entry_usd);
    if(accountEntryCap===null||accountEntryCap<=0n)
      return {status:"DENIED",reason:"ACCOUNT_ENTRY_LIMIT_UNPROVEN"};
    if(proposed>accountEntryCap)
      return {status:"DENIED",reason:"ACCOUNT_ENTRY_LIMIT_EXCEEDED"};
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
      policy_revision:input.policy_revision, loss_usdt:input.loss_usdt,loss_usd:input.loss_usd,
      entry_upper_usd:input.entry_upper_usd,entry_settled_usd:"0",
      entry_proof_digest:input.entry_proof_digest,entry_settlement_revision:0,entry_settlement_digest:undefined,
      usd_conversion:input.usd_conversion,
      state:"HELD", created_at_ms:input.now_ms,
      reserve_source_digest:census.source_digest,
      reserve_source_read_floor_ms:census.source_read_floor_ms,
      transition_proof:null,
    };
    const next=await saveEntry(tx,meta,entry,requested);
    await tx.put(KEY,{...next,entry_budget:{...entryBudget,
      reserved_usd:amount(units(entryBudget.reserved_usd)!+proposed)}});
    return {status:"HELD",entry,book_sequence:next.sequence,occupied_global_usdt:amount(total+requested)};
  });
}

/** Never release because of a timeout. Native source/ledger exact reconciliation
 * must provide the proof hash; all transition origins remain private to DO.
 */
export interface TerminalRiskLedgerProof {
  account_uid:string;run_id:string;intent_id:string;
  state:"FILLED"|"CANCELED"|"REJECTED";record_digest:string;cumulative_loss_usd:string;remaining_exposure_usdt:string;
  /** Cumulative actual entry spend, source attested by native account ledger. */
  cumulative_entry_usd?:string;entry_settlement_revision?:number;entry_settlement_digest?:string;
}
export interface TerminalRiskReleaseProof {
  census:CompleteRiskCensus;source_proof:NativeSessionRiskProof;
  ledger:TerminalRiskLedgerProof;now_ms:number;
}

/// A no-send release is strictly narrower than an exchange-terminal release:
/// it can only follow the authenticated account owner's durable SQL rejection
/// with no native post claim. No venue facts or speculative credits are used.
export interface UnsentRiskReleaseProof {
  reservation_key:string;account_key:string;intent_id:string;plan_hash:string;
  request_hash:string;reason:string;record_digest:string;
}
export async function releaseUnsentRisk(
  storage:any,key:string,plan_hash:string,expected_sequence:number,
  generation:number,proof:UnsentRiskReleaseProof
):Promise<{status:"UPDATED"|"DENIED";reason?:string}> {
  if(!proof||!HEX.test(plan_hash)||!HEX.test(proof.request_hash)||
    !HEX.test(proof.record_digest)||!TOKEN.test(proof.intent_id)||
    !validNoReserveAddress(key,proof.account_key,proof.intent_id)||
    proof.reservation_key!==key||proof.plan_hash!==plan_hash||
    !/^okx:real:[0-9a-f]{64}$/.test(proof.account_key)||
    typeof proof.reason!=="string"||proof.reason.length<1||proof.reason.length>128||
    !Number.isSafeInteger(expected_sequence)||expected_sequence<0||
    !Number.isSafeInteger(generation)||generation<1)
    return {status:"DENIED",reason:"INVALID_UNSENT_SQL_PROOF"};
  return storage.transaction(async(tx:any)=>{
    const meta=await tx.get(KEY);
    if(!validMeta(meta))return {status:"DENIED" as const,reason:"RISK_BOOK_MISSING_OR_CORRUPT"};
    if(await tx.get("generation")!==generation||meta.sequence!==expected_sequence)
      return {status:"DENIED" as const,reason:"STALE_SESSION_OR_BOOK_SEQUENCE"};
    const entry:RiskEntry|undefined=await tx.get(entryKey(key));
    if(!entry){
      const previous:NoReserveFence|undefined=await tx.get(noReserveKey(key));
      if(previous){
        return previous.account_key===proof.account_key&&previous.intent_id===proof.intent_id&&
          previous.plan_hash===plan_hash&&previous.record_digest===proof.record_digest
          ?{status:"UPDATED" as const}
          :{status:"DENIED" as const,reason:"NO_RESERVE_FENCE_CONFLICT"};
      }
      const governance:unknown=await tx.get(GOVERNANCE_STORAGE_KEY);
      if(!validGovernance(governance))return {status:"DENIED" as const,reason:"GOVERNANCE_UNAVAILABLE"};
      await tx.put(noReserveKey(key),{
        account_key:proof.account_key,intent_id:proof.intent_id,plan_hash,
        record_digest:proof.record_digest,policy_revision:governance.revision
      } satisfies NoReserveFence);
      await tx.put(KEY,{...meta,sequence:meta.sequence+1});
      return {status:"UPDATED" as const};
    }
    if(!validEntry(entry)||entry.plan_hash!==plan_hash||
      !["HELD","POSTED"].includes(entry.state)||
      accountKey(entry)!==proof.account_key||entry.intent_id!==proof.intent_id)
      return {status:"DENIED" as const,reason:"UNSENT_RELEASE_IDENTITY_OR_STATE_MISMATCH"};
    // Even if central BEGIN_POST committed but its reply was lost, a native
    // SQL rejection CAS prevents this intention from ever reaching venue.
    // Only the native writer, already authenticated by WS generation, may
    // supply this proof. A replayed release is blocked by the tombstone.
    await saveEntry(tx,meta,{...entry,state:"RELEASED",transition_proof:proof.record_digest},-units(entry.loss_usdt)!);
    return {status:"UPDATED" as const};
  });
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
      if(!occupied)
        return {status:"DENIED" as const,reason:"TERMINAL_ACCOUNT_EXPOSURE_NOT_FLAT"};
    }
    if(to==="RELEASED" && entry.entry_upper_usd!==undefined){
      const ledger=native.terminal!.ledger;
      const cumulative=units(ledger.cumulative_entry_usd);
      const before=units(entry.entry_settled_usd);
      const ceiling=units(entry.entry_upper_usd);
      const budget=meta.entry_budget;
      const recordedBreach=units(entry.entry_bound_breach_usd??"0");
      // Terminal RELEASE is NOT a second way to debit the campaign.
      // Every actual USD charge must have crossed SETTLE_ENTRY first.
      // Only the already-booked cumulative amount/revision/source digest
      // may release unused reserved capacity. saveEntry owns that one
      // reserved-capacity refund in the SAME RiskBook transaction.
      if(!budget||cumulative===null||before===null||ceiling===null||
        cumulative!==before||recordedBreach===null||
        recordedBreach!==(cumulative>ceiling?cumulative-ceiling:0n)||
        !Number.isSafeInteger(ledger.entry_settlement_revision)||
        entry.entry_settlement_revision!<1||
        ledger.entry_settlement_revision!==entry.entry_settlement_revision||
        !HEX.test(ledger.entry_settlement_digest??"")||
        ledger.entry_settlement_digest!==entry.entry_settlement_digest)
        return {status:"DENIED" as const,reason:"TERMINAL_ENTRY_SETTLEMENT_UNPROVEN"};
    }
    // Commit source-proven loss changes only AFTER validating the entire
    // terminal entry proof. A DENIED RELEASE must not mutate either campaign
    // budget or advance the global RiskBook CAS sequence.
    if(to==="RELEASED" && !await applyCensusLoss(tx,meta,native.terminal!.census))
      return {status:"DENIED" as const,reason:"TERMINAL_ACCOUNT_EXPOSURE_NOT_FLAT"};
    if(to==="RELEASED"){
      const seenKey="risk_loss_v2:"+accountKey(entry),previous=units(await tx.get(seenKey)??"0")!,current=units(native.terminal!.ledger.cumulative_loss_usd)!;
      if(current<previous)return {status:"DENIED" as const,reason:"CUMULATIVE_LOSS_REGRESSION"};
      meta.consumed_loss_usd=amount(units(meta.consumed_loss_usd)!+current-previous);await tx.put(seenKey,amount(current));
    }
    await saveEntry(tx,meta,{...entry,state:to,transition_proof:exact_native_proof_digest},to==="RELEASED"?-units(entry.loss_usdt)!:0n);
    return {status:"UPDATED" as const};
  });
}


/** Idempotent cumulative partial settlement; called ONLY by an authenticated
 * native source-verified fill/bill handler once its typed binding is complete.
 * A lost reply never doubles consumed entry money. No public MCP path.
 */
export async function settleEntrySpend(
  storage:any,key:string,plan_hash:string,native:{
    connection_generation:number;expected_book_sequence:number;
    cumulative_entry_usd:string;revision:number;source_digest:string;
  }
):Promise<{status:"UPDATED"|"DENIED";reason?:string;book_sequence?:number}>{
  const total=units(native.cumulative_entry_usd);
  if(!HEX.test(plan_hash)||!HEX.test(native.source_digest)||
     total===null||!Number.isSafeInteger(native.revision)||native.revision<1||
     !Number.isSafeInteger(native.expected_book_sequence)||native.expected_book_sequence<1||
     !Number.isSafeInteger(native.connection_generation)||native.connection_generation<1)
    return {status:"DENIED",reason:"ENTRY_SETTLEMENT_INVALID"};
  return storage.transaction(async(tx:any)=>{
    const meta:RiskMeta=await tx.get(KEY);
    const entry:RiskEntry|undefined=await tx.get(entryKey(key));
    if(!validMeta(meta)||!meta.entry_budget||!entry||!validEntry(entry)||
       entry.plan_hash!==plan_hash||
       await tx.get("generation")!==native.connection_generation)
      return {status:"DENIED" as const,reason:"ENTRY_SETTLEMENT_SOURCE_OR_STATE_UNPROVEN"};
    const before=units(entry.entry_settled_usd),ceiling=units(entry.entry_upper_usd);
    if(before===null||ceiling===null||total<before||
       native.revision<entry.entry_settlement_revision!)
      return {status:"DENIED" as const,reason:"ENTRY_SETTLEMENT_REGRESSION"};
    if(native.revision===entry.entry_settlement_revision){
      // An ACK can be lost after the transaction committed; meanwhile the
      // RiskBook sequence may advance for another account or a terminal
      // release. Replaying the EXACT cumulative amount/revision/digest is
      // read-only even with the original now-stale CAS, and must not debit
      // entry spend twice. Never accept a future or unknown CAS sequence.
      if(native.expected_book_sequence>meta.sequence ||
         !["POSTED","UNKNOWN","OPEN","RELEASED"].includes(entry.state)||
         total!==before||entry.entry_settlement_digest!==native.source_digest)
        return {status:"DENIED" as const,reason:"ENTRY_SETTLEMENT_REVISION_CONFLICT"};
      return {status:"UPDATED" as const,book_sequence:meta.sequence};
    }
    // A new authenticated native SQL revision can be posted after RELEASE:
    // late venue bills are actual campaign spend, not a reason to re-open a
    // position or resurrect a consumed POST fence. Exact CAS still required.
    const released=entry.state==="RELEASED";
    if(!["POSTED","UNKNOWN","OPEN","RELEASED"].includes(entry.state)||
       meta.sequence!==native.expected_book_sequence ||
       native.source_digest===entry.entry_settlement_digest ||
       (released && (!entry.transition_proof||!HEX.test(entry.transition_proof))))
      return {status:"DENIED" as const,reason:"ENTRY_SETTLEMENT_SOURCE_OR_STATE_UNPROVEN"};
    const delta=total-before,reserved=units(meta.entry_budget.reserved_usd);
    // RELEASE refunded all unused entry capacity. Charge a late bill as new
    // consumption but NEVER subtract a different live intent's reservation.
    const oldUnsettled=ceiling>before?ceiling-before:0n;
    const reservedDebit=released?0n:(delta<oldUnsettled?delta:oldUnsettled);
    if(reserved===null||reserved<reservedDebit)
      return {status:"DENIED" as const,reason:"ENTRY_SETTLEMENT_RESERVE_CORRUPT"};
    const previousOver=before>ceiling?before-ceiling:0n;
    const currentOver=total>ceiling?total-ceiling:0n;
    const entryBreachDelta=currentOver-previousOver;
    const bound=units(meta.entry_budget.bound_breach_usd??"0");
    const consumed=units(meta.entry_budget.consumed_usd);
    const limit=units(meta.entry_budget.limit_usd);
    if(bound===null||consumed===null||limit===null)
      return {status:"DENIED" as const,reason:"ENTRY_BOUND_BREACH_STATE_CORRUPT"};
    const afterConsumed=consumed+delta,afterReserved=reserved-reservedDebit;
    // Another valid entry may already have reserved the refunded capacity.
    // Never conceal a late over-budget charge: book it all and latch the
    // existing bound-breach gate to refuse further entries.
    const aggregateOver=afterConsumed+afterReserved>limit
      ?afterConsumed+afterReserved-limit:0n;
    const breachDelta=entryBreachDelta>aggregateOver?entryBreachDelta:aggregateOver;
    meta.entry_budget={...meta.entry_budget,
       consumed_usd:amount(afterConsumed),
       reserved_usd:amount(afterReserved),
       bound_breach_usd:amount(bound+breachDelta)};
    const next=await saveEntry(tx,meta,{...entry,entry_settled_usd:amount(total),
      entry_bound_breach_usd:amount(currentOver),
      entry_settlement_revision:native.revision,entry_settlement_digest:native.source_digest,
      // Preserve the original terminal release witness and state.
      transition_proof:released?entry.transition_proof:native.source_digest},0n);
    return {status:"UPDATED" as const,book_sequence:next.sequence,
      reason:breachDelta>0n?"BUDGET_BOUND_BREACH":undefined};
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
    causal_proof:CausalPostProof;
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
    !Number.isSafeInteger(native.now_ms)||native.now_ms<=0||
    !validPostProof(native.causal_proof,native.census,key))
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
    const occupied=censusRisk(native.census,authority,native.now_ms,true);
    if(occupied===null)
      return {status:"DENIED" as const,reason:"POST_CENSUS_UNPROVEN_OR_STALE"};
    const stored=await tx.get(KEY);
    if(!validMeta(stored))return {status:"DENIED" as const,reason:"RISK_BOOK_MISSING_OR_CORRUPT"};
    if(!await applyCensusLoss(tx,stored,native.census))return {status:"DENIED" as const,reason:"CUMULATIVE_LOSS_SOURCE_REGRESSION"};
    if(stored.sequence!==native.expected_book_sequence)return {status:"DENIED" as const,reason:"STALE_RISK_BOOK_SEQUENCE"};
    const entry:RiskEntry|undefined=await tx.get(entryKey(key));
    if(!entry||!validEntry(entry)||entry.plan_hash!==plan_hash||entry.state!=="HELD"||entry.policy_revision!==policy_revision)return {status:"DENIED" as const,reason:"INTENT_NOT_HELD_EXACTLY_ONCE"};
    if(units(stored.entry_budget?.bound_breach_usd??"0")!>0n)
      return {status:"DENIED" as const,reason:"BUDGET_BOUND_BREACH"};
    if(!stored.entry_budget||entry.entry_upper_usd===undefined||
       stored.entry_budget.campaign_id!==governance.campaign_id||
       units(stored.entry_budget.limit_usd)!==units(governance.campaign_entry_limit_usd)||
       !openingCoversManagedCohort(stored.entry_budget,governance.accounts)||
       !HEX.test(entry.entry_proof_digest??""))
      return {status:"DENIED" as const,reason:"ENTRY_RESERVATION_NOT_PROVEN"};
    // The anchor lives in Worker storage and uses Worker time only; the
    // authenticated native evidence uses elapsed ticks since HELD instead.
    if(native.now_ms<entry.created_at_ms||
       native.now_ms-entry.created_at_ms>5000)
      return {status:"DENIED" as const,reason:"POST_EVIDENCE_WINDOW_EXPIRED"};
    if(!entry.reserve_source_digest||!entry.reserve_source_read_floor_ms)
      return {status:"DENIED" as const,reason:"RESERVE_SOURCE_NOT_RECORDED"};
    // Identical exchange content from independently completed reads is valid.
    // Native source read IDs, coverage and relative ordering are checked
    // against the full registered cohort by validPostProof above.
    const account=governance.accounts.find(a=>
      a.venue===entry.venue&&a.environment==="real"&&a.uid_fingerprint===entry.uid
      &&accessEnabled(a));
    if(!account||!account.limits.allowed_instruments.includes(entry.instrument_id))
      return {status:"DENIED" as const,reason:"ACCOUNT_OR_INSTRUMENT_REVOKED"};
    const freshAccountCap=units(account.limits.max_entry_usd);
    const reservedEntryUpper=units(entry.entry_upper_usd);
    if(freshAccountCap===null||freshAccountCap<=0n||reservedEntryUpper===null||
       reservedEntryUpper>freshAccountCap)
      return {status:"DENIED" as const,reason:"POST_ACCOUNT_ENTRY_LIMIT_UNPROVEN_OR_EXCEEDED"};
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

