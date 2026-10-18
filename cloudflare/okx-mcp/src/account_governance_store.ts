import {defaultAccountPolicy,validAccountPolicy} from "./account_policy.js";
import {GOVERNANCE_SCHEMA,migrateV1,reviseGovernance,validGovernance,type GovernanceUpdate,type Governance} from "./account_governance.js";
import {readRiskBook,units,amount,type RiskBook} from "./risk_book.js";

export const GOVERNANCE_STORAGE_KEY="account_governance_v2" as const;
const LEGACY_KEY="account_policy_v1";
const MAX_UPDATE_BYTES=32*1024;
function isRecord(v:unknown):v is Record<string,unknown>{
  return v!==null&&typeof v==="object"&&!Array.isArray(v);
}
function parseUpdate(raw:unknown):GovernanceUpdate|null{
  if(!isRecord(raw)||Object.keys(raw).sort().join(",")!==
    "accounts,expected_revision,max_total_modelled_loss_usdt,trading_requested")return null;
  if(!Number.isSafeInteger(raw.expected_revision)||typeof raw.trading_requested!=="boolean"
    ||typeof raw.max_total_modelled_loss_usdt!=="string"
    ||!Array.isArray(raw.accounts)||raw.accounts.length>32)return null;
  return raw as unknown as GovernanceUpdate;
}
function result(policy:Governance,book:RiskBook){
  let reserved=0n,unknown=0;
  for(const item of book.entries){
    if(item.state==="RELEASED")continue;
    reserved+=units(item.loss_usdt)!;
    if(item.state==="UNKNOWN")unknown++;
  }
  return {
    ...policy,
    risk_book_sequence:book.sequence,
    risk_book_occupied_loss_usdt:amount(reserved),
    risk_book_unknown_count:unknown,
    complete_all_account_census:false,
    available_for_new_risk_usdt:null,
    available_risk_reason:"NATIVE_COMPLETE_ACCOUNT_CENSUS_UNPROVEN",
    effective_trading:false,
    effective_reason:"RUST_NATIVE_GOVERNANCE_AND_SOURCE_RESERVATION_NOT_ACCEPTED",
    status:"CONFIGURED"
  };
}
/** Single Durable Object CAS owner, with atomic migration. Writes never accept
 * identity_verified or operator_authorization from an operator's configure.
 */
export async function governanceRequest(
  storage:any, method:string, raw?:unknown
):Promise<{status:number;body:Record<string,unknown>}>{
  if(method!=="GET"&&method!=="POST")
    return {status:405,body:{status:"FAILED",reason:"METHOD_NOT_ALLOWED"}};
  const input=method==="POST"?parseUpdate(raw):null;
  if(method==="POST"&&!input)
    return {status:400,body:{status:"FAILED",reason:"INVALID_GOVERNANCE_REQUEST"}};
  if(method==="POST"&&JSON.stringify(raw).length>MAX_UPDATE_BYTES)
    return {status:413,body:{status:"FAILED",reason:"GOVERNANCE_REQUEST_TOO_LARGE"}};
  return storage.transaction(async(tx:any)=>{
    const stored:unknown=await tx.get(GOVERNANCE_STORAGE_KEY);
    let policy:Governance;
    if(stored===undefined){
      const legacy:unknown=await tx.get(LEGACY_KEY);
      if(legacy!==undefined&&!validAccountPolicy(legacy))
        return {status:503,body:{status:"FAILED",reason:"LEGACY_POLICY_CORRUPT"}};
      const converted=migrateV1(legacy===undefined?defaultAccountPolicy():legacy);
      if(converted===null)return {status:503,body:{status:"FAILED",reason:"GOVERNANCE_MIGRATION_INVALID"}};
      policy=converted;
    } else if(!validGovernance(stored)){
      return {status:503,body:{status:"FAILED",reason:"GOVERNANCE_CORRUPT"}};
    } else policy=stored;
    const book=await readRiskBook(tx);
    if(book===null)return {status:503,body:{status:"FAILED",reason:"RISK_BOOK_CORRUPT"}};
    // Materialize v2 only after BOTH policy and risk journal are valid.
    if(stored===undefined)await tx.put(GOVERNANCE_STORAGE_KEY,policy);
    if(method==="GET")return {status:200,body:result(policy,book)};
    if(input===null) return {status:400,body:{status:"FAILED",reason:"INVALID_GOVERNANCE_REQUEST"}};
    const revised=reviseGovernance(policy,input);
    if(!revised){
      return {status:409,body:{status:"FAILED",reason:"GOVERNANCE_REVISION_OR_LIMITS_REJECTED",revision:policy.revision}};
    }
    // Without native grant/identity attestation, writes never create authority.
    await tx.put(GOVERNANCE_STORAGE_KEY,revised);
    return {status:200,body:result(revised,book)};
  });
}


/**
 * Enrolment proof comes ONLY from the Worker-side authenticated Rust
 * trader_snapshot, never from an MCP-supplied UID. Metadata registration
 * does not activate the account or the separate operator authorization.
 */
export async function enrollObservedAccount(
  storage:any,
  observed:{uid_fingerprint:string;venue:"okx";environment:"real"|"demo";label:string}
):Promise<{status:number;body:Record<string,unknown>}>{
  if(!/^[0-9a-f]{64}$/.test(observed.uid_fingerprint)
    ||observed.venue!=="okx"
    ||!["real","demo"].includes(observed.environment)
    ||!observed.label||observed.label.length>80)
    return {status:400,body:{status:"FAILED",reason:"INVALID_AUTHENTICATED_ACCOUNT_OBSERVATION"}};
  return storage.transaction(async(tx:any)=>{
    const previous:unknown=await tx.get(GOVERNANCE_STORAGE_KEY);
    if(!validGovernance(previous))
      return {status:409,body:{status:"FAILED",reason:"GOVERNANCE_NOT_INITIALIZED_OR_CORRUPT"}};
    const book=await readRiskBook(tx);
    if(book===null)return {status:503,body:{status:"FAILED",reason:"RISK_BOOK_CORRUPT"}};
    const already=previous.accounts.find(a=>
      a.venue===observed.venue&&a.environment===observed.environment
      &&a.uid_fingerprint===observed.uid_fingerprint);
    if(already) return {status:200,body:{
      status:"ALREADY_REGISTERED",revision:previous.revision,
      account:already,effective_trading:false
    }};
    if(previous.accounts.length>=32||previous.revision===Number.MAX_SAFE_INTEGER)
      return {status:409,body:{status:"FAILED",reason:"ACCOUNT_REGISTRY_CAPACITY"}};
    const id=observed.venue+"_"+observed.environment+"_"+observed.uid_fingerprint.slice(0,12);
    if(previous.accounts.some(a=>a.id===id))
      return {status:409,body:{status:"FAILED",reason:"ACCOUNT_ID_COLLISION"}};
    const account={
      id,label:observed.label,venue:observed.venue,
      environment:observed.environment,uid_fingerprint:observed.uid_fingerprint,
      identity_verified:true,enabled:false,strategies:[],
      limits:{
        max_modelled_loss_usdt:previous.operator_authorization.ceiling_usdt,
        max_order_loss_usdt:previous.operator_authorization.ceiling_usdt,
        max_notional_usdt:null,max_leverage:null,
        max_daily_realized_loss_usdt:null,max_drawdown_ratio:null,
        allowed_instruments:[]
      }
    };
    const next={...previous,revision:previous.revision+1,
      trading_requested:false,accounts:[...previous.accounts,account]};
    if(!validGovernance(next))
      return {status:503,body:{status:"FAILED",reason:"REGISTRATION_INVARIANTS_REJECTED"}};
    await tx.put(GOVERNANCE_STORAGE_KEY,next);
    return {status:200,body:{
      status:"REGISTERED_DISABLED",revision:next.revision,
      account,effective_trading:false
    }};
  });
}


/**
 * Native central operator STOP: independent of app restart or account selection,
 * and never releases UNKNOWN / OPEN entries. It does not delete authorization.
 */
export async function stopNewRealRisk(storage:any):Promise<{status:number;body:Record<string,unknown>}>{
  return storage.transaction(async(tx:any)=>{
    const v:unknown=await tx.get(GOVERNANCE_STORAGE_KEY);
    if(!validGovernance(v))return {status:503,body:{status:"FAILED",reason:"GOVERNANCE_UNAVAILABLE"}};
    if(!v.trading_requested)return {status:200,body:{status:"STOPPED",revision:v.revision,
      effective_trading:false}};
    if(v.revision===Number.MAX_SAFE_INTEGER)
      return {status:503,body:{status:"FAILED",reason:"REVISION_EXHAUSTED"}};
    const next:Governance={...v,revision:v.revision+1,trading_requested:false};
    await tx.put(GOVERNANCE_STORAGE_KEY,next);
    return {status:200,body:{status:"STOPPED",revision:next.revision,effective_trading:false}};
  });
}

/**
 * Explicit OAuth operator authorization is a distinct CAS from selecting
 * business limits. No risk budget may exceed the already recorded grant
 * ceiling. Authentication of this operation belongs exclusively to the
 * Worker OAuth tool boundary, never to public /dispatch or native WS.
 */
export async function activateExistingOperatorGrant(
 storage:any,expectedRevision:number,approvalId:string
):Promise<{status:number;body:Record<string,unknown>}>{
  if(!Number.isSafeInteger(expectedRevision)||expectedRevision<0||
    !/^sha256:[0-9a-f]{64}$/.test(approvalId)||
    /^sha256:0{64}$/.test(approvalId))
    return {status:400,body:{status:"FAILED",reason:"INVALID_OPERATOR_AUTHORIZATION"}};
  return storage.transaction(async(tx:any)=>{
    const v:unknown=await tx.get(GOVERNANCE_STORAGE_KEY);
    if(!validGovernance(v))return {status:503,body:{status:"FAILED",reason:"GOVERNANCE_UNAVAILABLE"}};
    if(expectedRevision!==v.revision||v.revision===Number.MAX_SAFE_INTEGER)
      return {status:409,body:{status:"FAILED",reason:"STALE_AUTHORIZATION_REVISION",revision:v.revision}};
    if(v.operator_authorization.status==="REVOKED")
      return {status:409,body:{status:"FAILED",reason:"AUTHORIZATION_REVOKED"}};
    if(v.operator_authorization.status==="ACTIVE")
      return {status:409,body:{status:"FAILED",reason:"AUTHORIZATION_ALREADY_ACTIVE"}};
    const next:Governance={...v,revision:v.revision+1,trading_requested:false,
      operator_authorization:{...v.operator_authorization,status:"ACTIVE",id:approvalId}};
    if(!validGovernance(next))return {status:503,body:{status:"FAILED",reason:"AUTHORIZATION_INVALID"}};
    await tx.put(GOVERNANCE_STORAGE_KEY,next);
    return {status:200,body:{
      status:"AUTHORIZED_NOT_TRADING",revision:next.revision,
      authorized_risk_ceiling_usdt:next.operator_authorization.ceiling_usdt,
      effective_trading:false
    }};
  });
}
