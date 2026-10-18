import {accountPolicyRequest,defaultAccountPolicy,validAccountPolicy} from "./account_policy.js";
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
