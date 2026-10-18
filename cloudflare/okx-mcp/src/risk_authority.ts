import {validGovernance,projectRiskPolicy} from "./account_governance.js";
import {beginRiskPost,readRiskBook,reserveRisk,transitionRisk,
  type CompleteRiskCensus,type NativeSessionRiskProof,type RiskReservation} from "./risk_book.js";

const COMMAND_SCHEMA="okx.risk-command/v1";
const RESULT_SCHEMA="okx.risk-result/v1";
const TOKEN=/^[A-Za-z0-9_-]{8,128}$/;
const SHA=/^[0-9a-f]{64}$/;
type ResultStatus="DENIED"|"HELD"|"POST_AUTHORIZED"|"UPDATED";
interface Result {
  schema:typeof RESULT_SCHEMA;command_id:string;status:ResultStatus;
  reason:string|null;reservation_key:string|null;
  risk_book_sequence:number|null;policy_revision:number|null;
}
interface Session {
  connection_generation:number;
  runtime_profile:"production"|"demo_acceptance"|"unverified";
  hello:boolean;
}
function deny(id:string,reason:string):Result {
  return {schema:RESULT_SCHEMA,command_id:id,status:"DENIED",reason,
    reservation_key:null,risk_book_sequence:null,policy_revision:null};
}
function isObject(v:unknown):v is Record<string,unknown> {
  return v!==null&&typeof v==="object"&&!Array.isArray(v);
}
function nativeEvidence(raw:unknown,generation:number):NativeSessionRiskProof|null {
  if(!isObject(raw)||raw.complete!==true
    ||raw.native_account_evidence_complete!==true
    ||raw.native_order_evidence_complete!==true
    ||raw.native_ledger_evidence_complete!==true
    ||typeof raw.source_digest!=="string"||!SHA.test(raw.source_digest))
    return null;
  return {
    connection_generation:generation,source_digest:raw.source_digest,
    native_account_evidence_complete:true,native_order_evidence_complete:true,
    native_ledger_evidence_complete:true,
  };
}
/** Risk commands arrive only on an authenticated WebSocket from the native
 * Windows owner. No OAuth MCP or JSON /dispatch caller may use this method.
 * All material grant/reservation checks are repeated inside RiskBook's DO tx.
 */
export async function handleNativeRiskCommand(
 storage:any,session:Session,raw:unknown,now_ms:number
):Promise<Result>{
  const id=isObject(raw)&&typeof raw.command_id==="string"&&TOKEN.test(raw.command_id)
    ?raw.command_id:"invalid_risk_command";
  if(session.runtime_profile!=="production"||!session.hello||
    !Number.isSafeInteger(session.connection_generation)||session.connection_generation<1)
    return deny(id,"NATIVE_PRODUCTION_SESSION_REQUIRED");
  if(!isObject(raw)||raw.schema!==COMMAND_SCHEMA||!TOKEN.test(id)
    ||!isObject(raw.action)||!Number.isSafeInteger(now_ms)||now_ms<=0)
    return deny(id,"INVALID_TYPED_RISK_COMMAND");
  const a=raw.action;
  if(a.action==="RESERVE"){
    const proof=nativeEvidence(a.census,session.connection_generation);
    if(!proof||!isObject(a.census)||!Array.isArray(a.census.account_keys)||
      !Array.isArray(a.census.occupied))
      return deny(id,"ALL_ACCOUNT_NATIVE_EVIDENCE_UNPROVEN");
    if(typeof a.venue!=="string"||typeof a.account_uid!=="string"||
      typeof a.run_id!=="string"||typeof a.intent_id!=="string"||typeof a.strategy_id!=="string"||
      typeof a.instrument_id!=="string"||
      typeof a.plan_sha256!=="string"||typeof a.requested_loss_usdt!=="string"||
      !Number.isSafeInteger(a.policy_revision))
      return deny(id,"INVALID_RESERVATION_INPUT");
    const governance:unknown=await storage.get("account_governance_v2");
    if(!validGovernance(governance))
      return deny(id,"CENTRAL_GOVERNANCE_UNAVAILABLE");
    const policy=projectRiskPolicy(governance);
    if(!policy?.enabled)return deny(id,"CENTRAL_REAL_AUTHORIZATION_INACTIVE");
    const census:CompleteRiskCensus={
      complete:true,source_digest:proof.source_digest,
      generated_at_ms:a.census.generated_at_ms as number,
      account_keys:a.census.account_keys as string[],
      occupied:a.census.occupied as CompleteRiskCensus["occupied"],
    };
    const input:RiskReservation={
      venue:a.venue,environment:"real",uid:a.account_uid,run_id:a.run_id,
      intent_id:a.intent_id,strategy_id:a.strategy_id,instrument_id:a.instrument_id,
      plan_hash:a.plan_sha256,loss_usdt:a.requested_loss_usdt,
      policy_revision:a.policy_revision as number,now_ms,
    };
    const decision=await reserveRisk(storage,policy,census,input,proof);
    return decision.status==="HELD"
      ?{schema:RESULT_SCHEMA,command_id:id,status:"HELD",reason:null,
        reservation_key:decision.entry.key,risk_book_sequence:decision.book_sequence,
        policy_revision:decision.entry.policy_revision}
      :deny(id,decision.reason);
  }
  if(a.action==="BEGIN_POST"){
    if(typeof a.reservation_key!=="string"||typeof a.plan_sha256!=="string"
      ||!Number.isSafeInteger(a.policy_revision)||!Number.isSafeInteger(a.expected_risk_book_sequence)
      ||typeof a.source_proof_sha256!=="string")
      return deny(id,"INVALID_POST_FENCE");
    const proof=nativeEvidence(a.census,session.connection_generation);
    if(!proof||!isObject(a.census)||!Array.isArray(a.census.account_keys)||
      !Array.isArray(a.census.occupied))
      return deny(id,"POST_CENSUS_UNVERIFIED");
    const census:CompleteRiskCensus={
      complete:true,source_digest:proof.source_digest,
      generated_at_ms:a.census.generated_at_ms as number,
      account_keys:a.census.account_keys as string[],
      occupied:a.census.occupied as CompleteRiskCensus["occupied"],
    };
    const result=await beginRiskPost(storage,a.reservation_key,a.plan_sha256,
      a.policy_revision as number,a.source_proof_sha256,
      {connection_generation:session.connection_generation,
       expected_book_sequence:a.expected_risk_book_sequence as number,
       census,source_proof:proof,now_ms});
    return result.status==="POST_AUTHORIZED"
      ?{schema:RESULT_SCHEMA,command_id:id,status:"POST_AUTHORIZED",reason:null,
        reservation_key:a.reservation_key,risk_book_sequence:result.book_sequence!,
        policy_revision:a.policy_revision as number}
      :deny(id,result.reason??"POST_FENCE_DENIED");
  }
  if(a.action==="RESOLVE"){
    if(typeof a.reservation_key!=="string"||typeof a.plan_sha256!=="string"
      ||!Number.isSafeInteger(a.expected_risk_book_sequence)||
      typeof a.source_proof_sha256!=="string")
      return deny(id,"INVALID_RESOLUTION_INPUT");
    // A digest is not sufficient evidence of a terminal FLAT reconciliation:
    // do not release exchange risk without independently source-proven close.
    if(a.next_state==="RELEASED")
      return deny(id,"SOURCE_PROVEN_TERMINAL_RELEASE_NOT_INSTALLED");
    if(a.next_state!=="UNKNOWN"&&a.next_state!=="OPEN")
      return deny(id,"INVALID_RESOLUTION_STATE");
    const status=await transitionRisk(storage,a.reservation_key,a.plan_sha256,
      a.next_state,a.source_proof_sha256,
      {connection_generation:session.connection_generation,
       expected_book_sequence:a.expected_risk_book_sequence as number});
    if(status.status!=="UPDATED")return deny(id,status.reason??"RISK_RESOLUTION_DENIED");
    const book=await readRiskBook(storage);
    const entry=book?.entries.find(e=>e.key===a.reservation_key&&e.plan_hash===a.plan_sha256);
    return entry&&book
      ?{schema:RESULT_SCHEMA,command_id:id,status:"UPDATED",reason:null,
        reservation_key:entry.key,risk_book_sequence:book.sequence,
        policy_revision:entry.policy_revision}
      :deny(id,"RISK_BOOK_READBACK_FAILED");
  }
  return deny(id,"UNSUPPORTED_RISK_COMMAND");
}
