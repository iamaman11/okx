import {validGovernance,projectRiskPolicy,accessEnabled,GOVERNANCE_STORAGE_KEY} from "./account_governance.js";
import {amount,units,beginRiskPost,readRiskBook,inspectRisk,reserveRisk,transitionRisk,
  type CompleteRiskCensus,type NativeSessionRiskProof,type CausalPostProof,type RiskReservation,
  type TerminalRiskReleaseProof,type UnsentRiskReleaseProof,releaseUnsentRisk,inspectNoReserveFence} from "./risk_book.js";

const COMMAND_SCHEMA="okx.risk-command/v1";
const RESULT_SCHEMA="okx.risk-result/v1";
const TOKEN=/^[A-Za-z0-9_-]{8,128}$/;
const SHA=/^[0-9a-f]{64}$/;
type ResultStatus="DENIED"|"HELD"|"POST_AUTHORIZED"|"UPDATED"|"OBSERVED";
interface Result {
  schema:typeof RESULT_SCHEMA;command_id:string;status:ResultStatus;
  account_budget?:{account_key:string;active_reservations:number;occupied_usd:string;cumulative_loss_usd:string;latest_activity_ms:number};
  budget?:{campaign_limit_usd:string;consumed_loss_usd:string;reserved_loss_usd:string;remaining_loss_usd:string;occupied_modelled_loss_usdt:string;source_as_of_ms:number|null};
  reason:string|null;reservation_key:string|null;
  risk_book_sequence:number|null;policy_revision:number|null;
  existing_modelled_loss_usdt:string|null;
  reservation_state:"HELD"|"POSTED"|"UNKNOWN"|"OPEN"|"RELEASED"|null;
}
interface Session {
  connection_generation:number;
  runtime_profile:string;
  hello:boolean;
}
function deny(id:string,reason:string):Result {
  return {schema:RESULT_SCHEMA,command_id:id,status:"DENIED",reason,
    reservation_key:null,risk_book_sequence:null,policy_revision:null,
    existing_modelled_loss_usdt:null,reservation_state:null};
}
function isObject(v:unknown):v is Record<string,unknown> {
  return v!==null&&typeof v==="object"&&!Array.isArray(v);
}
function nativeEvidence(raw:unknown,generation:number):NativeSessionRiskProof|null {
  if(!isObject(raw)||raw.complete!==true
    ||raw.native_account_evidence_complete!==true
    ||raw.native_order_evidence_complete!==true
    ||raw.native_ledger_evidence_complete!==true
    ||typeof raw.source_digest!=="string"||!SHA.test(raw.source_digest)
    ||typeof raw.content_digest!=="string"||!SHA.test(raw.content_digest)
    ||!Number.isSafeInteger(raw.source_read_floor_ms)
    ||(raw.source_read_floor_ms as number)<=0)
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
  if(session.runtime_profile!=="engine"||!session.hello||
    !Number.isSafeInteger(session.connection_generation)||session.connection_generation<1)
    return deny(id,"NATIVE_PRODUCTION_SESSION_REQUIRED");
  if(!isObject(raw)||raw.schema!==COMMAND_SCHEMA||!TOKEN.test(id)
    ||!isObject(raw.action)||!Number.isSafeInteger(now_ms)||now_ms<=0)
    return deny(id,"INVALID_TYPED_RISK_COMMAND");
  const installation=await storage.get(GOVERNANCE_STORAGE_KEY);
  if(!validGovernance(installation)||raw.installation_id!==installation.installation_id)return deny(id,"INSTALLATION_ID_MISMATCH");
  const a=raw.action;
  if(a.action==="ACCOUNT_SUMMARY"){
    if(await storage.get("generation")!==session.connection_generation)return deny(id,"STALE_NATIVE_INSPECTION_SESSION");
    if(typeof a.account_key!=="string"||!/^okx:real:[0-9a-f]{64}$/.test(a.account_key)||!installation.accounts.some((v:any)=>v.account_key===a.account_key))return deny(id,"ACCOUNT_ACCESS_MISSING");
    const accountKey=a.account_key;
    return storage.transaction(async(tx:any)=>{
      const meta=await readRiskBook(tx);if(!meta)return deny(id,"RISK_BOOK_MISSING_OR_CORRUPT");
      const stored=await tx.get("risk_account_v2:"+a.account_key);
      if(!stored&&meta.accounts[accountKey]!==undefined)return deny(id,"RISK_ACCOUNT_AGGREGATE_MISSING");
      const row=stored??{active_reservations:0,occupied_usd:"0",occupied_usdt:"0",latest_activity_ms:0};
      const loss=await tx.get("risk_loss_v2:"+a.account_key)??"0";
      if(row.occupied_usdt!==(meta.accounts[accountKey]??"0")||(row.active_reservations===0)!==(row.occupied_usd==="0"))return deny(id,"RISK_ACCOUNT_AGGREGATE_CORRUPT");
      if(!Number.isSafeInteger(row.active_reservations)||row.active_reservations<0||units(row.occupied_usd)===null||units(loss)===null||!Number.isSafeInteger(row.latest_activity_ms))return deny(id,"RISK_ACCOUNT_AGGREGATE_CORRUPT");
      return {...deny(id,""),reason:null,status:"OBSERVED",risk_book_sequence:meta.sequence,policy_revision:installation.revision,account_budget:{account_key:accountKey,active_reservations:row.active_reservations,occupied_usd:row.occupied_usd,latest_activity_ms:row.latest_activity_ms,cumulative_loss_usd:loss}} as Result;
    });
  }
  if(a.action==="SUMMARY"){
    if(await storage.get("generation")!==session.connection_generation)return deny(id,"STALE_NATIVE_INSPECTION_SESSION");
    const meta=await readRiskBook(storage);if(!meta)return deny(id,"RISK_BOOK_MISSING_OR_CORRUPT");
    const remaining=units(meta.campaign_limit_usd)!-units(meta.consumed_loss_usd)!-units(meta.occupied_usd)!;
    return {schema:RESULT_SCHEMA,command_id:id,status:"OBSERVED",reason:null,reservation_key:null,risk_book_sequence:meta.sequence,policy_revision:installation.revision,existing_modelled_loss_usdt:null,reservation_state:null,
      budget:{campaign_limit_usd:meta.campaign_limit_usd,consumed_loss_usd:meta.consumed_loss_usd,reserved_loss_usd:meta.occupied_usd,remaining_loss_usd:amount(remaining<0n?0n:remaining),occupied_modelled_loss_usdt:meta.occupied_usdt,source_as_of_ms:meta.source_as_of_ms||null}};
  }
  if(a.action==="INSPECT"){
    if(typeof a.reservation_key!=="string"||typeof a.plan_sha256!=="string"
      ||!SHA.test(a.plan_sha256)||a.reservation_key.length>512)
      return deny(id,"INVALID_INSPECTION_IDENTITY");
    const generation=await storage.get("generation");
    if(generation!==session.connection_generation)
      return deny(id,"STALE_NATIVE_INSPECTION_SESSION");
    const observed=await inspectRisk(storage,a.reservation_key);
    const book=observed?.meta,found=observed?.entry;
    if(!found||found.plan_hash!==a.plan_sha256||!book){
      const fenced=await inspectNoReserveFence(storage,a.reservation_key,a.plan_sha256);
      if(fenced)return {schema:RESULT_SCHEMA,command_id:id,status:"OBSERVED",reason:null,
        reservation_key:a.reservation_key,risk_book_sequence:fenced.sequence,
        policy_revision:fenced.policy_revision,existing_modelled_loss_usdt:null,
        reservation_state:"RELEASED"};
      const meta=await readRiskBook(storage);
      const governance:unknown=await storage.get(GOVERNANCE_STORAGE_KEY);
      if(!meta||!validGovernance(governance))return deny(id,"RISK_BOOK_OR_GOVERNANCE_UNAVAILABLE");
      return {schema:RESULT_SCHEMA,command_id:id,status:"DENIED",
        reason:"RISK_RESERVATION_NOT_FOUND",reservation_key:null,
        risk_book_sequence:meta.sequence,policy_revision:governance.revision,
        existing_modelled_loss_usdt:null,reservation_state:null};
    }
    return {schema:RESULT_SCHEMA,command_id:id,status:"OBSERVED",reason:null,
      reservation_key:found.key,risk_book_sequence:book.sequence,
      policy_revision:found.policy_revision,existing_modelled_loss_usdt:null,
      reservation_state:found.state};
  }
  if(a.action==="RESERVE"){
    const proof=nativeEvidence(a.census,session.connection_generation);
    if(!proof||!isObject(a.census)||!Array.isArray(a.census.account_keys)||
      !Array.isArray(a.census.occupied))
      return deny(id,"ALL_ACCOUNT_NATIVE_EVIDENCE_UNPROVEN");
    if(typeof a.venue!=="string"||typeof a.account_uid!=="string"||
      typeof a.run_id!=="string"||typeof a.intent_id!=="string"||typeof a.strategy_id!=="string"||
      typeof a.instrument_id!=="string"||
      typeof a.plan_sha256!=="string"||typeof a.requested_loss_usdt!=="string"||
      typeof a.entry_upper_usd!=="string"||typeof a.entry_proof_digest!=="string"||
      !Number.isSafeInteger(a.policy_revision))
      return deny(id,"INVALID_RESERVATION_INPUT");
    const governance:unknown=await storage.get(GOVERNANCE_STORAGE_KEY);
    if(!validGovernance(governance))
      return deny(id,"CENTRAL_GOVERNANCE_UNAVAILABLE");
    const policy=projectRiskPolicy(governance);
    if(!policy?.enabled)return deny(id,"CENTRAL_REAL_AUTHORIZATION_INACTIVE");
    const census:CompleteRiskCensus={
      complete:true,source_digest:proof.source_digest,
      content_digest:a.census.content_digest as string,
      source_read_floor_ms:a.census.source_read_floor_ms as number,
      generated_at_ms:a.census.generated_at_ms as number,
      account_keys:a.census.account_keys as string[],
      occupied:a.census.occupied as CompleteRiskCensus["occupied"],
    };
    const input:RiskReservation={
      venue:a.venue,environment:"real",uid:a.account_uid,run_id:a.run_id,
      intent_id:a.intent_id,strategy_id:a.strategy_id,instrument_id:a.instrument_id,
      plan_hash:a.plan_sha256,loss_usdt:a.requested_loss_usdt,loss_usd:a.requested_loss_usd as string,
      entry_upper_usd:a.entry_upper_usd,entry_proof_digest:a.entry_proof_digest,
      usd_conversion:a.usd_conversion as RiskReservation["usd_conversion"],
      policy_revision:a.policy_revision as number,now_ms,
    };
    const decision=await reserveRisk(storage,policy,census,input,proof);
    return decision.status==="HELD"
      ?{schema:RESULT_SCHEMA,command_id:id,status:"HELD",reason:null,
        reservation_key:decision.entry.key,risk_book_sequence:decision.book_sequence,
        policy_revision:decision.entry.policy_revision,existing_modelled_loss_usdt:null,
        reservation_state:null}
      :deny(id,decision.reason);
  }
  if(a.action==="BEGIN_POST"){
    if(typeof a.reservation_key!=="string"||typeof a.plan_sha256!=="string"
      ||!Number.isSafeInteger(a.policy_revision)||!Number.isSafeInteger(a.expected_risk_book_sequence)
      ||typeof a.source_proof_sha256!=="string"
      ||!isObject(a.causal_proof))
      return deny(id,"INVALID_POST_FENCE");
    const proof=nativeEvidence(a.census,session.connection_generation);
    if(!proof||!isObject(a.census)||!Array.isArray(a.census.account_keys)||
      !Array.isArray(a.census.occupied))
      return deny(id,"POST_CENSUS_UNVERIFIED");
    const census:CompleteRiskCensus={
      complete:true,source_digest:proof.source_digest,
      content_digest:a.census.content_digest as string,
      source_read_floor_ms:a.census.source_read_floor_ms as number,
      generated_at_ms:a.census.generated_at_ms as number,
      account_keys:a.census.account_keys as string[],
      occupied:a.census.occupied as CompleteRiskCensus["occupied"],
    };
    const result=await beginRiskPost(storage,a.reservation_key,a.plan_sha256,
      a.policy_revision as number,a.source_proof_sha256,
      {connection_generation:session.connection_generation,
       expected_book_sequence:a.expected_risk_book_sequence as number,
       census,source_proof:proof,now_ms,causal_proof:a.causal_proof as unknown as CausalPostProof});
    return result.status==="POST_AUTHORIZED"
      ?{schema:RESULT_SCHEMA,command_id:id,status:"POST_AUTHORIZED",reason:null,
        reservation_key:a.reservation_key,risk_book_sequence:result.book_sequence!,
        policy_revision:a.policy_revision as number,
        existing_modelled_loss_usdt:result.existing_modelled_loss_usdt!,reservation_state:null}
      :deny(id,result.reason??"POST_FENCE_DENIED");
  }
  if(a.action==="RELEASE_UNSENT"){
    if(typeof a.reservation_key!=="string"||typeof a.plan_sha256!=="string"||
       !Number.isSafeInteger(a.expected_risk_book_sequence)||!isObject(a.proof))
      return deny(id,"INVALID_UNSENT_RELEASE_COMMAND");
    const proof=a.proof;
    if(typeof proof.reservation_key!=="string"||typeof proof.account_key!=="string"||
       typeof proof.intent_id!=="string"||typeof proof.plan_hash!=="string"||
       typeof proof.request_hash!=="string"||typeof proof.reason!=="string"||
       typeof proof.record_digest!=="string")
      return deny(id,"INVALID_UNSENT_SQL_PROOF");
    const result=await releaseUnsentRisk(storage,a.reservation_key,a.plan_sha256,
      a.expected_risk_book_sequence as number,session.connection_generation,
      proof as unknown as UnsentRiskReleaseProof);
    if(result.status!=="UPDATED")return deny(id,result.reason??"UNSENT_RELEASE_DENIED");
    const observed=await inspectRisk(storage,a.reservation_key);
    const fenced=observed?null:await inspectNoReserveFence(storage,a.reservation_key,a.plan_sha256);
    return observed||fenced
      ?{schema:RESULT_SCHEMA,command_id:id,status:"UPDATED",reason:null,
        reservation_key:a.reservation_key,
        risk_book_sequence:observed?.meta.sequence??fenced!.sequence,
        policy_revision:observed?.entry.policy_revision??fenced!.policy_revision,
        existing_modelled_loss_usdt:null,reservation_state:null}
      :deny(id,"RISK_BOOK_READBACK_FAILED");
  }
  if(a.action==="RESOLVE"){
    if(typeof a.reservation_key!=="string"||typeof a.plan_sha256!=="string"
      ||!Number.isSafeInteger(a.expected_risk_book_sequence)||
      typeof a.source_proof_sha256!=="string")
      return deny(id,"INVALID_RESOLUTION_INPUT");
    if(a.next_state!=="UNKNOWN"&&a.next_state!=="OPEN"&&a.next_state!=="RELEASED")
      return deny(id,"INVALID_RESOLUTION_STATE");
    let terminal:TerminalRiskReleaseProof|undefined;
    if(a.next_state==="RELEASED"){
      const provenance=nativeEvidence(a.terminal_census,session.connection_generation);
      if(!provenance||!isObject(a.terminal_census)||!isObject(a.terminal_ledger)
        ||!Array.isArray(a.terminal_census.account_keys)
        ||!Array.isArray(a.terminal_census.occupied)
        ||typeof a.terminal_ledger.account_uid!=="string"
        ||typeof a.terminal_ledger.run_id!=="string"
        ||typeof a.terminal_ledger.intent_id!=="string"
        ||typeof a.terminal_ledger.record_digest!=="string"
        ||!["FILLED","CANCELED","REJECTED"].includes(a.terminal_ledger.state as string))
        return deny(id,"TERMINAL_NATIVE_PROOF_UNAVAILABLE");
      terminal={census:{complete:true,
        source_digest:a.terminal_census.source_digest as string,
        content_digest:a.terminal_census.content_digest as string,
        generated_at_ms:a.terminal_census.generated_at_ms as number,
        source_read_floor_ms:a.terminal_census.source_read_floor_ms as number,
        account_keys:a.terminal_census.account_keys as string[],
        occupied:a.terminal_census.occupied as CompleteRiskCensus["occupied"]},
        source_proof:provenance,
        ledger:a.terminal_ledger as unknown as TerminalRiskReleaseProof["ledger"],
        now_ms};
    } else if(a.terminal_census!==undefined||a.terminal_ledger!==undefined){
      return deny(id,"UNEXPECTED_TERMINAL_RELEASE_PROOF");
    }
    const status=await transitionRisk(storage,a.reservation_key,a.plan_sha256,
      a.next_state,a.source_proof_sha256,
      {connection_generation:session.connection_generation,
       expected_book_sequence:a.expected_risk_book_sequence as number,terminal});
    if(status.status!=="UPDATED")return deny(id,status.reason??"RISK_RESOLUTION_DENIED");
    const observed=await inspectRisk(storage,a.reservation_key);
    const book=observed?.meta,entry=observed?.entry;
    return entry&&book
      ?{schema:RESULT_SCHEMA,command_id:id,status:"UPDATED",reason:null,
        reservation_key:entry.key,risk_book_sequence:book.sequence,
        policy_revision:entry.policy_revision,existing_modelled_loss_usdt:null,
        reservation_state:null}
      :deny(id,"RISK_BOOK_READBACK_FAILED");
  }
  return deny(id,"UNSUPPORTED_RISK_COMMAND");
}
