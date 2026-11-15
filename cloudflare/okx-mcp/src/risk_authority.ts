const RESULT_SCHEMA="okx.risk-result/v1";
const TOKEN=/^[A-Za-z0-9_-]{8,128}$/;
type ResultStatus="DENIED"|"HELD"|"POST_AUTHORIZED"|"UPDATED"|"OBSERVED";
interface Result {
  schema:typeof RESULT_SCHEMA;command_id:string;status:ResultStatus;
  account_budget?:{account_key:string;active_reservations:number;occupied_usd:string;cumulative_loss_usd:string;latest_activity_ms:number};
  budget?:{campaign_limit_usd:string;consumed_loss_usd:string;reserved_loss_usd:string;remaining_loss_usd:string;occupied_modelled_loss_usdt:string;source_as_of_ms:number|null;
    /** Immutable installation-owned inception; null on legacy books. */
    campaign_origin?:{campaign_id:string;installation_id:string;started_at_ms:number}|null;
    /** Null means opening finance history NOT PROVEN, not a zero spend. */
    entry_budget?:{campaign_id:string;limit_usd:string;consumed_usd:string;reserved_usd:string;remaining_usd:string;bound_breach_usd:string;opening_source_digest:string;opening_as_of_ms:number}|null};
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
/** Obsolete financial RPCs are denied after the transport authenticates the native
 * session. Rust owns all budgets, reservations and financial transitions.
 * No persisted legacy financial state is consulted or changed here.
 */
export async function handleNativeRiskCommand(
 _storage:unknown,session:Session,raw:unknown,_now_ms:number
):Promise<Result>{
 const id=isObject(raw)&&typeof raw.command_id==="string"&&TOKEN.test(raw.command_id)
   ?raw.command_id:"invalid_risk_command";
 if(session.runtime_profile!=="engine"||!session.hello||
    !Number.isSafeInteger(session.connection_generation)||session.connection_generation<1)
   return deny(id,"NATIVE_PRODUCTION_SESSION_REQUIRED");
 return deny(id,"NATIVE_FINANCE_AUTHORITY_REQUIRED");
}
