import {units, type RiskPolicy} from "./risk_book.js";
import {validAccountPolicy, type AccountPolicy} from "./account_policy.js";

/** One configuration owner. Neither labels nor local profiles establish identity.
 * Native authenticated evidence must separately set identity_verified.
 * Ordinary CAS updates cannot change or extend operator_authorization.
 */
export const GOVERNANCE_SCHEMA = "okx.governance/v2" as const;
export type Limits = {
  max_modelled_loss_usdt:string;
  max_order_loss_usdt:string;
  max_notional_usdt:string|null;
  max_leverage:string|null;
  max_daily_realized_loss_usdt:string|null;
  max_drawdown_ratio:string|null;
  allowed_instruments:string[];
};
export type StrategyLimits = {
  id:string;enabled:boolean;allocated_capital_usdt:string;
  max_modelled_loss_usdt:string;max_daily_realized_loss_usdt:string|null;
  max_drawdown_ratio:string|null;
};
function validStrategies(v:unknown,accountCap:string):v is StrategyLimits[]{
  if(!Array.isArray(v)||v.length>32)return false;
  const seen=new Set<string>();
  let total=0n;
  for(const s of v){
    if(!record(s)||typeof s.id!=="string"||!ID.test(s.id)||seen.has(s.id)
      ||typeof s.enabled!=="boolean"||!positive(s.allocated_capital_usdt)
      ||!positive(s.max_modelled_loss_usdt)
      ||units(s.max_modelled_loss_usdt)!>units(s.allocated_capital_usdt)!
      ||!optionalPositive(s.max_daily_realized_loss_usdt)
      ||!(s.max_drawdown_ratio===null||ratio(s.max_drawdown_ratio))
      ||(s.enabled&&(s.max_daily_realized_loss_usdt===null||s.max_drawdown_ratio===null)))return false;
    seen.add(s.id);
    if(s.enabled)total+=units(s.max_modelled_loss_usdt)!;
  }
  return total<=units(accountCap)!;
}
export type Account = {
  id:string;label:string;venue:string;environment:"real"|"demo";
  uid_fingerprint:string;identity_verified:boolean;enabled:boolean;
  limits:Limits;strategies:StrategyLimits[];
};
export type Governance = {
  schema:typeof GOVERNANCE_SCHEMA;
  revision:number;trading_requested:boolean;
  operator_authorization:{id:string;status:"UNVERIFIED"|"ACTIVE"|"REVOKED";ceiling_usdt:string};
  max_total_modelled_loss_usdt:string;
  accounts:Account[];
};
export type AccountUpdate = {id:string;label:string;enabled:boolean;limits:Limits;strategies:StrategyLimits[]};
export type GovernanceUpdate = {
  expected_revision:number;trading_requested:boolean;
  max_total_modelled_loss_usdt:string;accounts:AccountUpdate[];
};
const SHA=/^[0-9a-f]{64}$/;
const ID=/^[a-zA-Z][a-zA-Z0-9_-]{1,63}$/;
const INSTR=/^[A-Z0-9_-]{3,64}$/;
function record(value:unknown):value is Record<string,unknown>{
  return value!==null&&typeof value==="object"&&!Array.isArray(value);
}
function positive(value:unknown):boolean {
  const n=units(value);return n!==null&&n>0n;
}
function ratio(value:unknown):boolean{
  const n=units(value);return n!==null&&n>=0n&&n<=1000000000n;
}
function optionalPositive(value:unknown):boolean{return value===null||positive(value);}
function validLimits(value:unknown):value is Limits{
  if(!record(value))return false;
  const x=value as Record<string,unknown>;
  const fields=["max_modelled_loss_usdt","max_order_loss_usdt","max_notional_usdt",
    "max_leverage","max_daily_realized_loss_usdt","max_drawdown_ratio","allowed_instruments"];
  if(Object.keys(x).sort().join("|")!==fields.sort().join("|"))return false;
  return positive(x.max_modelled_loss_usdt)&&positive(x.max_order_loss_usdt)
    &&units(x.max_order_loss_usdt)!<=units(x.max_modelled_loss_usdt)!
    &&optionalPositive(x.max_notional_usdt)&&optionalPositive(x.max_leverage)
    &&optionalPositive(x.max_daily_realized_loss_usdt)
    &&(x.max_drawdown_ratio===null||ratio(x.max_drawdown_ratio))
    &&Array.isArray(x.allowed_instruments)&&x.allowed_instruments.length<=32
    &&x.allowed_instruments.every(t=>typeof t==="string"&&INSTR.test(t))
    &&new Set(x.allowed_instruments).size===x.allowed_instruments.length;
}
function validAccount(a:unknown):a is Account{
  if(!record(a))return false;
  const fields=["id","label","venue","environment","uid_fingerprint","identity_verified","enabled","limits","strategies"];
  if(Object.keys(a).sort().join("|")!==fields.sort().join("|"))return false;
  return typeof a.id==="string"&&ID.test(a.id)
    &&typeof a.label==="string"&&a.label.length<=80
    &&typeof a.venue==="string"&&/^[a-z][a-z0-9_-]{1,31}$/.test(a.venue)
    &&(a.environment==="real"||a.environment==="demo")
    &&typeof a.uid_fingerprint==="string"&&SHA.test(a.uid_fingerprint)
    &&typeof a.identity_verified==="boolean"&&typeof a.enabled==="boolean"
    &&(!a.enabled||a.identity_verified)&&validLimits(a.limits)
    &&validStrategies(a.strategies,a.limits.max_modelled_loss_usdt)
    &&(!a.enabled||a.environment!=="real"
      ||(a.limits.max_notional_usdt!==null&&a.limits.max_leverage!==null
        &&a.limits.max_daily_realized_loss_usdt!==null
        &&a.limits.max_drawdown_ratio!==null&&a.limits.allowed_instruments.length>0));
}
export function validGovernance(value:unknown):value is Governance{
  if(!record(value))return false;
  const fields=["schema","revision","trading_requested","operator_authorization","max_total_modelled_loss_usdt","accounts"];
  if(Object.keys(value).sort().join("|")!==fields.sort().join("|"))return false;
  const op=value.operator_authorization;
  if(value.schema!==GOVERNANCE_SCHEMA||!Number.isSafeInteger(value.revision)
    ||(value.revision as number)<0||typeof value.trading_requested!=="boolean"
    ||(value.trading_requested&&value.revision===0)
    ||!record(op)||Object.keys(op).sort().join("|")!=="ceiling_usdt|id|status"
    ||typeof op.id!=="string"||!/^sha256:[0-9a-f]{64}$/.test(op.id)
    ||!["UNVERIFIED","ACTIVE","REVOKED"].includes(String(op.status))
    ||!positive(op.ceiling_usdt)||!positive(value.max_total_modelled_loss_usdt)
    ||units(value.max_total_modelled_loss_usdt)!>units(op.ceiling_usdt)!
    ||!Array.isArray(value.accounts)||value.accounts.length>32
    ||!value.accounts.every(validAccount))return false;
  const accounts=value.accounts as Account[];
  const identities=accounts.map(a=>a.venue+":"+a.environment+":"+a.uid_fingerprint);
  return new Set(identities).size===identities.length
    &&new Set(accounts.map(a=>a.id)).size===accounts.length
    &&(!value.trading_requested||(op.status==="ACTIVE"&&accounts.some(a=>a.enabled&&a.environment==="real")));
}
/** Historical v1 allowlist is unverified enrollment metadata, not a new grant. */
export function migrateV1(old:AccountPolicy):Governance|null {
  if(!validAccountPolicy(old))return null;
  const result:Governance={
    schema:GOVERNANCE_SCHEMA,revision:old.revision,trading_requested:false,
    operator_authorization:{id:"sha256:"+"0".repeat(64),status:"UNVERIFIED",ceiling_usdt:"10"},
    max_total_modelled_loss_usdt:old.max_total_modelled_loss_usdt,
    accounts:old.allowed_account_uids.map((uid,i)=>({
      id:"legacy_"+(i+1),label:"Unverified "+uid.slice(0,8),venue:"okx",
      environment:"real",uid_fingerprint:uid,identity_verified:false,enabled:false,
      limits:{max_modelled_loss_usdt:old.max_total_modelled_loss_usdt,
        max_order_loss_usdt:old.max_total_modelled_loss_usdt,max_notional_usdt:null,
        max_leverage:null,max_daily_realized_loss_usdt:null,max_drawdown_ratio:null,
        allowed_instruments:[]},strategies:[]
    }))
  };
  return validGovernance(result)?result:null;
}
/** One CAS revision changes ALL business limits, but never mutates grant or UID. */
export function reviseGovernance(previous:Governance,change:GovernanceUpdate):Governance|null{
  if(!validGovernance(previous)||!Number.isSafeInteger(change.expected_revision)
    ||change.expected_revision!==previous.revision
    ||!Number.isSafeInteger(previous.revision+1)
    ||typeof change.trading_requested!=="boolean"
    ||!positive(change.max_total_modelled_loss_usdt)
    ||units(change.max_total_modelled_loss_usdt)!>units(previous.operator_authorization.ceiling_usdt)!
    ||!Array.isArray(change.accounts)||change.accounts.length!==previous.accounts.length)return null;
  const remaining=new Map(previous.accounts.map(a=>[a.id,a]));
  const accounts:Account[]=[];
  for(const row of change.accounts){
    const before=remaining.get(row.id);
    if(!before||!validLimits(row.limits)||typeof row.label!=="string"||row.label.length>80
      ||typeof row.enabled!=="boolean"||(row.enabled&&!before.identity_verified)
      ||!validStrategies(row.strategies,row.limits.max_modelled_loss_usdt))return null;
    accounts.push({...before,enabled:row.enabled,label:row.label,limits:row.limits,strategies:row.strategies});
    remaining.delete(row.id);
  }
  if(remaining.size!==0)return null;
  const next:Governance={...previous,revision:previous.revision+1,
    trading_requested:change.trading_requested,max_total_modelled_loss_usdt:change.max_total_modelled_loss_usdt,accounts};
  return validGovernance(next)?next:null;
}
/** RiskBook derives all its account-specific caps from the same policy. */
export function projectRiskPolicy(state:Governance):RiskPolicy|null{
  if(!validGovernance(state))return null;
  const accounts:Record<string,string>={};
  for(const a of state.accounts)if(a.environment==="real"){
    // Disabled registered accounts still occupy risk. An unverified UID
    // cannot be silently excluded as if it had zero exchange exposure.
    if(!a.identity_verified)return null;
    accounts[a.venue+":"+a.environment+":"+a.uid_fingerprint]=a.limits.max_modelled_loss_usdt;
  }
  return {revision:state.revision,
    enabled:state.trading_requested&&state.operator_authorization.status==="ACTIVE"
      &&state.accounts.some(a=>a.environment==="real"&&a.enabled)
      &&Object.keys(accounts).length>0,
    authorized_ceiling_usdt:state.operator_authorization.ceiling_usdt,
    global_limit_usdt:state.max_total_modelled_loss_usdt,
    account_limits_usdt:accounts};
}
