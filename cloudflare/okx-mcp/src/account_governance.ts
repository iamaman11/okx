import {units, type RiskPolicy} from "./risk_book.js";


/** The sole access/limits owner for this clean installation. */
export const GOVERNANCE_SCHEMA = "okx.governance/v3" as const;
export const GOVERNANCE_STORAGE_KEY = "account_access_v1" as const;
export type TradingAccess = {
  account_key:string;enabled:boolean;policy_revision:number;limits:Limits;explicit_pause:boolean;
};
export type Limits = {
  max_modelled_loss_usdt:string;
  max_order_loss_usdt:string;
  max_notional_usdt:string|null;
  /** Optional on old saved policy; Real entry admission refuses absent cap. */
  max_entry_usd?:string|null;
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
export type Account = TradingAccess & {
  id:string;label:string;venue:string;environment:"real"|"demo";
  uid_fingerprint:string;identity_verified:boolean;
  strategies:StrategyLimits[];
};
export type Governance = {
  schema:typeof GOVERNANCE_SCHEMA;
  revision:number;installation_id:string;campaign_id:string;campaign_loss_limit_usd:string;
  /** E1 spend budget, never the campaign modeled/realized loss limit.
   * Absent on pre-E1 policies: opening consumption must be imported first. */
  campaign_entry_limit_usd?:string;
  max_total_modelled_loss_usdt:string;
  accounts:Account[];
};
export type AccountUpdate = {id:string;label:string;enabled:boolean;explicit_pause:boolean;limits:Limits;strategies:StrategyLimits[]};
export type GovernanceUpdate = {
  expected_revision:number;
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
export function validLimits(value:unknown):value is Limits{
  if(!record(value))return false;
  const x=value as Record<string,unknown>;
  const fields=["max_modelled_loss_usdt","max_order_loss_usdt","max_notional_usdt",
    "max_leverage","max_daily_realized_loss_usdt","max_drawdown_ratio","allowed_instruments"];
  if("max_entry_usd" in x)fields.push("max_entry_usd");
  if(Object.keys(x).sort().join("|")!==fields.sort().join("|"))return false;
  return positive(x.max_modelled_loss_usdt)&&positive(x.max_order_loss_usdt)
    &&units(x.max_order_loss_usdt)!<=units(x.max_modelled_loss_usdt)!
    &&optionalPositive(x.max_notional_usdt)&&(!("max_entry_usd" in x)||optionalPositive(x.max_entry_usd))
    &&optionalPositive(x.max_leverage)
    &&optionalPositive(x.max_daily_realized_loss_usdt)
    &&(x.max_drawdown_ratio===null||ratio(x.max_drawdown_ratio))
    &&Array.isArray(x.allowed_instruments)&&x.allowed_instruments.length<=32
    &&x.allowed_instruments.every(t=>typeof t==="string"&&INSTR.test(t))
    &&new Set(x.allowed_instruments).size===x.allowed_instruments.length;
}
function validAccount(a:unknown):a is Account{
  if(!record(a))return false;
  const fields=["id","label","venue","environment","uid_fingerprint","identity_verified","enabled","limits","strategies","account_key","policy_revision","explicit_pause"];
  if(Object.keys(a).sort().join("|")!==fields.sort().join("|"))return false;
  return typeof a.id==="string"&&ID.test(a.id)
    &&typeof a.label==="string"&&a.label.length<=80
    &&typeof a.venue==="string"&&/^[a-z][a-z0-9_-]{1,31}$/.test(a.venue)
    &&(a.environment==="real"||a.environment==="demo")
    &&typeof a.uid_fingerprint==="string"&&SHA.test(a.uid_fingerprint)
    &&typeof a.identity_verified==="boolean"&&typeof a.enabled==="boolean"
    &&typeof a.explicit_pause==="boolean"&&Number.isSafeInteger(a.policy_revision)
    &&(a.policy_revision as number)>0&&a.account_key===a.venue+":"+a.environment+":"+a.uid_fingerprint
    &&(!a.enabled||a.identity_verified)&&validLimits(a.limits)
    &&validStrategies(a.strategies,a.limits.max_modelled_loss_usdt)
    &&(!a.enabled||a.environment!=="real"
      ||(a.limits.max_notional_usdt!==null&&a.limits.max_leverage!==null
        &&a.limits.max_daily_realized_loss_usdt!==null
        &&a.limits.max_drawdown_ratio!==null&&a.limits.allowed_instruments.length>0));
}
export function validGovernance(value:unknown):value is Governance{
  if(!record(value))return false;
  const fields=["schema","revision","installation_id","campaign_id","campaign_loss_limit_usd","max_total_modelled_loss_usdt","accounts"];
  if("campaign_entry_limit_usd" in value)fields.push("campaign_entry_limit_usd");
  if(Object.keys(value).sort().join("|")!==fields.sort().join("|"))return false;
  if(value.schema!==GOVERNANCE_SCHEMA||!Number.isSafeInteger(value.revision)||(value.revision as number)<0
    ||typeof value.installation_id!=="string"||value.installation_id.length<8||!ID.test(value.installation_id)
    ||typeof value.campaign_id!=="string"||!/^sha256:[0-9a-f]{64}$/.test(value.campaign_id)
    ||!positive(value.campaign_loss_limit_usd)||
    !(!("campaign_entry_limit_usd" in value)||positive(value.campaign_entry_limit_usd))||
    !positive(value.max_total_modelled_loss_usdt)
    ||!Array.isArray(value.accounts)||value.accounts.length>32||!value.accounts.every(validAccount))return false;
  const accounts=value.accounts as Account[];
  return new Set(accounts.map(a=>a.account_key)).size===accounts.length
    &&new Set(accounts.map(a=>a.id)).size===accounts.length
    &&accounts.every(a=>a.policy_revision<=(value.revision as number));
}
export function accessEnabled(a:Account):boolean{return a.identity_verified&&a.enabled&&!a.explicit_pause;}
/** One CAS revision changes business limits, but never mutates identity or campaign. */
export function reviseGovernance(previous:Governance,change:GovernanceUpdate):Governance|null{
  if(!validGovernance(previous)||!Number.isSafeInteger(change.expected_revision)
    ||change.expected_revision!==previous.revision
    ||!Number.isSafeInteger(previous.revision+1)
    ||!positive(change.max_total_modelled_loss_usdt)
    ||!Array.isArray(change.accounts)||change.accounts.length!==previous.accounts.length)return null;
  const remaining=new Map(previous.accounts.map(a=>[a.id,a]));
  const accounts:Account[]=[];
  for(const row of change.accounts){
    if(!record(row))return null;
    const before=remaining.get(row.id);
    if(!before||!validLimits(row.limits)||typeof row.label!=="string"||row.label.length>80
      ||typeof row.enabled!=="boolean"||typeof row.explicit_pause!=="boolean"||(row.enabled&&!before.identity_verified)
      ||!validStrategies(row.strategies,row.limits.max_modelled_loss_usdt))return null;
    accounts.push({...before,enabled:row.enabled,explicit_pause:row.explicit_pause,policy_revision:previous.revision+1,label:row.label,limits:row.limits,strategies:row.strategies});
    remaining.delete(row.id);
  }
  if(remaining.size!==0)return null;
  const next:Governance={...previous,revision:previous.revision+1,
    max_total_modelled_loss_usdt:change.max_total_modelled_loss_usdt,accounts};
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
    enabled:state.accounts.some(a=>a.environment==="real"&&accessEnabled(a)),
    authorized_ceiling_usdt:state.max_total_modelled_loss_usdt,
    global_limit_usdt:state.max_total_modelled_loss_usdt,
    account_limits_usdt:accounts};
}
