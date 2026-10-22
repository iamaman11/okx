import {GOVERNANCE_STORAGE_KEY,validGovernance,validLimits,accessEnabled,
  reviseGovernance,type GovernanceUpdate,type Governance,type Account,type Limits} from "./account_governance.js";
export {GOVERNANCE_STORAGE_KEY} from "./account_governance.js";

type Reply={status:number;body:Record<string,unknown>};
const fail=(status:number,reason:string):Reply=>({status,body:{status:"FAILED",reason}});
const record=(v:unknown):v is Record<string,unknown>=>v!==null&&typeof v==="object"&&!Array.isArray(v);
const operationKey=(key:unknown):key is string=>typeof key==="string"&&/^[A-Za-z0-9_-]{8,128}$/.test(key);
function canonical(v:unknown):string{
  if(Array.isArray(v))return "["+v.map(canonical).join(",")+"]";
  if(record(v))return "{"+Object.keys(v).sort().map(k=>JSON.stringify(k)+":"+canonical(v[k])).join(",")+"}";
  return JSON.stringify(v);
}
async function digest(value:unknown):Promise<string>{
  return Array.from(new Uint8Array(await crypto.subtle.digest("SHA-256",new TextEncoder().encode(canonical(value)))))
    .map(b=>b.toString(16).padStart(2,"0")).join("");
}
function result(policy:Governance){
  return {...policy,status:"CONFIGURED",access:policy.accounts.map(a=>({account:a.id,enabled:a.enabled,
    explicit_pause:a.explicit_pause,policy_revision:a.policy_revision})),
    readiness:{state:"not_evaluated",reason:null},last_admission:null,
    complete_all_account_census:"not_evaluated",available_for_new_risk_usdt:null};
}
/** Only the authenticated installation lifecycle may initialize this namespace.
 * A missing registered installation never becomes a new zero-budget state on GET.
 */
export async function initializeGovernance(storage:any,configuration:Governance):Promise<Reply>{
  if(!validGovernance(configuration)||configuration.revision!==0||configuration.accounts.length!==0)
    return fail(400,"INVALID_INSTALLATION_CONFIGURATION");
  return storage.transaction(async(tx:any)=>{
    const previous:unknown=await tx.get(GOVERNANCE_STORAGE_KEY);
    if(previous!==undefined)return fail(409,"INSTALLATION_ALREADY_INITIALIZED");
    await tx.put(GOVERNANCE_STORAGE_KEY,configuration);
    return {status:200,body:result(configuration)};
  });
}
/** Every accepted mutation has one durable body-bound outcome. */
async function mutate(storage:any,key:unknown,input:unknown,fn:(policy:Governance)=>Reply):Promise<Reply>{
  if(!operationKey(key))return fail(400,"IDEMPOTENCY_KEY_REQUIRED");
  const hash=await digest(input),receiptKey="access_operation:"+key;
  return storage.transaction(async(tx:any)=>{
    const receipt=await tx.get(receiptKey);
    if(receipt!==undefined)return receipt.hash===hash?receipt.reply:fail(409,"IDEMPOTENCY_CONFLICT");
    const previous:unknown=await tx.get(GOVERNANCE_STORAGE_KEY);
    if(!validGovernance(previous))return fail(503,"INSTALLATION_ACCESS_MISSING_OR_CORRUPT");
    const reply=fn(previous);
    if(reply.status===200){
      const next=reply.body.configuration as Governance;
      if(!validGovernance(next))return fail(503,"ACCESS_INVARIANTS_REJECTED");
      await tx.put(GOVERNANCE_STORAGE_KEY,next);
      delete reply.body.configuration;
    }
    await tx.put(receiptKey,{hash,reply});
    return reply;
  });
}
export async function governanceRequest(storage:any,method:string,raw?:unknown):Promise<Reply>{
  if(method==="GET"){
    const policy:unknown=await storage.get(GOVERNANCE_STORAGE_KEY);
    return validGovernance(policy)?{status:200,body:result(policy)}:fail(503,"INSTALLATION_ACCESS_MISSING_OR_CORRUPT");
  }
  if(method!=="POST")return fail(405,"METHOD_NOT_ALLOWED");
  if(!record(raw)||Object.keys(raw).sort().join(",")!=="accounts,expected_revision,idempotency_key,max_total_modelled_loss_usdt")
    return fail(400,"INVALID_ACCESS_REQUEST");
  if(new TextEncoder().encode(JSON.stringify(raw)).byteLength>32768)return fail(413,"ACCESS_REQUEST_TOO_LARGE");
  return mutate(storage,raw.idempotency_key,raw,policy=>{
    const revised=reviseGovernance(policy,raw as unknown as GovernanceUpdate);
    return revised?{status:200,body:{...result(revised),configuration:revised}}:fail(409,"ACCESS_REVISION_OR_LIMITS_REJECTED");
  });
}
/** Verified identity is supplied by authenticated native receipt, never public UID input.
 * The same connect persists the requested limits and access; other accounts are untouched.
 */
export async function enrollObservedAccount(storage:any,observed:{uid_fingerprint:string;venue:"okx";
  environment:"real"|"demo";label:string;alias:string;limits:Limits;enabled:boolean;
  idempotency_key:string;credential_ref:string}):Promise<Reply>{
  if(!/^[0-9a-f]{64}$/.test(observed.uid_fingerprint)||observed.venue!=="okx"
    ||!["real","demo"].includes(observed.environment)||typeof observed.label!=="string"
    ||observed.label.length<1||observed.label.length>80||!/^[A-Za-z][A-Za-z0-9_-]{1,63}$/.test(observed.alias)
    ||typeof observed.enabled!=="boolean"||!validLimits(observed.limits))return fail(400,"INVALID_ACCOUNT_CONNECTION");
  return mutate(storage,observed.idempotency_key,{action:"connect",...observed},previous=>{
    const key=observed.venue+":"+observed.environment+":"+observed.uid_fingerprint;
    if(previous.accounts.some(a=>a.id===observed.alias||a.account_key===key))return fail(409,"ACCOUNT_ALREADY_CONNECTED");
    if(previous.accounts.length>=32||previous.revision===Number.MAX_SAFE_INTEGER)return fail(409,"ACCOUNT_REGISTRY_CAPACITY");
    const revision=previous.revision+1;
    const account:Account={id:observed.alias,label:observed.label,venue:observed.venue,environment:observed.environment,
      uid_fingerprint:observed.uid_fingerprint,identity_verified:true,account_key:key,enabled:observed.enabled,
      explicit_pause:false,policy_revision:revision,limits:observed.limits,strategies:[]};
    const next:Governance={...previous,revision,accounts:[...previous.accounts,account]};
    if(!validGovernance(next))return fail(400,"INVALID_ACCOUNT_LIMITS");
    return {status:200,body:{status:"CONNECTED",account,readiness:{state:"not_evaluated"},configuration:next}};
  });
}
export async function setAccountPause(storage:any,raw:unknown):Promise<Reply>{
  if(!record(raw)||Object.keys(raw).sort().join(",")!=="account,expected_revision,explicit_pause,idempotency_key"
    ||typeof raw.account!=="string"||typeof raw.explicit_pause!=="boolean"||!Number.isSafeInteger(raw.expected_revision))
    return fail(400,"INVALID_PAUSE_REQUEST");
  return mutate(storage,raw.idempotency_key,{action:"pause",...raw},previous=>{
    if(previous.revision!==raw.expected_revision||previous.revision===Number.MAX_SAFE_INTEGER)return fail(409,"STALE_ACCESS_REVISION");
    if(!previous.accounts.some(a=>a.id===raw.account||a.account_key===raw.account))return fail(404,"ACCOUNT_NOT_FOUND");
    const revision=previous.revision+1;
    const accounts=previous.accounts.map(a=>a.id===raw.account||a.account_key===raw.account
      ?{...a,explicit_pause:raw.explicit_pause as boolean,policy_revision:revision}:a);
    const next={...previous,revision,accounts};
    return {status:200,body:{...result(next),configuration:next}};
  });
}
/** Emergency STOP preserves enabled access and keeps every account in the census. */
export async function stopNewRealRisk(storage:any,idempotency_key:string):Promise<Reply>{
  return mutate(storage,idempotency_key,{action:"stop_real",idempotency_key},policy=>{
    if(!policy.accounts.some(a=>a.environment==="real"&&accessEnabled(a)))
      return {status:200,body:{...result(policy),configuration:policy}};
    if(policy.revision===Number.MAX_SAFE_INTEGER)return fail(409,"ACCESS_REVISION_EXHAUSTED");
    const revision=policy.revision+1;
    const next={...policy,revision,accounts:policy.accounts.map(a=>a.environment==="real"
      ?{...a,explicit_pause:true,policy_revision:revision}:a)};
    return {status:200,body:{...result(next),configuration:next}};
  });
}
