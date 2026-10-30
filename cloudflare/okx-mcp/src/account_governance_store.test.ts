import {governanceRequest,lowerCampaignLimit,enrollObservedAccount,setAccountPause,stopNewRealRisk,initializeGovernance,GOVERNANCE_STORAGE_KEY} from "./account_governance_store.js";
import {FRAME_SCHEMA} from "./shared.js";
import {initial,limits} from "./account_governance.test.js";
function ok(v:boolean,label:string){if(!v)throw Error(label);}
class Storage{
 map=new Map<string,any>(); reads:string[]=[];deletes:string[][]=[];
 async get(key:string){this.reads.push(key);return structuredClone(this.map.get(key));}
 async put(key:string,value:unknown){this.map.set(key,structuredClone(value));}
 async delete(keys:string[]){this.deletes.push([...keys]);let n=0;for(const key of keys)if(this.map.delete(key))n++;return n;}
 private queue=Promise.resolve();
 async transaction<T>(fn:(tx:Storage)=>Promise<T>):Promise<T>{
   const preceding=this.queue;let unlock!:()=>void;
   this.queue=new Promise<void>(resolve=>{unlock=resolve;});await preceding;
   const backup=structuredClone(this.map);
   try{return await fn(this);}catch(e){this.map=backup;throw e;}finally{unlock();}
 }
}
const s=new Storage();
ok((await governanceRequest(s,"GET")).status===503,"missing registered state never silently initialized");
await s.put("account_policy_v1",{corrupt:true});await s.put("account_governance_v2",{corrupt:true});
await s.put("risk_book_v1",{corrupt:true});s.reads=[];
ok((await initializeGovernance(s,initial)).status===200,"explicit clean installation");
ok((await governanceRequest(s,"GET")).body.complete_all_account_census==="not_evaluated","unmeasured_census_is_not_denied");
ok(s.reads.every(k=>!k.includes("policy_v1")&&!k.includes("risk_book_v1")),"legacy_stop_claims_and_epochs_are_never_read");
ok((await initializeGovernance(s,initial)).status===409,"installation cannot reset existing state");
const observation={uid_fingerprint:"a".repeat(64),venue:"okx" as const,environment:"real" as const,label:"Primary",
 alias:"primary",credential_ref:"ref_primary",enabled:true,limits,idempotency_key:"connect_primary_01"};
const connected=await enrollObservedAccount(s,observation);
ok(connected.status===200&&(connected.body.account as any).enabled,"connect saves access and limits together");
ok((await enrollObservedAccount(s,observation)).body.status==="CONNECTED","same key returns original outcome");
ok((await enrollObservedAccount(s,{...observation,label:"changed"})).status===409,"same_key_different_body_conflicts");
const second=await enrollObservedAccount(s,{...observation,uid_fingerprint:"b".repeat(64),alias:"second",idempotency_key:"connect_second_01"});
ok(second.status===200,"connect second");
const status=(await governanceRequest(s,"GET")).body;
ok((status.accounts as any[])[0].enabled&&!(status.accounts as any[])[0].explicit_pause,"enroll_b_does_not_disable_a");
const revision=status.revision as number;
const lowered=await lowerCampaignLimit(s,{expected_revision:revision,campaign_loss_limit_usd:"8",idempotency_key:"lower_campaign_01"});
ok(lowered.status===200&&lowered.body.campaign_loss_limit_usd==="8","owner may only lower campaign ceiling");
ok((await lowerCampaignLimit(s,{expected_revision:revision+1,campaign_loss_limit_usd:"10",idempotency_key:"raise_campaign_01"})).status===409,"campaign ceiling cannot increase");
ok(JSON.stringify(await lowerCampaignLimit(s,{expected_revision:revision,campaign_loss_limit_usd:"8",idempotency_key:"lower_campaign_01"}))===JSON.stringify(lowered),"campaign lowering is body-bound idempotent");
console.log("account_governance_lower_campaign_limit_monotonic PASS");
const restarted=new Storage();restarted.map=structuredClone(s.map);
ok(((await governanceRequest(restarted,"GET")).body.accounts as any[])[0].enabled,"access_survives_restart");
console.log("access_restart_and_account_registration PASS");
const pause={account:"primary",expected_revision:revision+1,explicit_pause:true,idempotency_key:"pause_primary_001"};
ok((await setAccountPause(s,pause)).status===200,"explicit pause saved");
ok((await setAccountPause(s,pause)).status===200,"pause retry returns same revision");
const paused=(await governanceRequest(s,"GET")).body;
ok((paused.accounts as any[])[0].explicit_pause&&(paused.accounts as any[])[1].enabled,"pause scoped to account");
ok((await setAccountPause(s,{...pause,expected_revision:paused.revision,explicit_pause:false,idempotency_key:"resume_primary_01"})).status===200,"one command resume");
const before=(await governanceRequest(s,"GET")).body;
const update={expected_revision:before.revision,max_total_modelled_loss_usdt:"4",accounts:before.accounts};
const concurrent=await Promise.all([governanceRequest(s,"POST",{...update,idempotency_key:"limits_change_001"}),
 governanceRequest(s,"POST",{...update,idempotency_key:"limits_change_002"})]);
ok(concurrent.filter(r=>r.status===200).length===1&&concurrent.filter(r=>r.status===409).length===1,"concurrent CAS one winner");
ok((await governanceRequest(s,"POST",update)).status===400,"all mutations require idempotency key");
const current=(await governanceRequest(s,"GET")).body;
const currentRevision=current.revision as number;
const entryRequest={expected_revision:current.revision,
 max_total_modelled_loss_usdt:current.max_total_modelled_loss_usdt,
 accounts:current.accounts,campaign_entry_limit_usd:"8",
 idempotency_key:"entry_campaign_001"};
const changedEntry=await governanceRequest(s,"POST",entryRequest);
ok(changedEntry.status===200&&changedEntry.body.campaign_entry_limit_usd==="8",
 "authenticated existing CAS publishes independent entry ceiling");
ok(JSON.stringify(await governanceRequest(s,"POST",entryRequest))===JSON.stringify(changedEntry),
 "lost entry cap CAS reply is idempotent");
ok((await governanceRequest(s,"POST",{
 ...entryRequest,expected_revision:currentRevision+1,campaign_entry_limit_usd:"10",
 idempotency_key:"entry_campaign_raise_001"
})).status===409,"global entry limit cannot be raised by routine CAS");
ok((await governanceRequest(s,"POST",{
 ...entryRequest,expected_revision:currentRevision+1,campaign_entry_limit_usd:"0",
 idempotency_key:"entry_campaign_zero_001"
})).status===409,"cannot set entry cap zero to credit consumption");
ok((await stopNewRealRisk(s,"stop_real_test_01")).status===200,"emergency stop persisted");
const stopped=(await governanceRequest(s,"GET")).body;
ok((stopped.accounts as any[]).every(a=>a.enabled&&a.explicit_pause),"STOP does not revoke access or remove cohort");
const corrupt=new Storage();await corrupt.put(GOVERNANCE_STORAGE_KEY,{bad:true});
ok((await governanceRequest(corrupt,"GET")).status===503,"corrupt state never reset");

const malformed={...update,expected_revision:stopped.revision,accounts:[null],idempotency_key:"malformed_limits_01"};
ok((await governanceRequest(s,"POST",malformed)).status===409,"malformed account rows fail without throwing");
class FailingStorage extends Storage{
 failReceipt=false;
 override async put(key:string,value:unknown){if(this.failReceipt&&key.startsWith("access_operation:"))throw Error("disk full");return super.put(key,value);}
}
const atomic=new FailingStorage();await initializeGovernance(atomic,initial);atomic.failReceipt=true;
let failed=false;try{await enrollObservedAccount(atomic,observation);}catch{failed=true;}
ok(failed&&(await governanceRequest(atomic,"GET")).body.revision===0,"access and outcome rollback atomically");
atomic.failReceipt=false;
ok((await enrollObservedAccount(atomic,observation)).status===200,"failed commit is safely retried once");

const oldFinancialKeys=["risk_book_v1","account_governance_v2","account_policy_v1"];
const lifecycle={operation_id:"clean_initialize_0001",release_sha256:"a".repeat(64),contract:FRAME_SCHEMA};
function seedOld(storage:Storage){for(const key of oldFinancialKeys)storage.map.set(key,{fixture:"never read or import"});storage.map.set("oauth_fixture",{preserved:"oauth"});storage.map.set("session_fixture",{preserved:"session"});storage.map.set("generation",77);}
function oldPresent(storage:Storage){return oldFinancialKeys.every(key=>storage.map.has(key));}
function noOldReads(storage:Storage){return storage.reads.every(key=>!oldFinancialKeys.includes(key));}
const implicit=new Storage();seedOld(implicit);
ok((await initializeGovernance(implicit,initial)).status===200&&oldPresent(implicit)&&implicit.deletes.length===0,"no lifecycle never retires old keys");
const clean=new Storage();seedOld(clean);
const initialized=await initializeGovernance(clean,initial,lifecycle);
ok(initialized.status===200&&initialized.body.retired_financial_keys_removed===3,"confirmed exact retirement count");
ok(oldFinancialKeys.every(key=>!clean.map.has(key))&&JSON.stringify(clean.deletes)==JSON.stringify([oldFinancialKeys]),"only exact financial keys deleted");
ok(clean.map.get("oauth_fixture").preserved==="oauth"&&clean.map.get("session_fixture").preserved==="session"&&clean.map.get("generation")===77,"unrelated OAuth and session data preserved");
ok(clean.map.has("risk_meta_v2")&&clean.map.has(GOVERNANCE_STORAGE_KEY)&&clean.map.has("installation_release_v1"),"retirement and new state commit together");
for(const key of oldFinancialKeys)clean.map.set(key,{fixture:"reintroduced after init"});
const replay=await initializeGovernance(clean,initial,lifecycle);
ok(JSON.stringify(replay)===JSON.stringify(initialized)&&clean.deletes.length===1&&oldPresent(clean),"exact replay returns original outcome without deleting again");
ok((await initializeGovernance(clean,initial,{...lifecycle,release_sha256:"b".repeat(64)})).status===409&&clean.deletes.length===1&&oldPresent(clean),"conflicting replay never erases");
ok((await initializeGovernance(clean,initial,{...lifecycle,operation_id:"different_initialize_0001"})).status===409&&clean.deletes.length===1,"already initialized state never retires again");
const invalid=new Storage();seedOld(invalid);
ok((await initializeGovernance(invalid,{...initial,revision:1},lifecycle)).status===400&&oldPresent(invalid)&&invalid.deletes.length===0,"invalid init never erases");
for(const stateKey of ["risk_meta_v2","installation_release_v1"]){const existing=new Storage();seedOld(existing);existing.map.set(stateKey,{fixture:"existing new state"});ok((await initializeGovernance(existing,initial,lifecycle)).status===409&&oldPresent(existing)&&existing.deletes.length===0,"orphan new state cannot be reset");ok(noOldReads(existing),"orphan check reads no old values");}
class DeleteFailure extends Storage{override async delete(keys:string[]):Promise<number>{this.deletes.push([...keys]);this.map.delete(keys[0]);throw Error("fixture delete failure");}}
const deleteFailure=new DeleteFailure();seedOld(deleteFailure);let deleteFailed=false;
try{await initializeGovernance(deleteFailure,initial,lifecycle);}catch{deleteFailed=true;}
ok(deleteFailed&&oldPresent(deleteFailure)&&!deleteFailure.map.has("risk_meta_v2")&&!deleteFailure.map.has(GOVERNANCE_STORAGE_KEY),"partial delete failure rolls back atomically");
class NewStateFailure extends Storage{override async put(key:string,value:unknown){if(key==="risk_meta_v2")throw Error("fixture write failure");return super.put(key,value);}}
const writeFailure=new NewStateFailure();seedOld(writeFailure);let writeFailed=false;
try{await initializeGovernance(writeFailure,initial,lifecycle);}catch{writeFailed=true;}
ok(writeFailed&&oldPresent(writeFailure)&&!writeFailure.map.has("installation_operation:"+lifecycle.operation_id),"new state write failure restores retired keys and leaves no receipt");
ok([implicit,clean,invalid,deleteFailure,writeFailure].every(noOldReads),"retirement never reads old values");
console.log("clean_initialization_deletes_only_old_financial_keys PASS");
