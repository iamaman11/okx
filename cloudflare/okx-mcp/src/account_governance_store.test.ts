import {governanceRequest,enrollObservedAccount,setAccountPause,stopNewRealRisk,initializeGovernance,GOVERNANCE_STORAGE_KEY} from "./account_governance_store.js";
import {initial,limits} from "./account_governance.test.js";
function ok(v:boolean,label:string){if(!v)throw Error(label);}
class Storage{
 map=new Map<string,any>(); reads:string[]=[];
 async get(key:string){this.reads.push(key);return structuredClone(this.map.get(key));}
 async put(key:string,value:unknown){this.map.set(key,structuredClone(value));}
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
const restarted=new Storage();restarted.map=structuredClone(s.map);
ok(((await governanceRequest(restarted,"GET")).body.accounts as any[])[0].enabled,"access_survives_restart");
const pause={account:"primary",expected_revision:status.revision,explicit_pause:true,idempotency_key:"pause_primary_001"};
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
