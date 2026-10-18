import {governanceRequest,enrollObservedAccount} from "./account_governance_store.js";
import {defaultAccountPolicy} from "./account_policy.js";
function ok(v:boolean,label:string){if(!v)throw Error(label);}
class Storage{
  map=new Map<string,unknown>();
  async get(key:string){return this.map.get(key);}
  async put(key:string,value:unknown){this.map.set(key,value);}
  private queue=Promise.resolve();
  async transaction<T>(fn:(tx:Storage)=>Promise<T>):Promise<T>{
    const preceding=this.queue;
    let unlock!:()=>void;
    this.queue=new Promise<void>(resolve=>{unlock=resolve;});
    await preceding;
    try{return await fn(this);}finally{unlock();}
  }
}
const s=new Storage();
const first=await governanceRequest(s,"GET");
ok(first.status===200&&first.body.revision===0&&first.body.trading_requested===false,"cold migration OFF");
ok(first.body.effective_trading===false&&first.body.risk_book_sequence===0,"never advertise native GO");
ok(first.body.available_for_new_risk_usdt===null&&first.body.complete_all_account_census===false,
 "empty RiskBook does not prove free portfolio budget");
ok(first.body.available_risk_reason==="NATIVE_COMPLETE_ACCOUNT_CENSUS_UNPROVEN",
 "explicit native risk blocker");
const update={expected_revision:0,trading_requested:false,max_total_modelled_loss_usdt:"2.75",accounts:[]};
const changed=await governanceRequest(s,"POST",update);
ok(changed.status===200&&changed.body.revision===1&&changed.body.max_total_modelled_loss_usdt==="2.75",
 "CAS update in same DO");
ok((await governanceRequest(s,"POST",update)).status===409,"stale revision rejected");
ok((await governanceRequest(s,"POST",{...update,expected_revision:1,trading_requested:true})).status===409,
 "UNVERIFIED grant cannot enable Real");
ok((await governanceRequest(s,"POST",{...update,expected_revision:1,max_total_modelled_loss_usdt:"11"})).status===409,
 "operator cannot exceed grant");
const noDual=await governanceRequest(s,"GET");
ok(noDual.status===200&&noDual.body.revision===1,"durable readback");
const concurrent=await Promise.all([
 governanceRequest(s,"POST",{...update,expected_revision:1,max_total_modelled_loss_usdt:"3"}),
 governanceRequest(s,"POST",{...update,expected_revision:1,max_total_modelled_loss_usdt:"4"})
]);
ok(concurrent.filter(row=>row.status===200).length===1
  &&concurrent.filter(row=>row.status===409).length===1,
 "simultaneous configured-limit writes cannot both win a CAS revision");
ok((await governanceRequest(s,"GET")).body.revision===2,"one durable winning revision");
const legacy=new Storage();
await legacy.put("account_policy_v1",{...defaultAccountPolicy(),
 revision:3,trading_requested:true,allowed_account_uids:["a".repeat(64)]});
const migrated=await governanceRequest(legacy,"GET");
ok(migrated.status===200&&migrated.body.revision===3&&migrated.body.trading_requested===false,
 "legacy enabled never migrates into LIVE grant");
ok(Array.isArray(migrated.body.accounts)&&migrated.body.accounts.length===1
  &&(migrated.body.accounts[0] as any).identity_verified===false,"no UID attestation invented");
const corrupted=new Storage();
await corrupted.put("account_policy_v1",{schema:"corrupt"});
ok((await governanceRequest(corrupted,"GET")).status===503,"corrupt legacy fails closed");
const badrisk=new Storage();
await badrisk.put("risk_book_v1",{fake:true});
ok((await governanceRequest(badrisk,"GET")).status===503,"corrupt risk owner fails closed");
ok(await badrisk.get("account_governance_v2")===undefined,
  "corrupt risk owner never partially writes a migrated governance policy");
const badgovernance=new Storage();
await badgovernance.put("account_governance_v2",{bad:true});
ok((await governanceRequest(badgovernance,"GET")).status===503,"corrupt policy fails closed");

const fresh=new Storage();
ok((await governanceRequest(fresh,"GET")).status===200,"initialize native registry");
const observation={uid_fingerprint:"a".repeat(64),venue:"okx" as const,
 environment:"real" as const,label:"Primary OKX"};
const enrolled=await enrollObservedAccount(fresh,observation);
ok(enrolled.status===200&&enrolled.body.status==="REGISTERED_DISABLED",
 "fresh authenticated UID registers metadata into the same DO");
const current=await governanceRequest(fresh,"GET");
ok(current.status===200&&current.body.trading_requested===false
 &&Array.isArray(current.body.accounts)&&current.body.accounts.length===1
 &&(current.body.accounts[0] as any).enabled===false
 &&(current.body.accounts[0] as any).identity_verified===true,
 "registration is verified identity without automatically granting trading");
ok((await enrollObservedAccount(fresh,observation)).body.status==="ALREADY_REGISTERED",
 "same authenticated UID is idempotent");
const initialRevision=(current.body.revision as number);
ok((await enrollObservedAccount(fresh,{...observation,uid_fingerprint:"b".repeat(64)})).status===200,
 "second account uses same catalog owner");
ok((await governanceRequest(fresh,"GET")).body.revision===initialRevision+1,
 "central revision advances once per new registered account");
ok((await enrollObservedAccount(fresh,{...observation,uid_fingerprint:"x".repeat(64)})).status===400,
 "caller cannot invent invalid UID fingerprints");
ok((await governanceRequest(fresh,"GET")).body.trading_requested===false,
 "new account never converts old operator authority into GO");
