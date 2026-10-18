import {handleNativeRiskCommand} from "./risk_authority.js";
import {defaultAccountPolicy} from "./account_policy.js";
import {migrateV1} from "./account_governance.js";
function check(v:boolean,n:string){if(!v)throw Error(n);}
class Store{
  map=new Map<string,unknown>();
  async get(k:string){return this.map.get(k);}
  async put(k:string,v:unknown){this.map.set(k,v);}
  private tail=Promise.resolve();
  async transaction<T>(fn:(tx:Store)=>Promise<T>):Promise<T>{
    const last=this.tail;let release!:()=>void;
    this.tail=new Promise<void>(r=>{release=r;});
    await last;try{return await fn(this);}finally{release();}
  }
}
const s=new Store(),session={connection_generation:99,runtime_profile:"production" as const,hello:true};
const now=1792344435177,hash="a".repeat(64),uid="b".repeat(64);
await s.put("generation",99);
await s.put("account_governance_v2",migrateV1(defaultAccountPolicy()));
const base={schema:"okx.risk-command/v1",command_id:"risk_command_00001",
 action:{action:"RESERVE",venue:"okx",account_uid:uid,run_id:"run_12345678",
 intent_id:"intent_12345678",strategy_id:"momentum_A",instrument_id:"BTC-USDT-SWAP",plan_sha256:hash,requested_loss_usdt:"1",
 policy_revision:1,census:{complete:true,source_digest:hash,generated_at_ms:now,
 account_keys:["okx:real:"+uid],occupied:[{account_key:"okx:real:"+uid,occupied_usdt:"0"}],
 native_account_evidence_complete:true,native_order_evidence_complete:true,
 native_ledger_evidence_complete:true}}};
check((await handleNativeRiskCommand(s,{...session,hello:false},base,now)).status==="DENIED",
  "unsolicited socket cannot mint risk");
check((await handleNativeRiskCommand(s,{...session,runtime_profile:"demo_acceptance"},base,now)).status==="DENIED",
  "demo socket cannot mint real risk");
check((await handleNativeRiskCommand(s,session,{...base,action:{...base.action,
 census:{...base.action.census,native_ledger_evidence_complete:false}}},now)).reason
  ==="ALL_ACCOUNT_NATIVE_EVIDENCE_UNPROVEN","incomplete native provenance");
check((await handleNativeRiskCommand(s,session,base,now)).reason
  ==="CENTRAL_REAL_AUTHORIZATION_INACTIVE","unverified legacy operator policy cannot grant risk");
check((await handleNativeRiskCommand(s,session,{...base,action:{action:"RESOLVE",
 reservation_key:"x",plan_sha256:hash,expected_risk_book_sequence:1,
 next_state:"RELEASED",source_proof_sha256:hash}},now)).status==="DENIED",
 "terminal release never available based on digest alone");
check((await s.get("risk_book_v1"))===undefined,"denials make no reservation");
