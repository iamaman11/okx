import {initializeRiskBook} from "./risk_book.js";
import {handleNativeRiskCommand} from "./risk_authority.js";
import {initial,account} from "./account_governance.test.js";
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
const s=new Store(),session={connection_generation:99,runtime_profile:"engine" as const,hello:true};
const now=1792344435177,hash="a".repeat(64),uid="b".repeat(64);
await s.put("generation",99);
await s.put("account_access_v1",initial);
const base={schema:"okx.risk-command/v1",command_id:"risk_command_00001",installation_id:initial.installation_id,
 action:{action:"RESERVE",venue:"okx",account_uid:uid,run_id:"run_12345678",
 intent_id:"intent_12345678",strategy_id:"momentum_A",instrument_id:"BTC-USDT-SWAP",plan_sha256:hash,requested_loss_usdt:"1",
 policy_revision:1,census:{complete:true,source_digest:hash,
 content_digest:hash,source_read_floor_ms:now,generated_at_ms:now,
 account_keys:["okx:real:"+uid],occupied:[{account_key:"okx:real:"+uid,occupied_usdt:"0",cumulative_loss_usd:"0"}],
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
  ==="CENTRAL_REAL_AUTHORIZATION_INACTIVE","no connected account cannot grant risk");
check((await handleNativeRiskCommand(s,session,{...base,action:{action:"RESOLVE",
 reservation_key:"x",plan_sha256:hash,expected_risk_book_sequence:1,
 next_state:"RELEASED",source_proof_sha256:hash}},now)).status==="DENIED",
 "terminal release never available based on digest alone");
check((await s.get("risk_book_v1"))===undefined,"denials make no reservation");

await initializeRiskBook(s,initial.installation_id);
const summary=await handleNativeRiskCommand(s,session,{schema:"okx.risk-command/v1",command_id:"summary_12345",installation_id:initial.installation_id,action:{action:"SUMMARY"}},now);
check(summary.status==="OBSERVED"&&summary.budget?.remaining_loss_usd==="10"&&summary.budget.source_as_of_ms===null,"global summary comes from central owner with explicit source quality");
const own=account.account_key,other="okx:real:"+"b".repeat(64);
await s.put("account_access_v1",{...initial,revision:1,accounts:[account,{...account,id:"secondary",account_key:other,uid_fingerprint:"b".repeat(64)}]});
const meta:any=await s.get("risk_meta_v2");
await s.put("risk_meta_v2",{...meta,occupied_usdt:"2",occupied_usd:"2",accounts:{[other]:"2"}});
await s.put("risk_account_v2:"+other,{active_reservations:1,occupied_usd:"2",occupied_usdt:"2",latest_activity_ms:now});
const addressed=await handleNativeRiskCommand(s,session,{schema:"okx.risk-command/v1",command_id:"recovery_12345",installation_id:initial.installation_id,action:{action:"ACCOUNT_SUMMARY",account_key:own}},now);
check(addressed.status==="OBSERVED"&&addressed.account_budget?.active_reservations===0&&addressed.account_budget?.occupied_usd==="0","own flat recovery does not require other account flat");
s.map.delete("risk_account_v2:"+other);
check((await handleNativeRiskCommand(s,session,{schema:"okx.risk-command/v1",command_id:"recovery_12346",installation_id:initial.installation_id,action:{action:"ACCOUNT_SUMMARY",account_key:other}},now)).reason==="RISK_ACCOUNT_AGGREGATE_MISSING","missing addressed aggregate is never zero");
s.map.delete("risk_meta_v2");
check((await handleNativeRiskCommand(s,session,{schema:"okx.risk-command/v1",command_id:"summary_12346",installation_id:initial.installation_id,action:{action:"SUMMARY"}},now)).reason==="RISK_BOOK_MISSING_OR_CORRUPT","missing central state is never zero budget");
