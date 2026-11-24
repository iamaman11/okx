import {initial} from "./account_governance.test.js";
import {initializeRiskBook, lowerRiskBookLimit, lowerCampaignEntryLimit, installCampaignEntryProof} from "./risk_book.js";
import {handleNativeRiskCommand} from "./risk_authority.js";
import {initializeGovernance, lowerCampaignLimit} from "./account_governance_store.js";
import {disableLegacyFinance, inspectLegacyFinanceCutover, LEGACY_FINANCE_DISABLED_KEY,
  LEGACY_FINANCE_DISABLED} from "./legacy_finance_cutover.js";
import {RuntimeSession} from "./runtime_session.js";

function check(value: unknown, message: string): asserts value {if(!value)throw Error(message);}
class Store {
  map=new Map<string, any>(); writes=0;
  async get(key:string){return structuredClone(this.map.get(key));}
  async put(key:string,value:unknown){this.writes++;this.map.set(key,structuredClone(value));}
  async list(options:{prefix:string;limit:number;startAfter?:string}){
    return new Map([...this.map].sort(([a],[b])=>a<b?-1:a>b?1:0)
      .filter(([key])=>key.startsWith(options.prefix)&&(!options.startAfter||key>options.startAfter))
      .slice(0,options.limit).map(([key,value])=>[key,structuredClone(value)]));
  }
  private tail=Promise.resolve();
  async transaction<T>(fn:(tx:Store)=>Promise<T>):Promise<T>{
    const previous=this.tail;let release!:()=>void;
    this.tail=new Promise<void>(resolve=>{release=resolve;});
    await previous;
    const before=structuredClone(this.map);
    try{return await fn(this);}catch(error){this.map=before;throw error;}finally{release();}
  }
}
const storage=new Store();
await storage.put("account_access_v1",initial);
await initializeRiskBook(storage,initial.installation_id);
// Preserve nonzero UNKNOWN obligations and original opaque facts, never EMPTY.
await storage.put("risk_entry_v2:original",{state:"UNKNOWN",entry_settled_usd:"0.125",revision:7});
await storage.put("access_operation:original",{hash:"b".repeat(64),reply:{status:200}});
await storage.put("unknown_financial_format_v0",{opaque_original:true});
for(let i=0;i<1003;i++)await storage.put("risk_entry_v2:page_"+String(i).padStart(4,"0"),{state:"HELD",n:i});
const request={schema:"okx.legacy-finance-disable/v1",installation_id:initial.installation_id,
  operation_id:"disable_original_0001",source_tree:"a".repeat(40)};
check((await inspectLegacyFinanceCutover(storage)).status===409,"no fabricated disabled receipt before cutover");
check((await disableLegacyFinance(storage,{...request,installation_id:"different"},100)).status===409,
  "wrong installation cannot disable namespace");
check(!storage.map.has(LEGACY_FINANCE_DISABLED_KEY),"scope failure leaves no receipt");
const disabled=await disableLegacyFinance(storage,request,101);
check(disabled.status===200,"original owner commits disabled record");
const writes=storage.writes;
const replay=await disableLegacyFinance(storage,request,999);
check(JSON.stringify(replay)===JSON.stringify(disabled)&&storage.writes===writes,"exact replay preserves original receipt/time");
check((await disableLegacyFinance(storage,{...request,operation_id:"different_request"},102)).status===409,
  "different request cannot replace retirement evidence");
const readback=await inspectLegacyFinanceCutover(storage);
check(readback.status===200&&readback.body.inventory_complete===true,"complete paginated readback");
const sources=readback.body.sources as Record<string,any>;
check(sources["risk_entry_v2:original"].state==="UNKNOWN"&&Object.keys(sources).length===1008,
  "all pages and UNKNOWN obligation retained");
check(sources["access_operation:original"].hash==="b".repeat(64)
  &&sources.unknown_financial_format_v0.opaque_original===true,"policy receipts and unknown originals preserved");
check(storage.writes===writes,"readback is read-only");
const session={hello:true,runtime_profile:"engine",connection_generation:1};
for(const action of ["SUMMARY","INSTALL_ENTRY_OPENING","ACCOUNT_SUMMARY","INSPECT","RESERVE",
  "SETTLE_ENTRY","RELEASE_UNSENT","BEGIN_POST","RESOLVE"]){
  const result=await handleNativeRiskCommand(storage,session,{schema:"okx.risk-command/v1",
    command_id:"replayed_command_0001",installation_id:initial.installation_id,action:{action}},103);
  check(result.status==="DENIED"&&result.reason===LEGACY_FINANCE_DISABLED,"retired legacy action "+action);
}
check(await lowerRiskBookLimit(storage,"9")===false,"direct budget reduction guarded");
check(await lowerCampaignEntryLimit(storage,"10","9")===false,"direct entry limit guarded");
check(await installCampaignEntryProof(storage,{} as any)===false,"direct opening guarded");
let initializeDenied=false;
try{await initializeRiskBook(storage,initial.installation_id);}catch(error){initializeDenied=String(error).includes(LEGACY_FINANCE_DISABLED);}
check(initializeDenied,"direct reinitialization guarded");
check((await initializeGovernance(storage,initial)).body.reason===LEGACY_FINANCE_DISABLED,
  "governance reinitialization guarded before idempotent old receipt");
check((await lowerCampaignLimit(storage,{campaign_loss_limit_usd:"9",expected_revision:0,
  idempotency_key:"lower_original_0001"})).body.reason===LEGACY_FINANCE_DISABLED,
  "governance budget mutation guarded");
const owner1=new RuntimeSession({storage,getWebSockets:()=>[]},{} as any);
const owner2=new RuntimeSession({storage,getWebSockets:()=>[]},{} as any);
for(const owner of [owner1,owner2]){
  const response=await owner.fetch(new Request("https://runtime.internal/installation/legacy-finance-readback"));
  check(response.status===200,"new RuntimeSession retains same durable retirement");
  const status=await owner.fetch(new Request("https://runtime.internal/status"));
  check(status.status===200,"transport remains available after finance retirement");
}
check(storage.writes===writes,"denied commands and restarted readback never change financial history");
await storage.put("generation",42);
check((await inspectLegacyFinanceCutover(storage)).status===200,"transport generation is independent of financial retirement");
await storage.put("risk_entry_v2:original",{state:"RELEASED"});
check((await inspectLegacyFinanceCutover(storage)).body.reason==="LEGACY_FINANCE_INVENTORY_CHANGED",
  "independent financial corruption cannot pass immutable readback");
await storage.put(LEGACY_FINANCE_DISABLED_KEY,{state:"unknown"});
check((await inspectLegacyFinanceCutover(storage)).body.reason==="LEGACY_FINANCE_DISABLE_RECEIPT_INVALID",
  "unknown marker cannot become authority");
check((await handleNativeRiskCommand(storage,session,{schema:"okx.risk-command/v1",
  command_id:"corrupt_command_0001",action:{action:"RESERVE"}},104)).reason===LEGACY_FINANCE_DISABLED,
  "corrupt marker never re-enables financial route");
console.log("legacy_finance_cutover direct contracts PASS (not physical Cloudflare evidence)");
