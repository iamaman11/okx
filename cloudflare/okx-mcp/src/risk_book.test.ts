import {amount,units,readRiskBook,reserveRisk,transitionRisk} from "./risk_book.js";
function check(value: boolean, label:string) { if(!value) throw Error(label); }
class Storage {
  map=new Map<string,unknown>();
  async get(k:string){return this.map.get(k);}
  async put(k:string,v:unknown){this.map.set(k,v);}
  private pending=Promise.resolve();
  async transaction<T>(fn:(tx:Storage)=>Promise<T>):Promise<T> {
    const old=this.pending;
    let unlock!:()=>void;
    this.pending=new Promise<void>(resolve=>{unlock=resolve;});
    await old;try{return await fn(this);}finally{unlock();}
  }
}
const a="a".repeat(64),b="b".repeat(64),p="c".repeat(64),d="d".repeat(64);
const now=1792344435177;
const policy={revision:1,enabled:true,authorized_ceiling_usdt:"10",
  global_limit_usdt:"10",account_limits_usdt:{[a]:"10",[b]:"10"}};
const census={complete:true as const,source_digest:d,generated_at_ms:now,
  account_uids:[a,b],occupied:[{uid:a,occupied_usdt:"0"},{uid:b,occupied_usdt:"0"}]};
const base={venue:"okx",environment:"real" as const,run_id:"run_20261018_AAA",
  plan_hash:p,policy_revision:1,now_ms:now};
const sa=new Storage();
const reqA={...base,uid:a,intent_id:"intent_20261018_aaaa",loss_usdt:"6"};
const reqB={...base,uid:b,intent_id:"intent_20261018_bbbb",loss_usdt:"5"};
check(units("1.000000001")===1000000001n,"exact 9-decimal");
check(units("1e10")===null&&units("-1")===null&&units("0.0000000001")===null,"no float/exponents");
check(amount(1000000001n)==="1.000000001","round-trip exact");
const both=await Promise.all([reserveRisk(sa,policy,census,reqA),reserveRisk(sa,policy,census,reqB)]);
check(both[0].status==="HELD"&&both[1].status==="DENIED","serial competition 6+5 >10");
check((await readRiskBook(sa))?.entries.length===1,"one durable reservation");
check((await reserveRisk(sa,policy,census,reqA)).status==="HELD","idempotent exact retry");
check((await reserveRisk(sa,policy,census,{...reqA,plan_hash:d})).status==="DENIED","intent plan immutable");
check((await reserveRisk(sa,policy,{...census,account_uids:[a]},reqB)).status==="DENIED",
  "selected-only UID cannot assert empty other accounts");
check((await reserveRisk(sa,policy,{...census,generated_at_ms:now-5001},reqB)).status==="DENIED",
  "stale source is not zero risk");
check((await reserveRisk(sa,{...policy,global_limit_usdt:"4"},census,reqB)).status==="DENIED",
  "lowering limit does not free existing risk");
const entry=(await readRiskBook(sa))!.entries[0];
check((await transitionRisk(sa,entry.key,p,"UNKNOWN",d)).status==="UPDATED",
  "ack-loss risk remains");
check((await reserveRisk(sa,policy,census,reqB)).status==="DENIED","UNKNOWN consumes risk");
check((await transitionRisk(sa,entry.key,p,"HELD",d)).status==="DENIED","no backward mutation");
check((await transitionRisk(sa,entry.key,p,"RELEASED","invalid")).status==="DENIED","proof required");
check((await transitionRisk(sa,entry.key,p,"RELEASED",d)).status==="UPDATED","terminal reconcile release");
check((await reserveRisk(sa,policy,census,reqA)).status==="DENIED","released intent tombstone never reused");
check((await reserveRisk(sa,policy,census,reqB)).status==="HELD","risk freed after proof");
const v=await readRiskBook(sa);
check(v?.entries.length===2&&v.entries[0].state==="RELEASED"&&v.entries[1].state==="HELD","durability");
await sa.put("risk_book_v1",{fake:true});
check((await reserveRisk(sa,policy,census,{...reqB,intent_id:"intent_20261018_cccc"})).status==="DENIED",
  "corrupt owner fails closed");
