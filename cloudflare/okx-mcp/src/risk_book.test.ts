import {accountKey,amount,units,readRiskBook,reserveRisk,transitionRisk,beginRiskPost} from "./risk_book.js";
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
  global_limit_usdt:"10",account_limits_usdt:{["okx:real:"+a]:"10",["okx:real:"+b]:"10"}};
const census={complete:true as const,source_digest:d,generated_at_ms:now,
  account_keys:["okx:real:"+a,"okx:real:"+b],occupied:[{account_key:"okx:real:"+a,occupied_usdt:"0"},{account_key:"okx:real:"+b,occupied_usdt:"0"}]};
const base={venue:"okx",environment:"real" as const,run_id:"run_20261018_AAA",
  plan_hash:p,policy_revision:1,now_ms:now,strategy_id:"momentum_A"};
const sa=new Storage();
function account(uid:string){
 return {
  id:"acct_"+uid.slice(0,8),label:uid.slice(0,8),venue:"okx",
  environment:"real",uid_fingerprint:uid,identity_verified:true,enabled:true,
  strategies:[{id:"momentum_A",enabled:true,allocated_capital_usdt:"6",
    max_modelled_loss_usdt:"6",max_daily_realized_loss_usdt:"2",max_drawdown_ratio:"0.1"}],
  limits:{max_modelled_loss_usdt:"10",max_order_loss_usdt:"6",max_notional_usdt:"100",
    max_leverage:"3",max_daily_realized_loss_usdt:"2",max_drawdown_ratio:"0.1",
    allowed_instruments:["BTC-USDT-SWAP"]}
 };
}
const governance={
 schema:"okx.governance/v2",revision:1,trading_requested:true,
 operator_authorization:{id:"sha256:"+d,status:"ACTIVE",ceiling_usdt:"10"},
 max_total_modelled_loss_usdt:"10",accounts:[account(a),account(b)]
};
await sa.put("account_governance_v2",governance);
const unattested=await reserveRisk(sa,policy,census,{...base,uid:a,
  intent_id:"intent_20261018_unproof",loss_usdt:"1"});
check(unattested.status==="DENIED","raw census digest is not native attestation");
const native={connection_generation:77,source_digest:census.source_digest,
  native_account_evidence_complete:true,native_order_evidence_complete:true,
  native_ledger_evidence_complete:true};
await sa.put("generation",77);
const reserve=(storage:Parameters<typeof reserveRisk>[0],bounds:Parameters<typeof reserveRisk>[1],
  source:Parameters<typeof reserveRisk>[2],input:Parameters<typeof reserveRisk>[3])=>
  reserveRisk(storage,bounds,source,input,native);
check((await reserve(sa,policy,{...census,source_digest:p},{...base,uid:a,
  intent_id:"intent_20261018_tamper",loss_usdt:"1"})).status==="DENIED",
 "census digest mismatch never reserves");
await sa.put("generation",78);
check((await reserve(sa,policy,census,{...base,uid:a,
  intent_id:"intent_20261018_stale",loss_usdt:"1"})).status==="DENIED",
 "prior-session risk census cannot be replayed");
await sa.put("generation",77);
const untrusted=new Storage();
check((await reserve(untrusted,policy,census,{...base,uid:a,intent_id:"intent_20261018_outs",loss_usdt:"1"})).status==="DENIED",
 "caller-supplied risk policy cannot mint capacity without persisted DO governance");
const reqA={...base,uid:a,intent_id:"intent_20261018_aaaa",loss_usdt:"6"};
const reqB={...base,uid:b,intent_id:"intent_20261018_bbbb",loss_usdt:"5"};
check(units("1.000000001")===1000000001n,"exact 9-decimal");
check(units("1e10")===null&&units("-1")===null&&units("0.0000000001")===null,"no float/exponents");
check(amount(1000000001n)==="1.000000001","round-trip exact");
const both=await Promise.all([reserve(sa,policy,census,reqA),reserve(sa,policy,census,reqB)]);
check(both[0].status==="HELD"&&both[1].status==="DENIED","serial competition 6+5 >10");
check((await readRiskBook(sa))?.entries.length===1,"one durable reservation");
check((await reserve(sa,policy,census,{...reqB,loss_usdt:"7"})).status==="DENIED",
 "per-order limit is enforced centrally before any reservation");
check((await reserve(sa,policy,census,{...reqA,intent_id:"intent_20261018_extra",
 loss_usdt:"1"})).status==="DENIED",
 "strategy risk cap cannot be exceeded by splitting orders");
check((await reserve(sa,policy,census,{...reqA,intent_id:"intent_20261018_fake",
 strategy_id:"unknown_strat",loss_usdt:"1"})).status==="DENIED",
 "unregistered strategy cannot reserve portfolio risk");
check((await reserve(sa,policy,census,reqA)).status==="HELD","idempotent exact retry");
check((await reserve(sa,policy,census,{...reqA,plan_hash:d})).status==="DENIED","intent plan immutable");
check((await reserve(sa,policy,{...census,account_keys:["okx:real:"+a]},reqB)).status==="DENIED",
  "selected-only UID cannot assert empty other accounts");
check((await reserve(sa,policy,{...census,generated_at_ms:now-5001},reqB)).status==="DENIED",
  "stale source is not zero risk");
check((await reserve(sa,{...policy,global_limit_usdt:"4"},census,reqB)).status==="DENIED",
  "lowering limit does not free existing risk");
const entry=(await readRiskBook(sa))!.entries[0];
check((await transitionRisk(sa,entry.key,p,"UNKNOWN",d)).status==="DENIED",
  "caller-supplied proof digest does not unlock risk state transition");
const beforePost=await readRiskBook(sa);
check((await beginRiskPost(sa,entry.key,p,1,d,{
 connection_generation:77,expected_book_sequence:beforePost!.sequence
})).status==="POST_AUTHORIZED","one native POST granted after durable reserve");
check((await beginRiskPost(sa,entry.key,p,1,d,{
 connection_generation:77,expected_book_sequence:beforePost!.sequence
})).status==="DENIED","same reservation cannot POST twice");
const transition=async(to:Parameters<typeof transitionRisk>[3],proof:string)=>{
 const current=await readRiskBook(sa);
 return transitionRisk(sa,entry.key,p,to,proof,{
  connection_generation:77,expected_book_sequence:current!.sequence
 });
};
check((await transition("UNKNOWN",d)).status==="UPDATED",
  "ack-loss risk remains");
check((await reserve(sa,policy,census,reqB)).status==="DENIED","UNKNOWN consumes risk");
check((await transition("HELD",d)).status==="DENIED","no backward mutation");
check((await transition("RELEASED","invalid")).status==="DENIED","proof required");
check((await transition("RELEASED",d)).status==="UPDATED","terminal reconcile release");
check((await reserve(sa,policy,census,reqA)).status==="DENIED","released intent tombstone never reused");
check((await reserve(sa,policy,census,reqB)).status==="HELD","risk freed after proof");
const v=await readRiskBook(sa);
check(v?.entries.length===2&&v.entries[0].state==="RELEASED"&&v.entries[1].state==="HELD","durability");
const disabled={...governance,accounts:governance.accounts.map(ac=>
  ac.uid_fingerprint===b?{...ac,enabled:false}:ac)};
await sa.put("account_governance_v2",disabled);
check((await reserve(sa,policy,census,{...base,uid:b,
  intent_id:"intent_20261018_disabled",loss_usdt:"1"})).status==="DENIED",
 "disabled account cannot open new risk");
await sa.put("account_governance_v2",governance);
await sa.put("risk_book_v1",{fake:true});
check((await reserve(sa,policy,census,{...reqB,intent_id:"intent_20261018_cccc"})).status==="DENIED",
  "corrupt owner fails closed");
