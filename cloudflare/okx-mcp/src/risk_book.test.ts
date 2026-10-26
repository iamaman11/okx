import {accountKey,amount,units,readRiskBook,initializeRiskBook,inspectRisk,reserveRisk,transitionRisk,beginRiskPost,releaseUnsentRisk} from "./risk_book.js";
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
const census={complete:true as const,source_digest:d,content_digest:a,
  source_read_floor_ms:now,generated_at_ms:now,
  account_keys:["okx:real:"+a,"okx:real:"+b],occupied:[{account_key:"okx:real:"+a,occupied_usdt:"0",cumulative_loss_usd:"0"},{account_key:"okx:real:"+b,occupied_usdt:"0",cumulative_loss_usd:"0"}]};
const base={venue:"okx",environment:"real" as const,run_id:"run_20261018_AAA",
  loss_usd:"6",usd_conversion:{usd_per_usdt:"1",source_digest:d,observed_at_ms:now},plan_hash:p,policy_revision:1,now_ms:now,strategy_id:"momentum_A",instrument_id:"BTC-USDT-SWAP"};
const sa=new Storage();
function account(uid:string){
 return {
  id:"acct_"+uid.slice(0,8),label:uid.slice(0,8),venue:"okx",
  environment:"real",uid_fingerprint:uid,identity_verified:true,enabled:true,explicit_pause:false,policy_revision:1,account_key:"okx:real:"+uid,
  strategies:[{id:"momentum_A",enabled:true,allocated_capital_usdt:"6",
    max_modelled_loss_usdt:"6",max_daily_realized_loss_usdt:"2",max_drawdown_ratio:"0.1"}],
  limits:{max_modelled_loss_usdt:"10",max_order_loss_usdt:"6",max_notional_usdt:"100",
    max_leverage:"3",max_daily_realized_loss_usdt:"2",max_drawdown_ratio:"0.1",
    allowed_instruments:["BTC-USDT-SWAP"]}
 };
}
const governance={
 schema:"okx.governance/v3",revision:1,installation_id:"installation_test",campaign_id:"sha256:"+d,campaign_loss_limit_usd:"10",
 max_total_modelled_loss_usdt:"10",accounts:[account(a),account(b)]
};
await sa.put("account_access_v1",governance);await sa.put("generation",77);await initializeRiskBook(sa,"installation_test");
const native={connection_generation:77,source_digest:d,native_account_evidence_complete:true,native_order_evidence_complete:true,native_ledger_evidence_complete:true};
const request={...base,uid:a,intent_id:"intent_12345678",loss_usdt:"6"};
const reserve=(input= request,storage=sa)=>reserveRisk(storage,policy,census,input,native);
check(units("1.000000001")===1000000001n&&units("1e9")===null,"decimal exact");
const both=await Promise.all([reserve(),reserve({...request,uid:b,intent_id:"intent_23456789",loss_usdt:"5",loss_usd:"5"})]);
check(both[0].status==="HELD"&&both[1].status==="DENIED","RISK-01 competing accounts aggregate");
const held=both[0];if(held.status!=="HELD")throw Error("held");
check((await reserve()).status==="HELD","central_reserve_reply_lost retry same body same receipt");
check((await reserve({...request,plan_hash:d})).status==="DENIED","same key different body conflicts");
check((await reserve({...request,loss_usd:"5"})).status==="DENIED","USD exact conversion rejected");
const missing=new Storage();await missing.put("generation",77);await missing.put("account_access_v1",governance);
check((await reserve(request,missing)).status==="DENIED","worker_storage_missing_is_not_zero_budget");
check((await transitionRisk(sa,held.entry.key,p,"RELEASED",d,{connection_generation:77,expected_book_sequence:1})).status==="DENIED","no unknown/held release without exact terminal proof");
// The content is identical on fresh reads; the native monotonic proof, not
// a changed market digest or cross-host epoch, establishes causality.
const later={...census,source_read_floor_ms:now-60000,generated_at_ms:now-60000};
const causal_proof={reserve_anchor:held.entry.key,
  native_incarnation:"1".repeat(32),elapsed_to_proof_ms:40,
  account_reads:census.account_keys.map(account_key=>({account_key,private_generation:5,
    source_read_id:"a".repeat(64),content_digest:"b".repeat(64),
    started_after_held_ms:2,completed_after_held_ms:20}))};
const invalid=await beginRiskPost(sa,held.entry.key,p,1,d,{connection_generation:77,expected_book_sequence:1,census:later,source_proof:native,causal_proof:{...causal_proof,account_reads:[...causal_proof.account_reads].reverse()},now_ms:now+2});
check(invalid.status==="DENIED"&&invalid.reason==="INVALID_NATIVE_POST_FENCE","wrong order of cohort readings cannot authorize POST");
const begun=await beginRiskPost(sa,held.entry.key,p,1,d,{connection_generation:77,expected_book_sequence:1,census:later,source_proof:native,causal_proof,now_ms:now+2});
check(begun.status==="POST_AUTHORIZED","same-content fresh causal proof with -60s host skew");

check((await beginRiskPost(sa,held.entry.key,p,1,d,{connection_generation:77,expected_book_sequence:2,census:later,source_proof:native,causal_proof,now_ms:now+3})).status==="DENIED","no duplicate POST fence");
check((await inspectRisk(sa,held.entry.key))?.entry.state==="POSTED","addressed readback");
const terminalCensus={...census,occupied:census.occupied.map(v=>({...v,cumulative_loss_usd:v.account_key===accountKey(request)?"2":"0"})),source_digest:a,generated_at_ms:now+4,source_read_floor_ms:now+4};
check((await transitionRisk(sa,held.entry.key,p,"RELEASED",d,{connection_generation:77,expected_book_sequence:2,terminal:{census:terminalCensus,source_proof:{...native,source_digest:a},ledger:{account_uid:a,run_id:base.run_id,intent_id:request.intent_id,state:"FILLED",record_digest:p,cumulative_loss_usd:"2",remaining_exposure_usdt:"0"},now_ms:now+5}})).status==="UPDATED","terminal exact release consumes cumulative losses");
check((await readRiskBook(sa))?.consumed_loss_usd==="2"&&(await readRiskBook(sa))?.occupied_usdt==="0","loss survives restart and release");
const reported={...census,occupied:census.occupied.map(v=>({...v,cumulative_loss_usd:v.account_key===accountKey(request)?"17":"0"}))};
const campaign=await reserveRisk(sa,policy,reported,{...request,intent_id:"new_intent_001",loss_usdt:"4",loss_usd:"4"},native);
check(campaign.status==="DENIED"&&campaign.reason==="CAMPAIGN_LOSS_BUDGET_EXHAUSTED","fresh losses charged before reserve even when admission denied");
check((await readRiskBook(sa))?.consumed_loss_usd==="17","campaign losses remain charged without releasing any new reservation");
console.log("risk_concurrent_accounts_campaign_limit PASS");
// A fresh reservation can be released only by the exact current native SQL
// writer's durable no-post receipt. No venue, fills or finance source needed.
const unsent=new Storage();
await unsent.put("account_access_v1",governance);
await unsent.put("generation",77);
await initializeRiskBook(unsent,"installation_test");
const rescue=await reserveRisk(unsent,policy,census,{...request,intent_id:"intent_unsent_001"},native);
if(rescue.status!=="HELD")throw Error("unsent reserve");
const receipt={reservation_key:rescue.entry.key,account_key:accountKey(request),
  intent_id:"intent_unsent_001",plan_hash:p,request_hash:"e".repeat(64),
  reason:"POST_CENSUS_UNPROVEN",record_digest:"f".repeat(64)};
check((await releaseUnsentRisk(unsent,rescue.entry.key,p,1,76,receipt)).status==="DENIED",
  "stale authenticated generation cannot release");
check((await releaseUnsentRisk(unsent,rescue.entry.key,p,1,77,
  {...receipt,plan_hash:d})).status==="DENIED","wrong plan cannot release");
check((await releaseUnsentRisk(unsent,rescue.entry.key,p,1,77,receipt)).status==="UPDATED",
  "exact no-send SQL proof releases HELD");
check((await releaseUnsentRisk(unsent,rescue.entry.key,p,2,77,receipt)).status==="DENIED",
  "central release tombstone prevents repeated budget mutation");
check((await readRiskBook(unsent))?.occupied_usd==="0" &&
 (await readRiskBook(unsent))?.consumed_loss_usd==="0",
 "release only reserved capacity and never credits realized losses");
console.log("risk_unsent_cas_release PASS");
census.occupied[0].cumulative_loss_usd="17";
// One million terminal records must not be enumerated or decoded on the hot path.
for(let i=0;i<1_000_000;i++)sa.map.set("risk_entry_v2:tombstone_"+i,{state:"RELEASED",key:i});
let reads=0;const original=sa.get.bind(sa);sa.get=async(k:string)=>{reads++;return original(k);};
const started=performance.now();check((await reserve()).status==="DENIED","posted retry cannot authorize");
check(reads<=7,"million history bounded point reads");
console.log(`million_history_hot_read PASS records=1000000 reads=${reads} elapsed_ms=${(performance.now()-started).toFixed(3)}`);
