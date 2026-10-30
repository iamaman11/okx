import {initializeRiskBook,installCampaignEntryProof,openingDigest,type CampaignEntryOpeningSource} from "./risk_book.js";
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
 intent_id:"intent_12345678",strategy_id:"momentum_A",instrument_id:"BTC-USDT-SWAP",plan_sha256:hash,requested_loss_usdt:"1",entry_upper_usd:"1",entry_proof_digest:hash,
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
check(summary.status==="OBSERVED"&&summary.budget?.remaining_loss_usd==="10"&&summary.budget.source_as_of_ms===null&&
 summary.budget.entry_budget===null,
 "unproven campaign entry opening is explicitly null rather than zero");
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


// Three independently labelled decisions use the actual central owner and
// its transactional RiskBook. These are synthetic native-session inputs:
// a ChatGPT label is NOT a trusted actor identity or platform permission.
// Native SQLite + post_once are independently covered by the Rust fixtures.
{
 const t=new Store(),sameUid=account.uid_fingerprint;
 const sourceDigest="d".repeat(64),freshDigest="e".repeat(64),proof="f".repeat(64);
 const governance={...initial,revision:1,max_total_modelled_loss_usdt:"3",accounts:[{
   ...account,limits:{...account.limits,max_modelled_loss_usdt:"3",max_order_loss_usdt:"1",max_entry_usd:"10"},
   strategies:[{id:"native_alpha",enabled:true,allocated_capital_usdt:"1",
     max_modelled_loss_usdt:"1",max_daily_realized_loss_usdt:"2",
     max_drawdown_ratio:"0.1"}]
 }]};
 await t.put("generation",session.connection_generation);
 await t.put("account_access_v1",governance);
 await initializeRiskBook(t,initial.installation_id);
 const campaignStart=now-100000;
 const nativeOpening:CampaignEntryOpeningSource={
   campaign_id:governance.campaign_id,limit_usd:"10",consumed_usd:"0",
   source_digest:sourceDigest,as_of_ms:now,campaign_started_at_ms:campaignStart,
   accounts:[{account_key:account.account_key,history_from_ms:campaignStart,
     history_through_ms:now,consumed_entry_usd:"0",
     order_history_complete:true,fill_history_complete:true,
     bill_history_complete:true,finance_provenance_sealed:true,
     source_digest:"a".repeat(64)}]
 };
 nativeOpening.source_digest=await openingDigest(nativeOpening);
 check(await installCampaignEntryProof(t,nativeOpening),
   "synthetic native opening covers all managed real accounts");
 const census={complete:true,source_digest:sourceDigest,content_digest:hash,
   generated_at_ms:now,source_read_floor_ms:now,
   account_keys:[account.account_key],
   occupied:[{account_key:account.account_key,occupied_usdt:"0",cumulative_loss_usd:"0"}],
   native_account_evidence_complete:true,native_order_evidence_complete:true,
   native_ledger_evidence_complete:true};
 const reserve=(origin:string,strategy:string,plan:string)=>({
   schema:"okx.risk-command/v1",command_id:"reserve_"+origin+"_0001",
   installation_id:initial.installation_id,
   action:{action:"RESERVE",venue:"okx",account_uid:sameUid,
     run_id:"run_three_sources_001",intent_id:"intent_"+origin+"_0001",
     strategy_id:strategy,instrument_id:"BTC-USDT-SWAP",
     plan_sha256:plan,requested_loss_usdt:"1",requested_loss_usd:"1",entry_upper_usd:"2",entry_proof_digest:sourceDigest,
     usd_conversion:{usd_per_usdt:"1",source_digest:sourceDigest,observed_at_ms:now},
     policy_revision:1,census}
 });
 const externalHuman=reserve("human","manual","a".repeat(64));
 const externalChat=reserve("chat","manual","b".repeat(64));
 const autonomous=reserve("native","native_alpha","c".repeat(64));
 const admitted=await Promise.all([externalHuman,externalChat,autonomous]
   .map(command=>handleNativeRiskCommand(t,session,command,now)));
 check(admitted.every(r=>r.status==="HELD"&&!!r.reservation_key),
   "all three source labels must use the same central RiskBook transaction owner");
 check(new Set(admitted.map(r=>r.reservation_key)).size===3,
   "three different intents require three independent central reservations");
 const summary=await handleNativeRiskCommand(t,session,{
   schema:"okx.risk-command/v1",command_id:"summary_three_001",
   installation_id:initial.installation_id,action:{action:"SUMMARY"}},now);
 check(summary.status==="OBSERVED"&&summary.budget?.reserved_loss_usd==="3"&&
   summary.budget?.remaining_loss_usd==="7"&&
   summary.budget?.entry_budget?.reserved_usd==="6"&&
   summary.budget.entry_budget.remaining_usd==="4"&&
   summary.budget.entry_budget.consumed_usd==="0"&&
   summary.budget.entry_budget.opening_source_digest===nativeOpening.source_digest,
   "central readback separates modelled loss and entry spend for every source");
 const replay=await handleNativeRiskCommand(t,session,externalHuman,now);
 check(replay.status==="HELD"&&replay.reservation_key===admitted[0].reservation_key,
   "identical external retry returns the same central reservation");
 check((await handleNativeRiskCommand(t,session,{
   ...externalChat,action:{...externalChat.action,plan_sha256:proof}},now))
   .reason==="IDEMPOTENCY_CONFLICT",
   "changed ChatGPT-labelled request cannot reuse its central intent identity");
 const fourth=await handleNativeRiskCommand(t,session,reserve("fourth","manual",proof),now);
 check(fourth.status==="DENIED"&&fourth.reason==="INSUFFICIENT_AGGREGATE_OR_ACCOUNT_RISK",
   "fourth competing order must not bypass the single-account aggregate limit");
 const fresh={...census,source_digest:freshDigest,
   source_read_floor_ms:now+1,generated_at_ms:now+1};
 const post=(origin:string,index:number,plan:string,sequence:number)=>({
   schema:"okx.risk-command/v1",command_id:"post_"+origin+"_00001",
   installation_id:initial.installation_id,
   action:{action:"BEGIN_POST",reservation_key:admitted[index].reservation_key,
     plan_sha256:plan,policy_revision:1,expected_risk_book_sequence:sequence,
     source_proof_sha256:proof,census:fresh,
     causal_proof:{reserve_anchor:admitted[index].reservation_key,
       native_incarnation:"a".repeat(32),elapsed_to_proof_ms:30,
       account_reads:[{account_key:account.account_key,private_generation:7,
         source_read_id:"b".repeat(64),content_digest:hash,
         started_after_held_ms:5,completed_after_held_ms:20}]}}
 });
 const first=await handleNativeRiskCommand(t,session,post("human",0,"a".repeat(64),4),now+2);
 check(first.status==="POST_AUTHORIZED"&&first.risk_book_sequence===5,
   "fresh verified all-account census permits one exact central post fence");
 // This is an authenticated native-only settlement command, not an MCP
 // trading request. Use an isolated copy so revocation/late-no-send tests
 // continue to exercise their original risk-book sequence.
 const settlements=new Store();settlements.map=structuredClone(t.map);
 const sett=(seq:number,revision:number,amount:string,digest:string)=>({
   schema:"okx.risk-command/v1",command_id:"settle_native_0001",
   installation_id:initial.installation_id,
   action:{action:"SETTLE_ENTRY",reservation_key:admitted[0].reservation_key,
     plan_sha256:"a".repeat(64),expected_risk_book_sequence:seq,
     cumulative_entry_usd:amount,settlement_revision:revision,source_digest:digest}
 });
 check((await handleNativeRiskCommand(settlements,{...session,runtime_profile:"demo_acceptance"},
   sett(5,1,"0.5",sourceDigest),now+3)).status==="DENIED",
   "demo/public socket never inherits native money settlement authority");
 const heldCommand=sett(5,1,"0.5",sourceDigest);
 heldCommand.action.reservation_key=admitted[1].reservation_key!;
 heldCommand.action.plan_sha256="b".repeat(64);
 const heldCannotSettle=await handleNativeRiskCommand(t,session,
   heldCommand,now+2);
 check(heldCannotSettle.status==="DENIED"&&
   heldCannotSettle.reason==="ENTRY_SETTLEMENT_SOURCE_OR_STATE_UNPROVEN",
   "money cannot settle before the only authorized exchange POST");
 const partial=await handleNativeRiskCommand(settlements,session,
   sett(5,1,"0.5",sourceDigest),now+3);
 check(partial.status==="UPDATED"&&partial.risk_book_sequence===6,
   "native WSS settlement moves exactly one proved cumulative increment");
 const settlementReplay=await handleNativeRiskCommand(settlements,session,
   sett(6,1,"0.5",sourceDigest),now+4);
 check(settlementReplay.status==="UPDATED"&&settlementReplay.risk_book_sequence===6,
   "lost settlement receipt can be replayed without charging twice");
 check((await handleNativeRiskCommand(settlements,session,
   sett(6,1,"0.6",sourceDigest),now+5)).reason===
     "ENTRY_SETTLEMENT_REVISION_CONFLICT",
   "same source revision cannot replace settled amount");
 check((await handleNativeRiskCommand(settlements,session,
   sett(6,1,"0.5",hash),now+5)).reason===
     "ENTRY_SETTLEMENT_REVISION_CONFLICT",
   "same revision cannot substitute a different source digest");
 check((await handleNativeRiskCommand(settlements,session,
   sett(6,2,"3",sourceDigest),now+6)).reason===
     "ENTRY_SETTLEMENT_REGRESSION",
   "settlement above held cost never draws from another account");
 const settledSummary=await handleNativeRiskCommand(settlements,session,{
   schema:"okx.risk-command/v1",command_id:"summary_settled_001",
   installation_id:initial.installation_id,action:{action:"SUMMARY"}},now+7);
 check(settledSummary.budget?.entry_budget?.consumed_usd==="0.5"&&
   settledSummary.budget.entry_budget.reserved_usd==="5.5",
   "partial source settlement conserves sum across all three reservations");
 console.log("native_e1_wss_entry_settlement_cas PASS");

 check((await handleNativeRiskCommand(t,session,
   post("human_retry",0,"a".repeat(64),4),now+3)).status==="DENIED",
   "central RiskBook must never authorize the same POST twice");
 const revocation={...governance,revision:2,accounts:[
   {...governance.accounts[0],policy_revision:2,explicit_pause:true}]};
 await t.put("account_access_v1",revocation);
 const revoked=await handleNativeRiskCommand(t,session,
   post("chat",1,"b".repeat(64),4),now+4);
 check(revoked.status==="DENIED"&&
   revoked.reason==="CENTRAL_REVOCATION_OR_POLICY_CHANGE",
   "an existing HELD reservation grants no POST after central policy revocation");
 const unsent=await handleNativeRiskCommand(t,session,{
   schema:"okx.risk-command/v1",command_id:"unsent_chat_00001",
   installation_id:initial.installation_id,
   action:{action:"RELEASE_UNSENT",reservation_key:admitted[1].reservation_key,
     plan_sha256:"b".repeat(64),expected_risk_book_sequence:5,
     proof:{reservation_key:admitted[1].reservation_key,
       account_key:account.account_key,intent_id:"intent_chat_0001",
       plan_hash:"b".repeat(64),request_hash:"e".repeat(64),
       reason:"POLICY_CHANGED",record_digest:"f".repeat(64)}}
 },now+4);
 check(unsent.status==="UPDATED","native signed no-send frees held reserve despite governance pause");
 const inspect=await handleNativeRiskCommand(t,session,{
   schema:"okx.risk-command/v1",command_id:"inspect_chat_0001",
   installation_id:initial.installation_id,
   action:{action:"INSPECT",reservation_key:admitted[1].reservation_key,
     plan_sha256:"b".repeat(64)}},now+5);
 check(inspect.status==="OBSERVED"&&inspect.reservation_state==="RELEASED",
   "durable no-send proof releases capacity after revocation; no automatic release otherwise");
 check((await handleNativeRiskCommand(t,session,
   reserve("after_pause","native_alpha",proof),now+5)).status==="DENIED",
   "revoked account cannot admit more risk under stale policy");
 // The authenticated WSS handler must expose the empty book CAS and
 // a read-backable RELEASED fence, never an invented HELD reservation.
 const absentStore=new Store(),lateCommand=reserve("late","manual",hash);
 await absentStore.put("generation",session.connection_generation);
 await absentStore.put("account_access_v1",governance);
 await initializeRiskBook(absentStore,initial.installation_id);
 const lateKey=[lateCommand.action.venue,"real",sameUid,lateCommand.action.run_id,
   lateCommand.action.intent_id].map(v=>v.length+":"+v).join("");
 const inspectLate={schema:"okx.risk-command/v1",command_id:"inspect_absent_0001",
   installation_id:initial.installation_id,
   action:{action:"INSPECT",reservation_key:lateKey,plan_sha256:hash}};
 const before=await handleNativeRiskCommand(absentStore,session,inspectLate,now+1);
 check(before.status==="DENIED"&&before.reason==="RISK_RESERVATION_NOT_FOUND"&&
   before.risk_book_sequence===0,"F2 empty central book returns exact CAS zero");
 const abort=await handleNativeRiskCommand(absentStore,session,{
   schema:"okx.risk-command/v1",command_id:"abort_absent_0001",
   installation_id:initial.installation_id,
   action:{action:"RELEASE_UNSENT",reservation_key:lateKey,plan_sha256:hash,
     expected_risk_book_sequence:0,
     proof:{reservation_key:lateKey,account_key:account.account_key,
       intent_id:lateCommand.action.intent_id,plan_hash:hash,
       request_hash:"e".repeat(64),reason:"RESERVE_REPLY_LOST",
       record_digest:"f".repeat(64)}}},now+2);
 check(abort.status==="UPDATED"&&abort.risk_book_sequence===1,
   "F2 no-send fence accepts exact signed native proof at sequence zero");
 const seen=await handleNativeRiskCommand(absentStore,session,inspectLate,now+3);
 check(seen.status==="OBSERVED"&&seen.reservation_state==="RELEASED",
   "F2 lost negative-fence ACK is repeatably inspectable");
 const delayed=await handleNativeRiskCommand(absentStore,session,lateCommand,now+4);
 check(delayed.status==="DENIED"&&delayed.reason==="RESERVE_FENCED_NO_SEND",
   "late RESERVE can no longer mint risk");
 console.log("risk_f2_never_arrived_handler PASS");
 console.log("three_source_central_riskbook_cas PASS 3 HELD / 1 exact POST / revoked POST denied / no venue POST");
}
