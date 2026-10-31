import {accountKey,amount,units,readRiskBook,initializeRiskBook,inspectRisk,reserveRisk,transitionRisk,beginRiskPost,releaseUnsentRisk,installCampaignEntryProof,openingDigest,settleEntrySpend,type CampaignEntryOpeningSource} from "./risk_book.js";
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
  loss_usd:"6",entry_upper_usd:"2",entry_proof_digest:d,usd_conversion:{usd_per_usdt:"1",source_digest:d,observed_at_ms:now},plan_hash:p,policy_revision:1,now_ms:now,strategy_id:"momentum_A",instrument_id:"BTC-USDT-SWAP"};
async function openingFixture():Promise<CampaignEntryOpeningSource>{
 const start=now-100_000;
 const accounts=[a,b].map(uid=>({
  account_key:"okx:real:"+uid,
  history_from_ms:start,history_through_ms:now,consumed_entry_usd:"0",
  order_history_complete:true as const,fill_history_complete:true as const,
  bill_history_complete:true as const,finance_provenance_sealed:true as const,
  source_digest:uid,
 }));
 const source:CampaignEntryOpeningSource={
  campaign_id:governance.campaign_id,limit_usd:"10",consumed_usd:"0",
  source_digest:d,as_of_ms:now,campaign_started_at_ms:start,accounts
 };
 source.source_digest=await openingDigest(source);
 return source;
}
async function setupRiskBook(store:Storage){
 await initializeRiskBook(store,"installation_test",{
   installation_id:"installation_test",
   campaign_id:governance.campaign_id,
   started_at_ms:now-100_000
 });
 // Synthetic complete-venue fixture ONLY; no production native opening
 // importer exists yet. A hash alone cannot prove real campaign history.
 check(await installCampaignEntryProof(store,await openingFixture()),
  "synthetic full-cohort opening witness accepted in isolated test");
}
const sa=new Storage();
function account(uid:string){
 return {
  id:"acct_"+uid.slice(0,8),label:uid.slice(0,8),venue:"okx",
  environment:"real",uid_fingerprint:uid,identity_verified:true,enabled:true,explicit_pause:false,policy_revision:1,account_key:"okx:real:"+uid,
  strategies:[{id:"momentum_A",enabled:true,allocated_capital_usdt:"6",
    max_modelled_loss_usdt:"6",max_daily_realized_loss_usdt:"2",max_drawdown_ratio:"0.1"}],
  limits:{max_modelled_loss_usdt:"10",max_order_loss_usdt:"6",max_notional_usdt:"100",max_entry_usd:"10",
    max_leverage:"3",max_daily_realized_loss_usdt:"2",max_drawdown_ratio:"0.1",
    allowed_instruments:["BTC-USDT-SWAP"]}
 };
}
const governance={
 schema:"okx.governance/v3",revision:1,installation_id:"installation_test",campaign_id:"sha256:"+d,campaign_loss_limit_usd:"10",campaign_entry_limit_usd:"10",
 max_total_modelled_loss_usdt:"10",accounts:[account(a),account(b)]
};
await sa.put("account_access_v1",governance);await sa.put("generation",77);await setupRiskBook(sa);
// E1.1 nonzero historical USD cannot be reset to zero by reinstalling a
// captured opening. This source is constructed from immutable native SQL
// settlement in production; here only the typed Worker accountant is tested.
const past=new Storage();
await past.put("account_access_v1",governance);
await past.put("generation",77);
await initializeRiskBook(past,"installation_test",{
  installation_id:"installation_test",campaign_id:governance.campaign_id,
  started_at_ms:now-100_000
});
const pastProof=await openingFixture();
pastProof.accounts[0].consumed_entry_usd="5.01";
pastProof.consumed_usd="5.01";
pastProof.source_digest=await openingDigest(pastProof);
check(await installCampaignEntryProof(past,pastProof),
  "authenticated prior SWAP settlement consumes historical campaign capacity");
const pastBook=(await readRiskBook(past))!;
check(pastBook.entry_budget?.consumed_usd==="5.01"&&
  pastBook.entry_budget?.reserved_usd==="0"&&
  pastBook.entry_budget?.limit_usd==="10",
  "nonzero source initializes spent money, not a replenished budget");
const falsified={...pastProof,consumed_usd:"0"};
check(!await installCampaignEntryProof(past,falsified) &&
      (await readRiskBook(past))?.entry_budget?.consumed_usd==="5.01",
  "a later zero or different proof cannot overwrite historical spent USD");
// A persisted entry budget without its original campaign inception (or
// reattached to another campaign) is corrupt, not available capital.
const corruptedOrigin=new Storage();
corruptedOrigin.map=structuredClone(sa.map);
const originalMeta=await corruptedOrigin.get("risk_meta_v2") as any;
await corruptedOrigin.put("risk_meta_v2",{...originalMeta,
 campaign_origin:{...originalMeta.campaign_origin,campaign_id:"sha256:"+"f".repeat(64)}});
check(await readRiskBook(corruptedOrigin)===null,
 "cross-campaign budget never validates as money authority");
const native={connection_generation:77,source_digest:d,native_account_evidence_complete:true,native_order_evidence_complete:true,native_ledger_evidence_complete:true};
const request={...base,uid:a,intent_id:"intent_12345678",loss_usdt:"6"};
const reserve=(input= request,storage=sa)=>reserveRisk(storage,policy,census,input,native);
// Source-proven nonzero campaign opening still permits a FOLLOW-UP entry
// when both the remaining turnover and independent loss budgets allow it.
const historicalFollowup=await reserveRisk(past,policy,census,{
 ...request,intent_id:"intent_after_proven_history_002",
 loss_usdt:"1",loss_usd:"1",entry_upper_usd:"0.5"
},native);
check(historicalFollowup.status==="HELD",
 "a second entry is permitted within proved historical spend and risk limits");
const historicalNext=(await readRiskBook(past))!.entry_budget!;
const historicalRemaining = units(historicalNext.limit_usd)!
 - units(historicalNext.consumed_usd)!
 - units(historicalNext.reserved_usd)!;
check(historicalNext.consumed_usd==="5.01"&&
 historicalNext.reserved_usd==="0.5"&&historicalRemaining===units("4.49"),
 "next reservation never resets prior campaign spent or double-charges an entry");

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
// The Worker rechecks the currently stored per-account cap immediately
// before the only possible POST, even if RESERVE was already HELD.
const postCap=new Storage();
postCap.map=structuredClone(sa.map);
const postCapGov={...governance,accounts:[
 {...account(a),limits:{...account(a).limits,max_entry_usd:"1"}},
 account(b)
]};
await postCap.put("account_access_v1",postCapGov);
const narrowedPost=await beginRiskPost(postCap,held.entry.key,p,1,d,{
 connection_generation:77,expected_book_sequence:2,census:later,
 source_proof:native,causal_proof,now_ms:now+2
});
check(narrowedPost.status==="DENIED"&&
 narrowedPost.reason==="POST_ACCOUNT_ENTRY_LIMIT_UNPROVEN_OR_EXCEEDED",
 "after HELD, a tighter central account cap blocks the only exchange POST");
check((await inspectRisk(postCap,held.entry.key))?.entry.state==="HELD",
 "denied POST does not consume its one-shot exchange send fence");
const begun=await beginRiskPost(sa,held.entry.key,p,1,d,{connection_generation:77,expected_book_sequence:2,census:later,source_proof:native,causal_proof,now_ms:now+2});
check(begun.status==="POST_AUTHORIZED","same-content fresh causal proof with -60s host skew");
// Zero-fill CANCELED is a REAL source-backed terminal settlement amount,
// not an assumed price or a timeout. Test the same RiskBook transaction
// independently from the positive partial-fill path.
const zeroBook=new Storage();
zeroBook.map=structuredClone(sa.map); // original POSTED entry, sequence=3
const zeroSettled=await settleEntrySpend(zeroBook,held.entry.key,p,{
 connection_generation:77,expected_book_sequence:3,
 cumulative_entry_usd:"0",revision:1,source_digest:a
});
check(zeroSettled.status==="UPDATED"&&zeroSettled.book_sequence===4,
 "authenticated zero fill records a revision without debiting campaign spend");
const zeroCensus={...census,source_digest:a,
 source_read_floor_ms:now+4,generated_at_ms:now+4};
const zeroRelease=await transitionRisk(zeroBook,held.entry.key,p,"RELEASED",b,{
 connection_generation:77,expected_book_sequence:4,
 terminal:{
  census:zeroCensus,source_proof:{...native,source_digest:a},now_ms:now+5,
  ledger:{account_uid:a,run_id:base.run_id,intent_id:request.intent_id,
   state:"CANCELED",record_digest:p,cumulative_loss_usd:"0",
   remaining_exposure_usdt:"0",cumulative_entry_usd:"0",
   entry_settlement_revision:1,entry_settlement_digest:a}
 }
});
check(zeroRelease.status==="UPDATED"&&
 (await readRiskBook(zeroBook))?.entry_budget?.consumed_usd==="0"&&
 (await readRiskBook(zeroBook))?.entry_budget?.reserved_usd==="0",
 "proven zero canceled entry refunds reserved capacity without inventing spend");
const zeroReplay=await settleEntrySpend(zeroBook,held.entry.key,p,{
 connection_generation:77,expected_book_sequence:3,
 cumulative_entry_usd:"0",revision:1,source_digest:a
});
check(zeroReplay.status==="UPDATED"&&
 (await readRiskBook(zeroBook))?.entry_budget?.consumed_usd==="0",
 "lost zero-settlement ACK after release never charges twice");
const zeroForged=await settleEntrySpend(zeroBook,held.entry.key,p,{
 connection_generation:77,expected_book_sequence:3,
 cumulative_entry_usd:"0",revision:1,source_digest:b
});
check(zeroForged.status==="DENIED",
 "zero expense with different source digest cannot replay after release");
console.log("e1_zero_fill_canceled_central_settlement PASS");
const partial=await settleEntrySpend(sa,held.entry.key,p,{
 connection_generation:77,expected_book_sequence:3,cumulative_entry_usd:"0.5",
 revision:1,source_digest:d});
check(partial.status==="UPDATED"&&partial.book_sequence===4,
 "partial fill moves exactly its consumed entry money from reserved to spent");
const partialReplay=await settleEntrySpend(sa,held.entry.key,p,{
 connection_generation:77,expected_book_sequence:4,cumulative_entry_usd:"0.5",
 revision:1,source_digest:d});
check(partialReplay.status==="UPDATED"&&partialReplay.book_sequence===4,
 "lost settlement acknowledgement cannot charge entry twice");
const replayWithOriginalCas=await settleEntrySpend(sa,held.entry.key,p,{
 connection_generation:77,expected_book_sequence:3,cumulative_entry_usd:"0.5",
 revision:1,source_digest:d});
check(replayWithOriginalCas.status==="UPDATED"&&replayWithOriginalCas.book_sequence===4,
 "exact native retry with the original stale CAS must be read-only and successful");
const bogusFutureSequence=await settleEntrySpend(sa,held.entry.key,p,{
 connection_generation:77,expected_book_sequence:5,cumulative_entry_usd:"0.5",
 revision:1,source_digest:d});
check(bogusFutureSequence.status==="DENIED"&&bogusFutureSequence.reason===
 "ENTRY_SETTLEMENT_REVISION_CONFLICT",
 "future CAS sequence cannot be used even to inspect an idempotent settlement");
const staleNewExpense=await settleEntrySpend(sa,held.entry.key,p,{
 connection_generation:77,expected_book_sequence:3,cumulative_entry_usd:"0.75",
 revision:2,source_digest:d});
check(staleNewExpense.status==="DENIED"&&
 staleNewExpense.reason==="ENTRY_SETTLEMENT_SOURCE_OR_STATE_UNPROVEN",
 "new spend can never slip through the stale-CAS read-only replay exception");
const alteredStaleSource=await settleEntrySpend(sa,held.entry.key,p,{
 connection_generation:77,expected_book_sequence:3,cumulative_entry_usd:"0.5",
 revision:1,source_digest:a});
check(alteredStaleSource.status==="DENIED",
 "stale CAS cannot overwrite the authoritative settlement digest");
check((await readRiskBook(sa))?.entry_budget?.consumed_usd==="0.5"&&
 (await readRiskBook(sa))?.entry_budget?.reserved_usd==="1.5",
 "all idempotent and denied settlement replays conserve the entry ledger");
const spoof=await settleEntrySpend(sa,held.entry.key,p,{
 connection_generation:77,expected_book_sequence:4,cumulative_entry_usd:"0.5",
 revision:1,source_digest:a});
check(spoof.status==="DENIED"&&spoof.reason==="ENTRY_SETTLEMENT_REVISION_CONFLICT",
 "same revision must also preserve source digest, not just the amount");
const conflicting=await settleEntrySpend(sa,held.entry.key,p,{
 connection_generation:77,expected_book_sequence:4,cumulative_entry_usd:"0.6",
 revision:1,source_digest:d});
check(conflicting.status==="DENIED"&&conflicting.reason==="ENTRY_SETTLEMENT_REVISION_CONFLICT",
 "same revision cannot substitute a different cumulative spend");
const mid=(await readRiskBook(sa))!.entry_budget!;
check(mid.consumed_usd==="0.5"&&mid.reserved_usd==="1.5",
 "partial settlement conserves spent+reserved across Worker restart");

check((await beginRiskPost(sa,held.entry.key,p,1,d,{connection_generation:77,expected_book_sequence:2,census:later,source_proof:native,causal_proof,now_ms:now+3})).status==="DENIED","no duplicate POST fence");
check((await inspectRisk(sa,held.entry.key))?.entry.state==="POSTED","addressed readback");
const terminalCensus={...census,occupied:census.occupied.map(v=>({...v,cumulative_loss_usd:v.account_key===accountKey(request)?"2":"0"})),source_digest:a,generated_at_ms:now+4,source_read_floor_ms:now+4};
const ledgerFinal={account_uid:a,run_id:base.run_id,intent_id:request.intent_id,
 state:"FILLED",record_digest:p,cumulative_loss_usd:"2",remaining_exposure_usdt:"0",
 cumulative_entry_usd:"1.5",entry_settlement_revision:2,entry_settlement_digest:b} as const;
const terminalProof=(ledger:typeof ledgerFinal)=>({
 census:terminalCensus,source_proof:{...native,source_digest:a},ledger,now_ms:now+5
});
// The historical implementation accepted a NEW charge during RELEASE without
// a prior SETTLE_ENTRY. Denial must not advance RiskBook CAS or loss/entry money.
const beforeTerminal=(await readRiskBook(sa))!;
const premature=await transitionRisk(sa,held.entry.key,p,"RELEASED",d,{
 connection_generation:77,expected_book_sequence:4,terminal:terminalProof(ledgerFinal)
});
const afterDenied=(await readRiskBook(sa))!;
check(premature.status==="DENIED"&&premature.reason==="TERMINAL_ENTRY_SETTLEMENT_UNPROVEN"&&
 afterDenied.sequence===beforeTerminal.sequence&&
 afterDenied.consumed_loss_usd===beforeTerminal.consumed_loss_usd&&
 afterDenied.entry_budget?.consumed_usd===beforeTerminal.entry_budget?.consumed_usd&&
 afterDenied.entry_budget?.reserved_usd===beforeTerminal.entry_budget?.reserved_usd,
 "RELEASE cannot invent the final debit or change money/CAS on denial");
const fullyBooked=await settleEntrySpend(sa,held.entry.key,p,{
 connection_generation:77,expected_book_sequence:4,cumulative_entry_usd:"1.5",
 revision:2,source_digest:b
});
check(fullyBooked.status==="UPDATED"&&fullyBooked.book_sequence===5,
 "final entry debit must first cross typed SETTLE_ENTRY exactly once");
const conflictingTerminal=await transitionRisk(sa,held.entry.key,p,"RELEASED",d,{
 connection_generation:77,expected_book_sequence:5,
 terminal:terminalProof({...ledgerFinal,entry_settlement_digest:a})
});
check(conflictingTerminal.status==="DENIED"&&
 (await readRiskBook(sa))!.sequence===5,
 "terminal source digest cannot change without consuming RiskBook CAS");
const committed=await transitionRisk(sa,held.entry.key,p,"RELEASED",d,{
 connection_generation:77,expected_book_sequence:5,terminal:terminalProof(ledgerFinal)
});
check(committed.status==="UPDATED","terminal release uses EXACT previously settled source");
check((await readRiskBook(sa))?.consumed_loss_usd==="2"&&
 (await readRiskBook(sa))?.occupied_usdt==="0","loss survives restart and release");
const finalEntry=(await readRiskBook(sa))!.entry_budget!;
check(finalEntry.consumed_usd==="1.5"&&finalEntry.reserved_usd==="0",
 "terminal release returns unused reserve but NEVER refunds spent entry cost");
const terminalReplay=await settleEntrySpend(sa,held.entry.key,p,{
 connection_generation:77,expected_book_sequence:4,cumulative_entry_usd:"1.5",
 revision:2,source_digest:b
});
check(terminalReplay.status==="UPDATED",
 "lost final SETTLE_ENTRY reply remains idempotently readable after release");
const wrongGenerationReplay=await settleEntrySpend(sa,held.entry.key,p,{
 connection_generation:76,expected_book_sequence:4,cumulative_entry_usd:"1.5",
 revision:2,source_digest:b
});
check(wrongGenerationReplay.status==="DENIED",
 "old authenticated generation cannot replay terminal settlement after recovery");
const tamperedTerminalReplay=await settleEntrySpend(sa,held.entry.key,p,{
 connection_generation:77,expected_book_sequence:4,cumulative_entry_usd:"1.5",
 revision:2,source_digest:a
});
check(tamperedTerminalReplay.status==="DENIED"&&
 tamperedTerminalReplay.reason==="ENTRY_SETTLEMENT_REVISION_CONFLICT",
 "terminal replay may not replace the final financial source digest");
const finalAgain=(await readRiskBook(sa))!.entry_budget!;
check(finalAgain.consumed_usd==="1.5"&&finalAgain.reserved_usd==="0",
 "replayed terminal settlement cannot debit twice or revive a released grant");
const forbiddenTerminalIncrease=await settleEntrySpend(sa,held.entry.key,p,{
 connection_generation:77,expected_book_sequence:(await readRiskBook(sa))!.sequence,
 cumulative_entry_usd:"1.6",revision:3,source_digest:b
});
check(forbiddenTerminalIncrease.status==="DENIED",
 "a new revision cannot recycle the old financial source digest");
// A source-backed fee bill arriving AFTER terminal RELEASE is economic money,
// not a new exchange POST and not a reason to restore the old reservation.
const lateBook=new Storage();
lateBook.map=structuredClone(sa.map);
const releasedSeq=(await readRiskBook(lateBook))!.sequence;
const lateBill=await settleEntrySpend(lateBook,held.entry.key,p,{
 connection_generation:77,expected_book_sequence:releasedSeq,
 cumulative_entry_usd:"1.51",revision:3,source_digest:a
});
check(lateBill.status==="UPDATED"&&lateBill.book_sequence===releasedSeq+1,
 "new post-RELEASE native bill is charged under the original reservation identity");
const lateState=await inspectRisk(lateBook,held.entry.key);
const afterLate=(await readRiskBook(lateBook))!.entry_budget!;
check(lateState?.entry.state==="RELEASED"&&
 afterLate.consumed_usd==="1.51"&&afterLate.reserved_usd==="0",
 "late bill never revives exchange POST, creates loss exposure or steals reserved capacity");
const lateReplay=await settleEntrySpend(lateBook,held.entry.key,p,{
 connection_generation:77,expected_book_sequence:releasedSeq,
 cumulative_entry_usd:"1.51",revision:3,source_digest:a
});
check(lateReplay.status==="UPDATED"&&
 (await readRiskBook(lateBook))!.entry_budget?.consumed_usd==="1.51",
 "post-RELEASE lost ACK exact replay charges zero additional cents");
check((await settleEntrySpend(lateBook,held.entry.key,p,{
 connection_generation:77,expected_book_sequence:releasedSeq,
 cumulative_entry_usd:"1.52",revision:4,source_digest:b
})).status==="DENIED","late fee needs current global CAS for any new debit");
const secondLate=await settleEntrySpend(lateBook,held.entry.key,p,{
 connection_generation:77,expected_book_sequence:releasedSeq+1,
 cumulative_entry_usd:"2.03",revision:4,source_digest:"e".repeat(64)
});
check(secondLate.status==="UPDATED"&&secondLate.reason==="BUDGET_BOUND_BREACH",
 "native bill above initial entry admission is still charged in full");
const overLate=(await readRiskBook(lateBook))!.entry_budget!;
check(overLate.consumed_usd==="2.03"&&overLate.reserved_usd==="0"&&
 overLate.bound_breach_usd==="0.03","late bill over bound is a permanent new-entry STOP");
check((await inspectRisk(lateBook,held.entry.key))?.entry.state==="RELEASED",
 "late settlement does not reopen a resolved reservation");
const lateCensus={...terminalCensus,source_digest:d,
 source_read_floor_ms:now+8,generated_at_ms:now+8};
const lateDenied=await reserveRisk(lateBook,policy,lateCensus,{
 ...request,intent_id:"post_late_bill_new_order",now_ms:now+8,
 loss_usdt:"1",loss_usd:"1",entry_upper_usd:"0.5"
},native);
check(lateDenied.status==="DENIED"&&lateDenied.reason==="BUDGET_BOUND_BREACH",
 "further RESERVE fails closed after late authenticated budget breach");
// Separate accounting-only fixture: other source-verified intents consumed
// 8.49 USD after this reservation was released. Two new late bills must
// count only their INCREMENTS as budget breaches, not add the old breach on
// every revised cumulative source. Zero-delta source replay charges nothing.
const aggregateLate=new Storage();
aggregateLate.map=structuredClone(sa.map);
const ag0=(await readRiskBook(aggregateLate))!;
await aggregateLate.put("risk_meta_v2",{...ag0,entry_budget:{
 ...ag0.entry_budget!,consumed_usd:"9.99",reserved_usd:"0",
 bound_breach_usd:"0"
}});
const aggOne=await settleEntrySpend(aggregateLate,held.entry.key,p,{
 connection_generation:77,expected_book_sequence:ag0.sequence,
 cumulative_entry_usd:"1.62",revision:3,source_digest:a
});
check(aggOne.status==="UPDATED"&&aggOne.reason==="BUDGET_BOUND_BREACH"&&
 (await readRiskBook(aggregateLate))!.entry_budget?.bound_breach_usd==="0.11",
 "first late charge records only newly exceeded campaign budget");
const aggTwo=await settleEntrySpend(aggregateLate,held.entry.key,p,{
 connection_generation:77,expected_book_sequence:ag0.sequence+1,
 cumulative_entry_usd:"1.72",revision:4,source_digest:"e".repeat(64)
});
check(aggTwo.status==="UPDATED"&&aggTwo.reason==="BUDGET_BOUND_BREACH"&&
 (await readRiskBook(aggregateLate))!.entry_budget?.bound_breach_usd==="0.21",
 "second late fee must not count the old 0.11 USD breach again");
const aggReplay=await settleEntrySpend(aggregateLate,held.entry.key,p,{
 connection_generation:77,expected_book_sequence:ag0.sequence+2,
 cumulative_entry_usd:"1.72",revision:5,source_digest:"f".repeat(64)
});
const finalAggregate=(await readRiskBook(aggregateLate))!.entry_budget!;
check(aggReplay.status==="UPDATED"&&
 finalAggregate.consumed_usd==="10.21"&&
 finalAggregate.bound_breach_usd==="0.21"&&
 finalAggregate.reserved_usd==="0",
 "source revision without new monetary debit cannot increment existing breach");
// E2 positive next-intent admission on the SAME actual RiskBook after the
// original POST/partial/final/terminal RELEASE. No campaign reset and no
// test-only grant: the next intent traverses the production reserveRisk CAS.
const successorCensus={...terminalCensus,source_digest:a,
 source_read_floor_ms:now+7,generated_at_ms:now+7};
const successorRequest={...request,intent_id:"successor_after_terminal_001",
 now_ms:now+8,loss_usdt:"1",loss_usd:"1",entry_upper_usd:"0.5"};
const successorProof={...native,source_digest:a};
const successor=await reserveRisk(sa,policy,successorCensus,
 successorRequest,successorProof);
check(successor.status==="HELD",
 "same campaign must admit the second native intent after first RELEASE");
if(successor.status!=="HELD")throw Error("second entry missing");
const successorBudget=(await readRiskBook(sa))!.entry_budget!;
check(successorBudget.consumed_usd==="1.5"&&
 successorBudget.reserved_usd==="0.5"&&
 successorBudget.limit_usd==="10"&&
 (await inspectRisk(sa,held.entry.key))?.entry.state==="RELEASED",
 "second reserve consumes only remaining budget; no refund of first 1.5 USD");
const successorReplay=await reserveRisk(sa,policy,successorCensus,
 successorRequest,successorProof);
if(successorReplay.status!=="HELD")throw Error("second reserve replay missing");
check(successorReplay.entry.key===successor.entry.key&&
 (await readRiskBook(sa))!.entry_budget?.reserved_usd==="0.5",
 "second-intent lost reserve reply cannot allocate another 0.5 USD");
console.log("e2_same_campaign_second_entry_after_terminal PASS");
const reported={...census,occupied:census.occupied.map(v=>({...v,cumulative_loss_usd:v.account_key===accountKey(request)?"17":"0"}))};
const campaign=await reserveRisk(sa,policy,reported,{...request,intent_id:"new_intent_001",loss_usdt:"4",loss_usd:"4"},native);
check(campaign.status==="DENIED"&&campaign.reason==="CAMPAIGN_LOSS_BUDGET_EXHAUSTED","fresh losses charged before reserve even when admission denied");
check((await readRiskBook(sa))?.consumed_loss_usd==="17","campaign losses remain charged without releasing any new reservation");
console.log("risk_concurrent_accounts_campaign_limit PASS");
const unproven=new Storage();
await unproven.put("account_access_v1",governance);await unproven.put("generation",77);
await initializeRiskBook(unproven,"installation_test");
const noImport=await reserveRisk(unproven,policy,census,request,native);
check(noImport.status==="DENIED"&&noImport.reason==="CAMPAIGN_ENTRY_BASELINE_NOT_PROVEN",
 "existing campaign with unknown prior spend must never start at zero");
const absentOpening=new Storage();
await absentOpening.put("account_access_v1",governance);
await initializeRiskBook(absentOpening,"installation_test",{
 installation_id:"installation_test",
 campaign_id:governance.campaign_id,
 started_at_ms:now-100_000
});
const opening=await openingFixture();
const legacy=new Storage();
await legacy.put("account_access_v1",governance);
await initializeRiskBook(legacy,"installation_test");
check(!await installCampaignEntryProof(legacy,opening),
 "old RiskBook without original installation timestamp cannot claim zero baseline");
const untrusted=[
 {...opening,source_digest:d},
 {...opening,accounts:opening.accounts.slice(0,1)},
 {...opening,accounts:[...opening.accounts].reverse()},
 {...opening,accounts:opening.accounts.map((row,i)=>i===0?{...row,fill_history_complete:false as never}:row)},
 {...opening,accounts:opening.accounts.map((row,i)=>i===0?{...row,history_from_ms:row.history_from_ms+1}:row)},
 {...opening,accounts:opening.accounts.map((row,i)=>i===0?{...row,history_through_ms:row.history_through_ms-1}:row)},
 {...opening,accounts:opening.accounts.map((row,i)=>i===0?{...row,source_digest:"not-a-hash"}:row)},
 {...opening,consumed_usd:"1"},
 {...opening,campaign_started_at_ms:opening.as_of_ms+1},
 {...opening,campaign_started_at_ms:opening.campaign_started_at_ms+1},
] as CampaignEntryOpeningSource[];
for(let i=0;i<untrusted.length;i++){
 const accepted=await installCampaignEntryProof(absentOpening,untrusted[i]);
 check(!accepted,"E1 opening unproven history rejected "+i);
 check((await readRiskBook(absentOpening))?.entry_budget===undefined,
  "bad opening must not install any entry budget "+i);
}
const provedOpening=await openingFixture();
provedOpening.accounts[0]={...provedOpening.accounts[0],consumed_entry_usd:"3"};
provedOpening.accounts[1]={...provedOpening.accounts[1],consumed_entry_usd:"2"};
provedOpening.consumed_usd="5";
provedOpening.source_digest=await openingDigest(provedOpening);
check(await installCampaignEntryProof(absentOpening,provedOpening),
 "complete synthetic consumed history is imported without assuming zero");
check((await readRiskBook(absentOpening))?.entry_budget?.consumed_usd==="5",
 "opening spend cannot be overwritten with synthetic zero");
check(!await installCampaignEntryProof(absentOpening,await openingFixture()),
 "campaign opening is immutable after first acceptance");
console.log("entry_opening_all_managed_history_evidence PASS");
// A later real-account enrollment cannot inherit the two-account opening.
// Even if the native census is now complete for all three accounts, the
// original consumed amount covered only the two original account histories.
const enrolledAfterOpening=new Storage();
await enrolledAfterOpening.put("account_access_v1",governance);
await enrolledAfterOpening.put("generation",77);
await setupRiskBook(enrolledAfterOpening);
const third=account(p),thirdKey=third.account_key;
await enrolledAfterOpening.put("account_access_v1",{
 ...governance,accounts:[...governance.accounts,third]
});
const extendedPolicy={...policy,
 account_limits_usdt:{...policy.account_limits_usdt,[thirdKey]:"10"}
};
const extendedCensus={...census,
 account_keys:[...census.account_keys,thirdKey],
 occupied:[...census.occupied,{account_key:thirdKey,
  occupied_usdt:"0",cumulative_loss_usd:"0"}]
};
const laterEntry=await reserveRisk(enrolledAfterOpening,extendedPolicy,
 extendedCensus,request,native);
check(laterEntry.status==="DENIED"&&laterEntry.reason==="CAMPAIGN_OPENING_COHORT_CHANGED",
 "new managed real account cannot inherit an older campaign opening");
check((await readRiskBook(enrolledAfterOpening))?.entry_budget?.reserved_usd==="0",
 "cohort mismatch never allocates missing entry budget");
console.log("entry_opening_new_account_fails_closed PASS");
const entryRace=new Storage();
await entryRace.put("account_access_v1",governance);await entryRace.put("generation",77);
await setupRiskBook(entryRace);
const bounded={...request,loss_usdt:"1",loss_usd:"1",entry_upper_usd:"7"};
const entryResults=await Promise.all([
 reserveRisk(entryRace,policy,census,bounded,native),
 reserveRisk(entryRace,policy,census,{...bounded,intent_id:"intent_competing_entry"},native)
]);
check(entryResults.filter(x=>x.status==="HELD").length===1&&
 entryResults.some(x=>x.status==="DENIED"&&x.reason==="CAMPAIGN_ENTRY_BUDGET_EXHAUSTED"),
 "concurrent distinct orders cannot spend the same campaign entry capacity");
const afterRace=(await readRiskBook(entryRace))!.entry_budget!;
check(afterRace.reserved_usd==="7"&&afterRace.consumed_usd==="0",
 "denied entry race leaves one exact reservation and no phantom consumed money");
// Per-account cap is checked INDEPENDENTLY of campaign spend and loss.
const localCap=new Storage();
await localCap.put("generation",77);
const capPolicy={...governance,accounts:[{...account(a),
 limits:{...account(a).limits,max_entry_usd:"1.5"}},account(b)]};
await localCap.put("account_access_v1",capPolicy);
await setupRiskBook(localCap);
const tooBig=await reserveRisk(localCap,policy,census,request,native);
check(tooBig.status==="DENIED"&&tooBig.reason==="ACCOUNT_ENTRY_LIMIT_EXCEEDED",
 "campaign room must not bypass the account maximum entry expense");
check((await readRiskBook(localCap))?.entry_budget?.reserved_usd==="0",
 "account cap rejection must not consume any entry reservation");
await localCap.put("account_access_v1",{...capPolicy,accounts:[{
 ...capPolicy.accounts[0],limits:{...capPolicy.accounts[0].limits,max_entry_usd:null}
},capPolicy.accounts[1]]});
const noAccountCap=await reserveRisk(localCap,policy,census,request,native);
check(noAccountCap.status==="DENIED"&&noAccountCap.reason==="ACCOUNT_ENTRY_LIMIT_UNPROVEN",
 "legacy missing account cap is not unlimited");
console.log("entry_account_limit_independent_of_campaign PASS");
console.log("entry_budget_race_and_monotonic_settlement PASS");
// A real venue can invoice MORE than the frozen admitted upper. Preserve
// native money truth rather than dropping the debit to satisfy a budget cap.
// The first breach also disables every NEW risk grant, including an old HELD
// intent that has not yet been granted BEGIN_POST authority.
const breachStore=new Storage();
await breachStore.put("account_access_v1",governance);
await breachStore.put("generation",77);
await setupRiskBook(breachStore);
const breachRequest={...base,uid:a,intent_id:"intent_breach_001",loss_usdt:"1",loss_usd:"1"};
const bHeld=await reserveRisk(breachStore,policy,census,breachRequest,native);
if(bHeld.status!=="HELD")throw Error("Breach fixture not HELD");
const bFence=await beginRiskPost(breachStore,bHeld.entry.key,p,1,d,{
  connection_generation:77,expected_book_sequence:bHeld.book_sequence!,
  census:later,source_proof:native,
  causal_proof:{...causal_proof,reserve_anchor:bHeld.entry.key},now_ms:now+2
});
check(bFence.status==="POST_AUTHORIZED","breach fixture authorized once");
const bSettle=await settleEntrySpend(breachStore,bHeld.entry.key,p,{
  connection_generation:77,expected_book_sequence:bFence.book_sequence!,
  cumulative_entry_usd:"2.03",revision:1,source_digest:a
});
check(bSettle.status==="UPDATED"&&bSettle.reason==="BUDGET_BOUND_BREACH",
  "native fee-bound breach must settle full money with explicit incident");
const bMeta=(await readRiskBook(breachStore))!;
check(bMeta.entry_budget?.consumed_usd==="2.03"&&
  bMeta.entry_budget.reserved_usd==="0"&&
  bMeta.entry_budget.bound_breach_usd==="0.03",
  "breach is not truncated and reserve is never negative");
const repeat=await settleEntrySpend(breachStore,bHeld.entry.key,p,{
  connection_generation:77,expected_book_sequence:bFence.book_sequence!,
  cumulative_entry_usd:"2.03",revision:1,source_digest:a
});
check(repeat.status==="UPDATED"&&
  (await readRiskBook(breachStore))?.entry_budget?.consumed_usd==="2.03",
  "lost ACK cannot debit a bound breach twice");
const replayAltered=await settleEntrySpend(breachStore,bHeld.entry.key,p,{
  connection_generation:77,expected_book_sequence:bFence.book_sequence!,
  cumulative_entry_usd:"2.04",revision:1,source_digest:a
});
check(replayAltered.status==="DENIED","same settlement revision cannot add hidden fee");
// A genuine full fee overrun does NOT permanently orphan terminal occupancy.
// RELEASE must use the same independently settled cumulative source and
// retain the irreversible budget-breach STOP after freeing the old position.
const bCensus={...census,source_digest:a,
  generated_at_ms:now+4,source_read_floor_ms:now+4};
const bLedger={
  account_uid:a,run_id:base.run_id,intent_id:breachRequest.intent_id,
  state:"FILLED" as const,record_digest:p,cumulative_loss_usd:"0",
  remaining_exposure_usdt:"0",cumulative_entry_usd:"2.03",
  entry_settlement_revision:1,entry_settlement_digest:a
};
const bReleased=await transitionRisk(breachStore,bHeld.entry.key,p,"RELEASED",d,{
  connection_generation:77,
  expected_book_sequence:(await readRiskBook(breachStore))!.sequence,
  terminal:{
    census:bCensus,source_proof:{...native,source_digest:a},
    ledger:bLedger,now_ms:now+5
  }
});
check(bReleased.status==="UPDATED",
  "source-proven overrun permits terminal cleanup without refunding entry money");
const afterBreachRelease=(await readRiskBook(breachStore))!;
check(afterBreachRelease.occupied_usdt==="0"&&
  afterBreachRelease.entry_budget?.consumed_usd==="2.03"&&
  afterBreachRelease.entry_budget?.reserved_usd==="0"&&
  afterBreachRelease.entry_budget?.bound_breach_usd==="0.03",
  "terminal release frees exposure but retains the entire breach and spent turnover");
const forbidden=await reserveRisk(breachStore,policy,census,{
  ...breachRequest,intent_id:"intent_breach_002"
},native);
check(forbidden.status==="DENIED"&&forbidden.reason==="BUDGET_BOUND_BREACH",
  "after a bound breach, even unused campaign headroom cannot mint new risk");
console.log("entry_budget_native_breach_conservation PASS");
// A fresh reservation can be released only by the exact current native SQL
// writer's durable no-post receipt. No venue, fills or finance source needed.
const unsent=new Storage();
await unsent.put("account_access_v1",governance);
await unsent.put("generation",77);
await setupRiskBook(unsent);
const rescue=await reserveRisk(unsent,policy,census,{...request,intent_id:"intent_unsent_001"},native);
if(rescue.status!=="HELD")throw Error("unsent reserve");
const receipt={reservation_key:rescue.entry.key,account_key:accountKey(request),
  intent_id:"intent_unsent_001",plan_hash:p,request_hash:"e".repeat(64),
  reason:"POST_CENSUS_UNPROVEN",record_digest:"f".repeat(64)};
check((await releaseUnsentRisk(unsent,rescue.entry.key,p,1,76,receipt)).status==="DENIED",
  "stale authenticated generation cannot release");
check((await releaseUnsentRisk(unsent,rescue.entry.key,p,1,77,
  {...receipt,plan_hash:d})).status==="DENIED","wrong plan cannot release");
check((await releaseUnsentRisk(unsent,rescue.entry.key,p,2,77,receipt)).status==="UPDATED",
  "exact no-send SQL proof releases HELD");
check((await releaseUnsentRisk(unsent,rescue.entry.key,p,2,77,receipt)).status==="DENIED",
  "central release tombstone prevents repeated budget mutation");
check((await readRiskBook(unsent))?.occupied_usd==="0" &&
 (await readRiskBook(unsent))?.consumed_loss_usd==="0",
 "release only reserved capacity and never credits realized losses");
console.log("risk_unsent_cas_release PASS");
// F4: central BEGIN_POST may have committed before the native SQL claim.
// A durable native REJECTED_UNSENT transaction still gates place_order.
const postUnsent=new Storage();
await postUnsent.put("account_access_v1",governance);
await postUnsent.put("generation",77);
await setupRiskBook(postUnsent);
const postRes=await reserveRisk(postUnsent,policy,census,{...request,intent_id:"intent_f4_unsent"},native);
if(postRes.status!=="HELD")throw Error("F4 reserve");
const f4read={...census,generated_at_ms:now+1,source_read_floor_ms:now+1};
const f4proof={reserve_anchor:postRes.entry.key,native_incarnation:"a".repeat(32),
  elapsed_to_proof_ms:22,account_reads:census.account_keys.map(account_key=>({
    account_key,private_generation:77,source_read_id:"e".repeat(64),
    content_digest:d,started_after_held_ms:2,completed_after_held_ms:14}))};
const f4=await beginRiskPost(postUnsent,postRes.entry.key,p,1,d,{
  connection_generation:77,expected_book_sequence:2,census:f4read,
  source_proof:native,causal_proof:f4proof,now_ms:now+2});
check(f4.status==="POST_AUTHORIZED","F4 central post authorized");
const f4receipt={...receipt,reservation_key:postRes.entry.key,intent_id:"intent_f4_unsent"};
check((await releaseUnsentRisk(postUnsent,postRes.entry.key,p,3,77,f4receipt)).status==="UPDATED",
 "F4 central POSTED + native never-claimed release is safe");
check((await beginRiskPost(postUnsent,postRes.entry.key,p,1,d,{
 connection_generation:77,expected_book_sequence:3,census:f4read,source_proof:native,
 causal_proof:f4proof,now_ms:now+3})).status==="DENIED","old post fence cannot resurrect tombstone");
console.log("risk_f4_unsent_after_central_post PASS");
// F2: a lost RESERVE never arrived, but its native SQL attempt did. The
// Worker fences the exact address so that a delayed RESERVE cannot mint risk.
const neverArrived=new Storage();
await neverArrived.put("account_access_v1",governance);
await neverArrived.put("generation",77);
await setupRiskBook(neverArrived);
const lateIntent="intent_f2_late_001";
const lateRequest={...request,intent_id:lateIntent};
const lateKey=[base.venue,"real",a,base.run_id,lateIntent]
  .map(v=>v.length+":"+v).join("");
const lateProof={...receipt,reservation_key:lateKey,intent_id:lateIntent};
check((await releaseUnsentRisk(neverArrived,lateKey,p,0,77,
  {...lateProof,account_key:"okx:real:"+b})).status==="DENIED",
  "wrong native account cannot abort another risk identity");
check((await releaseUnsentRisk(neverArrived,lateKey,p,1,77,lateProof)).status==="UPDATED",
  "SQL no-send fences an absent reservation with initial sequence zero");
const lateAttempt=await reserveRisk(neverArrived,policy,census,lateRequest,native);
check(lateAttempt.status==="DENIED"&&lateAttempt.reason==="RESERVE_FENCED_NO_SEND",
  "a delayed exchange-independent RESERVE never mints risk after the no-send fence");
const absentBook=await readRiskBook(neverArrived);
check(absentBook?.occupied_usdt==="0"&&absentBook?.consumed_loss_usd==="0"&&
  absentBook?.sequence===2,"negative fence only advances book CAS, never consumes risk");
check((await releaseUnsentRisk(neverArrived,lateKey,p,2,77,lateProof)).status==="UPDATED",
  "repeated no-send fence is idempotent under new CAS sequence");
check((await readRiskBook(neverArrived))?.sequence===2,
  "idempotent absent release does not advance central book");
check((await releaseUnsentRisk(neverArrived,lateKey,p,1,77,
  {...lateProof,record_digest:d})).status==="DENIED",
  "conflicting terminal proof cannot replace an immutable absence fence");
// F2 concurrent delivery in both serializations: whichever Worker transaction
// wins, retry the exact same SQL no-send proof and never mint/re-mint risk.
for(const abortFirst of [true,false]){
  const racing=new Storage();
  await racing.put("account_access_v1",governance);
  await racing.put("generation",77);
  await setupRiskBook(racing);
  const id=abortFirst?"intent_f2_abort_first":"intent_f2_reserve_first";
  const requestRace={...request,intent_id:id};
  const key=[base.venue,"real",a,base.run_id,id].map(v=>v.length+":"+v).join("");
  const sqlProof={...receipt,reservation_key:key,intent_id:id};
  const release=()=>releaseUnsentRisk(racing,key,p,1,77,sqlProof);
  const reserve=()=>reserveRisk(racing,policy,census,requestRace,native);
  const first=abortFirst?await Promise.all([release(),reserve()]):await Promise.all([reserve(),release()]);
  const returnedRelease=abortFirst?first[0]:first[1];
  if(returnedRelease.status==="DENIED"){
    const current=await readRiskBook(racing);
    check(!!current,"racing Worker book present");
    check((await releaseUnsentRisk(racing,key,p,current!.sequence,77,sqlProof)).status==="UPDATED",
      "stale CAS retries same durable no-send proof after competing RESERVE");
  }
  const final=await readRiskBook(racing);
  check(final?.occupied_usdt==="0"&&final?.consumed_loss_usd==="0",
    "no-send race returns all unspent risk without crediting consumed loss");
  const late=await reserveRisk(racing,policy,census,requestRace,native);
  check(late.status==="DENIED","no second grant after no-send race");
}
console.log("risk_f2_concurrent_abort_reserve PASS");
console.log("risk_f2_absent_reserve_fence PASS");

census.occupied[0].cumulative_loss_usd="17";
// One million terminal records must not be enumerated or decoded on the hot path.
for(let i=0;i<1_000_000;i++)sa.map.set("risk_entry_v2:tombstone_"+i,{state:"RELEASED",key:i});
let reads=0;const original=sa.get.bind(sa);sa.get=async(k:string)=>{reads++;return original(k);};
const started=performance.now();check((await reserve()).status==="DENIED","posted retry cannot authorize");
check(reads<=7,"million history bounded point reads");
console.log(`million_history_hot_read PASS records=1000000 reads=${reads} elapsed_ms=${(performance.now()-started).toFixed(3)}`);
