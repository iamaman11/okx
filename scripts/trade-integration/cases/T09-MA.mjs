// T09-MA: two independent Native account SQL owners and two registered
// strategy instances; A Status and B's positive Execute start at one barrier.
// Same instrument and one persisted Worker campaign; foreign mutations denied.
import assert from "node:assert/strict";
import { actualNativeWorker, hash } from "../native-workerd.mjs";
export async function run({caseId,root,signal}) {
  process.env.OKX_B1_NATIVE_CAP_PROBE="1";
  process.env.OKX_B1_SECOND_POSITIVE_PROBE="1";
  process.env.OKX_T09_MA_CONCURRENT_PROBE="1";
  const t=await actualNativeWorker(root,signal,{
    statusProbe:true,secondAdmission:true
  });
  const a=t.nativeSecondAdmission, b=t.nativeB1Positive;
  const denied=t.nativeB1Cap, cross=t.nativeCrossUidDenial;
  const strategy=t.nativeStrategyAccountPreflight;
  const both=t.nativeMAConcurrent, worker=t.workerSecondAdmission;
  assert.ok(a&&b&&denied&&cross&&strategy&&both&&worker,
    "T09-MA requires authentic concurrent Native SQL/TradeService/Worker evidence");
  assert.equal(both.schema,"okx.t09-ma-public-native-concurrency/v1");
  assert.equal(both.native_public_barrier_participants,2);
  assert.equal(both.same_instrument,"BTC-USDT-SWAP");
  assert.equal(both.a_intent_id,a.intent_id);
  assert.equal(both.b_intent_id,b.b_intent_id);
  assert.notEqual(both.a_intent_id,both.b_intent_id);
  assert.equal(both.a_account_uid,a.account_uid);
  assert.equal(both.b_account_uid,b.account_b_uid);
  assert.notEqual(both.a_account_uid,both.b_account_uid);
  assert.equal(both.a_sql_state,"ACKNOWLEDGED");
  assert.equal(both.b_sql_state,"ACKNOWLEDGED");
  assert.equal(both.outbound_posts_after,5);
  assert.equal(denied.denial_origin,"CENTRAL");
  assert.equal(denied.denial_reason,"CAMPAIGN_ENTRY_BUDGET_EXHAUSTED");
  assert.equal(denied.outbound_posts_before,denied.outbound_posts_after);
  assert.equal(both.worker_shared_cap_usd,"10");
  assert.equal(both.worker_reserved_usd,"8.002");
  assert.equal(both.worker_consumed_usd,"1.0005");
  assert.equal(worker.budget.entry_budget.reserved_usd,"8.002");
  assert.equal(worker.budget.entry_budget.consumed_usd,"1.0005");
  assert.equal(b.after_budget.entry_budget.remaining_usd,"0.9975");
  assert.ok(Number.isSafeInteger(both.worker_sequence)&&both.worker_sequence>0);
  assert.equal(cross.status_reason,"INTENT_NOT_FOUND");
  assert.equal(cross.cancel_reason,"INTENT_NOT_FOUND");
  assert.equal(cross.foreign_sql_intent_absent,true);
  assert.equal(cross.foreign_sql_idempotency_absent,true);
  assert.equal(cross.outbound_posts_before,cross.outbound_posts_after);
  assert.equal(strategy.a_strategy_registered,true);
  assert.equal(strategy.b_strategy_registered,true);
  assert.equal(strategy.a_other_strategy_absent,true);
  assert.equal(strategy.b_other_strategy_absent,true);
  assert.equal(strategy.a_strategy_enabled,false);
  assert.equal(strategy.b_strategy_enabled,false);
  assert.equal(strategy.native_outbound_posts,4);
  const events=[
    ["public_trade_entered","TradeService",[both.a_intent_id,both.b_intent_id]],
    ["a_and_b_sql_distinct","NativeAccountSQL",[strategy,both]],
    ["concurrent_public_owner_routing","NativeTradeService",[both.native_public_barrier_participants,both.a_intent_id,both.b_intent_id]],
    ["shared_risk_book_verified","RuntimeSession/SQLiteDO",[both.worker_sequence,worker.budget]],
    ["cross_account_mutation_denied","NativeTradeService",[cross.status_reason,cross.cancel_reason,cross.outbound_posts_after]]
  ].map(([stage,owner,e])=>({stage,owner,source_digest:hash(e)}));
  return {
    case_id:caseId,status:"PASS",
    provenance:{schema:"okx.native-workerd-transcript/v1",sha256:t.transcript_sha256},
    assertions:[
      {id:"verified_distinct_sql_account_uids",expected:"2",
        actual:String(new Set([both.a_account_uid,both.b_account_uid]).size)},
      {id:"concurrent_public_actor_count",expected:"2",
        actual:String(both.native_public_barrier_participants)},
      {id:"same_instrument",expected:"BTC-USDT-SWAP",actual:both.same_instrument},
      {id:"shared_campaign_cap",expected:"10",actual:both.worker_shared_cap_usd},
      {id:"shared_held_usd",expected:"8.002",actual:both.worker_reserved_usd},
      {id:"shared_consumed_usd",expected:"1.0005",actual:both.worker_consumed_usd},
      {id:"wrong_account_cancel_denied",expected:"INTENT_NOT_FOUND",actual:cross.cancel_reason},
      {id:"central_over_cap_denied",expected:"CAMPAIGN_ENTRY_BUDGET_EXHAUSTED",
        actual:denied.denial_reason},
      {id:"total_venue_posts",expected:"5",actual:String(both.outbound_posts_after)}
    ],events,
    readbacks:{
      sql:[{revision:a.sql_revision,digest:hash(a)},
        {revision:strategy.native_outbound_posts,digest:hash(strategy)}],
      risk_book:[{revision:both.worker_sequence,digest:hash(worker.budget)}],
    },
    outbound_sends:[
      {account:both.a_account_uid,intent:both.a_intent_id,operation_kind:"create",count:1},
      {account:both.b_account_uid,intent:both.b_intent_id,operation_kind:"create",count:1},
    ],
  };
}
