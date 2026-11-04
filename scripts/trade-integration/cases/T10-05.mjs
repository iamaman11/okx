// T10-05: the same production Native/Worker stack distinguishes post-send
// lost ACK from an explicit signed venue rejection in two isolated cohorts.
import assert from "node:assert/strict";
import {actualNativeWorker,hash} from "../native-workerd.mjs";

export async function run({caseId,root,signal}){
  process.env.OKX_T10_05_LOST_ACK_PROBE="1";
  delete process.env.OKX_T10_05_EXPLICIT_REJECT_PROBE;
  const lost=await actualNativeWorker(root,signal,{
    statusProbe:true,secondAdmission:true
  });
  delete process.env.OKX_T10_05_LOST_ACK_PROBE;
  process.env.OKX_T10_05_EXPLICIT_REJECT_PROBE="1";
  const rejected=await actualNativeWorker(root,signal,{
    statusProbe:true,secondAdmission:true
  });
  const a=lost.nativeT10PostOutcome,b=rejected.nativeT10PostOutcome;
  const wa=lost.workerT10PostOutcome,wb=rejected.workerT10PostOutcome;
  assert.ok(a&&b&&wa&&wb,
    "T10-05 needs real Native/Worker receipts for both distinct outcomes");
  assert.equal(a.schema,"okx.t10-05-native-post-outcome/v1");
  assert.equal(b.schema,a.schema);
  assert.equal(a.mode,"lost_ack");
  assert.equal(b.mode,"venue_rejected");
  assert.equal(a.initial_state==="POSTING"||a.initial_state==="UNKNOWN",true);
  assert.equal(a.final_state,"ACKNOWLEDGED");
  assert.equal(a.venue_order_id,"recorded-t10-05-b-001");
  assert.equal(b.initial_state,"REJECTED");
  assert.equal(b.final_state,"REJECTED");
  assert.equal(b.venue_order_id,null);
  assert.notEqual(a.intent_id,b.intent_id);
  for(const witness of [a,b]){
    assert.equal(witness.replay_intent_id,witness.intent_id);
    assert.equal(witness.posts_before,4);
    assert.equal(witness.posts_after,5);
    assert.equal(witness.outbound_posts_after_replay,5);
    assert.ok(witness.sql_revision>0);
    assert.match(witness.client_order_id,/^.+$/);
    assert.equal(witness.worker_budget_after.entry_budget.consumed_usd,"1.0005");
  }
  for(const worker of [wa,wb]){
    assert.equal(worker.schema,"okx.t10-05-worker-post-outcome/v1");
    assert.equal(worker.no_extra_posts,true);
    assert.equal(worker.budget.entry_budget.consumed_usd,"1.0005");
    assert.equal(worker.budget.entry_budget.reserved_usd,"8.002");
  }
  const ev=[
    ["public_trade_entered","TradeService",[a.intent_id,b.intent_id]],
    ["venue_send_observed","RecordedAuthenticatedHTTP",[a.posts_after,b.posts_after]],
    ["venue_ack_lost_or_unbound","NativeAccountSQL",a.initial_state],
    ["unknown_without_resubmit","TradeService",[a.replay_intent_id,a.outbound_posts_after_replay]],
    ["same_client_order_readback_verified","NativeAccountSQL",[a.final_state,a.venue_order_id]],
    ["explicit_venue_reject_distinguished","NativeAccountSQL",[b.final_state,b.venue_order_id]],
  ].map(([stage,owner,source])=>({stage,owner,source_digest:hash(source)}));
  return {
    case_id:caseId,status:"PASS",
    assertions:[
      {id:"lost_ack_same_intent",expected:a.intent_id,actual:a.replay_intent_id},
      {id:"lost_ack_readback_resolved",expected:"ACKNOWLEDGED",actual:a.final_state},
      {id:"venue_reject_remains_rejected",expected:"REJECTED",actual:b.final_state},
      {id:"lost_ack_no_repost",expected:"5",actual:String(a.outbound_posts_after_replay)},
      {id:"rejected_no_repost",expected:"5",actual:String(b.outbound_posts_after_replay)},
      {id:"no_invented_worker_debit",expected:"1.0005",
        actual:wa.budget.entry_budget.consumed_usd},
    ],events:ev,
    provenance:{schema:"okx.native-workerd-two-cohorts/v1",
      hashes:[lost.transcript_sha256,rejected.transcript_sha256]},
    readbacks:{
      sql:[
        {revision:a.sql_revision,digest:hash([a.intent_id,a.final_state])},
        {revision:b.sql_revision,digest:hash([b.intent_id,b.final_state])}
      ],
      risk_book:[
        {revision:wa.risk_book_sequence,digest:hash(wa.budget)},
        {revision:wb.risk_book_sequence,digest:hash(wb.budget)}
      ],
    },
    outbound_sends:[
      {account:a.account_uid,intent:a.intent_id,operation_kind:"create",count:1},
      {account:b.account_uid,intent:b.intent_id,operation_kind:"create",count:1},
    ]
  };
}
