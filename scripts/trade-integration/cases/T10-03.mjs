// T10-03: actual Worker POST_AUTHORIZED committed, answer never delivered
// to Native, independent INSPECT sees original POSTED reserve, not RELEASED.
import assert from "node:assert/strict";
import {actualNativeWorker,hash} from "../native-workerd.mjs";

export async function run({caseId,root,signal}){
  process.env.OKX_T10_03_POST_AUTH_REPLY_LOST_PROBE="1";
  const t=await actualNativeWorker(root,signal,{
    statusProbe:true,secondAdmission:true
  });
  const n=t.nativeT10PostReplyLost,w=t.workerT10PostReplyLost;
  assert.ok(n&&w&&t.nativeSecondAdmission,
    "T10-03 needs real Native/Worker persisted post-fence witnesses");
  assert.equal(n.schema,"okx.t10-03-native-post-auth-lost/v1");
  assert.equal(w.schema,"okx.t10-03-post-auth-lost-worker/v1");
  assert.equal(n.lost_post_authorization,true);
  assert.equal(n.central_state,"POSTED");
  assert.equal(w.central_state,"POSTED");
  assert.equal(n.active_sql_receipt,true);
  assert.equal(n.replay_intent_id,n.intent_id);
  assert.ok(n.sql_revision>0);
  assert.notEqual(n.sql_state,"ACKNOWLEDGED");
  assert.notEqual(n.sql_state,"POSTING");
  assert.equal(n.sql_exchange_order_id,null);
  assert.equal(n.posts_before,4);
  assert.equal(n.posts_after,4);
  assert.equal(w.new_exchange_posts,0);
  assert.equal(n.worker_budget.entry_budget.consumed_usd,"1.0005");
  assert.equal(n.worker_budget.entry_budget.reserved_usd,"8.002");
  assert.equal(w.after_budget.entry_budget.reserved_usd,"8.002");
  assert.ok(w.after_sequence>=n.worker_sequence);
  assert.ok(n.central_sequence>0);
  const events=[
    ["public_trade_entered","TradeService",n.intent_id],
    ["worker_reply_lost_after_durable_commit","RuntimeSession/SQLiteDO",
      [n.reservation_key,n.central_state]],
    ["same_reservation_inspected","NativeRiskClient",n.central_sequence],
    ["no_duplicate_grant_or_send","RecordedAuthenticatedHTTP",n.posts_after],
    ["pending_risk_reconciled","NativeAccountSQL+RuntimeSession/SQLiteDO",
      [n.sql_state,w.after_budget]],
  ].map(([stage,owner,source])=>({stage,owner,source_digest:hash(source)}));
  return {
    case_id:caseId,status:"PASS",
    provenance:{schema:"okx.native-workerd-transcript/v1",sha256:t.transcript_sha256},
    assertions:[
      {id:"worker_committed_posted",expected:"POSTED",actual:n.central_state},
      {id:"duplicate_executions_prohibited",expected:"0",
        actual:String(n.posts_after-n.posts_before)},
      {id:"pending_central_money_preserved",expected:"8.002",
        actual:w.after_budget.entry_budget.reserved_usd},
      {id:"same_intent_replayed",expected:n.intent_id,actual:n.replay_intent_id},
      {id:"no_invented_venue_ack",expected:"null",
        actual:String(n.sql_exchange_order_id)},
    ],
    events,
    readbacks:{
      sql:[{revision:n.sql_revision,digest:hash([n.intent_id,n.sql_state])}],
      risk_book:[{revision:w.after_sequence,digest:hash(w.after_budget)}],
    },
    outbound_sends:[],
  };
}
