// T10-14: actual A SQL held while B's public Status and own venue Cancel
// complete on independent SQLite and shared persisted Worker RiskBook.
import assert from "node:assert/strict";
import {actualNativeWorker,hash} from "../native-workerd.mjs";

export async function run({caseId,root,signal}) {
  process.env.OKX_B1_NATIVE_CAP_PROBE="1";
  process.env.OKX_B1_SECOND_POSITIVE_PROBE="1";
  process.env.OKX_T10_14_NATIVE_PROBE="1";
  const t=await actualNativeWorker(root,signal,{statusProbe:true,secondAdmission:true});
  const v=t.nativeT10Isolation, a=t.nativeSecondAdmission;
  const b=t.nativeB1Positive, w=t.workerSecondAdmission;
  assert.ok(v&&a&&b&&w,"actual Native/Worker/A/B witnesses required");
  assert.equal(v.schema,"okx.t10-14-native-worker-isolation/v1");
  assert.equal(v.account_a,"okx:real:"+a.account_uid);
  assert.equal(v.account_b,"okx:real:"+b.account_b_uid);
  assert.notEqual(v.account_a,v.account_b);
  assert.equal(v.b_intent_id,b.b_intent_id);
  assert.equal(v.b_status_intent_id,v.b_intent_id);
  assert.equal(v.a_sql_held_during_b_cancel,true);
  assert.equal(v.b_status_while_a_held,true);
  assert.equal(v.a_sql_rejoined,true);
  assert.equal(v.b_cancel_ack_without_terminal_release,true);
  assert.ok(["partial","unknown"].includes(v.b_cancel_status));
  assert.equal(v.b_sql_state,"ACKNOWLEDGED");
  assert.equal(v.b_risk_held,true);
  assert.equal(v.outbound_posts_before,5);
  assert.equal(v.read_posts,5);
  assert.equal(v.outbound_posts_after,6);
  assert.equal(v.new_b_cancel_posts,1);
  assert.ok(Number.isSafeInteger(v.b_sql_revision)&&v.b_sql_revision>0);
  assert.ok(Number.isSafeInteger(v.worker_sequence)&&v.worker_sequence>0);
  assert.equal(v.worker_budget.entry_budget.consumed_usd,"1.0005");
  assert.equal(v.worker_budget.entry_budget.reserved_usd,"8.002");
  assert.equal(v.worker_budget.entry_budget.remaining_usd,"0.9975");
  assert.equal(w.budget.entry_budget.reserved_usd,"8.002");
  const proofs=[
    ["a_sql_held_during_b_cancel","true",String(v.a_sql_held_during_b_cancel)],
    ["b_public_status_while_a_held","true",String(v.b_status_while_a_held)],
    ["b_intent_identity",v.b_intent_id,v.b_status_intent_id],
    ["one_b_cancel_post","1",String(v.new_b_cancel_posts)],
    ["b_risk_held_without_terminal","true",String(v.b_risk_held)],
    ["shared_reserved_usd","8.002",v.worker_budget.entry_budget.reserved_usd],
    ["shared_consumed_usd","1.0005",v.worker_budget.entry_budget.consumed_usd],
    ["a_rejoined","true",String(v.a_sql_rejoined)]
  ].map(([id,expected,actual])=>({id,expected,actual}));
  const stages=[
    ["public_trade_entered","TradeService+NativeSQL",[a.intent_id,b.b_intent_id]],
    ["a_sql_owner_held","NativeAccountSQL-A",[v.account_a,true]],
    ["b_public_status_read","TradeService+NativeAccountSQL-B",[v.b_intent_id,v.b_status_intent_id]],
    ["b_exact_venue_cancel_observed","RecordedAuthenticatedHTTP-B",[v.new_b_cancel_posts,v.outbound_posts_after]],
    ["a_sql_owner_rejoined","NativeAccountSQL-A",[v.account_a,true]],
    ["worker_budget_after_cancel_verified","RuntimeSession/SQLiteDO",v.worker_budget]
  ].map(([stage,owner,e])=>({stage,owner,source_digest:hash(e)}));
  return {
    case_id:caseId,status:"PASS",
    provenance:{schema:"okx.native-workerd-transcript/v1",sha256:t.transcript_sha256},
    assertions:proofs,events:stages,
    readbacks:{
      sql:[{revision:v.b_sql_revision,digest:hash([v.b_sql_state,v.b_intent_id])}],
      risk_book:[{revision:v.worker_sequence,digest:hash(v.worker_budget)}],
    },
    outbound_sends:[
      {account:v.account_b,intent:v.b_intent_id,operation_kind:"create",count:1},
      {account:v.account_b,intent:v.b_intent_id,operation_kind:"cancel",count:1},
    ],
    cohort_execution:t.cohort_execution,
  };
}
