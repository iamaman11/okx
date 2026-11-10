// T10-04: real persisted Worker RESERVE completed, but one native WSS
// delivery has a foreign command ID and MUST NOT authorize a venue POST.
import assert from "node:assert/strict";
import { actualNativeWorker, hash } from "../native-workerd.mjs";

export async function run({caseId,root,signal}){
  process.env.OKX_T10_04_FOREIGN_WORKER_REPLY_PROBE="1";
  const t=await actualNativeWorker(root,signal,{
    statusProbe:true,secondAdmission:true
  });
  const n=t.nativeT10ForeignReply,w=t.workerT10ForeignReply;
  assert.ok(n&&w&&t.nativeSecondAdmission,
    "T10-04 requires actual public Native+Worker SQL-backed receipts");
  assert.equal(n.schema,"okx.t10-04-native-worker-binding/v1");
  assert.equal(w.schema,"okx.t10-04-worker-post-commit-binding/v1");
  assert.equal(n.fault_injected,true);
  assert.ok(n.sql_revision>0);
  assert.notEqual(n.sql_state,"ACKNOWLEDGED");
  assert.notEqual(n.sql_state,"POSTING");
  assert.equal(n.sql_has_central_receipt,false);
  assert.equal(n.sql_exchange_order_id,null);
  assert.equal(n.posts_before,4);
  assert.equal(n.posts_after,4);
  assert.equal(w.posts_for_denied_command,0);
  assert.equal(w.transport_error_expected,"RISK_RESULT_COMMAND_UNBOUND");
  assert.equal(n.worker_budget_before.entry_budget.reserved_usd,"7.0015");
  assert.equal(w.committed_worker_budget.entry_budget.reserved_usd,"8.002");
  assert.equal(w.committed_worker_budget.entry_budget.consumed_usd,"1.0005");
  assert.ok(w.committed_worker_sequence>=n.worker_sequence_before);
  const stages=[
    ["public_trade_entered","TradeService",n.sql_intent_id],
    ["foreign_worker_reply_injected","AuthenticatedNativeWSS",w.transport_error_expected],
    ["binding_error_observed","NativeRiskClient",n.denial_reason],
    ["sql_did_not_authorize_post","NativeAccountSQL",[n.sql_state,n.sql_revision]],
    ["worker_held_not_fabricated_release","RuntimeSession/SQLiteDO",w.committed_worker_budget],
    ["zero_venue_create_verified","RecordedAuthenticatedHTTP",n.posts_after],
  ].map(([stage,owner,source])=>({stage,owner,source_digest:hash(source)}));
  return {
    case_id:caseId,status:"PASS",
    provenance:{schema:"okx.native-workerd-transcript/v1",sha256:t.transcript_sha256},
    assertions:[
      {id:"foreign_command_no_create",expected:"0",
        actual:String(n.posts_after-n.posts_before)},
      {id:"worker_reserve_still_held",expected:"8.002",
        actual:w.committed_worker_budget.entry_budget.reserved_usd},
      {id:"worker_consumed_not_faked",expected:"1.0005",
        actual:w.committed_worker_budget.entry_budget.consumed_usd},
      {id:"native_central_receipt_not_forged",expected:"false",
        actual:String(n.sql_has_central_receipt)},
    ],
    events:stages,
    readbacks:{
      sql:[{revision:n.sql_revision,digest:hash([n.sql_state,n.sql_intent_id])}],
      risk_book:[{revision:w.committed_worker_sequence,
        digest:hash(w.committed_worker_budget)}],
    },
    outbound_sends:[],
  };
}
