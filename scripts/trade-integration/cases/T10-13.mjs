// T10-13: persisted Worker STOP after a real filled position must reject new
// claims, preserve the existing risk and permit the actual production Close.
import assert from "node:assert/strict";
import {actualNativeWorker,hash} from "../native-workerd.mjs";
export async function run({caseId,root,signal}){
  process.env.OKX_T10_13_STOP_PROBE="1";
  const t=await actualNativeWorker(root,signal,{statusProbe:true});
  const n=t.nativeT10Stop,w=t.workerT10Stop;
  const terminal=t.nativeTerminal;
  const close=t.nativeClose;
  assert.ok(n&&w&&terminal&&close,
    "T10-13 requires STOP and real source-linked native Close witnesses");
  assert.equal(n.schema,"okx.t10-13-native-stop-after-fill/v1");
  assert.equal(w.schema,"okx.t10-13-worker-stop/v1");
  assert.equal(n.new_claim_denial,"ACCOUNT_NEW_RISK_PAUSED");
  assert.equal(n.stop_governance_revision,w.stop_revision);
  assert.equal(n.native_sql_policy_matches,true);
  assert.equal(w.explicit_pause,true);
  assert.equal(w.persisted_worker_durable_object,true);
  assert.equal(w.stop_revision,w.before_revision+1);
  assert.ok(n.source_fault_reads>0);
  assert.notEqual(n.stale_reduce_blocker,"ACCOUNT_NEW_RISK_PAUSED");
  assert.match(n.source_recovered_digest,/^[0-9a-f]{64}$/);
  assert.equal(n.posts_before,1);
  assert.equal(n.posts_after_denied_reduce,1);
  assert.equal(close.created_posts,2);
  assert.equal(terminal.created_posts,3);
  assert.equal(terminal.close_status.data.central_release.state,"verified");
  assert.equal(terminal.opening_status.data.central_release.state,"verified");
  assert.equal(terminal.worker_budget.reserved_loss_usd,"0");
  const events=[
    ["stop_before_claim_ack","RuntimeSession/SQLiteDO",w.stop_revision],
    ["post_stop_new_claims_denied","TradeService+NativeAccountSQL",n.new_claim_denial],
    ["stop_after_fill_applied","NativeAccountSQL",[n.intent_id,n.worker_budget_before_close]],
    ["safe_reduce_or_cancel_attempted","TradeService+RecordedAuthenticatedHTTP",close],
    ["stale_reduction_blocker_verified","NativeAccountSQL",
      [n.stale_reduce_blocker,n.source_fault_reads]],
  ].map(([stage,owner,source])=>({stage,owner,source_digest:hash(source)}));
  return {
    case_id:caseId,status:"PASS",
    provenance:{schema:"okx.native-workerd-transcript/v1",sha256:t.transcript_sha256},
    assertions:[
      {id:"no_claim_after_stop",expected:"ACCOUNT_NEW_RISK_PAUSED",
        actual:n.new_claim_denial},
      {id:"policy_cas_persisted",expected:"true",
        actual:String(w.persisted_worker_durable_object)},
      {id:"no_bad_reduce_post",expected:"1",
        actual:String(n.posts_after_denied_reduce)},
      {id:"real_close_post_follows_fresh_source",expected:"2",
        actual:String(close.created_posts)},
      {id:"all_active_risk_released_only_after_close",expected:"0",
        actual:terminal.worker_budget.reserved_loss_usd},
    ],events,
    readbacks:{
      sql:[{revision:w.stop_revision,digest:hash([n.intent_id,n.new_claim_denial])}],
      risk_book:[{revision:terminal.worker_risk_book_sequence,
        digest:hash(terminal.worker_budget)}],
    },
    outbound_sends:[
      {account:n.account_uid,intent:n.intent_id,operation_kind:"create",count:1},
      {account:n.account_uid,intent:terminal.close_intent_id,
        operation_kind:"create",count:1},
      {account:n.account_uid,intent:terminal.close_intent_id,
        operation_kind:"cancel",count:1}
    ],
  };
}
