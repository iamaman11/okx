// T10-01: no venue mutation from an incomplete authenticated balance,
// successful admission only after fresh real signed source, plus an independent
// post-HELD fee mutation cohort that must also fail closed.
import assert from "node:assert/strict";
import {actualNativeWorker,hash} from "../native-workerd.mjs";
export async function run({caseId,root,signal}){
  process.env.OKX_T10_01_PARTIAL_SOURCE_RECOVERY_PROBE="1";
  delete process.env.OKX_T10_15_POST_HELD_SOURCE_PROBE;
  const recovered=await actualNativeWorker(root,signal,{
    statusProbe:true,secondAdmission:true
  });
  delete process.env.OKX_T10_01_PARTIAL_SOURCE_RECOVERY_PROBE;
  process.env.OKX_T10_15_POST_HELD_SOURCE_PROBE="1";
  const postHeld=await actualNativeWorker(root,signal,{
    statusProbe:true,secondAdmission:true
  });
  const n=recovered.nativeT10SourceRecovery,w=recovered.workerT10SourceRecovery;
  const p=postHeld.nativeT10SourceChanged,q=postHeld.workerT10SourceChanged;
  assert.ok(n&&w&&p&&q,"two independent real Native/Worker cohorts required");
  assert.equal(n.schema,"okx.t10-01-native-private-source-recovery/v1");
  assert.equal(w.schema,"okx.t10-01-worker-source-recovery/v1");
  assert.equal(n.posts_before,4);
  assert.equal(n.posts_on_incomplete,4);
  assert.equal(n.posts_after_recovery,5);
  assert.equal(n.fresh_sql_state,"ACKNOWLEDGED");
  assert.equal(n.fresh_central_reserve,true);
  assert.equal(n.fresh_order_id,"recorded-t10-01-b-001");
  assert.match(n.partial_source_digest,/^[0-9a-f]{64}$/);
  assert.match(n.fresh_source_digest,/^[0-9a-f]{64}$/);
  assert.notEqual(n.partial_source_digest,n.fresh_source_digest);
  assert.ok(n.fresh_sql_revision>0);
  assert.equal(n.worker_budget_before.entry_budget.reserved_usd,"7.0015");
  assert.equal(n.worker_budget_after.entry_budget.reserved_usd,"8.002");
  assert.equal(w.budget.entry_budget.reserved_usd,"8.002");
  assert.equal(p.worker_held_witness,true);
  assert.equal(p.posts_before,4);
  assert.equal(p.posts_after,4);
  assert.match(p.first_denial,/POST_ENTRY_FEE_OR_SPEC_CHANGED|POST_ENTRY_FEE_SOURCE_CHANGED/);
  assert.equal(p.old_quote_usd,"1.0005");
  assert.equal(p.new_quote_usd,"1.0006");
  assert.equal(q.posts_for_changed_plan,0);
  const events=[
    ["public_trade_entered","TradeService",n.incomplete_denial],
    ["stale_source_before_admission_denied","RecordedAuthenticatedHTTP",n.partial_source_digest],
    ["stale_source_after_held_denied","RuntimeSession/SQLiteDO",p.signed_changed_fee_digest],
    ["source_denials_no_venue_send","RecordedAuthenticatedHTTP",
      [n.posts_on_incomplete,p.posts_after]],
    ["fresh_source_recovery_verified","NativeAccountSQL+RuntimeSession/SQLiteDO",
      [n.fresh_sql_state,n.worker_budget_after]],
  ].map(([stage,owner,source])=>({stage,owner,source_digest:hash(source)}));
  return {
    case_id:caseId,status:"PASS",
    provenance:{schema:"okx.native-workerd-two-cohorts/v1",
      hashes:[recovered.transcript_sha256,postHeld.transcript_sha256]},
    assertions:[
      {id:"incomplete_zero_posts",expected:"0",
        actual:String(n.posts_on_incomplete-n.posts_before)},
      {id:"fresh_recovery_one_post",expected:"1",
        actual:String(n.posts_after_recovery-n.posts_on_incomplete)},
      {id:"post_held_changed_zero_posts",expected:"0",
        actual:String(p.posts_after-p.posts_before)},
      {id:"fresh_sql_recovered",expected:"ACKNOWLEDGED",actual:n.fresh_sql_state},
      {id:"old_signed_fee_quote",expected:"1.0005",actual:p.old_quote_usd},
      {id:"new_signed_fee_quote",expected:"1.0006",actual:p.new_quote_usd},
      {id:"real_worker_one_held_recovery",expected:"8.002",
        actual:w.budget.entry_budget.reserved_usd},
    ],
    events,
    readbacks:{
      sql:[
        {revision:n.fresh_sql_revision,digest:hash(n.fresh_intent_id)},
        {revision:p.sql_revision,digest:hash(p.intent_id)},
      ],
      risk_book:[
        {revision:w.risk_book_sequence,digest:hash(w.budget)},
        {revision:q.risk_book_sequence,digest:hash(q.budget)},
      ],
    },
    outbound_sends:[{
      account:n.account_uid,intent:n.fresh_intent_id,operation_kind:"create",count:1
    }],
  };
}
