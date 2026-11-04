// T10-15 real Worker HELD is a causal barrier: a new signed venue fee
// invalidates the prior preview/plan, so zero venue create is permitted.
import assert from "node:assert/strict";
import {actualNativeWorker,hash} from "../native-workerd.mjs";
export async function run({caseId,root,signal}){
  process.env.OKX_T10_15_POST_HELD_SOURCE_PROBE="1";
  const t=await actualNativeWorker(root,signal,{
    statusProbe:true,secondAdmission:true
  });
  const n=t.nativeT10SourceChanged,w=t.workerT10SourceChanged;
  assert.ok(n&&w&&t.nativeSecondAdmission,
    "T10-15 requires authenticated Native and persisted Worker witnesses");
  assert.equal(n.schema,"okx.t10-15-post-held-source-change/v1");
  assert.equal(w.schema,"okx.t10-15-worker-source-change/v1");
  assert.equal(n.worker_held_witness,true);
  assert.equal(n.old_quote_usd,"1.0005");
  assert.equal(n.new_quote_usd,"1.0006");
  assert.match(n.signed_changed_fee_digest,/^[0-9a-f]{64}$/);
  assert.match(n.first_denial,/POST_ENTRY_FEE_OR_SPEC_CHANGED|POST_ENTRY_FEE_SOURCE_CHANGED/);
  assert.ok(n.sql_revision>0);
  assert.notEqual(n.sql_state,"ACKNOWLEDGED");
  assert.notEqual(n.sql_state,"POSTING");
  assert.equal(n.posts_before,4);
  assert.equal(n.posts_after,4);
  assert.equal(w.posts_for_changed_plan,0);
  assert.equal(w.budget.entry_budget.consumed_usd,"1.0005");
  assert.ok(["7.0015","8.002"].includes(w.budget.entry_budget.reserved_usd));
  const events=[
    ["preview_source_snapshot","TradeService",n.old_quote_usd],
    ["held_source_revision_changed","RuntimeSession/SQLiteDO",
      [n.worker_held_witness,n.signed_changed_fee_digest]],
    ["post_held_proof_rejected","NativeAccountSQL",n.first_denial],
    ["fee_change_prevents_stale_post","RecordedAuthenticatedHTTP",n.posts_after],
    ["fresh_plan_revalidated","TradeService",n.new_quote_usd],
  ].map(([stage,owner,source])=>({stage,owner,source_digest:hash(source)}));
  return {
    case_id:caseId,status:"PASS",
    provenance:{schema:"okx.native-workerd-transcript/v1",sha256:t.transcript_sha256},
    assertions:[
      {id:"old_immutable_preview",expected:"1.0005",actual:n.old_quote_usd},
      {id:"fresh_venue_fee_quote",expected:"1.0006",actual:n.new_quote_usd},
      {id:"stale_plan_zero_venue_posts",expected:"0",
        actual:String(n.posts_after-n.posts_before)},
      {id:"unchanged_consumed_entry_money",expected:"1.0005",
        actual:w.budget.entry_budget.consumed_usd},
    ],
    events,
    readbacks:{
      sql:[{revision:n.sql_revision,digest:hash([n.sql_state,n.intent_id])}],
      risk_book:[{revision:w.risk_book_sequence,digest:hash(w.budget)}],
    },
    outbound_sends:[],
  };
}
