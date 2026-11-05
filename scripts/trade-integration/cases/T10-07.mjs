// T10-07 is ONE actual Native/Worker campaign. No Worker-only reservations,
// copied B1 receipts, independent T09 cohorts or direct SQL money inserts.
import assert from "node:assert/strict";
import {writeFile,mkdir} from "node:fs/promises";
import {resolve} from "node:path";
import {actualT10_07NativeWorker,hash} from "../native-workerd.mjs";

export async function run({caseId,root,signal,artifactsDir}){
  assert.equal(caseId,"T10-07");
  const t=await actualT10_07NativeWorker(root,signal);
  const n=t.native,w=t.worker;
  assert.equal(n.schema,"okx.t10-07-native-single-campaign/v1");
  assert.equal(w.schema,"okx.t10-07-worker-single-campaign/v1");
  assert.equal(t.cohort_execution.reused,false);
  assert.equal(w.same_native_process_and_campaign,true);
  assert.equal(n.two_real_sql_owners,true);
  assert.equal(new Set(n.account_keys).size,2);
  assert.equal(n.native_execute_count,3);
  assert.equal(n.a_sql_state,"ACKNOWLEDGED");
  assert.equal(n.b_sql_state,"REJECTED");
  assert.ok(n.a_sql_revision>0&&n.b_sql_revision>0);
  assert.equal(n.b_release_receipt_verified,true);
  assert.equal(n.b_sql_active_central_receipt,false);
  assert.equal(n.third_denial,"CAMPAIGN_ENTRY_BUDGET_EXHAUSTED");
  assert.equal(n.third_extra_create_posts,0);
  assert.equal(n.posts_after-n.posts_before,1);
  const before=n.worker_before_release.entry_budget;
  const after=n.worker_after_release.entry_budget;
  assert.deepEqual(
    [before.consumed_usd,before.reserved_usd,before.remaining_usd],
    ["0","8","2"]);
  assert.deepEqual(
    [after.consumed_usd,after.reserved_usd,after.remaining_usd],
    ["0","4","6"]);
  assert.equal(w.budget.entry_budget.reserved_usd,"4");
  assert.equal(w.budget.entry_budget.remaining_usd,"6");
  assert.equal(w.risk_book_sequence,n.worker_sequence);
  assert.ok(n.worker_before_release_sequence<n.worker_sequence);
  assert.ok(n.source_events.some(e=>e.includes(
    "event=SIGNED_PHASE_ADVANCED")&&e.includes("t10-07-b-fee-changed")));
  const events=[
    ["two_verified_sql_owners","NativeAccountSQL",n.account_keys],
    ["first_real_execute_ack","TradeService",n.a_intent_id],
    ["both_held_same_worker","RuntimeSession/SQLiteDO",n.worker_before_release],
    ["third_real_execute_denied","TradeService",n.third_denial],
    ["authenticated_source_changed","RecordedAccountHttp",n.source_events],
    ["original_b_held_released","NativeAccountSQL+RiskBook",n.b_intent_id],
    ["worker_persisted_readback","RuntimeSession/SQLiteDO",w.budget],
  ].map(([stage,owner,value])=>({stage,owner,source_digest:hash(value)}));
  const evidence={
    case_id:caseId,status:"PASS",
    provenance:{schema:"okx.native-worker-single-campaign/v1",
      sha256:t.transcript_sha256},
    assertions:[
      {id:"native_three_execute",expected:"3",actual:String(n.native_execute_count)},
      {id:"two_held_before_release",expected:"8",actual:before.reserved_usd},
      {id:"remaining_before_release",expected:"2",actual:before.remaining_usd},
      {id:"third_central_denial_no_post",expected:"0",
        actual:String(n.third_extra_create_posts)},
      {id:"one_source_backed_b_release",expected:"4",actual:after.reserved_usd},
      {id:"remaining_after_release",expected:"6",actual:after.remaining_usd},
      {id:"only_one_actual_a_create",expected:"1",
        actual:String(n.posts_after-n.posts_before)},
    ],
    events,
    readbacks:{
      sql:[
        {revision:n.a_sql_revision,digest:hash(n.a_intent_id)},
        {revision:n.b_sql_revision,digest:hash(n.b_intent_id)},
      ],
      risk_book:[
        {revision:n.worker_before_release_sequence,digest:hash(n.worker_before_release)},
        {revision:w.risk_book_sequence,digest:hash(w.budget)},
      ],
    },
    outbound_sends:[
      {account:n.account_keys[0],intent:n.a_intent_id,operation_kind:"create",count:1},
    ],
  };
  if(artifactsDir){
    const dir=resolve(artifactsDir,"T10-07");
    await mkdir(dir,{recursive:true});
    await writeFile(resolve(dir,"single-campaign.json"),
      JSON.stringify({provenance:evidence.provenance,assertions:evidence.assertions,
        events:evidence.events,readbacks:evidence.readbacks},null,2)+"\n");
  }
  return evidence;
}
