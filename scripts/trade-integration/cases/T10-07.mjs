// T10-07: one independently launched production cohort, NOT concatenated
// Worker-only $4 receipts and separate Native T09/B1/T10-15 evidence.
// Native account A/B own distinct persisted SQL files; all monetary facts
// come from the same production RuntimeSession / SQLite Durable Object.
import assert from "node:assert/strict";
import {mkdir,writeFile} from "node:fs/promises";
import {resolve} from "node:path";
import {actualNativeWorker} from "../native-workerd.mjs";

export async function run({caseId,root,signal,artifactsDir}) {
  assert.equal(caseId,"T10-07");
  assert.ok(artifactsDir,"T10-07 must retain real source-bound receipts");
  process.env.OKX_T10_07_ONE_CAMPAIGN="1";
  delete process.env.OKX_T10_07_NATIVE_PROBE;
  delete process.env.OKX_T10_15_POST_HELD_SOURCE_PROBE;
  delete process.env.OKX_B1_NATIVE_CAP_PROBE;
  delete process.env.OKX_B1_SECOND_POSITIVE_PROBE;
  let witness;
  try {
    witness=await actualNativeWorker(root,signal);
  } finally {
    delete process.env.OKX_T10_07_ONE_CAMPAIGN;
  }
  assert.equal(witness.cohort_execution.reused,false,
    "mandatory T10-07 evidence must be one NEW real Native/Worker cohort");
  const n=witness.nativeT10OneCampaign,w=witness.workerT10OneCampaign;
  assert.ok(n&&w,"the SAME child must emit both Native and Worker receipts");
  assert.equal(n.schema,"okx.t10-07-native-one-campaign/v1");
  assert.equal(w.schema,"okx.t10-07-worker-one-campaign/v1");
  const sql=witness.nativeBootstrap;
  assert.equal(sql.accounts.length,2);
  assert.equal(sql.sqlite_files.length,2);
  assert.notEqual(sql.sqlite_files[0],sql.sqlite_files[1]);
  assert.notEqual(n.a_account_uid,n.b_account_uid);
  assert.equal(n.a_sql_state,"ACKNOWLEDGED");
  assert.equal(n.b_sql_state,"REJECTED");
  assert.equal(n.third_sql_state,"REJECTED");
  assert.deepEqual(n.held_receipts,["HELD","HELD","DENIED"]);
  assert.equal(n.denial_reason,"CAMPAIGN_ENTRY_BUDGET_EXHAUSTED");
  assert.equal(n.denied_extra_posts,0);
  assert.equal(n.release_extra_posts,0);
  assert.equal(n.total_venue_posts,1,
    "exactly the original A authenticated ACK may reach venue in this campaign");
  assert.equal(n.sql_release_verified,true);
  assert.equal(n.worker_release_verified,true);
  assert.ok(n.original_b_held_source_reads>0);
  assert.match(n.signed_changed_fee_digest,/^[a-f0-9]{64}$/);
  assert.match(n.b_release_plan_sha256,/^[a-f0-9]{64}$/);
  assert.ok(n.b_release_reservation_key);
  assert.notEqual(n.a_intent_id,n.b_intent_id);
  const first=n.worker_before_release.entry_budget;
  const after=n.worker_after_release.entry_budget;
  const persisted=w.budget.entry_budget;
  assert.deepEqual([first.limit_usd,first.reserved_usd,
    first.remaining_usd,first.consumed_usd],["10","8","2","0"]);
  assert.deepEqual([after.limit_usd,after.reserved_usd,
    after.remaining_usd,after.consumed_usd],["10","4","6","0"]);
  assert.deepEqual([persisted.limit_usd,persisted.reserved_usd,
    persisted.remaining_usd,persisted.consumed_usd],["10","4","6","0"]);
  assert.ok(w.risk_book_sequence>=n.worker_after_sequence);
  assert.equal(w.persisted_generation_advanced,true);
  assert.equal(w.release_from_original_held,true);
  assert.equal(w.two_native_sql_owners,true);
  assert.equal(w.denied_extra_posts,0);
  assert.equal(w.release_extra_posts,0);
  assert.match(witness.transcript_sha256,/^[a-f0-9]{64}$/);
  const directory=resolve(artifactsDir,"T10-07");
  await mkdir(directory,{recursive:true});
  const proof={
    schema:"okx.t10-07-same-campaign-acceptance/v1",
    status:"PASS",single_campaign_native_sql_worker:true,
    transcript_sha256:witness.transcript_sha256,
    native_sql_owners:sql,
    native:n,worker:w,
  };
  await writeFile(resolve(directory,"native-worker-one-campaign.json"),
    JSON.stringify(proof,null,2)+"\n");
  return {
    schema:proof.schema,
    same_campaign:true,three_native_executes:true,
    statuses:n.held_receipts,
    before_reserved_usd:first.reserved_usd,
    after_reserved_usd:after.reserved_usd,
    after_remaining_usd:after.remaining_usd,
    new_venue_posts_during_denial_and_release:0,
    worker_risk_book_sequence:w.risk_book_sequence,
    cohort_transcript_sha256:witness.transcript_sha256,
  };
}
