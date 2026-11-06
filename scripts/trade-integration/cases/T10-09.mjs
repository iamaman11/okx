// T10-09: authenticated public Cancel ACK is NOT terminal evidence.
// This first leg runs the production A/B SQLite owners + persisted Worker.
// Late fill, finance ingestion, partial close and remaining protection must
// happen AFTER this ACK on the same B intent before mandatory PASS.
import assert from "node:assert/strict";
import { mkdir,writeFile } from "node:fs/promises";
import { resolve } from "node:path";
import { actualNativeWorker, hash } from "../native-workerd.mjs";

export async function run({caseId,root,signal,artifactsDir}) {
  assert.equal(caseId,"T10-09");
  assert.ok(artifactsDir,"T10-09 requires retained causal evidence");
  process.env.OKX_B1_NATIVE_CAP_PROBE="1";
  process.env.OKX_B1_SECOND_POSITIVE_PROBE="1";
  process.env.OKX_T10_14_NATIVE_PROBE="1";
  process.env.OKX_T10_09_LATE_FILL_PROBE="1";
  const t=await actualNativeWorker(root,signal,{
    statusProbe:true,secondAdmission:true,
  });
  const cancel=t.nativeT10Isolation, b=t.nativeB1Positive;
  const late=t.nativeT10LateFill, partialReduce=t.nativeT10PartialReduce;
  const reduceFinance=t.nativeT10ReduceFinance;
  const a=t.nativeSecondAdmission, worker=t.workerSecondAdmission;
  assert.ok(cancel&&b&&a&&worker&&late&&partialReduce&&reduceFinance,
    "Native A/B Worker, signed late fill and real public partial Reduce ACK required");
  assert.equal(cancel.schema,"okx.t10-14-native-worker-isolation/v1");
  assert.equal(cancel.b_intent_id,b.b_intent_id);
  assert.equal(cancel.b_status_intent_id,b.b_intent_id);
  assert.equal(cancel.account_a,"okx:real:"+a.account_uid);
  assert.equal(cancel.account_b,"okx:real:"+b.account_b_uid);
  assert.notEqual(cancel.account_a,cancel.account_b);
  assert.equal(cancel.b_cancel_ack_without_terminal_release,true);
  assert.equal(cancel.b_sql_state,"ACKNOWLEDGED");
  assert.equal(cancel.b_risk_held,true);
  assert.equal(cancel.new_b_cancel_posts,1);
  assert.equal(cancel.outbound_posts_before,5);
  assert.equal(cancel.outbound_posts_after,6);
  assert.equal(cancel.worker_budget.entry_budget.reserved_usd,"8.002");
  assert.equal(cancel.worker_budget.entry_budget.consumed_usd,"1.0005");
  // The Worker may settle an authentic portion of B's entry spend after the
  // late signed fill, so only the pre-fill Cancel checkpoint is fixed at 8.002.
  assert.equal(late.schema,"okx.t10-09-late-signed-b-fill/v1");
  assert.equal(late.same_b_intent,b.b_intent_id);
  assert.equal(late.account_uid,b.account_b_uid);
  assert.equal(late.after_cancel_ack,true);
  assert.equal(late.signed_fill_and_bill_ingested_by_native_sql,true);
  assert.equal(late.sql.quantity,"0.04");
  assert.equal(late.sql.fill_count,1);
  assert.equal(late.sql.bill_count,1);
  assert.equal(late.sql.verified_fee_usdt,"0.0002");
  assert.equal(late.sql.active_central_receipt,true);
  // Independent economics: fill0.04 × multiplier0.01 × price1000
  // plus signed fee0.0002 transfers 0.4002 from held to consumed.
  assert.equal(late.worker_budget.entry_budget.consumed_usd,"1.4007");
  assert.equal(late.worker_budget.entry_budget.reserved_usd,"7.6018");
  assert.equal(late.worker_budget.entry_budget.remaining_usd,"0.9975");
  assert.equal(late.posts_before,6);
  assert.equal(late.posts_after,6);
  assert.equal(partialReduce.schema,"okx.t10-09-source-bound-partial-reduce/v1");
  assert.equal(partialReduce.original_intent_id,b.b_intent_id);
  assert.equal(partialReduce.reduce.quantity,"0.02");
  assert.equal(partialReduce.reduce.operation,"reduce");
  assert.equal(partialReduce.reduce.risk_increase,false);
  assert.equal(partialReduce.reduce.exchange_order_id,"recorded-t10-09-b-reduce-020");
  assert.equal(partialReduce.outbound_posts_before,6);
  assert.equal(partialReduce.outbound_posts_after,7);
  assert.equal(partialReduce.close_fill_and_remnant_protection_pending,true);
  assert.equal(reduceFinance.schema,"okx.t10-09-signed-partial-reduce-finance/v1");
  assert.equal(reduceFinance.reduce_intent_id,partialReduce.reduce.intent_id);
  assert.equal(reduceFinance.sql.remaining_lot_qty,"0.02");
  assert.equal(reduceFinance.sql.remaining_lot_count,1);
  assert.equal(reduceFinance.linked_fee_signed_usdt,"-0.0001");
  assert.equal(reduceFinance.linked_fill_count,1);
  assert.match(reduceFinance.source_digest,/^[a-f0-9]{64}$/);
  assert.equal(reduceFinance.reduce_status,"FILLED");
  assert.notEqual(reduceFinance.central_release_state,"verified");
  assert.equal(reduceFinance.outbound_posts,7);
  assert.notEqual(partialReduce.original_central_release.state,"verified");
  assert.ok(Number.isSafeInteger(late.sql.sql_revision)&&late.sql.sql_revision>0);
  assert.match(late.sql.bill_digest,/^[a-f0-9]{64}$/);
  assert.deepEqual(late.worker_budget,worker.budget,
    "final Worker readback must match Native post-cancel finance");
  assert.notEqual(worker.budget.entry_budget.reserved_usd,"0",
    "late fill cannot release B exposure while a position remains");
  assert.ok(Number.isSafeInteger(cancel.b_sql_revision));
  assert.ok(Number.isSafeInteger(cancel.worker_sequence));
  assert.match(t.transcript_sha256,/^[0-9a-f]{64}$/);
  const dir=resolve(artifactsDir,caseId);
  await mkdir(dir,{recursive:true});
  await writeFile(resolve(dir,"cancel-ack-no-release.json"),
    JSON.stringify({
      schema:"okx.t10-09-cancel-first-causal-leg/v1",
      mandatory_case_pass:false,
      native_b_intent_id:b.b_intent_id,
      owner_b:cancel.account_b,
      cancel_ack_source:"RecordedAuthenticatedHTTP",
      production_status_and_cancel:true,
      same_sql_owner:true,
      retained_worker_entry_budget:cancel.worker_budget.entry_budget,
      worker_revision_before_late_fill:cancel.worker_sequence,
      worker_revision_after_late_fill:late.worker_sequence,
      sql_revision:cancel.b_sql_revision,
      extra_cancel_posts:cancel.new_b_cancel_posts,
      transcript_sha256:t.transcript_sha256,
      witness_digest:hash([cancel,b.b_intent_id,late.sql]),
      signed_late_fill_sql:late.sql,
      signed_public_partial_reduce_ack:partialReduce,
      signed_partial_reduce_finance:reduceFinance,
      worker_after_late_fill:late.worker_budget,
      public_status_after_late_fill:late.public_status_state,
      missing:["remaining_position_live_protection_after_ack_bound_amend",
        "terminal_source_backed_risk_release"],
    },null,2)+"\n");
  // The late fill and bill are genuine B owner facts, but a partial close,
  // remaining protection and final terminal release are still mandatory.
  throw Error("T10_09_PARTIAL_CLOSE_PROTECTION_AND_TERMINAL_NOT_PROVEN");
}
