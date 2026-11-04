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
  const t=await actualNativeWorker(root,signal,{
    statusProbe:true,secondAdmission:true,
  });
  const cancel=t.nativeT10Isolation, b=t.nativeB1Positive;
  const a=t.nativeSecondAdmission, worker=t.workerSecondAdmission;
  assert.ok(cancel&&b&&a&&worker,"real Native A/B/Worker owners required");
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
  assert.equal(worker.budget.entry_budget.reserved_usd,"8.002");
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
      worker_revision:cancel.worker_sequence,
      sql_revision:cancel.b_sql_revision,
      extra_cancel_posts:cancel.new_b_cancel_posts,
      transcript_sha256:t.transcript_sha256,
      witness_digest:hash([cancel,b.b_intent_id]),
      missing:["late_signed_fill_after_cancel_ack_on_same_b_intent",
        "native_sql_finance_ingestion_after_late_fill",
        "public_reduce_only_partial_close_without_flip",
        "remaining_position_live_protection",
        "terminal_source_backed_risk_release"],
    },null,2)+"\n");
  // No terminal risk release, protection or reduction can be inferred from
  // Cancel ACK. A T09.3 fill from another account/cohort is NOT late B fill.
  throw Error("T10_09_SAME_B_CANCEL_LATE_FILL_CLOSE_PROTECTION_NOT_PROVEN");
}
