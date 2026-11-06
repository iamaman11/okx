// T10-08: first causal leg uses real signed partial/terminal fills and
// source-bound protection from the existing production TradeService path.
// A deliberate protection failure and late fill MUST occur on this SAME
// native SQL intent. This file does not impersonate the emergency exit owner.
import assert from "node:assert/strict";
import { mkdir,writeFile } from "node:fs/promises";
import { resolve } from "node:path";
import { actualNativeWorker,hash } from "../native-workerd.mjs";

export async function run({caseId,root,signal,artifactsDir}) {
  assert.equal(caseId,"T10-08");
  assert.ok(artifactsDir,"T10-08 requires retained signed-source evidence");
  const t=await actualNativeWorker(root,signal,{
    statusProbe:true,secondAdmission:true,partialFills:true,
    protectionFailure:true,
  });
  const witness=t.nativePartialFills, settlement=t.nativeTerminalSettlement;
  const failed=t.nativeT10ProtectionFailure;
  assert.ok(witness&&settlement&&failed,
    "actual same-intent Native stop failure, late fill and Worker readback required");
  assert.equal(failed.schema,"okx.t10-08-native-protection-failure/v1");
  assert.equal(failed.intent_id,witness.intent_id,
    "failed stop must be owned by the partial/late fill intent");
  assert.equal(failed.first_partial_sql.intent_id,witness.intent_id);
  assert.equal(failed.first_partial_sql.state,"PARTIALLY_FILLED");
  assert.equal(failed.first_partial_sql.fill_quantity,"0.12");
  assert.equal(failed.first_partial_sql.bill_count,1);
  assert.equal(failed.first_partial_sql.central_held,true);
  assert.equal(failed.signed_stop_failure.algo_id,"900000002");
  assert.equal(failed.signed_stop_failure.state,"canceled");
  assert.equal(failed.signed_stop_failure.fail_code,"51008");
  assert.equal(failed.protected_action.state,"blocked");
  assert.equal(failed.protected_action.retry_create,false);
  assert.equal(failed.protected_action.phase,"before_financial_capture");
  assert.equal(failed.protected_action.reason_code,
    "SAFETY_CANCEL_OUTCOME_UNKNOWN_NO_RESUBMIT");
  assert.ok(failed.protected_action.elapsed_ms<=2000,
    "ambiguous protective cancel outcome must be blocked within 2 seconds");
  assert.equal(failed.sql_after.intent_id,witness.intent_id);
  assert.equal(failed.sql_after.risk_stop_latched,true);
  assert.equal(failed.sql_after.central_held,true);
  assert.equal(failed.sql_after.cancel_claim.kind,"safety_cancel");
  assert.notEqual(failed.worker_budget.entry_budget.reserved_usd,"0");
  assert.equal(failed.outbound_posts_before,4);
  assert.equal(failed.outbound_posts_after,5);
  assert.equal(failed.outbound.length,1);
  assert.equal(failed.outbound[0].path,"/api/v5/trade/cancel-order");
  assert.equal(failed.late_fill_after_this_checkpoint,true);
  assert.equal(witness.phases.length,2);
  const [partial,terminal]=witness.phases;
  assert.equal(partial.phase,"partial");
  assert.equal(terminal.phase,"terminal");
  assert.equal(partial.sql.quantity,"0.12");
  assert.equal(terminal.sql.quantity,"0.3");
  assert.equal(partial.sql.fill_count,1);
  assert.equal(terminal.sql.fill_count,2);
  assert.equal(partial.sql.bill_count,1);
  assert.equal(terminal.sql.bill_count,2);
  assert.equal(partial.intent_state,"PARTIALLY_FILLED");
  assert.equal(terminal.intent_state,"FILLED");
  assert.equal(partial.protection.trigger_basis,"mark");
  assert.equal(terminal.protection.trigger_basis,"mark");
  assert.ok(Number(partial.protection.quantity)>=0.12);
  assert.ok(Number(terminal.protection.quantity)>=0.3);
  assert.match(partial.sql.source_digest,/^[a-f0-9]{64}$/);
  assert.match(terminal.sql.source_digest,/^[a-f0-9]{64}$/);
  assert.equal(witness.outbound_posts,5,
    "the sole additional POST is the failed protective Cancel, never a resend");
  assert.equal(settlement.outbound_posts,5);
  assert.equal(settlement.reserved_usd,"4");
  assert.equal(settlement.consumed_usd,"4.002");
  const dir=resolve(artifactsDir,caseId);
  await mkdir(dir,{recursive:true});
  await writeFile(resolve(dir,"signed-partial-fill-protection.json"),
    JSON.stringify({
      schema:"okx.t10-08-protection-failure-late-fill-prerequisite/v2",
      mandatory_case_pass:false,
      intent_id:witness.intent_id,
      native_sql_fill_revisions:[partial.sql,terminal.sql],
      signed_failed_stop_before_late_fill:failed.signed_stop_failure,
      native_emergency_attempt:failed.protected_action,
      native_protection_sql_claim:failed.sql_after.cancel_claim,
      worker_at_stop_failure:{
        sequence:failed.worker_risk_book_sequence,
        budget:failed.worker_budget,
      },
      request_trace_at_failure:failed.outbound,
      timestamp_to_emergency_ms:failed.protected_action.elapsed_ms,
      protection:[partial.protection,terminal.protection],
      worker_sequence:witness.risk_book_sequence,
      worker_entry_budget:witness.worker_budget.entry_budget,
      venue_create_posts:witness.outbound_posts,
      source_digest:hash([partial.sql,terminal.sql]),
      transcript_sha256:t.transcript_sha256,
      missing:[
        "independent_post_failure_confirmation_of_remaining_position_protection",
        "same_incident_recovery_readback_after_late_fill_on_original_sql_owner",
        "no_risk_reentry_until_terminal_unprotected_exposure_is_resolved"
      ],
    },null,2)+"\n");
  // The Native stop failure, bounded rejected Cancel and same-intent late
  // signed fill are now evidenced. The terminal phase still publishes a live
  // stop without a proven recovery mutation; do not count that source as
  // authenticated protective restoration or permit a new-risk admission.
  throw Error("T10_08_PROTECTION_RESTORATION_AND_INCIDENT_RECOVERY_NOT_PROVEN");
}
