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
  const failed=t.nativeT10ProtectionFailure, recovery=t.nativeT10Recovery;
  assert.ok(witness&&settlement&&failed&&recovery,
    "actual same-intent Native failed stop, signed late fill, Close and Worker release required");
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
  assert.equal(terminal.protection,null,
    "a reused live algoId after the failed stop would fabricate restored protection");
  assert.equal(terminal.public_status_deferred_until_signed_close_sources,true,
    "the terminal fill must not trigger a Close before its own signed source is prepared");
  assert.ok(Number(partial.protection.quantity)>=0.12);
  assert.match(partial.sql.source_digest,/^[a-f0-9]{64}$/);
  assert.match(terminal.sql.source_digest,/^[a-f0-9]{64}$/);
  assert.equal(recovery.schema,"okx.t10-08-native-flat-recovery/v1");
  assert.equal(recovery.original_intent_id,witness.intent_id);
  assert.equal(recovery.native_sql.original_intent_id,witness.intent_id);
  assert.equal(recovery.native_sql.original_state,"FILLED");
  assert.equal(recovery.native_sql.original_incident,null);
  assert.equal(recovery.native_sql.risk_stop_latched,false);
  assert.equal(recovery.native_sql.central_still_held,false);
  assert.equal(recovery.native_sql.remaining_instrument_lots,0);
  assert.equal(recovery.native_sql.close_risk_increase,false);
  assert.equal(recovery.native_sql.close_quantity,"0.3");
  assert.equal(recovery.native_sql.recovery_receipt_kind,
    "PROTECTION_INCIDENT_FLAT_RESOLVED");
  assert.equal(recovery.close.source_intent_id,witness.intent_id);
  assert.equal(recovery.close.venue_order_id,"recorded-t10-08-close-030");
  assert.equal(recovery.close.quantity,"0.3");
  assert.equal(recovery.close.close_intent_id,recovery.native_sql.close_intent_id);
  assert.ok(recovery.signed_close_event_at_ms>=recovery.signed_late_event_at_ms,
    "a signed closing fill must occur after the signed late fill");
  assert.equal(recovery.close_terminal_status.data.intent.state,"FILLED");
  assert.equal(recovery.close_terminal_status.data.central_release.state,"verified");
  assert.equal(recovery.original_terminal_status.data.intent.state,"FILLED");
  assert.equal(recovery.original_terminal_status.data.central_release.state,"verified");
  assert.equal(recovery.original_terminal_status.data.protection_incident,false);
  assert.equal(recovery.original_terminal_status.data.protection_remediation.state,
    "verified_flat_no_orphans");
  assert.match(recovery.original_terminal_status.data.protection_remediation.source_digest,
    /^[a-f0-9]{64}$/);
  assert.notEqual(recovery.worker_before.entry_budget.reserved_usd,"0",
    "unprotected exposure must retain central risk before the signed Close");
  assert.equal(recovery.worker_after.entry_budget.reserved_usd,"0");
  assert.equal(recovery.worker_after.entry_budget.consumed_usd,"4.002");
  assert.ok(recovery.worker_sequence_after>recovery.worker_sequence_before);
  assert.equal(recovery.outbound_posts_before,5);
  assert.equal(recovery.outbound_posts_after,6);
  assert.equal(recovery.no_venue_post_replayed,true);
  assert.equal(witness.outbound_posts,6,
    "the sixth POST must be exactly the signed reduce-only Close, not an entry retry");
  assert.equal(settlement.outbound_posts,6);
  assert.equal(settlement.reserved_usd,"0");
  assert.equal(settlement.consumed_usd,"4.002");
  const dir=resolve(artifactsDir,caseId);
  await mkdir(dir,{recursive:true});
  await writeFile(resolve(dir,"signed-partial-fill-protection.json"),
    JSON.stringify({
      schema:"okx.t10-08-protection-failure-late-fill-recovery/v3",
      mandatory_case_pass:true,
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
      native_close_and_flat_recovery:recovery,
      post_failure_stop_remained_unverified:true,
      signed_flat_after_close:true,
      timestamp_to_emergency_ms:failed.protected_action.elapsed_ms,
      protection_before_incident:partial.protection,
      proof_of_new_live_stop_after_failure:false,
      worker_sequence:witness.risk_book_sequence,
      worker_entry_budget:witness.worker_budget.entry_budget,
      venue_mutations_total:witness.outbound_posts,
      source_digest:hash([partial.sql,terminal.sql]),
      transcript_sha256:t.transcript_sha256,
      acceptance:{
        initial_stop_failure_latched:true,
        unknown_cancel_not_resubmitted:true,
        late_fill_and_bill_captured_once:true,
        source_bound_reduce_only_close:true,
        signed_flat_and_orphan_free:true,
        original_central_release_verified:true,
        durable_sql_incident_resolved:true,
        no_risk_reentry_before_resolution:true
      },
    },null,2)+"\n");
  // Acceptance requires every causally linked Native + Worker assertion
  // above, including a distinct real Close POST and independently signed
  // closing fill/bill/flat source. Merely deleting the old throw cannot pass.
  return {caseId,status:"PASS",artifact:resolve(dir,"signed-partial-fill-protection.json")};
}
