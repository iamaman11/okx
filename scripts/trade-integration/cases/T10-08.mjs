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
  });
  const witness=t.nativePartialFills, settlement=t.nativeTerminalSettlement;
  assert.ok(witness&&settlement,"real Native fills/Worker readback required");
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
  assert.equal(witness.outbound_posts,4);
  assert.equal(settlement.outbound_posts,4);
  assert.equal(settlement.reserved_usd,"4");
  assert.equal(settlement.consumed_usd,"4.002");
  const dir=resolve(artifactsDir,caseId);
  await mkdir(dir,{recursive:true});
  await writeFile(resolve(dir,"signed-partial-fill-protection.json"),
    JSON.stringify({
      schema:"okx.t10-08-partial-fill-prerequisite/v1",
      mandatory_case_pass:false,
      intent_id:witness.intent_id,
      native_sql_fill_revisions:[partial.sql,terminal.sql],
      protection:[partial.protection,terminal.protection],
      worker_sequence:witness.risk_book_sequence,
      worker_entry_budget:witness.worker_budget.entry_budget,
      venue_create_posts:witness.outbound_posts,
      source_digest:hash([partial.sql,terminal.sql]),
      transcript_sha256:t.transcript_sha256,
      missing:["signed_protection_failure_after_initial_partial_fill",
        "late_signed_fill_after_protection_failure",
        "no_false_active_and_no_new_risk_admission",
        "bounded_emergency_protect_cancel_or_reduce_attempt",
        "same_native_sql_and_worker_financial_readbacks_after_late_fill"],
    },null,2)+"\n");
  // The source above proves successful protection; it does not prove the
  // emergency policy under protection failure. Never call it T10-08 PASS.
  throw Error("T10_08_SAME_INTENT_PROTECTION_FAILURE_AND_EMERGENCY_NOT_PROVEN");
}
