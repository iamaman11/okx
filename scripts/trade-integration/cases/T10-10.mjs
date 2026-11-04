// T10-10: real signed duplicate financial ingestion (first causal leg).
// No mandatory PASS until a later independent fee/rebate and a Worker
// settlement revision/breach are also demonstrated in the SAME cohort.
import assert from "node:assert/strict";
import { mkdir, writeFile } from "node:fs/promises";
import { resolve } from "node:path";
import { actualNativeWorker, hash } from "../native-workerd.mjs";

export async function run({ caseId, root, signal, artifactsDir }) {
  assert.equal(caseId, "T10-10");
  assert.ok(artifactsDir,"T10-10 needs retained per-stage artifacts");
  process.env.OKX_T10_10_FINANCE_REPLAY_PROBE="1";
  const t=await actualNativeWorker(root,signal,{
    statusProbe:true,secondAdmission:true,partialFills:true,
  });
  const p=t.nativeT10FinanceReplay, original=t.nativePartialFills;
  const settled=t.nativeTerminalSettlement;
  assert.ok(p&&original&&settled,
    "genuine SQL finance duplicate and Worker settlement receipts required");
  assert.equal(p.schema,"okx.t10-10-signed-finance-duplicate/v1");
  assert.equal(p.same_native_sql_owner,true);
  assert.equal(p.before.fill_count,2);
  assert.equal(p.before.bill_count,2);
  assert.equal(p.before.quantity,"0.3");
  assert.equal(p.before.verified_fee_usdt,"0.0015");
  for(const field of ["fill_count","bill_count","quantity","gross_notional_usdt",
    "verified_fee_usdt","bill_source_digest"])
    assert.deepEqual(p.after[field],p.before[field],"finance duplicate changed "+field);
  assert.ok(p.source_reads_after.fills>p.source_reads_before.fills);
  assert.ok(p.source_reads_after.bills>p.source_reads_before.bills);
  assert.match(p.after.bill_source_digest,/^[0-9a-f]{64}$/);
  assert.ok(p.after.finance_revision>=p.before.finance_revision);
  assert.equal(p.worker_budget.entry_budget.consumed_usd,"4.002");
  assert.equal(p.worker_budget.entry_budget.reserved_usd,"4");
  assert.equal(p.venue_posts,4);
  assert.equal(settled.consumed_usd,"4.002");
  const dir=resolve(artifactsDir,"T10-10");
  await mkdir(dir,{recursive:true});
  await writeFile(resolve(dir,"duplicate-signed-finance.json"),
    JSON.stringify({
      schema:"okx.t10-10-first-causal-leg/v1",
      status:"DUPLICATE_INGESTION_CHECKPOINT",
      mandatory_case_pass:false,
      input_source:"RecordedAuthenticatedHTTP",
      native_sql_replay:p,
      worker_sequence:p.risk_book_sequence,
      transcript_sha256:t.transcript_sha256,
      source_digest:hash([p.before,p.after]),
      still_required:["late_signed_fee_or_rebate",
        "new_idempotent_settlement_revision","unknown_fee_coverage_hold",
        "actual_exceeds_upper_bound_breach"],
    },null,2)+"\n");
  assert.equal(p.late_fee_rebate_source_proven,false);
  assert.equal(p.upper_bound_breach_proven,false);
  throw Error("T10_10_LATE_FEE_REBATE_REVISION_AND_BREACH_NOT_PROVEN");
}
