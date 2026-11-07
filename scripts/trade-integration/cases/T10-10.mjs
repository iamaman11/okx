// T10-10: signed duplicate, unknown-fee recovery, late revision and bound breach.
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
  const unknown=original.phases[0].unknown_fee_hold;
  assert.ok(unknown,"missing-fee hold before the independently signed bill arrives");
  assert.equal(unknown.intent_id,p.intent_id);
  assert.equal(unknown.sql.reason,"ENTRY_FEE_BILL_SOURCE_MISMATCH");
  assert.equal(unknown.sql.fill_count,1);
  assert.equal(unknown.sql.quantity,"0.12");
  assert.equal(unknown.financial_complete,false);
  assert.deepEqual(unknown.worker_before.entry_budget,unknown.worker_after.entry_budget);
  assert.equal(unknown.venue_posts,4);
  assert.equal(original.phases[0].sql.bill_count,1,"restored bill is actually ingested");
  assert.equal(original.phases[0].sql.fee_usdt,"0.0006");
  const late=p.late_released_fee;
  assert.ok(late,"independent post-release bill must produce a real settlement revision");
  assert.notEqual(late.intent_id,p.intent_id,"late subcase owns its own coherent released intent");
  assert.match(late.source_sha256,/^[0-9a-f]{64}$/);
  assert.equal(late.source_bill_id,"t10-10-independent-late-fee-001");
  assert.equal(late.before.reservation_active,false);
  // SQL Decimal scale is not economic identity. Preserve the raw receipts,
  // accept only exactly equal decimal text (no float conversion or tolerance).
  assert.match(late.before.entry_upper_usd,/^1\.00050*$/);
  assert.match(late.before.terminal.cumulative_entry_usd,/^1\.00050*$/);
  assert.match(late.after.late.cumulative_entry_usd,/^1\.00060*$/);
  assert.equal(late.after.late.intent_id,late.intent_id);
  assert.equal(late.after.late.reservation_key,late.before.terminal.reservation_key);
  assert.equal(late.after.late.plan_hash,late.before.terminal.plan_hash);
  assert.ok(late.after.late.settlement_revision>late.before.terminal.settlement_revision);
  assert.notEqual(late.after.late.source_digest,late.before.terminal.source_digest);
  assert.equal(late.after.pending,false);
  assert.deepEqual(late.replay.late,late.after.late);
  for(const worker of [late.worker_after,late.worker_replay,late.worker_after_denial]) {
    assert.equal(worker.entry_budget.consumed_usd,"4.0021");
    assert.equal(worker.entry_budget.reserved_usd,"4");
    assert.equal(worker.entry_budget.bound_breach_usd,"0.0001");
  }
  assert.equal(late.new_risk_denial.data.primary_reason,"BUDGET_BOUND_BREACH");
  assert.equal(late.new_risk_denial.data.send_state,"NOT_STARTED");
  assert.equal(late.new_risk_denial.data.state,"REJECTED");
  assert.equal(late.venue_posts,4);
  const dir=resolve(artifactsDir,"T10-10");
  await mkdir(dir,{recursive:true});
  await writeFile(resolve(dir,"duplicate-signed-finance.json"),
    JSON.stringify({
      schema:"okx.t10-10-financial-revisions/v1",
      status:"PASS",
      input_source:"RecordedAuthenticatedHTTP",
      native_sql_replay:p,
      worker_sequence:p.risk_book_sequence,
      transcript_sha256:t.transcript_sha256,
      unknown_fee_hold:unknown,
      source_digest:hash([p.before,p.after,late]),
    },null,2)+"\n");
  const assertions=[
    ["duplicate_fill_quantity","0.3",p.after.quantity],
    ["duplicate_verified_fee_usdt","0.0015",p.after.verified_fee_usdt],
    ["unknown_fee_retains_reserved_usd",unknown.worker_before.entry_budget.reserved_usd,
      unknown.worker_after.entry_budget.reserved_usd],
    ["restored_signed_fee_usdt","0.0006",original.phases[0].sql.fee_usdt],
    ["late_cumulative_entry_usd","1.0006",late.after.late.cumulative_entry_usd.replace(/0+$/,"")],
    ["late_campaign_consumed_usd","4.0021",late.worker_after_denial.entry_budget.consumed_usd],
    ["other_reservation_retained_usd","4",late.worker_after_denial.entry_budget.reserved_usd],
    ["actual_above_upper_recorded_usd","0.0001",late.worker_after_denial.entry_budget.bound_breach_usd],
    ["post_breach_new_risk_reason","BUDGET_BOUND_BREACH",late.new_risk_denial.data.primary_reason],
    ["subcase_extra_venue_posts","0",String(late.venue_posts-p.venue_posts)],
  ].map(([id,expected,actual])=>({id,expected,actual}));
  const events=[
    ["unknown_fee_coverage_retained","NativeAccountSQL+RuntimeSessionSQLiteDO",unknown],
    ["unknown_fee_source_restored","NativeAccountSQL",original.phases[0].sql],
    ["duplicate_fill_and_bill_replayed","NativeAccountSQL",[p.before,p.after,p.source_reads_after]],
    ["single_economic_debit_verified","RuntimeSessionSQLiteDO",p.worker_budget],
    ["late_fee_or_rebate_ingested","NativeAccountSQL",[late.source_bill_id,late.source_sha256,late.after]],
    ["settlement_revision_replayed","NativeAccountSQL+RuntimeSessionSQLiteDO",[late.after,late.replay,late.worker_replay]],
    ["fee_upper_breach_or_pending_verified","RuntimeSessionSQLiteDO+NativeTradeService",
      [late.worker_after_denial,late.new_risk_denial]],
  ].map(([stage,owner,source])=>({stage,owner,source_digest:hash(source)}));
  for(const revision of [p.before.finance_revision,p.after.finance_revision,
    late.after.finance_revision,p.risk_book_sequence,late.worker_sequence_after,
    late.worker_sequence_replay,late.worker_sequence_after_denial])
    assert.ok(Number.isSafeInteger(revision)&&revision>0,"actual owner revision required");
  return {case_id:caseId,status:"PASS",
    provenance:{schema:"okx.native-workerd-transcript/v1",sha256:t.transcript_sha256},
    assertions,events,
    readbacks:{
      sql:[{revision:p.before.finance_revision,digest:hash(p.before)},
        {revision:p.after.finance_revision,digest:hash(p.after)},
        {revision:late.after.finance_revision,digest:hash(late.after)}],
      risk_book:[{revision:p.risk_book_sequence,digest:hash(p.worker_budget)},
        {revision:late.worker_sequence_after,digest:hash(late.worker_after)},
        {revision:late.worker_sequence_replay,digest:hash(late.worker_replay)},
        {revision:late.worker_sequence_after_denial,digest:hash(late.worker_after_denial)}],
    },
    outbound_sends:[{account:late.before.terminal.account_key,intent:late.intent_id,
      operation_kind:"late-fee-settlement",count:0}],
    limitations:["Late debit fee variant; positive rebate economics are not claimed by this case"],
  };
}
