// T09.3: signed partial0.12 and terminal0.18 fills and linked bills.
import assert from "node:assert/strict";
import { actualNativeWorker, hash } from "../native-workerd.mjs";
export async function run({root,signal}) {
  const source=await actualNativeWorker(root,signal,{statusProbe:true,secondAdmission:true,partialFills:true});
  const proof=source.nativePartialFills;
  const cross=source.nativeCrossUidDenial;
  const admitted=source.nativeSecondAdmission;
  const multi=source.nativeStrategyAccountPreflight;
  assert.ok(cross&&admitted,"independent cross-UID Native/Worker checkpoint required");
  assert.ok(multi,"independent Native A/B strategy SQL and public preflight receipt required");
  assert.equal(multi.account_a_uid,admitted.account_uid);
  assert.equal(multi.account_b_uid,cross.foreign_uid);
  assert.notEqual(multi.account_a_uid,multi.account_b_uid);
  assert.equal(multi.a_strategy_registered,true);
  assert.equal(multi.b_strategy_registered,true);
  assert.equal(multi.a_other_strategy_absent,true);
  assert.equal(multi.b_other_strategy_absent,true);
  assert.equal(multi.a_strategy_enabled,false,
    "registration is not an automatic authority for trading");
  assert.equal(multi.b_strategy_enabled,false);
  assert.equal(multi.a_public_account,"okx:real:"+multi.account_a_uid);
  assert.equal(multi.b_public_account,"okx:real:"+multi.account_b_uid);
  assert.equal(multi.a_new_risk_authorized,false);
  assert.equal(multi.b_new_risk_authorized,false);
  assert.equal(multi.native_outbound_posts,4,
    "two concurrent account preflights must not create venue POST");
  assert.match(cross.owner_uid,/^[a-f0-9]{64}$/);
  assert.match(cross.foreign_uid,/^[a-f0-9]{64}$/);
  assert.notEqual(cross.owner_uid,cross.foreign_uid);
  assert.equal(cross.owner_uid,admitted.account_uid);
  assert.equal(cross.owner_intent_id,admitted.intent_id);
  assert.equal(cross.status_reason,"INTENT_NOT_FOUND");
  assert.equal(cross.cancel_reason,"INTENT_NOT_FOUND");
  assert.equal(cross.foreign_sql_intent_absent,true);
  assert.equal(cross.foreign_sql_idempotency_absent,true);
  assert.equal(cross.outbound_posts_before,4);
  assert.equal(cross.outbound_posts_after,4);
  assert.equal(cross.worker_budget.entry_budget.consumed_usd,
    admitted.after_worker_budget.entry_budget.consumed_usd);
  assert.equal(cross.worker_budget.entry_budget.reserved_usd,
    admitted.after_worker_budget.entry_budget.reserved_usd);
  assert.equal(cross.worker_budget.reserved_loss_usd,
    admitted.after_worker_budget.reserved_loss_usd);
  assert.ok(proof && proof.phases.length===2,"Native account capture evidence required");
  const [partial,terminal]=proof.phases;
  for (const phase of [partial,terminal]) {
    assert.ok(phase.source_read_counts,"raw Native source count checkpoint missing");
    for (const [route,count] of Object.entries(phase.source_read_counts))
      assert.ok(Number.isSafeInteger(count)&&count>=0,
        "invalid bounded native HTTP count for "+route);
  }
  for (const [phase,quantity,fee,fillCount] of [
    [partial,"0.12","0.0006",1],[terminal,"0.3","0.0015",2]
  ]) {
    assert.equal(phase.sql.quantity,quantity);
    assert.equal(phase.sql.fee_usdt,fee);
    assert.equal(phase.sql.fill_count,fillCount);
    assert.equal(phase.sql.bill_count,fillCount);
    assert.match(phase.sql.source_digest,/^[a-f0-9]{64}$/);
  }
  assert.equal(partial.phase,"partial");
  assert.ok(["verified_native_fill_and_bill_fee_sources","not_evaluated"].includes(partial.entry_fill_evidence_state));
  if (partial.entry_fill_evidence_state==="not_evaluated")
    assert.equal(partial.financial_complete,false,"missing fresh status evidence must fail closed");
  assert.equal(terminal.phase,"terminal");
  assert.equal(terminal.sql.notional_usdt,"3");
  assert.equal(partial.intent_state,"PARTIALLY_FILLED");
  assert.equal(terminal.intent_state,"FILLED");
  for (const phase of [partial,terminal]) {
    assert.equal(phase.protection?.trigger_basis,"mark");
    assert.equal(String(phase.protection?.trigger_price),"900");
    assert.match(String(phase.protection?.source_digest),/^[a-f0-9]{64}$/);
    assert.ok(Number(phase.protection?.quantity)>=Number(phase.sql.quantity),
      "signed stop coverage must include confirmed actual fill");
  }
  assert.equal(source.nativeRestart.second_intent_state,"FILLED");
  assert.equal(source.nativeRestart.second_active_central_receipt,true);
  assert.equal(source.nativeRestart.remaining_lot_quantity,"0.3");
  assert.equal(proof.outbound_posts,4,"no new POST for incoming venue fills");
  assert.equal(proof.worker_budget.entry_budget.consumed_usd,"4.002");
  assert.equal(proof.worker_budget.entry_budget.reserved_usd,"4");
  assert.deepEqual(source.workerSecondAdmission.budget,proof.worker_budget);
  return {
    case_id:"T09.3",status:"PASS",
    provenance:{schema:"okx.native-workerd-transcript/v1",
      sha256:source.transcript_sha256,
      source_read_counts:{
        partial:partial.source_read_counts,terminal:terminal.source_read_counts,
      }},
    assertions:[
      {id:"partial_qty",expected:"0.12",actual:partial.sql.quantity},
      {id:"partial_fee",expected:"0.0006",actual:partial.sql.fee_usdt},
      {id:"terminal_qty",expected:"0.3",actual:terminal.sql.quantity},
      {id:"terminal_fee",expected:"0.0015",actual:terminal.sql.fee_usdt},
      {id:"cumulative_spent",expected:"4.002",actual:proof.worker_budget.entry_budget.consumed_usd},
      {id:"foreign_uid_status_no_read",expected:"INTENT_NOT_FOUND",actual:cross.status_reason},
      {id:"foreign_uid_cancel_no_mutation",expected:"INTENT_NOT_FOUND",actual:cross.cancel_reason},
      {id:"foreign_uid_no_exchange_post",expected:"4",actual:String(cross.outbound_posts_after)},
      {id:"foreign_uid_has_no_sql_intent",expected:"true",actual:String(cross.foreign_sql_intent_absent)},
      {id:"foreign_uid_has_no_sql_key",expected:"true",actual:String(cross.foreign_sql_idempotency_absent)},
      {id:"account_a_strategy_isolated",expected:"true",actual:String(multi.a_other_strategy_absent)},
      {id:"account_b_strategy_isolated",expected:"true",actual:String(multi.b_other_strategy_absent)},
      {id:"account_a_public_route",expected:"okx:real:"+admitted.account_uid,
        actual:multi.a_public_account},
      {id:"account_b_public_route",expected:"okx:real:"+cross.foreign_uid,
        actual:multi.b_public_account},
      {id:"two_account_preflight_no_post",expected:"4",actual:String(multi.native_outbound_posts)}
    ],
    events:[
      {stage:"public_trade_entered",owner:"TradeService",source_digest:hash(source.nativeSecondAdmission.intent_id)},
      {stage:"reserve_committed",owner:"RuntimeSession/SQLiteDO",source_digest:hash(source.nativeSecondAdmission.central_receipt_hash)},
      {stage:"begin_post_committed",owner:"NativeAccountSQL",source_digest:hash(source.nativeSecondAdmission.sql_begin_post_at_ms)},
      {stage:"transport_send_observed",owner:"OKXRecordedHttpBoundary",source_digest:hash(source.nativeSecondAdmission.sql_exchange_order_id)},
      {stage:"fill_ingested",owner:"NativeAccountSQL",source_digest:hash([partial.sql,terminal.sql])},
      {stage:"protection_verified",owner:"TradeService",source_digest:hash([partial.protection,terminal.protection])},
      {stage:"settlement_committed",owner:"RuntimeSession/SQLiteDO",source_digest:hash(proof.worker_budget)},
      {stage:"release_confirmed",owner:"RuntimeSession/SQLiteDO",source_digest:hash(source.nativeTerminal.opening_status.data.central_release)},
      {stage:"cross_uid_denial_verified",owner:"TradeService+AccountStore+RiskBook",
        source_digest:hash([cross.owner_uid,cross.foreign_uid,cross.status_reason,
          cross.cancel_reason,cross.foreign_sql_intent_absent,cross.worker_budget])},
      {stage:"two_account_strategy_sql_preflight",owner:"TradeService+AccountStore(A/B)",
        source_digest:hash(multi)}
    ],
    readbacks:{
      sql:[{revision:terminal.sql.fill_watermark,digest:hash([partial.sql,terminal.sql])}],
      risk_book:[
        {revision:proof.risk_book_sequence,digest:hash(proof.worker_budget)},
        {revision:cross.risk_book_sequence,digest:hash(cross.worker_budget)}
      ]
    },
    outbound_sends:[{account:source.nativeSecondAdmission.account_uid,intent:proof.intent_id,operation_kind:"create",count:1}]
  };
}
