// T09.3: signed partial0.12 and terminal0.18 fills and linked bills.
import assert from "node:assert/strict";
import { actualNativeWorker, hash } from "../native-workerd.mjs";
export async function run({root,signal}) {
  const source=await actualNativeWorker(root,signal,{statusProbe:true,secondAdmission:true,partialFills:true});
  const proof=source.nativePartialFills;
  assert.ok(proof && proof.phases.length===2,"Native account capture evidence required");
  const [partial,terminal]=proof.phases;
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
  assert.notEqual(partial.financial_complete,true);
  assert.equal(terminal.phase,"terminal");
  assert.equal(terminal.sql.notional_usdt,"3");
  assert.equal(proof.outbound_posts,4,"no new POST for incoming venue fills");
  assert.equal(proof.worker_budget.entry_budget.consumed_usd,"4.002");
  assert.equal(proof.worker_budget.entry_budget.reserved_usd,"4");
  assert.deepEqual(source.workerSecondAdmission.budget,proof.worker_budget);
  return {
    case_id:"T09.3",status:"PASS",
    provenance:{schema:"okx.native-workerd-transcript/v1",sha256:source.transcript_sha256},
    assertions:[
      {id:"partial_qty",expected:"0.12",actual:partial.sql.quantity},
      {id:"partial_fee",expected:"0.0006",actual:partial.sql.fee_usdt},
      {id:"terminal_qty",expected:"0.3",actual:terminal.sql.quantity},
      {id:"terminal_fee",expected:"0.0015",actual:terminal.sql.fee_usdt},
      {id:"cumulative_spent",expected:"4.002",actual:proof.worker_budget.entry_budget.consumed_usd}
    ],
    events:[
      {stage:"public_trade_entered",owner:"TradeService",source_digest:hash(source.nativeSecondAdmission.intent_id)},
      {stage:"reserve_committed",owner:"RuntimeSession/SQLiteDO",source_digest:hash(source.nativeSecondAdmission.central_receipt_hash)},
      {stage:"begin_post_committed",owner:"NativeAccountSQL",source_digest:hash(source.nativeSecondAdmission.sql_begin_post_at_ms)},
      {stage:"transport_send_observed",owner:"OKXRecordedHttpBoundary",source_digest:hash(source.nativeSecondAdmission.sql_exchange_order_id)},
      {stage:"fill_ingested",owner:"NativeAccountSQL",source_digest:hash([partial.sql,terminal.sql])},
      {stage:"protection_verified",owner:"TradeService",source_digest:hash([partial.protection,terminal.protection])},
      {stage:"settlement_committed",owner:"RuntimeSession/SQLiteDO",source_digest:hash(proof.worker_budget)},
      {stage:"release_confirmed",owner:"RuntimeSession/SQLiteDO",source_digest:hash(source.nativeTerminal.opening_status.data.central_release)}
    ],
    readbacks:{
      sql:[{revision:terminal.sql.fill_watermark,digest:hash([partial.sql,terminal.sql])}],
      risk_book:[{revision:proof.risk_book_sequence,digest:hash(proof.worker_budget)}]
    },
    outbound_sends:[{account:source.nativeSecondAdmission.account_uid,intent:proof.intent_id,operation_kind:"create",count:1}]
  };
}
