// T09.4 is not accepted unless the SAME production TradeService/AccountStore
// and persisted Worker prove Add0.20, fill/bill0.001 and cumulative spend6.003.
// The separate cohort has no obsolete T09.0 component reservations.
import assert from "node:assert/strict";
import {actualNativeWorker, hash} from "../native-workerd.mjs";
export async function run({root,signal}) {
  const proof = await actualNativeWorker(root,signal,{
    statusProbe:true,secondAdmission:true,partialFills:true,managedAdd:true
  });
  const add = proof.nativeManagedAdd;
  assert.ok(add,"T09.4 signed Native Add receipt is mandatory");
  assert.equal(add.operation,"add");
  assert.equal(add.instrument,"BTC-USDT-SWAP");
  assert.equal(add.added_quantity,"0.2");
  assert.equal(add.add_entry_usd,"2.001");
  assert.equal(add.linked_bill_fee_usdt,"0.001");
  assert.equal(add.cumulative_spent_usd,"6.003");
  assert.equal(add.outbound_posts,5);
  assert.equal(add.worker_budget.entry_budget.consumed_usd,"6.003");
  assert.equal(add.worker_budget.entry_budget.reserved_usd,"0");
  assert.deepEqual(proof.workerSecondAdmission.budget,add.worker_budget);
  assert.equal(add.sql_restart_managed_exposure,"0.5");
  return {
    case_id:"T09.4",status:"PASS",
    provenance:{schema:"okx.native-workerd-transcript/v1",sha256:proof.transcript_sha256},
    assertions:[
      {id:"managed_add_quantity",expected:"0.2",actual:add.added_quantity},
      {id:"add_fee",expected:"0.001",actual:add.linked_bill_fee_usdt},
      {id:"campaign_consumed",expected:"6.003",actual:add.cumulative_spent_usd}
    ],
    events:[
      {stage:"public_trade_entered",owner:"TradeService",source_digest:hash(add.intent_id)},
      {stage:"reserve_committed",owner:"RuntimeSession/SQLiteDO",source_digest:hash(add.central_receipt_hash)},
      {stage:"begin_post_committed",owner:"NativeAccountSQL",source_digest:hash(add.sql_begin_post_at_ms)},
      {stage:"transport_send_observed",owner:"OKXRecordedHttpBoundary",source_digest:hash(add.venue_order_id)},
      {stage:"fill_ingested",owner:"NativeAccountSQL",source_digest:hash(add.linked_bill_source_digest)},
      {stage:"protection_verified",owner:"TradeService",source_digest:hash(add.protection)},
      {stage:"settlement_committed",owner:"RuntimeSession/SQLiteDO",source_digest:hash(add.worker_budget)},
      {stage:"release_confirmed",owner:"RuntimeSession/SQLiteDO",source_digest:hash(proof.nativeTerminal.opening_status.data.central_release)}
    ],
    readbacks:{
      sql:[{revision:add.sql_revision,digest:hash(add.intent_id)}],
      risk_book:[{revision:add.risk_book_sequence,digest:hash(add.worker_budget)}]
    },
    outbound_sends:[{account:add.account_uid,intent:add.intent_id,operation_kind:"create",count:1}]
  };
}
