// T09.6 requires original public TradeService Close0.30 and Close0.20,
// terminal signed fills/bills, two canceled real protective algos,
// persisted Native SQL zero exposure and RELEASE in the production RiskBook.
import assert from "node:assert/strict";
import {actualNativeWorker,hash} from "../native-workerd.mjs";

export async function run({root,signal}) {
  const transcript=await actualNativeWorker(root,signal,{
    statusProbe:true,secondAdmission:true,partialFills:true,managedAdd:true,closeMode:true,
  });
  const ack=transcript.nativeCloseAck;
  const terminal=transcript.nativeCloseTerminal;
  const sources=transcript.nativeCloseFinancialSources;
  const native=transcript.nativeRestart;
  const worker=transcript.workerCloseReconciled;
  assert.ok(ack&&terminal&&sources&&native&&worker,
    "T09.6 Native and persisted Worker receipts must all exist");
  assert.equal(ack.closes.length,2,"separate 0.30 and 0.20 Close required");
  assert.deepEqual(ack.closes.map(c=>c.quantity),["0.3","0.2"]);
  assert.deepEqual(ack.closes.map(c=>c.post_count),[6,7]);
  assert.deepEqual(
    ack.closes.map(c=>c.source_intent_id),
    [transcript.nativeSecondAdmission.intent_id,transcript.nativeManagedAdd.intent_id],
    "two signed closes must reference their own distinct persisted Native entry intents"
  );
  assert.equal(new Set(ack.closes.map(c=>c.source_intent_id)).size,2,
    "each managed Close must belong to a different signed entry intent");
  assert.equal(terminal.closes.length,2);
  assert.equal(terminal.entries.length,2);
  for (const c of terminal.closes) {
    assert.equal(c.state,"FILLED");
    assert.equal(c.release.state,"verified");
  }
  for (const e of terminal.entries)
    assert.equal(e.release.state,"verified");
  // Seven independent immutable source bills: four entry + three exit.
  // Close ACK/flat venue state alone cannot establish this money proof.
  const billDebits=new Map([
    ["bill:t09-entry-bill-001","-0.0005"],
    ["bill:t09-second-001-bill","-0.0006"],
    ["bill:t09-second-002-bill","-0.0009"],
    ["bill:t09-managed-add-001-bill","-0.001"],
    ["bill:t09-close-bill-001","-0.0005"],
    ["bill:t09-managed-close-030-bill","-0.0015"],
    ["bill:t09-managed-close-020-bill","-0.001"],
  ]);
  const observedBills=terminal.fee_bill_evidence;
  assert.ok(Array.isArray(observedBills));
  assert.equal(observedBills.length,billDebits.size);
  for (const bill of observedBills) {
    assert.equal(bill.amount_usdt,billDebits.get(bill.economic_event_key),
      "no fabricated, missing or duplicated fee bill amount");
    assert.equal(bill.legs,1,
      "each bill must contribute exactly one immutable economic fee debit");
    assert.ok(billDebits.delete(bill.economic_event_key),
      "unexpected or repeated economic fee source");
  }
  assert.equal(billDebits.size,0);
  assert.equal(terminal.entry_fees_usdt,"-0.003");
  assert.equal(terminal.exit_fees_usdt,"-0.003");
  assert.equal(sources.flat,true);
  assert.equal(sources.capture_reason,null,"financial owner must report no source error");
  assert.equal(terminal.remaining_lots,0);
  assert.equal(native.remaining_lot_count,0);
  assert.equal(native.remaining_lot_quantity,"0");
  assert.equal(terminal.unresolved_census,0);
  assert.equal(terminal.orphan_algos,0);
  assert.equal(terminal.worker_budget.entry_budget.consumed_usd,"6.003");
  assert.equal(terminal.worker_budget.entry_budget.reserved_usd,"0");
  assert.equal(terminal.worker_budget.reserved_loss_usd,"0");
  assert.equal(terminal.outbound_posts,9,
    "five earlier POSTs, two closes and two cancel-algos");
  assert.deepEqual(worker.worker_budget,terminal.worker_budget,
    "native termination must retain exact persisted Worker budget");
  const uid=transcript.nativeManagedAdd.account_uid;
  const closeIds=ack.closes.map(c=>String(c.close_intent_id));
  assert.equal(new Set(closeIds).size,2);
  return {
    case_id:"T09.6", status:"PASS",
    provenance:{schema:"okx.native-workerd-transcript/v1",
      sha256:transcript.transcript_sha256},
    assertions:[
      {id:"remaining_exposure",expected:"0",actual:native.remaining_lot_quantity},
      {id:"orphan_algo_orders",expected:"0",actual:String(terminal.orphan_algos)},
      {id:"outstanding_reserve_usd",expected:"0",
        actual:terminal.worker_budget.entry_budget.reserved_usd},
      {id:"campaign_consumed_usd",expected:"6.003",
        actual:terminal.worker_budget.entry_budget.consumed_usd},
      {id:"central_reserved_loss_usd",expected:"0",
        actual:terminal.worker_budget.reserved_loss_usd},
      {id:"entry_fees_usdt",expected:"-0.003",actual:terminal.entry_fees_usdt},
      {id:"exit_fees_usdt",expected:"-0.003",actual:terminal.exit_fees_usdt},
      {id:"verified_fee_bill_count",expected:"7",actual:String(observedBills.length)},
      {id:"total_mutation_posts",expected:"9",actual:String(terminal.outbound_posts)},
    ],
    events:[
      {stage:"public_trade_entered",owner:"TradeService",
        source_digest:hash(ack.closes)},
      {stage:"fill_ingested",owner:"NativeAccountSQL",
        source_digest:hash([sources.fills,sources.bills])},
      {stage:"protection_verified",owner:"TradeService+OKXRecordedHttpBoundary",
        source_digest:hash(terminal.entries)},
      {stage:"settlement_committed",owner:"RuntimeSession/SQLiteDO",
        source_digest:hash(terminal.worker_budget)},
      {stage:"release_confirmed",owner:"RuntimeSession/SQLiteDO",
        source_digest:hash(terminal.closes.map(c=>c.release)
          .concat(terminal.entries.map(e=>e.release)))},
    ],
    readbacks:{
      sql:[{revision:native.managed_add.sql_revision,
        digest:hash([native,sources,terminal.closes])}],
      risk_book:[{revision:terminal.worker_risk_book_sequence,
        digest:hash(worker.worker_budget)}],
    },
    outbound_sends:[
      ...closeIds.map(intent=>({account:uid,intent,operation_kind:"create",count:1})),
      ...["900000002","900000003"].map(intent=>({
        account:uid,intent,operation_kind:"cancel-algo",count:1
      })),
    ],
  };
}