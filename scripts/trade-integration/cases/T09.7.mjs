// Real native OS-process and persisted Worker/DO restart after T09.6.
// The Rust child reopens BOTH original AccountStore SQLite files with no
// transport, independently reading captured opening NAV and finance facts.
import assert from "node:assert/strict";
import {actualNativeWorker,hash} from "../native-workerd.mjs";

export async function run({root,signal}) {
  const transcript=await actualNativeWorker(root,signal,{
    statusProbe:true,secondAdmission:true,partialFills:true,managedAdd:true,closeMode:true,
  });
  const native=transcript.nativeProcessRestart;
  const worker=transcript.workerTerminalRestart;
  const terminal=transcript.nativeCloseTerminal;
  const sql=transcript.nativeRestart;
  const ack=transcript.nativeCloseAck;
  assert.ok(native&&worker&&terminal&&sql&&ack,
    "T09.7 requires real independent Native/Worker restart receipts");
  assert.equal(native.new_os_process,true);
  assert.equal(native.new_exchange_posts,0,
    "restarted Native must issue zero new recorded exchange POSTs");
  assert.equal(native.recorded_transport_posts_before,9,
    "T09.7 baseline must include both terminal closes and protection cancels");
  assert.equal(native.recorded_transport_posts_after,9,
    "reopening Native SQL must not replay a previously sent exchange request");
  assert.equal(native.account_a.account_opening_equity_usd,"100",
    "opening NAV must come from the retained original source-backed SQL account");
  assert.equal(terminal.entry_fees_usdt,"-0.003",
    "restarted finance must retain immutable signed entry bills");
  assert.equal(terminal.exit_fees_usdt,"-0.003",
    "restarted finance must retain immutable signed exit bills");
  assert.equal(native.account_a.total_fees_usdt,"-0.006",
    "opening/closing fees must be read from immutable SQLite finance entries");
  assert.equal(native.account_a.realized_pnl_usdt,"0");
  assert.equal(native.account_a.equity_a_usd,"99.994");
  assert.equal(native.account_a.remaining_lots,0);
  assert.equal(native.account_a.active_reserve_second,false);
  assert.equal(native.account_a.active_reserve_add,false);
  assert.equal(native.account_a.close_030_state,"FILLED");
  assert.equal(native.account_a.close_020_state,"FILLED");
  assert.ok(Number.isSafeInteger(native.account_a.finance_revision)
    && native.account_a.finance_revision>0);
  assert.equal(native.account_b.equity_b_usd,"100",
    "B must reload its own source-backed opening NAV");
  assert.equal(native.account_b.remaining_lots,0);
  assert.equal(native.account_b.finance_totals,0,
    "A's finance entries must not appear on B");
  assert.equal(sql.remaining_lot_quantity,"0");
  assert.equal(worker.native_sql_reopened,true);
  assert.equal(worker.native_remaining_lot_quantity,"0");
  assert.equal(worker.native_second_risk_held,false);
  assert.equal(worker.native_add_risk_held,false);
  assert.ok(worker.connection_generation>worker.prior_connection_generation,
    "actual persisted Worker generation must advance");
  assert.ok(worker.worker_risk_book_sequence>=worker.previous_risk_book_sequence,
    "Worker restart cannot roll back the CAS sequence");
  assert.deepEqual(worker.worker_budget,terminal.worker_budget,
    "persisted Worker cannot reset or replay consumed campaign money");
  assert.equal(worker.worker_budget.entry_budget.consumed_usd,"6.003");
  assert.equal(worker.worker_budget.entry_budget.reserved_usd,"0");
  assert.equal(worker.worker_budget.reserved_loss_usd,"0");
  assert.equal(terminal.outbound_posts,9);
  assert.equal(ack.closes.length,2);

  return {
    case_id:"T09.7",status:"PASS",
    provenance:{schema:"okx.native-workerd-transcript/v1",
      sha256:transcript.transcript_sha256},
    assertions:[
      {id:"equity_a_usd",expected:"99.994",actual:native.account_a.equity_a_usd},
      {id:"campaign_consumed_usd",expected:"6.003",
        actual:worker.worker_budget.entry_budget.consumed_usd},
      {id:"duplicate_posts",expected:"0",
        actual:String(native.new_exchange_posts)},
      {id:"equity_b_usd",expected:"100",actual:native.account_b.equity_b_usd},
      {id:"remaining_exposure",expected:"0",
        actual:String(native.account_a.remaining_lots)},
      {id:"outstanding_reserve_usd",expected:"0",
        actual:worker.worker_budget.entry_budget.reserved_usd},
    ],
    events:[
      {stage:"public_trade_entered",owner:"TradeService",
        source_digest:hash(ack.closes)},
      {stage:"settlement_committed",owner:"NativeAccountSQL+RuntimeSession/SQLiteDO",
        source_digest:hash([native.account_a,worker.worker_budget])},
      {stage:"release_confirmed",owner:"RuntimeSession/SQLiteDO",
        source_digest:hash([terminal.entries,worker.worker_budget])},
    ],
    readbacks:{
      sql:[
        {revision:native.account_a.finance_revision,digest:hash(native.account_a)},
        {revision:native.account_b.finance_revision,digest:hash(native.account_b)},
      ],
      risk_book:[{revision:worker.worker_risk_book_sequence,
        digest:hash(worker.worker_budget)}],
    },
    outbound_sends:[],
  };
}
