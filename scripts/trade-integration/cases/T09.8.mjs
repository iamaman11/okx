// The real production TradeService attempts a new 0.40 entry only AFTER
// both managed lots have been flattened, settled and released. The existing
// Worker rejects a source-proven 4.002 USD upper against 3.997 available.
import assert from "node:assert/strict";
import {actualNativeWorker,hash} from "../native-workerd.mjs";

export async function run({root,signal}) {
  const transcript=await actualNativeWorker(root,signal,{
    statusProbe:true,secondAdmission:true,partialFills:true,managedAdd:true,closeMode:true,
  });
  const denied=transcript.nativeBudgetDenial;
  const terminal=transcript.nativeCloseTerminal;
  const restart=transcript.workerTerminalRestart;
  assert.ok(denied&&terminal&&restart,"T09.8 needs both real Native and Worker receipts");
  assert.equal(denied.denial_reason,"CAMPAIGN_ENTRY_BUDGET_EXHAUSTED",
    "deny by true central campaign cap rather than stale fee/source/transport");
  assert.equal(denied.denial_origin,"CENTRAL");
  assert.equal(denied.send_state,"NOT_STARTED");
  assert.equal(denied.requested_entry_upper_usd,"4.002");
  assert.equal(denied.sql_entry_quote_usd,"4.002",
    "immutable SQL quote must match the proven Worker RESERVE upper");
  assert.equal(denied.remaining_entry_budget_usd,"3.997");
  assert.equal(denied.campaign_consumed_usd,"6.003");
  assert.equal(denied.sql_intent_state,"REJECTED");
  assert.equal(denied.sql_risk_held,false);
  assert.equal(denied.new_exchange_posts,0);
  assert.equal(denied.outbound_posts_before,9);
  assert.equal(denied.outbound_posts_after,9);
  assert.deepEqual(denied.worker_budget_after,terminal.worker_budget,
    "budget exhaustion cannot consume or free another reservation");
  assert.deepEqual(restart.worker_budget,denied.worker_budget_after,
    "Worker restart must not erase the denied budget witness");
  assert.equal(restart.worker_budget.entry_budget.consumed_usd,"6.003");
  assert.equal(restart.worker_budget.entry_budget.reserved_usd,"0");

  return {
    case_id:"T09.8",status:"PASS",
    provenance:{schema:"okx.native-workerd-transcript/v1",
      sha256:transcript.transcript_sha256},
    assertions:[
      {id:"campaign_consumed_usd",expected:"6.003",
        actual:denied.campaign_consumed_usd},
      {id:"remaining_entry_budget_usd",expected:"3.997",
        actual:denied.remaining_entry_budget_usd},
      {id:"denial_reason",expected:"CAMPAIGN_ENTRY_BUDGET_EXHAUSTED",
        actual:denied.denial_reason},
      {id:"requested_entry_upper_usd",expected:"4.002",
        actual:denied.requested_entry_upper_usd},
      {id:"new_exchange_posts",expected:"0",
        actual:String(denied.new_exchange_posts)},
      {id:"sql_intent_state",expected:"REJECTED",
        actual:denied.sql_intent_state},
    ],
    events:[
      {stage:"public_trade_entered",owner:"TradeService+RuntimeSession/SQLiteDO",
        source_digest:hash([denied.denial_reason,denied.sql_entry_quote_usd,
          denied.sql_intent_state,denied.worker_budget_after])},
    ],
    readbacks:{
      sql:[{revision:transcript.nativeProcessRestart.account_a.finance_revision,
        digest:hash([denied.sql_intent_state,denied.sql_entry_quote_usd])}],
      risk_book:[{revision:denied.risk_book_sequence_after,
        digest:hash(denied.worker_budget_after)}],
    },
    outbound_sends:[],
  };
}
