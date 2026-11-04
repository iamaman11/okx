// T09-SPEC: a second authorized linear venue spec, different symbol,
// tick, lot, min and multiplier, on the unchanged production trade path.
import assert from "node:assert/strict";
import { actualNativeWorker, hash } from "../native-workerd.mjs";
export async function run({caseId,root,signal}){
  process.env.OKX_T09_SPEC_SECOND_LINEAR_PROBE="1";
  const t=await actualNativeWorker(root,signal,{
    statusProbe:true,secondAdmission:true
  });
  const n=t.nativeSecondarySpec,w=t.workerSecondarySpec;
  assert.ok(n&&w&&t.nativeSecondAdmission,
    "T09-SPEC requires actual Native/Worker public execution witnesses");
  assert.equal(n.schema,"okx.t09-spec-second-linear-native-worker/v1");
  assert.equal(w.schema,"okx.t09-spec-worker-risk/v1");
  assert.equal(n.instrument,"ETH-USDT-SWAP");
  assert.equal(n.tick,"0.5");
  assert.equal(n.lot,"0.05");
  assert.equal(n.minimum,"0.05");
  assert.equal(n.multiplier,"0.02");
  assert.equal(n.no_metadata_permission_expansion,"INSTRUMENT_NOT_ALLOWED");
  assert.equal(n.invalid_lot_reason,"QUANTITY_NOT_ON_VENUE_LOT");
  assert.equal(n.invalid_tick_reason,"NO_EXECUTABLE_PRICE_WITHIN_SLIPPAGE");
  assert.equal(n.quote_entry_upper_usd,"1.0005");
  assert.equal(n.intent_state,"ACKNOWLEDGED");
  assert.equal(n.sql_risk_held,true);
  assert.equal(n.venue_order_id,"recorded-eth-spec-001");
  assert.ok(n.begin_post_at_ms>0&&n.sql_revision>0);
  assert.equal(n.posts_before,4);
  assert.equal(n.posts_after,5);
  assert.equal(w.venue_new_posts,1);
  assert.equal(w.budget.entry_budget.reserved_usd,"8.002");
  assert.equal(w.budget.entry_budget.consumed_usd,"1.0005");
  assert.ok(w.risk_book_sequence>=n.worker_sequence);
  const events=[
    ["public_trade_entered","TradeService",[n.instrument,n.intent_id]],
    ["reserve_committed","RuntimeSession/SQLiteDO",n.worker_budget],
    ["begin_post_committed","NativeAccountSQL",[n.begin_post_at_ms,n.sql_revision]],
    ["transport_send_observed","RecordedAuthenticatedHTTP",[n.venue_order_id,n.posts_after]],
    ["second_spec_rules_verified","PublicWsCoordinator",
      [n.tick,n.lot,n.minimum,n.multiplier]],
  ].map(([stage,owner,source])=>({stage,owner,source_digest:hash(source)}));
  return {
    case_id:caseId,status:"PASS",
    provenance:{schema:"okx.native-workerd-transcript/v1",sha256:t.transcript_sha256},
    assertions:[
      {id:"different_symbol",expected:"ETH-USDT-SWAP",actual:n.instrument},
      {id:"registry_does_not_grant_permission",expected:"INSTRUMENT_NOT_ALLOWED",
        actual:n.no_metadata_permission_expansion},
      {id:"second_contract_multiplier",expected:"0.02",actual:n.multiplier},
      {id:"second_venue_lot",expected:"0.05",actual:n.lot},
      {id:"second_entry_cost_usd",expected:"1.0005",actual:n.quote_entry_upper_usd},
      {id:"off_lot_refused",expected:"QUANTITY_NOT_ON_VENUE_LOT",actual:n.invalid_lot_reason},
      {id:"off_bound_tick_refused",expected:"NO_EXECUTABLE_PRICE_WITHIN_SLIPPAGE",
        actual:n.invalid_tick_reason},
      {id:"single_venue_create",expected:"1",actual:String(n.posts_after-n.posts_before)},
    ],
    events,
    readbacks:{
      sql:[{revision:n.sql_revision,digest:hash([n.intent_id,n.venue_order_id])}],
      risk_book:[{revision:w.risk_book_sequence,digest:hash(w.budget)}],
    },
    outbound_sends:[{
      account:n.account_uid,intent:n.intent_id,operation_kind:"create",count:1
    }],
  };
}
