// T10-06: 100 simultaneous real public TradeService replays on one Native
// account SQL and one persisted Worker risk, no synthetic financial outcome.
import assert from "node:assert/strict";
import { actualNativeWorker, hash } from "../native-workerd.mjs";

export async function run({caseId,root,signal}) {
  process.env.OKX_T10_06_CONCURRENT_PROBE="1";
  const t=await actualNativeWorker(root,signal);
  const v=t.nativeT10Concurrency;
  const n=t.nativeReplay,w=t.workerReplay;
  assert.ok(v && n && w,"actual Native/Worker concurrent witness required");
  assert.equal(v.requested,100);
  assert.equal(v.same_intent_returns + v.local_no_send_read_denials,100);
  assert.equal(v.nonlocal_failures,0);
  assert.equal(v.original_intent_id,n.opened_intent_id);
  assert.equal(v.persisted_intent_id,n.opened_intent_id);
  // A status/finance mutation already holding capacity may return
  // exact-identity UNKNOWN/no-resubmit; it may not generate a second intent.
  assert.equal(v.created_posts,1);
  assert.equal(v.conflict_reason,"IDEMPOTENCY_CONFLICT");
  assert.ok(Number.isSafeInteger(v.sql_revision) && v.sql_revision>0);
  assert.ok(v.elapsed_ms>=0);
  assert.equal(w.budget.entry_budget.reserved_usd,n.reserved_entry_usd_after_replay);
  assert.equal(w.budget.entry_budget.consumed_usd,"0");
  return {
    case_id:caseId,status:"PASS",
    provenance:{schema:"okx.native-workerd-transcript/v1",sha256:t.transcript_sha256},
    assertions:[
      {id:"concurrent_requests",expected:"100",actual:String(v.requested)},
      {id:"same_intent_or_proven_no_send",expected:"100",
        actual:String(v.same_intent_returns+v.local_no_send_read_denials)},
      {id:"one_venue_create",expected:"1",actual:String(v.created_posts)},
      {id:"changed_payload_rejected",expected:"IDEMPOTENCY_CONFLICT",
        actual:v.conflict_reason},
      {id:"same_durable_intent",expected:n.opened_intent_id,
        actual:v.persisted_intent_id},
      {id:"no_spurious_worker_debit",expected:"0",
        actual:w.budget.entry_budget.consumed_usd},
    ],
    events:[
      {stage:"public_trade_entered",owner:"TradeService",
        source_digest:hash([v.requested,v.original_intent_id])},
      {stage:"reserve_committed",owner:"RuntimeSession/SQLiteDO",
        source_digest:hash(w.budget)},
      {stage:"begin_post_committed",owner:"NativeAccountSQL",
        source_digest:hash(n.sql_intent_id)},
      {stage:"transport_send_observed",owner:"RecordedAuthenticatedHTTP",
        source_digest:hash([v.created_posts,n.sql_client_order_id])},
      {stage:"concurrent_idempotency_reconciled",owner:"TradeService+NativeAccountSQL",
        source_digest:hash(v)},
    ],
    readbacks:{
      sql:[{revision:v.sql_revision,digest:hash(v.persisted_intent_id)}],
      risk_book:[{revision:w.risk_book_sequence,digest:hash(w.budget)}],
    },
    outbound_sends:[{account:n.account_uid,intent:n.opened_intent_id,
      operation_kind:"create",count:1}],
  };
}
