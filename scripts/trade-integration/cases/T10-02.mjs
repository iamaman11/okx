// T10-02: the public managed Add succeeds with exact native SQL lots and
// Worker money; an independently signed 0.70 venue position must then block
// a new 0.10 Add because only 0.50 belongs to this SQL/strategy owner.
import assert from "node:assert/strict";
import { actualNativeWorker, hash } from "../native-workerd.mjs";

export async function run({caseId,root,signal}) {
  process.env.OKX_T10_02_EXTERNAL_EXPOSURE_PROBE="1";
  const t=await actualNativeWorker(root,signal,{
    statusProbe:true,secondAdmission:true,partialFills:true,managedAdd:true
  });
  const v=t.nativeT10Unowned, add=t.nativeManagedAdd;
  assert.ok(v&&add&&t.nativeCrossUidDenial&&t.nativeSecondAdmission,
    "T10-02 requires genuine Native+Worker signed source and owner witnesses");
  assert.equal(v.schema,"okx.t10-02-unowned-native-worker/v1");
  assert.equal(v.managed_intent,add.intent_id);
  assert.equal(v.managed_state,"FILLED");
  assert.ok(v.managed_sql_revision>0);
  assert.equal(v.sql_owned_lots,"0.5");
  assert.equal(v.venue_signed_position,"0.7");
  assert.match(v.source_digest,/^[a-f0-9]{64}$/);
  assert.match(v.denial_reason,
    /UNATTRIBUTED_EXCHANGE_POSITION|COHORT_UNMANAGED_EXTERNAL_EXPOSURE/);
  assert.equal(v.posts_before,5);
  assert.equal(v.posts_after,5);
  // Read-only source timestamps may advance while the same financial
  // values remain unchanged; test the money oracle, not JSON metadata.
  for (const field of ["campaign_limit_usd","consumed_loss_usd",
    "reserved_loss_usd","remaining_loss_usd"]) {
    assert.equal(v.worker_after[field],v.worker_before[field],field);
  }
  for (const field of ["limit_usd","consumed_usd","reserved_usd","remaining_usd"]) {
    assert.equal(v.worker_after.entry_budget[field],
      v.worker_before.entry_budget[field],field);
  }
  assert.equal(v.worker_after.entry_budget.consumed_usd,"6.003");
  assert.equal(v.worker_after.entry_budget.reserved_usd,"0");
  assert.equal(t.nativeCrossUidDenial.foreign_sql_intent_absent,true);
  assert.equal(t.nativeCrossUidDenial.status_reason,"INTENT_NOT_FOUND");
  assert.equal(t.nativeCrossUidDenial.cancel_reason,"INTENT_NOT_FOUND");
  const events=[
    ["managed_exposure_positive_admitted","TradeService+NativeAccountSQL",add],
    ["external_or_unowned_exposure_observed","RecordedAuthenticatedHTTP",v.source_digest],
    ["unknown_exposure_denied","TradeService",v.denial_reason],
    ["sql_worker_exposure_unchanged","NativeAccountSQL+RuntimeSession/SQLiteDO",
      [v.sql_owned_lots,v.worker_before,v.worker_after]],
  ].map(([stage,owner,source])=>({stage,owner,source_digest:hash(source)}));
  return {
    case_id:caseId,status:"PASS",
    provenance:{schema:"okx.native-workerd-transcript/v1",sha256:t.transcript_sha256},
    assertions:[
      {id:"positive_managed_lots",expected:"0.5",actual:v.sql_owned_lots},
      {id:"observed_unowned_position",expected:"0.7",actual:v.venue_signed_position},
      {id:"no_post_for_unowned_risk",expected:"5",actual:String(v.posts_after)},
      {id:"central_entry_spend_unchanged",expected:"6.003",
        actual:v.worker_after.entry_budget.consumed_usd},
      {id:"central_unspent_reserve_unchanged",expected:"0",
        actual:v.worker_after.entry_budget.reserved_usd},
      {id:"cross_uid_stays_isolated",expected:"INTENT_NOT_FOUND",
        actual:t.nativeCrossUidDenial.cancel_reason},
    ],
    events,
    readbacks:{
      sql:[{revision:v.managed_sql_revision,
        digest:hash([v.managed_intent,v.sql_owned_lots])}],
      risk_book:[{revision:v.worker_sequence_after,
        digest:hash(v.worker_after)}],
    },
    outbound_sends:[{account:v.account_uid,intent:v.managed_intent,
      operation_kind:"create",count:1}],
  };
}
