// Replays the same Public TradeService idempotency key on the shared
// production Native SQL + SQLite-backed Worker RiskBook assembly.
import assert from "node:assert/strict";
import { actualNativeWorker, hash } from "../native-workerd.mjs";

export async function run({ caseId, root, signal }) {
  const evidence = await actualNativeWorker(root, signal);
  const { nativeReplay: native, workerReplay: worker, transcript_sha256 } = evidence;
  const scope = evidence.nativeAccountScope;
  assert.equal(scope.same_client_key, true);
  assert.equal(scope.distinct_account_scopes, true);
  assert.equal(scope.foreign_sql_intent_absent, true);
  assert.equal(scope.owner_semantic_sha256, native.sql_request_hash);
  assert.match(scope.other_semantic_sha256, /^[0-9a-f]{64}$/);
  assert.notEqual(scope.other_semantic_sha256,scope.owner_semantic_sha256);
  assert.equal(scope.outbound_posts, 1,
    "S2-07 verified second account cannot cause a duplicate send");
  const policy = process.env.OKX_S2_05_POLICY_CAS_PROBE === "1"
    ? evidence.nativePolicyReplay : null;
  const workerCas = process.env.OKX_S2_05_POLICY_CAS_PROBE === "1"
    ? evidence.workerPolicyCas : null;
  if (process.env.OKX_S2_05_POLICY_CAS_PROBE === "1") {
    assert.ok(policy && workerCas, "S2-05 actual Native/Worker policy receipts");
    assert.equal(workerCas.prior_revision, native.sql_policy_revision);
    assert.equal(workerCas.new_revision, native.sql_policy_revision + 1);
    assert.equal(workerCas.persisted_worker_durable_object, true);
    assert.equal(policy.new_governance_revision, workerCas.new_revision);
    assert.equal(policy.native_sql_revision, workerCas.new_revision);
    assert.equal(policy.native_sql_matches_governance, true);
    assert.equal(policy.original_intent_id, native.opened_intent_id);
    assert.equal(policy.replayed_intent_id, native.opened_intent_id);
    assert.equal(policy.original_request_hash, policy.persisted_request_hash);
    assert.equal(policy.original_deadline_ms, policy.persisted_deadline_ms);
    assert.equal(policy.new_key_reason, "ACCOUNT_NEW_RISK_PAUSED");
    assert.equal(policy.posts_before, 1);
    assert.equal(policy.posts_after, 1);
  }
  assert.equal(native.opened_intent_id, native.replayed_intent_id);
  assert.equal(native.opened_intent_id, native.sql_intent_id);
  assert.equal(native.sql_state, "ACKNOWLEDGED");
  assert.match(native.sql_request_hash,/^[0-9a-f]{64}$/);
  assert.match(native.sql_plan_hash,/^[0-9a-f]{64}$/);
  assert.ok(Number.isSafeInteger(native.sql_deadline_ms)&&native.sql_deadline_ms>0);
  assert.ok(Number.isSafeInteger(native.sql_policy_revision)&&native.sql_policy_revision>0);
  assert.ok(Number.isSafeInteger(native.sql_revision) && native.sql_revision > 0);
  assert.equal(native.created_posts, 1);
  assert.equal(native.semantic_decimal_equivalent_replay, true,
    "S2-04 Native/Worker must reuse decimal-semantic intent");
  assert.equal(native.semantic_amount_conflict_reason, "IDEMPOTENCY_CONFLICT");
  assert.equal(native.semantic_side_conflict_reason, "IDEMPOTENCY_CONFLICT");
  assert.equal(native.semantic_reference_conflict_reason, "IDEMPOTENCY_CONFLICT");
  assert.equal(native.semantic_basis_conflict_reason, "IDEMPOTENCY_CONFLICT");
  assert.equal(native.semantic_deadline_conflict_reason, "IDEMPOTENCY_CONFLICT");
  assert.equal(native.reserved_entry_usd_before_replay, "5.0005");
  assert.equal(native.reserved_entry_usd_after_replay,
    native.reserved_entry_usd_before_replay);
  assert.equal(worker.budget.entry_budget.reserved_usd,
    native.reserved_entry_usd_after_replay);
  assert.equal(worker.budget.entry_budget.consumed_usd, "0",
    "unsettled ACK must never become an invented financial debit");
  assert.ok(Number.isSafeInteger(worker.connection_generation)
    && worker.connection_generation > 0);
  assert.ok(Number.isSafeInteger(worker.risk_book_sequence)
    && worker.risk_book_sequence >= 0,
    "Worker risk-book revision must come from its real SQL-backed SUMMARY");
  return {
    case_id:caseId, status:"PASS",
    provenance:{schema:"okx.native-workerd-transcript/v1",sha256:transcript_sha256},
    assertions:[
      { id:"identical_intent_from_independent_sql_readback",
        expected:native.opened_intent_id, actual:native.sql_intent_id },
      { id:"identical_replay_intent",
        expected:native.opened_intent_id, actual:native.replayed_intent_id },
      { id:"one_possible_create_post", expected:"1", actual:String(native.created_posts) },
      { id:"s2_07_independent_uid_scope", expected:"true",
        actual:String(scope.foreign_sql_intent_absent) },
      { id:"s2_07_owner_identity", expected:native.sql_request_hash,
        actual:scope.owner_semantic_sha256 },
      { id:"s2_04_equivalent_decimal_replay", expected:"true",
        actual:String(native.semantic_decimal_equivalent_replay) },
      { id:"s2_04_different_amount_conflict", expected:"IDEMPOTENCY_CONFLICT",
        actual:native.semantic_amount_conflict_reason },
      { id:"s2_04_different_side_conflict", expected:"IDEMPOTENCY_CONFLICT",
        actual:native.semantic_side_conflict_reason },
      { id:"s2_04_changed_reference_conflict", expected:"IDEMPOTENCY_CONFLICT",
        actual:native.semantic_reference_conflict_reason },
      { id:"s2_04_changed_stop_basis_conflict", expected:"IDEMPOTENCY_CONFLICT",
        actual:native.semantic_basis_conflict_reason },
      { id:"s2_04_changed_expiry_conflict", expected:"IDEMPOTENCY_CONFLICT",
        actual:native.semantic_deadline_conflict_reason },
      { id:"no_second_risk_reserve", expected:"5.0005",
        actual:native.reserved_entry_usd_after_replay },
      { id:"worker_cross_owner_reserved_entry", expected:"5.0005",
        actual:worker.budget.entry_budget.reserved_usd },
    ],
    events:[
      { stage:"public_trade_entered", owner:"TradeService+AccountStore",
        source_digest:hash({intent:native.opened_intent_id,sql_revision:native.sql_revision}) },
      { stage:"idempotent_replay_verified", owner:"TradeService+RuntimeSession",
        source_digest:hash({native,worker}) },
    ],
    readbacks:{
      sql:[{revision:native.sql_revision,digest:hash(native)}],
      risk_book:[{revision:worker.risk_book_sequence,digest:hash(worker.budget)}],
    },
    outbound_sends:[{
      account:native.account_uid,intent:native.opened_intent_id,
      operation_kind:"create",count:native.created_posts,
    }],
  };
}
