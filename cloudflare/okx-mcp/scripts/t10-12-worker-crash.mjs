// Worker component of T10-12. Authenticated WSS inputs here are scripted;
// these receipts do not certify a Native SQL account or venue settlement.
import assert from "node:assert/strict";

export async function settleBeforeWorkerCrash({socket,risk,summary,
  reservation,request,census,accountKeys,policyRevision}) {
  const committed=await summary(socket,"t10_12_before_begin_post");
  const afterHeld=Date.now();
  const posted=await risk(socket,"t10_12_begin_post_before_crash",{
    action:"BEGIN_POST",reservation_key:reservation.reservation_key,
    plan_sha256:request.plan_sha256,policy_revision:policyRevision,
    expected_risk_book_sequence:committed.risk_book_sequence,
    source_proof_sha256:"f".repeat(64),
    census:{...census,source_digest:"e".repeat(64),
      generated_at_ms:afterHeld,source_read_floor_ms:afterHeld},
    causal_proof:{reserve_anchor:reservation.reservation_key,
      native_incarnation:"a".repeat(32),elapsed_to_proof_ms:30,
      account_reads:accountKeys.map(account_key=>({account_key,
        private_generation:7,source_read_id:"b".repeat(64),
        content_digest:census.content_digest,
        started_after_held_ms:5,completed_after_held_ms:20}))},
  });
  assert.equal(posted.status,"POST_AUTHORIZED");
  const action={action:"SETTLE_ENTRY",
    reservation_key:reservation.reservation_key,
    plan_sha256:request.plan_sha256,
    expected_risk_book_sequence:posted.risk_book_sequence,
    cumulative_entry_usd:"1.0005",settlement_revision:1,
    source_digest:"c".repeat(64)};
  const commandId="t10_12_settlement_reply_fenced_0001";
  socket.ws.send({type:"risk_control",schema:"okx.direct-transport.frame/v2",
    session_id:socket.hello.session_id,
    connection_generation:socket.hello.connection_generation,
    command:{schema:"okx.risk-command/v1",command_id:commandId,
      installation_id:"installation_test",action}});
  // A later independent production SUMMARY proves the settlement committed
  // while the integration-only send wrapper withholds its original response.
  const after=await summary(socket,"t10_12_after_fenced_settlement_commit");
  assert.equal(after.budget.entry_budget.consumed_usd,"1.0005");
  assert.equal(after.budget.entry_budget.reserved_usd,"6.9995");
  assert.equal(after.budget.entry_budget.remaining_usd,"2");
  assert.equal(socket.ws.has(frame=>
    frame.result?.command_id===commandId),false);
  return {summary:after,settlement_action:action,
    fenced_command_id:commandId,reservation_state:"POSTED"};
}

export async function recoverWorkerCrash({mf,proof,request,installation,
  session,summary,risk,frameSchema,configuration}) {
  assert.equal(proof.schema,"okx.t10-12-worker-pre-kill/v2");
  assert.ok(["reserve","settlement"].includes(proof.boundary));
  const installed=await request(mf,"/installation/initialize","POST",installation);
  assert.equal(installed.status,200);
  const recovered=await session(mf);
  assert.ok(recovered.hello.connection_generation>proof.connection_generation,
    "independent new process must advance persisted Durable Object generation");
  const before=await summary(recovered,"t10_12_recovered_risk");
  assert.equal(before.risk_book_sequence,proof.risk_book_sequence);
  assert.deepEqual(before.budget.entry_budget,proof.entry_budget,
    "Worker OS kill must preserve every persisted entry-budget field");
  const original=await risk(recovered,"t10_12_original_reservation_inspect",{
    action:"INSPECT",reservation_key:proof.reservation_key,
    plan_sha256:proof.plan_sha256,
  });
  assert.equal(original.status,"OBSERVED");
  assert.equal(original.reservation_state,proof.reservation_state);
  let settlementReplay=null;
  if(proof.boundary==="settlement") {
    assert.ok(proof.settlement_action);
    // Replay the exact original revision with its original stale CAS on the
    // NEW authenticated generation. It may acknowledge, never debit twice.
    const replay=await risk(recovered,"t10_12_settlement_replay_after_os_kill",
      proof.settlement_action);
    assert.equal(replay.status,"UPDATED");
    assert.equal(replay.risk_book_sequence,before.risk_book_sequence);
    const afterReplay=await summary(recovered,"t10_12_after_exact_settlement_replay");
    assert.equal(afterReplay.risk_book_sequence,before.risk_book_sequence);
    assert.deepEqual(afterReplay.budget.entry_budget,before.budget.entry_budget);
    const conflict=await risk(recovered,"t10_12_conflicting_revision_after_os_kill",{
      ...proof.settlement_action,cumulative_entry_usd:"1.0006",
    });
    assert.equal(conflict.status,"DENIED");
    assert.equal(conflict.reason,"ENTRY_SETTLEMENT_REVISION_CONFLICT");
    settlementReplay={status:replay.status,
      sequence_before:before.risk_book_sequence,
      sequence_after:afterReplay.risk_book_sequence,
      budget_before:before.budget.entry_budget,
      budget_after:afterReplay.budget.entry_budget,
      conflicting_revision_reason:conflict.reason};
  }
  const closed=recovered.ws.closed();
  recovered.ws.send({type:"risk_control",schema:frameSchema,
    session_id:proof.session_id,
    connection_generation:proof.connection_generation,
    command:{schema:"okx.risk-command/v1",
      command_id:"t10_12_old_session_summary_rejected",
      installation_id:configuration.installation_id,action:{action:"SUMMARY"}}});
  const staleCloseCode=await closed;
  assert.equal(staleCloseCode,1008);
  const current=await session(mf);
  const after=await summary(current,"t10_12_after_stale_session_denial");
  assert.equal(after.risk_book_sequence,before.risk_book_sequence);
  assert.deepEqual(after.budget.entry_budget,before.budget.entry_budget);
  const result={schema:"okx.t10-12-worker-process-recovery/v2",
    boundary:proof.boundary,persisted_do_reopened:true,
    generation_before:proof.connection_generation,
    generation_after:current.hello.connection_generation,
    sequence_before:proof.risk_book_sequence,
    sequence_after:after.risk_book_sequence,
    consumed_usd:after.budget.entry_budget.consumed_usd,
    reserved_usd:after.budget.entry_budget.reserved_usd,
    remaining_usd:after.budget.entry_budget.remaining_usd,
    original_state:original.reservation_state,
    original_reply_not_emitted:true,stale_frame_close_code:staleCloseCode,
    settlement_replay:settlementReplay,
    native_sql_account_proven:false,native_old_reply_rejection_proven:false};
  current.ws.close();
  return result;
}
