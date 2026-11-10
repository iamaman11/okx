// Validates the kill boundary only; no process is terminated by these tests.
import test from "node:test";
import assert from "node:assert/strict";
import {join} from "node:path";
import {tmpdir} from "node:os";
import {validateUnsentKillBoundary,nativeCrashFailure,awaitCommitBoundary} from "./t10-11-native-crash.mjs";
const boundary=()=>({schema:"okx.t10-11-native-unsent-kill-boundary/v1",phase:"F2",
  pid:process.pid+1000,root:join(tmpdir(),"okx-raw-native-session-"+"a".repeat(32)),
  command:{command_id:"reserve_t11_f2",action:{action:"RESERVE",intent_id:"original",plan_sha256:"plan"}},
  worker_result:{status:"HELD",reservation_key:"original-key"},
  sql:{state:"PREPARED",intent_present:true,central_receipt:null,central_release:null,exchange_order_id:null,
    intent_id:"original",plan_hash:"plan",reservation_key:"original-key"},venue_posts:0,source_floor_ms:1});

function beforeReserveBoundary(phase){
  const value=boundary();value.phase=phase;
  value.sql={...value.sql,intent_present:phase==="F1",state:phase==="F1"?"PREPARED":null,
    plan_hash:phase==="F1"?"plan":null,request_hash:phase==="F1"?"request":null,
    reserve_attempt:null,terminal_receipt:null,reservation_key:null};
  value.command.action={action:"INSPECT",reservation_key:"candidate-key",plan_sha256:"plan"};
  value.candidate_reservation_key="candidate-key";
  value.worker_result={status:"DENIED",reason:"RISK_RESERVATION_NOT_FOUND",reservation_key:null};
  value.prepare_input={intent:{intent_id:"original",plan_hash:"plan",request_hash:"request"}};
  value.before_sequence=1;
  return value;
}
for(const phase of ["F0","F1"]){
  test(`${phase} binds absent reserve to the exact original SQL and candidate identity`,()=>{
    const value=beforeReserveBoundary(phase);
    assert.equal(validateUnsentKillBoundary(value),value);
    for(const change of [x=>x.sql.reserve_attempt={},x=>x.sql.central_receipt={},
      x=>x.sql.reservation_key="invented",x=>x.sql.intent_present=!x.sql.intent_present,
      x=>x.sql.terminal_receipt={},x=>x.worker_result.reason="AUTHORITY_UNAVAILABLE",
      x=>x.command.action.reservation_key="other",x=>x.prepare_input.intent.intent_id="other",
      x=>x.command.action.plan_sha256="other",x=>x.venue_posts=1]){
      const invalid=beforeReserveBoundary(phase);change(invalid);
      assert.throws(()=>validateUnsentKillBoundary(invalid));
    }
  });
}

function laterBoundary(phase){
  const value=boundary();value.phase=phase;
  value.sql.state=phase==="F8"?"REJECTED":"RESERVED";
  value.sql.central_receipt={reservation_key:"original-key",intent_id:"original",plan_hash:"plan"};
  value.command.action={action:{F3:"INSPECT",F4:"BEGIN_POST",F8:"RELEASE_UNSENT"}[phase],
    reservation_key:"original-key",plan_sha256:"plan"};
  value.worker_result.status={F3:"OBSERVED",F4:"POST_AUTHORIZED",F8:"UPDATED"}[phase];
  if(phase==="F3")value.worker_result.reservation_state="HELD";
  if(phase==="F8"){
    value.sql.terminal_receipt={kind:"REJECTED_UNSENT"};
    value.command.action.proof={reservation_key:"original-key",intent_id:"original",plan_hash:"plan"};
  }
  return value;
}
for(const phase of ["F3","F4","F8"]){
  test(`${phase} requires its committed Worker result and original unclaimed SQL`,()=>{
    const value=laterBoundary(phase);
    assert.equal(validateUnsentKillBoundary(value),value);
    for(const change of [x=>x.phase="F5",x=>x.sql.state="POSTING",
      x=>x.worker_result.status="DENIED",x=>x.sql.central_release={},
      x=>x.sql.central_receipt.intent_id="other",x=>x.sql.central_receipt.plan_hash="other",
      x=>x.command.action.reservation_key="other",x=>x.venue_posts=1]){
      const invalid=laterBoundary(phase);change(invalid);
      assert.throws(()=>validateUnsentKillBoundary(invalid));
    }
  });
}
test("F5 requires a committed SQL claim and unreleased actual Worker reserve",()=>{
  function claimed(){
    const value=laterBoundary("F3");value.phase="F5";
    value.sql.state="POSTING";value.sql.post_fence=[123,"POSTED"];
    value.sql.terminal_receipt=null;value.worker_result.reservation_state="POSTED";
    value.claimed_budget={entry_budget:{reserved_usd:"3.0005"}};
    return value;
  }
  const value=claimed();assert.equal(validateUnsentKillBoundary(value),value);
  for(const change of [x=>x.sql.state="RESERVED",x=>x.sql.state="UNKNOWN",
    x=>x.sql.post_fence=[0,"POSTED"],x=>x.sql.post_fence=[123,"HELD"],
    x=>x.sql.terminal_receipt={},x=>x.sql.central_release={},x=>x.venue_posts=1,
    x=>x.worker_result.reservation_state="HELD",x=>x.worker_result.reservation_state="RELEASED",
    x=>x.sql.central_receipt.intent_id="other",x=>x.command.action.reservation_key="other",
    x=>x.claimed_budget=null]){
    const invalid=claimed();change(invalid);assert.throws(()=>validateUnsentKillBoundary(invalid));
  }
});
test("F3 requires HELD rather than an already authorized or released reservation",()=>{
  for(const state of ["POSTED","RELEASED",null]){
    const value=laterBoundary("F3");value.worker_result.reservation_state=state;
    assert.throws(()=>validateUnsentKillBoundary(value));
  }
});

test("F6 requires one actual send with lost ACK and an unreleased SQL claim",()=>{
  function sent(){
    const value=laterBoundary("F3");value.phase="F6";
    value.sql.state="POSTING";value.sql.post_fence=[123,"POSTED"];
    value.sql.terminal_receipt=null;value.worker_result.reservation_state="POSTED";
    value.claimed_budget={entry_budget:{reserved_usd:"3.0005"}};
    value.venue_posts=1;value.venue_ack_lost=true;
    return value;
  }
  const value=sent();assert.equal(validateUnsentKillBoundary(value),value);
  for(const change of [x=>x.venue_posts=0,x=>x.venue_posts=2,
    x=>x.venue_ack_lost=false,x=>delete x.venue_ack_lost,
    x=>x.sql.state="RESERVED",x=>x.sql.state="FILLED",x=>x.sql.exchange_order_id="123456",
    x=>x.sql.central_release={},x=>x.sql.terminal_receipt={},
    x=>x.worker_result.reservation_state="RELEASED",x=>x.sql.post_fence=[0,"POSTED"]]){
    const invalid=sent();change(invalid);assert.throws(()=>validateUnsentKillBoundary(invalid));
  }
});
test("F8 cannot replace durable no-send with a terminal or foreign proof",()=>{
  for(const change of [x=>x.sql.terminal_receipt.kind="FILLED",
    x=>x.command.action.proof.intent_id="other",x=>x.command.action.proof.plan_hash="other"]){
    const value=laterBoundary("F8");change(value);
    assert.throws(()=>validateUnsentKillBoundary(value));
  }
});

test("F7 requires persisted raw facts and a pending projection, not an already projected lot",()=>{
  function pending(){
    const value=laterBoundary("F3");value.phase="F7";
    value.sql.state="FILLED";value.sql.exchange_order_id="123456";value.sql.post_fence=null;
    value.sql.terminal_receipt={kind:"TERMINAL",result_ref:"authenticated-order-readback"};
    value.worker_result.reservation_state="POSTED";
    value.claimed_budget={entry_budget:{reserved_usd:"3.0005"}};
    value.venue_posts=1;value.venue_ack_lost=false;
    value.sql_path=join(value.root,"accounts","original","account.sqlite");
    value.projection_boundary={counts:[1,1,0,1],fee:"-0.0005",fills:["fill"],bills:["bill"],fees:["fee"],lots:[]};
    return value;
  }
  const value=pending();assert.equal(validateUnsentKillBoundary(value),value);
  for(const change of [x=>x.projection_boundary.counts=[1,0,1,1],
    x=>x.projection_boundary.counts=[0,0,0,0],x=>x.projection_boundary.fee="0",
    x=>x.projection_boundary.bills=[],x=>x.projection_boundary.fees=["fee","duplicate"],
    x=>x.projection_boundary.lots=["already-projected"],x=>x.venue_posts=0,x=>x.venue_posts=2,
    x=>x.venue_ack_lost=true,x=>x.sql.central_release={},x=>x.sql.exchange_order_id=null,
    x=>x.sql.state="UNKNOWN",x=>x.worker_result.reservation_state="RELEASED",
    x=>x.sql_path=join(tmpdir(),"unrelated","account.sqlite"),
    x=>x.sql.terminal_receipt.result_ref="invented"]){
    const invalid=pending();change(invalid);assert.throws(()=>validateUnsentKillBoundary(invalid));
  }
});
test("F2 kill boundary binds the unacknowledged SQL intent to actual Worker HELD",()=>{
  const value=boundary();assert.equal(validateUnsentKillBoundary(value),value);
});
test("unsafe or already advanced F2 boundaries are rejected before taskkill",()=>{
  for(const change of [
    x=>x.pid=process.pid,x=>x.pid=-1,x=>x.root=tmpdir(),
    x=>x.root=join(tmpdir(),"..","okx-raw-native-session-"+"a".repeat(32)),
    x=>x.sql.state="RESERVED",x=>x.sql.central_receipt={},
    x=>x.sql.exchange_order_id="already-sent",x=>x.venue_posts=1,
    x=>x.sql.reservation_key="other",x=>x.command.action.intent_id="other",
    x=>x.command.action.plan_sha256="other",x=>x.worker_result.status="DENIED",
  ]){const value=boundary();change(value);assert.throws(()=>validateUnsentKillBoundary(value));}
});
test("an early Native failure retains its first Rust boundary before Node wraps it",()=>{
  const failure=nativeCrashFailure({stderr:
    "thread 'native_crash_unsent_boundary' panicked at crates/okx-agent/src/trade_integration_t10_11.rs:70:4:\n"+
    "F2_STORE_NOT_READY\n"});
  assert.equal(failure.rust_assertion,
    "crates/okx-agent/src/trade_integration_t10_11.rs:70:4: F2_STORE_NOT_READY");
  assert.equal(failure.rust_failures.count,1);
});

test("non-panic process and Node assertion failures retain a bounded cause",()=>{
  const stack=nativeCrashFailure({code:3221225725,stderr:"process exited with stack overflow"});
  assert.equal(stack.windows_stack_overflow,true);
  assert.equal(stack.exit_code,"3221225725");
  assert.equal(stack.rust_assertion,"NATIVE_STACK_OVERFLOW");
  const node=nativeCrashFailure({code:"ERR_ASSERTION",stack:"at validate (file:///private/t10-11-native-crash.mjs:99:3)"});
  assert.equal(node.node_assertion_line,99);
  assert.equal(node.rust_assertion,"NATIVE_BOUNDARY_ASSERTION_LINE_99");
  assert.equal(nativeCrashFailure({code:101}).rust_assertion,"NATIVE_EXIT_CODE_101");
  assert.ok(!JSON.stringify(nativeCrashFailure({code:"secret-value",stderr:"token=private"})).includes("private"));
});
test("intentional kill after the committed boundary is not reported as early failure",async()=>{
  let killed;const processExit=new Promise((_,reject)=>{killed=reject;});
  let reports=0;
  assert.equal(await awaitCommitBoundary(Promise.resolve("committed"),processExit,
    error=>{reports++;throw error;}),"committed");
  killed(Error("intentional OS kill"));
  await new Promise(resolve=>setImmediate(resolve));
  assert.equal(reports,0);
});
test("Native failure before the boundary is reported once and stops the wait",async()=>{
  let reports=0;
  await assert.rejects(awaitCommitBoundary(new Promise(()=>{}),Promise.reject(Error("early")),
    error=>{reports++;throw error;}),/early/);
  assert.equal(reports,1);
});
