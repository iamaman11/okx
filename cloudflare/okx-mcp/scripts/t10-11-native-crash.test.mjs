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
  sql:{state:"PREPARED",central_receipt:null,central_release:null,exchange_order_id:null,
    intent_id:"original",plan_hash:"plan",reservation_key:"original-key"},venue_posts:0,source_floor_ms:1});

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
test("F3 requires HELD rather than an already authorized or released reservation",()=>{
  for(const state of ["POSTED","RELEASED",null]){
    const value=laterBoundary("F3");value.worker_result.reservation_state=state;
    assert.throws(()=>validateUnsentKillBoundary(value));
  }
});
test("F8 cannot replace durable no-send with a terminal or foreign proof",()=>{
  for(const change of [x=>x.sql.terminal_receipt.kind="FILLED",
    x=>x.command.action.proof.intent_id="other",x=>x.command.action.proof.plan_hash="other"]){
    const value=laterBoundary("F8");change(value);
    assert.throws(()=>validateUnsentKillBoundary(value));
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
