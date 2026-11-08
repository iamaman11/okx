// Validates the kill boundary only; no process is terminated by these tests.
import test from "node:test";
import assert from "node:assert/strict";
import {join} from "node:path";
import {tmpdir} from "node:os";
import {validateF2KillBoundary,nativeF2Failure,awaitF2CommitBoundary} from "./t10-11-native-crash.mjs";
const boundary=()=>({schema:"okx.t10-11-native-f2-kill-boundary/v1",phase:"F2",
  pid:process.pid+1000,root:join(tmpdir(),"okx-raw-native-session-"+"a".repeat(32)),
  command:{command_id:"reserve_t11_f2",action:{action:"RESERVE",intent_id:"original",plan_sha256:"plan"}},
  worker_held:{status:"HELD",reservation_key:"original-key"},
  sql:{state:"PREPARED",central_receipt:null,exchange_order_id:null,
    intent_id:"original",plan_hash:"plan",reservation_key:"original-key"},venue_posts:0});
test("F2 kill boundary binds the unacknowledged SQL intent to actual Worker HELD",()=>{
  const value=boundary();assert.equal(validateF2KillBoundary(value),value);
});
test("unsafe or already advanced F2 boundaries are rejected before taskkill",()=>{
  for(const change of [
    x=>x.pid=process.pid,x=>x.pid=-1,x=>x.root=tmpdir(),
    x=>x.root=join(tmpdir(),"..","okx-raw-native-session-"+"a".repeat(32)),
    x=>x.sql.state="RESERVED",x=>x.sql.central_receipt={},
    x=>x.sql.exchange_order_id="already-sent",x=>x.venue_posts=1,
    x=>x.sql.reservation_key="other",x=>x.command.action.intent_id="other",
    x=>x.command.action.plan_sha256="other",x=>x.worker_held.status="DENIED",
  ]){const value=boundary();change(value);assert.throws(()=>validateF2KillBoundary(value));}
});
test("an early Native failure retains its first Rust boundary before Node wraps it",()=>{
  const failure=nativeF2Failure({stderr:
    "thread 'native_crash_f2_boundary' panicked at crates/okx-agent/src/trade_integration_t10_11.rs:70:4:\n"+
    "F2_STORE_NOT_READY\n"});
  assert.equal(failure.rust_assertion,
    "crates/okx-agent/src/trade_integration_t10_11.rs:70:4: F2_STORE_NOT_READY");
  assert.equal(failure.rust_failures.count,1);
});
test("intentional kill after the committed boundary is not reported as early failure",async()=>{
  let killed;const processExit=new Promise((_,reject)=>{killed=reject;});
  let reports=0;
  assert.equal(await awaitF2CommitBoundary(Promise.resolve("committed"),processExit,
    error=>{reports++;throw error;}),"committed");
  killed(Error("intentional OS kill"));
  await new Promise(resolve=>setImmediate(resolve));
  assert.equal(reports,0);
});
test("Native failure before the boundary is reported once and stops the wait",async()=>{
  let reports=0;
  await assert.rejects(awaitF2CommitBoundary(new Promise(()=>{}),Promise.reject(Error("early")),
    error=>{reports++;throw error;}),/early/);
  assert.equal(reports,1);
});
