// Validates the kill boundary only; no process is terminated by these tests.
import test from "node:test";
import assert from "node:assert/strict";
import {join} from "node:path";
import {tmpdir} from "node:os";
import {validateF2KillBoundary} from "./t10-11-native-crash.mjs";
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
