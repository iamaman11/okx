// OS lifecycle only. The existing Native/Worker owners make every SQL/risk
// decision. No replacement RiskBook, exchange or recovery algorithm lives here.
import assert from "node:assert/strict";
import {execFile} from "node:child_process";
import {promisify} from "node:util";
import {readFile} from "node:fs/promises";
import {setTimeout as delay} from "node:timers/promises";
import {basename,resolve,relative,isAbsolute} from "node:path";
import {tmpdir} from "node:os";
import {summarizeRustPanics} from "../../../scripts/trade-integration/native-failure.mjs";
const exec=promisify(execFile);

export function nativeCrashFailure(error){
  const rust_failures=summarizeRustPanics(error);
  const first=rust_failures.panics[0];
  const code=String(error?.code??"UNKNOWN");
  const exit_code=/^(?:-?\d{1,12}|ERR_ASSERTION|ENOENT|ETIMEDOUT)$/.test(code)?code:"UNKNOWN";
  const windows_stack_overflow=code==="3221225725"||code==="-1073741571"
    || /stack overflow|has overflowed its stack/i.test(String(error?.stderr??""));
  const frame=String(error?.stack??"").match(/t10-11-native-crash\.mjs:(\d{1,5}):\d{1,5}/);
  const node_assertion_line=code==="ERR_ASSERTION"&&frame?Number(frame[1]):null;
  const reason=windows_stack_overflow?"NATIVE_STACK_OVERFLOW"
    :node_assertion_line?`NATIVE_BOUNDARY_ASSERTION_LINE_${node_assertion_line}`
    :`NATIVE_EXIT_CODE_${exit_code}`;
  return {schema:"okx.t09-native-child-failure/v2",rust_failures,exit_code,
    windows_stack_overflow,node_assertion_line,
    rust_assertion:first?`${first.location}: ${first.reason}`:reason};
}
function failNative(error){
  process.stderr.write("T09_1_NATIVE_CHILD_FAILURE_V2:"+JSON.stringify(nativeCrashFailure(error))+"\n");
  throw error;
}

export function awaitCommitBoundary(ready,nativeProcess,onFailure=failNative){
  // Only the winner may report failure. The losing process promise will reject
  // later when we intentionally kill Native; that is not an early failure.
  return Promise.race([ready,nativeProcess.then(()=>{
    throw Error("T10_11_NATIVE_EXITED_WITHOUT_OS_KILL");
  })]).catch(onFailure);
}

export function validateUnsentKillBoundary(value){
  assert.equal(value.schema,"okx.t10-11-native-unsent-kill-boundary/v1");
  assert.ok(["F0","F1","F2","F3","F4","F5","F6","F7","F8"].includes(value.phase));
  assert.ok(Number.isSafeInteger(value.pid)&&value.pid>0&&value.pid!==process.pid);
  const root=resolve(value.root),within=relative(resolve(tmpdir()),root);
  assert.ok(within&&!within.startsWith("..")&&!isAbsolute(within));
  assert.match(basename(root),/^okx-raw-native-session-[a-f0-9]{32}$/);
  const action=value.command.action;
  assert.equal(value.venue_posts,["F6","F7"].includes(value.phase)?1:0);
  if(value.phase==="F6")assert.equal(value.venue_ack_lost,true);
  assert.equal(value.sql.exchange_order_id,value.phase==="F7"?"123456":null);
  assert.equal(value.sql.central_release,null);
  assert.ok(Number.isSafeInteger(value.source_floor_ms)&&value.source_floor_ms>0);
  if(["F0","F1"].includes(value.phase)){
    assert.equal(action.action,"INSPECT");
    assert.equal(value.worker_result.status,"DENIED");
    assert.equal(value.worker_result.reason,"RISK_RESERVATION_NOT_FOUND");
    assert.equal(value.worker_result.reservation_key,null);
    assert.equal(value.sql.intent_present,value.phase==="F1");
    assert.equal(value.sql.state,value.phase==="F1"?"PREPARED":null);
    for(const key of ["central_receipt","reserve_attempt","reservation_key","terminal_receipt"])
      assert.equal(value.sql[key],null,`pre-reserve ${key} must be absent`);
    const input=value.prepare_input.intent;
    assert.equal(value.sql.intent_id,input.intent_id);
    assert.equal(action.plan_sha256,input.plan_hash);
    assert.equal(action.reservation_key,value.candidate_reservation_key);
    assert.ok(Number.isSafeInteger(value.before_sequence));
    assert.equal(value.sql.plan_hash,value.phase==="F1"?input.plan_hash:null);
    assert.equal(value.sql.request_hash,value.phase==="F1"?input.request_hash:null);
    return value;
  }
  assert.equal(value.sql.intent_present,true);
  const expected={F2:["RESERVE","HELD","PREPARED"],
    F3:["INSPECT","OBSERVED","RESERVED"],
    F4:["BEGIN_POST","POST_AUTHORIZED","RESERVED"],
    F5:["INSPECT","OBSERVED","POSTING"],
    F6:["INSPECT","OBSERVED","POSTING"],
    F7:["INSPECT","OBSERVED","FILLED"],
    F8:["RELEASE_UNSENT","UPDATED","REJECTED"]}[value.phase];
  assert.equal(action.action,expected[0]);
  assert.equal(value.worker_result.status,expected[1]);
  assert.equal(value.sql.state,expected[2]);
  if(value.phase==="F3")assert.equal(value.worker_result.reservation_state,"HELD");
  if(["F5","F6"].includes(value.phase)){
    assert.equal(value.worker_result.reservation_state,"POSTED");
    assert.ok(Number.isSafeInteger(value.sql.post_fence[0])&&value.sql.post_fence[0]>0);
    assert.equal(value.sql.post_fence[1],"POSTED");
    assert.equal(value.sql.terminal_receipt,null);
    assert.ok(value.claimed_budget?.entry_budget);
  }
  assert.equal(value.sql.central_release,null,"local release ACK must still be missing");
  if(value.phase==="F2"){
    assert.equal(value.sql.central_receipt,null);
    assert.equal(value.sql.intent_id,action.intent_id);
  }else{
    assert.equal(value.sql.reservation_key,action.reservation_key);
    assert.equal(value.sql.central_receipt.reservation_key,value.sql.reservation_key);
    assert.equal(value.sql.central_receipt.intent_id,value.sql.intent_id);
    assert.equal(value.sql.central_receipt.plan_hash,value.sql.plan_hash);
  }
  if(value.phase==="F7"){
    assert.equal(value.worker_result.reservation_state,"POSTED");
    assert.equal(value.sql.terminal_receipt.kind,"TERMINAL");
    assert.equal(value.sql.terminal_receipt.result_ref,"authenticated-order-readback");
    assert.equal(value.sql.post_fence,null);
    assert.equal(value.venue_ack_lost,false);
    assert.ok(value.claimed_budget?.entry_budget);
    assert.deepEqual(value.projection_boundary.counts,[1,1,0,1]);
    assert.equal(value.projection_boundary.fee,"-0.0005");
    for(const key of ["fills","bills","fees"])assert.equal(value.projection_boundary[key].length,1);
    assert.deepEqual(value.projection_boundary.lots,[]);
    const sql=relative(resolve(value.root,"accounts"),resolve(value.sql_path));
    assert.ok(sql&&!sql.startsWith("..")&&!isAbsolute(sql));
    assert.equal(basename(value.sql_path),"account.sqlite");
  }
  if(value.phase==="F8"){
    assert.equal(value.sql.terminal_receipt.kind,"REJECTED_UNSENT");
    assert.equal(action.proof.intent_id,value.sql.intent_id);
    assert.equal(action.proof.reservation_key,value.sql.reservation_key);
    assert.equal(action.proof.plan_hash,value.sql.plan_hash);
  }
  assert.equal(value.sql.exchange_order_id,value.phase==="F7"?"123456":null);
  assert.equal(value.sql.plan_hash,action.plan_sha256);
  assert.equal(value.sql.reservation_key,value.worker_result.reservation_key);
  assert.equal(value.venue_posts,["F6","F7"].includes(value.phase)?1:0);
  assert.ok(Number.isSafeInteger(value.source_floor_ms)&&value.source_floor_ms>0);
  return value;
}

export async function killAndRecoverUnsentNative({nativeProcess,handoffPath,cwd}){
  const waiting=new AbortController();
  const ready=(async()=>{
    const until=Date.now()+60_000;
    while(Date.now()<until){
      try{return validateUnsentKillBoundary(JSON.parse(await readFile(
        handoffPath+".t11-native-kill-ready","utf8")));}
      catch(error){if(error.code!=="ENOENT")throw error;}
      await delay(25,undefined,{signal:waiting.signal});
    }
    throw Error("T10_11_UNSENT_COMMIT_BOUNDARY_TIMEOUT");
  })();
  const boundary=await awaitCommitBoundary(ready,nativeProcess)
    .finally(()=>waiting.abort());
  assert.equal(boundary.phase,process.env.OKX_T10_11_NATIVE_CRASH);
  if(process.platform==="win32"){
    await exec("taskkill",["/PID",String(boundary.pid),"/F"],{
      windowsHide:true,timeout:15_000,
    });
  }else process.kill(boundary.pid,"SIGKILL");
  const termination=await nativeProcess.then(
    ()=>{throw Error("T10_11_NATIVE_EXITED_SUCCESSFULLY_AFTER_KILL");},
    error=>({code:error.code,signal:error.signal??null}),
  );
  assert.notEqual(termination.code,0);
  const recovered=await exec("cargo",[
    "test","--locked","-p","okx-agent","--lib",
    "cloudflare_transport::raw_native_session_tests::native_crash_unsent_recover",
    "--","--ignored","--exact","--nocapture",
  ],{cwd,env:{...process.env,RUST_MIN_STACK:"16777216",
      OKX_INTEGRATION_HANDOFF_PATH:handoffPath},
    timeout:90_000,maxBuffer:4*1024*1024,windowsHide:true}).catch(failNative);
  const prefix="T10_11_NATIVE_UNSENT_RECOVERY_V1:";
  const lines=recovered.stdout.split(/\r?\n/).filter(x=>x.startsWith(prefix));
  assert.equal(lines.length,1,"one genuine Native recovery receipt required");
  const native=JSON.parse(lines[0].slice(prefix.length));
  assert.equal(native.phase,boundary.phase);
  assert.notEqual(native.pid,boundary.pid,"a new OS process must own recovery");
  assert.equal(native.root,boundary.root);
  assert.deepEqual(native.before,["F5","F6"].includes(boundary.phase)
    ?{...boundary.sql,state:"UNKNOWN",post_fence:null}:boundary.sql);
  assert.equal(native.original_sql_reopened,true);
  assert.equal(["F6","F7"].includes(boundary.phase)?native.production_recovery_replayed:native.public_recovery_replayed,true);
  assert.equal(native.after.reservation_key,boundary.sql.reservation_key);
  if(["F0","F1"].includes(boundary.phase)){
    assert.equal(native.after.central_release,null);
    assert.equal(native.after.reserve_attempt,null);
    assert.equal(native.after.terminal_receipt.kind,"REJECTED_UNSENT");
    assert.deepEqual(native.recovery_result.data.central_release,
      {state:"not_required",proof:"PRE_RESERVE_SQL"});
    assert.equal(native.worker_after.risk_book_sequence,boundary.before_sequence);
    if(boundary.phase==="F0"){
      assert.deepEqual(native.prepare_replay.first,native.prepare_replay.replay);
      assert.equal(native.prepare_replay.first.intent_id,boundary.sql.intent_id);
      assert.equal(native.prepare_replay.first.state,"PREPARED");
    }else{
      assert.equal(native.recovery_request.action,"status");
      assert.equal(native.recovery_result.data.primary_reason,"COMMAND_EXPIRED");
      assert.equal(native.after.terminal_receipt.result_ref,"COMMAND_EXPIRED");
    }
  }else if(boundary.phase==="F5"){
    assert.deepEqual(native.after,native.before,"unresolved Status cannot invent a terminal outcome");
    assert.equal(native.after.central_release,null);
    assert.equal(native.after.terminal_receipt,null);
    assert.equal(native.claim_replay.claimed,false);
    assert.equal(native.claim_replay.no_send_receipt,false);
    assert.ok(native.claim_replay.pending_intent_ids.includes(boundary.sql.intent_id));
    assert.equal(native.recovery_request.action,"status");
    assert.equal(native.recovery_result.status,"unknown");
    assert.equal(native.recovery_result.reason_code,"ORDER_READBACK_INCOMPLETE_NO_RESUBMIT");
    assert.equal(native.order_reads.length,2);
    assert.ok(native.order_reads.every(r=>r.startsWith("fixture-executor-a:/api/v5/trade/order?")));
  }else if(boundary.phase==="F6"){
    assert.equal(native.after.state,"FILLED");
    assert.equal(native.after.exchange_order_id,"123456");
    assert.equal(native.after.central_release,null);
    assert.equal(native.after.terminal_receipt.kind,"TERMINAL");
    assert.equal(native.after.terminal_receipt.result_ref,"authenticated-order-readback");
    assert.deepEqual(native.after.central_receipt,boundary.sql.central_receipt);
    assert.equal(native.recovered.order_id,"123456");
    assert.equal(native.recovered.send_claim_replays,2);
    assert.equal(native.recovered.order_readbacks,2);
    assert.equal(native.recovered.capture_turns,2);
    const economics=native.recovered.economics;
    assert.equal(economics.length,2);
    assert.deepEqual(economics[0],economics[1]);
    assert.equal(economics[0].fill_count,1);
    assert.equal(economics[0].bill_count,1);
    assert.equal(economics[0].quantity,"0.1");
    assert.equal(economics[0].verified_fee,"0.0005");
    assert.equal(economics[0].lot_count,1);
    assert.equal(economics[0].lot_quantity,"0.1");
    assert.deepEqual(economics[0].totals.filter(x=>x.currency==="USDT"&&x.component==="FEE"),
      [{currency:"USDT",component:"FEE",amount:"-0.0005"}]);
  }else if(boundary.phase==="F7"){
    assert.deepEqual(native.after,native.before);
    assert.equal(native.after.state,"FILLED");
    assert.equal(native.after.central_release,null);
    const p=native.recovered;
    assert.deepEqual(p.before,boundary.projection_boundary);
    assert.deepEqual(p.after.counts,[1,0,1,1]);
    for(const key of ["fills","bills","fees","fee"])assert.deepEqual(p.before[key],p.after[key]);
    assert.deepEqual(p.projections.map(x=>x.projected),[1,0]);
    assert.ok(p.projections.every(x=>x.lot_quantity==="0.1"));
    assert.equal(p.projections[0].finance_revision,p.projections[1].finance_revision);
    assert.equal(p.original_sql_owner_closed_before_reconnect,true);
  }else assert.equal(native.after.central_release.kind,"CENTRAL_RELEASE");
  assert.equal(native.venue_posts,0);
  assert.deepEqual(native.worker_after.budget,{...(["F5","F6","F7"].includes(boundary.phase)?boundary.claimed_budget:boundary.before_budget),
    source_as_of_ms:boundary.source_floor_ms});
  if(boundary.phase==="F4")assert.equal(native.old_fence_denial.status,"DENIED");
  if(["F5","F6","F7","F8"].includes(boundary.phase)){
    assert.equal(native.worker_after.risk_book_sequence,boundary.worker_result.risk_book_sequence);
    assert.equal(native.worker_before.risk_book_sequence,native.worker_after.risk_book_sequence);
    assert.deepEqual(native.worker_before.budget,native.worker_after.budget);
  }
  return {schema:"okx.t10-11-native-unsent-process-crash/v1",
    phase:boundary.phase,native_os_kills:1,boundary,termination,native,
    input_scope:"direct SQL prepare/reserve and explicit census/causal fixture inputs; actual Native WSS and Worker; public Cancel/Status for unsent; production submission/order observation/source capture for F6/F7, pending-fill projection through the original SQL owner for F7",
    full_t10_11_pass:false};
}
