// Genuine Worker process-tree kills after RESERVE and SETTLE_ENTRY commits,
// before the selected original replies are emitted. Mandatory T10-12 remains
// blocked until the SAME Native SQL and Native old-reply witnesses are proved.
import assert from "node:assert/strict";
import {createHash} from "node:crypto";
import {spawn,execFile} from "node:child_process";
import {promisify} from "node:util";
import {cp,mkdir,mkdtemp,readFile,writeFile,rm} from "node:fs/promises";
import {join,resolve} from "node:path";
import {tmpdir} from "node:os";
import {actualNativeWorker} from "../native-workerd.mjs";
const exec=promisify(execFile);
const digest=value=>createHash("sha256").update(JSON.stringify(value)).digest("hex");

// Adapt already verified owner receipts to the sole runner's evidence contract.
// Exported so captured crash artifacts can exercise this boundary without kills.
export function buildT10_12Evidence(observed,settlement,reservePacket){
  const {crash,recovery,local,terminal}=settlement;
  const {reserve,worker,reserveCrash}=reservePacket;
  assert.equal(observed.length,2);
  assert.deepEqual(observed.map(x=>x.boundary),["reserve","settlement"]);
  for(const x of observed){
    assert.equal(x.os_process_killed,true);
    assert.equal(x.recovery.persisted_do_reopened,true);
    assert.ok(x.recovery.generation_after>x.recovery.generation_before);
  }
  for(const x of [crash,reserveCrash]){
    assert.equal(x.worker_os_kills,1);
    assert.equal(x.native_process_survived,true);
    assert.equal(x.original_do_storage_reopened,true);
    assert.equal(x.native_recovery.old_reply_rejected,true);
  }
  assert.equal(recovery.old_reply_rejected,true);
  assert.equal(reserve.old_reply_rejected,true);
  assert.equal(local.local_settlement_ack,true);
  assert.equal(local.intent_id,recovery.sql_before.intent_id);
  assert.deepEqual(local.proof,recovery.sql_before.proof);
  assert.deepEqual(recovery.worker_before,recovery.worker_after);
  assert.equal(reserve.after_status.central_release.kind,"CENTRAL_RELEASE");
  assert.equal(reserve.before_status.reservation_key,reserve.after_status.reservation_key);
  assert.equal(reserve.after_status.reservation_key,reserveCrash.committed.reservation_key);
  assert.deepEqual(worker.budget,reserve.worker_budget);
  const assertions=[
    ["actual_worker_os_kills","4",observed.length+crash.worker_os_kills+reserveCrash.worker_os_kills],
    ["original_sql_settlement_ack","true",local.local_settlement_ack],
    ["original_sql_reserve_release","CENTRAL_RELEASE",reserve.after_status.central_release.kind],
    ["settlement_cumulative_entry_usd","1.0005",local.proof.cumulative_entry_usd],
    ["settlement_fill_count","1",local.fill_count],
    ["settlement_bill_count","1",local.bill_count],
    ["reserve_crash_venue_posts","0",reserve.outbound_create_posts],
    ["reserve_crash_consumed_usd","0",worker.budget.entry_budget.consumed_usd],
    ["unrelated_entry_reservations_usd","4",worker.budget.entry_budget.reserved_usd],
    ["reserve_crash_remaining_usd","6",worker.budget.entry_budget.remaining_usd],
  ].map(([id,expected,actual])=>({id,expected,actual:String(actual)}));
  for(const a of assertions)assert.equal(a.actual,a.expected,a.id);
  const events=[
    ["worker_committed_before_kill","RuntimeSessionSQLiteDO",[crash.committed,reserveCrash.committed]],
    ["worker_process_terminated","OSProcess",[observed,crash,reserveCrash]],
    ["worker_new_generation_recovered","RuntimeSessionSQLiteDO",[recovery,reserveCrash.native_recovery]],
    ["stale_reply_denied","NativeRiskClient",[recovery,reserve.old_reply_rejected,reserveCrash.native_recovery]],
    ["single_risk_mutation_verified","NativeAccountSQL+RuntimeSessionSQLiteDO",[local,reserve,worker]],
  ].map(([stage,owner,source])=>({stage,owner,source_digest:digest(source)}));
  for(const revision of [local.intent_revision,recovery.sql_before.intent_revision,
    recovery.worker_sequence,terminal.worker_risk_book_sequence])
    assert.ok(Number.isSafeInteger(revision)&&revision>0,"actual owner revision required");
  for(const hash of [settlement.transcript_sha256,reservePacket.transcript_sha256])
    assert.match(hash,/^[a-f0-9]{64}$/);
  return {case_id:"T10-12",status:"PASS",assertions,events,
    provenance:{schema:"okx.native-workerd-crash-transcripts/v1",
      settlement_sha256:settlement.transcript_sha256,reserve_sha256:reservePacket.transcript_sha256},
    readbacks:{
      sql:[{revision:recovery.sql_before.intent_revision,digest:digest(recovery.sql_before)},
        {revision:local.intent_revision,digest:digest(local)}],
      risk_book:[{revision:recovery.worker_sequence,digest:digest(recovery.worker_after)},
        {revision:terminal.worker_risk_book_sequence,digest:digest(terminal.worker_budget)}],
    },
    outbound_sends:[{account:"fixture-a",intent:reserve.after_status.intent_id,
      operation_kind:"create_after_reserve_crash",count:reserve.outbound_create_posts}],
  };
}
// stdout is a byte stream. A durable-commit JSON marker can be split across
// arbitrary chunks. Parse only complete lines before killing the process.
function lineFramer(onLine) {
  let pending="";
  return (chunk) => {
    pending+=chunk;
    const lines=pending.split(/\r?\n/);
    pending=lines.pop() ?? "";
    if(pending.length>32768) throw Error("T10_12_WORKER_READY_LINE_TOO_LONG");
    for(const line of lines) onLine(line);
  };
}

async function terminateTree(child){
  if(process.platform==="win32"){
    await exec("taskkill",["/PID",String(child.pid),"/T","/F"],
      {windowsHide:true,timeout:15000});
  }else{
    process.kill(-child.pid,"SIGKILL");
  }
}
async function rejectMissingSettlement({file,cwd,common,witness,signal}){
  // Reopen a copy of the ACTUAL reserve-only crash store while claiming a
  // settlement receipt. The recovery oracle must reject the absent durable
  // debit; a marker/JSON handoff alone cannot manufacture financial proof.
  const copiedRoot=await mkdtemp(join(tmpdir(),"okx-workerd-risk-negative-"));
  try{
    await cp(witness.root,copiedRoot,{recursive:true});
    const path=join(copiedRoot,"t10-12-handoff.json");
    const proof=JSON.parse(await readFile(path,"utf8"));
    proof.entry_budget={...proof.entry_budget,
      consumed_usd:"1.0005",reserved_usd:"6.9995"};
    await writeFile(path,JSON.stringify(proof)+"\n");
    await assert.rejects(exec(process.execPath,[file],{
      cwd,env:{...common,OKX_T10_03_LOST_RESERVE_REPLY_PROBE:"0",
        OKX_T10_12_WORKER_CRASH_CHILD:"0",
        OKX_T10_12_WORKER_CRASH_RECOVERY:"1",
        OKX_T10_12_PERSISTED_ROOT:copiedRoot},
      timeout:120000,signal,maxBuffer:4*1024*1024,windowsHide:true,
    }),error=>{
      assert.notEqual(error.code,0);
      assert.match(String(error.stderr),
        /Worker OS kill must preserve every persisted entry-budget field/);
      return true;
    },"a reserve-only SQLite store cannot prove a settlement");
    return true;
  }finally{
    await rm(copiedRoot,{recursive:true,force:true});
  }
}
async function killAndRecover(root,signal,boundary){
  const file=resolve(root,"cloudflare/okx-mcp/scripts/test-runtime-do.mjs");
  const cwd=resolve(root,"cloudflare/okx-mcp");
  const common={...process.env,OKX_T10_7_SHARED_CAP_PROBE:"1",
    OKX_T10_12_CRASH_BOUNDARY:boundary,
    OKX_T10_7_CONCURRENT_CAP_PROBE:"0",
    OKX_NATIVE_WORKER_INTEGRATION:"0",OKX_T09_4_MANAGED_ADD_PROBE:"0"};
  const child=spawn(process.execPath,[file],{
    cwd,env:{...common,OKX_T10_03_LOST_RESERVE_REPLY_PROBE:"1",
      OKX_T10_12_WORKER_CRASH_CHILD:"1",
      OKX_T10_12_WORKER_CRASH_RECOVERY:"0"},
    detached:process.platform!=="win32",
    stdio:["ignore","pipe","pipe"],windowsHide:true,
  });
  let ready=null,exited=null,stderr="",wasKilled=false;
  const withheldReplies=new Map();
  const awaiting=new Promise((done,fail)=>{
    const timer=setTimeout(()=>fail(Error("T10_12_WORKER_DURABLE_READY_TIMEOUT")),120000);
    child.stdout.setEncoding("utf8");
    const consume=lineFramer(line=>{
      const fenced="T10_12_WORKER_REPLY_FENCED_V1:";
      const position=line.indexOf(fenced);
      if(position>=0){
        const frame=JSON.parse(line.slice(position+fenced.length));
        assert.equal(frame.schema,"okx.t10-12-fenced-response/v1");
        withheldReplies.set(frame.command_id,frame);
      }
      const marker="T10_12_WORKER_COMMIT_READY_V1:";
      if(line.startsWith(marker)&&!ready){
        ready=JSON.parse(line.slice(marker.length));
        clearTimeout(timer);
        done(ready);
      }
    });
    child.stdout.on("data",chunk=>{
      try{consume(chunk)}
      catch(e){clearTimeout(timer);fail(e)}
    });
    child.stderr.setEncoding("utf8");
    child.stderr.on("data",chunk=>{stderr=(stderr+chunk).slice(-512)});
    child.once("error",e=>{clearTimeout(timer);fail(e)});
    child.once("exit",(code,signal)=>{
      exited={code,signal};
      if(!ready){clearTimeout(timer);fail(Error("T10_12_CHILD_EXITED_BEFORE_WORKER_COMMIT"))}
    });
  });
  try{
    const witness=await Promise.race([
      awaiting,new Promise((_,fail)=>{
        if(signal?.aborted)fail(Error("T10_12_ABORTED"));
        signal?.addEventListener("abort",()=>fail(Error("T10_12_ABORTED")),{once:true});
      }),
    ]);
    assert.equal(witness.boundary,boundary);
    assert.equal(witness.original_reply_not_emitted,true);
    const withheld=withheldReplies.get(witness.fenced_command_id);
    assert.ok(withheld,"actual Worker must witness interception before ws.send");
    assert.equal(withheld.session_id,witness.session_id);
    assert.equal(withheld.connection_generation,witness.connection_generation);
    assert.equal(withheld.status,boundary==="reserve"?"HELD":"UPDATED");
    assert.equal(withheld.reservation_key,witness.reservation_key);
    assert.equal(witness.entry_budget.reserved_usd,
      boundary==="reserve"?"8":"6.9995");
    assert.match(witness.root,/okx-workerd-risk-/);
    await terminateTree(child);
    wasKilled=true;
    const terminated=await new Promise((done,fail)=>{
      if(exited)return done(exited);
      const timer=setTimeout(()=>fail(Error("T10_12_PROCESS_KILL_UNVERIFIED")),15000);
      child.once("exit",(code,signal)=>{
        clearTimeout(timer);done({code,signal});
      });
    });
    assert.ok(terminated.signal!==null||terminated.code!==0,
      "OS child must not exit normally");
    const absentSettlementRejected=boundary==="reserve"
      ? await rejectMissingSettlement({file,cwd,common,witness,signal})
      : null;
    const recovered=await exec(process.execPath,[file],{
      cwd,env:{...common,OKX_T10_03_LOST_RESERVE_REPLY_PROBE:"0",
        OKX_T10_12_WORKER_CRASH_CHILD:"0",
        OKX_T10_12_WORKER_CRASH_RECOVERY:"1",
        OKX_T10_12_PERSISTED_ROOT:witness.root},
      timeout:120000,signal,maxBuffer:4*1024*1024,windowsHide:true,
    });
    const prefix="T10_12_ACTUAL_WORKER_PROCESS_RECOVERY_V2:";
    const markers=recovered.stdout.split(/\r?\n/).filter(x=>x.startsWith(prefix));
    assert.equal(markers.length,1);
    const result=JSON.parse(markers[0].slice(prefix.length));
    assert.equal(result.persisted_do_reopened,true);
    assert.ok(result.generation_after>result.generation_before);
    assert.equal(result.boundary,boundary);
    assert.equal(result.reserved_usd,boundary==="reserve"?"8":"6.9995");
    assert.equal(result.remaining_usd,"2");
    assert.equal(result.consumed_usd,boundary==="reserve"?"0":"1.0005");
    assert.equal(result.original_state,boundary==="reserve"?"HELD":"POSTED");
    assert.equal(result.original_reply_not_emitted,true);
    assert.equal(result.stale_frame_close_code,1008);
    if(boundary==="settlement"){
      assert.equal(result.settlement_replay.status,"UPDATED");
      assert.equal(result.settlement_replay.sequence_before,
        result.settlement_replay.sequence_after);
      assert.deepEqual(result.settlement_replay.budget_before,
        result.settlement_replay.budget_after);
    }
    return {boundary,os_process_killed:true,
      absent_settlement_rejected:absentSettlementRejected,
      original_reply_fenced:withheld,recovery:result};
  }finally{
    if(!wasKilled&&child.exitCode===null){
      try{await terminateTree(child)}catch{child.kill("SIGKILL")}
    }
    if(ready?.root)await rm(ready.root,{recursive:true,force:true});
    if(!wasKilled&&stderr)process.stderr.write(
      "T10_12_PRE_KILL_DIAGNOSTIC:"+stderr.slice(-160)+"\n");
  }
}
export async function run({caseId,root,signal,artifactsDir}){
  assert.equal(caseId,"T10-12");
  assert.ok(artifactsDir,"crash artifacts required");
  const dir=resolve(artifactsDir,"T10-12");
  await mkdir(dir,{recursive:true});
  const observed=[];
  for(const boundary of ["reserve","settlement"]){
    const result=await killAndRecover(root,signal,boundary);
    observed.push(result);
    // Preserve each completed kill even if the following boundary fails.
    await writeFile(resolve(dir,"worker-"+boundary+"-process-crash.json"),
      JSON.stringify(result,null,2)+"\n");
  }
  await writeFile(resolve(dir,"worker-process-crash.json"),
    JSON.stringify({schema:"okx.t10-12-worker-crash-component/v2",
      mandatory_case_pass:false,worker_os_kills:observed.length,
      authenticated_wss_source:"scripted_component_inputs",observed,
      missing:[
        "same_native_sql_account_and_no_double_financial_mutation",
        "old_worker_reply_binding_rejected_by_native"]},null,2)+"\n");
  process.env.OKX_T10_12_NATIVE_WORKER_CRASH="1";
  let native;
  try {
    native=await actualNativeWorker(root,signal,{statusProbe:true});
  } finally {
    delete process.env.OKX_T10_12_NATIVE_WORKER_CRASH;
  }
  const recovery=native.nativeOldReplyRecovery;
  const local=native.nativeLocalRecovery;
  const crash=native.nativeWorkerCrash;
  assert.equal(crash.worker_os_kills,1);
  assert.equal(crash.native_process_survived,true);
  assert.equal(crash.original_do_storage_reopened,true);
  assert.equal(recovery.old_reply_rejected,true);
  assert.equal(recovery.reason,"SESSION_GENERATION_MISMATCH");
  assert.deepEqual(recovery.sql_before,recovery.sql_after_old_reply);
  assert.deepEqual(recovery.sql_before,recovery.sql_after_worker_replay);
  assert.deepEqual(recovery.worker_before,recovery.worker_after);
  assert.equal(recovery.sql_before.local_settlement_ack,false);
  assert.equal(local.local_settlement_ack,true);
  assert.equal(local.intent_id,recovery.sql_before.intent_id);
  assert.deepEqual(local.proof,recovery.sql_before.proof);
  assert.equal(local.fill_count,recovery.sql_before.fill_count);
  assert.equal(local.bill_count,recovery.sql_before.bill_count);
  assert.equal(native.nativeTerminal.created_posts,3);
  assert.equal(native.nativeTerminal.opening_status.data.central_release.state,"verified");
  assert.equal(native.nativeRestart.active_central_receipt,false);
  await writeFile(resolve(dir,"native-worker-settlement-crash.json"),JSON.stringify({
    schema:"okx.t10-12-native-worker-settlement-crash/v1",
    mandatory_case_pass:false,crash,recovery,local,
    terminal:native.nativeTerminal,reopened:native.nativeRestart,
    transcript_sha256:native.transcript_sha256,
    missing:["reserve_crash_same_native_sql_recovery"],
  },null,2)+"\n");

  // Independent production public Execute, but Native and its original SQL
  // owner remain alive while the ACTUAL Worker commits RESERVE then dies.
  // The same Native process must reject the stale generation, preserve the
  // durable RISK_RESERVE_ATTEMPT, and reconcile that one HELD to RELEASED
  // with zero POST. Worker-only crash evidence above is not counted here.
  process.env.OKX_T10_12_NATIVE_RESERVE_CRASH="1";
  let reserveNative;
  try {
    reserveNative=await actualNativeWorker(root,signal,{});
  } finally {
    delete process.env.OKX_T10_12_NATIVE_RESERVE_CRASH;
  }
  const reserve=reserveNative.nativeReserveRecovery;
  const worker=reserveNative.workerReserveRecovery;
  const reserveCrash=reserveNative.workerCrash;
  // Retain genuine producer receipts before consumer assertions. A schema
  // mismatch can then be reproduced locally without repeating the OS kills.
  await writeFile(resolve(dir,"native-worker-reserve-receipts.json"),
    JSON.stringify({mandatory_case_pass:false,reserve,worker,reserveCrash,
      transcript_sha256:reserveNative.transcript_sha256},null,2)+"\n");
  assert.equal(reserveCrash.worker_os_kills,1);
  assert.equal(reserveCrash.native_process_survived,true);
  assert.equal(reserveCrash.original_do_storage_reopened,true);
  assert.equal(reserveCrash.committed.action,"RESERVE");
  assert.equal(reserveCrash.committed.status,"HELD");
  assert.equal(reserve.old_reply_rejected,true,
    "final Native receipt must carry the verified old-generation rejection");
  assert.equal(reserveCrash.native_recovery.old_reply_rejected,true);
  assert.equal(reserveCrash.native_recovery.original_reservation_key,
    reserve.before_status.reservation_key,
    "old-generation witness must belong to the same original reservation");
  assert.equal(reserve.before_status.intent_id,reserve.after_status.intent_id);
  assert.equal(reserve.before_status.reservation_key,
    reserve.after_status.reservation_key);
  assert.equal(reserve.before_status.attempt_kind,"RISK_RESERVE_ATTEMPT");
  assert.equal(reserve.after_status.central_release.kind,"CENTRAL_RELEASE");
  assert.equal(reserve.outbound_create_posts,0);
  assert.equal(reserve.worker_reservation_state,"RELEASED");
  assert.equal(worker.original_native_sql_release,true);
  assert.equal(worker.no_new_venue_post,true);
  assert.equal(worker.reservation_key,reserve.before_status.reservation_key);
  assert.deepEqual(worker.budget,reserve.worker_budget);
  await writeFile(resolve(dir,"native-worker-reserve-crash.json"),
    JSON.stringify({schema:"okx.t10-12-native-worker-reserve-crash/v1",
      reserve,worker,reserveCrash,
      transcript_sha256:reserveNative.transcript_sha256,
      mandatory_case_pass:true},null,2)+"\n");
  return buildT10_12Evidence(observed,
    {crash,recovery,local,terminal:native.nativeTerminal,transcript_sha256:native.transcript_sha256},
    {reserve,worker,reserveCrash,transcript_sha256:reserveNative.transcript_sha256});
}

// Standalone parser regression. Real OS process kill remains mandatory.
if(process.argv.includes("--self-test")){
  const observed=[];
  const accept=lineFramer(line=>observed.push(line));
  accept("diagnostic\r");
  accept("\nT10_12_WORKER_COMMIT_");
  accept('READY_V1:{"entry_reserved_usd":"8"}\r');
  assert.equal(observed.length,1,"do not parse incomplete JSON");
  accept("\n");
  assert.deepEqual(observed,["diagnostic",
    'T10_12_WORKER_COMMIT_READY_V1:{"entry_reserved_usd":"8"}']);
  const bounded=lineFramer(()=>{});
  assert.throws(()=>bounded("x".repeat(32769)),
    /T10_12_WORKER_READY_LINE_TOO_LONG/);
  process.stdout.write("T10_12_WORKER_LINE_FRAMER_SELF_TEST_PASS\n");
}
