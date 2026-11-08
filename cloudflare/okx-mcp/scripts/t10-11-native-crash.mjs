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

export function nativeF2Failure(error){
  const rust_failures=summarizeRustPanics(error);
  const first=rust_failures.panics[0];
  return {schema:"okx.t09-native-child-failure/v2",rust_failures,
    rust_assertion:first?`${first.location}: ${first.reason}`:"T10_11_CHILD_EXIT_WITHOUT_RUST_ASSERTION"};
}
function failNative(error){
  process.stderr.write("T09_1_NATIVE_CHILD_FAILURE_V2:"+JSON.stringify(nativeF2Failure(error))+"\n");
  throw error;
}

export function validateF2KillBoundary(value){
  assert.equal(value.schema,"okx.t10-11-native-f2-kill-boundary/v1");
  assert.equal(value.phase,"F2");
  assert.ok(Number.isSafeInteger(value.pid)&&value.pid>0&&value.pid!==process.pid);
  const root=resolve(value.root),within=relative(resolve(tmpdir()),root);
  assert.ok(within&&!within.startsWith("..")&&!isAbsolute(within));
  assert.match(basename(root),/^okx-raw-native-session-[a-f0-9]{32}$/);
  assert.equal(value.command.command_id,"reserve_t11_f2");
  assert.equal(value.command.action.action,"RESERVE");
  assert.equal(value.worker_held.status,"HELD");
  assert.equal(value.sql.state,"PREPARED");
  assert.equal(value.sql.central_receipt,null);
  assert.equal(value.sql.exchange_order_id,null);
  assert.equal(value.sql.intent_id,value.command.action.intent_id);
  assert.equal(value.sql.plan_hash,value.command.action.plan_sha256);
  assert.equal(value.sql.reservation_key,value.worker_held.reservation_key);
  assert.equal(value.venue_posts,0);
  return value;
}

export async function killAndRecoverNativeF2({nativeProcess,handoffPath,cwd}){
  const waiting=new AbortController();
  const ready=(async()=>{
    const until=Date.now()+60_000;
    while(Date.now()<until){
      try{return validateF2KillBoundary(JSON.parse(await readFile(
        handoffPath+".t11-native-kill-ready","utf8")));}
      catch(error){if(error.code!=="ENOENT")throw error;}
      await delay(25,undefined,{signal:waiting.signal});
    }
    throw Error("T10_11_F2_COMMIT_BOUNDARY_TIMEOUT");
  })();
  const boundary=await Promise.race([ready,nativeProcess.then(
    ()=>{throw Error("T10_11_NATIVE_EXITED_WITHOUT_OS_KILL");},
    failNative,
  )]).finally(()=>waiting.abort());
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
    "cloudflare_transport::raw_native_session_tests::native_crash_f2_recover",
    "--","--ignored","--exact","--nocapture",
  ],{cwd,env:{...process.env,RUST_MIN_STACK:"16777216",
      OKX_INTEGRATION_HANDOFF_PATH:handoffPath},
    timeout:90_000,maxBuffer:4*1024*1024,windowsHide:true}).catch(failNative);
  const prefix="T10_11_NATIVE_F2_RECOVERY_V1:";
  const lines=recovered.stdout.split(/\r?\n/).filter(x=>x.startsWith(prefix));
  assert.equal(lines.length,1,"one genuine Native recovery receipt required");
  const native=JSON.parse(lines[0].slice(prefix.length));
  assert.notEqual(native.pid,boundary.pid,"a new OS process must own recovery");
  assert.equal(native.root,boundary.root);
  assert.deepEqual(native.before,boundary.sql);
  assert.equal(native.original_sql_reopened,true);
  assert.equal(native.public_cancel_replayed,true);
  assert.equal(native.after.reservation_key,boundary.sql.reservation_key);
  assert.equal(native.after.central_release.kind,"CENTRAL_RELEASE");
  assert.equal(native.venue_posts,0);
  assert.deepEqual(native.worker_after.budget,{...boundary.before_budget,
    source_as_of_ms:boundary.command.action.census.source_read_floor_ms});
  return {schema:"okx.t10-11-native-f2-process-crash/v1",
    phase:"F2",native_os_kills:1,boundary,termination,native,
    input_scope:"direct SQL prepare and explicit census fixture; actual Native WSS and Worker",
    full_t10_11_pass:false};
}
