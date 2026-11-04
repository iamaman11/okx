// One genuine Worker process tree kill at a durable RESERVE boundary.
// Mandatory T10-12 remains blocked until settlement crash and Native SQL
// binding are proved. The same SQLite DO files are reopened in another OS process.
import assert from "node:assert/strict";
import {spawn,execFile} from "node:child_process";
import {promisify} from "node:util";
import {mkdir,writeFile,rm} from "node:fs/promises";
import {resolve} from "node:path";
const exec=promisify(execFile);
async function terminateTree(child){
  if(process.platform==="win32"){
    await exec("taskkill",["/PID",String(child.pid),"/T","/F"],
      {windowsHide:true,timeout:15000});
  }else{
    process.kill(-child.pid,"SIGKILL");
  }
}
async function killAndRecover(root,signal){
  const file=resolve(root,"cloudflare/okx-mcp/scripts/test-runtime-do.mjs");
  const cwd=resolve(root,"cloudflare/okx-mcp");
  const common={...process.env,OKX_T10_7_SHARED_CAP_PROBE:"1",
    OKX_T10_7_CONCURRENT_CAP_PROBE:"0",
    OKX_NATIVE_WORKER_INTEGRATION:"0",OKX_T09_4_MANAGED_ADD_PROBE:"0"};
  const child=spawn(process.execPath,[file],{
    cwd,env:{...common,OKX_T10_03_LOST_RESERVE_REPLY_PROBE:"1",
      OKX_T10_12_WORKER_CRASH_CHILD:"1",
      OKX_T10_12_WORKER_CRASH_RECOVERY:"0"},
    detached:process.platform!=="win32",
    stdio:["ignore","pipe","pipe"],windowsHide:true,
  });
  let ready=null,exited=null,output="",stderr="",wasKilled=false;
  const awaiting=new Promise((done,fail)=>{
    const timer=setTimeout(()=>fail(Error("T10_12_WORKER_DURABLE_READY_TIMEOUT")),120000);
    child.stdout.setEncoding("utf8");
    child.stdout.on("data",chunk=>{
      output=(output+chunk).slice(-32000);
      for(const line of output.split(/\r?\n/)){
        const marker="T10_12_WORKER_COMMIT_READY_V1:";
        if(line.startsWith(marker)&&!ready){
          try{ready=JSON.parse(line.slice(marker.length))}
          catch(e){clearTimeout(timer);fail(e);return}
          clearTimeout(timer);done(ready);
        }
      }
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
    assert.equal(witness.original_reply_unread,true);
    assert.equal(witness.entry_reserved_usd,"8");
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
    const recovered=await exec(process.execPath,[file],{
      cwd,env:{...common,OKX_T10_03_LOST_RESERVE_REPLY_PROBE:"0",
        OKX_T10_12_WORKER_CRASH_CHILD:"0",
        OKX_T10_12_WORKER_CRASH_RECOVERY:"1",
        OKX_T10_12_PERSISTED_ROOT:witness.root},
      timeout:120000,signal,maxBuffer:4*1024*1024,windowsHide:true,
    });
    const prefix="T10_12_ACTUAL_WORKER_PROCESS_RECOVERY_V1:";
    const markers=recovered.stdout.split(/\r?\n/).filter(x=>x.startsWith(prefix));
    assert.equal(markers.length,1);
    const result=JSON.parse(markers[0].slice(prefix.length));
    assert.equal(result.persisted_do_reopened,true);
    assert.ok(result.generation_after>result.generation_before);
    assert.equal(result.reserved_usd,"8");
    assert.equal(result.remaining_usd,"2");
    assert.equal(result.consumed_usd,"0");
    assert.equal(result.original_state,"HELD");
    assert.equal(result.original_reply_unread,true);
    assert.equal(result.stale_frame_close_code,1008);
    return {os_process_killed:true,recovery:result};
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
  const observed=await killAndRecover(root,signal);
  const dir=resolve(artifactsDir,"T10-12");
  await mkdir(dir,{recursive:true});
  await writeFile(resolve(dir,"worker-process-crash.json"),
    JSON.stringify({schema:"okx.t10-12-worker-crash-component/v1",
      mandatory_case_pass:false,...observed,
      missing:["real_worker_settlement_commit_before_reply",
        "same_native_sql_account_and_no_double_financial_mutation",
        "old_worker_reply_binding_rejected_by_native"]},null,2)+"\n");
  throw Error("T10_12_SETTLEMENT_CRASH_AND_NATIVE_REPLY_NOT_PROVEN");
}
