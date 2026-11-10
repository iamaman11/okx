// A separate OS host for the EXISTING persisted RuntimeSession. Native cargo
// remains a sibling of this process tree and keeps its original SQL owners.
import assert from "node:assert/strict";
import {spawn,execFile} from "node:child_process";
import {promisify} from "node:util";
import {readFile,writeFile,rename} from "node:fs/promises";
import {setTimeout as delay} from "node:timers/promises";
import {fileURLToPath} from "node:url";
import {join} from "node:path";

const exec=promisify(execFile);
const hostPrefix="T10_12_NATIVE_WORKER_HOST_READY_V1:";

async function readBarrier(path,signal){
  const until=Date.now()+90_000;
  while(Date.now()<until){
    signal.throwIfAborted();
    try{return JSON.parse(await readFile(path,"utf8"));}
    catch(error){if(error?.code!=="ENOENT")throw error;}
    await delay(25,undefined,{signal});
  }
  throw Error("T10_12_NATIVE_WORKER_BARRIER_TIMEOUT");
}
async function publish(path,value){
  await writeFile(path+".pending",JSON.stringify(value)+"\n",{mode:0o600});
  await rename(path+".pending",path);
}
async function startHost(optionsPath){
  const child=spawn(process.execPath,[fileURLToPath(import.meta.url),"--host",optionsPath],{
    stdio:["ignore","pipe","pipe"],windowsHide:true,
    detached:process.platform!=="win32",
  });
  let stderr="",pending="";
  child.stderr.setEncoding("utf8");
  child.stderr.on("data",chunk=>{stderr=(stderr+chunk).slice(-2000);});
  const ready=await new Promise((resolve,reject)=>{
    const timer=setTimeout(()=>reject(Error("T10_12_NATIVE_HOST_START_TIMEOUT")),30_000);
    child.stdout.setEncoding("utf8");
    child.stdout.on("data",chunk=>{
      pending+=chunk;
      const lines=pending.split(/\r?\n/);pending=lines.pop()??"";
      for(const line of lines){
        if(line.startsWith(hostPrefix)){
          clearTimeout(timer);
          try{resolve(JSON.parse(line.slice(hostPrefix.length)));}catch(error){reject(error);}
        }
      }
    });
    child.once("error",error=>{clearTimeout(timer);reject(error);});
    child.once("exit",()=>{clearTimeout(timer);reject(Error(
      "T10_12_NATIVE_HOST_EXITED_BEFORE_READY:"+stderr.slice(-200)));});
  }).catch(async error=>{await killHost(child).catch(()=>{});throw error;});
  assert.match(ready.ws_url,/^ws:\/\/127\.0\.0\.1:\d+\/runtime$/);
  return {child,ws_url:ready.ws_url};
}
async function killHost(child){
  if(child.exitCode!==null||child.signalCode!==null)
    return {code:child.exitCode,signal:child.signalCode};
  const exited=new Promise((resolve,reject)=>{
    const timer=setTimeout(()=>reject(Error("T10_12_NATIVE_HOST_KILL_TIMEOUT")),15_000);
    child.once("exit",(code,signal)=>{clearTimeout(timer);resolve({code,signal});});
  });
  if(process.platform==="win32")await exec("taskkill",["/PID",String(child.pid),"/T","/F"],{
    windowsHide:true,timeout:15_000});
  else process.kill(-child.pid,"SIGKILL");
  const outcome=await exited;
  assert.ok(outcome.signal!==null||outcome.code!==0,"test Worker must be OS-killed");
  return outcome;
}

export async function nativeWorkerCrashHost({opts,root,handoffPath,action="SETTLE_ENTRY"}){
  const optionsPath=join(root,"t10-12-worker-host-options.json");
  await writeFile(optionsPath,JSON.stringify(opts),{mode:0o600});
  let host=await startHost(optionsPath);
  const initialUrl=host.ws_url;
  const abort=new AbortController();
  const completion=(async()=>{
    const committed=await readBarrier(handoffPath+".t12-worker-kill-ready",abort.signal);
    assert.equal(committed.schema,"okx.t10-12-native-held-worker-response/v1");
    assert.equal(committed.action,action);
    assert.equal(committed.status,action==="RESERVE"?"HELD":"UPDATED");
    const killed=await killHost(host.child);
    await publish(handoffPath+".t12-worker-killed",{os_killed:true,...killed});
    if(action==="RESERVE"){
      // The Worker must remain OFFLINE until the original public Native
      // Execute has committed a durable no-send SQL rejection. Reconnecting
      // early could let a still-running INSPECT resume its pre-POST path.
      const noSend=await readBarrier(handoffPath+".t12-native-no-send",abort.signal);
      assert.equal(noSend.native_sql_no_send,true);
      assert.equal(noSend.original_reservation_key,committed.reservation_key);
    }
    host=await startHost(optionsPath);
    await publish(handoffPath+".t12-worker-restarted",{ws_url:host.ws_url});
    const recovered=await readBarrier(handoffPath+".t12-native-recovered",abort.signal);
    assert.equal(recovered.old_reply_rejected,true);
    return {schema:"okx.t10-12-native-worker-os-kill/v1",worker_os_kills:1,
      native_process_survived:true,original_do_storage_reopened:true,
      committed,native_recovery:recovered};
  })();
  completion.catch(()=>{});
  return {ws_url:initialUrl,completion,
    async close(){abort.abort();await killHost(host.child);await completion.catch(()=>{});}};
}

if(process.argv[2]==="--host"){
  const {Miniflare}=await import("miniflare");
  const opts=JSON.parse(await readFile(process.argv[3],"utf8"));
  const mf=new Miniflare(opts);
  const ready=await mf.ready;
  assert.equal(ready.hostname,"127.0.0.1");
  const url=new URL("/runtime",ready);url.protocol="ws:";
  process.stdout.write(hostPrefix+JSON.stringify({ws_url:url.toString()})+"\n");
  // Parent terminates the actual process tree. No graceful dispose simulates
  // the crash and no in-process replacement stands in for an OS restart.
  await new Promise(()=>{});
}
