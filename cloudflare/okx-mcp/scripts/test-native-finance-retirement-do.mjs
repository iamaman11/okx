#!/usr/bin/env node
// Production RuntimeSession in actual workerd with SQLite DO persistence.
// This proves auth, typed retirement denial and transport; no financial witness.
import assert from "node:assert/strict";
import {createHash} from "node:crypto";
import {mkdtemp,readFile,rm} from "node:fs/promises";
import {tmpdir} from "node:os";
import {join} from "node:path";
import {fileURLToPath} from "node:url";
import {build} from "esbuild";
import {Miniflare} from "miniflare";
const root=await mkdtemp(join(tmpdir(),"okx-retired-finance-transport-"));
const outfile=join(root,"runtime-test.mjs");
const FRAME_SCHEMA="okx.direct-transport.frame/v2";
const configuration={schema:"okx.governance/v3",revision:0,installation_id:"installation_test",campaign_id:"sha256:"+"c".repeat(64),campaign_loss_limit_usd:"10",campaign_entry_limit_usd:"10",max_total_modelled_loss_usdt:"5",accounts:[]};
const installation={contract:FRAME_SCHEMA,release_sha256:"f".repeat(64),operation_id:"install_integration_001",configuration};
const token = "integration-only-ephemeral-token";
const tokenHash = createHash("sha256").update(token).digest("hex");
const researchToken = "integration-only-research-token";
const researchTokenHash = createHash("sha256").update(researchToken).digest("hex");
// Miniflare 5 uses the build-output worker manifest and typed bindings.
const makeOptions = (bundleContents) => ({
  resourcePersistencePath: join(root, "sqlite-do"),
  workers: [{
    config: {
      name: "okx-test-runtime",
      compatibilityDate: "2026-09-30",
      compatibilityFlags: ["global_fetch_strictly_public"],
      manifest: {
        mainModule: "runtime-test.mjs",
        modulesRoot: root,
        modules: {
          "runtime-test.mjs": { type: "esm", contents: bundleContents },
        },
      },
      env: {
        RUNTIME: {
          type: "durable-object",
          worker: "okx-test-runtime",
          exportName: "RuntimeSession",
        },
        RUNTIME_TOKEN_SHA256: { type: "text", value: tokenHash },
        RESEARCH_RUNTIME_TOKEN_SHA256: { type: "text", value: researchTokenHash },
      },
      exports: {
        RuntimeSession: { type: "durable-object", storage: "sqlite" },
      },
    },
  }],
});
async function request(mf, pathname, method = "GET", body) {
  const response = await mf.dispatchFetch("http://localhost" + pathname, {
    method,
    ...(body === undefined ? {} : {
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    }),
  });
  return { status: response.status, body: await response.json() };
}
function frames(ws) {
  const received = [];
  const waiters = [];
  const onMessage = (event) => {
    const frame = JSON.parse(event.data);
    const pending = waiters.find((w) => w.match(frame));
    if (pending) {
      waiters.splice(waiters.indexOf(pending), 1);
      clearTimeout(pending.timer);
      pending.resolve(frame);
    } else {
      received.push(frame);
    }
  };
  ws.addEventListener("message", onMessage);
  ws.accept();
  return {
    send(frame) { ws.send(JSON.stringify(frame)); },
    has(match) { return received.some(match); },
    next(match) {
      const idx = received.findIndex(match);
      if (idx >= 0) return Promise.resolve(received.splice(idx, 1)[0]);
      return new Promise((resolve, reject) => {
        const waiter = { match, resolve, timer: null };
        waiter.timer = setTimeout(() => {
          const pos = waiters.indexOf(waiter);
          if (pos >= 0) waiters.splice(pos, 1);
          reject(new Error("WORKER_FRAME_TIMEOUT"));
        }, 5000);
        waiters.push(waiter);
      });
    },
    closed(){
      return new Promise((resolve,reject)=>{
        const timeout=setTimeout(()=>reject(Error("T10_12_STALE_FRAME_CLOSE_TIMEOUT")),5000);
        ws.addEventListener("close",e=>{clearTimeout(timeout);resolve(e.code)},{once:true});
      });
    },
    close() { ws.close(1000, "integration complete"); },
  };
}
async function session(mf) {
  const reply = await mf.dispatchFetch("http://localhost/runtime", {
    headers: { upgrade: "websocket", authorization: "Bearer " + token },
  });
  assert.equal(reply.status, 101, "auth-bound real runtime WSS handshake");
  assert.ok(reply.webSocket, "WebSocketPair exists only through production RuntimeSession");
  const ws = frames(reply.webSocket);
  ws.send({
    type: "hello",
    schema: FRAME_SCHEMA,
    runtime_id: "windows-primary",
    connection_id: "native_" + "a".repeat(32),
    installation_id: configuration.installation_id,
    contract: "okx.product/v1",
    runtime_profile: "engine",
    agent_pid: 12345,
    agent_binary_sha256: "b".repeat(64),
    agent_incarnation: "ctl_" + "d".repeat(32),
  });
  const hello = await ws.next((f) => f.type === "hello_ack");
  assert.ok(hello.connection_generation > 0);
  assert.ok(hello.session_id.startsWith("session_"));
  const policy = await ws.next((f) => f.type === "policy_snapshot");
  assert.equal(policy.connection_generation, hello.connection_generation);
  assert.equal(policy.snapshot.state.installation_id, configuration.installation_id);
  return {ws,hello};
}

let mf;
try {
 const built=await build({entryPoints:[fileURLToPath(new URL("../src/integration/runtime_entry.ts",import.meta.url))],outfile,bundle:true,write:true,format:"esm",platform:"browser",target:"es2022",metafile:true,logLevel:"warning"});
 assert.ok(!Object.keys(built.metafile.inputs).some(path=>/risk_book\.ts$|legacy_finance_cutover\.ts$/.test(path)),"active runtime must not bundle historical financial authorities");
 const opts=makeOptions(await readFile(outfile,"utf8"));
 mf=new Miniflare(opts);
 for(const authorization of [undefined,"Bearer incorrect"]){
  const denied=await mf.dispatchFetch("http://localhost/runtime",{headers:{upgrade:"websocket",...(authorization?{authorization}:{})}});
  assert.equal(denied.status,401,"runtime token required before WSS");
 }
 assert.equal((await request(mf,"/installation/initialize","POST",installation)).status,200);
 const first=await session(mf);
 for(const action of ["INSTALL_ENTRY_OPENING","SUMMARY","ACCOUNT_SUMMARY","INSPECT","RESERVE","BEGIN_POST","SETTLE_ENTRY","RELEASE_UNSENT","TRANSITION"]){
  const id="retired_"+action;
  first.ws.send({type:"risk_control",schema:FRAME_SCHEMA,session_id:first.hello.session_id,connection_generation:first.hello.connection_generation,command:{schema:"okx.risk-command/v1",command_id:id,installation_id:configuration.installation_id,action:{action}}});
  const frame=await first.ws.next(f=>f.type==="risk_control_result"&&f.result.command_id===id);
  assert.equal(frame.result.reason,"NATIVE_FINANCE_AUTHORITY_REQUIRED");
  assert.equal(frame.result.status,"DENIED");
  assert.equal(frame.result.risk_book_sequence,null);
  assert.ok(!("budget" in frame.result)&&!("account_budget" in frame.result));
 }
 first.ws.send({type:"ping",schema:FRAME_SCHEMA,session_id:first.hello.session_id,connection_generation:first.hello.connection_generation,nonce:"transport_still_live"});
 assert.equal((await first.ws.next(f=>f.type==="pong")).nonce,"transport_still_live");
 const dispatched=request(mf,"/dispatch","POST",{schema:"okx.agent.request/v1",request_id:"dispatch_transport_001",operation:{type:"portfolio",request:{action:"summary"}}});
 const challenge=await first.ws.next(f=>f.type==="ping");
 first.ws.send({...challenge,type:"pong"});
 const delivered=await first.ws.next(f=>f.type==="request");
 assert.equal(delivered.request.request_id,"dispatch_transport_001");
 const bound={schema:FRAME_SCHEMA,session_id:first.hello.session_id,connection_generation:first.hello.connection_generation};
 first.ws.send({...bound,type:"delivery_ack",request_id:delivered.request.request_id});
 first.ws.send({...bound,type:"response",response:{request_id:delivered.request.request_id,status:"completed",data:{transport_fixture:true}}});
 assert.equal((await dispatched).body.status,"completed","request ACK and response cross actual WSS");
 const researchHello={type:"hello_research",schema:FRAME_SCHEMA,runtime_id:"windows-primary",connection_id:"native_"+"a".repeat(32),contract:"okx.product/v1",host_scope:"a".repeat(64),runtime_profile:"research",agent_pid:12346,agent_binary_sha256:"b".repeat(64),agent_incarnation:"ctl_"+"e".repeat(32)};
 const legacyReply=await mf.dispatchFetch("http://localhost/runtime",{headers:{upgrade:"websocket",authorization:"Bearer "+token}});
 const legacy=frames(legacyReply.webSocket);const legacyClosed=legacy.closed();
 legacy.send({...researchHello,runtime_profile:"engine"});
 assert.equal(await legacyClosed,1008,"Engine bearer cannot establish Research hello");
 assert.equal((await request(mf,"/dispatch","POST",{schema:"okx.agent.request/v1",request_id:"research_offline_001",operation:{type:"research",request:{action:"capabilities"}}})).body.reason,"RESEARCH_RUNTIME_OFFLINE");
 const researchReply=await mf.dispatchFetch("http://localhost/runtime",{headers:{upgrade:"websocket",authorization:"Bearer "+researchToken}});
 const research=frames(researchReply.webSocket);research.send(researchHello);
 const researchAck=await research.next(f=>f.type==="hello_ack");
 const researchDispatch=request(mf,"/dispatch","POST",{schema:"okx.agent.request/v1",request_id:"research_direct_001",operation:{type:"research",request:{action:"capabilities"}}});
 const researchPing=await research.next(f=>f.type==="ping");research.send({...researchPing,type:"pong"});
 const researchRequest=await research.next(f=>f.type==="request");
 assert.equal(researchRequest.request.request_id,"research_direct_001");
 const researchBound={schema:FRAME_SCHEMA,session_id:researchAck.session_id,connection_generation:researchAck.connection_generation};
 research.send({...researchBound,type:"delivery_ack",request_id:researchRequest.request.request_id});
 research.send({...researchBound,type:"response",response:{request_id:researchRequest.request.request_id,status:"completed"}});
 assert.equal((await researchDispatch).body.status,"completed");research.close();
 console.log("actual_workerd_research_ownership PASS legacy hello rejected, offline fails closed, Research ACK and response");
 const closed=first.ws.closed();
 first.ws.send({type:"risk_control",schema:FRAME_SCHEMA,session_id:first.hello.session_id,connection_generation:first.hello.connection_generation-1,command:{}});
 assert.equal(await closed,1008,"stale frame closed before RPC denial");
 await mf.dispose();mf=null;
 mf=new Miniflare(opts);
 const second=await session(mf);
 assert.ok(second.hello.connection_generation>first.hello.connection_generation,"SQLite-backed generation survives worker restart");
 const inspected=await request(mf,"/installation/inspect");
 assert.equal(inspected.body.state,"BOUND");assert.equal(inspected.body.finance_authority,"native");assert.ok(!("risk" in inspected.body));
 assert.equal((await mf.dispatchFetch("http://localhost/installation/disable-legacy-finance",{method:"POST"})).status,404);
 second.ws.close();
 console.log("actual_workerd_native_finance_retirement_transport PASS auth, session, typed denial, ping, dispatch ACK/response, SQLite generation restart; no financial lifecycle claim");
} finally {
 if(mf)await mf.dispose();
 await rm(root,{recursive:true,force:true});
}
