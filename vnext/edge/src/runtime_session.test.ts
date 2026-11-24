import {RuntimeSession} from "./runtime_session.js";
import {FRAME_SCHEMA} from "./shared.js";
import {initial,account} from "./account_governance.test.js";
function check(v:boolean,label:string){if(!v)throw Error(label);}
const generation=4;
const attachment={runtimeId:"windows-primary",sessionId:"session_test_012345",generation,hello:true,
 runtimeProfile:"engine",connectedAtMs:Date.now(),lastPongAtMs:Date.now()};
async function fixture(mode:"before_ack"|"after_ack"|"complete"|"stale"|"ping_send_failure"|"request_send_failure"|"duplicate"|"paused_complete"){
 const map=new Map<string,unknown>([["account_access_v1",{...initial,revision:1,accounts:[mode==="paused_complete"?{...account,explicit_pause:true}:account]}]]);
 let owner:any;
 const ws={deserializeAttachment:()=>({...attachment}),serializeAttachment:()=>{},
  send:(text:string)=>{
   const frame=JSON.parse(text);
   if(mode==="ping_send_failure"||mode==="request_send_failure"&&frame.type==="request")throw Error("socket closed");
   if(frame.type==="ping")queueMicrotask(()=>owner.resolvePending(`pong:${frame.nonce}`,generation,{}));
   else if(frame.type==="request")queueMicrotask(()=>{
    if(mode==="before_ack"){void owner.webSocketClose(ws);return;}
    owner.resolvePending(`ack:${frame.request.request_id}`,generation,{});
    if(mode==="after_ack"){void owner.webSocketClose(ws);return;}
    if(mode==="stale"){
      owner.resolvePending(`response:${frame.request.request_id}`,generation-1,{status:"WRONG"});
      check(owner.pending.has(`response:${frame.request.request_id}`),"old_generation_cannot_complete_new_request");
    }
    owner.resolvePending(`response:${frame.request.request_id}`,generation,{status:"completed"});
   });
  },close:()=>{}};
 const state={storage:{get:async(k:string)=>map.get(k)},getWebSockets:()=>[ws]};
 owner=new RuntimeSession(state,{} as any);
 const operation=(mode==="after_ack"||mode==="paused_complete")?{type:"trade",request:{action:"close",reference:"intent_test_012345"}}:{type:"portfolio",request:{action:"summary"}};
 const request={schema:"okx.agent.request/v1",request_id:"request_test_012345",operation};
 const running=owner.dispatch(request);
 if(mode==="duplicate")check((await owner.dispatch(request)).reason==="REQUEST_ALREADY_IN_FLIGHT","duplicate request rejected before async freshness");
 const result=await running;
 check(owner.pending.size===0&&owner.inflight===0,"all stages settle and capacity is released");
 return result;
}
const before=await fixture("before_ack");
check(before.reason==="RUNTIME_OFFLINE","disconnect_before_ack_settles_all_promises");
const after=await fixture("after_ack");
check(after.reason==="MUTATION_OUTCOME_UNKNOWN_RECONCILE_EXACT_INTENT"&&after.retryable===false,
 "disconnect_after_acceptance_is_unknown");
check((await fixture("complete")).status==="completed","ACK and response complete once");
check((await fixture("stale")).status==="completed","fresh response completes after stale rejected");

check((await fixture("ping_send_failure")).reason==="RUNTIME_OFFLINE","synchronous ping failure settles pending");
check((await fixture("request_send_failure")).status==="FAILED","synchronous request failure settles both stages");

check((await fixture("duplicate")).status==="completed","duplicate request cannot clear original pending stages");

check((await fixture("paused_complete")).status==="completed","pause_allows_native_verified_close_delivery");

// Windows CONTROL is the sole owner of native binary installation evidence.
// Runtime status reports authenticated liveness, not independent file proof.
const now=Date.now();
let online=true;
const socket:any={...attachment,agentPid:5551,agentBinarySha256:'b'.repeat(64),
 agentIncarnation:'ctl_'+'1'.repeat(32),installationId:'okx-clean-398-20261023',
 connectedAtMs:now-10000,lastPongAtMs:now};
const owner=new RuntimeSession({
 storage:{get:async()=>null},
 getWebSockets:()=>online?[{deserializeAttachment:()=>socket}]:[],
},{} as any);
const ready:any=await(await owner.fetch(new Request('https://runtime.internal/status'))).json();
check(ready.status==='PASS'&&ready.agent_pid===5551&&ready.agent_incarnation===socket.agentIncarnation,
 'transport returns Agent-reported identity without claiming CONTROL proof');
socket.generation=5;
socket.connectedAtMs=now-5000;
const reconnected:any=await(await owner.fetch(new Request('https://runtime.internal/status'))).json();
check(reconnected.status==='PASS'&&reconnected.connection_generation===5,
 'same child WSS reconnect has no GitHub REST dependency');
online=false;
const offline:any=await(await owner.fetch(new Request('https://runtime.internal/status'))).json();
check(offline.status==='OFFLINE'&&offline.connected===false,
 'offline transport stays offline independently of native installation');
console.log('runtime_native_owner_and_wss_separation PASS');

// The bearer-authenticated scope is a namespace claim, not key attestation.
{
 let stored:any={runtimeId:"windows-primary",sessionId:"session_research_0123",generation:9,hello:false,connectedAtMs:Date.now()};
 let closed=""; const sent:any[]=[]; let instance:any;
 const ws:any={deserializeAttachment:()=>({...stored}),serializeAttachment:(v:any)=>{stored=v;},close:(_:number,why:string)=>{closed=why;},send:(text:string)=>{
   const frame=JSON.parse(text);sent.push(frame);
   if(frame.type==="ping")queueMicrotask(()=>instance.resolvePending(`pong:${frame.nonce}`,9,{}));
   if(frame.type==="request")queueMicrotask(()=>{instance.resolvePending(`ack:${frame.request.request_id}`,9,{});instance.resolvePending(`response:${frame.request.request_id}`,9,{status:"completed"});});
 }};
 instance=new RuntimeSession({storage:{get:async()=>undefined},getWebSockets:()=>[ws]} as any,{} as any);
 const hello:any={type:"hello_research",schema:FRAME_SCHEMA,runtime_id:"windows-primary",connection_id:"native_0123456789abcdef0123456789abcdef",runtime_profile:"engine",host_scope:"a".repeat(64),contract:"okx.product/v1",agent_pid:12,agent_binary_sha256:"b".repeat(64),agent_incarnation:"ctl_"+"c".repeat(32)};
 await instance.webSocketMessage(ws,JSON.stringify({...hello,installation_id:"fake_install"}));
 check(closed==="invalid research hello"&&!stored.hello,"mixed finance claim rejected");
 closed="";await instance.webSocketMessage(ws,JSON.stringify({...hello,host_scope:"BAD"}));
 check(closed==="invalid research hello"&&!stored.hello,"invalid scope rejected");
 closed="";await instance.webSocketMessage(ws,JSON.stringify({...hello,connection_id:""}));
 check(closed==="invalid research hello"&&!stored.hello,"invalid connection id rejected");
 closed="";await instance.webSocketMessage(ws,JSON.stringify(hello));
 check(stored.hello&&stored.sessionMode==="research_only"&&!stored.installationId,"research namespace only");
 check(sent.some(f=>f.type==="hello_ack")&&!sent.some(f=>f.type==="policy_snapshot"),"no policy");
 instance.state.storage.get=async()=>{throw new Error("research must not read governance");};
 const req=(operation:any)=>({schema:"okx.agent.request/v1",request_id:"request_research_012345",operation});
 check((await instance.dispatch(req({type:"trade",request:{action:"status"}}))).reason==="RESEARCH_OPERATION_FORBIDDEN","trade status denied");
 check((await instance.dispatch(req({type:"portfolio",request:{action:"summary"}}))).reason==="RESEARCH_OPERATION_FORBIDDEN","portfolio denied");
 check((await instance.dispatch(req({type:"diagnostics",request:{view:"finance"}}))).reason==="RESEARCH_OPERATION_FORBIDDEN","finance diagnostics denied");
 check((await instance.dispatch(req({type:"research",request:{action:"capabilities"}}))).status==="completed","research reaches session");
 for(const action of ["backtest","grid","cancel","record_decision"]){
   check((await instance.dispatch(req({type:"research",request:{action}}))).status==="completed",`${action} reaches research session without finance`);
 }
 check(sent.filter(f=>f.type==="request").length===5,"all research requests delivered");
 check(sent.filter(f=>f.type==="request").every(f=>!("operator_governance" in f)),"no governance delivered");
 await instance.webSocketMessage(ws,JSON.stringify({type:"ping",schema:FRAME_SCHEMA,session_id:stored.sessionId,connection_generation:8,nonce:"stale"}));
 check(closed==="stale generation","stale generation denied");
}
