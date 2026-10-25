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
