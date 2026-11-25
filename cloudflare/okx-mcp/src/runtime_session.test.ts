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
      check(owner.pending.has(`${generation}:response:${frame.request.request_id}`),"old_generation_cannot_complete_new_request");
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
 check(owner.pending.size===0&&owner.admissions.engine.inflight===0,"all stages settle and capacity is released");
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
 const req=(operation:any,target_owner?:string)=>({schema:"okx.agent.request/v1",request_id:"request_research_012345",...(target_owner?{target_owner}:{}),operation});
 check((await instance.dispatch(req({type:"trade",request:{action:"status"}},"research"))).reason==="RESEARCH_OPERATION_FORBIDDEN","trade status denied");
 check((await instance.dispatch(req({type:"portfolio",request:{action:"summary"}},"research"))).reason==="RESEARCH_OPERATION_FORBIDDEN","portfolio denied");
 check((await instance.dispatch(req({type:"diagnostics",request:{view:"finance"}},"research"))).reason==="RESEARCH_OPERATION_FORBIDDEN","finance diagnostics denied");
 check((await instance.dispatch(req({type:"research",request:{action:"capabilities"}}))).status==="completed","research reaches session");
 for(const action of ["backtest","grid","cancel","record_decision"]){
   check((await instance.dispatch(req({type:"research",request:{action}}))).status==="completed",`${action} reaches research session without finance`);
 }
 check(sent.filter(f=>f.type==="request").length===5,"all research requests delivered");
 check(sent.filter(f=>f.type==="request").every(f=>!("operator_governance" in f)),"no governance delivered");
 await instance.webSocketMessage(ws,JSON.stringify({type:"ping",schema:FRAME_SCHEMA,session_id:stored.sessionId,connection_generation:8,nonce:"stale"}));
 check(closed==="stale generation","stale generation denied");
}

// Distinct owner sockets coexist. Research reconnect must not revoke the
// financial lane; owner-directed reads never silently fall back to Engine.
{
 const finance={...attachment,sessionMode:"finance" as const,generation:15,sessionId:"finance_15"};
 const research={...attachment,sessionMode:"research_only" as const,runtimeProfile:"research" as const,credentialOwner:"research" as const,generation:16,sessionId:"research_16"};
 const sent:{owner:string;frame:any}[]=[];
 let owner:any;
 let financeClosed="",researchClosed="";
 const mk=(ownerName:string,initial:any)=>{
   let stored={...initial};
   return {
     deserializeAttachment:()=>({...stored}),
     serializeAttachment:(value:any)=>{stored=value;},
     close:(_:number,why:string)=>{if(ownerName==="finance")financeClosed=why;else researchClosed=why;},
     send:(message:string)=>{
       const frame=JSON.parse(message);sent.push({owner:ownerName,frame});
       if(frame.type==="ping")queueMicrotask(()=>owner.resolvePending(`pong:${frame.nonce}`,stored.generation,{}));
       if(frame.type==="request")queueMicrotask(()=>{
         owner.resolvePending(`ack:${frame.request.request_id}`,stored.generation,{});
         owner.resolvePending(`response:${frame.request.request_id}`,stored.generation,{status:"completed",owner:ownerName});
       });
     },
   };
 };
 const financeWs=mk("finance",finance);
 const researchWs=mk("research",research);
 let sockets:any[]=[financeWs,researchWs];
 const state={storage:{get:async(k:string)=>k==="account_access_v1"?{...initial,revision:1,accounts:[account]}:undefined},getWebSockets:()=>sockets};
 const ownerEnv:any={};
 owner=new RuntimeSession(state,ownerEnv);
 const req=(id:string,target_owner:string,operation:any)=>({schema:"okx.agent.request/v1",request_id:id,target_owner,operation});
 check((await owner.dispatch(req("direct_research_1","research",{type:"research",request:{action:"capabilities"}}))).owner==="research",
   "direct Research routed only to research owner");
 check((await owner.dispatch(req("direct_finance_1","engine",{type:"portfolio",request:{action:"summary"}}))).owner==="finance",
   "Engine financial reads stay on finance owner");
 check(sent.filter(s=>s.frame.type==="request").map(s=>s.owner).join(",")==="research,finance",
   "distinct owner routes share DO without accidental cross-delivery");
 // Edge's target_owner is an internal routing decision. Native Rust uses
 // #[serde(deny_unknown_fields)] AgentRequest, so even a correctly routed
 // frame fails parsing if the routing hint crosses the WSS boundary.
 const nativeRequestKeys=["schema","request_id","operation"];
 const nativeRequests=()=>sent.filter(s=>s.frame.type==="request");
 check(nativeRequests().every(s=>JSON.stringify(Object.keys(s.frame.request).sort())===JSON.stringify([...nativeRequestKeys].sort())),
   "Research and Engine native AgentRequest contain only Rust contract fields");
 check(nativeRequests()[0].frame.request.operation.type==="research"&&
       nativeRequests()[1].frame.request.operation.type==="portfolio",
   "removing routing metadata preserves each native operation unchanged");

 // A valid Research hello replaces only the Research generation.
 const nextResearch=mk("research",{runtimeId:"windows-primary",sessionId:"research_17",generation:17,hello:false,credentialOwner:"research",connectedAtMs:Date.now()});
 sockets.push(nextResearch);
 const hello={type:"hello_research",schema:FRAME_SCHEMA,runtime_id:"windows-primary",connection_id:"native_0123456789abcdef0123456789abcdef",runtime_profile:"research",host_scope:"a".repeat(64),contract:"okx.product/v1",agent_pid:12,agent_binary_sha256:"b".repeat(64),agent_incarnation:"ctl_"+"c".repeat(32)};
 await owner.webSocketMessage(nextResearch,JSON.stringify(hello));
 check(researchClosed==="owner new generation"&&!financeClosed,"Research reconnect leaves Engine running");
 sockets=[financeWs,nextResearch];
 check((await owner.dispatch(req("direct_research_2","research",{type:"research",request:{action:"capabilities"}}))).owner==="research",
   "Research reconnect continues serving independently");

 // Reconnecting the financial owner must not close Research either.
 researchClosed="";
 const nextFinance=mk("finance",{runtimeId:"windows-primary",sessionId:"finance_18",generation:18,hello:false,connectedAtMs:Date.now()});
 sockets.push(nextFinance);
 await owner.webSocketMessage(nextFinance,JSON.stringify({type:"hello",schema:FRAME_SCHEMA,runtime_id:"windows-primary",connection_id:"native_0123456789abcdef0123456789abcdef",runtime_profile:"engine",contract:"okx.product/v1",installation_id:initial.installation_id,agent_pid:13,agent_binary_sha256:"b".repeat(64),agent_incarnation:"ctl_"+"d".repeat(32)}));
 check(financeClosed==="owner new generation"&&!researchClosed,"Engine reconnect leaves Research alive");
 const researchWithFinanceHello=mk("research",{runtimeId:"windows-primary",sessionId:"research_invalid",generation:19,hello:false,credentialOwner:"research",connectedAtMs:Date.now()});
 let invalidClosed="";
 researchWithFinanceHello.close=(_:number,why:string)=>{invalidClosed=why;};
 const invalidFinanceHello={type:"hello",schema:FRAME_SCHEMA,runtime_id:"windows-primary",connection_id:"native_0123456789abcdef0123456789abcdef",runtime_profile:"engine",contract:"okx.product/v1",installation_id:initial.installation_id,agent_pid:14,agent_binary_sha256:"b".repeat(64),agent_incarnation:"ctl_"+"e".repeat(32)};
 financeClosed="";
 await owner.webSocketMessage(researchWithFinanceHello,JSON.stringify(invalidFinanceHello));
 check(invalidClosed==="installation or contract mismatch"&&!financeClosed&&!researchClosed,
   "Research credential cannot replace finance session");
 sockets=[nextFinance,nextResearch];
 check((await owner.dispatch(req("direct_research_4","research",{type:"research",request:{action:"capabilities"}}))).owner==="research",
   "Research survives Engine reconnect");

 // With only Engine present, explicitly owner-scoped Research must fail closed.
 sockets=[financeWs];
 check((await owner.dispatch(req("direct_research_3","research",{type:"research",request:{action:"capabilities"}}))).reason==="RESEARCH_RUNTIME_OFFLINE",
   "Research offline must never route through Agent");
 check((await owner.dispatch(req("direct_finance_2","engine",{type:"portfolio",request:{action:"summary"}}))).owner==="finance",
   "Research loss cannot disable Engine reads");
 check((await owner.dispatch(req("direct_finance_3","research",{type:"trade",request:{action:"status"}}))).reason==="RESEARCH_OPERATION_FORBIDDEN",
   "finance operations cannot target Research even if offline");
 check((await owner.dispatch(req("direct_finance_4","unknown",{type:"portfolio",request:{action:"summary"}}))).reason==="INVALID_OWNER",
   "unknown owner cannot route to any session");
 sockets=[nextResearch];
 check((await owner.dispatch(req("direct_finance_5","engine",{type:"portfolio",request:{action:"summary"}}))).reason==="RUNTIME_OFFLINE",
   "Engine does not fall back to Research if its own lane is offline");

 // Legacy Engine-authenticated research_only sessions do not represent an
 // independent Research owner, even when they are the latest socket.
 const legacyResearch=mk("legacy",{...attachment,sessionMode:"research_only",runtimeProfile:"engine",credentialOwner:"engine",generation:20,sessionId:"legacy_research_20"});
 let legacyClosed="";
 legacyResearch.close=(_:number,why:string)=>{legacyClosed=why;};
 sockets=[financeWs,legacyResearch];
 check((await owner.dispatch(req("direct_research_6","research",{type:"research",request:{action:"capabilities"}}))).reason==="RESEARCH_RUNTIME_OFFLINE",
   "Engine bearer research_only never masquerades as independent Research");
 sockets=[legacyResearch];
 const legacyRequest={schema:"okx.agent.request/v1",request_id:"legacy_research_21",operation:{type:"research",request:{action:"capabilities"}}};
 check((await owner.dispatch(legacyRequest)).owner==="legacy",
   "unmarked legacy research_only remains available during cutover without finance permission");
 const nextIndependent=mk("research",{runtimeId:"windows-primary",sessionId:"research_22",generation:22,hello:false,credentialOwner:"research",connectedAtMs:Date.now()});
 sockets.push(nextIndependent);
 await owner.webSocketMessage(nextIndependent,JSON.stringify(hello));
 check(!legacyClosed,"new independent Research hello does not evict legacy Engine-owned research_only");
 check((await owner.dispatch(req("direct_research_7","research",{type:"research",request:{action:"capabilities"}}))).owner==="research",
   "explicit Research route chooses separately credentialed Research, not legacy");

 // Configuring a distinct Research credential turns owner separation on for
 // every unmarked internal dispatch as well as the public MCP tool facade.
 ownerEnv.RESEARCH_RUNTIME_TOKEN_SHA256="f".repeat(64);
 sockets=[financeWs,legacyResearch];
 const unmarked=(id:string,operation:any,account?:string)=>({schema:"okx.agent.request/v1",request_id:id,
   ...(account?{account}:{}),operation});
 const countSent=()=>sent.filter(s=>s.frame.type==="request").length;
 const sentBeforeOffline=countSent();
 check((await owner.dispatch(unmarked("configured_research_offline",{type:"research",request:{action:"capabilities"}}))).reason==="RESEARCH_RUNTIME_OFFLINE",
   "configured Research outage never silently falls back to Engine or legacy Engine-bearer Research");
 check((await owner.dispatch(unmarked("configured_market_offline",{type:"market_capabilities"}))).reason==="RESEARCH_RUNTIME_OFFLINE",
   "configured Market read is independent from financial Engine liveness");
 check((await owner.dispatch(unmarked("configured_scenario_offline",{type:"portfolio_scenario",request:{equity_usd:"100",positions:[],correlations:[]}}))).reason==="RESEARCH_RUNTIME_OFFLINE",
   "virtual portfolio scenario cannot fall back to finance when Research is offline");
 check(countSent()===sentBeforeOffline,"configured Research outage sends no data to finance or legacy socket");
 check((await owner.dispatch(unmarked("configured_finance_online",{type:"portfolio",request:{action:"summary"}}))).owner==="finance",
   "financial reads continue when configured Research is offline");
 check((await owner.dispatch(req("configured_forced_engine","engine",{type:"research",request:{action:"capabilities"}}))).reason==="RESEARCH_OWNER_REQUIRED",
   "explicit Engine override cannot bypass configured Research ownership");
 check((await owner.dispatch(req("configured_finance_direct","engine",{type:"portfolio",request:{action:"summary"}}))).owner==="finance",
   "explicit Engine finance route remains permitted after Research owner cutover");
 check(nativeRequests().every(s=>!("target_owner" in s.frame.request)),
   "configured and legacy Engine paths also strip owner hint from native frames");
 sockets=[financeWs,nextResearch];
 check((await owner.dispatch(unmarked("configured_research_online",{type:"research",request:{action:"capabilities"}}))).owner==="research",
   "configured unmarked Research reaches independent owner while finance is online");
 check((await owner.dispatch(unmarked("configured_diagnostics_online",{type:"diagnostics",request:{view:"research"}}))).owner==="research",
   "Research diagnostics do not silently use Engine");
 check((await owner.dispatch(unmarked("configured_scenario_online",{type:"portfolio_scenario",request:{equity_usd:"100",positions:[],correlations:[]}}))).owner==="research",
   "pure virtual portfolio scenario uses independent Research without finance credentials");
 check(nativeRequests().every(s=>!("target_owner" in s.frame.request)) &&
       nativeRequests().filter(s=>s.owner==="research").every(s=>!("account" in s.frame.request)&&!("operator_governance" in s.frame)),
   "Research native frames expose neither routing hints, finance accounts nor governance");
 const sentBeforeForged=countSent();
 check((await owner.dispatch(unmarked("configured_nested_account",{type:"market",request:{action:"screen",account:"finance_account"}}))).reason==="RESEARCH_OPERATION_FORBIDDEN",
   "nested finance account in Research operation fails closed instead of being reclassified as Engine");
 check((await owner.dispatch(unmarked("configured_top_account",{type:"research",request:{action:"capabilities"}},"finance_account"))).reason==="RESEARCH_OPERATION_FORBIDDEN",
   "top-level finance account in Research operation fails closed before socket delivery");
 check(countSent()===sentBeforeForged,"forged account selector never reaches either owner");
}
