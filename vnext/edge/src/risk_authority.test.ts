import {handleNativeRiskCommand} from "./risk_authority.js";
import {RuntimeSession} from "./runtime_session.js";
import {FRAME_SCHEMA} from "./shared.js";
function check(v:boolean,label:string){if(!v)throw Error(label);}
const noStorage=new Proxy({}, {get(){throw Error("OBSOLETE_FINANCE_TOUCHED_STORAGE");}});
const session={connection_generation:3,runtime_profile:"engine",hello:true};
const actions=["INSTALL_ENTRY_OPENING","ACCOUNT_SUMMARY","SUMMARY","INSPECT","RESERVE","BEGIN_POST","SETTLE_ENTRY","RELEASE_UNSENT","TRANSITION"];
for(const action of actions){
 const result=await handleNativeRiskCommand(noStorage,session,{schema:"okx.risk-command/v1",command_id:"retired_command_001",installation_id:"installation_test",action:{action}},Date.now());
 check(result.status==="DENIED"&&result.reason==="NATIVE_FINANCE_AUTHORITY_REQUIRED","obsolete action rejected: "+action);
 check(result.risk_book_sequence===null&&result.policy_revision===null&&result.reservation_key===null&&!("budget" in result)&&!("account_budget" in result),"denial exposes no financial authority");
}
for(const invalid of [{...session,hello:false},{...session,runtime_profile:"fixture"},{...session,connection_generation:0}]){
 check((await handleNativeRiskCommand(noStorage,invalid,{},Date.now())).reason==="NATIVE_PRODUCTION_SESSION_REQUIRED","native session required before denial");
}
const attachment={runtimeId:"windows-primary",sessionId:"session_test_012345",generation:3,hello:true,runtimeProfile:"engine",connectedAtMs:Date.now()};
let sent:any[]=[];let closed:any[]=[];
const ws={deserializeAttachment:()=>attachment,send:(text:string)=>sent.push(JSON.parse(text)),close:(...args:any[])=>closed.push(args)};
const owner=new RuntimeSession({storage:noStorage},{} as any);
const frame={type:"risk_control",schema:FRAME_SCHEMA,session_id:attachment.sessionId,connection_generation:3,command:{schema:"okx.risk-command/v1",command_id:"retired_command_001",action:{action:"RESERVE"}}};
await owner.webSocketMessage(ws,JSON.stringify(frame));
check(sent.length===1&&sent[0].type==="risk_control_result"&&sent[0].result.reason==="NATIVE_FINANCE_AUTHORITY_REQUIRED","authenticated typed adapter returns exact retirement reason");
sent=[];
await owner.webSocketMessage(ws,JSON.stringify({...frame,connection_generation:2}));
check(sent.length===0&&closed.some(c=>c[0]===1008),"stale generation cannot receive financial RPC response");
closed=[];
await owner.webSocketMessage(ws,JSON.stringify({...frame,session_id:"wrong_session"}));
check(sent.length===0&&closed.some(c=>c[0]===1008),"wrong session cannot receive financial RPC response");
console.log("obsolete_financial_rpc_denied_without_storage_and_session_bound PASS");
