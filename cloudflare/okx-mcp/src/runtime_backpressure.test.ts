// #398 A4: exercise the ACTUAL Durable Object RuntimeSession scheduler.
// Simulated WebSocket and central storage, not a physical network latency proof.
import {RuntimeSession} from './runtime_session.js';
import {FRAME_SCHEMA,MAX_INFLIGHT,MAX_READ_INFLIGHT,MAX_READ_WAITERS} from './shared.js';
import {initial,account} from './account_governance.test.js';

function check(value:unknown,reason:string){if(!value)throw Error(reason);}
const generation=19;
const attachment={runtimeId:'windows-primary',runtimeProfile:'engine',
 sessionId:'session_backpressure_398',generation,hello:true,
 connectedAtMs:Date.now(),lastPongAtMs:Date.now()};
const policyStore=new Map<string,any>([['account_access_v1',{...initial,revision:1,accounts:[account]}]]);
const storage={
 async get(key:string){return structuredClone(policyStore.get(key));},
 async put(key:string,value:unknown){policyStore.set(key,structuredClone(value));},
 async transaction<T>(fn:(tx:typeof storage)=>Promise<T>):Promise<T>{
  const backup=structuredClone(policyStore);
  try{return await fn(this);}catch(error){policyStore.clear();for(const [k,v] of backup)policyStore.set(k,v);throw error;}
 }
};
let session:any;
let readActive=0,maxReadActive=0,totalActive=0,maxTotalActive=0;
let sentReads=0,sentPriority=0,maxQueued=0,policyPublished=0,stopDuringLoad=false;
const identifiers=new Set<string>();
const latency:number[]=[];
let delayMs=32;
const ws={
 deserializeAttachment:()=>({...attachment}),serializeAttachment:()=>{},close:()=>{},
 send:(body:string)=>{
  const frame=JSON.parse(body);
  if(frame.type==='ping'){
   queueMicrotask(()=>session.resolvePending('pong:'+frame.nonce,generation,{}));
   return;
  }
  if(frame.type==='policy_snapshot'){
   policyPublished++;
   if(readActive>0)stopDuringLoad=true;
   return;
  }
  check(frame.type==='request','unexpected fixture WSS frame');
  const id=frame.request.request_id as string;
  check(!identifiers.has(id),'same id delivered more than once');
  identifiers.add(id);
  const kind=frame.request.operation.type;
  const privileged=kind==='trade';
  if(privileged)sentPriority++;else{sentReads++;readActive++;maxReadActive=Math.max(maxReadActive,readActive);}
  totalActive++;maxTotalActive=Math.max(maxTotalActive,totalActive);
  check(totalActive<=MAX_INFLIGHT,'real owner exceeded native delivery budget');
  check(readActive<=MAX_READ_INFLIGHT,'ordinary reads occupied priority lane');
  setTimeout(()=>{
   session.resolvePending('ack:'+id,generation,{});
   session.resolvePending('response:'+id,generation,{status:'completed',id});
   totalActive--;if(!privileged)readActive--;
  },delayMs);
 }};
session=new RuntimeSession({
 storage,getWebSockets:()=>[ws],
},{RUNTIME_TOKEN_SHA256:'a'.repeat(64)} as any);
const rpc=(id:string,operation:any,accountKey?:string)=>
 session.fetch(new Request('https://fixture/dispatch',{method:'POST',
  body:JSON.stringify({schema:'okx.agent.request/v1',request_id:id,
   ...(accountKey?{account:accountKey}:{}),operation})})).then((r:Response)=>r.json());
const read=(id:string)=>rpc(id,{type:'portfolio',request:{action:'summary'}});
const waitActive=async(expected:number)=>{
 for(let i=0;i<100&&readActive<expected;i++)
  await new Promise(resolve=>setTimeout(resolve,1));
 check(readActive===expected,'synthetic socket read saturation not reached');
};
const started=performance.now();
const calls=Array.from({length:100},(_,i)=>{
 const timestamp=performance.now();
 return read('read_100_'+i).then((result:any)=>{
  latency.push(performance.now()-timestamp);return result;
 });
});
await waitActive(MAX_READ_INFLIGHT);
const status0:any=await(await session.fetch(new Request('https://fixture/status'))).json();
maxQueued=Math.max(maxQueued,status0.readonly_queue_depth);
check(status0.readonly_inflight===MAX_READ_INFLIGHT &&
 status0.readonly_queue_depth>0 &&
 status0.readonly_queue_depth<=MAX_READ_WAITERS &&
 status0.readonly_queue_capacity===MAX_READ_WAITERS,'bounded observable queue');
const close=rpc('priority_close_398',{type:'trade',
 request:{action:'close',reference:'intent_fixture',idempotency_key:'close_fixture_398',
 expires_in_ms:30000}},account.id);
const stop:any=await(await session.fetch(new Request('https://fixture/account-governance/stop',{
 method:'POST',body:JSON.stringify({idempotency_key:'stop_fixture_398_01'})}))).json();
check(stop.status==='CONFIGURED'&&stop.accounts[0].explicit_pause===true,'STOP persisted despite saturated read queue');
check(stopDuringLoad&&policyPublished>0,'STOP policy published while reads active');
check((await close).status==='completed','protective close occupied reserved delivery slot');
const terminal=await rpc('priority_status_398',{type:'trade',
 request:{action:'status',reference:'intent_fixture'}},account.id);
check(terminal.status==='completed','exact intent status bypassed the read-only queue');
const answers=await Promise.all(calls);
check(answers.every((a:any)=>a.status==='completed'),'100 read-only jobs were lost or marked BUSY');
check(sentReads===100&&sentPriority===2,'unexpected native fixture send count');
check(maxReadActive===MAX_READ_INFLIGHT && maxTotalActive===MAX_INFLIGHT,
 'reserved native slot was not demonstrated');
const end:any=await(await session.fetch(new Request('https://fixture/status'))).json();
check(end.inflight===0&&end.readonly_inflight===0&&end.readonly_queue_depth===0 &&
 session.activeRequests.size===0 && session.pending.size===0,'all waiters, active ids and ACK stages settled');
const ordered=[...latency].sort((a,b)=>a-b);
check(ordered.length===100,'exactly 100 timings');
const report={schema:'okx.runtime-backpressure.fixture/v1',operations:100,
 completed:answers.length,priority_completed:2,real_exchange_posts:0,
 max_read_inflight:maxReadActive,max_total_inflight:maxTotalActive,
 queued_observed:maxQueued,queue_capacity:MAX_READ_WAITERS,
 stop_policy_published:stopDuringLoad,elapsed_ms:Math.ceil(performance.now()-started),
 p50_ms:Math.round(ordered[49]*1000)/1000,p95_ms:Math.round(ordered[94]*1000)/1000,
 network_p95_measured:false,simulation:'in_process_fake_ws_not_physical_network'};
console.log('runtime_session_100_bounded_priority_fixture PASS '+JSON.stringify(report));

// Saturation must fail without ever delivering extra reads, and should not
// create infinite queue capacity or an inferred mutation outcome.
delayMs=140;
const beforeSent=sentReads;
const burst=Array.from({length:MAX_READ_WAITERS+MAX_READ_INFLIGHT+15},(_,i)=>
 read('burst_'+i));
await waitActive(MAX_READ_INFLIGHT);
const overloaded:any=await(await session.fetch(new Request('https://fixture/status'))).json();
check(overloaded.readonly_queue_depth===MAX_READ_WAITERS,'queue must enforce hard 100-call bound');
const outcomes=await Promise.all(burst);
const busy=outcomes.filter((x:any)=>x.status==='FAILED'&&x.reason==='TRANSPORT_BUSY');
const succeeded=outcomes.filter((x:any)=>x.status==='completed');
check(busy.length>=15&&succeeded.length<=MAX_READ_WAITERS+MAX_READ_INFLIGHT,
 'overflow must reject BEFORE any extra native delivery');
check(sentReads-beforeSent===succeeded.length &&
 session.activeRequests.size===0 && session.pending.size===0,
 'full queue never dispatched rejected requests or leaked pending waiters');
console.log('runtime_session_queue_overflow_fail_closed PASS '+JSON.stringify({
 requested:burst.length,completed:succeeded.length,busy:busy.length,
 delivered_to_fixture:sentReads-beforeSent,real_exchange_posts:0}));
