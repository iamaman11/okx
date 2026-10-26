// #398 A2/A3: real Worker routing + actual central governance transaction owner,
// with a stateful fake Agent/venue. Never classify this as live WSS/native SQL.
import {mcpApi} from './mcp.js';
import {RuntimeSession} from './runtime_session.js';
import {FRAME_SCHEMA} from './shared.js';
import {governanceRequest,initializeGovernance,enrollObservedAccount,setAccountPause,setAccountLimits} from './account_governance_store.js';
import {initial,limits} from './account_governance.test.js';
import type {Governance} from './account_governance.js';
import {handleNativeRiskCommand} from './risk_authority.js';
import {readRiskBook} from './risk_book.js';

function check(ok:unknown,message:string):asserts ok{if(!ok)throw Error(message);}
class Storage {
 map=new Map<string,any>();
 private queue=Promise.resolve();
 async get(key:string){return structuredClone(this.map.get(key));}
 async put(key:string,value:unknown){this.map.set(key,structuredClone(value));}
 async delete(keys:string[]){let count=0;for(const k of keys)if(this.map.delete(k))count++;return count;}
 async transaction<T>(fn:(tx:Storage)=>Promise<T>):Promise<T>{
  const preceding=this.queue;let unlock!:()=>void;
  this.queue=new Promise<void>(resolve=>{unlock=resolve;});await preceding;
  const original=structuredClone(this.map);
  try{return await fn(this);}catch(error){this.map=original;throw error;}finally{unlock();}
 }
}
const storage=new Storage();
// This trace is emitted only during CI's native SQL bridge test. It records
// actual Worker governance transactions, not mocked native acknowledgements.
const nativePolicyTrace:{schema:string;source:string;events:Array<{
 stage:string;governance:Record<string,unknown>;frame:Record<string,unknown>;frame_count:number}>}={
 schema:'okx.a2.worker-sql-cas-trace/v1',
 source:'mcpApi.fetch+account_governance_store.transaction',
 events:[]
};
const policyFrames:Array<Record<string,unknown>>=[];
const generation=4;
const policySocket={
 deserializeAttachment:()=>({runtimeId:'windows-primary',runtimeProfile:'engine',
  sessionId:'session_test_012345',generation,hello:true,
  connectedAtMs:Date.now()-1000,lastPongAtMs:Date.now()}),
 send:(raw:string)=>{
  const frame=JSON.parse(raw);
  check(frame.type==='policy_snapshot'&&frame.schema===FRAME_SCHEMA,
   'native policy fixture must observe only genuine RuntimeSession policy frames');
  policyFrames.push(structuredClone(frame));
 },
 close:()=>{throw Error('no policy-frame send failures expected')}
};
const runtimeOwner=new RuntimeSession(
 {storage,getWebSockets:()=>[policySocket]},{} as any);
async function recordNativePolicy(stage:string):Promise<void>{
 const snapshot=await governanceRequest(storage,'GET');
 check(snapshot.status===200,'central CAS trace must come from committed storage');
 const {status,access,readiness,last_admission,complete_all_account_census,
  available_for_new_risk_usdt,...governance}=snapshot.body;
 const frame=policyFrames.at(-1);
 check(frame&&frame.type==='policy_snapshot','native bridge requires outbound DO WSS policy');
 const outgoing=frame.snapshot as Record<string,unknown>;
 check(JSON.stringify(outgoing.state)===JSON.stringify(governance),
  'outbound DO frame must reflect committed central owner state');
 nativePolicyTrace.events.push({
  stage,governance:structuredClone(governance),frame:structuredClone(frame),
  frame_count:policyFrames.length
 });
}
check((await initializeGovernance(storage,initial)).status===200,'synthetic central installation');
const ctx={auth:{scope:['mcp:use']}};
let id=0,venuePostCount=0,transportCalls=0;
const intents=new Map<string,{intent_id:string;state:string;plan:Record<string,unknown>;protected:boolean}>();
const bodyHash=new Map<string,string>();
const intentByKey=new Map<string,string>();
const verifiedUid='a'.repeat(64);
const accountKey='okx:demo:'+verifiedUid;
const success=(data:unknown)=>({schema:'okx.agent.response/v1',request_id:'fixture_native_'+(++id),
 status:'completed',quality:'FRESH',warnings:[],result:{status:'completed',as_of_ms:1792890000000,
 data,quality:{state:'fresh'},warnings:[]}});
const rejected=(reason:string)=>({schema:'okx.agent.response/v1',request_id:'fixture_native_'+(++id),
 status:'rejected',quality:'NOT_READY',warnings:[],result:null,
 failure:{code:reason,message:reason,retryable:false}});
const handler=async(url:string,init?:RequestInit)=>{
 transportCalls++;
 const path=new URL(url).pathname;
 const raw=init?.body?JSON.parse(String(init.body)):null;
 // Route through the actual Durable Object owner (including publishPolicy),
 // never call governanceStore mutation methods directly from this fixture.
 if(['/account-governance','/account-governance/enroll',
     '/account-governance/pause','/account-governance/limits'].includes(path))
  return runtimeOwner.fetch(new Request(url,{
   method:init?.method??'GET',
   headers:{'content-type':'application/json'},
   ...(init?.body?{body:String(init.body)}:{})
  }));
 check(path==='/dispatch','unexpected transport endpoint');
 const op=raw?.operation;
 if(op?.type==='connect_account'){
  return Response.json({status:'completed',result:{alias:op.alias,credential_ref:op.credential_ref,
   connect_operation_id:op.operation_id,configuration_digest:op.request_digest,
   account_key:{venue:'okx',environment:op.credential_ref==='credential_fixture_real'?'real':'demo',verified_uid:verifiedUid}}});
 }
 const req=op?.request;
 if(op?.type!=='trade'||!req)return Response.json(rejected('FAKE_OPERATION_UNSUPPORTED'));
 const governance=(await governanceRequest(storage,'GET')).body as any;
 const row=governance.accounts?.find((a:any)=>a.account_key===raw.account||a.id===raw.account);
 if(!row)return Response.json(rejected('ACCOUNT_NOT_FOUND'));
 if(req.action==='preflight')return Response.json(success({ready_for_new_risk:false,side_effects:'none'}));
 if(req.action==='preview')return Response.json(success({admission:'preview_only',
  plan:{instrument:req.instrument,stop_price:'99',planned_loss_usd:'0.5'},exchange_post:false}));
 if(req.action==='status'){
  const found=intents.get(req.reference);
  return Response.json(found?success({intent:found}):rejected('INTENT_NOT_FOUND'));
 }
 if(['execute','cancel','close'].includes(req.action)&&row.explicit_pause&&req.action==='execute'){
  return Response.json(rejected('ACCOUNT_NEW_RISK_PAUSED'));
 }
 if(req.action==='execute'){
  // Explicit synthetic Real branch: MCP routes to the fixture Agent, which
  // requests actual central Worker RiskBook RESERVE and BEGIN_POST decisions.
  // The invented native census/session here is TEST ONLY, never an MCP input.
  if(row.environment==='real'){
   const hash=JSON.stringify(req),key=String(req.idempotency_key);
   if(bodyHash.has(key)&&bodyHash.get(key)!==hash)
    return Response.json(rejected('IDEMPOTENCY_CONFLICT'));
   const existing=intentByKey.get(key);
   if(existing)return Response.json(success({intent:intents.get(existing)}));
   const at=1792890000000;
   const intentId='intent_real_'+(intents.size+1);
   const planHash=Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256',
     new TextEncoder().encode(JSON.stringify({account:row.account_key,request:req}))))).
     map(b=>b.toString(16).padStart(2,'0')).join('');
   const census={complete:true,source_digest:'c'.repeat(64),
     content_digest:'b'.repeat(64),generated_at_ms:at-1,source_read_floor_ms:at-1,
     account_keys:[row.account_key],occupied:[{account_key:row.account_key,
       occupied_usdt:'0',cumulative_loss_usd:'0'}],
     native_account_evidence_complete:true,native_order_evidence_complete:true,
     native_ledger_evidence_complete:true};
   const nativeSession={connection_generation:generation,runtime_profile:'engine',hello:true};
   const reserve=await handleNativeRiskCommand(storage,nativeSession,{
     schema:'okx.risk-command/v1',command_id:'reserve_fixture_'+(++id),
     installation_id:initial.installation_id,
     action:{action:'RESERVE',venue:'okx',account_uid:row.uid_fingerprint,
       run_id:'run_fixture_real_001',intent_id:intentId,
       strategy_id:String(req.strategy_instance),instrument_id:String(req.instrument),
       plan_sha256:planHash,requested_loss_usdt:'0.5',requested_loss_usd:'0.5',
       usd_conversion:{usd_per_usdt:'1',source_digest:census.source_digest,observed_at_ms:at},
       policy_revision:governance.revision,census}
   },at);
   if(reserve.status!=='HELD'||!reserve.reservation_key||
      reserve.risk_book_sequence===null)
    return Response.json(rejected(reserve.reason??'CENTRAL_RESERVE_DENIED'));
   const fresh={...census,source_digest:'d'.repeat(64),
     generated_at_ms:at+1,source_read_floor_ms:at+1};
   const fence=await handleNativeRiskCommand(storage,nativeSession,{
     schema:'okx.risk-command/v1',command_id:'post_fixture_'+(++id),
     installation_id:initial.installation_id,
     action:{action:'BEGIN_POST',reservation_key:reserve.reservation_key,
       plan_sha256:planHash,policy_revision:governance.revision,
       expected_risk_book_sequence:reserve.risk_book_sequence,
       source_proof_sha256:'e'.repeat(64),census:fresh}
   },at+2);
   if(fence.status!=='POST_AUTHORIZED')
    return Response.json(rejected(fence.reason??'CENTRAL_POST_FENCE_DENIED'));
   const intent={intent_id:intentId,state:'FILLED',plan:{instrument:req.instrument,
     operation:req.operation,protection:req.protection,stop_price:'99',
     planned_loss_usd:'0.5'},protected:true};
   // The fixture venue can POST only AFTER central risk says POST_AUTHORIZED.
   venuePostCount++;
   intents.set(intentId,intent);bodyHash.set(key,hash);intentByKey.set(key,intentId);
   return Response.json(success({intent}));
  }
  const hash=JSON.stringify(req),key=req.idempotency_key;
  if(bodyHash.has(key)&&bodyHash.get(key)!==hash)return Response.json(rejected('IDEMPOTENCY_CONFLICT'));
  const prior=intentByKey.get(key);
  if(prior)return Response.json(success({intent:intents.get(prior)}));
  const intent_id='intent_fake_'+(intents.size+1);
  const intent={intent_id,state:'FILLED',plan:{instrument:req.instrument,operation:req.operation,
   protection:req.protection,stop_price:'99',planned_loss_usd:'0.5'},protected:true};
  // Fake-venue POST counter measures fixture operations, NEVER real exchange POST.
  venuePostCount++;
  intents.set(intent_id,intent);bodyHash.set(key,hash);intentByKey.set(key,intent_id);
  return Response.json(success({intent}));
 }
 const found=intents.get(req.reference);
 if(!found)return Response.json(rejected('INTENT_NOT_FOUND'));
 if(req.action==='cancel'){
  if(found.state==='CANCELLED')return Response.json(success({intent:found}));
  found.state='CANCELLED';return Response.json(success({intent:found}));
 }
 if(req.action==='close'){
  const closed={...found,intent_id:'close_'+found.intent_id,state:'FILLED',
   plan:{...found.plan,operation:'close'}};
  intents.set(closed.intent_id,closed);return Response.json(success({intent:closed}));
 }
 return Response.json(rejected('FAKE_OPERATION_UNSUPPORTED'));
};
const env:any={RUNTIME:{idFromName:()=>0,get:()=>({fetch:handler})}};
async function call(tool:string,args:Record<string,unknown>){
 const {action,...fields}=args;
 const name=tool==='trade'?'trade.'+action:tool;
 const publicArgs=tool==='trade'?fields:args;
 const rpc:any=await(await mcpApi.fetch(new Request('https://fixture/mcp',{method:'POST',
  body:JSON.stringify({jsonrpc:'2.0',id:++id,method:'tools/call',
   params:{name,arguments:publicArgs}})}),env,ctx)).json();
 check(!rpc.error,tool+'.'+args.action+' rejected at schema '+JSON.stringify(rpc.error));
 const response=rpc.result?.structuredContent;
 check(response&&typeof response.status==='string','missing MCP typed response');
 return response;
}
const config=await call('accounts',{action:'list'});
check(config.status==='completed'&&config.data.accounts.length===0,'central empty read');
const connectArgs={action:'connect',alias:'fixture_a',label:'Fixture A',
 credential_ref:'credential_fixture_a',enabled:true,limits,
 idempotency_key:'fixture_connect_0001'};
const first=await call('accounts',connectArgs);
check(first.status==='completed'&&first.data.account.account_key===accountKey&&
 first.data.account.identity_verified===true,'connect must reach real central enrollment owner');
await recordNativePolicy('connect');
const repeated=await call('accounts',connectArgs);
check(repeated.status==='completed'&&repeated.data.account.account_key===accountKey,'connect replay');
const changed=await call('accounts',{...connectArgs,label:'Different'});
check(changed.status==='failed'&&changed.reason_code==='IDEMPOTENCY_CONFLICT','body-bound connect');
let read=await call('accounts',{action:'list'});
check(read.data.revision===1&&read.data.accounts[0].id==='fixture_a','durable account readback');
const pauseArgs={action:'pause',account:'fixture_a',expected_revision:1,
 idempotency_key:'fixture_pause_0001'};
let pause=await call('accounts',pauseArgs);
check(pause.status==='completed'&&pause.data.revision===2&&pause.data.accounts[0].explicit_pause,
 'central pause and revision');
await recordNativePolicy('pause');
const pausedReplay=await call('accounts',pauseArgs);
check(pausedReplay.data.revision===2,'pause replay same revision');
const staleResume=await call('accounts',{...pauseArgs,action:'resume',idempotency_key:'fixture_resume_stale_001'});
check(staleResume.status==='failed'&&staleResume.reason_code==='STALE_ACCESS_REVISION',
 'stale governance revision rejected');
await recordNativePolicy('stale_resume_rejected');
const plan={operation:'open',strategy_instance:'manual',instrument:'BTC-USDT-SWAP',
 side:'long',sizing:{contracts:'0.01'},order:{type:'limit',limit_price:'100',max_slippage_bps:25},
 protection:{stop_distance_bps:100}};
const blocked=await call('trade',{action:'execute',account:'fixture_a',
 idempotency_key:'fake_blocked_001',expires_in_ms:30000,...plan});
check(blocked.status==='rejected'&&blocked.reason_code==='ACCOUNT_NEW_RISK_PAUSED',
 'paused fake venue blocks new risk without post');
check(Number(venuePostCount)===0,'no fake post during paused state');
const resume=await call('accounts',{action:'resume',account:'fixture_a',expected_revision:2,
 idempotency_key:'fixture_resume_0001'});
check(resume.status==='completed'&&resume.data.revision===3&&
 resume.data.accounts[0].explicit_pause===false,'central resume');
await recordNativePolicy('resume');
const revisedLimits={...limits,max_order_loss_usdt:'0.5'};
const limitsArgs={action:'set_limits',account:'fixture_a',expected_revision:3,
 idempotency_key:'fixture_limits_0001',limits:revisedLimits};
const revised=await call('accounts',limitsArgs);
check(revised.status==='completed'&&revised.data.revision===4&&
 revised.data.accounts[0].limits.max_order_loss_usdt==='0.5','central limits CAS');
await recordNativePolicy('set_limits');
const staleLimit=await call('accounts',{...limitsArgs,idempotency_key:'fixture_limits_stale_001'});
check(staleLimit.status==='failed'&&staleLimit.reason_code==='STALE_ACCESS_REVISION',
 'stale limits cannot overwrite');
await recordNativePolicy('stale_limits_rejected');
const replayLimit=await call('accounts',limitsArgs);
check(replayLimit.data.revision===4,'limits identical replay');
await recordNativePolicy('set_limits_replay');
read=await call('accounts',{action:'status',account:'fixture_a'});
check(read.data.account.policy_revision===4&&read.data.account.limits.max_order_loss_usdt==='0.5',
 'account status sees durable central owner');
check((await call('trade',{action:'preflight',account:'fixture_a'})).status==='completed','preflight');
const preview=await call('trade',{action:'preview',account:'fixture_a',...plan});
check(preview.status==='completed'&&preview.data.admission==='preview_only'&&
 preview.data.exchange_post===false,'preview did not post');
const executeArgs={action:'execute',account:'fixture_a',
 idempotency_key:'fake_execute_0001',expires_in_ms:30000,...plan};
const executed=await call('trade',executeArgs);
const ref=executed.data?.intent?.intent_id;
check(executed.status==='completed'&&typeof ref==='string'&&executed.data.intent.protected,
 'protected fake venue execute');
check((await call('trade',executeArgs)).data.intent.intent_id===ref&&Number(venuePostCount)===1,
 'exact replay never duplicates fixture POST');
const conflicting=await call('trade',{...executeArgs,operation:'replace'});
check(conflicting.status==='rejected'&&conflicting.reason_code==='IDEMPOTENCY_CONFLICT',
 'changed-body execution rejects duplicate key');
const status=await call('trade',{action:'status',account:'fixture_a',reference:ref});
check(status.data.intent.protected&&status.data.intent.state==='FILLED','fake status/protection');
const replace=await call('trade',{...executeArgs,operation:'replace',idempotency_key:'fake_replace_0001'});
check(replace.status==='completed'&&replace.data.intent.plan.operation==='replace'&&Number(venuePostCount)===2,
 'replace is a separate protected fake venue intent');
const cancel=await call('trade',{action:'cancel',account:'fixture_a',
 reference:replace.data.intent.intent_id,idempotency_key:'fake_cancel_0001'});
check(cancel.status==='completed'&&cancel.data.intent.state==='CANCELLED','fake cancel business transition');
const closed=await call('trade',{action:'close',account:'fixture_a',reference:ref,
 idempotency_key:'fake_close_0001',expires_in_ms:30000});
check(closed.status==='completed'&&closed.data.intent.state==='FILLED'&&
 closed.data.intent.plan.operation==='close','fake close outcome');
check((await call('trade',{action:'status',account:'fixture_a',reference:'unknown'}))
 .reason_code==='INTENT_NOT_FOUND','exact missing intent failure');
const final=(await governanceRequest(storage,'GET')).body as Governance&{status:string};
check(final.revision===4&&final.accounts.length===1&&
 final.accounts[0].limits.max_order_loss_usdt==='0.5'&&
 final.accounts[0].explicit_pause===false,'persisted central owner was not overwritten');
check(Number(venuePostCount)===2&&transportCalls>24,'no unexpected fixture posts');
await recordNativePolicy('final');
if((globalThis as any).process?.env?.OKX_A2_TRACE==='1'){
 check(nativePolicyTrace.events.length===8,'the native SQL bridge requires all CAS transitions');
 check(policyFrames.length===7,'only successful CAS/replay may emit native WSS policies');
 check(JSON.stringify(nativePolicyTrace.events.map(e=>e.frame_count))===
       JSON.stringify([1,3,4,5,6,6,7,7]),
       'stale CAS must not send policy and successful replay is idempotent');
 console.log('A2_SQL_BRIDGE_TRACE_V1:'+JSON.stringify(nativePolicyTrace));
}
console.log('mcp_central_owner_composed_fake_venue PASS central connect/pause/resume/limits CAS + protected trade lifecycle, two fixture posts, zero real exchange POST, no native SQL/WSS claim');


// A second account in the SAME composed MCP fixture exercises the true Worker
// transaction code on the way to a fake venue. This intentionally follows the
// A2 policy trace: it must not alter its exact committed CAS evidence.
const realConnect=await call('accounts',{action:'connect',alias:'fixture_real',
 label:'Fixture Real',credential_ref:'credential_fixture_real',enabled:true,limits,
 idempotency_key:'fixture_connect_real_0001'});
check(realConnect.status==='completed'&&realConnect.data.account.account_key===
 'okx:real:'+verifiedUid,'real account identity arrived via same MCP connect path');
// Simulates the authenticated fixture socket's own generation, not an
// externally asserted MCP parameter or a production WSS attestation.
await storage.put('generation',generation);
const beforeReal=venuePostCount;
const realPreview=await call('trade',{action:'preview',account:'fixture_real',...plan});
check(realPreview.status==='completed'&&realPreview.data.admission==='preview_only'&&
 venuePostCount===beforeReal,'real preview never reserves or posts');
const realArgs={...executeArgs,account:'fixture_real',
 idempotency_key:'fixture_real_execute_0001'};
const realOrder=await call('trade',realArgs);
check(realOrder.status==='completed'&&realOrder.data.intent.protected&&
 venuePostCount===beforeReal+1,'MCP route→fake native→actual RiskBook→fake venue');
const reservedRisk=await readRiskBook(storage);
check(reservedRisk?.occupied_usd==='0.5'&&reservedRisk?.occupied_usdt==='0.5',
 'fake FILLED is NOT a real terminal ledger proof: central risk remains occupied');
const replayedReal=await call('trade',realArgs);
check(replayedReal.data.intent.intent_id===realOrder.data.intent.intent_id&&
 venuePostCount===beforeReal+1,'replayed MCP command never duplicates risk or venue POST');
const realChanged=await call('trade',{...realArgs,operation:'replace'});
check(realChanged.status==='rejected'&&realChanged.reason_code==='IDEMPOTENCY_CONFLICT'&&
 venuePostCount===beforeReal+1,'changed body fails before new risk or venue POST');
const realStatus=await call('trade',{action:'status',account:'fixture_real',
 reference:realOrder.data.intent.intent_id});
check(realStatus.data.intent.state==='FILLED'&&realStatus.data.intent.protected,
 'fake venue status is visible through same MCP routing');
const latest=(await governanceRequest(storage,'GET')).body as Governance;
const pausedReal=await call('accounts',{action:'pause',account:'fixture_real',
 expected_revision:latest.revision,idempotency_key:'fixture_real_pause_0001'});
check(pausedReal.status==='completed'&&pausedReal.data.accounts.find(
 (a:any)=>a.id==='fixture_real')?.explicit_pause===true,'central pause through MCP');
const blockedReal=await call('trade',{...realArgs,idempotency_key:'fixture_real_blocked_0001'});
check(blockedReal.status==='rejected'&&blockedReal.reason_code==='ACCOUNT_NEW_RISK_PAUSED'&&
 venuePostCount===beforeReal+1,'pause denies additional real risk before fixture POST');
check((await readRiskBook(storage))?.occupied_usd==='0.5',
 'pause cannot silently release a POSTED reservation without terminal native proof');
console.log('mcp_riskbook_fake_venue_composed PASS real MCP route, central RESERVE/BEGIN_POST, fixture venue once, replay, pause, held risk; zero real exchange POST, no physical native SQL claim');
