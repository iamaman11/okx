import {mcpApi,projectNativeResponse} from './mcp.js';
import {payloadFixtures} from './execution_tools.test.js';
const ctx={auth:{scope:['mcp:use']}};
const call=(method:string,params?:unknown)=>mcpApi.fetch(new Request('https://fixture/mcp',{method:'POST',body:JSON.stringify({jsonrpc:'2.0',id:1,method,params})}),{} as any,ctx);
const response=await call('tools/list'),body:any=await response.json();
const bytes=new TextEncoder().encode(JSON.stringify(body)).byteLength;
if(body.result.tools.length!==7||bytes>24576)throw Error(`tools budget ${bytes}`);
for(const fixture of payloadFixtures){const r:any=await(await call('tools/call',{name:fixture.tool,arguments:fixture.negative[0]})).json();if(r.error?.code!==-32602)throw Error('negative dispatched');}
console.log(`tools_list_actual_serialized PASS 7 tools ${bytes} bytes`);

const fixture=payloadFixtures.find(x=>x.tool==='trade'&&x.action==='execute')!;
const env:any={RUNTIME:{idFromName:()=>0,get:()=>({fetch:async()=>Response.json({status:'FAILED',reason:'MUTATION_OUTCOME_UNKNOWN_RECONCILE_EXACT_INTENT',retryable:false})})}};
const unknown:any=await(await mcpApi.fetch(new Request('https://fixture/mcp',{method:'POST',body:JSON.stringify({id:2,method:'tools/call',params:{name:'trade',arguments:fixture.positive}})}),env,ctx)).json();
if(unknown.result.structuredContent.status!=='unknown'||unknown.result.structuredContent.retryable!==false||!unknown.result.structuredContent.request_id||!unknown.result.structuredContent.as_of_ms)throw Error('unknown outcome common shell');

const nestedUnknown:any=projectNativeResponse({schema:'okx.agent.response/v1',request_id:'req_native_unknown',status:'completed',quality:'FRESH',warnings:['native_warning'],result:{status:'UNKNOWN',reason_code:'POSTING_UNKNOWN',intent_id:'intent_398'}});
if(nestedUnknown.status!=='unknown'||nestedUnknown.reason_code!=='POSTING_UNKNOWN'||nestedUnknown.retryable!==false||nestedUnknown.data.intent_id!=='intent_398'||nestedUnknown.warnings[0]!=='native_warning')throw Error('nested UNKNOWN must not become completed');
console.log("mcp_nested_failure_preserves_reason_code PASS");

const failedEnvelope:any=projectNativeResponse({schema:'okx.agent.response/v1',request_id:'req_native_failed',status:'failed',quality:'NOT_READY',warnings:[],failure:{code:'RESPONSE_TOO_LARGE',message:'native result bounded',retryable:false}});
if(failedEnvelope.status!=='failed'||failedEnvelope.reason_code!=='RESPONSE_TOO_LARGE'||failedEnvelope.data!==null)throw Error('native envelope failure must keep exact failure code');

// Rust TradeService and Strategies currently return a second, strictly bounded
// completed shell. The public MCP must present only the typed product, with
// native request identity, freshness and failure safety intact.
const completedBusiness={status:'completed',as_of_ms:1780000000000,
 data:{admission:'preview_only',plan:{risk_usd:'1'},quality:'complete'},
 quality:{state:'not_evaluated'},warnings:['service_warning']};
const once:any=projectNativeResponse({schema:'okx.agent.response/v1',request_id:'req_preview_once',
 status:'completed',quality:'FRESH',warnings:['native_warning'],result:completedBusiness});
if(once.status!=='completed'||once.request_id!=='req_preview_once'||
 once.as_of_ms!==completedBusiness.as_of_ms||once.data.admission!=='preview_only'||
 'status' in once.data||'data' in once.data||once.quality!=='complete'||
 once.warnings.join(',')!=='native_warning,service_warning')
 throw Error('strict completed business result not projected once');
const strategies:any=projectNativeResponse({schema:'okx.agent.response/v1',request_id:'req_strategies_once',
 status:'completed',quality:'FRESH',warnings:[],
 result:{status:'completed',as_of_ms:1780000000001,
         data:{instances:[]},quality:{state:'not_evaluated'},warnings:[]}});
if(strategies.status!=='completed'||!Array.isArray(strategies.data.instances)||
 'status' in strategies.data)throw Error('nested strategy list response');
const fakeExtra:any=projectNativeResponse({schema:'okx.agent.response/v1',request_id:'req_extra',
 status:'completed',quality:'FRESH',warnings:[],
 result:{...completedBusiness,extra_untyped:'not_a_completed_shell'}});
if(fakeExtra.data.extra_untyped!=='not_a_completed_shell'||
 fakeExtra.data.data.admission!=='preview_only')
 throw Error('untyped business result was unwrapped by shape');
const stillUnknown:any=projectNativeResponse({schema:'okx.agent.response/v1',request_id:'req_trade_unknown',
 status:'completed',quality:'FRESH',warnings:[],
 result:{status:'unknown',reason_code:'EXCHANGE_POST_OUTCOME_UNKNOWN',intent_id:'intent_398'}});
if(stillUnknown.status!=='unknown'||stillUnknown.reason_code!=='EXCHANGE_POST_OUTCOME_UNKNOWN'||
 stillUnknown.retryable!==false||stillUnknown.data.intent_id!=='intent_398')
 throw Error('UNKNOWN outcome erased by completed response projection');
console.log('native_completed_business_envelope_once PASS');

const queuedJob:any=projectNativeResponse({schema:'okx.agent.response/v1',request_id:'req_native_job',status:'completed',quality:'FRESH',warnings:[],result:{id:'job_398',state:'queued',progress:0}});
if(queuedJob.status!=='accepted'||queuedJob.job_id!=='job_398'||queuedJob.data.state!=='queued')throw Error('queued job must remain accepted with an id');
console.log("mcp_queued_job_remains_accepted PASS");

const partial:any=projectNativeResponse({schema:'okx.agent.response/v1',request_id:'req_native_partial',status:'completed',quality:'DEGRADED',warnings:[],result:{rows:[],quality:'partial'}});
if(partial.status!=='partial'||partial.quality!=='partial')throw Error('partial native quality must remain partial');

const oversized='x'.repeat(13_000);
const budgetEnv:any={RUNTIME:{idFromName:()=>0,get:()=>({fetch:async()=>Response.json({schema:'okx.agent.response/v1',request_id:'req_native_large',status:'completed',quality:'FRESH',warnings:[],result:{rows:[oversized]}})})}};
const budget:any=await(await mcpApi.fetch(new Request('https://fixture/mcp',{method:'POST',body:JSON.stringify({id:3,method:'tools/call',params:{name:'market',arguments:payloadFixtures.find(x=>x.tool==='market')!.positive}})}),budgetEnv,ctx)).json();
const budgetResult=budget.result.structuredContent;
if(budgetResult.status!=='failed'||budgetResult.reason_code!=='RESPONSE_TOO_LARGE'||budgetResult.data.response_budget_bytes!==12_288||budgetResult.data.response_bytes<=12_288)throw Error('oversized native result must terminalize as bounded MCP failure');

const nativeResponse=(response:unknown):any=>({RUNTIME:{idFromName:()=>0,get:()=>({fetch:async()=>Response.json(response)})}});
const mcpResponse=async(tool:string,args:unknown,response:unknown):Promise<any>=>{
 const env=nativeResponse(response);
 const reply:any=await(await mcpApi.fetch(new Request('https://fixture/mcp',{method:'POST',
  body:JSON.stringify({jsonrpc:'2.0',id:99,method:'tools/call',params:{name:tool,arguments:args}})}),env,ctx)).json();
 if(reply.error)throw Error(JSON.stringify(reply.error));
 return reply.result.structuredContent;
};
// Exercise the real Worker MCP projection/dispatch for 100 independent read-only
// requests. The Durable Object is a deterministic fixture, not a physical network SLA.
let inFlightRead=0,maxInFlightRead=0,nativeReadCount=0;
const readOnlyEnv:any={RUNTIME:{idFromName:()=>0,get:()=>({fetch:async()=>{
 nativeReadCount++;inFlightRead++;maxInFlightRead=Math.max(maxInFlightRead,inFlightRead);
 await Promise.resolve();inFlightRead--;
 return Response.json({schema:'okx.agent.response/v1',request_id:'fake_readonly_native',
   status:'completed',quality:'FRESH',warnings:[],
   result:{schema:'okx.research.capabilities/v1',strategy_ids:['ema_cross_v1']}});
}})}};
const hundred=await Promise.all(Array.from({length:100},async(_,i)=>{
 const res=await mcpApi.fetch(new Request('https://fixture/mcp',{method:'POST',body:JSON.stringify({
   jsonrpc:'2.0',id:i+100,method:'tools/call',params:{name:'research',arguments:{action:'capabilities'}}
 })}),readOnlyEnv,ctx);
 return res.json() as Promise<any>;
}));
if(nativeReadCount!==100||maxInFlightRead<2||hundred.length!==100||
 hundred.some((x:any,i:number)=>x.id!==i+100||x.result?.structuredContent?.status!=='completed'||
 x.result?.structuredContent?.data?.schema!=='okx.research.capabilities/v1'||x.error))
 throw Error('100 concurrent Worker read-only fixture requests lost or incorrectly projected');
console.log('worker_100_concurrent_readonly_fixture PASS 100 native fixture reads zero real POST');

// Eleven real Worker MCP routes through an in-memory DO with NO exchange access.
// Success and business refusal: route fidelity, identity, retries, bounded shell.
const fakeMutationActions:[string,string][]=[
 ['accounts','connect'],['accounts','pause'],['accounts','resume'],['accounts','set_limits'],
 ['strategies','create'],['strategies','update'],['strategies','start'],['strategies','stop'],
 ['trade','execute'],['trade','cancel'],['trade','close'],
];
for(const [tool,action] of fakeMutationActions){
 const fixture=payloadFixtures.find(x=>x.tool===tool&&x.action===action)
  ?? (action==='resume'||action==='stop'
    ?payloadFixtures.find(x=>x.tool===tool&&x.action===(action==='resume'?'pause':'start'))
    :undefined);
 if(!fixture)throw Error('missing mutation fixture '+tool+'.'+action);
 const args={...fixture.positive,action};
 for(const negative of [false,true]){
  const visits:{path:string;request:any}[]=[];
  const fakeHandler=async(url:string,init?:RequestInit)=>{
   const path=new URL(url).pathname;
   const request=init?.body?JSON.parse(String(init.body)):null;
   visits.push({path,request});
   if(negative)return Response.json({status:'failed',reason_code:'FAKE_VENUE_REJECTED',retryable:false});
   if(path==='/dispatch'&&request?.operation?.type==='connect_account'){
    const op=request.operation;
    return Response.json({status:'completed',result:{alias:op.alias,credential_ref:op.credential_ref,
     connect_operation_id:op.operation_id,configuration_digest:op.request_digest,
     account_key:{venue:'okx',environment:'demo',verified_uid:'a'.repeat(64)}}});
   }
   if(path==='/dispatch')return Response.json({schema:'okx.agent.response/v1',
    request_id:'fake_mutation_native',status:'completed',quality:'FRESH',warnings:[],
    result:{schema:'okx.fake-venue.fixture/v1',venue:'fake',exchange_post_count:0}});
   return Response.json({status:'completed',data:{venue:'fake',exchange_post_count:0}});
  };
  const fake:any={RUNTIME:{idFromName:()=>0,get:()=>({fetch:fakeHandler})}};
  const rpc:any=await(await mcpApi.fetch(new Request('https://fixture/mcp',{method:'POST',
   body:JSON.stringify({jsonrpc:'2.0',id:902,method:'tools/call',params:{name:tool,arguments:args}})}),fake,ctx)).json();
  if(rpc.error||!rpc.result?.structuredContent)throw Error('mutation schema/dispatch failed '+tool+'.'+action+':'+JSON.stringify(rpc.error));
  const envelope=rpc.result.structuredContent;
  if(negative){
   if(envelope.status!=='failed'||envelope.reason_code!=='FAKE_VENUE_REJECTED'||visits.length!==1)
    throw Error('fake venue business refusal not preserved '+tool+'.'+action);
  }else if(envelope.status!=='completed')throw Error('fake venue success not projected '+tool+'.'+action);
  const first=visits[0];
  const expectedPath=tool==='accounts'&&['pause','resume','set_limits'].includes(action)
    ?(action==='set_limits'?'/account-governance/limits':'/account-governance/pause'):'/dispatch';
  if(!first||first.path!==expectedPath)throw Error('wrong owner '+tool+'.'+action);
  if(expectedPath==='/dispatch'){
   const expectedType=tool==='accounts'?'connect_account':tool;
   if(first.request?.operation?.type!==expectedType)throw Error('mutation operation lost '+tool+'.'+action);
   if(tool!=='accounts'&&(first.request.operation.request?.action!==action||first.request.account!==args.account))
    throw Error('mutation action/account lost '+tool+'.'+action);
  }else if(expectedPath==='/account-governance/pause'){
   if(first.request?.explicit_pause!==(action==='pause')||first.request?.account!==args.account)
    throw Error('pause/resume action/account lost');
  }else if(first.request?.action!==action||first.request?.account!==args.account)
    throw Error('set_limits action/account lost');
  if(tool==='accounts'&&action==='connect'&&!negative){
   if(visits.length!==2||visits[1].path!=='/account-governance/enroll'||
     visits[1].request?.alias!==args.alias||visits[1].request?.credential_ref!==args.credential_ref)
    throw Error('connect did not prove central enrollment binding');
  }else if(visits.length!==1)throw Error('mutation dispatched repeatedly '+tool+'.'+action);
 }
}
console.log('mcp_fake_venue_mutation_routes PASS 11 actions positive/negative 0 real exchange POST');

const rawFailure=(code:string)=>({schema:'okx.agent.response/v1',request_id:'native_fail_398',
 status:'rejected',generated_at:'2026-10-23T00:00:00Z',quality:'NOT_READY',result_schema:'okx.product/v1',
 result:null,failure:{code,message:code,retryable:false},warnings:[]});
const nativeNotFound=await mcpResponse('research',{action:'status',job_id:'missing_job_398'},
 {status:'completed',data:rawFailure('JOB_NOT_FOUND')});
if(nativeNotFound.status!=='rejected'||nativeNotFound.reason_code!=='JOB_NOT_FOUND'||
 nativeNotFound.data!==null||nativeNotFound.retryable!==false||
 nativeNotFound.request_id!=='native_fail_398')throw Error('DO wrapped JOB_NOT_FOUND code lost');
console.log('native_failure_code_passthrough PASS');

const previewFixture=payloadFixtures.find(x=>x.tool==='trade'&&x.action==='preview')!;
const nativeNotional=await mcpResponse('trade',previewFixture.positive,
 {status:'completed',result:rawFailure('ORDER_NOTIONAL_LIMIT')});
if(nativeNotional.status!=='rejected'||nativeNotional.reason_code!=='ORDER_NOTIONAL_LIMIT'||
 nativeNotional.data!==null)throw Error('DO wrapped ORDER_NOTIONAL_LIMIT code lost');

const gridFixture=payloadFixtures.find(x=>x.tool==='research'&&x.action==='grid')!;
const nativeQueued=await mcpResponse('research',gridFixture.positive,
 {schema:'okx.agent.response/v1',request_id:'queued_native_398',status:'completed',quality:'FRESH',
  result:{id:'queued_job_398',state:'queued',progress:0},failure:null,warnings:[]});
if(nativeQueued.status!=='accepted'||nativeQueued.job_id!=='queued_job_398'||
 nativeQueued.data.state!=='queued')throw Error('DO wrapped queued job projected complete');
console.log('do_wrapped_queued_job_is_accepted PASS');

const nativePartial=await mcpResponse('market',payloadFixtures.find(x=>x.tool==='market')!.positive,
 {schema:'okx.agent.response/v1',request_id:'partial_398',status:'completed',quality:'FRESH',
  result:{rows:[{instrument:'BTC-USDT-SWAP',quality:'partial',metrics:{returns:null}}]},failure:null,warnings:[]});
if(nativePartial.status!=='partial'||nativePartial.data.rows[0].quality!=='partial')
 throw Error('partial input row promoted to completed');
console.log('partial_coverage_not_fresh PASS');

const deliveredUnknown=await mcpResponse('trade',payloadFixtures.find(x=>x.tool==='trade'&&x.action==='execute')!.positive,
 {schema:'okx.direct-transport.result/v1',status:'FAILED',reason:'MUTATION_OUTCOME_UNKNOWN_RECONCILE_EXACT_INTENT',retryable:false});
if(deliveredUnknown.status!=='unknown'||deliveredUnknown.retryable!==false||
 deliveredUnknown.next_action!=='inspect_exact_intent')throw Error('UNKNOWN exchange outcome can be retried');
const deterministicUnknown=projectNativeResponse({status:'FAILED',reason:'UNKNOWN_ACCOUNT',retryable:false});
if(deterministicUnknown.status!=='failed'||deterministicUnknown.reason_code!=='UNKNOWN_ACCOUNT')
 throw Error('deterministic unknown reason incorrectly became uncertain POST');
console.log('transport_unknown_without_resubmit PASS');

const invalidNative=projectNativeResponse({schema:'okx.agent.response/v1',status:'completed',result:{id:'bad'}});
if(invalidNative.status!=='failed'||invalidNative.reason_code!=='INVALID_NATIVE_ENVELOPE')
 throw Error('malformed native response must fail closed');

const healthyRuntime=await mcpResponse('diagnostics',{action:'runtime'},
 {schema:'okx.direct-transport.status/v1',status:'PASS',runtime_id:'windows-primary',
  runtime_profile:'engine',connected:true,session_fresh:true,connection_generation:73});
if(healthyRuntime.status!=='completed'||healthyRuntime.data.schema!=='okx.runtime-status/v1'||
 healthyRuntime.data.transport.session_fresh!==true||healthyRuntime.data.transport.connection_generation!==73||
 typeof healthyRuntime.data.tool_contract!=='string')throw Error('runtime status must prove fresh session');
if(healthyRuntime.data.installation_verification.owner!=='WINDOWS_CONTROL'||
 healthyRuntime.data.installation_verification.worker_independent_proof!=='NOT_PERFORMED'||
 'release_attestation' in healthyRuntime.data)
 throw Error('authenticated WSS cannot imply independent installed-file proof');
console.log('native_control_owner_without_worker_gh_attestation PASS');
const offlineRuntime=await mcpResponse('diagnostics',{action:'runtime'},
 {schema:'okx.direct-transport.status/v1',status:'OFFLINE',runtime_id:'windows-primary',
  connected:false,session_fresh:false});
if(offlineRuntime.status!=='partial'||offlineRuntime.data.reason_code!=='RUNTIME_SESSION_NOT_FRESH'||
 offlineRuntime.data.transport.connected!==false)throw Error('offline session promoted to fresh');
const corruptRuntime=await mcpResponse('diagnostics',{action:'runtime'},{status:'PASS'});
if(corruptRuntime.status!=='failed'||corruptRuntime.reason_code!=='INVALID_RUNTIME_STATUS')
 throw Error('malformed runtime observation accepted');
console.log('diagnostics_runtime_freshness_and_negative PASS');
