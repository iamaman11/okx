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
const offlineRuntime=await mcpResponse('diagnostics',{action:'runtime'},
 {schema:'okx.direct-transport.status/v1',status:'OFFLINE',runtime_id:'windows-primary',
  connected:false,session_fresh:false});
if(offlineRuntime.status!=='partial'||offlineRuntime.data.reason_code!=='RUNTIME_SESSION_NOT_FRESH'||
 offlineRuntime.data.transport.connected!==false)throw Error('offline session promoted to fresh');
const corruptRuntime=await mcpResponse('diagnostics',{action:'runtime'},{status:'PASS'});
if(corruptRuntime.status!=='failed'||corruptRuntime.reason_code!=='INVALID_RUNTIME_STATUS')
 throw Error('malformed runtime observation accepted');
console.log('diagnostics_runtime_freshness_and_negative PASS');
