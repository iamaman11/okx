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
