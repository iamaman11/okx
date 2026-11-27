import {mcpApi,operationTrace,projectNativeResponse} from './mcp.js';
import {payloadFixtures} from './execution_tools.test.js';
import {MAX_BODY_BYTES} from './shared.js';
const ctx={auth:{scope:['mcp:use']}};
const originJob='a'.repeat(64);
const researchTrace=operationTrace('research','request:req_original_01',{
 request_id:'req_diagnostic_02',status:'partial',data:{schema:'okx.research-job-diagnostics/v1',
  state:'OBSERVED',reason_code:'JOB_SNAPSHOT',job:{id:originJob,state:'completed',
   origin_invocation:'req_original_01',result_ref:'b'.repeat(64)}}
});
if(researchTrace.state!=='OBSERVED'||researchTrace.job_id!==originJob||
 researchTrace.subject?.kind!=='research_job'||researchTrace.subject.job_id!==originJob||
 researchTrace.origin_request_id!=='req_original_01'||
 researchTrace.diagnostic_request_id!=='req_diagnostic_02'||
 researchTrace.next_action!=='research.result'||researchTrace.read_only!==true)
 throw Error('correlated Research job has no readable operation trace');
const forgedTrace=operationTrace('research','request:req_different',{
 request_id:'req_diagnostic_02',status:'partial',data:{schema:'okx.research-job-diagnostics/v1',
  state:'OBSERVED',job:{id:originJob,state:'completed',origin_invocation:'req_original_01'}}
});
if(forgedTrace.state!=='NOT_EVALUATED'||forgedTrace.job_id!==null||forgedTrace.subject!==null)
 throw Error('foreign Research origin incorrectly promoted to observed job');
const nativeIntentTrace=operationTrace('operation','intent:abc',{
 request_id:'req_read',status:'partial',data:{schema:'okx.operation-diagnostics/v1',
  family:'native_intent',reference:'intent:abc',state:'posting',stage:'posting',
  intent:{intent_id:'abc'},
  missing_evidence:['AUTHORITATIVE_ORDER_READBACK'],next_action:'trade.status'}
});
if(nativeIntentTrace.state!=='OBSERVED'||nativeIntentTrace.first_unproven_fact!==
 'AUTHORITATIVE_ORDER_READBACK'||nativeIntentTrace.next_action!=='trade.status'||
 nativeIntentTrace.subject?.kind!=='trade_intent'||nativeIntentTrace.subject.intent_id!=='abc')
 throw Error('Engine native intent stage was lost');
if(operationTrace('operation','intent:wrong',{
 request_id:'req_read',status:'partial',data:{schema:'okx.operation-diagnostics/v1',
 family:'native_intent',reference:'intent:abc',state:'posting',stage:'posting',
 intent:{intent_id:'abc'}}}).state!=='NOT_EVALUATED')
 throw Error('foreign Engine intent incorrectly promoted to observed');
const engineRequestTrace=operationTrace('operation','request:req_initial_trade',{
 request_id:'req_diagnostic',status:'partial',data:{schema:'okx.operation-diagnostics/v1',
 family:'native_intent',reference:'request:req_initial_trade',origin_request_id:'req_initial_trade',
 resolved_intent_reference:'intent:intent_123',state:'POSTING',stage:'POSTING',
 intent:{intent_id:'intent_123'},
 missing_evidence:['AUTHORITATIVE_VENUE_ORDER_READBACK']}
});
if(engineRequestTrace.state!=='OBSERVED'||engineRequestTrace.reference_kind!=='request_id'||
 engineRequestTrace.origin_request_id!=='req_initial_trade'||
 engineRequestTrace.subject?.kind!=='trade_intent'||engineRequestTrace.subject.intent_id!=='intent_123'||
 engineRequestTrace.first_unproven_fact!=='AUTHORITATIVE_VENUE_ORDER_READBACK')
 throw Error('durable Engine request-to-intent correlation not projected');
const forgedEngineTrace=operationTrace('operation','request:req_initial_trade',{
 request_id:'req_diagnostic',status:'partial',data:{schema:'okx.operation-diagnostics/v1',
 family:'native_intent',reference:'request:req_initial_trade',origin_request_id:'req_other',
 resolved_intent_reference:'intent:intent_123',intent:{intent_id:'intent_123'},state:'POSTING'}
});
if(forgedEngineTrace.state!=='NOT_EVALUATED'||forgedEngineTrace.origin_request_id!==null||
 forgedEngineTrace.subject!==null)
 throw Error('foreign Engine request-to-intent binding promoted to observed');
for(const [label,reference,record] of [
 ['wrong intent row','intent:intent_123',{family:'native_intent',intent:{intent_id:'other'}}],
 ['missing request target','request:req_initial_trade',{family:'native_intent',intent:{intent_id:'other'},
   origin_request_id:'req_initial_trade',resolved_intent_reference:'intent:intent_123'}],
 ['unknown intent state','intent:intent_123',{family:'native_intent',intent:{intent_id:'intent_123'},
   state:'REPLAY_WITHOUT_SQL'}],
] as const){
 const trace=operationTrace('operation',reference,{request_id:'req_diagnostic',status:'partial',
  data:{schema:'okx.operation-diagnostics/v1',reference,state:'POSTING',...record}});
 if(trace.state!=='NOT_EVALUATED'||trace.subject!==null||trace.origin_request_id!==null)
  throw Error(`Engine malformed owner subject promoted: ${label}`);
}
const releaseTrace=operationTrace('operation','terminal_release:intent_123',{
 request_id:'req_read',status:'partial',data:{schema:'okx.operation-diagnostics/v1',
 family:'terminal_release',reference:'terminal_release:intent_123',state:'OBSERVED',
 operation:{state:'OBSERVED',required_evidence:'current_governance',
  next_action:'read_trade_status_for_authoritative_result',
  snapshot:{intent_id:'intent_123',stage:'Authority'}}}
});
if(releaseTrace.state!=='OBSERVED'||releaseTrace.subject?.kind!=='terminal_release'||
 releaseTrace.subject.intent_id!=='intent_123'||
 releaseTrace.first_unproven_fact!=='CURRENT_GOVERNANCE'||
 releaseTrace.observed_stage!=='Authority'||
 releaseTrace.next_action!=='read_trade_status_for_authoritative_result')
 throw Error('terminal release owner snapshot lost its exact subject or action');
const wrongRelease=operationTrace('operation','terminal_release:intent_123',{
 request_id:'req_read',status:'partial',data:{schema:'okx.operation-diagnostics/v1',
 family:'terminal_release',reference:'terminal_release:intent_123',state:'OBSERVED',
 operation:{snapshot:{intent_id:'different',stage:'Authority'}}}
});
if(wrongRelease.state!=='NOT_EVALUATED'||wrongRelease.subject!==null)
 throw Error('foreign terminal release intent was promoted');
const protectionTrace=operationTrace('operation','order_client_alias',{
 request_id:'req_read',status:'partial',data:{schema:'okx.operation-diagnostics/v1',
 reference:'order_client_alias',state:'OBSERVED',operation:{
 state:'OBSERVED',current_boundary:'SIGNED_ORDER_READBACK',
 snapshot:{operation_id:'cleanup_123',client_order_id:'order_client_alias',stage:'VERIFIED',
 first_unproven_boundary:'SIGNED_ORDER_READBACK',next_action:'trade.status'}}}
});
if(protectionTrace.state!=='OBSERVED'||protectionTrace.subject?.kind!=='protection_operation'||
 protectionTrace.subject.operation_id!=='cleanup_123'||
 protectionTrace.first_unproven_fact!=='SIGNED_ORDER_READBACK'||
 protectionTrace.next_action!=='trade.status')
 throw Error('protection alias did not resolve to owner operation identity');
const wrongProtection=operationTrace('operation','unrelated_alias',{
 request_id:'req_read',status:'partial',data:{schema:'okx.operation-diagnostics/v1',
 reference:'unrelated_alias',state:'OBSERVED',operation:{
 snapshot:{operation_id:'cleanup_123',client_order_id:'known_order'}}}
});
if(wrongProtection.state!=='NOT_EVALUATED'||wrongProtection.subject!==null)
 throw Error('unrelated protection alias was promoted by outer reference echo');
const accountRefusal=operationTrace('operation','request:req_missing_account',{
 request_id:'req_read',status:'partial',data:{scope:'engine',stage:'operation',
 reason_code:'ACCOUNT_REQUIRED',quality:'partial',retryable:false,blockers:['ACCOUNT_REQUIRED'],
 next_action:'supply an authorized account and operation reference'}
});
if(accountRefusal.state!=='NOT_EVALUATED'||accountRefusal.subject!==null||
 accountRefusal.reason_code!=='ACCOUNT_REQUIRED'||
 accountRefusal.first_unproven_fact!=='AUTHORIZED_ACCOUNT_AND_OPERATION_REFERENCE'||
 accountRefusal.next_action!=='supply_authorized_account_and_operation_reference')
 throw Error('account preflight denial lost its safe next action');
const forgedRefusal=operationTrace('operation','intent:abc',{
 request_id:'req_read',status:'partial',data:{scope:'research',stage:'operation',
 reason_code:'ACCOUNT_REQUIRED',quality:'partial',retryable:false,blockers:['ACCOUNT_REQUIRED']}
});
if(forgedRefusal.reason_code==='ACCOUNT_REQUIRED'||forgedRefusal.state!=='NOT_EVALUATED')
 throw Error('foreign account refusal was accepted');
// Each finance subject is attested by its *own* durable owner identity. A
// local cancel ACK never implies venue terminal state; a replace candidate
// never proves which original order was cancelled.
const financeKey='trade_key_12345678';
const financeSubjectBase={schema:'okx.operation-diagnostics/v1',
 owner:'execution.unified_finance',account:'okx:demo:fixture',installation_id:'okx_v2_fixture',
 reference_kind:'idempotency_key',read_only:true,quality:'partial',history:'UNKNOWN',
 causal_history:'NOT_RECONSTRUCTED'};
const cancelData={...financeSubjectBase,family:'cancel',reference:`cancel:${financeKey}`,
 state:'OBSERVED',observed_stage:'CANCEL_STARTED_SQL_ONLY',
 first_unproven_fact:'CANCEL_VENUE_ACK_AND_FINAL_STATE',
 reason_code:'CANCEL_POSTING_OUTCOME_UNKNOWN',
 next_action:'inspect_original_order_status_do_not_resend',
 operation:{kind:'cancel',operation_id:financeKey,committed_at_ms:1234}};
const cancelTrace=operationTrace('operation',`cancel:${financeKey}`,{
 request_id:'req_read',status:'partial',data:cancelData});
if(cancelTrace.state!=='OBSERVED'||cancelTrace.reference_kind!=='idempotency_key'||
 cancelTrace.subject?.kind!=='cancel_operation'||cancelTrace.subject.operation_id!==financeKey||
 cancelTrace.first_unproven_fact!=='CANCEL_VENUE_ACK_AND_FINAL_STATE'||
 cancelTrace.observed_stage!=='CANCEL_STARTED_SQL_ONLY'||
 cancelTrace.next_action!=='inspect_original_order_status_do_not_resend'||
 cancelTrace.origin_request_id!==null||cancelTrace.quality!=='partial'||cancelTrace.history!=='UNKNOWN')
 throw Error('cancel SQL receipt was converted into a false venue outcome');
const closeData={...financeSubjectBase,family:'close',reference:`close:${financeKey}`,
 state:'OBSERVED',observed_stage:'RESERVED',
 first_unproven_fact:'ORIGINAL_CLOSE_TARGET_AND_CURRENT_VENUE_STATE',
 reason_code:'DURABLE_CLOSE_INTENT_ONLY',next_action:'trade.status',
 intent:{intent_id:'close_intent_42',idempotency_key:financeKey,operation:'close',state:'RESERVED'}};
const closeTrace=operationTrace('operation',`close:${financeKey}`,{
 request_id:'req_read',status:'partial',data:closeData});
if(closeTrace.state!=='OBSERVED'||closeTrace.subject?.kind!=='close_intent'||
 closeTrace.subject.intent_id!=='close_intent_42'||
 closeTrace.first_unproven_fact!=='ORIGINAL_CLOSE_TARGET_AND_CURRENT_VENUE_STATE')
 throw Error('close SQL intent was not bound to its own idempotency identity');
const replaceData={...financeSubjectBase,family:'replace',reference:`replace:${financeKey}`,
 state:'NOT_EVALUATED',observed_stage:'IDEMPOTENCY_KEY_INTENT_OBSERVED',
 first_unproven_fact:'REPLACE_COMMAND_AND_ORIGINAL_REFERENCE_BINDING',
 reason_code:'REPLACE_ORIGIN_UNVERIFIED',next_action:'inspect_original_and_replacement_status_do_not_resend',
 causal_link:'UNVERIFIED',candidate_new_intent:{intent_id:'candidate_42',idempotency_key:financeKey}};
const replaceTrace=operationTrace('operation',`replace:${financeKey}`,{
 request_id:'req_read',status:'partial',data:replaceData});
if(replaceTrace.state!=='NOT_EVALUATED'||replaceTrace.subject!==null||
 replaceTrace.reason_code!=='REPLACE_ORIGIN_UNVERIFIED'||
 replaceTrace.first_unproven_fact!=='REPLACE_COMMAND_AND_ORIGINAL_REFERENCE_BINDING'||
 replaceTrace.next_action!=='inspect_original_and_replacement_status_do_not_resend')
 throw Error('unverified replacement causality was promoted to observed');
for(const [label,reference,changed] of [
 ['wrong cancel receipt',`cancel:${financeKey}`,{operation:{...cancelData.operation,operation_id:'other_key_1234'}}],
 ['wrong cancel kind',`cancel:${financeKey}`,{operation:{...cancelData.operation,kind:'replace'}}],
 ['missing account',`cancel:${financeKey}`,{account:''}],
 ['wrong owner',`cancel:${financeKey}`,{owner:'research'}],
 ['wrong identity kind',`cancel:${financeKey}`,{reference_kind:'request_id'}],
 ['cross family',`close:${financeKey}`,{reference:`close:${financeKey}`}],
] as const){
 const trace=operationTrace('operation',reference,{request_id:'req_read',status:'partial',
  data:{...cancelData,...changed}});
 if(trace.state!=='NOT_EVALUATED'||trace.subject!==null)
  throw Error(`forged cancel operation promoted: ${label}`);
}
for(const [label,changed] of [
 ['wrong close key',{intent:{...closeData.intent,idempotency_key:'different_key_123'}}],
 ['wrong close operation',{intent:{...closeData.intent,operation:'open'}}],
 ['missing installation',{installation_id:''}],
 ['wrong family',{family:'cancel'}],
] as const){
 const trace=operationTrace('operation',`close:${financeKey}`,{request_id:'req_read',
  status:'partial',data:{...closeData,...changed}});
 if(trace.state!=='NOT_EVALUATED'||trace.subject!==null)
  throw Error(`forged close intent promoted: ${label}`);
}
const forgedReplace=operationTrace('operation',`replace:${financeKey}`,{
 request_id:'req_read',status:'partial',data:{...replaceData,state:'OBSERVED',
 intent:{...closeData.intent,operation:'replace'}}});
if(forgedReplace.state!=='NOT_EVALUATED'||forgedReplace.subject!==null)
 throw Error('forged replace outcome promoted without original reference binding');
console.log('read_only_operation_trace_reference_binding PASS');
for(const unauthorized of [{}, {auth:{scope:['research:read']}}, {auth:{scope:[]}}]){
 const forbidden=await mcpApi.fetch(new Request('https://fixture/mcp',{method:'POST',body:JSON.stringify({
  jsonrpc:'2.0',id:101,method:'tools/call',params:{name:'research',arguments:{action:'benchmark',idempotency_key:'test_auth_deny_01'}}
 })}),{RUNTIME:{idFromName:()=>{throw Error('unauthorized request reached Research');}}} as any,unauthorized);
 if(forbidden.status!==403)throw Error('Research MCP request bypassed authenticated mcp:use scope');
}
// Model-visible trading operations are separately named and have no caller
// action selector. The legacy test fixtures continue to describe native calls.
const publicArgs=(name:string,args:any)=>{
 if(name!=='trade')return {name,arguments:args};
 const {action,...input}=args;
 return {name:'trade.'+action,arguments:input};
};
const call=(method:string,params?:unknown)=>mcpApi.fetch(new Request('https://fixture/mcp',{method:'POST',body:JSON.stringify({jsonrpc:'2.0',id:1,method,params})}),{} as any,ctx);
// Parse and bound the actual request before routing. Invalid envelopes must
// remain JSON-RPC errors, never unhandled Worker exceptions or native calls.
for(const malformed of [null,[],42,{method:'tools/list',id:1},
 {jsonrpc:'1.0',method:'tools/list',id:1},
 {jsonrpc:'2.0',method:'tools/list'},
 {jsonrpc:'2.0',method:'tools/list',id:true}]){
 const response=await mcpApi.fetch(new Request('https://fixture/mcp',{method:'POST',body:JSON.stringify(malformed)}),{} as any,ctx);
 const rpc:any=await response.json();
 if(rpc.error?.code!==-32600)throw Error('malformed JSON-RPC envelope was accepted');
}
const notification=await mcpApi.fetch(new Request('https://fixture/mcp',{method:'POST',body:JSON.stringify({jsonrpc:'2.0',method:'notifications/initialized'})}),{} as any,ctx);
if(notification.status!==202)throw Error('proper JSON-RPC notification was rejected');
const missingParams:any=await(await mcpApi.fetch(new Request('https://fixture/mcp',{method:'POST',body:JSON.stringify({jsonrpc:'2.0',id:1,method:'tools/call'})}),{} as any,ctx)).json();
if(missingParams.error?.code!==-32602)throw Error('missing tools/call params must be invalid params, not unknown method');
const oversizedRequest=await mcpApi.fetch(new Request('https://fixture/mcp',{method:'POST',body:'x'.repeat(MAX_BODY_BYTES+1)}),{} as any,ctx);
if(oversizedRequest.status!==413)throw Error('oversized MCP request was buffered or parsed');
console.log('mcp_bounded_input_and_strict_jsonrpc PASS');
const response=await call('tools/list'),body:any=await response.json();
const bytes=new TextEncoder().encode(JSON.stringify(body)).byteLength;

for(const action of ['preview','execute','status','cancel','close','capabilities','preflight']){
 const tool=body.result.tools.find((t:any)=>t.name==='trade.'+action);
 if(!tool||!tool.inputSchema||'action' in tool.inputSchema.properties||
   !tool.annotations||tool.annotations.openWorldHint!==false||
   tool.annotations.readOnlyHint!==['preview','status','capabilities','preflight'].includes(action)||
   tool.annotations.destructiveHint!==['execute','cancel','close'].includes(action))
  throw Error('truthful trade metadata missing '+action);
}
if(body.result.tools.some((t:any)=>t.name==='trade'))throw Error('generic trade must not remain model-callable');

if(body.result.tools.length!==13||bytes>24576)throw Error(`tools budget ${bytes}`);
for(const fixture of payloadFixtures){const r:any=await(await call('tools/call',publicArgs(fixture.tool,fixture.negative[0]))).json();if(r.error?.code!==-32602)throw Error('negative dispatched');}
const unconfirmedStart:any=await(await call('tools/call',{name:'strategies',arguments:{
 action:'start',account:'fixture',instance_id:'alpha',idempotency_key:'start_fixture'
}})).json();
if(unconfirmedStart.error?.code!==-32602||!unconfirmedStart.error.message.includes('configuration_digest'))
 throw Error('old strategy start must explain the missing confirmation');
console.log(`tools_list_actual_serialized PASS 13 tools ${bytes} bytes`);

const fixture=payloadFixtures.find(x=>x.tool==='trade'&&x.action==='execute')!;
let earlyRequestId='';
const env:any={RUNTIME:{idFromName:()=>0,get:()=>({fetch:async(_url:string,init:RequestInit)=>{
 earlyRequestId=JSON.parse(String(init.body)).request_id;
 return Response.json({status:'FAILED',reason:'MUTATION_OUTCOME_UNKNOWN_RECONCILE_EXACT_INTENT',retryable:false});
}})}};
const unknown:any=await(await mcpApi.fetch(new Request('https://fixture/mcp',{method:'POST',body:JSON.stringify({jsonrpc:'2.0',id:2,method:'tools/call',params:publicArgs('trade',fixture.positive)})}),env,ctx)).json();
if(unknown.result.structuredContent.status!=='unknown'||unknown.result.structuredContent.retryable!==false||
 unknown.result.structuredContent.request_id!==earlyRequestId||!unknown.result.structuredContent.as_of_ms)
 throw Error('pre-delivery UNKNOWN lost the exact native request_id');
for(const [tool,args,scope,next] of [
 ['research',{action:'capabilities'},'research','inspect_research_job_or_idempotency_receipt'],
 ['trade',fixture.positive,'engine','inspect_exact_intent']
] as const){
 let sentId='';
 const mismatchEnv:any={RUNTIME:{idFromName:()=>0,get:()=>({fetch:async(_url:string,init:RequestInit)=>{
  sentId=JSON.parse(String(init.body)).request_id;
  return Response.json({schema:'okx.agent.response/v1',request_id:'req_different_native',
   status:'completed',quality:'FRESH',warnings:[],result:{state:'completed'}});
 }})}};
 const actual:any=await(await mcpApi.fetch(new Request('https://fixture/mcp',{method:'POST',
  body:JSON.stringify({jsonrpc:'2.0',id:3,method:'tools/call',params:publicArgs(tool,args)})}),mismatchEnv,ctx)).json();
 const projected=actual.result?.structuredContent;
 if(projected?.status!=='unknown'||projected?.request_id!==sentId||
   projected?.reason_code!=='RESPONSE_REQUEST_ID_MISMATCH'||projected?.retryable!==false||
   projected?.next_action!==next||projected?.scope!==scope||projected?.data!==null)
  throw Error('mismatched native reply falsely claimed operation success '+tool);
}
console.log('mcp_exact_request_identity_fail_closed PASS');
for(const [tool,args,expectedStatus,expectedReason] of [
 ['trade',fixture.positive,'unknown','MUTATION_OUTCOME_UNKNOWN_RECONCILE_EXACT_INTENT'],
 ['research',{action:'benchmark',idempotency_key:'research_benchmark_01'},'unknown',
  'RESEARCH_MUTATION_OUTCOME_UNKNOWN_CHECK_JOB'],
 ['research',{action:'capabilities'},'failed','EDGE_DISPATCH_UNAVAILABLE']
] as const){
 let sentId='';
 const interrupted:any={RUNTIME:{idFromName:()=>0,get:()=>({fetch:async(_url:string,init:RequestInit)=>{
  sentId=JSON.parse(String(init.body)).request_id;
  throw Error('private transport detail must not escape');
 }})}};
 const actual:any=await(await mcpApi.fetch(new Request('https://fixture/mcp',{method:'POST',
  body:JSON.stringify({jsonrpc:'2.0',id:4,method:'tools/call',params:publicArgs(tool,args)})}),interrupted,ctx)).json();
 const projected=actual.result?.structuredContent;
 if(projected?.request_id!==sentId||projected?.status!==expectedStatus||
   projected?.reason_code!==expectedReason||projected?.data!==null||
   JSON.stringify(projected).includes('private transport detail'))
  throw Error('transport exception lost correlation or leaked internals '+tool);
}
for(const action of ['capabilities','preflight'] as const){
 const read=payloadFixtures.find(x=>x.tool==='trade'&&x.action===action)!.positive;
 const unavailable:any={RUNTIME:{idFromName:()=>0,get:()=>({fetch:async()=>{throw Error('offline');}})}};
 const response:any=await(await mcpApi.fetch(new Request('https://fixture/mcp',{method:'POST',
  body:JSON.stringify({jsonrpc:'2.0',id:5,method:'tools/call',params:publicArgs('trade',read)})}),unavailable,ctx)).json();
 const result=response.result?.structuredContent;
 if(result?.status!=='failed'||result?.reason_code!=='EDGE_DISPATCH_UNAVAILABLE'||result?.next_action!=='inspect_diagnostics')
  throw Error(`read-only trade.${action} was classified as uncertain mutation`);
}
console.log('mcp_interrupted_dispatch_tracks_original_request PASS');

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

// Candidate counters are native job facts. MCP preserves them without
// upgrading running/interrupted jobs or recalculating their progress.
const candidateProgress={phase:'evaluating_candidates',completed_candidates:1,
 total_candidates:64,completed_work_units:66,total_work_units:193};
for(const [state,status,progress] of [['running','accepted',33],['interrupted','failed',33],['completed','completed',100]] as const){
 const projected:any=projectNativeResponse({schema:'okx.agent.response/v1',
  request_id:'req_candidate_progress',status:'completed',quality:'FRESH',warnings:[],
  result:{id:'a'.repeat(64),state,progress,candidate_progress:candidateProgress}});
 if(projected.status!==status||projected.job_id!=='a'.repeat(64)||
    projected.data.progress!==progress||JSON.stringify(projected.data.candidate_progress)!==JSON.stringify(candidateProgress))
  throw Error('MCP lost or reinterpreted native candidate progress');
}
console.log('mcp_native_candidate_progress_preserved PASS');

const partial:any=projectNativeResponse({schema:'okx.agent.response/v1',request_id:'req_native_partial',status:'completed',quality:'DEGRADED',warnings:[],result:{rows:[],quality:'partial'}});
if(partial.status!=='partial'||partial.quality!=='partial')throw Error('partial native quality must remain partial');

// P0.2: manual is an account-capital attribution label, not a second
// independently funded strategy. Its empty non-additive row must not degrade
// a verified account, but genuinely missing capital must stay partial.
const manualWindow={scope_id:'strategy:manual',currency:'USD',coverage:'NOT_APPLICABLE',
 opening_equity:null,current_equity:null,flows:null,pnl:null,
 opening_as_of:null,current_as_of:null};
const manualRow={kind:'nav',capital_scope:'account',additive_with_account_nav:false,
 coverage_reason:'MANUAL_USES_ACCOUNT_CAPITAL',window:manualWindow};
const accountRow={kind:'nav',capital_scope:'account',additive_with_account_nav:false,
 coverage_reason:null,window:{scope_id:'account',currency:'USD',coverage:'COMPLETE',
 opening_equity:'100',current_equity:'100',flows:'0',pnl:'0',
 opening_as_of:1,current_as_of:2}};
const portfolioEnvelope=(rows:any[],quality='complete')=>projectNativeResponse({
 schema:'okx.agent.response/v1',request_id:'req_finance_p02',status:'completed',
 quality:'FRESH',warnings:[],result:{rows,quality,diagnostics:[]}});
const manualSummary:any=portfolioEnvelope([accountRow,manualRow]);
if(manualSummary.status!=='completed'||manualSummary.quality!=='complete'||
 manualSummary.data.rows.length!==2||manualSummary.data.rows[1].window.current_equity!==null)
 throw Error('P0.2 manual account-capital label incorrectly degrades complete portfolio');
// The Rust finance owner marks incomplete sources PARTIAL. The Worker
// must never upgrade that evidence, including for a manual-shaped row.
for(const unknown of [
 {...manualRow,window:{...manualWindow,coverage:'PARTIAL'}},
 {...manualRow,capital_scope:'strategy',window:{...manualWindow,coverage:'PARTIAL'}},
 {...manualRow,additive_with_account_nav:true,window:{...manualWindow,coverage:'PARTIAL'}},
 {...manualRow,coverage_reason:'STRATEGY_CAPITAL_PROVENANCE_MISSING',
  window:{...manualWindow,coverage:'PARTIAL'}},
 {...manualRow,window:{...manualWindow,coverage:'PARTIAL',current_equity:'10'}},
 {kind:'nav',capital_scope:'strategy',coverage_reason:'STRATEGY_CAPITAL_PROVENANCE_MISSING',
  window:{...manualWindow,coverage:'PARTIAL',scope_id:'strategy:ema_cross'}},
]){
 const response:any=portfolioEnvelope([accountRow,unknown]);
 if(response.status!=='partial'||response.quality!=='partial')
  throw Error('P0.2 unknown/duplicate strategy capital promoted to complete portfolio');
}
const missingAccount:any=portfolioEnvelope([{...accountRow,
 window:{...accountRow.window,coverage:'PARTIAL'}},manualRow]);
if(missingAccount.status!=='partial')throw Error('P0.2 manual label hid missing account capital');
const markedPartial:any=portfolioEnvelope([accountRow,manualRow],'partial');
if(markedPartial.status!=='partial')throw Error('P0.2 authoritative partial quality overruled by manual');
console.log('mcp_finance_manual_is_account_capital_without_false_partial PASS');

const oversized='x'.repeat(13_000);
const budgetEnv:any={RUNTIME:{idFromName:()=>0,get:()=>({fetch:async(_url:string,init:RequestInit)=>Response.json({schema:'okx.agent.response/v1',request_id:JSON.parse(String(init.body)).request_id,status:'completed',quality:'FRESH',warnings:[],result:{rows:[oversized]}})})}};
const budget:any=await(await mcpApi.fetch(new Request('https://fixture/mcp',{method:'POST',body:JSON.stringify({jsonrpc:'2.0',id:3,method:'tools/call',params:{name:'market',arguments:payloadFixtures.find(x=>x.tool==='market')!.positive}})}),budgetEnv,ctx)).json();
const budgetResult=budget.result.structuredContent;
if(budgetResult.status!=='failed'||budgetResult.reason_code!=='RESPONSE_TOO_LARGE'||budgetResult.data.response_budget_bytes!==12_288||budgetResult.data.response_bytes<=12_288)throw Error('oversized native result must terminalize as bounded MCP failure');

// T01/T15: the read-only on-demand typed catalogue is larger than a normal
// decision packet. Keep every descriptor while retaining a finite bound and
// preserving the ordinary 12 KiB limit established above.
const catalogCall=async(padding:number):Promise<any>=>{
 const env:any={RUNTIME:{idFromName:()=>0,get:()=>({fetch:async(_url:string,init:RequestInit)=>Response.json({
  schema:'okx.agent.response/v1',request_id:JSON.parse(String(init.body)).request_id,status:'completed',quality:'FRESH',warnings:[],
  result:{schema:'okx.market.capabilities/v1',operators:[{name:'returns',version:1,unit:'ratio'}],
   composition:{operations:[{op:'rolling_range',version:1,input_unit:'price'}]},details:'x'.repeat(padding)}
 })})}};
 const reply:any=await(await mcpApi.fetch(new Request('https://fixture/mcp',{method:'POST',body:JSON.stringify({
  jsonrpc:'2.0',id:4,method:'tools/call',params:{name:'market',arguments:{action:'capabilities'}}
 })}),env,ctx)).json();
 return reply.result.structuredContent;
};
const catalog=await catalogCall(13_000);
if(catalog.status!=='completed'||catalog.data.schema!=='okx.market.capabilities/v1'||
 catalog.data.operators[0].unit!=='ratio'||catalog.data.composition.operations[0].op!=='rolling_range'||
 catalog.data.details.length!==13_000)throw Error('typed market capability descriptor lost at the public 12 KiB boundary');
const oversizedCatalog=await catalogCall(21_000);
if(oversizedCatalog.status!=='failed'||oversizedCatalog.reason_code!=='RESPONSE_TOO_LARGE'||
 oversizedCatalog.data.response_budget_bytes!==20_480||oversizedCatalog.data.response_bytes<=20_480)
 throw Error('catalogue exception must still terminate above its own finite 20 KiB bound');
console.log('market_capabilities_20k_only_preserves_descriptors_and_finite_bound PASS');

const nativeResponse=(response:unknown):any=>({RUNTIME:{idFromName:()=>0,get:()=>({fetch:async(_url:string,init:RequestInit)=>{
 if(!init?.body)return Response.json(response);
 const id=JSON.parse(String(init.body)).request_id;
 const isNative=(value:any)=>value?.schema==='okx.agent.response/v1';
 const align=(value:any):any=>isNative(value)?{...value,request_id:id}:value;
 const fixed=align(response&&typeof response==='object'?{...response}:response);
 if(fixed?.data&&isNative(fixed.data))fixed.data=align(fixed.data);
 if(fixed?.result&&isNative(fixed.result))fixed.result=align(fixed.result);
 return Response.json(fixed);
}})}});
const mcpResponse=async(tool:string,args:unknown,response:unknown,releaseTree?:string):Promise<any>=>{
 const env={...nativeResponse(response),RELEASE_SOURCE_TREE:releaseTree};
 const reply:any=await(await mcpApi.fetch(new Request('https://fixture/mcp',{method:'POST',
  body:JSON.stringify({jsonrpc:'2.0',id:99,method:'tools/call',params:publicArgs(tool,args)})}),env,ctx)).json();
 if(reply.error)throw Error(JSON.stringify(reply.error));
 return reply.result.structuredContent;
};

// Native Research rejects a non-incremental graph operator before admitting a
// job. The Edge exposes only the exact allowlisted step/operator identity, not
// a potentially sensitive or unbounded native failure message.
const graphPreflightFixture=payloadFixtures.find(f=>f.tool==='research'&&f.action==='graph_backtest_causal')!.positive;
const graphPreflightFailure=(code:string,message:string)=>({schema:'okx.agent.response/v1',
 request_id:'req_graph_preflight',status:'failed',quality:'FRESH',warnings:[],result:null,
 failure:{code,message,retryable:false}});
const supportedGraphRejection=await mcpResponse('research',graphPreflightFixture,
 graphPreflightFailure('GRAPH_OPERATOR_NOT_INCREMENTAL','graph_preflight:steps[volatility_1].op=rolling_stddev'));
if(supportedGraphRejection.status!=='failed'||supportedGraphRejection.reason_code!=='GRAPH_OPERATOR_NOT_INCREMENTAL'||
 supportedGraphRejection.retryable!==false||supportedGraphRejection.data!==null||
 supportedGraphRejection.job_id!==undefined||supportedGraphRejection.result_id!==undefined||
 supportedGraphRejection.next_action!=='choose_supported_graph_operator'||
 supportedGraphRejection.diagnostic?.stage!=='graph_preflight'||
 supportedGraphRejection.diagnostic?.step_id!=='volatility_1'||
 supportedGraphRejection.diagnostic?.operator!=='rolling_stddev'||
 JSON.stringify(supportedGraphRejection).includes('graph_preflight:steps['))
 throw Error('graph preflight failed to project a bounded pre-job rejection');
for(const operator of ['rolling_pearson','rolling_zscore'] as const){
 const rejected:any=projectNativeResponse(graphPreflightFailure('GRAPH_OPERATOR_NOT_INCREMENTAL',
  `graph_preflight:steps[step_1].op=${operator}`));
 if(rejected.diagnostic?.operator!==operator||rejected.diagnostic.step_id!=='step_1'||
    rejected.next_action!=='choose_supported_graph_operator')
  throw Error('known non-incremental graph operator lost its typed diagnosis');
}
for(const [code,message] of [
 ['GRAPH_OPERATOR_NOT_INCREMENTAL','graph_preflight:steps[volatility_1].op=rolling_stddev\nsecret'],
 ['GRAPH_OPERATOR_NOT_INCREMENTAL','graph_preflight:steps[volatile-ID].op=rolling_stddev'],
 ['GRAPH_OPERATOR_NOT_INCREMENTAL',`graph_preflight:steps[${'a'.repeat(33)}].op=rolling_stddev`],
 ['GRAPH_OPERATOR_NOT_INCREMENTAL','graph_preflight:steps[volatility_1].op=trade_execute'],
 ['GRAPH_OPERATOR_NOT_INCREMENTAL','other_stage:steps[volatility_1].op=rolling_stddev'],
 ['RESEARCH_REQUEST_FAILED','graph_preflight:steps[volatility_1].op=rolling_stddev'],
] as const){
 const rejected:any=projectNativeResponse(graphPreflightFailure(code,message));
 if(rejected.diagnostic!==undefined||rejected.next_action!=='inspect_diagnostics'||
    rejected.data!==null||JSON.stringify(rejected).includes(message))
  throw Error('untrusted graph failure message escaped strict preflight projection');
}
const inconsistentGraphResponse:any=projectNativeResponse({
 ...graphPreflightFailure('GRAPH_OPERATOR_NOT_INCREMENTAL','graph_preflight:steps[risk].op=rolling_stddev'),
 failure:{code:'GRAPH_OPERATOR_NOT_INCREMENTAL',
   message:'graph_preflight:steps[risk].op=rolling_stddev',retryable:true}
});
if(inconsistentGraphResponse.diagnostic!==undefined||
 inconsistentGraphResponse.next_action!=='inspect_diagnostics')
 throw Error('retryable native error was promoted to a deterministic graph preflight');
console.log('mcp_research_graph_preflight_safe_diagnostic PASS');

// A configured Research bearer is the public routing cutover. Verify every
// Research operation family, and ensure trading/portfolio financial ownership
// is unchanged even with the independent owner enabled.
const researchRoutes:[string,any,string,boolean][]=[
 ['market',{action:'capabilities'},'market_capabilities',true],
 ['market',{action:'instruments',asset:'BTC'},'find_instruments',true],
 ['market',{action:'snapshot',instrument:'BTC-USDT'},'market_overview',true],
 ['market',{action:'microstructure',instrument:'BTC-USDT'},'market_microstructure',true],
 ['market',payloadFixtures.find(f=>f.tool==='market'&&f.action==='screen')!.positive,'market',true],
 ['research',{action:'capabilities'},'research',true],
 ['diagnostics',{action:'explain',view:'research',reference:'job_fixture'},'diagnostics',true],
 ['diagnostics',{action:'explain',view:'finance',reference:'intent_fixture'},'diagnostics',false],
 ['portfolio',payloadFixtures.find(f=>f.tool==='portfolio'&&f.action==='scenario')!.positive,'portfolio_scenario',true],
 ['portfolio',payloadFixtures.find(f=>f.tool==='portfolio'&&f.action==='summary')!.positive,'portfolio',false],
 ['trade',{action:'status',account:'demo',reference:'intent_fixture'},'trade',false],
];
const fullTrace=await mcpResponse('diagnostics',
 {action:'explain',view:'research',reference:'request:req_original_01'},
 {status:'completed',data:{schema:'okx.research-job-diagnostics/v1',
  state:'OBSERVED',reason_code:'JOB_SNAPSHOT',job:{id:originJob,state:'running',
   origin_invocation:'req_original_01'}}});
if(fullTrace?.operation_trace?.state!=='OBSERVED'||
 fullTrace.operation_trace.next_action!=='research.status'||
 fullTrace.operation_trace.first_unproven_fact!=='TERMINAL_RESEARCH_OUTCOME'||
 fullTrace.operation_trace.reference_kind!=='request_id'||
 fullTrace.data.job.id!==originJob)
 throw Error('public diagnostics.explain lost Research operation summary');
console.log('diagnostics_explain_research_request_id PASS');
// A lost-response retry has a distinct request ID but the same durable job.
// Research now explicitly attests the *queried* invocation on an OBSERVED
// bounded lookup. Edge must not mistake the old origin ID for a retry match.
const retriedRequest='req_retry_after_lost_reply_02';
const retryJob={id:originJob,state:'completed',origin_invocation:'req_original_01',
 result_ref:`sha256:${'a'.repeat(64)}`};
const aliasTrace=await mcpResponse('diagnostics',
 {action:'explain',view:'research',reference:`request:${retriedRequest}`},
 {status:'partial',data:{schema:'okx.research-job-diagnostics/v1',
  state:'OBSERVED',reason_code:'JOB_SNAPSHOT',reference_kind:'request_id',
  matched_request_id:retriedRequest,job:retryJob}});
if(aliasTrace?.operation_trace?.state!=='OBSERVED'||
 aliasTrace.operation_trace.job_id!==originJob||
 aliasTrace.operation_trace.origin_request_id!=='req_original_01'||
 aliasTrace.operation_trace.next_action!=='research.result'||
 aliasTrace.operation_trace.quality!=='partial'||aliasTrace.operation_trace.history!=='UNKNOWN'||
 aliasTrace.operation_trace.first_unproven_fact!=='FULL_CAUSAL_HISTORY'||
 aliasTrace.data.job.result_ref!==retryJob.result_ref)
 throw Error('Research retry request was not bound to its durable job');
const accountDeniedTrace=await mcpResponse('diagnostics',
 {action:'explain',view:'operation',reference:'request:req_missing_account'},
 {status:'partial',data:{scope:'engine',stage:'operation',reason_code:'ACCOUNT_REQUIRED',
  quality:'partial',retryable:false,blockers:['ACCOUNT_REQUIRED'],
  next_action:'supply an authorized account and operation reference'}});
if(accountDeniedTrace?.operation_trace?.state!=='NOT_EVALUATED'||
 accountDeniedTrace.operation_trace.reason_code!=='ACCOUNT_REQUIRED'||
 accountDeniedTrace.operation_trace.subject!==null||
 accountDeniedTrace.operation_trace.next_action!==
  'supply_authorized_account_and_operation_reference')
 throw Error('public diagnostics.explain obscured owner account preflight denial');
const cancelOwnerTrace=await mcpResponse('diagnostics',
 {action:'explain',view:'operation',account:'demo',reference:`cancel:${financeKey}`},
 {status:'partial',data:cancelData});
if(cancelOwnerTrace?.operation_trace?.state!=='OBSERVED'||
 cancelOwnerTrace.operation_trace.subject?.kind!=='cancel_operation'||
 cancelOwnerTrace.operation_trace.first_unproven_fact!=='CANCEL_VENUE_ACK_AND_FINAL_STATE')
 throw Error('public diagnostics.explain lost owner cancel SQL-only evidence');
const replaceOwnerTrace=await mcpResponse('diagnostics',
 {action:'explain',view:'operation',account:'demo',reference:`replace:${financeKey}`},
 {status:'partial',data:replaceData});
if(replaceOwnerTrace?.operation_trace?.state!=='NOT_EVALUATED'||
 replaceOwnerTrace.operation_trace.subject!==null||
 replaceOwnerTrace.operation_trace.next_action!==
  'inspect_original_and_replacement_status_do_not_resend')
 throw Error('public diagnostics.explain inferred a replacement causal chain');
for(const [label,changed] of [
 ['missing owner match',{}],
 ['wrong match',{matched_request_id:'req_unrelated_03'}],
 ['wrong kind',{reference_kind:'job_id',matched_request_id:retriedRequest}],
 ['spoofed origin',{matched_request_id:'req_unrelated_03',job:{...retryJob,origin_invocation:retriedRequest}}],
 ['invalid job',{matched_request_id:retriedRequest,job:{...retryJob,id:'not-a-job'}}],
] as const){
 const trace=await mcpResponse('diagnostics',
  {action:'explain',view:'research',reference:`request:${retriedRequest}`},
  {status:'partial',data:{schema:'okx.research-job-diagnostics/v1',
   state:'OBSERVED',reason_code:'JOB_SNAPSHOT',reference_kind:'request_id',
   job:retryJob,...changed}});
 if(trace?.operation_trace?.state!=='NOT_EVALUATED'||trace.operation_trace.job_id!==null)
  throw Error(`unproven Research retry was promoted: ${label}`);
}
for(const [state,reason] of [
 ['NOT_OBSERVED','UNKNOWN_NOT_RETAINED'],
 ['NOT_EVALUATED','AMBIGUOUS_REQUEST_ORIGIN'],
] as const){
 const trace=await mcpResponse('diagnostics',
  {action:'explain',view:'research',reference:`request:${retriedRequest}`},
  {status:'partial',data:{schema:'okx.research-job-diagnostics/v1',
   state,reason_code:reason,reference_kind:'request_id',
   matched_request_id:retriedRequest,job:null}});
 if(trace?.operation_trace?.state!=='NOT_EVALUATED'||trace.operation_trace.job_id!==null)
  throw Error(`unobserved Research request was promoted: ${reason}`);
}
console.log('diagnostics_explain_research_retry_owner_match_fail_closed PASS');
for(const configured of [false,true]){
 const seen:any[]=[];
 const routeEnv:any={...(configured?{RESEARCH_RUNTIME_TOKEN_SHA256:'f'.repeat(64)}:{}),
  RUNTIME:{idFromName:()=>0,get:()=>({fetch:async(_url:string,init:RequestInit)=>{
   seen.push(JSON.parse(String(init.body)));
   return Response.json({status:'completed',data:{ok:true}});
  }})}};
 for(const [name,args,operation,researchOwned] of researchRoutes){
  const reply:any=await(await mcpApi.fetch(new Request('https://fixture/mcp',{method:'POST',body:JSON.stringify({
   jsonrpc:'2.0',id:23,method:'tools/call',params:publicArgs(name,args)
  })}),routeEnv,ctx)).json();
  if(reply.error||reply.result?.structuredContent?.status!=='completed')
   throw Error(`routing fixture rejected ${name}.${args.action}: ${JSON.stringify(reply)}`);
  const sent=seen.at(-1);
  if(sent?.operation?.type!==operation || sent.target_owner!==(researchOwned?'research':undefined))
   throw Error(`incorrect ${configured?'configured':'legacy'} owner route for ${name}.${args.action}`);
 }
 if(seen.length!==researchRoutes.length)throw Error('owner routing test lost a native dispatch');
}
for(const forbidden of [
 {name:'research',arguments:{action:'capabilities',target_owner:'engine'}},
 {name:'market',arguments:{action:'capabilities',target_owner:'engine'}},
]){
 const reply:any=await(await mcpApi.fetch(new Request('https://fixture/mcp',{method:'POST',body:JSON.stringify({
  jsonrpc:'2.0',id:24,method:'tools/call',params:forbidden
 })}),{RESEARCH_RUNTIME_TOKEN_SHA256:'f'.repeat(64)} as any,ctx)).json();
 if(reply.error?.code!==-32602)throw Error('MCP caller tried to override authenticated target owner');
}
console.log('mcp_research_owner_routes_independent_of_credential PASS');

// P0 #537: an optional user graph must reach the Native request unchanged.
// Otherwise different hypotheses can silently reuse the flat market job.
const graphBase={action:'screen',instruments:['BTC-USDT'],fields:['returns'],
 timeframe_ms:60_000,start_ms:1795366800000,end_ms:1795374000000,limit:50};
const graphVariants=[
 graphBase,
 {...graphBase,composition:{version:'okx.market.composition/v1',
  steps:[{id:'width',op:'rolling_range',input:'close',period:3}],result_steps:['width']}},
 {...graphBase,composition:{version:'okx.market.composition/v1',steps:[
  {id:'change',op:'returns_series',input:'close'},
  {id:'avg3',op:'rolling_mean',input:'change',period:3},
  {id:'positive',op:'greater_than',input:'avg3',threshold:0},
  {id:'turn_on',op:'rising_edge',input:'positive'}],result_steps:['avg3','turn_on']}},
];
const graphCalls:any[]=[];
const graphEnv:any={RUNTIME:{idFromName:()=>0,get:()=>({fetch:async(_url:string,init:RequestInit)=>{
 const native=JSON.parse(String(init.body));graphCalls.push(native);
 const fields=native.operation.request.composition?.result_steps??[];
 return Response.json({schema:'okx.agent.response/v1',request_id:native.request_id,
  status:'completed',quality:'FRESH',warnings:[],result:{kind:'market',
   job_id:'job-'+(fields.join('-')||'flat'),market_request_fingerprint:'hash-'+(fields.join('-')||'flat'),
   result_artifact:'artifact-'+(fields.join('-')||'flat'),query_plan:{composition:native.operation.request.composition??null},
   rows:[{metrics:{returns:-0.0073745589964719915,...(fields.includes('width')?{width:46.5}:{}),
    ...(fields.includes('avg3')?{avg3:-0.00031571803975194435,turn_on:false}:{})}}]}});
}})}};
const graphResults:any[]=[];
for(const args of graphVariants){
 const reply:any=await(await mcpApi.fetch(new Request('https://fixture/mcp',{method:'POST',
  body:JSON.stringify({jsonrpc:'2.0',id:91,method:'tools/call',params:{name:'market',arguments:args}})}),graphEnv,ctx)).json();
 if(reply.error)throw Error('MCP graph rejected: '+JSON.stringify(reply.error));
 graphResults.push(reply.result.structuredContent);
 const sent=graphCalls.at(-1);
 if(sent.schema!=='okx.agent.request/v1'||sent.operation.type!=='market'||
   JSON.stringify(sent.operation.request)!==JSON.stringify(args))
  throw Error('MCP graph definition lost before Native dispatch');
}
if(new Set(graphResults.map(r=>r.data.job_id)).size!==3||
 new Set(graphResults.map(r=>r.data.market_request_fingerprint)).size!==3||
 new Set(graphResults.map(r=>r.data.result_artifact)).size!==3||
 graphResults[0].data.rows[0].metrics.width!==undefined||
 graphResults[1].data.rows[0].metrics.width!==46.5||
 graphResults[2].data.rows[0].metrics.avg3!==-0.00031571803975194435||
 graphResults[2].data.rows[0].metrics.turn_on!==false||
 !body.result.tools.find((tool:any)=>tool.name==='market')?.description.includes('composition'))
 throw Error('MCP graph projection or public invocation guidance lost');
console.log('P0_mcp_three_graphs_forwarded_and_distinct PASS');
// Exercise the real Worker MCP projection/dispatch for 100 independent read-only
// requests. The Durable Object is a deterministic fixture, not a physical network SLA.
let inFlightRead=0,maxInFlightRead=0,nativeReadCount=0;
const readOnlyEnv:any={RUNTIME:{idFromName:()=>0,get:()=>({fetch:async(_url:string,init:RequestInit)=>{
 nativeReadCount++;inFlightRead++;maxInFlightRead=Math.max(maxInFlightRead,inFlightRead);
 await Promise.resolve();inFlightRead--;
 return Response.json({schema:'okx.agent.response/v1',request_id:JSON.parse(String(init.body)).request_id,
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

// Worker MCP mutation routes through an in-memory DO with NO exchange access.
// Success and business refusal: route fidelity, identity, retries, bounded shell.
const fakeMutationActions:[string,string][]=[
 ['accounts','connect'],['accounts','pause'],['accounts','resume'],['accounts','set_limits'],
 ['strategies','create'],['strategies','update'],['strategies','start'],['strategies','stop'],
 ['strategies','restore_configuration'],
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
   if(path==='/account-governance')return Response.json({status:'CONFIGURED',revision:0,accounts:[]});
   if(path==='/dispatch')return Response.json({schema:'okx.agent.response/v1',
    request_id:request.request_id,status:'completed',quality:'FRESH',warnings:[],
    result:{schema:'okx.fake-venue.fixture/v1',venue:'fake',exchange_post_count:0}});
   return Response.json({status:'completed',data:{venue:'fake',exchange_post_count:0}});
  };
  const fake:any={RUNTIME:{idFromName:()=>0,get:()=>({fetch:fakeHandler})}};
  const rpc:any=await(await mcpApi.fetch(new Request('https://fixture/mcp',{method:'POST',
   body:JSON.stringify({jsonrpc:'2.0',id:902,method:'tools/call',params:publicArgs(tool,args)})}),fake,ctx)).json();
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
   if(visits.length!==3||visits[1].path!=='/account-governance'||
     visits[2].path!=='/account-governance/enroll'||
     visits[2].request?.alias!==args.alias||visits[2].request?.credential_ref!==args.credential_ref)
    throw Error('connect did not prove central enrollment binding');
  }else if(visits.length!==1)throw Error('mutation dispatched repeatedly '+tool+'.'+action);
 }
}
console.log(`mcp_fake_venue_mutation_routes PASS ${fakeMutationActions.length} actions positive/negative 0 real exchange POST`);

const connectFixture=payloadFixtures.find(x=>x.tool==='accounts'&&x.action==='connect')!;
const attachArgs={...connectFixture.positive,enabled:false};
const attachedUid='a'.repeat(64),attachedKey='okx:demo:'+attachedUid;
const attachedAccount={id:attachArgs.alias,label:attachArgs.label??attachArgs.alias,
 account_key:attachedKey,uid_fingerprint:attachedUid,venue:'okx',environment:'demo',
 identity_verified:true,enabled:true,explicit_pause:false,policy_revision:3,
 limits:attachArgs.limits,strategies:[]};
for(const variation of ['requires_pause','already_disabled','wrong_identity','unsafe_enable','pause_refused']){
 const args={...attachArgs,enabled:variation==='unsafe_enable'};
 const account={...attachedAccount,...(variation==='already_disabled'?{enabled:false}:{})};
 if(variation==='wrong_identity')account.uid_fingerprint='b'.repeat(64);
 const state={status:'CONFIGURED',revision:3,accounts:[account]};
 const visits:{path:string;request:any}[]=[];
 const fakeHandler=async(url:string,init?:RequestInit)=>{
  const path=new URL(url).pathname;
  const request=init?.body?JSON.parse(String(init.body)):null;
  visits.push({path,request});
  if(path==='/dispatch'){
   const op=request.operation;
   return Response.json({status:'completed',result:{alias:op.alias,credential_ref:op.credential_ref,
    connect_operation_id:op.operation_id,configuration_digest:op.request_digest,
    account_key:{venue:'okx',environment:'demo',verified_uid:attachedUid}}});
  }
  if(path==='/account-governance')return Response.json(state);
  if(path==='/account-governance/pause'){
   if(request.account!==args.alias||request.expected_revision!==3||
      request.explicit_pause!==true||request.idempotency_key!==args.idempotency_key)
    throw Error('unsafe attached account CAS pause');
   if(variation==='pause_refused')return Response.json({status:'FAILED',reason:'STALE_ACCESS_REVISION'});
   return Response.json({status:'CONFIGURED',configuration:{...state,revision:4,
    accounts:[{...account,explicit_pause:true,policy_revision:4}]}});
  }
  if(path==='/account-governance/enroll'&&variation==='unsafe_enable'){
   if(request.alias!==args.alias||request.enabled!==true)
    throw Error('unsafe enrollment replay request changed');
   // A new operation key cannot register a duplicate; the real DO owns
   // exact original-key replay and refuses any new enable grant.
   return Response.json({status:'FAILED',reason:'ACCOUNT_ALREADY_CONNECTED'});
  }
  throw Error('reattachment must not enroll or mutate an independent owner: '+path);
 };
 const fake:any={RUNTIME:{idFromName:()=>0,get:()=>({fetch:fakeHandler})}};
 const rpc:any=await(await mcpApi.fetch(new Request('https://fixture/mcp',{method:'POST',
  body:JSON.stringify({jsonrpc:'2.0',id:913,method:'tools/call',
   params:publicArgs('accounts',args)})}),fake,ctx)).json();
 if(rpc.error||!rpc.result?.structuredContent)throw Error('verified reattach tool error '+JSON.stringify(rpc.error));
 const outcome=rpc.result.structuredContent;
 if(variation==='requires_pause'){
  if(outcome.status!=='completed'||outcome.data.reattached_existing!==true||
     outcome.data.configuration.accounts.length!==1||outcome.data.account.explicit_pause!==true||
     visits.map(v=>v.path).join(',')!=='/dispatch,/account-governance,/account-governance/pause')
   throw Error('existing verified account was not paused/reused exactly once');
 }else if(variation==='already_disabled'){
  if(outcome.status!=='completed'||outcome.data.reattached_existing!==true||
    visits.map(v=>v.path).join(',')!=='/dispatch,/account-governance')
   throw Error('already-disabled account created new central mutation');
 }else{
  const expected=variation==='wrong_identity'?'EXISTING_ACCOUNT_IDENTITY_MISMATCH':
    variation==='unsafe_enable'?'ACCOUNT_ALREADY_CONNECTED':'STALE_ACCESS_REVISION';
  const enrollmentVisits=visits.filter(v=>v.path==='/account-governance/enroll').length;
  if(outcome.reason_code!==expected||!['failed','rejected'].includes(outcome.status)||
    enrollmentVisits!==(variation==='unsafe_enable'?1:0))
   throw Error('reattach failure not fail-closed '+variation+': '+JSON.stringify(outcome));
 }
}
console.log('existing_verified_account_reattach_fail_closed PASS one account no venue POST');


const rawFailure=(code:string)=>({schema:'okx.agent.response/v1',request_id:'native_fail_398',
 status:'rejected',generated_at:'2026-10-23T00:00:00Z',quality:'NOT_READY',result_schema:'okx.product/v1',
 result:null,failure:{code,message:code,retryable:false},warnings:[]});
const nativeNotFound=await mcpResponse('research',{action:'status',job_id:'missing_job_398'},
 {status:'completed',data:rawFailure('JOB_NOT_FOUND')});
if(nativeNotFound.status!=='rejected'||nativeNotFound.reason_code!=='JOB_NOT_FOUND'||
 nativeNotFound.data!==null||nativeNotFound.retryable!==false||
 !nativeNotFound.request_id.startsWith('req_mcp_'))throw Error('DO wrapped JOB_NOT_FOUND code lost');
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
const researchOutcomeUnknown=await mcpResponse('research',{action:'benchmark',idempotency_key:'research_benchmark_01'},
 {schema:'okx.direct-transport.result/v1',status:'UNKNOWN',reason:'RESEARCH_MUTATION_OUTCOME_UNKNOWN_CHECK_JOB',
  scope:'research',retryable:false,next_action:'inspect_research_job_or_idempotency_receipt'});
if(researchOutcomeUnknown.status!=='unknown'||researchOutcomeUnknown.retryable!==false||
 researchOutcomeUnknown.scope!=='research'||researchOutcomeUnknown.reason_code!=='RESEARCH_MUTATION_OUTCOME_UNKNOWN_CHECK_JOB'||
 researchOutcomeUnknown.next_action!=='inspect_research_job_or_idempotency_receipt'||
 researchOutcomeUnknown.next_action==='inspect_exact_intent')
 throw Error('Research outcome unknown incorrectly became a trade reconciliation or blind retry');
const deterministicUnknown=projectNativeResponse({status:'FAILED',reason:'UNKNOWN_ACCOUNT',retryable:false});
if(deterministicUnknown.status!=='failed'||deterministicUnknown.reason_code!=='UNKNOWN_ACCOUNT')
 throw Error('deterministic unknown reason incorrectly became uncertain POST');
console.log('transport_unknown_without_resubmit PASS');

const invalidNative=projectNativeResponse({schema:'okx.agent.response/v1',status:'completed',result:{id:'bad'}});
if(invalidNative.status!=='failed'||invalidNative.reason_code!=='INVALID_NATIVE_ENVELOPE')
 throw Error('malformed native response must fail closed');

const healthyRuntime=await mcpResponse('diagnostics',{action:'runtime'},
 {schema:'okx.direct-transport.status/v1',status:'PASS',runtime_id:'windows-primary',
  runtime_profile:'engine',connected:true,session_fresh:true,connection_generation:73},'b'.repeat(40));
if(healthyRuntime.status!=='completed'||healthyRuntime.data.schema!=='okx.runtime-status/v1'||
 healthyRuntime.data.transport.session_fresh!==true||healthyRuntime.data.transport.connection_generation!==73||
 typeof healthyRuntime.data.tool_contract!=='string'||healthyRuntime.data.release_source_tree!=='b'.repeat(40))
 throw Error('runtime must expose exact deployment source tree without claiming installed Windows proof');
if(healthyRuntime.data.installation_verification.owner!=='WINDOWS_CONTROL'||
 healthyRuntime.data.installation_verification.worker_independent_proof!=='NOT_PERFORMED'||
 'release_attestation' in healthyRuntime.data)
 throw Error('authenticated WSS cannot imply independent installed-file proof');
console.log('native_control_owner_without_worker_gh_attestation PASS');
const offlineRuntime=await mcpResponse('diagnostics',{action:'runtime'},
 {schema:'okx.direct-transport.status/v1',status:'OFFLINE',runtime_id:'windows-primary',
  connected:false,session_fresh:false});
if(offlineRuntime.status!=='partial'||offlineRuntime.data.reason_code!=='RUNTIME_SESSION_NOT_FRESH'||
 offlineRuntime.data.transport.connected!==false||offlineRuntime.data.release_source_tree!==null)
 throw Error('offline session promoted to fresh or invented deployment source tree');
const corruptRuntime=await mcpResponse('diagnostics',{action:'runtime'},{status:'PASS'});
if(corruptRuntime.status!=='failed'||corruptRuntime.reason_code!=='INVALID_RUNTIME_STATUS')
 throw Error('malformed runtime observation accepted');
console.log('diagnostics_runtime_freshness_and_negative PASS');

const deploymentTree='a'.repeat(40);
for(const [transportStatus,connected,sessionFresh] of [
 ['PASS',true,true],['STALE',true,false],['OFFLINE',false,false]
] as const){
 const verifiedEdge=transportStatus==='PASS';
 const deploymentEnv:any={RELEASE_SOURCE_TREE:deploymentTree,RUNTIME:{idFromName:()=>0,get:()=>({
  fetch:async(url:string)=>{
   if(!url.includes('/status?probe=1&owner=research'))throw Error('deployment must observe Research transport');
   return Response.json({schema:'okx.direct-transport.status/v1',owner:'research',status:transportStatus,
    connected,session_fresh:sessionFresh,agent_pid:connected?31:null,
    agent_binary_sha256:connected?'b'.repeat(64):null,
    ...(connected?{runtime_profile:'research'}:{})});
  }
 })}};
 const response:any=await(await mcpApi.fetch(new Request('https://fixture/mcp',{method:'POST',body:JSON.stringify({
  jsonrpc:'2.0',id:96,method:'tools/call',params:{name:'diagnostics',arguments:{action:'explain',view:'research',reference:'deployment'}}
 })}),deploymentEnv,ctx)).json();
 const result=response.result.structuredContent, data=result.data;
 if(result.status!=='partial'||data.schema!=='okx.research.deployment-edge/v1'||
    data.edge.release_source_tree!==deploymentTree||data.edge.research_transport.connected!==connected||
    data.edge.research_transport.agent_reported_pid!==(connected?31:null)||
    data.edge.research_transport.agent_reported_binary_sha256!==(connected?'b'.repeat(64):null)||
    data.control.status!=='unavailable'||data.control.evidence!=='NOT_PERFORMED'||
    data.control.pid!==null||data.control.active_worker_sha256!==null||
    data.control.installed_worker_sha256!==null||data.control.process_generation!==null||
    data.control.reason_code!=='CONTROL_EVIDENCE_UNAVAILABLE'||
    data.verification.edge_authenticated_session!==(verifiedEdge?'PASS':'UNVERIFIED')||
    data.verification.control_process_proof!=='NOT_PERFORMED'||
    data.verification.first_unproven_stage!==(verifiedEdge?'CONTROL_PID_SHA_GENERATION':'EDGE_AUTHENTICATED_SESSION')||
    data.verification.next_action!==(verifiedEdge?'inspect_control_research_status':'inspect_research_connection'))
  throw Error('deployment must report Edge observation and unavailable CONTROL evidence');
}
// P0 independent CONTROL observation: only a fresh challenge bound to the
// same Research WSS generation/PID/SHA may change partial to completed.
for(const mode of ['bound','wrong_pid','wrong_sha','stale_generation','unavailable','malformed','unverified'] as const){
 const contacted:string[]=[];
 const status={schema:'okx.direct-transport.status/v1',owner:'research',status:'PASS',
  connected:true,session_fresh:true,runtime_profile:'research',connection_generation:88,
  agent_pid:32,agent_binary_sha256:'b'.repeat(64)};
 const attestation={schema:'okx.research.attestation/v1',status:mode==='unverified'?'UNVERIFIED':'PASS',
  reason_code:mode==='unverified'?'CONTROL_ATTEST_OFFLINE':null,
  pid:mode==='wrong_pid'?99:32,
  active_worker_sha256:mode==='wrong_sha'?'c'.repeat(64):'b'.repeat(64),
  installed_worker_sha256:'b'.repeat(64),process_generation:7,
  research_connection_generation:mode==='stale_generation'?87:88,observed_at_ms:Date.now()};
 const env:any={CONTROL_ATTEST_TOKEN_SHA256:'d'.repeat(64),RUNTIME:{idFromName:()=>0,get:()=>({
  fetch:async(url:string)=>{
   contacted.push(url);
   if(url.includes('/status?probe=1&owner=research'))return Response.json(status);
   if(url.includes('/attestation/research')){
    if(mode==='unavailable')throw Error('CONTROL readback offline');
    return Response.json(mode==='malformed'?{status:'PASS'}:attestation);
   }
   throw Error('CONTROL diagnostic attempted dispatch or finance');
  }
 })}};
 const reply:any=await(await mcpApi.fetch(new Request('https://fixture/mcp',{method:'POST',body:JSON.stringify({
  jsonrpc:'2.0',id:187,method:'tools/call',params:{name:'diagnostics',
   arguments:{action:'explain',view:'research',reference:'deployment'}}
 })}),env,ctx)).json();
 const result=reply.result.structuredContent;
 const verified=mode==='bound';
 if(result.status!==(verified?'completed':'partial')||
    result.data.verification.control_process_proof!==(verified?'PASS':'UNVERIFIED')||
    result.data.control.status!==(verified?'verified':'unavailable')||
    result.data.control.process_generation!==(verified?7:null)||
    result.data.control.pid!==(verified?32:null)||
    result.data.verification.first_unproven_stage!==(verified?null:'CONTROL_PID_SHA_GENERATION')||
    contacted.length!==2||!contacted[1].includes('/attestation/research')||
    (verified&&result.warnings.length!==0)||(!verified&&result.warnings.length===0))
   throw Error(`CONTROL ${mode}: cannot promote unbound PROCESS proof to PASS`);
}
console.log('diagnostics_research_control_process_attestation_bound_and_negative PASS');
// Two stopped native applications are a valid installation state: both
// read-only diagnostics must remain available and must not attempt a hidden
// dispatch, start, credential lookup, or fallback to the other owner.
{
 const observed:string[]=[];
 const env:any={RUNTIME:{idFromName:()=>0,get:()=>({fetch:async(url:string,init?:RequestInit)=>{
  observed.push(url);
  if(init?.method==='POST'||url.includes('/dispatch'))throw Error('status started a native owner');
  const research=url.includes('owner=research');
  return Response.json({schema:'okx.direct-transport.status/v1',owner:research?'research':'engine',
   status:'OFFLINE',connected:false,session_fresh:false});
 }})}};
 const read=async(args:unknown)=>{
  const response:any=await(await mcpApi.fetch(new Request('https://fixture/mcp',{method:'POST',body:JSON.stringify({
   jsonrpc:'2.0',id:142,method:'tools/call',params:{name:'diagnostics',arguments:args}
  })}),env,ctx)).json();
  if(response.error)throw Error('stopped owner diagnostic rejected');
  return response.result.structuredContent;
 };
 const engine=await read({action:'runtime'});
 const research=await read({action:'explain',view:'research',reference:'deployment'});
 if(engine.status!=='partial'||engine.data.transport.connected!==false||
    engine.data.transport.status!=='OFFLINE'||research.status!=='partial'||
    research.data.verification.edge_authenticated_session!=='UNVERIFIED'||
    research.data.verification.first_unproven_stage!=='EDGE_AUTHENTICATED_SESSION'||
    research.data.control.evidence!=='NOT_PERFORMED'||
    observed.length!==2||!observed[0].includes('/status?probe=1')||
    !observed[1].includes('/status?probe=1&owner=research'))
  throw Error('both native owners stopped: read-only diagnostic must be honest and side-effect free');
}
const deploymentRead={action:'explain',view:'research',reference:'deployment'};
const wrongOwner=await mcpResponse('diagnostics',deploymentRead,
 {schema:'okx.direct-transport.status/v1',owner:'engine',runtime_profile:'engine',
  status:'PASS',connected:true,session_fresh:true});
if(wrongOwner.reason_code!=='INVALID_RUNTIME_STATUS')
 throw Error('Engine transport cannot be used as Research deployment evidence');
const accountDeployment=await mcpResponse('diagnostics',{...deploymentRead,account:'scope-not-applicable'},
 {schema:'okx.direct-transport.status/v1',owner:'research',status:'OFFLINE',connected:false,session_fresh:false});
if(accountDeployment.reason_code!=='RESEARCH_OPERATION_FORBIDDEN')
 throw Error('Research deployment cannot select a trading account');
console.log('diagnostics_research_deployment_edge_only_readback PASS');
