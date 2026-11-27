import {PUBLIC_TOOLS,isExecutionMutation,validTool} from './execution_tools.js';
import {dispatchRuntime,type Env,isObject,type Json,jsonRpc,jsonRpcError,MAX_BODY_BYTES,MCP_PROTOCOL_VERSION,MCP_SERVER_VERSION,TOOL_CONTRACT_VERSION,requestId,runtimeFetch,toolResult as baseToolResult,sha256Hex,transportFailure,workerReleaseSourceTree} from './shared.js';
const NORMAL_RESULT_BYTES=12_288;
const RESEARCH_OWNER = {target_owner:'research'} as const;
// An on-demand, read-only operator catalogue includes all versioned type and
// provenance descriptors. It is not a normal Research decision packet; keep
// its own finite bound rather than silently dropping required T01/T15 fields.
const MARKET_CAPABILITIES_RESULT_BYTES=20_480;
function hasOnlyKeys(v:Record<string,unknown>,keys:string[]){return Object.keys(v).every(k=>keys.includes(k));}
function canonical(v:unknown):string{if(Array.isArray(v))return '['+v.map(canonical).join(',')+']';if(isObject(v))return '{'+Object.keys(v).sort().map(k=>JSON.stringify(k)+':'+canonical(v[k])).join(',')+'}';return JSON.stringify(v);}
function bytes(value:Json):number{return new TextEncoder().encode(JSON.stringify(value)).byteLength;}
function stringField(value:Record<string,unknown>,field:string):string|undefined{return typeof value[field]==='string'?value[field]:undefined;}
function numericField(value:Record<string,unknown>,field:string):number|undefined{return typeof value[field]==='number'&&Number.isFinite(value[field])?value[field]:undefined;}
function warnings(...sources:Record<string,unknown>[]):string[]{return sources.flatMap(source=>Array.isArray(source.warnings)?source.warnings.filter((v):v is string=>typeof v==='string'):[]);}
function jobState(value:Record<string,unknown>):string|undefined {
 const state=stringField(value,'state')?.toLowerCase();
 return state&&['queued','running','completed','failed','cancelled','partial','interrupted','expired'].includes(state)?state:undefined;
}
function partialQuality(value:unknown):boolean {
 if(typeof value==='string')return ['partial','missing','stale','degraded','not_ready','unavailable'].includes(value.toLowerCase());
 // Inspect real result rows as well as the top-level quality. A bounded walk
 // prevents adversarially deep or oversized native JSON from blocking the Worker.
 const stack:[unknown,number][]=[[value,0]];let visited=0;
 while(stack.length){
   const [next,depth]=stack.pop()!;
   if(++visited>4096||depth>24)return true;
   if(!isObject(next)&&!Array.isArray(next))continue;
   if(Array.isArray(next)){for(const child of next)stack.push([child,depth+1]);continue;}
   for(const [key,child] of Object.entries(next)){
     if((key==='quality'||key==='coverage')&&typeof child==='string'&&
       ['partial','missing','stale','degraded','not_ready','unavailable'].includes(child.toLowerCase()))return true;
     if(key==='state'&&typeof child==='string'&&child.toLowerCase()==='partial')return true;
     if(isObject(child)||Array.isArray(child))stack.push([child,depth+1]);
   }
 }
 return false;
}
function nativeEnvelope(value:Record<string,unknown>):Record<string,unknown>|undefined {
 // /dispatch returns the native AgentResponse as the DO reply. Test harnesses
 // and intermediate transports may contain one or two typed wrapping layers.
 // Only the exact schema is unwrapped, never an arbitrary result.data object.
 let current=value;
 for(let i=0;i<3;i++){
   if(current.schema==='okx.agent.response/v1')return current;
   const nested=isObject(current.data)&&current.data.schema==='okx.agent.response/v1'
     ?current.data:isObject(current.result)&&current.result.schema==='okx.agent.response/v1'?current.result:undefined;
   if(nested)return nested;
   if(isObject(current.data)&&isObject(current.data.result)&&
      current.data.result.schema==='okx.agent.response/v1'){current=current.data;continue;}
   if(isObject(current.result)&&isObject(current.result.data)&&
      current.result.data.schema==='okx.agent.response/v1'){current=current.result;continue;}
   break;
 }
 return undefined;
}
/** Bind an early transport failure to the same request as the native call.
 * Never accept a different native response identity as mutation proof.
 */
async function dispatchCorrelated(env:Env,request:Record<string,unknown>):Promise<Json>{
 const expected=String(request.request_id);
 const raw=await dispatchRuntime(env,request);
 if(!isObject(raw))return {schema:'okx.direct-transport.result/v1',request_id:expected,
   status:'FAILED',data:null,reason:'INVALID_RUNTIME_RESULT',retryable:false};
 const native=nativeEnvelope(raw);
 if((raw.request_id!==undefined&&raw.request_id!==expected)||
    (native&&native.request_id!==expected)){
   const research=request.target_owner==='research';
   return {schema:'okx.direct-transport.result/v1',request_id:expected,
     status:'UNKNOWN',data:null,reason:'RESPONSE_REQUEST_ID_MISMATCH',retryable:false,
     scope:research?'research':'engine',
     next_action:research?'inspect_research_job_or_idempotency_receipt':'inspect_exact_intent'};
 }
 // A pre-delivery DO error has no native request_id and is safe to bind.
 // A malformed AgentResponse is already rejected by its strict mismatch.
 return {...raw,request_id:expected} as Json;
}
function normalizedStatus(outer:Record<string,unknown>,frame:Record<string,unknown>,result:Record<string,unknown>|undefined):string{
 const outerStatus=stringField(outer,'status')?.toLowerCase();
 const nativeStatus=stringField(frame,'status')?.toLowerCase();
 const resultStatus=stringField(result??{},'status')?.toLowerCase();
 const reason=stringField(frame,'reason')??stringField(frame,'reason_code')??stringField(outer,'reason')??'';
 // Only the canonical delivery-unknown code means an uncertain exchange POST.
 // UNKNOWN_ACCOUNT, UNKNOWN_FIELD, etc. are ordinary deterministic failures.
 if([outerStatus,nativeStatus,resultStatus].includes('unknown')||
    reason==='MUTATION_OUTCOME_UNKNOWN_RECONCILE_EXACT_INTENT')return 'unknown';
 if(['failed','rejected'].includes(nativeStatus??''))return nativeStatus!;
 if(['failed','rejected'].includes(outerStatus??''))return outerStatus!;
 if(['failed','rejected'].includes(resultStatus??''))return resultStatus!;
 const job=jobState(result??{});
 if(job==='queued'||job==='running')return 'accepted';
 if(['failed','cancelled','interrupted','expired'].includes(job??''))return 'failed';
 if(job==='partial'||partialQuality(frame.quality)||partialQuality(result)||partialQuality(outer.quality))return 'partial';
 if(resultStatus==='accepted'||nativeStatus==='accepted')return 'accepted';
 if(['completed','configured','connected','observed','updated'].includes(nativeStatus??'')||
    ['completed','configured','connected','observed','updated'].includes(outerStatus??''))return 'completed';
 return nativeStatus??outerStatus??'completed';
}
/** One legacy TradeService/Strategies completion shell inside the typed AgentResponse.
 * Exact field and status validation prevents arbitrary business objects being unwrapped.
 * The native AgentResponse remains the only status and identity owner.
 */
function completedBusinessEnvelope(value:unknown):Record<string,unknown>|undefined{
 if(!isObject(value)||value.status!=='completed'||!('data' in value)||
    typeof value.as_of_ms!=='number'||!Number.isSafeInteger(value.as_of_ms)||
    !isObject(value.quality)||!Array.isArray(value.warnings)||
    !value.warnings.every(w=>typeof w==='string')||
    !Object.keys(value).every(k=>['status','as_of_ms','data','quality','warnings'].includes(k)))
    return undefined;
 return value;
}
/** Normalize exactly one native product response, not a nested JSON passthrough. */
export function projectNativeResponse(value:Json):Record<string,Json>{
 const outer=isObject(value)?value:{data:value};
 const native=nativeEnvelope(outer);
 if(native&&(typeof native.request_id!=='string'||typeof native.status!=='string'||!('result' in native||'failure' in native))){
   return {request_id:stringField(outer,'request_id')??requestId(),status:'failed',as_of_ms:Date.now(),
     data:null,quality:'NOT_READY',warnings:[],reason_code:'INVALID_NATIVE_ENVELOPE',scope:'transport',retryable:false,next_action:'inspect_diagnostics'};
 }
 const frame=native??outer;
 const supplied=native?frame.result:('result' in frame?frame.result:'data' in frame?frame.data:frame);
 // Rust TradeService can return a completed product inside AgentResponse.
 // Project its business data once rather than sending duplicate envelopes.
 const completedInner=native&&frame.status==='completed'&&!isObject(frame.failure)
   ?completedBusinessEnvelope(supplied):undefined;
 const rawResult=completedInner?completedInner.data:supplied;
 const result=isObject(rawResult)?rawResult:undefined;
 const failure=isObject(frame.failure)?frame.failure:undefined;
 const status=normalizedStatus(outer,frame,result);
 const job=jobState(result??{});
 const reason=stringField(failure??{},'code')??stringField(result??{},'reason_code')??
   stringField(result??{},'reason')??stringField(frame,'reason_code')??stringField(frame,'reason')??
   stringField(outer,'reason_code')??stringField(outer,'reason');
 // Rejects carry null product data, never an entire AgentResponse nested inside
 // data. Completed research results preserve their bounded typed product fields.
 const data=native?(failure?null:rawResult??null):rawResult;
 const quality=(status==='partial'?'partial':result?.quality??completedInner?.quality??frame.quality??outer.quality??{state:'not_evaluated'}) as Json;
 const projected:Record<string,Json>={
   request_id:stringField(frame,'request_id')??stringField(outer,'request_id')??requestId(),
   status,as_of_ms:numericField(completedInner??{},'as_of_ms')??numericField(frame,'as_of_ms')??numericField(outer,'as_of_ms')??Date.now(),
   data:data as Json,quality,warnings:warnings(outer,...(frame===outer?[]:[frame]),...(completedInner?[completedInner]:[]))
 };
 if(['failed','rejected','unknown'].includes(status)){
   projected.reason_code=reason??(job==='cancelled'?'JOB_CANCELLED':'REQUEST_FAILED');
   projected.scope=(frame.scope??outer.scope??'request') as Json;
   projected.retryable=status==='unknown'?false:!!(frame.retryable??failure?.retryable??outer.retryable);
   projected.next_action=(frame.next_action??outer.next_action??
     (status==='unknown'?'inspect_exact_intent':'inspect_diagnostics')) as Json;
 }
 const jobId=stringField(result??{},'job_id')??(job?stringField(result??{},'id'):undefined)??stringField(frame,'job_id');
 if(jobId)projected.job_id=jobId;
 const cursor=stringField(result??{},'cursor')??stringField(frame,'cursor');if(cursor)projected.cursor=cursor;
 const artifact=stringField(result??{},'artifact_id')??stringField(result??{},'artifact')??stringField(frame,'artifact_id');
 if(artifact)projected.artifact_id=artifact;
 return projected;
}
type TraceSubject=
 | {kind:'research_job';job_id:string}
 | {kind:'trade_intent';intent_id:string}
 | {kind:'cancel_operation';operation_id:string}
 | {kind:'close_intent';intent_id:string}
 | {kind:'terminal_release';intent_id:string}
 | {kind:'protection_operation';operation_id:string};
type TraceReferenceKind='request_id'|'job_id'|'trade_intent'|'idempotency_key'|
 'terminal_release'|'protection_operation';
type TraceOwner='research_job_catalog'|'trading_engine';
type TraceState='OBSERVED'|'NOT_EVALUATED';
export type OperationTraceSummary={schema:'okx.operation-trace-summary/v1';owner:TraceOwner;
 reference_kind:TraceReferenceKind;diagnostic_request_id:string|null;
 origin_request_id:string|null;job_id:string|null;subject:TraceSubject|null;
 state:TraceState;observed_stage:string;first_unproven_fact:string;reason_code:string;
 quality:'partial';history:'UNKNOWN';read_only:true;next_action:string};
const subjectId=(id:unknown):id is string=>typeof id==='string'&&
 id.length>=1&&id.length<=128&&/^[A-Za-z0-9_-]+$/.test(id);
const intentStates=new Set(['PREPARED','RESERVED','POSTING','UNKNOWN','ACKNOWLEDGED',
 'PARTIALLY_FILLED','FILLED','CANCELLED','REJECTED']);
type ReasonCode=string&{readonly __reasonCode:unique symbol};
const validReason=(v:unknown):v is ReasonCode=>typeof v==='string'&&/^[A-Z0-9_]{1,96}$/.test(v);
const validStage=(v:unknown):v is string=>typeof v==='string'&&/^[A-Za-z0-9_]{1,64}$/.test(v);
const validNextAction=(v:unknown):v is string=>typeof v==='string'&&/^[a-z_.]{1,80}$/.test(v);
/** A typed subject is emitted only when the owner snapshot proves its exact ID.
 * A request ID is never itself an intent, and a protection alias is not the
 * protection operation's ID. Domain IDs never come from a guessed mapping.
 */
function traceSubject(view:'operation'|'research',reference:string|undefined,
 data:Record<string,unknown>|undefined,job:Record<string,unknown>|undefined,
 trusted:boolean,observed:boolean):TraceSubject|null{
 if(!trusted||!observed||!data)return null;
 if(view==='research')return typeof job?.id==='string'&&/^[a-f0-9]{64}$/.test(job.id)
   ?{kind:'research_job',job_id:job.id}:null;
 if(data.family==='native_intent'){
   const intent=isObject(data.intent)?data.intent:undefined;
   const resolved=reference?.startsWith('request:')?data.resolved_intent_reference:reference;
   const id=typeof resolved==='string'&&resolved.startsWith('intent:')?resolved.slice(7):null;
   return subjectId(id)&&intent?.intent_id===id?{kind:'trade_intent',intent_id:id}:null;
 }
 // New owner-specific finance operation identities are durable SQL subjects,
 // not aliases of the incoming MCP request or reconstructed venue outcomes.
 if(data.family==='cancel'||data.family==='close'||data.family==='replace'){
   if(data.family==='replace')return null; // Original reference binding is unverified.
   const prefix=`${data.family}:`;
   const key=reference?.startsWith(prefix)?reference.slice(prefix.length):null;
   const scoped=data.owner==='execution.unified_finance'&&
     typeof data.account==='string'&&data.account.length>0&&data.account.length<=256&&
     typeof data.installation_id==='string'&&data.installation_id.length>0&&
     data.reference_kind==='idempotency_key'&&data.read_only===true&&
     data.state==='OBSERVED'&&data.quality==='partial'&&data.history==='UNKNOWN'&&
     data.causal_history==='NOT_RECONSTRUCTED';
   if(!scoped||!subjectId(key)||key.length<8)return null;
   if(data.family==='cancel'){
     const operation=isObject(data.operation)?data.operation:undefined;
     return operation?.kind==='cancel'&&operation.operation_id===key
       ?{kind:'cancel_operation',operation_id:key}:null;
   }
   const intent=isObject(data.intent)?data.intent:undefined;
   return intent?.operation==='close'&&intent.idempotency_key===key&&subjectId(intent.intent_id)
     ?{kind:'close_intent',intent_id:intent.intent_id}:null;
 }
 if(data.family==='terminal_release'&&reference?.startsWith('terminal_release:')){
   const id=reference.slice('terminal_release:'.length);
   const operation=isObject(data.operation)?data.operation:undefined;
   const snapshot=operation&&isObject(operation.snapshot)?operation.snapshot:undefined;
   return subjectId(id)&&snapshot?.intent_id===id?{kind:'terminal_release',intent_id:id}:null;
 }
 // The remaining legacy projection is protection cleanup. Do not interpret
 // a new owner family (cancel/replace/close/strategy) as cleanup before its
 // exact identity and durable semantics have a dedicated contract.
 if(data.family!==undefined)return null;
 const operation=isObject(data.operation)?data.operation:undefined;
 const snapshot=operation&&isObject(operation.snapshot)?operation.snapshot:undefined;
 const exactReference=reference===snapshot?.operation_id||reference===snapshot?.intent_id||
   reference===snapshot?.client_order_id;
 return data.state==='OBSERVED'&&subjectId(snapshot?.operation_id)&&exactReference
   ?{kind:'protection_operation',operation_id:snapshot.operation_id}:null;
}
/** One read-only summary over owner evidence, never a reconstructed history.
 * Foreign/failed envelopes must not be promoted to an observed operation.
 */
export function operationTrace(view:'operation'|'research',reference:string|undefined,
 projected:Record<string,Json>):OperationTraceSummary{
 const owner=view==='research'?'research_job_catalog':'trading_engine';
 const data=isObject(projected.data)?projected.data:undefined;
 const job=data&&isObject(data.job)?data.job:undefined;
 const researchRequest=view==='research'&&reference?.startsWith('request:')?reference.slice(8):undefined;
 // A retry legitimately has a different request ID from the original job.
 // Only the Research owner may attest that its bounded lookup matched the
 // exact queried ID. Older Research owners remain valid for the *original*
 // invocation alone; a present but mismatched attestation fails closed.
 const matchedResearchRequest=researchRequest!==undefined&&
   /^[A-Za-z0-9_.:-]{1,128}$/.test(researchRequest)&&
   data?.reference_kind==='request_id'&&data.matched_request_id===researchRequest;
 const originalResearchRequest=researchRequest!==undefined&&
   data?.matched_request_id===undefined&&job?.origin_invocation===researchRequest;
 const bound=view==='research'?data?.state!=='OBSERVED'||
   (job&&typeof job.id==='string'&&job.id.length===64&&/^[a-f0-9]+$/.test(job.id)&&
    (researchRequest!==undefined?(matchedResearchRequest||originalResearchRequest):job.id===reference)):
   !!data&&data.reference===reference&&(!reference?.startsWith('request:')||
    (data.origin_request_id===reference.slice(8)&&
     (data.state==='NOT_OBSERVED'||
      (typeof data.resolved_intent_reference==='string'&&
       /^intent:[a-zA-Z0-9_-]{1,128}$/.test(data.resolved_intent_reference)))));
 const trusted=!['unknown','failed','rejected'].includes(String(projected.status))&&
   data?.schema===(view==='research'
   ?'okx.research-job-diagnostics/v1':'okx.operation-diagnostics/v1')&&!!bound;
 const engineObserved=data?.family==='native_intent'?typeof data.state==='string'&&
   intentStates.has(data.state.toUpperCase()):data?.state==='OBSERVED';
 // Every OBSERVED summary must have a concrete owner-attested domain subject.
 // Mismatched nested IDs (or an absent durable row) are not enough evidence.
 const candidateObserved=!!trusted&&(view==='research'?data?.state==='OBSERVED':engineObserved);
 const subject=traceSubject(view,reference,data,job,!!trusted,!!candidateObserved);
 const observed=candidateObserved&&subject!==null;
 const reason=validReason(projected.reason_code)
   ?projected.reason_code:'OWNER_EVIDENCE_UNAVAILABLE';
 const nativeReason=validReason(data?.reason_code)
   ?data.reason_code:'NOT_RECONSTRUCTED';
 let firstMissing='CORRELATED_OWNER_DIAGNOSTIC';
 let nextAction=view==='research'?'retry_read_only_diagnostic':'trade.status';
 let stage='NOT_EVALUATED';
 let ownerReason=trusted?nativeReason:reason;
 if(trusted&&data){
   if(view==='research'){
     const jobStage=job?jobState(job):undefined;
     stage=observed?(jobStage??'JOB_SNAPSHOT'):'NOT_EVALUATED';
     firstMissing=observed?(jobStage==='queued'||jobStage==='running'
       ?'TERMINAL_RESEARCH_OUTCOME':'FULL_CAUSAL_HISTORY'):'RETAINED_JOB_SNAPSHOT';
     nextAction=observed?(jobStage==='completed'?'research.result':
       jobStage==='queued'||jobStage==='running'?'research.status':
       'inspect_research_job_snapshot'):
       data.reason_code==='AMBIGUOUS_REQUEST_ORIGIN'?'supply_exact_job_id':'retry_read_only_diagnostic';
   }else{
     const operation=isObject(data.operation)?data.operation:undefined;
     const snapshot=operation&&isObject(operation.snapshot)?operation.snapshot:undefined;
     stage=validStage(data.observed_stage)?data.observed_stage:
       validStage(data.stage)?data.stage:validStage(snapshot?.stage)?snapshot.stage:
       observed?'OPERATION_SNAPSHOT':'NOT_EVALUATED';
     const missing=Array.isArray(data.missing_evidence)?data.missing_evidence:[];
     const required=validReason(operation?.required_evidence)?operation.required_evidence:
       typeof operation?.required_evidence==='string'&&
       /^[a-z_]{1,96}$/.test(operation.required_evidence)?operation.required_evidence.toUpperCase():undefined;
     firstMissing=validReason(data.first_unproven_fact)?data.first_unproven_fact:
       validReason(missing[0])?missing[0]:
       validReason(snapshot?.first_unproven_boundary)?snapshot.first_unproven_boundary:
       required??(observed?'FULL_CAUSAL_HISTORY':'ACCOUNT_OPERATION_SNAPSHOT');
     nextAction=validNextAction(data.next_action)?data.next_action:
       validNextAction(operation?.next_action)?operation.next_action:
       validNextAction(snapshot?.next_action)?snapshot.next_action:'trade.status';
     if(!validReason(data.reason_code)&&validReason(snapshot?.reason_code))
       ownerReason=snapshot.reason_code;
   }
 }
 // A schema-less preflight refusal is NOT operation evidence. However, the
 // installed Engine can explicitly reject an explain request without an
 // account before it reaches account-owned diagnostics. Preserve that denial
 // and the missing input without making up a financial state or intent.
 const accountRefusal=!trusted&&view==='operation'&&data?.scope==='engine'&&
   data.stage==='operation'&&data.quality==='partial'&&data.retryable===false&&
   data.reason_code==='ACCOUNT_REQUIRED'&&Array.isArray(data.blockers)&&
   data.blockers.length===1&&data.blockers[0]==='ACCOUNT_REQUIRED';
 if(accountRefusal){
   ownerReason='ACCOUNT_REQUIRED';
   firstMissing='AUTHORIZED_ACCOUNT_AND_OPERATION_REFERENCE';
   nextAction='supply_authorized_account_and_operation_reference';
 }
 return {schema:'okx.operation-trace-summary/v1',owner,
   reference_kind:view==='research'?(reference?.startsWith('request:')?'request_id':'job_id'):
     reference?.startsWith('request:')?'request_id':reference?.startsWith('intent:')?'trade_intent':
     reference?.startsWith('cancel:')||reference?.startsWith('close:')||reference?.startsWith('replace:')
       ?'idempotency_key':reference?.startsWith('terminal_release:')?'terminal_release':'protection_operation',
   diagnostic_request_id:typeof projected.request_id==='string'?projected.request_id:null,
   origin_request_id:trusted&&observed&&view==='research'&&job&&
     typeof job.origin_invocation==='string'?job.origin_invocation:
     trusted&&observed&&view==='operation'&&reference?.startsWith('request:')?reference.slice(8):null,
   job_id:observed&&view==='research'&&job&&typeof job.id==='string'?job.id:null,
   subject,
   state:observed?'OBSERVED':'NOT_EVALUATED',observed_stage:stage,
   first_unproven_fact:firstMissing,reason_code:ownerReason,
   quality:'partial',history:'UNKNOWN',read_only:true,
   next_action:nextAction};
}
function toolResult(value:Json,resultBudgetBytes=NORMAL_RESULT_BYTES,
 trace?:{view:'operation'|'research';reference?:string}):Json{
 const projected=projectNativeResponse(value);
 if(trace)projected.operation_trace=operationTrace(trace.view,trace.reference,projected);
 if(bytes(projected)<=resultBudgetBytes)return baseToolResult(projected);
 const status=projected.status==='unknown'?'unknown':'failed';
 return baseToolResult({request_id:projected.request_id,status,as_of_ms:Date.now(),data:{response_bytes:bytes(projected),response_budget_bytes:resultBudgetBytes},quality:{state:'partial'},warnings:['RESPONSE_BUDGET_EXCEEDED'],reason_code:'RESPONSE_TOO_LARGE',scope:'response',retryable:false,next_action:status==='unknown'?'inspect_exact_intent':'inspect_diagnostics'});
}
export const mcpApi={async fetch(request:Request,env:Env,ctx:any):Promise<Response>{
 if(!ctx?.auth?.scope?.includes('mcp:use'))return new Response('insufficient scope',{status:403});
 if(request.method!=='POST')return new Response('method not allowed',{status:405});
 const raw=await request.text();if(new TextEncoder().encode(raw).byteLength>MAX_BODY_BYTES)return new Response('request too large',{status:413});
 let rpc:any;try{rpc=JSON.parse(raw);}catch{return jsonRpcError(null,-32700,'parse error');}
 const id=(rpc.id??null) as Json;
 if(rpc.method==='notifications/initialized')return new Response(null,{status:202});
 if(rpc.method==='initialize')return jsonRpc(id,{protocolVersion:MCP_PROTOCOL_VERSION,capabilities:{tools:{}},serverInfo:{name:'okx-cloudflare-mcp',version:MCP_SERVER_VERSION}});
 if(rpc.method==='ping')return jsonRpc(id,{});
 if (rpc.method === "tools/list") {return jsonRpc(id,{tools:PUBLIC_TOOLS});}
 if (rpc.method !== "tools/call" || !isObject(rpc.params))return jsonRpcError(id,-32601,'method not found');
 const name=rpc.params.name,args=rpc.params.arguments;
 // Validate the actual public descriptor first. Retired generic "trade"
 // is not a hidden mutation route; trade.execute remains explicitly high-risk.
 const published=typeof name==='string'&&PUBLIC_TOOLS.some(tool=>(tool as any).name===name);
 const scopedAction=typeof name==='string'&&name.startsWith('trade.')?name.slice(6):null;
 const nativeName=scopedAction===null?name:'trade';
 const nativeArgs=scopedAction===null?args:{...args,action:scopedAction};
 if(name==='strategies'&&isObject(args)&&args.action==='start'&&
    (!('expected_revision' in args)||!('configuration_digest' in args)))
   return jsonRpcError(id,-32602,'strategy start requires expected_revision and configuration_digest from status');
 if(!published||!isObject(args)||(scopedAction!==null&&'action' in args)||
    !validTool(nativeName,nativeArgs))return jsonRpcError(id,-32602,'invalid tool action payload');
 const edgeRequestId=requestId();
 try{
      if (name === "accounts") {
        if((args.action==="list"||args.action==="status")&&hasOnlyKeys(args,["action","account"])){
          const state=await runtimeFetch(env,"/account-governance");
          if(args.action==="status"&&isObject(state)&&Array.isArray(state.accounts)){
            const account=state.accounts.find((a:any)=>a.id===args.account||a.account_key===args.account);
            return jsonRpc(id,toolResult(account?{account,readiness:state.readiness as Json,last_admission:state.last_admission as Json}
              :{status:"FAILED",reason:"ACCOUNT_NOT_FOUND"}));
          }
          return jsonRpc(id,toolResult(state));
        }
        if((args.action==="pause"||args.action==="resume")&&hasOnlyKeys(args,["action","account","expected_revision","idempotency_key"])){
          return jsonRpc(id,toolResult(await runtimeFetch(env,"/account-governance/pause",{method:"POST",
            headers:{"content-type":"application/json"},body:JSON.stringify({account:args.account,
              expected_revision:args.expected_revision,idempotency_key:args.idempotency_key,explicit_pause:args.action==="pause"})})));
        }
        if(args.action==="connect"&&hasOnlyKeys(args,["action","alias","label","limits","enabled","credential_ref","idempotency_key"])){
          if(typeof args.alias!=="string"||typeof args.credential_ref!=="string"||typeof args.idempotency_key!=="string"
            ||typeof args.enabled!=="boolean"||!isObject(args.limits))return jsonRpcError(id,-32602,"invalid account connection");
          const request_digest=await sha256Hex(canonical(args));
          const snapshot=await dispatchCorrelated(env,{schema:"okx.agent.request/v1",request_id:edgeRequestId,
            operation:{type:"connect_account",credential_ref:args.credential_ref,alias:args.alias,operation_id:args.idempotency_key,request_digest}});
          if(!isObject(snapshot)||snapshot.status!=="completed"||!isObject(snapshot.result)||!isObject(snapshot.result.account_key)
            ||snapshot.result.configuration_digest!==request_digest||snapshot.result.alias!==args.alias||snapshot.result.credential_ref!==args.credential_ref
            ||snapshot.result.connect_operation_id!==args.idempotency_key||snapshot.result.account_key.venue!=="okx"
            ||!["real","demo"].includes(String(snapshot.result.account_key.environment))
            ||typeof snapshot.result.account_key.verified_uid!=="string"||!/^[0-9a-f]{64}$/.test(snapshot.result.account_key.verified_uid))
            return jsonRpc(id,toolResult(snapshot));
          const enrollment={venue:"okx",environment:snapshot.result.account_key.environment,
            uid_fingerprint:snapshot.result.account_key.verified_uid,alias:args.alias,label:args.label??args.alias,
            credential_ref:args.credential_ref,limits:args.limits,enabled:args.enabled,idempotency_key:args.idempotency_key};
          const enroll=()=>runtimeFetch(env,"/account-governance/enroll",{method:"POST",
            headers:{"content-type":"application/json"},body:JSON.stringify(enrollment)});
          // One owner, one account. A clean local Windows catalog may be
          // attached to the verified account already held by this same DO.
          // Never reenroll the same venue identity or reset its RiskBook.
          const central=await runtimeFetch(env,"/account-governance");
          if(!isObject(central)||!Array.isArray(central.accounts)||!Number.isSafeInteger(central.revision))
            return jsonRpc(id,toolResult({status:"failed",reason_code:"CENTRAL_GOVERNANCE_UNAVAILABLE",retryable:false}));
          const observedKey="okx:"+snapshot.result.account_key.environment+":"+snapshot.result.account_key.verified_uid;
          const existing=central.accounts.filter((row:unknown)=>isObject(row)&&
            (row.id===args.alias||row.account_key===observedKey));
          if(existing.length){
            const old=existing[0];
            if(existing.length!==1||!isObject(old)||old.id!==args.alias||old.account_key!==observedKey||
              old.uid_fingerprint!==snapshot.result.account_key.verified_uid||old.venue!=="okx"||
              old.environment!==snapshot.result.account_key.environment||old.identity_verified!==true||
              typeof old.enabled!=="boolean"||typeof old.explicit_pause!=="boolean")
              return jsonRpc(id,toolResult({status:"failed",reason_code:"EXISTING_ACCOUNT_IDENTITY_MISMATCH",retryable:false}));
            // Reattachment never enables real trading. Existing limits,
            // strategies and financial data remain under the same DO.
            // The exact original enabled enrollment may be replayed through
            // the DO's existing body-bound durable receipt. A different key
            // still rejects ACCOUNT_ALREADY_CONNECTED; no new risk grant.
            if(args.enabled!==false)
              return jsonRpc(id,toolResult(await enroll()));
            let stopped:Record<string,unknown>=central;
            if(old.enabled===true&&old.explicit_pause!==true){
              const pause=await runtimeFetch(env,"/account-governance/pause",{method:"POST",
                headers:{"content-type":"application/json"},body:JSON.stringify({
                  account:args.alias,expected_revision:central.revision,
                  explicit_pause:true,idempotency_key:args.idempotency_key,
                })});
              if(!isObject(pause)||!isObject(pause.configuration))
                return jsonRpc(id,toolResult(pause));
              stopped=pause.configuration;
            }
            const retained=Array.isArray(stopped.accounts)?stopped.accounts.filter((row:unknown)=>isObject(row)&&
              (row.id===args.alias||row.account_key===observedKey)):[];
            const attached=retained[0];
            if(!Number.isSafeInteger(stopped.revision)||retained.length!==1||
              !isObject(attached)||attached.id!==args.alias||attached.account_key!==observedKey||
              attached.uid_fingerprint!==snapshot.result.account_key.verified_uid||attached.identity_verified!==true||
              !(attached.enabled===false||attached.explicit_pause===true)||
              !Array.isArray(stopped.accounts)||stopped.accounts.length!==central.accounts.length)
              return jsonRpc(id,toolResult({status:"failed",reason_code:"EXISTING_ACCOUNT_STOP_UNVERIFIED",retryable:false}));
            return jsonRpc(id,toolResult({status:"CONNECTED",account:attached as Json,configuration:stopped as Json,
              readiness:{state:"not_evaluated"},reattached_existing:true}));
          }
          return jsonRpc(id,toolResult(await enroll()));
        }
        if(args.action==="set_limits"){
          return jsonRpc(id,toolResult(await runtimeFetch(env,"/account-governance/limits",{method:"POST",headers:{"content-type":"application/json"},body:JSON.stringify(args)})));
        }
        return jsonRpcError(id,-32602,"invalid accounts request");
      }

      if(name==='diagnostics'&&args.action==='runtime'){
        // The Durable Object owns the authenticated transport session. Query
        // it directly; never synthesize readiness from a cached Agent reply.
        const observed=await runtimeFetch(env,'/status?probe=1');
        if(!isObject(observed)||observed.schema!=='okx.direct-transport.status/v1'||
          typeof observed.connected!=='boolean'||typeof observed.session_fresh!=='boolean'||
          !['PASS','STALE','OFFLINE'].includes(String(observed.status))){
          return jsonRpc(id,toolResult({status:'failed',data:null,quality:'not_ready',
            reason_code:'INVALID_RUNTIME_STATUS',scope:'transport',retryable:false,
            next_action:'inspect_diagnostics'}));
        }
        const fresh=observed.status==='PASS'&&observed.connected===true&&observed.session_fresh===true;
        // CONTROL verifies the installed file locally. The Worker only proves
        // authenticated WSS freshness and reports Agent identity as untrusted claims.
        return jsonRpc(id,toolResult({status:fresh?'completed':'partial',
          data:{schema:'okx.runtime-status/v1',
            worker_version:MCP_SERVER_VERSION,tool_contract:TOOL_CONTRACT_VERSION,
            release_source_tree:workerReleaseSourceTree(env),
            transport:{schema:'okx.direct-transport.status/v1',status:stringField(observed,'status')??'OFFLINE',connected:observed.connected,
              session_fresh:observed.session_fresh,runtime_id:stringField(observed,'runtime_id')??null,
              runtime_profile:stringField(observed,'runtime_profile')??null,
              connection_generation:numericField(observed,'connection_generation')??null,
              inflight:numericField(observed,'inflight')??null,
              readonly_inflight:numericField(observed,'readonly_inflight')??null,
              normal_inflight:numericField(observed,'normal_inflight')??null,
              readonly_queue_depth:numericField(observed,'readonly_queue_depth')??null,
              readonly_queue_capacity:numericField(observed,'readonly_queue_capacity')??null,
              agent_reported_pid:numericField(observed,'agent_pid')??null,
              agent_reported_binary_sha256:stringField(observed,'agent_binary_sha256')??null,
              agent_reported_incarnation:stringField(observed,'agent_incarnation')??null,
              agent_reported_installation_id:stringField(observed,'installation_id')??null},
            installation_verification:{owner:'WINDOWS_CONTROL',worker_independent_proof:'NOT_PERFORMED'},
            reason_code:fresh?null:'RUNTIME_SESSION_NOT_FRESH'},
          quality:fresh?'fresh':'partial',
          warnings:fresh?[]:['RUNTIME_SESSION_NOT_FRESH']}));
      }
 if(name==='diagnostics'&&args.action==='explain'&&args.view==='research'&&args.reference==='deployment'){
   if(args.account!==undefined||args.cycle_id!==undefined||args.limit!==undefined)
     return jsonRpc(id,toolResult(transportFailure('RESEARCH_OPERATION_FORBIDDEN',false)));
   const observed=await runtimeFetch(env,'/status?probe=1&owner=research');
   if(!isObject(observed)||observed.schema!=='okx.direct-transport.status/v1'||
      observed.owner!=='research'||(observed.connected===true&&observed.runtime_profile!=='research')||
      typeof observed.connected!=='boolean'||typeof observed.session_fresh!=='boolean'||
      !['PASS','STALE','OFFLINE'].includes(String(observed.status)))
     return jsonRpc(id,toolResult({status:'failed',data:null,quality:'not_ready',reason_code:'INVALID_RUNTIME_STATUS'}));
   const edgeFresh=observed.status==='PASS'&&observed.connected===true&&observed.session_fresh===true;
   // Optional CONTROL attestation is an independent, live read-only challenge
   // in the SAME Durable Object. With no dedicated credential configured this
   // remains the historical Edge-only partial response, byte-for-byte in its
   // status/verification semantics and without another DO request.
   const controlConfigured=typeof env.CONTROL_ATTEST_TOKEN_SHA256==='string'&&
     /^[0-9a-f]{64}$/.test(env.CONTROL_ATTEST_TOKEN_SHA256);
   let attestation:Record<string,unknown>|null=null;
   if(controlConfigured&&edgeFresh){
     try {
       const candidate=await runtimeFetch(env,'/attestation/research');
       if(isObject(candidate)&&candidate.schema==='okx.research.attestation/v1')attestation=candidate;
     }catch{
       attestation={reason_code:'CONTROL_ATTEST_UNAVAILABLE'};
     }
   }
   const bound=!!(attestation&&attestation.status==='PASS'&&
     attestation.pid===observed.agent_pid&&
     attestation.active_worker_sha256===observed.agent_binary_sha256&&
     attestation.installed_worker_sha256===observed.agent_binary_sha256&&
     attestation.research_connection_generation===observed.connection_generation&&
     Number.isSafeInteger(attestation.process_generation)&&Number(attestation.process_generation)>0&&
     Number.isSafeInteger(attestation.observed_at_ms));
   const controlReason=bound?null:!controlConfigured?'CONTROL_EVIDENCE_UNAVAILABLE':
     !edgeFresh?'RESEARCH_EDGE_SESSION_NOT_FRESH':
     typeof attestation?.reason_code==='string'&&/^[A-Z0-9_]{1,128}$/.test(attestation.reason_code)
       ?attestation.reason_code:'CONTROL_ATTEST_INVALID_RESPONSE';
   // These fields are authenticated Edge session observations. A PID and SHA
   // carried in a hello frame are claims from that process, not independent
   // installed-image evidence. Only Windows CONTROL can establish that proof.
   return jsonRpc(id,toolResult({status:bound?'completed':'partial',quality:bound?'fresh':'partial',
     warnings:bound?[]:[controlReason!],
     data:{schema:'okx.research.deployment-edge/v1',owner:'research',
       verification:{edge_authenticated_session:edgeFresh?'PASS':'UNVERIFIED',
         control_process_proof:bound?'PASS':controlConfigured?'UNVERIFIED':'NOT_PERFORMED',
         first_unproven_stage:bound?null:edgeFresh?'CONTROL_PID_SHA_GENERATION':'EDGE_AUTHENTICATED_SESSION',
         next_action:bound?null:edgeFresh?'inspect_control_research_status':'inspect_research_connection'},
       edge:{worker_version:MCP_SERVER_VERSION,
       release_source_tree:workerReleaseSourceTree(env),research_transport:{
         schema:'okx.direct-transport.status/v1',owner:'research',status:stringField(observed,'status')??'OFFLINE',
         connected:observed.connected,session_fresh:observed.session_fresh,
         connection_generation:numericField(observed,'connection_generation')??null,
         agent_reported_pid:numericField(observed,'agent_pid')??null,
         agent_reported_binary_sha256:stringField(observed,'agent_binary_sha256')??null,
         agent_reported_incarnation:stringField(observed,'agent_incarnation')??null}},
       control:{status:bound?'verified':'unavailable',
         evidence:bound?'CONTROL_RESEARCH_PROCESS_OBSERVATION':controlConfigured&&edgeFresh?'UNVERIFIED':'NOT_PERFORMED',
         reason_code:controlReason,
         pid:bound?numericField(attestation!,'pid')??null:null,
         active_worker_sha256:bound?stringField(attestation!,'active_worker_sha256')??null:null,
         installed_worker_sha256:bound?stringField(attestation!,'installed_worker_sha256')??null:null,
         process_generation:bound?numericField(attestation!,'process_generation')??null:null}}}));
 }
 if(name==='market'&&args.action==='instruments'){
   return jsonRpc(id,toolResult(await dispatchCorrelated(env,{schema:'okx.agent.request/v1',request_id:edgeRequestId,
     ...RESEARCH_OWNER,
     operation:{type:'find_instruments',asset:args.asset,settle_currency:args.settle_currency??null,instrument_type:null}})));
 }
 if(name==='market'&&args.action==='snapshot'){
   return jsonRpc(id,toolResult(await dispatchCorrelated(env,{schema:'okx.agent.request/v1',request_id:edgeRequestId,
     ...RESEARCH_OWNER,
     operation:{type:'market_overview',instrument:args.instrument}})));
 }
 if(name==='market'&&args.action==='microstructure'){
   return jsonRpc(id,toolResult(await dispatchCorrelated(env,{schema:'okx.agent.request/v1',request_id:edgeRequestId,
     ...RESEARCH_OWNER,
     operation:{type:'market_microstructure',instrument:args.instrument,
       size_contracts:args.size_contracts??null}})));
 }
 if(name==='market'&&args.action==='capabilities'){
   return jsonRpc(id,toolResult(await dispatchCorrelated(env,{schema:'okx.agent.request/v1',request_id:edgeRequestId,
     ...RESEARCH_OWNER,
     operation:{type:'market_capabilities'}}),MARKET_CAPABILITIES_RESULT_BYTES));
 }
 if(name==='portfolio'&&args.action==='scenario'){
   const {action:scopedAction,...scenario}=args;
   return jsonRpc(id,toolResult(await dispatchCorrelated(env,{schema:'okx.agent.request/v1',request_id:edgeRequestId,
      ...RESEARCH_OWNER,
      operation:{type:'portfolio_scenario',request:scenario}})));
 }
 const {account,...body}=nativeArgs;
 const payload=nativeName==='portfolio'?nativeArgs:nativeName==='diagnostics'?{reference:nativeArgs.reference??null,account:nativeArgs.account??null,view:nativeArgs.view??null,cycle_id:nativeArgs.cycle_id??null,limit:nativeArgs.limit??null}:body;
 const operation={type:nativeName,request:payload};
 const nativeRequestId=edgeRequestId;
 const researchRoute=nativeName==='research'||nativeName==='market'||
   (nativeName==='diagnostics'&&nativeArgs.view==='research')
   ?RESEARCH_OWNER:{};
 const reply=await dispatchCorrelated(env,{schema:'okx.agent.request/v1',request_id:nativeRequestId,
   ...researchRoute,...(typeof account==='string'?{account}:{}),operation});
 return jsonRpc(id,toolResult(reply,NORMAL_RESULT_BYTES,
   nativeName==='diagnostics'&&nativeArgs.action==='explain'&&
   (nativeArgs.view==='research'||nativeArgs.view==='operation')
     ?{view:nativeArgs.view,reference:nativeArgs.reference}:undefined));
 }catch{
   const research=nativeName==='research'||nativeName==='market'||
     (nativeName==='diagnostics'&&nativeArgs.view==='research')||
     (nativeName==='portfolio'&&nativeArgs.action==='scenario');
   const mutation=isExecutionMutation(nativeName,nativeArgs.action)||
     (nativeName==='accounts'&&!['list','status'].includes(String(nativeArgs.action)))||
     (nativeName==='market'&&nativeArgs.action==='screen');
   return jsonRpc(id,toolResult({schema:'okx.direct-transport.result/v1',request_id:edgeRequestId,
     status:mutation?'UNKNOWN':'FAILED',data:null,
     reason:mutation?(research?'RESEARCH_MUTATION_OUTCOME_UNKNOWN_CHECK_JOB':
       'MUTATION_OUTCOME_UNKNOWN_RECONCILE_EXACT_INTENT'):'EDGE_DISPATCH_UNAVAILABLE',
     retryable:!mutation,scope:research?'research':'engine',
     next_action:mutation?(research?'inspect_research_job_or_idempotency_receipt':'inspect_exact_intent'):
       'inspect_diagnostics'}));
 }
}};
