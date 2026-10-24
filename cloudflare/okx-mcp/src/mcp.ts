import {PUBLIC_TOOLS,validTool} from './execution_tools.js';
import {dispatchRuntime,type Env,isObject,type Json,jsonRpc,jsonRpcError,MAX_BODY_BYTES,MCP_PROTOCOL_VERSION,MCP_SERVER_VERSION,TOOL_CONTRACT_VERSION,requestId,runtimeFetch,toolResult as baseToolResult,sha256Hex,transportFailure} from './shared.js';
const NORMAL_RESULT_BYTES=12_288;
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
function toolResult(value:Json):Json{
 const projected=projectNativeResponse(value);
 if(bytes(projected)<=NORMAL_RESULT_BYTES)return baseToolResult(projected);
 const status=projected.status==='unknown'?'unknown':'failed';
 return baseToolResult({request_id:projected.request_id,status,as_of_ms:Date.now(),data:{response_bytes:bytes(projected),response_budget_bytes:NORMAL_RESULT_BYTES},quality:{state:'partial'},warnings:['RESPONSE_BUDGET_EXCEEDED'],reason_code:'RESPONSE_TOO_LARGE',scope:'response',retryable:false,next_action:status==='unknown'?'inspect_exact_intent':'inspect_diagnostics'});
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
 if(typeof name!=='string'||!validTool(name,args)||!isObject(args))return jsonRpcError(id,-32602,'invalid tool action payload');
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
          const snapshot=await dispatchRuntime(env,{schema:"okx.agent.request/v1",request_id:requestId(),
            operation:{type:"connect_account",credential_ref:args.credential_ref,alias:args.alias,operation_id:args.idempotency_key,request_digest}});
          if(!isObject(snapshot)||snapshot.status!=="completed"||!isObject(snapshot.result)||!isObject(snapshot.result.account_key)
            ||snapshot.result.configuration_digest!==request_digest||snapshot.result.alias!==args.alias||snapshot.result.credential_ref!==args.credential_ref
            ||snapshot.result.connect_operation_id!==args.idempotency_key||snapshot.result.account_key.venue!=="okx"
            ||!["real","demo"].includes(String(snapshot.result.account_key.environment))
            ||typeof snapshot.result.account_key.verified_uid!=="string"||!/^[0-9a-f]{64}$/.test(snapshot.result.account_key.verified_uid))
            return jsonRpc(id,toolResult(snapshot));
          return jsonRpc(id,toolResult(await runtimeFetch(env,"/account-governance/enroll",{method:"POST",headers:{"content-type":"application/json"},
            body:JSON.stringify({venue:"okx",environment:snapshot.result.account_key.environment,
              uid_fingerprint:snapshot.result.account_key.verified_uid,alias:args.alias,label:args.label??args.alias,
              credential_ref:args.credential_ref,limits:args.limits,enabled:args.enabled,idempotency_key:args.idempotency_key})})));
        }
        if(args.action==="set_limits"){
          return jsonRpc(id,toolResult(await runtimeFetch(env,"/account-governance/limits",{method:"POST",headers:{"content-type":"application/json"},body:JSON.stringify(args)})));
        }
        return jsonRpcError(id,-32602,"invalid accounts request");
      }

      if(name==='diagnostics'&&args.action==='runtime'){
        // The Durable Object owns the authenticated transport session. Query
        // it directly; never synthesize readiness from a cached Agent reply.
        const observed=await runtimeFetch(env,'/status?probe=1&attest=1');
        if(!isObject(observed)||observed.schema!=='okx.direct-transport.status/v1'||
          typeof observed.connected!=='boolean'||typeof observed.session_fresh!=='boolean'||
          !['PASS','STALE','OFFLINE'].includes(String(observed.status))){
          return jsonRpc(id,toolResult({status:'failed',data:null,quality:'not_ready',
            reason_code:'INVALID_RUNTIME_STATUS',scope:'transport',retryable:false,
            next_action:'inspect_diagnostics'}));
        }
        const fresh=observed.status==='PASS'&&observed.connected===true&&observed.session_fresh===true;
        // Independent CONTROL evidence is owned and bound to the live session by
        // the existing Durable Object, not synthesized by a Worker reply.
        const proof=observed.release_attestation;
        const validProof=isObject(proof)&&(
          (proof.verified===true&&proof.reason_code===null&&
           typeof proof.source_tree==='string'&&typeof proof.agent_binary_sha256==='string'&&
           typeof proof.artifact_id==='number'&&typeof proof.control_receipt_url==='string')||
          (proof.verified===false&&typeof proof.reason_code==='string'));
        // Whitelist typed JSON scalars; never expose an arbitrary DO object.
        const p=isObject(proof)?proof:{};
        const releaseAttestation=validProof?{
          verified:p.verified===true,
          reason_code:typeof p.reason_code==='string'?p.reason_code:null,
          source_tree:typeof p.source_tree==='string'?p.source_tree:null,
          agent_binary_sha256:typeof p.agent_binary_sha256==='string'?p.agent_binary_sha256:null,
          artifact_id:typeof p.artifact_id==='number'&&Number.isSafeInteger(p.artifact_id)?p.artifact_id:null,
          control_observed_at:typeof p.control_observed_at==='string'?p.control_observed_at:null,
          control_receipt_url:typeof p.control_receipt_url==='string'?p.control_receipt_url:null,
        }:{verified:false,reason_code:'RELEASE_ATTESTATION_UNAVAILABLE',
           source_tree:null,agent_binary_sha256:null,artifact_id:null,
           control_observed_at:null,control_receipt_url:null};
        return jsonRpc(id,toolResult({status:fresh?'completed':'partial',
          data:{schema:'okx.runtime-status/v1',
            worker_version:MCP_SERVER_VERSION,tool_contract:TOOL_CONTRACT_VERSION,
            transport:{schema:'okx.direct-transport.status/v1',status:stringField(observed,'status')??'OFFLINE',connected:observed.connected,
              session_fresh:observed.session_fresh,runtime_id:stringField(observed,'runtime_id')??null,
              runtime_profile:stringField(observed,'runtime_profile')??null,
              connection_generation:numericField(observed,'connection_generation')??null},
            reason_code:fresh?null:'RUNTIME_SESSION_NOT_FRESH',
            release_attestation:releaseAttestation},
          quality:fresh?'fresh':'partial',
          warnings:fresh?[]:['RUNTIME_SESSION_NOT_FRESH']}));
      }
 if(name==='market'&&args.action==='instruments'){
   return jsonRpc(id,toolResult(await dispatchRuntime(env,{schema:'okx.agent.request/v1',request_id:requestId(),
     operation:{type:'find_instruments',asset:args.asset,settle_currency:args.settle_currency??null,instrument_type:null}})));
 }
 if(name==='market'&&args.action==='snapshot'){
   return jsonRpc(id,toolResult(await dispatchRuntime(env,{schema:'okx.agent.request/v1',request_id:requestId(),
     operation:{type:'market_overview',instrument:args.instrument}})));
 }
 if(name==='market'&&args.action==='microstructure'){
   return jsonRpc(id,toolResult(await dispatchRuntime(env,{schema:'okx.agent.request/v1',request_id:requestId(),
     operation:{type:'market_microstructure',instrument:args.instrument,
       size_contracts:args.size_contracts??null}})));
 }
 if(name==='market'&&args.action==='capabilities'){
   return jsonRpc(id,toolResult(await dispatchRuntime(env,{schema:'okx.agent.request/v1',request_id:requestId(),
     operation:{type:'market_capabilities'}})));
 }
 if(name==='portfolio'&&args.action==='scenario'){
   const {action:scopedAction,...scenario}=args;
   return jsonRpc(id,toolResult(await dispatchRuntime(env,{schema:'okx.agent.request/v1',request_id:requestId(),
      operation:{type:'portfolio_scenario',request:scenario}})));
 }
 const {account,...body}=args;
 const payload=name==='portfolio'?args:name==='diagnostics'?{reference:args.reference??null,account:args.account??null}:body;
 const operation={type:name,request:payload};
 return jsonRpc(id,toolResult(await dispatchRuntime(env,{schema:'okx.agent.request/v1',request_id:requestId(),...(typeof account==='string'?{account}:{}),operation})));
 }catch(error){return jsonRpc(id,toolResult(transportFailure(error instanceof Error?error.message:'transport failure')));}
}};
