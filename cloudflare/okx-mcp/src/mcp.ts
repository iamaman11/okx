import {PUBLIC_TOOLS,validTool} from './execution_tools.js';
import {dispatchRuntime,type Env,isObject,type Json,jsonRpc,jsonRpcError,MAX_BODY_BYTES,MCP_PROTOCOL_VERSION,MCP_SERVER_VERSION,requestId,runtimeFetch,toolResult as baseToolResult,sha256Hex,transportFailure} from './shared.js';
const NORMAL_RESULT_BYTES=12_288;
function hasOnlyKeys(v:Record<string,unknown>,keys:string[]){return Object.keys(v).every(k=>keys.includes(k));}
function canonical(v:unknown):string{if(Array.isArray(v))return '['+v.map(canonical).join(',')+']';if(isObject(v))return '{'+Object.keys(v).sort().map(k=>JSON.stringify(k)+':'+canonical(v[k])).join(',')+'}';return JSON.stringify(v);}
function bytes(value:Json):number{return new TextEncoder().encode(JSON.stringify(value)).byteLength;}
function stringField(value:Record<string,unknown>,field:string):string|undefined{return typeof value[field]==='string'?value[field]:undefined;}
function numericField(value:Record<string,unknown>,field:string):number|undefined{return typeof value[field]==='number'&&Number.isFinite(value[field])?value[field]:undefined;}
function warnings(...sources:Record<string,unknown>[]):string[]{return sources.flatMap(source=>Array.isArray(source.warnings)?source.warnings.filter((v):v is string=>typeof v==='string'):[]);}
function partial(value:unknown):boolean{if(typeof value==='string')return ['partial','missing','stale','degraded'].includes(value.toLowerCase());return isObject(value)&&(partial(value.state)||partial(value.status));}
function jobState(value:Record<string,unknown>):string|undefined{const state=stringField(value,'state')?.toLowerCase();return state&&['queued','running','completed','failed','cancelled','partial'].includes(state)?state:undefined;}
function projectedStatus(outer:Record<string,unknown>,payload:Record<string,unknown>|undefined):string{
 const outerStatus=stringField(outer,'status')?.toLowerCase(),nestedStatus=payload&&stringField(payload,'status')?.toLowerCase();
 const reason=[stringField(outer,'reason_code'),stringField(outer,'reason'),payload&&stringField(payload,'reason_code'),payload&&stringField(payload,'reason')].filter((v):v is string=>!!v).join(' ');
 if(reason.toUpperCase().includes('UNKNOWN')||nestedStatus==='unknown')return 'unknown';
 if(outerStatus==='failed'||outerStatus==='rejected')return outerStatus;
 if(nestedStatus==='failed'||nestedStatus==='rejected')return nestedStatus;
 const state=payload&&jobState(payload);
 if(state==='queued'||state==='running')return 'accepted';
 if(state==='failed'||state==='cancelled')return 'failed';
 if(nestedStatus==='accepted')return nestedStatus;
 if(partial(payload?.quality)||partial(outer.quality)||state==='partial')return 'partial';
 if(nestedStatus==='partial'||nestedStatus==='completed')return nestedStatus;
 if(['configured','connected','observed','updated'].includes(nestedStatus??outerStatus??''))return 'completed';
 return outerStatus==='completed'||!outerStatus?'completed':'failed';
}
/** Project a typed native envelope without turning nested uncertainty into success. */
export function projectNativeResponse(value:Json):Record<string,Json>{
 const outer=isObject(value)?value:{data:value},payload=isObject(outer.result)?outer.result:isObject(outer.data)?outer.data:undefined,failure=isObject(outer.failure)?outer.failure:undefined;
 const status=projectedStatus(outer,payload),state=payload&&jobState(payload);
 const reason=stringField(payload??{},'reason_code')??stringField(payload??{},'reason')??stringField(failure??{},'code')??stringField(outer,'reason_code')??stringField(outer,'reason');
 const data=isObject(outer.result)?outer.result:('data' in outer?outer.data:failure?null:value);
 const result:Record<string,Json>={request_id:stringField(outer,'request_id')??stringField(payload??{},'request_id')??requestId(),status,as_of_ms:numericField(payload??{},'as_of_ms')??numericField(outer,'as_of_ms')??Date.now(),data:data as Json,quality:(payload?.quality??outer.quality??{state:'not_evaluated'}) as Json,warnings:warnings(outer,payload??{})};
 if(['failed','rejected','unknown'].includes(status)){result.reason_code=reason??(state==='cancelled'?'JOB_CANCELLED':'REQUEST_FAILED');result.scope=(payload?.scope??outer.scope??'request') as Json;result.retryable=status==='unknown'?false:!!(payload?.retryable??outer.retryable??failure?.retryable);result.next_action=(payload?.next_action??outer.next_action??(status==='unknown'?'inspect_exact_intent':'inspect_diagnostics')) as Json;}
 const jobId=stringField(payload??{},'job_id')??stringField(outer,'job_id')??(state?stringField(payload??{},'id'):undefined);if(jobId)result.job_id=jobId;
 const cursor=stringField(payload??{},'cursor')??stringField(outer,'cursor');if(cursor)result.cursor=cursor;
 const artifact=stringField(payload??{},'artifact_id')??stringField(payload??{},'artifact')??stringField(outer,'artifact_id');if(artifact)result.artifact_id=artifact;
 return result;
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

 const {account,...body}=args;
 const payload=name==='portfolio'?args:name==='diagnostics'?{reference:args.reference??null,account:args.account??null}:body;
 const operation={type:name,request:payload};
 return jsonRpc(id,toolResult(await dispatchRuntime(env,{schema:'okx.agent.request/v1',request_id:requestId(),...(typeof account==='string'?{account}:{}),operation})));
 }catch(error){return jsonRpc(id,toolResult(transportFailure(error instanceof Error?error.message:'transport failure')));}
}};
