import {PUBLIC_TOOLS,validTool} from './execution_tools.js';
import {dispatchRuntime,type Env,isObject,type Json,jsonRpc,jsonRpcError,MAX_BODY_BYTES,MCP_PROTOCOL_VERSION,MCP_SERVER_VERSION,requestId,runtimeFetch,toolResult as baseToolResult,sha256Hex,transportFailure} from './shared.js';
function hasOnlyKeys(v:Record<string,unknown>,keys:string[]){return Object.keys(v).every(k=>keys.includes(k));}
function canonical(v:unknown):string{if(Array.isArray(v))return '['+v.map(canonical).join(',')+']';if(isObject(v))return '{'+Object.keys(v).sort().map(k=>JSON.stringify(k)+':'+canonical(v[k])).join(',')+'}';return JSON.stringify(v);}
function toolResult(value:Json):Json{
 const outer=isObject(value)?value:{data:value};const v=isObject(outer.result)&&typeof outer.result.status==='string'?{...outer.result,request_id:outer.request_id}:outer;const raw=String(v.status??'completed').toLowerCase();
 const reason=String(v.reason_code??v.reason??'');
 const status=reason.includes('UNKNOWN')?'unknown':['completed','accepted','partial','rejected','unknown','failed'].includes(raw)?raw:['configured','connected','observed','updated'].includes(raw)?'completed':'failed';
 return baseToolResult({request_id:typeof v.request_id==='string'?v.request_id:requestId(),status,as_of_ms:typeof v.as_of_ms==='number'?v.as_of_ms:Date.now(),data:(v.result??v.data??value) as Json,quality:(v.quality??{state:'not_evaluated'}) as Json,warnings:(v.warnings??[]) as Json,...(['failed','rejected','unknown'].includes(status)?{reason_code:reason||'REQUEST_FAILED',scope:(v.scope??'request') as Json,retryable:status==='unknown'?false:!!v.retryable,next_action:(v.next_action??(status==='unknown'?'inspect_exact_intent':'inspect_diagnostics')) as Json}:{}),...(typeof v.job_id==='string'?{job_id:v.job_id}:{}),...(typeof v.cursor==='string'?{cursor:v.cursor}:{}),...(typeof v.artifact_id==='string'?{artifact_id:v.artifact_id}:{})});
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
