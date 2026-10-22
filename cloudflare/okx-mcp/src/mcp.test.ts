import {mcpApi} from './mcp.js';
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
