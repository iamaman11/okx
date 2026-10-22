import {mcpApi} from "./mcp.js";
const response=await mcpApi.fetch(new Request("https://fixture/mcp",{method:"POST",
 headers:{"content-type":"application/json"},body:JSON.stringify({jsonrpc:"2.0",id:1,method:"tools/list"})}),{} as any,{auth:{scope:["mcp:use"]}});
const body:any=await response.json();
const bytes=new TextEncoder().encode(JSON.stringify(body.result)).byteLength;
if(body.result.tools.length>16||bytes>24*1024)throw Error(`tools_list_serialized_budget: ${body.result.tools.length} tools, ${bytes} bytes`);
if(body.result.tools.some((t:any)=>t.name==="account_control"))throw Error("retired account activation API exported");
const accounts=body.result.tools.find((t:any)=>t.name==="accounts");
if(!accounts||accounts.inputSchema.properties.action.enum.includes("authorize"))throw Error("persistent access contract absent");
console.log(`tools_list_serialized_budget: PASS (${body.result.tools.length} tools, ${bytes} bytes)`);
