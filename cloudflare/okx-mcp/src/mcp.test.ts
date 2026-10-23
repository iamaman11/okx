undefined
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
