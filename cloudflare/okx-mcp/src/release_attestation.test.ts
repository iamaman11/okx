import {attestRuntimeRelease,releaseSessionIdentity,cachedSessionAttestation} from './release_attestation.js';
const fail=(value:boolean,label:string)=>{if(!value)throw Error(label);};
const tree='a'.repeat(40), hash='b'.repeat(64), now=1_792_850_000_000;
const live={schema:'okx.direct-transport.status/v1',status:'PASS',connected:true,session_fresh:true,
 runtime_id:'windows-primary',runtime_profile:'engine',connection_generation:15,connected_at_ms:now-10_000,
 agent_pid:3216,agent_binary_sha256:hash,agent_incarnation:'ctl_'+'1'.repeat(32),
 installation_id:'okx-clean-398-20261023'};
const receipt={schema:'okx.windows.control.result/v1',request_id:'ctl_test_release_398',
 operation:{type:'status'},status:'PASS',observed_at:new Date(now-5_000).toISOString(),
 details:{agent_pid:3216,agent_incarnation:live.agent_incarnation,agent_desired:'RUNNING',agent_owned_running:true,
 account_catalog:{installation_id:live.installation_id},
 release_parity:{state:'MATCHED',source_tree:tree,blockers:[]},
 controller_update:{active:{value:{active:{source_tree:tree}}}},
 launcher:{active:{value:{active:{source_tree:tree}}}},
 installed_agent:{state:'VERIFIED',reason:'HASH_MATCH',binary_present:true,binary_sha256:hash,
 provenance:{schema:'okx.host-control.installed-agent/v1',repository_id:1_388_071_566,
 run_id:123,artifact_id:456,source_head_sha:'c'.repeat(40),source_tree:tree,agent_sha256:hash}}},
 failure:null};
const comment={id:1234,user:{id:44_100_369},body:JSON.stringify(receipt),
 created_at:new Date(now-3_000).toISOString(),
 html_url:'https://github.com/iamaman11/okx/issues/12#issuecomment-1234'};
function response(comments:unknown, status=200):typeof fetch{
 return (async()=>Response.json(comments,{status})) as typeof fetch;
}
async function reason(rows:unknown,observed=live,source=tree){
 return (await attestRuntimeRelease(observed,source,now,response(rows))).reason_code;
}
const ok=await attestRuntimeRelease(live,tree,now,response([comment]));
fail(ok.verified===true&&ok.reason_code===null&&ok.source_tree===tree&&
 ok.agent_binary_sha256===hash&&ok.artifact_id===456,'independent native CONTROL match');
const spoof={...comment,id:1235,user:{id:999}};
fail((await reason([spoof]))==='CONTROL_RECEIPT_MISSING','no arbitrary GH user evidence');
fail((await reason([]))==='CONTROL_RECEIPT_MISSING','no receipt does not become verified');
fail((await reason([comment],{...live,agent_pid:3217}))==='LIVE_AGENT_IDENTITY_MISMATCH','different live PID rejected');
fail((await reason([comment],{...live,agent_incarnation:'ctl_'+'2'.repeat(32)}))==='CONTROL_RELEASE_UNVERIFIED','different CONTROL child rejected');
fail((await reason([comment],{...live,agent_binary_sha256:'d'.repeat(64)}))==='AGENT_BINARY_HASH_MISMATCH','different native executable rejected');
fail((await reason([comment],{...live,connection_generation:0}))==='LIVE_RELEASE_IDENTITY_UNAVAILABLE','stale generation rejected');
fail((await reason([comment],{...live,connected_at_ms:now+1_000}))==='CONTROL_RECEIPT_PREDATES_SESSION','previous generation evidence rejected');
fail((await reason([comment],{...live,session_fresh:false}))==='LIVE_RELEASE_IDENTITY_UNAVAILABLE','offline session rejected');
fail((await reason([comment],live,'d'.repeat(40)))==='RELEASE_SOURCE_TREE_MISMATCH','different Worker git tree rejected');
fail((await reason([{...comment,body:JSON.stringify({...receipt,observed_at:new Date(now-800_000).toISOString()})}]))==='CONTROL_RECEIPT_STALE','old native receipt rejected');
fail((await reason([{...comment,created_at:new Date(now-800_000).toISOString()}]))==='CONTROL_RECEIPT_STALE','old GH comment rejected');
fail((await reason([{...comment,body:JSON.stringify({...receipt,details:{...receipt.details,agent_owned_running:false}})}]))==='CONTROL_RELEASE_UNVERIFIED','CONTROL not running rejected');
fail((await reason([{...comment,body:JSON.stringify({...receipt,details:{...receipt.details,release_parity:{state:'MISMATCH',source_tree:tree,blockers:['broken']}}})}]))==='CONTROL_RELEASE_UNVERIFIED','mismatched CONTROL parity rejected');
fail((await reason([comment],live,''))==='WORKER_RELEASE_TREE_UNSET','no Worker source receipt rejected');
fail((await attestRuntimeRelease(live,tree,now,response([],503))).reason_code==='CONTROL_RECEIPT_FETCH_FAILED','GitHub outage fails closed');
const headerProof:typeof fetch=(async(input,init)=>{
 const headers=new Headers(init?.headers);
 fail(headers.get('user-agent')==='okx-cloudflare-mcp-release-attestation','GitHub User-Agent required');
 fail(headers.get('accept')==='application/vnd.github+json','GitHub Accept required');
 fail(headers.get('x-github-api-version')==='2022-11-28','GitHub API version required');
 fail(String(input).startsWith('https://api.github.com/repos/iamaman11/okx/issues/12/comments?'),
   'only canonical CONTROL endpoint');
 return Response.json([comment]);
}) as typeof fetch;
fail((await attestRuntimeRelease(live,tree,now,headerProof)).verified===true,
 'GitHub User-Agent and independent receipt verified');
fail((await attestRuntimeRelease(live,tree,now,response([],401))).reason_code==='CONTROL_RECEIPT_GITHUB_UNAUTHORIZED',
 'HTTP 401 cannot be misreported as HTTP 403 or a rate limit');
fail((await attestRuntimeRelease(live,tree,now,response([],403))).reason_code==='CONTROL_RECEIPT_GITHUB_403_UNCLASSIFIED',
 'HTTP 403 without rate-limit headers is not proof of permission denial');
const rateResponse=(status:number,headers:Record<string,string>):typeof fetch=>
 (async()=>Response.json([],{status,headers})) as typeof fetch;
fail((await attestRuntimeRelease(live,tree,now,rateResponse(403,{'x-ratelimit-remaining':'0',
 'x-ratelimit-reset':'1792850360'}))).reason_code==='CONTROL_RECEIPT_GITHUB_RATE_LIMITED',
 'HTTP 403 with exhausted primary budget is a rate limit');
fail((await attestRuntimeRelease(live,tree,now,rateResponse(403,{'x-ratelimit-remaining':'55',
 'retry-after':'60'}))).reason_code==='CONTROL_RECEIPT_GITHUB_RATE_LIMITED',
 'HTTP 403 with secondary retry-after is a rate limit');
fail((await attestRuntimeRelease(live,tree,now,response([],429))).reason_code==='CONTROL_RECEIPT_GITHUB_RATE_LIMITED',
 'HTTP 429 is a rate limit');
fail((await reason(Array.from({length:100},()=>comment)))==='CONTROL_RECEIPT_PAGE_UNBOUNDED','unbounded issue window fails closed');
const newer={...comment,id:1236,body:JSON.stringify({...receipt,details:{...receipt.details,agent_pid:999}})};
fail((await reason([comment,newer]))==='LIVE_AGENT_IDENTITY_MISMATCH','never select older matching status over latest');
// Persisted owner receipt remains valid only for the unchanged authenticated
// session. A new generation, PID, executable, installation or source tree cannot
// inherit prior acceptance. No additional GitHub fetch is needed within a proven
// live generation even when the original five-minute receipt becomes old.
const sessionIdentity=releaseSessionIdentity(live,tree);
fail(typeof sessionIdentity==='string','WSS identity must be canonical');
const persisted={schema:'okx.runtime-session-attestation/v1',identity:sessionIdentity,proof:ok};
fail(cachedSessionAttestation(persisted,live,tree)?.verified===true,'unchanged live session reuses owner proof');
fail(cachedSessionAttestation(persisted,{...live,connection_generation:16},tree)?.verified===true,'same CONTROL child can reconnect without GitHub');
fail(cachedSessionAttestation(persisted,{...live,connected_at_ms:now-9_000},tree)?.verified===true,'new WSS for same child can reuse proof');
fail(cachedSessionAttestation(persisted,{...live,agent_incarnation:'ctl_'+'2'.repeat(32)},tree)===null,'new CONTROL child cannot inherit accepted proof even with reused PID');
fail(cachedSessionAttestation(persisted,{...live,agent_pid:3217},tree)===null,'different PID never inherits proof');
fail(cachedSessionAttestation(persisted,{...live,agent_binary_sha256:'d'.repeat(64)},tree)===null,'different executable never inherits proof');
fail(cachedSessionAttestation(persisted,{...live,installation_id:'other-installed-engine'},tree)===null,'different installation never inherits proof');
fail(cachedSessionAttestation(persisted,live,'c'.repeat(40))===null,'new release tree requires new owner receipt');
fail(cachedSessionAttestation(persisted,{...live,session_fresh:false},tree)===null,'stale WSS cannot reuse proof');
fail(cachedSessionAttestation({...persisted,proof:{...ok,verified:false}},live,tree)===null,'negative receipts cannot be cached as success');
fail(cachedSessionAttestation({...persisted,proof:{...ok,artifact_id:0}},live,tree)===null,'malformed artifact cannot be accepted');
console.log('generation_bound_release_attestation_positive_negative PASS');

console.log('independent_release_attestation_positive_negative PASS');
