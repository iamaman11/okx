#!/usr/bin/env node
// Test-only Miniflare host for genuine HTTP/MCP -> DO -> WSS -> Native calls.
// It creates an isolated, empty, ephemeral DO; never contacts a real exchange
// or an installed production Worker and does not provide OAuth acceptance.
import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import {mkdtemp,readFile,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {isAbsolute,join} from 'node:path';
import {fileURLToPath} from 'node:url';
import {build} from 'esbuild';
import {Miniflare} from 'miniflare';

const researchOnly=process.argv[2]==='--research-only';
const installationId=process.argv[2];
let desired;
if(researchOnly){
 assert.equal(process.argv.length,3,'research-only takes no additional arguments');
}else{
assert.match(installationId??'',/^[a-zA-Z0-9_:-]{8,128}$/);
const governancePath=process.argv[3];
assert.ok(isAbsolute(governancePath??''),'test-only synthetic governance path required');
desired=JSON.parse(await readFile(governancePath,'utf8'));
assert.equal(desired.schema,'okx.governance/v3');
assert.equal(desired.installation_id,installationId);
assert.equal(desired.revision,2);
assert.equal(desired.accounts.length,1);
}
const root=await mkdtemp(join(tmpdir(),'okx-t10-mcp-load-'));
let mf;
const token=researchOnly?'integration-only-research-ephemeral-token':'integration-only-ephemeral-token';
const sha256=value=>createHash('sha256').update(value).digest('hex');
async function cleanup(){
 if(mf)await mf.dispose();
 await rm(root,{recursive:true,force:true});
}
process.once('SIGTERM',()=>{cleanup().then(()=>process.exit(0),()=>process.exit(1));});
process.once('SIGINT',()=>{cleanup().then(()=>process.exit(0),()=>process.exit(1));});
try{
 const outfile=join(root,'mcp-load.mjs');
 await build({entryPoints:[fileURLToPath(new URL('../src/integration/mcp_load_entry.ts',import.meta.url))],
  outfile,bundle:true,platform:'browser',format:'esm',target:'es2022',logLevel:'silent'});
 const bundle=await readFile(outfile,'utf8');
 mf=new Miniflare({
  resourcePersistencePath:join(root,'sqlite-do'),
  workers:[{config:{name:'okx-t10-mcp-load',compatibilityDate:'2026-09-30',
   compatibilityFlags:['global_fetch_strictly_public'],
   manifest:{mainModule:'mcp-load.mjs',modulesRoot:root,modules:{
    'mcp-load.mjs':{type:'esm',contents:bundle},
   }},
   env:{RUNTIME:{type:'durable-object',worker:'okx-t10-mcp-load',exportName:'RuntimeSession'},
    [researchOnly?'RESEARCH_RUNTIME_TOKEN_SHA256':'RUNTIME_TOKEN_SHA256']:{type:'text',value:sha256(token)}},
   exports:{RuntimeSession:{type:'durable-object',storage:'sqlite'}},
  }}],
 });
 const ready=await mf.ready;
 assert.equal(ready.hostname,'127.0.0.1');
 if(!researchOnly){
 const installation={contract:'okx.direct-transport.frame/v2',
  release_sha256:'f'.repeat(64),operation_id:'t10_mcp_load_install_01',
  configuration:{...desired,revision:0,accounts:[]}};
 const post=async(path,body)=>{
  const response=await mf.dispatchFetch(new URL(path,ready).toString(),{
   method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify(body)});
  assert.equal(response.status,200,`isolated governance transaction ${path}`);
  return response.json();
 };
 await post('/installation/initialize',installation);
 // Use the real installation -> verified enrollment -> revision CAS routes.
 // A SQL seed or test-only DO bypass would not exercise production policy.
 const account=desired.accounts[0];
 await post('/account-governance/enroll',{
  uid_fingerprint:account.uid_fingerprint,venue:account.venue,
  environment:account.environment,label:account.label,alias:account.id,
  enabled:account.enabled,limits:account.limits,
  idempotency_key:'t10_mcp_enroll_01',credential_ref:'synthetic_test_credential',
 });
 await post('/account-governance/pause',{
  account:account.id,expected_revision:1,explicit_pause:account.explicit_pause,
  idempotency_key:'t10_mcp_sync_revision_02',
 });
 const readback=await mf.dispatchFetch(new URL('/account-governance',ready).toString());
 assert.equal(readback.status,200);
 const actual=await readback.json();
 for(const key of ['revision','installation_id','campaign_id','accounts'])
  assert.deepEqual(actual[key],desired[key],`DO governance ${key} must match Native fixture`);
 }
 const runtime=new URL('/runtime',ready);runtime.protocol='ws:';
 process.stdout.write('T10_MCP_LOAD_HOST_READY:'+JSON.stringify({
  schema:'okx.518.t10-mcp-load-host/v1',
  http_url:new URL('/mcp',ready).toString(),ws_url:runtime.toString(),
  oauth:'test_scope_only_not_evaluated',storage:'ephemeral_sqlite_do',
 })+'\n');
 // The Rust test closes its owned stdin on normal exit. Drop the test-only
 // SQLite store, workerd process and temporary directory without OS killing.
 process.stdin.resume();
 process.stdin.once('end',()=>{cleanup().then(()=>process.exit(0),()=>process.exit(1));});
 await new Promise(()=>{});
}catch(error){
 process.stderr.write('T10_MCP_LOAD_HOST_ERROR:'+String(error)+'\n');
 await cleanup();
 process.exitCode=1;
}
