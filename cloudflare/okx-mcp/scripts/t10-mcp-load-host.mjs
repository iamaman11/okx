#!/usr/bin/env node
// Test-only Miniflare host for genuine HTTP/MCP -> DO -> WSS -> Native calls.
// It creates an isolated, empty, ephemeral DO; never contacts a real exchange
// or an installed production Worker and does not provide OAuth acceptance.
import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import {mkdtemp,readFile,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {fileURLToPath} from 'node:url';
import {build} from 'esbuild';
import {Miniflare} from 'miniflare';

const installationId=process.argv[2];
assert.match(installationId??'',/^[a-zA-Z0-9_:-]{8,128}$/);
const root=await mkdtemp(join(tmpdir(),'okx-t10-mcp-load-'));
let mf;
const token='integration-only-ephemeral-token';
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
    RUNTIME_TOKEN_SHA256:{type:'text',value:sha256(token)}},
   exports:{RuntimeSession:{type:'durable-object',storage:'sqlite'}},
  }}],
 });
 const ready=await mf.ready;
 assert.equal(ready.hostname,'127.0.0.1');
 const installation={contract:'okx.direct-transport.frame/v2',
  release_sha256:'f'.repeat(64),operation_id:'t10_mcp_load_install_01',
  configuration:{schema:'okx.governance/v3',revision:0,installation_id:installationId,
   campaign_id:'sha256:'+'c'.repeat(64),campaign_loss_limit_usd:'10',
   campaign_entry_limit_usd:'10',max_total_modelled_loss_usdt:'5',accounts:[]}};
 const result=await mf.dispatchFetch(new URL('/installation/initialize',ready).toString(),{
  method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify(installation)});
 assert.equal(result.status,200,'isolated Durable Object installation must initialize');
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
