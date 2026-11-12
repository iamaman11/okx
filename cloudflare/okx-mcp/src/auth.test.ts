import {defaultHandler} from './auth.js';
import {RuntimeSession} from './runtime_session.js';
import {initial} from './account_governance.test.js';
import {FRAME_SCHEMA} from './shared.js';
class Storage{map=new Map();async get(k:string){return structuredClone(this.map.get(k));}async list(opts:{prefix:string;limit:number}){return new Map([...this.map.entries()].filter(([key])=>key.startsWith(opts.prefix)).slice(0,opts.limit));}async put(k:string,v:any){this.map.set(k,structuredClone(v));}async delete(keys:string[]){let n=0;for(const key of keys)if(this.map.delete(key))n++;return n;}async transaction(fn:any){const backup=structuredClone(this.map);try{return await fn(this);}catch(e){this.map=backup;throw e;}}}
const storage=new Storage(),owner=new RuntimeSession({storage},{} as any);
let calls=0;const forwards:{url:string;has_auth:boolean}[]=[];
const env:any={MCP_OWNER_SECRET:'fixture_secret',RUNTIME:{idFromName:()=>0,get:()=>({
 fetch:(r:Request|string,init?:RequestInit)=>{
   const target=typeof r==="string"?new Request(r,init):r;
   calls++;forwards.push({url:target.url,has_auth:target.headers.has("authorization")});
   return owner.fetch(target);
 }
})}};
const releaseHealth:any=await(await defaultHandler.fetch(new Request('https://fixture/health'),
 {...env,RELEASE_SOURCE_TREE:'a'.repeat(40)})).json();
if(releaseHealth.status!=='ok'||releaseHealth.release_source_tree!=='a'.repeat(40))
 throw Error('health must expose the exact Worker deploy source tree');
const unboundHealth:any=await(await defaultHandler.fetch(new Request('https://fixture/health'),
 {...env,RELEASE_SOURCE_TREE:'not-a-git-tree'})).json();
if(unboundHealth.release_source_tree!==null)throw Error('health must not invent Worker provenance');
console.log('health_worker_release_tree_readback PASS');
const body={configuration:initial,operation_id:'initialize_0001',contract:FRAME_SCHEMA,release_sha256:'a'.repeat(64)};
const req=(auth?:string,payload=body)=>new Request('https://fixture/installation/initialize',{method:'POST',headers:auth?{authorization:auth}:{},body:JSON.stringify(payload)});
if((await defaultHandler.fetch(req(),env)).status!==401||calls!==0)throw Error('initialize must authenticate owner');
if((await defaultHandler.fetch(req('Bearer fixture_secret'),env)).status!==200)throw Error('owner initialization failed');
if((await defaultHandler.fetch(req('Bearer fixture_secret'),env)).status!==200)throw Error('lost init response exact retry');
if((await defaultHandler.fetch(req('Bearer fixture_secret',{...body,release_sha256:'b'.repeat(64)}),env)).status!==409)throw Error('init same key changed body conflict');
if(!storage.map.has('risk_meta_v2')||!storage.map.has('installation_release_v1'))throw Error('initialization not atomic');
const lower=new Request('https://fixture/account-governance/campaign-limit',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({expected_revision:0,campaign_loss_limit_usd:'8',idempotency_key:'lower_campaign_01'})});
if((await defaultHandler.fetch(lower,env)).status!==401)throw Error('campaign reduction must authenticate owner');
const lowerAuthorized=new Request('https://fixture/account-governance/campaign-limit',{method:'POST',headers:{authorization:'Bearer fixture_secret','content-type':'application/json'},body:JSON.stringify({expected_revision:0,campaign_loss_limit_usd:'8',idempotency_key:'lower_campaign_01'})});
if((await defaultHandler.fetch(lowerAuthorized,env)).status!==200)throw Error('owner lowers campaign without resetting installation');
console.log('owner_install_init_auth_body_bound PASS');

const inspection=(token?:string)=>new Request('https://fixture/installation/inspect',{
 method:'GET',headers:token?{authorization:token}:{}
});
const forwardedBefore=calls;
if((await defaultHandler.fetch(inspection(),env)).status!==401||calls!==forwardedBefore)
 throw Error('installation inspect must reject unauthenticated request before DO');
const snapshot=JSON.stringify([...storage.map.entries()]);
const inspected:any=await(await defaultHandler.fetch(inspection('Bearer fixture_secret'),env)).json();
const lastForward=forwards.at(-1);
if(lastForward?.url!=="https://runtime.internal/installation/inspect"||lastForward.has_auth)
 throw Error('owner inspection must use canonical internal DO URL without forwarding owner credentials');
if(inspected.schema!=='okx.installation-inspection/v1'||inspected.state!=='BOUND'
 ||inspected.accounts_total!==0||inspected.risk.metadata_present!==true
 ||inspected.risk.unresolved_entries!==0||inspected.risk.occupied_usd_nonzero!==false
 ||JSON.stringify([...storage.map.entries()])!==snapshot)
 throw Error('owner inspection must be read-only with bounded safe summary');
if(JSON.stringify(inspected).includes('fixture_secret')||'occupied_usd' in inspected.risk)
 throw Error('inspection must not disclose secrets or financial amounts');
storage.map.set('risk_entry_v2:fixture_01',{state:'UNKNOWN'});
const unsettled:any=await(await defaultHandler.fetch(inspection('Bearer fixture_secret'),env)).json();
if(unsettled.risk.unresolved_entries!==1||unsettled.risk.reservation_entries!==1)
 throw Error('unresolved risk must never be reported as settled');
storage.map.delete('account_access_v1');
const missing:any=await(await defaultHandler.fetch(inspection('Bearer fixture_secret'),env)).json();
if(missing.state!=='MISSING_OR_INVALID')throw Error('missing governance must fail closed');
console.log('owner_only_installation_inspection_no_mutation PASS');
