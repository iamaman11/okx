import {defaultHandler} from './auth.js';
import {RuntimeSession} from './runtime_session.js';
import {initial} from './account_governance.test.js';
import {FRAME_SCHEMA} from './shared.js';
class Storage{map=new Map();async get(k:string){return structuredClone(this.map.get(k));}async put(k:string,v:any){this.map.set(k,structuredClone(v));}async transaction(fn:any){const backup=structuredClone(this.map);try{return await fn(this);}catch(e){this.map=backup;throw e;}}}
const storage=new Storage(),owner=new RuntimeSession({storage},{} as any);
let calls=0;const env:any={MCP_OWNER_SECRET:'fixture_secret',RUNTIME:{idFromName:()=>0,get:()=>({fetch:(r:Request)=>{calls++;return owner.fetch(r);}})}};
const body={configuration:initial,operation_id:'initialize_0001',contract:FRAME_SCHEMA,release_sha256:'a'.repeat(64)};
const req=(auth?:string,payload=body)=>new Request('https://fixture/installation/initialize',{method:'POST',headers:auth?{authorization:auth}:{},body:JSON.stringify(payload)});
if((await defaultHandler.fetch(req(),env)).status!==401||calls!==0)throw Error('initialize must authenticate owner');
if((await defaultHandler.fetch(req('Bearer fixture_secret'),env)).status!==200)throw Error('owner initialization failed');
if((await defaultHandler.fetch(req('Bearer fixture_secret'),env)).status!==200)throw Error('lost init response exact retry');
if((await defaultHandler.fetch(req('Bearer fixture_secret',{...body,release_sha256:'b'.repeat(64)}),env)).status!==409)throw Error('init same key changed body conflict');
if(!storage.map.has('risk_meta_v2')||!storage.map.has('installation_release_v1'))throw Error('initialization not atomic');
console.log('owner_install_init_auth_body_bound PASS');
