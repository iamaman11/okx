// #398 A2/A3: real Worker routing + actual central governance transaction owner,
// with a stateful fake Agent/venue. Never classify this as live WSS/native SQL.
import {mcpApi} from './mcp.js';
import {governanceRequest,initializeGovernance,enrollObservedAccount,setAccountPause,setAccountLimits} from './account_governance_store.js';
import {initial,limits} from './account_governance.test.js';
import type {Governance} from './account_governance.js';

function check(ok:unknown,message:string):asserts ok{if(!ok)throw Error(message);}
class Storage {
 map=new Map<string,any>();
 private queue=Promise.resolve();
 async get(key:string){return structuredClone(this.map.get(key));}
 async put(key:string,value:unknown){this.map.set(key,structuredClone(value));}
 async delete(keys:string[]){let count=0;for(const k of keys)if(this.map.delete(k))count++;return count;}
 async transaction<T>(fn:(tx:Storage)=>Promise<T>):Promise<T>{
  const preceding=this.queue;let unlock!:()=>void;
  this.queue=new Promise<void>(resolve=>{unlock=resolve;});await preceding;
  const original=structuredClone(this.map);
  try{return await fn(this);}catch(error){this.map=original;throw error;}finally{unlock();}
 }
}
const storage=new Storage();
check((await initializeGovernance(storage,initial)).status===200,'synthetic central installation');
const ctx={auth:{scope:['mcp:use']}};
let id=0,venuePostCount=0,transportCalls=0;
const intents=new Map<string,{intent_id:string;state:string;plan:Record<string,unknown>;protected:boolean}>();
const bodyHash=new Map<string,string>();
const intentByKey=new Map<string,string>();
const verifiedUid='a'.repeat(64);
const accountKey='okx:demo:'+verifiedUid;
const success=(data:unknown)=>({schema:'okx.agent.response/v1',request_id:'fixture_native_'+(++id),
 status:'completed',quality:'FRESH',warnings:[],result:{status:'completed',as_of_ms:1792890000000,
 data,quality:{state:'fresh'},warnings:[]}});
const rejected=(reason:string)=>({schema:'okx.agent.response/v1',request_id:'fixture_native_'+(++id),
 status:'rejected',quality:'NOT_READY',warnings:[],result:null,
 failure:{code:reason,message:reason,retryable:false}});
const handler=async(url:string,init?:RequestInit)=>{
 transportCalls++;
 const path=new URL(url).pathname;
 const raw=init?.body?JSON.parse(String(init.body)):null;
 const reply=async(out:{status:number;body:Record<string,unknown>})=>Response.json(out.body);
 if(path==='/account-governance')return reply(await governanceRequest(storage,'GET'));
 if(path==='/account-governance/enroll')return reply(await enrollObservedAccount(storage,raw));
 if(path==='/account-governance/pause')return reply(await setAccountPause(storage,raw));
 if(path==='/account-governance/limits')return reply(await setAccountLimits(storage,raw));
 check(path==='/dispatch','unexpected transport endpoint');
 const op=raw?.operation;
 if(op?.type==='connect_account'){
  return Response.json({status:'completed',result:{alias:op.alias,credential_ref:op.credential_ref,
   connect_operation_id:op.operation_id,configuration_digest:op.request_digest,
   account_key:{venue:'okx',environment:'demo',verified_uid:verifiedUid}}});
 }
 const req=op?.request;
 if(op?.type!=='trade'||!req)return Response.json(rejected('FAKE_OPERATION_UNSUPPORTED'));
 const governance=(await governanceRequest(storage,'GET')).body as any;
 const row=governance.accounts?.find((a:any)=>a.account_key===raw.account||a.id===raw.account);
 if(!row)return Response.json(rejected('ACCOUNT_NOT_FOUND'));
 if(req.action==='preflight')return Response.json(success({ready_for_new_risk:false,side_effects:'none'}));
 if(req.action==='preview')return Response.json(success({admission:'preview_only',
  plan:{instrument:req.instrument,stop_price:'99',planned_loss_usd:'0.5'},exchange_post:false}));
 if(req.action==='status'){
  const found=intents.get(req.reference);
  return Response.json(found?success({intent:found}):rejected('INTENT_NOT_FOUND'));
 }
 if(['execute','cancel','close'].includes(req.action)&&row.explicit_pause&&req.action==='execute'){
  return Response.json(rejected('ACCOUNT_NEW_RISK_PAUSED'));
 }
 if(req.action==='execute'){
  const hash=JSON.stringify(req),key=req.idempotency_key;
  if(bodyHash.has(key)&&bodyHash.get(key)!==hash)return Response.json(rejected('IDEMPOTENCY_CONFLICT'));
  const prior=intentByKey.get(key);
  if(prior)return Response.json(success({intent:intents.get(prior)}));
  const intent_id='intent_fake_'+(intents.size+1);
  const intent={intent_id,state:'FILLED',plan:{instrument:req.instrument,operation:req.operation,
   protection:req.protection,stop_price:'99',planned_loss_usd:'0.5'},protected:true};
  // Fake-venue POST counter measures fixture operations, NEVER real exchange POST.
  venuePostCount++;
  intents.set(intent_id,intent);bodyHash.set(key,hash);intentByKey.set(key,intent_id);
  return Response.json(success({intent}));
 }
 const found=intents.get(req.reference);
 if(!found)return Response.json(rejected('INTENT_NOT_FOUND'));
 if(req.action==='cancel'){
  if(found.state==='CANCELLED')return Response.json(success({intent:found}));
  found.state='CANCELLED';return Response.json(success({intent:found}));
 }
 if(req.action==='close'){
  const closed={...found,intent_id:'close_'+found.intent_id,state:'FILLED',
   plan:{...found.plan,operation:'close'}};
  intents.set(closed.intent_id,closed);return Response.json(success({intent:closed}));
 }
 return Response.json(rejected('FAKE_OPERATION_UNSUPPORTED'));
};
const env:any={RUNTIME:{idFromName:()=>0,get:()=>({fetch:handler})}};
async function call(tool:string,args:Record<string,unknown>){
 const rpc:any=await(await mcpApi.fetch(new Request('https://fixture/mcp',{method:'POST',
  body:JSON.stringify({jsonrpc:'2.0',id:++id,method:'tools/call',
   params:{name:tool,arguments:args}})}),env,ctx)).json();
 check(!rpc.error,tool+'.'+args.action+' rejected at schema '+JSON.stringify(rpc.error));
 const response=rpc.result?.structuredContent;
 check(response&&typeof response.status==='string','missing MCP typed response');
 return response;
}
const config=await call('accounts',{action:'list'});
check(config.status==='completed'&&config.data.accounts.length===0,'central empty read');
const connectArgs={action:'connect',alias:'fixture_a',label:'Fixture A',
 credential_ref:'credential_fixture_a',enabled:true,limits,
 idempotency_key:'fixture_connect_0001'};
const first=await call('accounts',connectArgs);
check(first.status==='completed'&&first.data.account.account_key===accountKey&&
 first.data.account.identity_verified===true,'connect must reach real central enrollment owner');
const repeated=await call('accounts',connectArgs);
check(repeated.status==='completed'&&repeated.data.account.account_key===accountKey,'connect replay');
const changed=await call('accounts',{...connectArgs,label:'Different'});
check(changed.status==='failed'&&changed.reason_code==='IDEMPOTENCY_CONFLICT','body-bound connect');
let read=await call('accounts',{action:'list'});
check(read.data.revision===1&&read.data.accounts[0].id==='fixture_a','durable account readback');
const pauseArgs={action:'pause',account:'fixture_a',expected_revision:1,
 idempotency_key:'fixture_pause_0001'};
let pause=await call('accounts',pauseArgs);
check(pause.status==='completed'&&pause.data.revision===2&&pause.data.accounts[0].explicit_pause,
 'central pause and revision');
const pausedReplay=await call('accounts',pauseArgs);
check(pausedReplay.data.revision===2,'pause replay same revision');
const staleResume=await call('accounts',{...pauseArgs,action:'resume',idempotency_key:'fixture_resume_stale_001'});
check(staleResume.status==='failed'&&staleResume.reason_code==='STALE_ACCESS_REVISION',
 'stale governance revision rejected');
const plan={operation:'open',strategy_instance:'manual',instrument:'BTC-USDT-SWAP',
 side:'long',sizing:{contracts:'0.01'},order:{type:'limit',limit_price:'100',max_slippage_bps:25},
 protection:{stop_distance_bps:100}};
const blocked=await call('trade',{action:'execute',account:'fixture_a',
 idempotency_key:'fake_blocked_001',expires_in_ms:30000,...plan});
check(blocked.status==='rejected'&&blocked.reason_code==='ACCOUNT_NEW_RISK_PAUSED',
 'paused fake venue blocks new risk without post');
check(Number(venuePostCount)===0,'no fake post during paused state');
const resume=await call('accounts',{action:'resume',account:'fixture_a',expected_revision:2,
 idempotency_key:'fixture_resume_0001'});
check(resume.status==='completed'&&resume.data.revision===3&&
 resume.data.accounts[0].explicit_pause===false,'central resume');
const revisedLimits={...limits,max_order_loss_usdt:'0.5'};
const limitsArgs={action:'set_limits',account:'fixture_a',expected_revision:3,
 idempotency_key:'fixture_limits_0001',limits:revisedLimits};
const revised=await call('accounts',limitsArgs);
check(revised.status==='completed'&&revised.data.revision===4&&
 revised.data.accounts[0].limits.max_order_loss_usdt==='0.5','central limits CAS');
const staleLimit=await call('accounts',{...limitsArgs,idempotency_key:'fixture_limits_stale_001'});
check(staleLimit.status==='failed'&&staleLimit.reason_code==='STALE_ACCESS_REVISION',
 'stale limits cannot overwrite');
const replayLimit=await call('accounts',limitsArgs);
check(replayLimit.data.revision===4,'limits identical replay');
read=await call('accounts',{action:'status',account:'fixture_a'});
check(read.data.account.policy_revision===4&&read.data.account.limits.max_order_loss_usdt==='0.5',
 'account status sees durable central owner');
check((await call('trade',{action:'preflight',account:'fixture_a'})).status==='completed','preflight');
const preview=await call('trade',{action:'preview',account:'fixture_a',...plan});
check(preview.status==='completed'&&preview.data.admission==='preview_only'&&
 preview.data.exchange_post===false,'preview did not post');
const executeArgs={action:'execute',account:'fixture_a',
 idempotency_key:'fake_execute_0001',expires_in_ms:30000,...plan};
const executed=await call('trade',executeArgs);
const ref=executed.data?.intent?.intent_id;
check(executed.status==='completed'&&typeof ref==='string'&&executed.data.intent.protected,
 'protected fake venue execute');
check((await call('trade',executeArgs)).data.intent.intent_id===ref&&Number(venuePostCount)===1,
 'exact replay never duplicates fixture POST');
const conflicting=await call('trade',{...executeArgs,operation:'replace'});
check(conflicting.status==='rejected'&&conflicting.reason_code==='IDEMPOTENCY_CONFLICT',
 'changed-body execution rejects duplicate key');
const status=await call('trade',{action:'status',account:'fixture_a',reference:ref});
check(status.data.intent.protected&&status.data.intent.state==='FILLED','fake status/protection');
const replace=await call('trade',{...executeArgs,operation:'replace',idempotency_key:'fake_replace_0001'});
check(replace.status==='completed'&&replace.data.intent.plan.operation==='replace'&&Number(venuePostCount)===2,
 'replace is a separate protected fake venue intent');
const cancel=await call('trade',{action:'cancel',account:'fixture_a',
 reference:replace.data.intent.intent_id,idempotency_key:'fake_cancel_0001'});
check(cancel.status==='completed'&&cancel.data.intent.state==='CANCELLED','fake cancel business transition');
const closed=await call('trade',{action:'close',account:'fixture_a',reference:ref,
 idempotency_key:'fake_close_0001',expires_in_ms:30000});
check(closed.status==='completed'&&closed.data.intent.state==='FILLED'&&
 closed.data.intent.plan.operation==='close','fake close outcome');
check((await call('trade',{action:'status',account:'fixture_a',reference:'unknown'}))
 .reason_code==='INTENT_NOT_FOUND','exact missing intent failure');
const final=(await governanceRequest(storage,'GET')).body as Governance&{status:string};
check(final.revision===4&&final.accounts.length===1&&
 final.accounts[0].limits.max_order_loss_usdt==='0.5'&&
 final.accounts[0].explicit_pause===false,'persisted central owner was not overwritten');
check(Number(venuePostCount)===2&&transportCalls>24,'no unexpected fixture posts');
console.log('mcp_central_owner_composed_fake_venue PASS central connect/pause/resume/limits CAS + protected trade lifecycle, two fixture posts, zero real exchange POST, no native SQL/WSS claim');
