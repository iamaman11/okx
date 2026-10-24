/** Independent, read-only release evidence from the existing native CONTROL issue #12.
 * An authenticated WSS session and its self-reported executable hash are NEVER
 * sufficient by themselves. Missing GitHub evidence fails closed.
 */
const OWNER_ID=44_100_369;
const REPOSITORY_ID=1_388_071_566;
const MAX_RECEIPT_AGE_MS=300_000;
const MAX_COMMENTS=100;
const GITHUB_COMMENTS='https://api.github.com/repos/iamaman11/okx/issues/12/comments';
const cache=new Map<string,{time:number;result:ReleaseAttestation}>();
export type ReleaseAttestation={
  verified:boolean;
  reason_code:string|null;
  source_tree:string|null;
  agent_binary_sha256:string|null;
  artifact_id:number|null;
  control_observed_at?:string|null;
  control_receipt_url?:string|null;
};
type JsonObject=Record<string,unknown>;
function obj(x:unknown):JsonObject|null{
 return x!==null&&typeof x==='object'&&!Array.isArray(x)?x as JsonObject:null;
}
function field(x:JsonObject|null,k:string){return obj(x?.[k]);}
function hex(x:unknown,n:number){return typeof x==='string'&&new RegExp('^[0-9a-f]{'+n+'}$').test(x);}
function validPositive(x:unknown):x is number{return Number.isSafeInteger(x)&&Number(x)>0;}
function failure(code:string):ReleaseAttestation{return {verified:false,reason_code:code,source_tree:null,agent_binary_sha256:null,artifact_id:null};}
function validateReceipt(comment:JsonObject,observed:JsonObject,sourceTree:string,now:number):ReleaseAttestation{
 const user=field(comment,'user');
 if(user?.id!==OWNER_ID)return failure('CONTROL_RECEIPT_AUTHOR_MISMATCH');
 if(typeof comment.body!=='string'||comment.body.length>24_576)return failure('CONTROL_RECEIPT_INVALID');
 let raw:unknown;try{raw=JSON.parse(comment.body);}catch{return failure('CONTROL_RECEIPT_INVALID');}
 const receipt=obj(raw);
 const operation=field(receipt,'operation');
 const details=field(receipt,'details');
 const installed=field(details,'installed_agent');
 const provenance=field(installed,'provenance');
 const parity=field(details,'release_parity');
 const catalog=field(details,'account_catalog');
 const controller=field(field(field(details,'controller_update'),'active'),'value');
 const launcher=field(field(field(details,'launcher'),'active'),'value');
 if(!receipt||receipt.schema!=='okx.windows.control.result/v1'||receipt.status!=='PASS'||
    operation?.type!=='status'||typeof receipt.request_id!=='string'||
    !details||!installed||!provenance||!parity||!catalog)
     return failure('CONTROL_RECEIPT_INVALID');
 const at=Date.parse(String(receipt.observed_at));
 const created=Date.parse(String(comment.created_at));
 if(!Number.isFinite(at)||!Number.isFinite(created)||now-at< -10_000||now-created< -10_000||
    now-at>MAX_RECEIPT_AGE_MS||now-created>MAX_RECEIPT_AGE_MS)
    return failure('CONTROL_RECEIPT_STALE');
 if(installed.state!=='VERIFIED'||installed.reason!=='HASH_MATCH'||installed.binary_present!==true||
    details.agent_owned_running!==true||details.agent_desired!=='RUNNING'||
    parity.state!=='MATCHED'||!Array.isArray(parity.blockers)||parity.blockers.length!==0||
    provenance.schema!=='okx.host-control.installed-agent/v1'||
    provenance.repository_id!==REPOSITORY_ID||
    !validPositive(provenance.run_id)||!validPositive(provenance.artifact_id)||
    !hex(provenance.source_head_sha,40)||
    !hex(provenance.source_tree,40)||!hex(provenance.agent_sha256,64)||
    !hex(installed.binary_sha256,64))
    return failure('CONTROL_RELEASE_UNVERIFIED');
 if(parity.source_tree!==sourceTree||provenance.source_tree!==sourceTree||
    (controller?.active&&field(controller,'active')?.source_tree!==sourceTree)||
    (launcher?.active&&field(launcher,'active')?.source_tree!==sourceTree))
    return failure('RELEASE_SOURCE_TREE_MISMATCH');
 if(installed.binary_sha256!==provenance.agent_sha256||
    observed.agent_binary_sha256!==provenance.agent_sha256)
    return failure('AGENT_BINARY_HASH_MISMATCH');
 if(details.agent_pid!==observed.agent_pid||
    catalog.installation_id!==observed.installation_id)
    return failure('LIVE_AGENT_IDENTITY_MISMATCH');
 if(typeof comment.html_url!=='string'||!comment.html_url.startsWith('https://github.com/iamaman11/okx/issues/12#issuecomment-'))
    return failure('CONTROL_RECEIPT_URL_INVALID');
 return {verified:true,reason_code:null,source_tree:sourceTree,
   agent_binary_sha256:provenance.agent_sha256 as string,
   artifact_id:provenance.artifact_id as number,
   control_observed_at:receipt.observed_at as string,control_receipt_url:comment.html_url};
}
export async function attestRuntimeRelease(
 observed:JsonObject, sourceTree:unknown,
 now=Date.now(), get:typeof fetch=fetch,
):Promise<ReleaseAttestation>{
 if(!hex(sourceTree,40))return failure('WORKER_RELEASE_TREE_UNSET');
 if(observed.schema!=='okx.direct-transport.status/v1'||observed.status!=='PASS'||
    observed.connected!==true||observed.session_fresh!==true||
    observed.runtime_id!=='windows-primary'||observed.runtime_profile!=='engine'||
    !validPositive(observed.connection_generation)||!validPositive(observed.agent_pid)||
    !hex(observed.agent_binary_sha256,64)||
    typeof observed.installation_id!=='string'||observed.installation_id.length<8)
    return failure('LIVE_RELEASE_IDENTITY_UNAVAILABLE');
 const key=JSON.stringify([sourceTree,observed.connection_generation,observed.agent_pid,
   observed.agent_binary_sha256,observed.installation_id]);
 if(get===fetch){
   const cached=cache.get(key);
   if(cached&&now>=cached.time&&now-cached.time<20_000)return cached.result;
 }
 let result:ReleaseAttestation;
 try{
   const since=new Date(now-2*MAX_RECEIPT_AGE_MS).toISOString();
   const response=await get(GITHUB_COMMENTS+'?per_page=100&since='+encodeURIComponent(since),{
     method:'GET',headers:{'Accept':'application/vnd.github+json','X-GitHub-Api-Version':'2022-11-28'},
     signal:AbortSignal.timeout(4_000),
   });
   if(!response.ok)return failure('CONTROL_RECEIPT_FETCH_FAILED');
   const comments:unknown=await response.json();
   if(!Array.isArray(comments)||comments.length>=MAX_COMMENTS)return failure('CONTROL_RECEIPT_PAGE_UNBOUNDED');
   const candidates=comments.map(obj).filter((comment):comment is JsonObject=>{
     if(!comment||field(comment,'user')?.id!==OWNER_ID||typeof comment.body!=='string')return false;
     try{
       const receipt=obj(JSON.parse(comment.body));
       return receipt?.schema==='okx.windows.control.result/v1'&&field(receipt,'operation')?.type==='status';
     }catch{return false;}
   });
   const newest=candidates.sort((a,b)=>Number(b.id)-Number(a.id))[0];
   result=newest?validateReceipt(newest,observed,sourceTree as string,now)
     :failure('CONTROL_RECEIPT_MISSING');
 }catch{result=failure('CONTROL_RECEIPT_FETCH_FAILED');}
 if(get===fetch){
   if(cache.size>32)cache.clear();
   cache.set(key,{time:now,result});
 }
 return result;
}
