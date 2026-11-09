import {ACTION_SCHEMAS,PUBLIC_TOOLS,validTool,matches} from './execution_tools.js';
const fail=(v:boolean,n:string)=>{if(!v)throw Error(n);};
fail(validTool('diagnostics',{action:'explain',view:'research',reference:'a'.repeat(64)}),'research diagnostics uses existing strict tool');
fail(!validTool('diagnostics',{action:'explain',view:'unknown',reference:'a'.repeat(64)}),'unknown diagnostic view is rejected');
function sample(s:any):any{if('const'in s)return s.const;if(s.enum)return s.enum[0];if(s.oneOf)return sample(s.oneOf[0]);if(s.anyOf)return sample(s.anyOf[0]);if(s.type==='object')return Object.fromEntries(Object.entries(s.properties).filter(([k])=>s.required.includes(k)).map(([k,x])=>[k,sample(x)]));if(s.type==='array')return [sample(s.items)];if(s.type==='string'&&s.pattern==='^sha256:[0-9a-f]{64}$')return 'sha256:'+'a'.repeat(64);if(s.type==='string'&&s.pattern==='^[0-9a-f]{64}$')return 'a'.repeat(64);if(s.type==='string')return (s.pattern && s.maxLength===29 && new RegExp(s.pattern).test('1'))?'1':(s.pattern?.includes('0-9]{1,9}')||s.pattern?.includes('\\d{1,9}'))?'1':s.minLength===8?'command_0001':'fixture';if(s.type==='integer'||s.type==='number')return s.minimum??1;if(s.type==='boolean')return true;return null;}
export const payloadFixtures:{tool:string;action:string;positive:any;negative:any[]}[]=[];
for(const [tool,schemas]of Object.entries(ACTION_SCHEMAS))for(const schema of schemas){const good=sample(schema);if('start_ms'in good)good.end_ms=good.start_ms+100000;if(tool==='market'&&good.action==='screen')good.instruments=['BTC-USDT-SWAP'];if(tool==='research'&&good.action==='backtest')good.parameters={fast:9,slow:21};if(tool==='portfolio'&&good.action==='scenario')good.correlations=[];if(tool==='trade'&&['execute','preview'].includes(good.action))good.protection={stop_distance_bps:50};if(tool==='diagnostics'&&good.action==='explain')good.reference='intent_fixture';if(good.order)good.order.type='fok';fail(validTool(tool,good),`positive ${tool}.${good.action}`);const badExtra={...good,raw_rpc:'forbidden'},badRequired={...good};delete badRequired.action;const badType={...good,action:12};for(const bad of [badExtra,badRequired,badType])fail(!validTool(tool,bad),`negative ${tool}.${good.action}`);payloadFixtures.push({tool,action:good.action,positive:good,negative:[badExtra,badRequired,badType]});}
const screen=payloadFixtures.find(x=>x.tool==='market'&&x.action==='screen')!.positive;
fail(validTool('market',{...screen,action:'compare'}),'shared strict screen/compare schema');
fail(!validTool('market',{...screen,action:'compare',raw_rpc:'forbidden'}),'compare rejects unknown fields');
const summary=payloadFixtures.find(x=>x.tool==='portfolio'&&x.action==='summary')!.positive;
fail(validTool('portfolio',{...summary,action:'history'}),'shared portfolio history schema');
fail(!validTool('portfolio',{...summary,action:'history',raw_rpc:'forbidden'}),'strict history schema');
const marketScreen=payloadFixtures.find(x=>x.tool==='market'&&x.action==='screen')!.positive;
const {instruments:unused,...withoutList}=marketScreen;
fail(validTool('market',{...withoutList,universe:{instrument_type:'SWAP'}}),'market universe is a strict schema');
fail(!validTool('market',{...withoutList,fields:[],universe:{instrument_type:'SWAP'}}),'empty market fields rejected before native dispatch');
fail(!validTool('market',{...marketScreen,fields:[]}), 'explicit market screen requires at least one operator');
fail(!validTool('market',{...marketScreen,universe:{instrument_type:'SWAP'}}),'ambiguous universe rejected');
fail(!validTool('market',{...withoutList,action:'compare',universe:{instrument_type:'SWAP'}}),'compare requires explicit instruments');
fail(validTool('market',{...withoutList,universe:{instrument_type:'SWAP',reference_generation:'generation-a',after_instrument:'BTC-USDT-SWAP'}}),'generation-fenced native market universe wire');
fail(!validTool('market',{...withoutList,universe:{instrument_type:'SWAP',reference_generation:''}}),'empty generation rejected');
const pause=payloadFixtures.find(x=>x.tool==='accounts'&&x.action==='pause')!.positive;
fail(validTool('accounts',{...pause,action:'resume'}),'resume shares strict typed account schema');
fail(!validTool('accounts',{...pause,action:'resume',raw_rpc:'forbidden'}),'resume rejects raw RPC');
const run=payloadFixtures.find(x=>x.tool==='research'&&x.action==='backtest')!.positive;
const {parameters:unusedRunParams,...gridBase}=run;
const gridPayload={...gridBase,action:'grid',variants:[{fast:9,slow:21}]};
fail(validTool('research',gridPayload),'grid shares typed research-run schema');
fail(!validTool('research',{...gridPayload,parameters:{fast:9,slow:21}}),'grid cannot also provide backtest parameters');
fail(!validTool('research',{...run,variants:[{fast:9,slow:21}]}),'backtest cannot also provide grid variants');
fail(!validTool('research',{...gridBase,action:'grid'}),'grid variants are mandatory');
fail(!validTool('research',{...gridPayload,raw_rpc:'forbidden'}),'grid remains strictly typed');
payloadFixtures.push({tool:'research',action:'grid',positive:gridPayload,negative:[{...gridPayload,raw_rpc:'forbidden'}]});
// Public acceptance must not promise unsupported live strategy versions/bounds.
const liveCreate=payloadFixtures.find(x=>x.tool==='strategies'&&x.action==='create')!.positive;
const liveUpdate=payloadFixtures.find(x=>x.tool==='strategies'&&x.action==='update')!.positive;
for(const [action,valid]of [['create',liveCreate],['update',liveUpdate]] as const){
 fail(validTool('strategies',valid),`valid strategy ${action} accepted`);
 fail(!validTool('strategies',{...valid,parameters:{fast:1,slow:1001}}),`strategy ${action} slow upper bound`);
 fail(!validTool('strategies',{...valid,parameters:{fast:1000,slow:1000}}),`strategy ${action} fast upper bound`);
}
fail(!validTool('strategies',{...liveCreate,version:2}),'only live strategy definition v1 supported');
const researchResult=payloadFixtures.find(x=>x.tool==='research'&&x.action==='result')!.positive;
for(const view of ['summary','trades','equity','variants'])
 fail(validTool('research',{...researchResult,view}),`research result view ${view} accepted`);
fail(!validTool('research',{...researchResult,view:'unsupported'}),'research view strictly enumerated');
console.log('live_strategy_v1_and_research_views PASS');
const start=payloadFixtures.find(x=>x.tool==='strategies'&&x.action==='start')!.positive;
fail(validTool('strategies',{...start,action:'stop'}),'strategy stop shares strict schema');
fail(!validTool('strategies',{...start,action:'stop',raw_rpc:'forbidden'}),'strict stop rejects extra');
const accountLimit=payloadFixtures.find(x=>x.tool==='accounts'&&x.action==='set_limits')!.positive;
fail(validTool('accounts',{...accountLimit,limits:{...accountLimit.limits,max_entry_usd:'10'}}),'typed account USD entry cap via existing CAS management');
fail(!validTool('accounts',{...accountLimit,limits:{...accountLimit.limits,max_entry_usd:'0'}}),'zero account entry cap rejected at tool boundary');
fail(!validTool('accounts',{...accountLimit,limits:{...accountLimit.limits,max_entry_usd:'10e0'}}),'exponential account entry cap rejected at tool boundary');
const trade=payloadFixtures.find(x=>x.tool==='trade'&&x.action==='execute')!.positive;
fail(!validTool('trade',{...trade,sizing:{contracts:'1',max_loss_usd:'5'}}),'sizing exactly one');
fail(validTool('trade',{...trade,sizing:{entry_budget_usd:'10'},order:{...trade.order,max_entry_usd:'10'}}),'fee-inclusive entry-budget wire');
fail(!validTool('trade',{...trade,order:{...trade.order,max_entry_usd:'0'}}),'reject zero entry cap');
fail(!validTool('trade',{...trade,order:{...trade.order,max_entry_usd:'10e0'}}),'reject exponent entry cap');
fail(!validTool('trade',{...trade,sizing:{contracts:'1',entry_budget_usd:'10'}}),'reject mixed quantity and money basis');
fail(!validTool('trade',{...trade,expires_in_ms:30001}),'deadline bounded');
fail(!validTool('trade',{...trade,protection:undefined}),'increase needs protection');
fail(!matches({type:'object',properties:{x:{type:'integer'}},required:['x'],additionalProperties:false},{x:1,y:2}),'strict nested');
console.log(`strict_action_fixtures PASS ${payloadFixtures.length} actions`);

for(const action of ['backtest','grid']) {
 const fixture=payloadFixtures.find(x=>x.tool==='research'&&x.action===action)!;
 const two={...fixture.positive,instruments:['BTC-USDT-SWAP','ETH-USDT-SWAP']};
 const tooMany={...two,instruments:Array.from({length:9},(_,i)=>`ASSET${i}-USDT-SWAP`)};
 const duplicate={...two,instruments:['BTC-USDT-SWAP','BTC-USDT-SWAP']};
 fail(validTool('research',JSON.parse(JSON.stringify(two))),'two-asset wire roundtrip');
 for(const bad of [tooMany,duplicate,{...two,instruments:[]}])fail(!validTool('research',bad),'batch instrument bound');
 payloadFixtures.push({tool:'research',action:action+'_batch',positive:two,negative:[tooMany,duplicate,{...two,instruments:[]}]});
}


const capitalAction=payloadFixtures.find(x=>x.tool==='strategies'&&x.action==='capital_assign')!.positive;
for(const invalid of [
 {...capitalAction,amount_usd:'0'},
 {...capitalAction,amount_usd:'-10'},
 {...capitalAction,direction:'override'},
 {...capitalAction,expected_authority_revision:0},
])fail(!validTool('strategies',invalid),'P0.2 invalid capital assignment passed schema');
const strategiesTool=PUBLIC_TOOLS.find((t:any)=>t.name==='strategies') as any;
fail(!!strategiesTool?.inputSchema?.$defs?.positiveDecimal,'P0.2 shared public decimal schema missing');
console.log('strategy_capital_schema_strict_and_tools_factored PASS');

const decisionFixture=payloadFixtures.find(x=>x.tool==='research'&&x.action==='record_decision')!;
const decision=decisionFixture.positive;
for(const status of ['accepted','rejected','deferred'])fail(validTool('research',{...decision,status}),`research annotation ${status}`);
for(const [field,max]of [['name',128],['hypothesis',4096],['reason',2048]]as const){
 const negative=[{...decision,[field]:' '},{...decision,[field]:'x'.repeat(max+1)},{...decision,[field]:'я'.repeat(max)}];
 for(const bad of negative)fail(!validTool('research',bad),`research annotation ${field} bounded`);
 decisionFixture.negative.push(...negative);
}
for(const bad of [{...decision,status:'approved_to_trade'},{...decision,result_id:'a'.repeat(64)},{...decision,expected_decision_id:'wrong'},{...decision,account:'a'}]){
 fail(!validTool('research',bad),'research annotations have strict metadata contract');decisionFixture.negative.push(bad);
}

for(const text of ['\0'.repeat(2000),'"'.repeat(4096),'\\'.repeat(4096)]){
 const bad={...decision,hypothesis:text};fail(!validTool('research',bad),'serialized annotation tuple bounded');decisionFixture.negative.push(bad);
}
fail(validTool('research',{...decision,hypothesis:'я'.repeat(2048),reason:'r'.repeat(1900)}),'maximum UTF-8 annotation respects serialized budget');

function resolvePublicSchema(node:any,defs:Record<string,any>):any{
 if(Array.isArray(node))return node.map(x=>resolvePublicSchema(x,defs));
 if(node&&typeof node==='object'){
  if(node.$ref){const key=String(node.$ref).replace('#/$defs/','');if(!(key in defs))throw Error('unresolved schema ref');return resolvePublicSchema(defs[key],defs);}
  return Object.fromEntries(Object.entries(node).filter(([key])=>key!=='$defs').map(([key,value])=>[key,resolvePublicSchema(value,defs)]));
 }return node;
}
for(const tool of PUBLIC_TOOLS as any[]){
 if(tool.name.startsWith('trade.'))continue;
 const expanded=resolvePublicSchema(tool.inputSchema,tool.inputSchema.$defs??{});
 fail(JSON.stringify(expanded)===JSON.stringify({oneOf:ACTION_SCHEMAS[tool.name]}),`exact public schema factoring ${tool.name}`);
 for(const fixture of payloadFixtures.filter(x=>x.tool===tool.name))for(const input of [fixture.positive,...fixture.negative])
  fail(matches(expanded,input)===matches({oneOf:ACTION_SCHEMAS[tool.name]},input),`factored positive/negative semantics ${tool.name}.${fixture.action}`);
}
