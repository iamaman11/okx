import {ACTION_SCHEMAS,PUBLIC_TOOLS,validTool,matches} from './execution_tools.js';
const fail=(v:boolean,n:string)=>{if(!v)throw Error(n);};
function sample(s:any):any{if('const'in s)return s.const;if(s.enum)return s.enum[0];if(s.oneOf)return sample(s.oneOf[0]);if(s.anyOf)return sample(s.anyOf[0]);if(s.type==='object')return Object.fromEntries(Object.entries(s.properties).filter(([k])=>s.required.includes(k)).map(([k,x])=>[k,sample(x)]));if(s.type==='array')return [sample(s.items)];if(s.type==='string')return (s.pattern?.includes('0-9]{1,9}')||s.pattern?.includes('\\d{1,9}'))?'1':s.minLength===8?'command_0001':'fixture';if(s.type==='integer'||s.type==='number')return s.minimum??1;if(s.type==='boolean')return true;return null;}
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
