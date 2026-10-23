import {ACTION_SCHEMAS,PUBLIC_TOOLS,validTool,matches} from './execution_tools.js';
const fail=(v:boolean,n:string)=>{if(!v)throw Error(n);};
function sample(s:any):any{if('const'in s)return s.const;if(s.enum)return s.enum[0];if(s.oneOf)return sample(s.oneOf[0]);if(s.anyOf)return sample(s.anyOf[0]);if(s.type==='object')return Object.fromEntries(Object.entries(s.properties).filter(([k])=>s.required.includes(k)).map(([k,x])=>[k,sample(x)]));if(s.type==='array')return [sample(s.items)];if(s.type==='string')return s.pattern?.includes('0-9]{1,9}')?'1':s.minLength===8?'command_0001':'fixture';if(s.type==='integer'||s.type==='number')return s.minimum??1;if(s.type==='boolean')return true;return null;}
export const payloadFixtures:{tool:string;action:string;positive:any;negative:any[]}[]=[];
for(const [tool,schemas]of Object.entries(ACTION_SCHEMAS))for(const schema of schemas){const good=sample(schema);if('start_ms'in good)good.end_ms=good.start_ms+100000;if(tool==='trade'&&['execute','preview'].includes(good.action))good.protection={stop_distance_bps:50};if(tool==='diagnostics'&&good.action==='explain')good.reference='intent_fixture';if(good.order)good.order.type='fok';fail(validTool(tool,good),`positive ${tool}.${good.action}`);const badExtra={...good,raw_rpc:'forbidden'},badRequired={...good};delete badRequired.action;const badType={...good,action:12};for(const bad of [badExtra,badRequired,badType])fail(!validTool(tool,bad),`negative ${tool}.${good.action}`);payloadFixtures.push({tool,action:good.action,positive:good,negative:[badExtra,badRequired,badType]});}
const screen=payloadFixtures.find(x=>x.tool==='market'&&x.action==='screen')!.positive;
fail(validTool('market',{...screen,action:'compare'}),'shared strict screen/compare schema');
fail(!validTool('market',{...screen,action:'compare',raw_rpc:'forbidden'}),'compare rejects unknown fields');
const summary=payloadFixtures.find(x=>x.tool==='portfolio'&&x.action==='summary')!.positive;
fail(validTool('portfolio',{...summary,action:'history'}),'shared portfolio history schema');
fail(!validTool('portfolio',{...summary,action:'history',raw_rpc:'forbidden'}),'strict history schema');
const trade=payloadFixtures.find(x=>x.tool==='trade'&&x.action==='execute')!.positive;
fail(!validTool('trade',{...trade,sizing:{contracts:'1',max_loss_usd:'5'}}),'sizing exactly one');
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
