import {isObject,type Json} from './shared.js';
type Schema=Record<string,any>;
const text=(max=128):Schema=>({type:'string',minLength:1,maxLength:max});
const token={...text(),minLength:8,pattern:'^[A-Za-z0-9_-]+$'};
const money={type:'string',pattern:'^(?!0+(?:\\.0+)?$)(?:0|[1-9][0-9]{0,14})(?:\\.[0-9]{1,9})?$',maxLength:32};
const integer=(min=0,max=Number.MAX_SAFE_INTEGER):Schema=>({type:'integer',minimum:min,maximum:max});
const choice=(...values:string[]):Schema=>({type:'string',enum:values});
const array=(items:Schema,max=32):Schema=>({type:'array',items,maxItems:max,uniqueItems:true});
const object=(properties:Schema,required=Object.keys(properties)):Schema=>({type:'object',properties,required,additionalProperties:false});
const action=(name:string,props:Schema={},required=Object.keys(props)):Schema=>object({action:{const:name},...props},['action',...required]);
const account=text();const mutation={idempotency_key:token};
const limits=object({max_modelled_loss_usdt:money,max_order_loss_usdt:money,max_notional_usdt:{anyOf:[money,{type:'null'}]},max_leverage:{anyOf:[money,{type:'null'}]},max_daily_realized_loss_usdt:{anyOf:[money,{type:'null'}]},max_drawdown_ratio:{anyOf:[{type:'string',pattern:'^(?:0(?:\\.[0-9]{1,9})?|1(?:\\.0{1,9})?)$',maxLength:11},{type:'null'}]},allowed_instruments:array(text(64))});
const sizing={oneOf:[object({contracts:money}),object({max_loss_usd:money}),object({entry_budget_usd:money})]};
const constraints=object({type:choice('limit','market','post_only','ioc','fok'),limit_price:money,reference:text(),max_entry_usd:money,max_slippage_bps:integer(0,10000)},['type','max_slippage_bps']);
const protection={oneOf:[object({stop_price:money}),object({stop_distance_bps:integer(1,10000)})]};
const trade={operation:choice('open','add','reduce','close','replace'),strategy_instance:text(64),instrument:text(64),side:choice('long','short'),sizing,order:constraints,protection};
const ema=object({fast:integer(1,10000),slow:integer(2,10000)});
// The live Engine supports only StrategyParametersV1 and definition version 1.
// Research grid parameters have a separate contract and retain their bounds.
const liveStrategyParams=object({fast:integer(1,999),slow:integer(2,1000)});
const costs=object({maker_fee_bps:{type:'number',minimum:0,maximum:1000},taker_fee_bps:{type:'number',minimum:0,maximum:1000},slippage_bps:{type:'number',minimum:0,maximum:1000},funding_mode:choice('observed','zero_assumption')});
const range={instruments:array(text(64),100),timeframe_ms:integer(1000,2678400000),start_ms:integer(1),end_ms:integer(1)};
const research={...mutation,...range,instruments:{...array(text(64),8),minItems:1},strategy_id:choice('ema_cross_v1'),costs,data_mode:choice('historical_observed','counterfactual'),seed:integer()};
const instance={instance_id:text(64)};
const number=(minimum=0,maximum=Number.MAX_SAFE_INTEGER)=>({type:'number',minimum,maximum});
const marketParameters=object({ema_period:integer(1,1000),atr_period:integer(1,1000),rsi_period:integer(1,1000),budget:number(0.000000001),price:number(0.000000001),stop_distance:number(0.000000001,0.999999999),notional:number(),fee_bps:number(0,10000),slippage_bps:number(0,10000)},[]);
const universe=object({instrument_type:choice('SPOT','SWAP','FUTURES'),settle_currency:text(16),after_instrument:text(64),reference_generation:text(128),limit:integer(1,100),rank_globally:{type:'boolean'}},['instrument_type']);
const screenCompare=action('screen',{...range,universe,fields:{...array(text(64)),minItems:1},parameters:marketParameters,filters:array(object({field:text(64),min:number(-Number.MAX_SAFE_INTEGER),max:number(-Number.MAX_SAFE_INTEGER)},['field']),16),sort:object({field:text(64),descending:{type:'boolean'}}),limit:integer(1,50),cursor:text(512)},['fields','timeframe_ms','start_ms','end_ms','limit']);
screenCompare.properties.action=choice('screen','compare');
screenCompare.allOf=[{oneOf:[{required:['instruments']},{required:['universe']}]},
 {if:{properties:{action:{const:'compare'}}},then:{required:['instruments']}}];
const portfolioHistory=action('summary',{accounts:array(account),start_ms:integer(0),end_ms:integer(1),strategies:array(text()),currencies:array(text(16)),cursor:text(512),limit:integer(1,50)},['accounts','start_ms','end_ms','limit']);portfolioHistory.properties.action=choice('summary','history');
const startStop=action('start',{account,...mutation,...instance});startStop.properties.action=choice('start','stop');
// These pairs differ only by action; share strict schemas, not public semantics.
const pauseResume=action('pause',{account,...mutation,expected_revision:integer()});pauseResume.properties.action=choice('pause','resume');
const researchRun=action('backtest',{...research,parameters:ema,variants:array(ema,64)},Object.keys(research).filter(k=>k!=='seed'));
researchRun.properties.action=choice('backtest','grid');
researchRun.allOf=[{oneOf:[{required:['parameters']},{required:['variants']}]},
 {if:{properties:{action:{const:'backtest'}}},then:{required:['parameters']}},
 {if:{properties:{action:{const:'grid'}}},then:{required:['variants']}}];
export const ACTION_SCHEMAS:Record<string,Schema[]>={
 accounts:[action('list'),action('status',{account}),action('connect',{...mutation,credential_ref:text(),alias:text(64),label:text(128),limits,enabled:{type:'boolean'}},['idempotency_key','credential_ref','alias','limits','enabled']),pauseResume,action('set_limits',{account,...mutation,expected_revision:integer(),limits})],
 strategies:[action('list',{account}),action('status',{account,...instance}),action('create',{account,...mutation,...instance,definition_id:text(64),version:integer(1,1),parameters:liveStrategyParams,instruments:array(text(64)),timeframe_ms:integer(1000),budget_usd:money}),action('update',{account,...mutation,...instance,expected_revision:integer(),parameters:liveStrategyParams,budget_usd:money}),startStop],
 market:[screenCompare,action('instruments',{asset:text(16),settle_currency:text(16)},['asset']),action('snapshot',{instrument:text(64)}),action('microstructure',{instrument:text(64),size_contracts:money},['instrument']),action('capabilities')],
 research:[researchRun,action('status',{job_id:text()}),action('cancel',{job_id:text(),...mutation}),action('result',{job_id:text(),cursor:text(512),limit:integer(1,50),view:choice('summary','trades','equity','variants')},['job_id','limit']),action('capabilities'),action('validate',{...range,instruments:{...array(text(64),8),minItems:1},strategy_id:choice('ema_cross_v1'),parameters:ema,costs,data_mode:choice('counterfactual','historical_observed')},['instruments','timeframe_ms','start_ms','end_ms','strategy_id','parameters','costs','data_mode'])],
 trade:[action('preview',{account,...trade},['account','operation','strategy_instance','instrument','side','sizing','order']),action('execute',{account,...mutation,...trade,expires_in_ms:integer(1,30000)},['account','idempotency_key','operation','strategy_instance','instrument','side','sizing','order','expires_in_ms']),action('status',{account,reference:text()}),action('cancel',{account,...mutation,reference:text()}),action('close',{account,...mutation,reference:text(),expires_in_ms:integer(1,30000)}),action('capabilities',{account}),action('preflight',{account})],
 portfolio:[portfolioHistory,action('scenario',{equity_usd:money,positions:array(object({instrument:text(64),side:choice('long','short'),notional_usd:money,margin_usd:money,price_shock_bps:integer(-10000,10000),volatility_bps:integer(0,10000),liquidity_cost_bps:integer(0,10000)}),8),correlations:array(object({left:integer(0,7),right:integer(0,7),rho:number(-1,1)}),28)},['equity_usd','positions','correlations'])],
 diagnostics:[action('explain',{reference:text(),account},[]),action('runtime')],
};
for(const schema of ACTION_SCHEMAS.trade){if(['preview','execute'].includes(schema.properties.action.const))schema.allOf=[{if:{properties:{operation:{enum:['open','add','replace']}}},then:{required:['protection']}}];}
ACTION_SCHEMAS.diagnostics[0].allOf=[{anyOf:[{required:['account']},{required:['reference']}]}];
// The MCP tool is the permission/approval boundary. Do not mix preview
// with real order submission under a single model-callable descriptor.
const TOOL_NOTES:Record<string,{description:string;readOnlyHint:boolean;destructiveHint:boolean}>={
 accounts:{description:'Manage accounts and limits.',readOnlyHint:false,destructiveHint:true},
 strategies:{description:'Manage strategies; may trade.',readOnlyHint:false,destructiveHint:true},
 market:{description:'Read OKX market data.',readOnlyHint:true,destructiveHint:false},
 research:{description:'Manage research jobs.',readOnlyHint:false,destructiveHint:true},
 portfolio:{description:'Read portfolio scenarios.',readOnlyHint:true,destructiveHint:false},
 diagnostics:{description:'Read runtime diagnostics.',readOnlyHint:true,destructiveHint:false},
};
const TRADE_NOTES:Record<string,{description:string;readOnlyHint:boolean;destructiveHint:boolean}>={
 preview:{description:'Preview a trade; no order.',readOnlyHint:true,destructiveHint:false},
 execute:{description:'Submit a real order; financial loss possible. Requires approval, account limits and STOP.',readOnlyHint:false,destructiveHint:true},
 status:{description:'Read trade status.',readOnlyHint:true,destructiveHint:false},
 cancel:{description:'Cancel an exchange order.',readOnlyHint:false,destructiveHint:true},
 close:{description:'Close a position by real exchange order.',readOnlyHint:false,destructiveHint:true},
 capabilities:{description:'Read trade permissions.',readOnlyHint:true,destructiveHint:false},
 preflight:{description:'Check readiness; no order.',readOnlyHint:true,destructiveHint:false},
};
const annotation=(meta:{readOnlyHint:boolean;destructiveHint:boolean})=>({
 readOnlyHint:meta.readOnlyHint,destructiveHint:meta.destructiveHint,openWorldHint:false
});
export const PUBLIC_TOOLS:Json[]=[
 ...Object.entries(ACTION_SCHEMAS).filter(([name])=>name!=='trade')
   .map(([name,schemas])=>({name,description:TOOL_NOTES[name].description,
     inputSchema:{oneOf:schemas},annotations:annotation(TOOL_NOTES[name])})),
 ...ACTION_SCHEMAS.trade.map(schema=>{
   const scopedAction=String(schema.properties.action.const);
   const {action:_action,...properties}=schema.properties;
   // The explicit name already determines the action. Never let the model
   // override it through a second caller-controlled action field.
   const inputSchema={...schema,properties,
     required:(schema.required as string[]).filter(k=>k!=='action')};
   return {name:'trade.'+scopedAction,description:TRADE_NOTES[scopedAction].description,
     inputSchema,annotations:annotation(TRADE_NOTES[scopedAction])};
 })
];
export function matches(s:Schema,v:unknown):boolean {
 if(s.allOf&&!s.allOf.every((x:Schema)=>matches(x,v)))return false;
 if(s.if&&matches(s.if,v)&&s.then&&!matches(s.then,v))return false;
 if(s.required&&(!isObject(v)||!s.required.every((k:string)=>k in v)))return false;
 if(!s.type&&s.properties&&isObject(v)&&!Object.entries(s.properties).every(([k,x])=>!(k in v)||matches(x as Schema,v[k])))return false;
 if(s.oneOf)return s.oneOf.filter((x:Schema)=>matches(x,v)).length===1;
 if(s.anyOf)return s.anyOf.some((x:Schema)=>matches(x,v));
 if('const'in s&&v!==s.const)return false;
 if(s.enum&&!s.enum.includes(v))return false;
 if(s.type==='null')return v===null;
 if(s.type==='object')return isObject(v)&&Object.keys(v).every(k=>k in s.properties)&&s.required.every((k:string)=>k in v)&&Object.entries(v).every(([k,x])=>matches(s.properties[k],x));
 if(s.type==='array')return Array.isArray(v)&&v.length>=(s.minItems??0)&&v.length<=(s.maxItems??100)&&(!s.uniqueItems||new Set(v.map(x=>JSON.stringify(x))).size===v.length)&&v.every(x=>matches(s.items,x));
 if(s.type==='string')return typeof v==='string'&&v.length>=(s.minLength??0)&&v.length<=(s.maxLength??Infinity)&&(!s.pattern||new RegExp(s.pattern).test(v));
 if(s.type==='integer'||s.type==='number')return typeof v==='number'&&Number.isFinite(v)&&(s.type!=='integer'||Number.isSafeInteger(v))&&v>=(s.minimum??-Infinity)&&v<=(s.maximum??Infinity);
 if(s.type==='boolean')return typeof v==='boolean';return true;
}
export function validTool(name:string,args:unknown):boolean {if(name==='trade'&&isObject(args)&&['preview','execute'].includes(String(args.action))&&['open','add','replace'].includes(String(args.operation))&&!args.protection)return false;return !!ACTION_SCHEMAS[name]&&matches({oneOf:ACTION_SCHEMAS[name]},args)&&!(isObject(args)&&typeof args.start_ms==='number'&&typeof args.end_ms==='number'&&args.end_ms<=args.start_ms);}
export function isExecutionMutation(type:unknown,action?:unknown):boolean{return type==='connect_account'||(type==='trade'&&action!=='preview'&&action!=='status')||(type==='strategies'&&!['list','status'].includes(String(action)))||(type==='research'&&['backtest','grid','cancel'].includes(String(action)));}
export function executionProfilePermitsOperation(profile:unknown,_type:unknown):boolean{return profile==='engine';}

