import {isObject,type Json} from './shared.js';
import {normalizeDecimal} from './risk_book.js';
type Schema=Record<string,any>;
const text=(max=128):Schema=>({type:'string',minLength:1,maxLength:max});
const token={...text(),minLength:8,pattern:'^[A-Za-z0-9_-]+$'};
// Count all wire digits without binary floats or implicit rounding.
const money={type:'string',pattern:'^(?!\\d{29})(?!0(?:\\.0+)?$)(?:0|[1-9]\\d*)(?:\\.\\d+)?$',maxLength:29};
const integer=(min=0,max=Number.MAX_SAFE_INTEGER):Schema=>({type:'integer',minimum:min,maximum:max});
// Enum values already constrain JSON types; omit redundant type to keep tools/list bounded.
const choice=(...values:string[]):Schema=>({enum:values});
const array=(items:Schema,max=32):Schema=>({type:'array',items,maxItems:max,uniqueItems:true});
const object=(properties:Schema,required=Object.keys(properties)):Schema=>({type:'object',properties,required,additionalProperties:false});
const action=(name:string,props:Schema={},required=Object.keys(props)):Schema=>object({action:{const:name},...props},['action',...required]);
const account=text();const mutation={idempotency_key:token};
const limits=object({max_modelled_loss_usdt:money,max_order_loss_usdt:money,max_notional_usdt:{anyOf:[money,{type:'null'}]},max_leverage:{anyOf:[money,{type:'null'}]},max_daily_realized_loss_usdt:{anyOf:[money,{type:'null'}]},max_drawdown_ratio:{anyOf:[{type:'string',pattern:'^(?:0(?:\\.[0-9]{1,27})?|1(?:\\.0{1,27})?)$',maxLength:29},{type:'null'}]},allowed_instruments:array(text(64))});
const limitsWithEntry=object({...limits.properties,max_entry_usd:money},limits.required);
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
 accounts:[action('list'),action('status',{account}),action('connect',{...mutation,credential_ref:text(),alias:text(64),label:text(128),limits,enabled:{type:'boolean'}},['idempotency_key','credential_ref','alias','limits','enabled']),pauseResume,action('set_limits',{account,...mutation,expected_revision:integer(),limits:limitsWithEntry})],
 strategies:[action('list',{account}),action('status',{account,...instance}),action('create',{account,...mutation,...instance,definition_id:text(64),version:integer(1,1),parameters:liveStrategyParams,instruments:array(text(64)),timeframe_ms:integer(1000),budget_usd:money}),action('update',{account,...mutation,...instance,expected_revision:integer(),parameters:liveStrategyParams,budget_usd:money}),startStop,action('capital_assign',{account,...mutation,...instance,direction:choice('assign','release'),amount_usd:money,expected_authority_revision:integer(1)},['account','idempotency_key','instance_id','direction','amount_usd','expected_authority_revision'])],
 market:[screenCompare,action('instruments',{asset:text(16),settle_currency:text(16)},['asset']),action('snapshot',{instrument:text(64)}),action('microstructure',{instrument:text(64),size_contracts:money},['instrument']),action('capabilities')],
 research:[researchRun,action('benchmark',{idempotency_key:token}),action('status',{job_id:text()}),action('cancel',{job_id:text(),...mutation}),action('result',{job_id:text(),cursor:text(512),limit:integer(1,50),view:choice('summary','trades','equity','variants','export')},['job_id','limit']),action('capabilities'),action('validate',{...range,range_mode:choice('strict','normalize'),instruments:{...array(text(64),8),minItems:1},strategy_id:choice('ema_cross_v1'),parameters:ema,costs,data_mode:choice('counterfactual','historical_observed')},['instruments','timeframe_ms','start_ms','end_ms','strategy_id','parameters','costs','data_mode'])],
 trade:[action('preview',{account,...trade},['account','operation','strategy_instance','instrument','side','sizing','order']),action('execute',{account,...mutation,...trade,expires_in_ms:integer(1,30000)},['account','idempotency_key','operation','strategy_instance','instrument','side','sizing','order','expires_in_ms']),action('status',{account,reference:text()}),action('cancel',{account,...mutation,reference:text()}),action('close',{account,...mutation,reference:text(),expires_in_ms:integer(1,30000)}),action('capabilities',{account}),action('preflight',{account})],
 portfolio:[portfolioHistory,action('scenario',{equity_usd:money,positions:array(object({instrument:text(64),side:choice('long','short'),notional_usd:money,margin_usd:money,price_shock_bps:integer(-10000,10000),volatility_bps:integer(0,10000),liquidity_cost_bps:integer(0,10000)}),8),correlations:array(object({left:integer(0,7),right:integer(0,7),rho:number(-1,1)}),28)},['equity_usd','positions','correlations'])],
 diagnostics:[action('explain',{reference:text(),account,view:choice('finance','operation','research','storage'),cycle_id:integer(1),limit:integer(1,4)},[]),action('runtime')],
};
for(const schema of ACTION_SCHEMAS.trade){if(['preview','execute'].includes(schema.properties.action.const))schema.allOf=[{if:{properties:{operation:{enum:['open','add','replace']}}},then:{required:['protection']}}];}
ACTION_SCHEMAS.diagnostics[0].allOf=[{anyOf:[{required:['account']},{required:['reference']}]}];
// The MCP tool is the permission/approval boundary. Do not mix preview
// with real order submission under a single model-callable descriptor.
const TOOL_NOTES:Record<string,{description:string;readOnlyHint:boolean;destructiveHint:boolean}>={
 accounts:{description:'Accounts and limits.',readOnlyHint:false,destructiveHint:true},
 strategies:{description:'Strategies; may trade.',readOnlyHint:false,destructiveHint:true},
 market:{description:'Market data.',readOnlyHint:true,destructiveHint:false},
 research:{description:'Research.',readOnlyHint:false,destructiveHint:true},
 portfolio:{description:'Portfolio.',readOnlyHint:true,destructiveHint:false},
 diagnostics:{description:'Diagnostics.',readOnlyHint:true,destructiveHint:false},
};
const TRADE_NOTES:Record<string,{description:string;readOnlyHint:boolean;destructiveHint:boolean}>={
 preview:{description:'Preview; no order.',readOnlyHint:true,destructiveHint:false},
 execute:{description:'Submit a real order; financial loss possible. Requires approval, account limits and STOP.',readOnlyHint:false,destructiveHint:true},
 status:{description:'Trade status.',readOnlyHint:true,destructiveHint:false},
 cancel:{description:'Cancel venue order.',readOnlyHint:false,destructiveHint:true},
 close:{description:'Close via real exchange order.',readOnlyHint:false,destructiveHint:true},
 capabilities:{description:'Trade permissions.',readOnlyHint:true,destructiveHint:false},
 preflight:{description:'Readiness; no order.',readOnlyHint:true,destructiveHint:false},
};
const annotation=(meta:{readOnlyHint:boolean;destructiveHint:boolean})=>({
 readOnlyHint:meta.readOnlyHint,destructiveHint:meta.destructiveHint,openWorldHint:false
});
// Deduplicate only the exact, positive-decimal schema in public tools/list.
// Internal validation retains the fully expanded ACTION_SCHEMAS, including
// all limits; standard JSON Schema $defs keeps external contracts identical.
function publicSchema(schema:Schema):Schema {
 let repeats=0;
 const factor=(node:any):any=>{
  if(Array.isArray(node))return node.map(factor);
  if(node&&typeof node==='object'){
   if(node.type==='string'&&node.pattern===money.pattern&&node.maxLength===money.maxLength&&Object.keys(node).length===3){
    repeats++;return {$ref:'#/$defs/positiveDecimal'};
   }
   return Object.fromEntries(Object.entries(node).map(([k,v])=>[k,factor(v)]));
  }
  return node;
 };
 const normalized=factor(schema);
 return repeats>=2?{...normalized,$defs:{positiveDecimal:money}}:schema;
}
export const PUBLIC_TOOLS:Json[]=[
 ...Object.entries(ACTION_SCHEMAS).filter(([name])=>name!=='trade')
   .map(([name,schemas])=>({name,description:TOOL_NOTES[name].description,
     inputSchema:publicSchema({oneOf:schemas}),annotations:annotation(TOOL_NOTES[name])})),
 ...ACTION_SCHEMAS.trade.map(schema=>{
   const scopedAction=String(schema.properties.action.const);
   const {action:_action,...properties}=schema.properties;
   // The explicit name already determines the action. Never let the model
   // override it through a second caller-controlled action field.
   const inputSchema={...schema,properties,
     required:(schema.required as string[]).filter(k=>k!=='action')};
   return {name:'trade.'+scopedAction,description:TRADE_NOTES[scopedAction].description,
     inputSchema:publicSchema(inputSchema),annotations:annotation(TRADE_NOTES[scopedAction])};
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

// Semantic DTO matches the Native TradeRequest::Execute wire representation,
// not raw MCP field order. Dynamic policy/strategy revisions are NOT request
// identity. Absent optional fields become the existing serde defaults only.
export function canonicalTradeSemantic(accountKey:string, request:unknown):string {
  if(!isObject(request)||request.action!=="execute")
    throw Error("INVALID_TRADE_SEMANTIC_INPUT");
  const sizing=request.sizing;
  const order=request.order;
  if(!isObject(sizing)||!isObject(order))throw Error("INVALID_TRADE_SEMANTIC_INPUT");
  const sizeFields=["contracts","max_loss_usd","entry_budget_usd"]
    .filter(field=>field in sizing);
  if(sizeFields.length!==1)throw Error("INVALID_TRADE_SEMANTIC_INPUT");
  const positive=(value:unknown):string=>{
    const normalized=normalizeDecimal(value);
    if(normalized.canonical===null)throw Error(normalized.error_code);
    return normalized.canonical;
  };
  const basis=sizeFields[0];
  const normalizedOrder:Record<string,unknown>={
    type:order.type,
    limit_price:order.limit_price==null?null:positive(order.limit_price),
    reference:order.reference??null,
    max_slippage_bps:order.max_slippage_bps,
  };
  if(order.max_entry_usd!=null)
    normalizedOrder.max_entry_usd=positive(order.max_entry_usd);
  const inputProtection=request.protection;
  let protection:Record<string,unknown>|null=null;
  if(inputProtection!=null){
    if(!isObject(inputProtection))throw Error("INVALID_TRADE_SEMANTIC_INPUT");
    if("stop_price" in inputProtection)
      protection={stop_price:positive(inputProtection.stop_price)};
    else if("stop_distance_bps" in inputProtection)
      protection={stop_distance_bps:inputProtection.stop_distance_bps};
    else throw Error("INVALID_TRADE_SEMANTIC_INPUT");
  }
  const semanticRequest={
    action:"execute",idempotency_key:request.idempotency_key,
    operation:request.operation,strategy_instance:request.strategy_instance,
    instrument:request.instrument,side:request.side,
    sizing:{[basis]:positive(sizing[basis])},
    order:normalizedOrder,protection,expires_in_ms:request.expires_in_ms,
  };
  const stable=(value:unknown):unknown=>{
    if(Array.isArray(value))return value.map(stable);
    if(isObject(value))return Object.fromEntries(
      Object.entries(value).sort(([left],[right])=>left<right?-1:left>right?1:0)
        .map(([key,child])=>[key,stable(child)])
    );
    return value;
  };
  return JSON.stringify(stable({
    account_key:accountKey,request:semanticRequest,
    schema:"okx.trade.semantic/v1",
  }));
}
export async function tradeSemanticSha256(accountKey:string,request:unknown):Promise<string>{
  const input=new TextEncoder().encode(canonicalTradeSemantic(accountKey,request));
  const digest=await crypto.subtle.digest("SHA-256",input);
  return Array.from(new Uint8Array(digest),v=>v.toString(16).padStart(2,"0")).join("");
}
