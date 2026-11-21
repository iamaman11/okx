import {isObject,type Json} from './shared.js';
import {normalizeDecimal} from './decimal.js';
type Schema=Record<string,any>;
const text=(max=128):Schema=>({type:'string',minLength:1,maxLength:max});
const artifactId={type:'string',pattern:'^sha256:[0-9a-f]{64}$'};
const decisionJob={type:'string',pattern:'^[0-9a-f]{64}$'};
const annotationText=(max:number):Schema=>({...text(max),pattern:'\\S'});
const token={...text(),minLength:8,pattern:'^[A-Za-z0-9_-]+$'};
// Count all wire digits without binary floats or implicit rounding.
const money={type:'string',pattern:'^(?!\\d{29})(?!0(?:\\.0+)?$)(?:0|[1-9]\\d*)(?:\\.\\d+)?$',maxLength:29};
const integer=(min=0,max=Number.MAX_SAFE_INTEGER):Schema=>({type:'integer',minimum:min,maximum:max});
// Enum values already constrain JSON types; omit redundant type to keep tools/list bounded.
const choice=(...values:string[]):Schema=>({enum:values});
const array=(items:Schema,max=32):Schema=>({type:'array',items,maxItems:max,uniqueItems:true});
const object=(properties:Schema,required=Object.keys(properties)):Schema=>({type:'object',properties,required,additionalProperties:false});
// Every multi-action tool is a oneOf of >=2 branches and every branch requires
// its action discriminator. Omitting a redundant branch-level `type:object`
// preserves the union's accepted values (including rejection of non-objects)
// while keeping required and additionalProperties strict. Standalone trade
// tools restore the explicit object type below.
const action=(name:string,props:Schema={},required=Object.keys(props)):Schema=>{
 const {type:_type,...strict}=object({action:{const:name},...props},['action',...required]);
 return strict;
};
const account=text();const mutation={idempotency_key:token};
const limits=object({max_modelled_loss_usdt:money,max_order_loss_usdt:money,max_notional_usdt:{anyOf:[money,{type:'null'}]},max_leverage:{anyOf:[money,{type:'null'}]},max_daily_realized_loss_usdt:{anyOf:[money,{type:'null'}]},max_drawdown_ratio:{anyOf:[{type:'string',pattern:'^(?:0(?:\\.[0-9]{1,27})?|1(?:\\.0{1,27})?)$',maxLength:29},{type:'null'}]},allowed_instruments:array(text(64))});
const limitsWithEntry=object({...limits.properties,max_entry_usd:money},limits.required);
const sizing={oneOf:[object({contracts:money}),object({max_loss_usd:money}),object({entry_budget_usd:money})]};
const constraints=object({type:choice('limit','market','post_only','ioc','fok'),limit_price:money,reference:text(),max_entry_usd:money,max_slippage_bps:integer(0,10000)},['type','max_slippage_bps']);
const protection={oneOf:[object({stop_price:money}),object({stop_distance_bps:integer(1,10000)})]};
const trade={operation:choice('open','add','reduce','close','replace'),strategy_instance:text(64),instrument:text(64),side:choice('long','short'),sizing,order:constraints,protection};
// The live Engine supports only StrategyParametersV1 and definition version 1.
// Research shares the same supported EMA period bounds.
const liveStrategyParams=object({fast:integer(1,999),slow:integer(2,1000)});
const ema=liveStrategyParams;
const researchParameters={oneOf:[ema,object({lookback:integer(2,1000)})]};
const costs=object({maker_fee_bps:{type:'number',minimum:0,maximum:1000},taker_fee_bps:{type:'number',minimum:0,maximum:1000},slippage_bps:{type:'number',minimum:0,maximum:1000},funding_mode:choice('observed','zero_assumption')});
const range={instruments:array(text(64),100),timeframe_ms:integer(1000,2678400000),start_ms:integer(1),end_ms:integer(1)};
const researchStrategies=choice('ema_cross_v1','ema_momentum_confirm_v1','close_breakout_v1');
const research={...mutation,...range,instruments:{...array(text(64),8),minItems:1},strategy_id:researchStrategies,costs,data_mode:choice('historical_observed','counterfactual'),seed:integer()};
const instance={instance_id:text(64)};
const number=(minimum=0,maximum=Number.MAX_SAFE_INTEGER)=>({type:'number',minimum,maximum});
export const MARKET_OPERATORS=["returns","log_returns","realized_volatility","downside_volatility","max_drawdown","pearson_correlation","ema","rolling_mean","atr","rsi","spread_bps","volume_change","oi_change","funding_sum","position_size","cost_estimate"] as const;
// The evolving operator catalog is exposed on demand by market.capabilities.
const MARKET_OPERATOR_SET:ReadonlySet<string>=new Set(MARKET_OPERATORS);
const knownMarketOperator=(value:unknown):boolean=>
 typeof value==='string'&&MARKET_OPERATOR_SET.has(value);
const marketParameters=object({ema_period:integer(1,1000),atr_period:integer(1,1000),rsi_period:integer(1,1000),budget:number(0.000000001),price:number(0.000000001),stop_distance:number(0.000000001,0.999999999),notional:number(),fee_bps:number(0,10000),slippage_bps:number(0,10000)},[]);
const stepId={type:'string',pattern:'^[a-z0-9_]{1,32}$'};
const compositionStep=object({id:stepId,op:choice('returns_series','rolling_mean','rolling_max','rolling_min','rolling_range','rolling_pearson','rolling_zscore','rolling_stddev','add_series','subtract_series','divide_series','and','or','lag','greater_than','greater_than_series','less_than','less_than_series','rising_edge','falling_edge','forward_return_label'),input:stepId,other:stepId,period:integer(1,1000),threshold:{type:'number'},when:stepId},['id','op','input']);
const composition=object({version:choice('okx.market.composition/v1'),steps:{type:'array',items:compositionStep,minItems:1,maxItems:32},result_steps:{type:'array',items:stepId,minItems:1,maxItems:14}});
// Open mathematical IDs belong to Research. This schema constrains only
// transport shape and size; it is not a second mathematical operator catalog.
const graphInputMap={type:'object',propertyNames:stepId,additionalProperties:stepId,minProperties:1,maxProperties:4};
const graphParameterMap={type:'object',propertyNames:stepId,additionalProperties:{anyOf:[{type:'null'},{type:'boolean'},{type:'number'},text(128)]},maxProperties:8};
const graphStep=object({id:stepId,op:stepId,op_version:integer(1,65535),inputs:graphInputMap,parameters:graphParameterMap},['id','op','op_version','inputs']);
const graph=object({version:choice('okx.graph/v1'),steps:{type:'array',items:graphStep,minItems:1,maxItems:32},result_steps:{type:'array',items:stepId,minItems:1,maxItems:14}});
const universe=object({instrument_type:choice('SPOT','SWAP','FUTURES'),settle_currency:text(16),after_instrument:text(64),reference_generation:text(128),limit:integer(1,100),rank_globally:{type:'boolean'}},['instrument_type']);
// The strict graph structure is validated in validGraphWire on every call.
// Keep the model-visible tool compact; mathematical descriptors are on-demand.
const screenCompare=action('screen',{...range,universe,fields:{...array(text(64),14),minItems:1},composition,graph:{type:'object'},parameters:marketParameters,filters:array(object({field:text(64),min:number(-Number.MAX_SAFE_INTEGER),max:number(-Number.MAX_SAFE_INTEGER)},['field']),16),sort:object({field:text(64),descending:{type:'boolean'}},['field']),limit:integer(1,50),cursor:text(512)},['fields','timeframe_ms','start_ms','end_ms','limit']);
screenCompare.properties.action=choice('screen','compare');
screenCompare.allOf=[{oneOf:[{required:['instruments']},{required:['universe']}]},
 {if:{properties:{action:{const:'compare'}}},then:{required:['instruments']}}];
const portfolioHistory=action('summary',{accounts:array(account),start_ms:integer(0),end_ms:integer(1),strategies:array(text()),currencies:array(text(16)),cursor:text(512),limit:integer(1,50)},['accounts','start_ms','end_ms','limit']);portfolioHistory.properties.action=choice('summary','history');
const startStop=action('start',{account,...mutation,...instance});startStop.properties.action=choice('start','stop');
// These pairs differ only by action; share strict schemas, not public semantics.
const pauseResume=action('pause',{account,...mutation,expected_revision:integer()});pauseResume.properties.action=choice('pause','resume');
const researchRun=action('backtest',{...research,parameters:researchParameters,variants:array(researchParameters,64)},Object.keys(research).filter(k=>k!=='seed'));
researchRun.properties.action=choice('backtest','grid');
researchRun.allOf=[{oneOf:[{required:['parameters']},{required:['variants']}]},
 {if:{properties:{action:{const:'backtest'}}},then:{required:['parameters']}},
 {if:{properties:{action:{const:'grid'}}},then:{required:['variants']}}];
const exactSigned={type:'string',pattern:'^-?(?:0|[1-9]\\d*)(?:\\.\\d+)?$',maxLength:29};
const economicAmount=object({currency:text(16),amount:exactSigned});
const economicCase=object({instrument:text(64),product:choice('spot','linear_swap','inverse_swap','option'),
 side:choice('long','short'),quantity:money,entry_price:money,exit_price:money,
 contract_type:choice('linear','inverse'),contract_value:money,
 contract_value_currency:text(16),settle_currency:text(16),
 fees:economicAmount,funding:economicAmount},
 ['instrument','product','side','quantity','entry_price','exit_price','fees','funding']);
export const ACTION_SCHEMAS:Record<string,Schema[]>={
 accounts:[action('list'),action('status',{account}),action('connect',{...mutation,credential_ref:text(),alias:text(64),label:text(128),limits,enabled:{type:'boolean'}},['idempotency_key','credential_ref','alias','limits','enabled']),pauseResume,action('set_limits',{account,...mutation,expected_revision:integer(),limits:limitsWithEntry})],
 strategies:[action('list',{account}),action('status',{account,...instance}),action('create',{account,...mutation,...instance,definition_id:text(64),version:integer(1,1),parameters:liveStrategyParams,instruments:array(text(64)),timeframe_ms:integer(1000),budget_usd:money}),action('update',{account,...mutation,...instance,expected_revision:integer(),parameters:liveStrategyParams,budget_usd:money}),startStop,action('capital_assign',{account,...mutation,...instance,direction:choice('assign','release'),amount_usd:money,expected_authority_revision:integer(1)},['account','idempotency_key','instance_id','direction','amount_usd','expected_authority_revision'])],
 market:[screenCompare,action('instruments',{asset:text(16),settle_currency:text(16)},['asset']),action('snapshot',{instrument:text(64)}),action('microstructure',{instrument:text(64),size_contracts:money},['instrument']),action('capabilities')],
 research:[action('record_decision',{job_id:decisionJob,result_id:artifactId,expected_decision_id:artifactId,name:annotationText(128),hypothesis:annotationText(4096),status:choice('accepted','rejected','deferred'),reason:annotationText(2048)},['job_id','result_id','name','hypothesis','status','reason']),action('decision',{job_id:decisionJob,decision_id:artifactId},['job_id']),researchRun,action('benchmark',{idempotency_key:token}),action('status',{job_id:text()}),action('cancel',{job_id:text(),...mutation}),action('result',{job_id:text(),cursor:text(512),limit:integer(1,50),view:choice('summary','trades','equity','variants','export')},['job_id','limit']),action('capabilities'),action('economics',{cases:{type:'array',items:economicCase,minItems:1,maxItems:8},data_mode:choice('counterfactual','historical_observed')}),action('validate',{...range,range_mode:choice('strict','normalize'),instruments:{...array(text(64),8),minItems:1},strategy_id:researchStrategies,parameters:researchParameters,costs,data_mode:choice('counterfactual','historical_observed')},['instruments','timeframe_ms','start_ms','end_ms','strategy_id','parameters','costs','data_mode'])],
 trade:[action('preview',{account,...trade},['account','operation','strategy_instance','instrument','side','sizing','order']),action('execute',{account,...mutation,...trade,expires_in_ms:integer(1,30000)},['account','idempotency_key','operation','strategy_instance','instrument','side','sizing','order','expires_in_ms']),action('status',{account,reference:text()}),action('cancel',{account,...mutation,reference:text()}),action('close',{account,...mutation,reference:text(),expires_in_ms:integer(1,30000)}),action('capabilities',{account}),action('preflight',{account})],
 portfolio:[portfolioHistory,action('scenario',{equity_usd:money,positions:array(object({instrument:text(64),side:choice('long','short'),notional_usd:money,margin_usd:money,price_shock_bps:integer(-10000,10000),volatility_bps:integer(0,10000),liquidity_cost_bps:integer(0,10000)}),8),correlations:array(object({left:integer(0,7),right:integer(0,7),rho:number(-1,1)}),28)},['equity_usd','positions','correlations'])],
 diagnostics:[action('explain',{reference:text(),account,view:choice('finance','operation','research','storage'),cycle_id:integer(1),limit:integer(1,4)},[]),action('runtime')],
};
for(const schema of ACTION_SCHEMAS.trade){if(['preview','execute'].includes(schema.properties.action.const))schema.allOf=[{if:{properties:{operation:{enum:['open','add','replace']}}},then:{required:['protection']}}];}
ACTION_SCHEMAS.diagnostics[0].allOf=[{anyOf:[{required:['account']},{required:['reference']}]}];
// The MCP tool is the permission/approval boundary. Do not mix preview
// with real order submission under a single model-callable descriptor.
const TOOL_NOTES:Record<string,{description:string;readOnlyHint:boolean;destructiveHint:boolean}>={
 accounts:{description:'Accounts.',readOnlyHint:false,destructiveHint:true},
 strategies:{description:'Strategies; may trade.',readOnlyHint:false,destructiveHint:true},
 market:{description:'Market.',readOnlyHint:true,destructiveHint:false},
 research:{description:'Research.',readOnlyHint:false,destructiveHint:true},
 portfolio:{description:'Portfolio.',readOnlyHint:true,destructiveHint:false},
 diagnostics:{description:'Diagnostics.',readOnlyHint:true,destructiveHint:false},
};
const TRADE_NOTES:Record<string,{description:string;readOnlyHint:boolean;destructiveHint:boolean}>={
 preview:{description:'Preview; no order.',readOnlyHint:true,destructiveHint:false},
 execute:{description:'Real order; loss possible. Approval, limits, STOP required.',readOnlyHint:false,destructiveHint:true},
 status:{description:'Status.',readOnlyHint:true,destructiveHint:false},
 cancel:{description:'Cancel order.',readOnlyHint:false,destructiveHint:true},
 close:{description:'Real close order.',readOnlyHint:false,destructiveHint:true},
 capabilities:{description:'Rights.',readOnlyHint:true,destructiveHint:false},
 preflight:{description:'Readiness.',readOnlyHint:true,destructiveHint:false},
};
const annotation=(meta:{readOnlyHint:boolean;destructiveHint:boolean})=>({
 readOnlyHint:meta.readOnlyHint,destructiveHint:meta.destructiveHint,openWorldHint:false
});
// Every public tool states all three risk hints, including the multi-action
// tools. Financial hosts must not infer a permissive open-world default.
// Factor identical bounded scalar schemas only; internal validation keeps every
// expanded constraint. Standard JSON Schema references preserve tools/list bounds.
function publicSchema(schema:Schema):Schema {
const definitions:Record<string,Schema>={d:money,t:text(),s:text(64),b:text(16),a:artifactId,j:decisionJob,k:token,i:integer(),n:integer(1),p:integer(1,50),y:integer(1,1000),r:researchStrategies,q:researchParameters,x:stepId,c:costs,e:economicAmount,h:liveStrategyParams,u:{...array(text(64),8),minItems:1},v:choice('counterfactual','historical_observed'),w:array(account),l:{type:'string',pattern:'^link_[a-f0-9]{32}$'},z:{anyOf:[money,{type:'null'}]}};
 const counts:Record<string,number>={};
 const key=(node:any):string|undefined=>Object.keys(definitions).find(k=>JSON.stringify(node)===JSON.stringify(definitions[k]));
 const count=(node:any):void=>{
  if(Array.isArray(node)){node.forEach(count);return;}
  if(node&&typeof node==='object'){const k=key(node);if(k){counts[k]=(counts[k]??0)+1;return;}Object.values(node).forEach(count);}
 };
 count(schema);
 const used=Object.fromEntries(Object.entries(definitions).filter(([k])=>(counts[k]??0)>=2));
 const factor=(node:any):any=>{
  if(Array.isArray(node))return node.map(factor);
  if(node&&typeof node==='object'){const k=key(node);if(k&&k in used)return {$ref:'#/$defs/'+k};return Object.fromEntries(Object.entries(node).map(([k,v])=>[k,factor(v)]));}
  return node;
 };
 // Definitions can reuse other definitions too (e.g. the nullable amount
 // contains the same bounded decimal as the non-null amount). Preserve the
 // exact constraints without duplicating wire bytes inside $defs.
 const nested=(node:any):any=>Array.isArray(node)?node.map(factor):
   node&&typeof node==='object'?Object.fromEntries(Object.entries(node).map(([k,v])=>[k,factor(v)])):node;
 const shared=Object.fromEntries(Object.entries(used).map(([k,v])=>[k,nested(v)]));
 return Object.keys(used).length?{...factor(schema),$defs:shared}:schema;
}

// Multi-action accounts is an object tool with a strict server-side oneOf.
// Unlike a top-level bare oneOf, an object root lets linked MCP clients add
// their host-managed account selector (link_id) without changing action DTOs.
// The host consumes that selector; validTool still rejects any extra wire keys.
function publicAccountsSchema():Schema {
 const variants=ACTION_SCHEMAS.accounts;
 // One typed field dictionary and strict action-specific allowed keys.
 // The linked-app host consumes link_id; native validTool still rejects it.
 const linkSelector={type:'string',pattern:'^link_[a-f0-9]{32}$'};
 const properties=Object.assign({},...variants.map(s=>s.properties));
 properties.action=choice('list','status','connect','pause','resume','set_limits');
 properties.link_id=linkSelector;
 return publicSchema({
   type:'object',properties,required:['action'],additionalProperties:false,
   oneOf:variants.map(s=>({
     properties:{
       ...Object.fromEntries(Object.keys(s.properties)
         .map(key=>[key,key==='action'?s.properties.action:{}])),
       link_id:linkSelector,
     },
     required:s.required,additionalProperties:false
   }))
 });
}
export const PUBLIC_TOOLS:Json[]=[
 ...Object.entries(ACTION_SCHEMAS).filter(([name])=>name!=='trade')
   .map(([name,schemas])=>({name,description:TOOL_NOTES[name].description,
     inputSchema:name==='accounts'?publicAccountsSchema():publicSchema({oneOf:schemas}),
     annotations:annotation(TOOL_NOTES[name])})),
 ...ACTION_SCHEMAS.trade.map(schema=>{
   const scopedAction=String(schema.properties.action.const);
   const {action:_action,...properties}=schema.properties;
   // The explicit name already determines the action. Never let the model
   // override it through a second caller-controlled action field.
   const inputSchema={...schema,type:'object',properties,
     required:(schema.required as string[]).filter(k=>k!=='action')};
   return {name:'trade.'+scopedAction,description:TRADE_NOTES[scopedAction].description,
     inputSchema:publicSchema(inputSchema),annotations:annotation(TRADE_NOTES[scopedAction])};
 })
];
export function matches(s:Schema,v:unknown):boolean {
 if(s.allOf&&!s.allOf.every((x:Schema)=>matches(x,v)))return false;
 if(s.if&&matches(s.if,v)&&s.then&&!matches(s.then,v))return false;
 if(s.required&&(!isObject(v)||!s.required.every((k:string)=>k in v)))return false;
 if(!s.type&&s.properties&&isObject(v)&&((s.additionalProperties===false&&Object.keys(v).some(k=>!(k in s.properties)))||!Object.entries(s.properties).every(([k,x])=>!(k in v)||matches(x as Schema,v[k]))))return false;
 if(s.oneOf)return s.oneOf.filter((x:Schema)=>matches(x,v)).length===1;
 if(s.anyOf)return s.anyOf.some((x:Schema)=>matches(x,v));
 if('const'in s&&v!==s.const)return false;
 if(s.enum&&!s.enum.includes(v))return false;
 if(s.type==='null')return v===null;
 if(s.type==='object')return isObject(v)&&Object.keys(v).length>=(s.minProperties??0)&&Object.keys(v).length<=(s.maxProperties??Infinity)&&(s.required??[]).every((k:string)=>k in v)&&Object.entries(v).every(([k,x])=>
   (!s.propertyNames||matches(s.propertyNames,k))&&(s.properties&&k in s.properties?matches(s.properties[k],x):s.additionalProperties===false?false:typeof s.additionalProperties==='object'?matches(s.additionalProperties,x):true));
 if(s.type==='array')return Array.isArray(v)&&v.length>=(s.minItems??0)&&v.length<=(s.maxItems??100)&&(!s.uniqueItems||new Set(v.map(x=>JSON.stringify(x))).size===v.length)&&v.every(x=>matches(s.items,x));
 if(s.type==='string')return typeof v==='string'&&v.length>=(s.minLength??0)&&v.length<=(s.maxLength??Infinity)&&(!s.pattern||new RegExp(s.pattern).test(v));
 if(s.type==='integer'||s.type==='number')return typeof v==='number'&&Number.isFinite(v)&&(s.type!=='integer'||Number.isSafeInteger(v))&&v>=(s.minimum??-Infinity)&&v<=(s.maximum??Infinity);
 if(s.type==='boolean')return typeof v==='boolean';return true;
}
function validMarketOperatorRefs(args:Record<string,unknown>):boolean {
 if(args.action!=='screen'&&args.action!=='compare')return true;
 if(!Array.isArray(args.fields)||!args.fields.every(knownMarketOperator))return false;
 if(Array.isArray(args.filters)&&args.filters.some((filter:unknown)=>
    !isObject(filter)||!knownMarketOperator(filter.field)))return false;
 if(isObject(args.sort)&&!knownMarketOperator(args.sort.field))return false;
 if(isObject(args.composition)&&Array.isArray(args.composition.steps)
   &&args.composition.steps.some((step:unknown)=>isObject(step)&&
     (['open_interest','funding_rate'].includes(String(step.input))||step.other==='funding_rate'))){
   if(Array.isArray(args.instruments)&&args.instruments.some((id:unknown)=>
     typeof id!=='string'||!id.endsWith('-SWAP')))return false;
   if(isObject(args.universe)&&args.universe.instrument_type!=='SWAP')return false;
 }
 return true;
}
function validCompositionStep(s:unknown):boolean {
 if(!isObject(s))return false;
 if(['add_series','subtract_series','divide_series','rolling_pearson','greater_than_series','less_than_series','and','or'].includes(String(s.op))){
  if(typeof s.other!=='string')return false;
 }else if('other'in s)return false;
 const hasPeriod=['rolling_mean','rolling_max','rolling_min','rolling_range','rolling_pearson','rolling_zscore','rolling_stddev','lag','forward_return_label'].includes(String(s.op));
 if(hasPeriod?!Number.isInteger(s.period)||Number(s.period)<1||Number(s.period)>1000:'period'in s)return false;
 if(s.op==='rolling_pearson'&&Number(s.period)<30)return false;
 if(['rolling_zscore','rolling_stddev','rolling_range'].includes(String(s.op))&&Number(s.period)<2)return false;
 if(['greater_than','less_than'].includes(String(s.op))){
  if(typeof s.threshold!=='number'||!Number.isFinite(s.threshold))return false;
 }else if('threshold'in s)return false;
 if(s.op==='forward_return_label'){
  if(typeof s.when!=='string')return false;
 }else if('when'in s)return false;
 return true;
}
function validGraphWire(value:unknown):boolean {
 if(!isObject(value)||!matches(graph,value)||new TextEncoder().encode(JSON.stringify(value)).byteLength>16384)return false;
 const ids=new Set<string>();
 for(const step of value.steps as Record<string,unknown>[]){
  if(ids.has(String(step.id)))return false;
  ids.add(String(step.id));
 }
 const results=value.result_steps as string[];
 return new Set(results).size===results.length&&results.every(id=>ids.has(id));
}
function validResearchStrategy(args:Record<string,unknown>):boolean {
 if(!['validate','backtest','grid'].includes(String(args.action)))return true;
 const breakout=args.strategy_id==='close_breakout_v1';
 const parameters=args.action==='grid'?args.variants:[args.parameters];
 return Array.isArray(parameters)&&parameters.every(p=>isObject(p)&&
   (breakout?'lookback'in p:'fast'in p&&'slow'in p));
}
export function validTool(name:string,args:unknown):boolean {
 if(name==='market'&&isObject(args)&&!validMarketOperatorRefs(args))return false;
 if(name==='research'&&isObject(args)&&!validResearchStrategy(args))return false;
 if(name==='research'&&isObject(args)&&args.action==='record_decision'){
  for(const [field,max]of [['name',128],['hypothesis',4096],['reason',2048]]as const){const value=args[field];if(typeof value!=='string'||!value.trim()||new TextEncoder().encode(value).byteLength>max)return false;}
  if(new TextEncoder().encode(JSON.stringify([args.name,args.hypothesis,args.reason])).byteLength>6144)return false;
 }
 if(name==='market'&&isObject(args)){
  if('graph'in args){
   if(args.action!=='screen'||'composition'in args||!Array.isArray(args.fields)||
      !validGraphWire(args.graph)||args.fields.length+(args.graph as {result_steps:string[]}).result_steps.length>14)return false;
  }
  if(isObject(args.composition)){
   if(args.action!=='screen'||!Array.isArray(args.fields)||!Array.isArray(args.composition.result_steps)||args.fields.length+args.composition.result_steps.length>14)return false;
   if(Array.isArray(args.composition.steps)&&!args.composition.steps.every(validCompositionStep))return false;
  }
 }
 if(name==='trade'&&isObject(args)&&['preview','execute'].includes(String(args.action))&&['open','add','replace'].includes(String(args.operation))&&!args.protection)return false;
 return !!ACTION_SCHEMAS[name]&&matches({oneOf:ACTION_SCHEMAS[name]},args)&&!(isObject(args)&&typeof args.start_ms==='number'&&typeof args.end_ms==='number'&&args.end_ms<=args.start_ms);
}
export function isExecutionMutation(type:unknown,action?:unknown):boolean{return type==='connect_account'||(type==='trade'&&action!=='preview'&&action!=='status')||(type==='strategies'&&!['list','status'].includes(String(action)))||(type==='research'&&['backtest','grid','cancel','record_decision'].includes(String(action)));}
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
