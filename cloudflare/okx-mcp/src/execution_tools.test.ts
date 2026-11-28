import {ACTION_SCHEMAS,PUBLIC_TOOLS,MARKET_OPERATORS,SUPPORTED_CANDLE_TIMEFRAMES_MS,validTool,matches,isExecutionMutation,isResearchNativeOperation,executionProfilePermitsOperation} from './execution_tools.js';
const fail=(v:boolean,n:string)=>{if(!v)throw Error(n);};
// One owner policy drives both Research routing and runtime-profile admission.
for(const type of ['market','market_capabilities','market_microstructure','research','find_instruments','market_overview','portfolio_scenario']){
 const op={type,request:{action:'capabilities'}};
 fail(isResearchNativeOperation(op)&&executionProfilePermitsOperation('research',op),`Research owner admits ${type}`);
}
for(const type of ['connect_account','trade','strategies','portfolio','account_summary','mailbox_telemetry','unknown']){
 const op={type,request:{action:'status'}};
 fail(!isResearchNativeOperation(op)&&!executionProfilePermitsOperation('research',op),`Research owner has no finance or unrecognized operation ${type}`);
}
for(const view of ['finance','operation','storage',undefined]){
 const op={type:'diagnostics',request:{view}};
 fail(!isResearchNativeOperation(op)&&!executionProfilePermitsOperation('research',op),`Research cannot request diagnostics view ${view}`);
}
fail(isResearchNativeOperation({type:'diagnostics',request:{view:'research'}}), 'Research owns its diagnostics view');
fail(!isResearchNativeOperation({type:'diagnostics'}), 'Research diagnostics require an explicit scoped view');
fail(validTool('diagnostics',{action:'explain',view:'research',reference:'a'.repeat(64)}),'research diagnostics uses existing strict tool');
fail(!validTool('diagnostics',{action:'explain',view:'unknown',reference:'a'.repeat(64)}),'unknown diagnostic view is rejected');
function sample(s:any):any{if('const'in s)return s.const;if(s.enum)return s.enum[0];if(s.oneOf)return sample(s.oneOf[0]);if(s.anyOf)return sample(s.anyOf[0]);if(s.type==='object'||(s.properties&&s.required))return Object.fromEntries(Object.entries(s.properties).filter(([k])=>s.required.includes(k)).map(([k,x])=>[k,sample(x)]));if(s.type==='array')return [sample(s.items)];if(s.type==='string'&&s.pattern==='^sha256:[0-9a-f]{64}$')return 'sha256:'+'a'.repeat(64);if(s.type==='string'&&s.pattern==='^[0-9a-f]{64}$')return 'a'.repeat(64);if(s.type==='string')return (s.pattern && s.maxLength===29 && new RegExp(s.pattern).test('1'))?'1':(s.pattern?.includes('0-9]{1,9}')||s.pattern?.includes('\\d{1,9}'))?'1':s.minLength===8?'command_0001':'fixture';if(s.type==='integer'||s.type==='number')return s.minimum??1;if(s.type==='boolean')return true;return null;}
export const payloadFixtures:{tool:string;action:string;positive:any;negative:any[]}[]=[];
for(const [tool,schemas]of Object.entries(ACTION_SCHEMAS))for(const schema of schemas){const good=sample(schema);if('start_ms'in good)good.end_ms=good.start_ms+100000;if(tool==='market'&&good.action==='screen'){good.instruments=['BTC-USDT-SWAP'];good.fields=[MARKET_OPERATORS[0]];}if(tool==='research'&&good.action==='backtest')good.parameters={fast:9,slow:21};if(tool==='research'&&good.action==='economics')good.cases=[{instrument:'BTC-USDT',product:'spot',side:'long',quantity:'2',entry_price:'100',exit_price:'110',fees:{currency:'USDT',amount:'0'},funding:{currency:'USDT',amount:'0'}}];if(tool==='portfolio'&&good.action==='scenario')good.correlations=[];if(tool==='trade'&&['execute','preview'].includes(good.action))good.protection={stop_distance_bps:50};if(tool==='diagnostics'&&good.action==='explain')good.reference='intent_fixture';if(good.order)good.order.type='fok';fail(validTool(tool,good),`positive ${tool}.${good.action}`);const badExtra={...good,raw_rpc:'forbidden'},badRequired={...good};delete badRequired.action;const badType={...good,action:12};for(const bad of [badExtra,badRequired,badType])fail(!validTool(tool,bad),`negative ${tool}.${good.action}`);payloadFixtures.push({tool,action:good.action,positive:good,negative:[badExtra,badRequired,badType]});}
const economic=payloadFixtures.find(x=>x.tool==='research'&&x.action==='economics')!.positive;
const baseEconomic=economic.cases[0];
fail(validTool('research',{...economic,cases:[{...baseEconomic,instrument:'BTC-USDT-SWAP',product:'linear_swap',contract_type:'linear',contract_value:'0.1',contract_value_currency:'BTC',settle_currency:'USDT'}]}),'T02/T09 explicit linear contract unit is accepted');
for(const invalid of [
 {...economic,cases:[]},
 {...economic,cases:Array.from({length:9},()=>({...baseEconomic}))},
 {...economic,cases:[{...baseEconomic,quantity:'BTC'}]},
 {...economic,cases:[{...baseEconomic,fees:{...baseEconomic.fees,amount:'NaN'}}]},
 {...economic,cases:[{...baseEconomic,product:'unknown'}]},
 {...economic,cases:[{...baseEconomic,side:'both'}]},
 {...economic,cases:[{...baseEconomic,raw_rpc:'forbidden'}]},
])fail(!validTool('research',invalid),'T02/T09 rejects malformed economics input at MCP boundary');
fail(!isExecutionMutation('research','economics'),'pure economics cannot acquire research mutation authority');
for(const action of ['preview','status','capabilities','preflight'])
 fail(!isExecutionMutation('trade',action),`read-only trade.${action} must not be an uncertain mutation`);
for(const action of ['execute','cancel','close'])
 fail(isExecutionMutation('trade',action),`real trade.${action} must remain a mutation`);
const screen=payloadFixtures.find(x=>x.tool==='market'&&x.action==='screen')!.positive;
fail(validTool('market',{...screen,action:'compare'}),'shared strict screen/compare schema');
fail(!validTool('market',{...screen,action:'compare',raw_rpc:'forbidden'}),'compare rejects unknown fields');
const summary=payloadFixtures.find(x=>x.tool==='portfolio'&&x.action==='summary')!.positive;
fail(validTool('portfolio',{...summary,action:'history'}),'shared portfolio history schema');
fail(!validTool('portfolio',{...summary,action:'history',raw_rpc:'forbidden'}),'strict history schema');
const marketScreen=payloadFixtures.find(x=>x.tool==='market'&&x.action==='screen')!.positive;
const backtest=payloadFixtures.find(x=>x.tool==='research'&&x.action==='backtest')!.positive;
const strategyCreate=payloadFixtures.find(x=>x.tool==='strategies'&&x.action==='create')!.positive;
const publicTimeframe=JSON.stringify({enum:[...SUPPORTED_CANDLE_TIMEFRAMES_MS]});
for(const name of ['market','research','strategies'])
 fail(JSON.stringify(PUBLIC_TOOLS.find((tool:any)=>tool.name===name)).includes(publicTimeframe),
  `${name} publishes Native timeframe enum, not just internal validation`);
for(const ms of SUPPORTED_CANDLE_TIMEFRAMES_MS){
 fail(validTool('market',{...marketScreen,timeframe_ms:ms}),`market supports Native closed-bar timeframe ${ms}`);
 fail(validTool('research',{...backtest,timeframe_ms:ms}),`research supports Native closed-bar timeframe ${ms}`);
 fail(validTool('strategies',{...strategyCreate,timeframe_ms:ms}),`strategy supports Native live timeframe ${ms}`);
}
for(const ms of [1000,2678400000]){
 fail(!validTool('market',{...marketScreen,timeframe_ms:ms}),`market rejects unsupported ${ms}`);
 fail(!validTool('research',{...backtest,timeframe_ms:ms}),`research rejects unsupported ${ms}`);
 fail(!validTool('strategies',{...strategyCreate,timeframe_ms:ms}),`strategy rejects unsupported ${ms}`);
}
fail(validTool('market',{...marketScreen,start_ms:0}),'Native market range permits Unix epoch zero');
fail(validTool('research',{...backtest,start_ms:0}),'Native research range permits Unix epoch zero');
const {instruments:unused,...withoutList}=marketScreen;
fail(validTool('market',{...withoutList,universe:{instrument_type:'SWAP'}}),'market universe is a strict schema');
fail(!validTool('market',{...withoutList,fields:[],universe:{instrument_type:'SWAP'}}),'empty market fields rejected before native dispatch');
fail(!validTool('market',{...marketScreen,fields:[]}), 'explicit market screen requires at least one operator');
const operatorNames=[...MARKET_OPERATORS];
fail(operatorNames.length===16&&new Set(operatorNames).size===16,'T01 unique sixteen public operators');
for(const id of operatorNames){
 fail(validTool('market',{...marketScreen,fields:[id]}),`T01 accepted known operator ${id}`);
 fail(!validTool('market',{...marketScreen,fields:[id+'_unknown']}),`T01 rejected unknown operator ${id}`);
}
fail(!validTool('market',{...marketScreen,fields:['unknown_operator']}),'T01 rejects an arbitrary public operator');
fail(!validTool('market',{...marketScreen,filters:[{field:'unknown_operator',min:0,max:1}]}),'T01 rejects unknown filter operator');
fail(!validTool('market',{...marketScreen,sort:{field:'unknown_operator',descending:false}}),'T01 rejects unknown sort operator');
const publicMarket=(PUBLIC_TOOLS.find((tool:any)=>tool.name==='market') as any).inputSchema;
const publicMarketScreen=publicMarket.oneOf.find((schema:any)=>schema.properties?.fields);
fail(publicMarketScreen?.properties.fields.items.$ref==='#/$defs/s','T01 compact public operator IDs');
fail(!('o' in (publicMarket.$defs??{})),'T01 catalog stays on demand, not in tools/list');
fail(validTool('market',{...marketScreen,filters:[{field:'returns',min:0,max:1}],sort:{field:'returns',descending:false}}),'T01 registered filter and sort accepted');
fail(validTool('market',{...marketScreen,filters:[{field:'returns'}],sort:{field:'returns'}}),'T01 optional filter bounds and default sort direction match Native');
fail(!validTool('market',{...marketScreen,filters:[{field:'unknown_operator'}]}),'T01 unbounded unknown filter ID rejected');
fail(!validTool('market',{...marketScreen,sort:{field:'unknown_operator'}}),'T01 default-direction unknown sort ID rejected');
fail(validTool('market',{...marketScreen,fields:operatorNames.slice(0,14)}),'T01 fourteen known operators pass public boundary');
fail(!validTool('market',{...marketScreen,fields:operatorNames.slice(0,15)}),'T01 fifteen operators rejected by public boundary');
fail(!validTool('market',{...marketScreen,fields:operatorNames}),'T01 sixteen operators rejected by public boundary');
const composed={version:'okx.market.composition/v1',steps:[
 {id:'r1',op:'returns_series',input:'close'},
 {id:'w1',op:'rolling_mean',input:'r1',period:2},
],result_steps:['w1']};
fail(validTool('market',{...marketScreen,composition:composed}), 'versioned market composition accepted');
fail(!validTool('market',{...marketScreen,action:'compare',composition:composed}), 'composition limited to screen');
fail(!validTool('market',{...marketScreen,composition:{...composed,version:'unknown'}}), 'composition version rejected');
fail(!validTool('market',{...marketScreen,composition:{...composed,steps:[{id:'r1',op:'rolling_mean',input:'close'}]}}), 'window must specify period');
fail(!validTool('market',{...marketScreen,composition:{...composed,steps:[{id:'r1',op:'returns_series',input:'close',period:2}]}}), 'returns does not accept window period');
const extremaGraph={...composed,steps:[
 {id:'upper',op:'rolling_max',input:'close',period:20},
 {id:'lower',op:'rolling_min',input:'close',period:20},
],result_steps:['upper','lower']};
fail(validTool('market',{...marketScreen,composition:extremaGraph}),'typed causal extrema share one public request');
const rangeExtension={...composed,steps:[
 {id:'width',op:'rolling_range',input:'close',period:3},
 {id:'wide',op:'greater_than',input:'width',threshold:5},
 {id:'entry',op:'rising_edge',input:'wide'},
 {id:'smoothed',op:'rolling_mean',input:'width',period:2},
],result_steps:['width','entry','smoothed']};
fail(validTool('market',{...marketScreen,composition:rangeExtension}),
 'T15 new pure range operator and user-defined causal branching share existing market screen');
fail(validTool('market',{...marketScreen,composition:{...rangeExtension,steps:[
 ...rangeExtension.steps.slice(0,1),{...rangeExtension.steps[1],threshold:20},...rangeExtension.steps.slice(2)
]}}),'T15 user can change graph parameters without an API route change');
for(const malformed of [
 {id:'width',op:'rolling_range',input:'close'},
 {id:'width',op:'rolling_range',input:'close',period:1},
 {id:'width',op:'rolling_range',input:'close',period:1001},
 {id:'width',op:'rolling_range',input:'close',period:3,threshold:0},
 {id:'width',op:'rolling_range',input:'close',period:3,other:'open'},
])fail(!validTool('market',{...marketScreen,composition:{...rangeExtension,steps:[malformed],result_steps:['width']}}),
 'T15 extension rejects illegal windows and unrelated parameters at MCP');
const zscoreGraph={...composed,steps:[
 {id:'standard',op:'rolling_zscore',input:'close',period:20},
 {id:'high',op:'greater_than',input:'standard',threshold:1.5},
],result_steps:['standard','high']};
fail(validTool('market',{...marketScreen,composition:zscoreGraph}),
 'T15 typed causal zscore is public without a new route');
const twoSidedRisk={...composed,steps:[
 {id:'fast',op:'rolling_mean',input:'close',period:2},
 {id:'slow',op:'rolling_mean',input:'close',period:3},
 {id:'below',op:'less_than_series',input:'fast',other:'slow'},
 {id:'exit',op:'falling_edge',input:'below'},
 {id:'risk',op:'rolling_stddev',input:'close',period:2},
 {id:'quiet',op:'less_than',input:'risk',threshold:4},
],result_steps:['exit','risk','quiet']};
fail(validTool('market',{...marketScreen,composition:twoSidedRisk}),
 'T07/T15 symmetric typed exit and causal population volatility use the existing route');
for(const bad of [
 {id:'risk',op:'rolling_stddev',input:'close'},
 {id:'risk',op:'rolling_stddev',input:'close',period:1},
 {id:'risk',op:'rolling_stddev',input:'close',period:2,threshold:1},
 {id:'risk',op:'rolling_stddev',input:'close',period:2,other:'close'},
])fail(!validTool('market',{...marketScreen,composition:{...twoSidedRisk,steps:[bad],result_steps:['risk']}}),
 'T02/T03 stddev rejects invalid period or unrelated parameters');
for(const bad of [
 {id:'below',op:'less_than_series',input:'fast'},
 {id:'below',op:'less_than_series',input:'fast',other:'slow',threshold:0},
 {id:'below',op:'less_than_series',input:'fast',other:'slow',period:2},
 {id:'quiet',op:'less_than',input:'close'},
 {id:'quiet',op:'less_than',input:'close',threshold:0,other:'open'},
 {id:'exit',op:'falling_edge',input:'below',period:2},
])fail(!validTool('market',{...marketScreen,composition:{...twoSidedRisk,
 steps:[...twoSidedRisk.steps.slice(0,2),bad],result_steps:[bad.id]}}),
 'T02/T03 symmetric thresholds and edge reject malformed arguments');
const fundingZscore={...composed,steps:[
 {id:'standard',op:'rolling_zscore',input:'funding_rate',period:2},
],result_steps:['standard']};
fail(validTool('market',{...marketScreen,instruments:['BTC-USDT-SWAP'],composition:fundingZscore}),
 'funding normalization available for swap research');
fail(!validTool('market',{...marketScreen,instruments:['BTC-USDT'],composition:fundingZscore}),
 'funding normalization rejects spot before venue reads');
for(const bad of [
 {id:'standard',op:'rolling_zscore',input:'close'},
 {id:'standard',op:'rolling_zscore',input:'close',period:1},
 {id:'standard',op:'rolling_zscore',input:'close',period:1001},
 {id:'standard',op:'rolling_zscore',input:'close',period:20,threshold:1},
 {id:'standard',op:'rolling_zscore',input:'close',period:20,other:'close'},
]){
 fail(!validTool('market',{...marketScreen,composition:{...zscoreGraph,steps:[bad],result_steps:['standard']}}),
  'rolling_zscore rejects malformed period and extraneous arguments');
}
const joinedGraph={...composed,steps:[
 {id:'price_return',op:'returns_series',input:'close'},
 {id:'oi_return',op:'returns_series',input:'open_interest'},
 {id:'combined',op:'add_series',input:'price_return',other:'oi_return'},
],result_steps:['combined']};
fail(validTool('market',{...marketScreen,instruments:['BTC-USDT-SWAP'],composition:joinedGraph}),
 'two dimensionless series combine in one public composition');
const divergenceGraph={...joinedGraph,steps:[
 ...joinedGraph.steps.slice(0,2),
 {id:'difference',op:'subtract_series',input:'price_return',other:'oi_return'},
 {id:'relative',op:'divide_series',input:'price_return',other:'oi_return'},
],result_steps:['difference','relative']};
fail(validTool('market',{...marketScreen,instruments:['BTC-USDT-SWAP'],composition:divergenceGraph}),
 'T02/T05/T15 typed difference and division share the registered market route');
const correlationGraph={...joinedGraph,steps:[
 ...joinedGraph.steps.slice(0,2),
 {id:'relationship',op:'rolling_pearson',input:'price_return',other:'oi_return',period:30},
],result_steps:['relationship']};
fail(validTool('market',{...marketScreen,instruments:['BTC-USDT-SWAP'],composition:correlationGraph}),
 'T06 exact cross-source 30-pair Pearson uses the existing market screen route');
for(const bad of [
 {id:'relationship',op:'rolling_pearson',input:'price_return',period:30},
 {id:'relationship',op:'rolling_pearson',input:'price_return',other:'oi_return',period:29},
 {id:'relationship',op:'rolling_pearson',input:'price_return',other:'oi_return',period:1001},
 {id:'relationship',op:'rolling_pearson',input:'price_return',other:'oi_return',period:30,threshold:0.9},
]){
 fail(!validTool('market',{...marketScreen,instruments:['BTC-USDT-SWAP'],composition:{...correlationGraph,steps:[...correlationGraph.steps.slice(0,2),bad]}}),
  'rolling_pearson rejects missing source, undersized window and unrelated parameters');
}
for(const op of ['subtract_series','divide_series']){
 for(const bad of [
  {id:'difference',op,input:'price_return'},
  {id:'difference',op,input:'price_return',other:'oi_return',period:2},
  {id:'difference',op,input:'price_return',other:'oi_return',threshold:0},
 ]){
  fail(!validTool('market',{...marketScreen,instruments:['BTC-USDT-SWAP'],composition:{...divergenceGraph,steps:[...divergenceGraph.steps.slice(0,2),bad]}}),
   `${op} rejects missing other and unrelated parameters`);
 }
}
for(const bad of [
 {id:'combined',op:'add_series',input:'price_return'},
 {id:'combined',op:'add_series',input:'price_return',other:'oi_return',period:2},
 {id:'combined',op:'returns_series',input:'price_return',other:'oi_return'},
]){
 fail(!validTool('market',{...marketScreen,instruments:['BTC-USDT-SWAP'],composition:{...joinedGraph,steps:[...joinedGraph.steps.slice(0,2),bad]}}),
  'add_series rejects malformed parameters');
}
for(const op of ['rolling_max','rolling_min']){
 for(const bad of [{id:'upper',op,input:'close'},{id:'upper',op,input:'close',period:1001},{id:'upper',op,input:'close',period:2,threshold:3}]){
  fail(!validTool('market',{...marketScreen,composition:{...extremaGraph,steps:[bad]}}),`${op} rejects absent, excessive and unrelated parameters`);
 }
}
const causalSteps=[
 {id:'returns',op:'returns_series',input:'close'},
 {id:'positive',op:'greater_than',input:'returns',threshold:0},
 {id:'event',op:'rising_edge',input:'positive'},
 {id:'prior',op:'lag',input:'close',period:1},
 {id:'outcome',op:'forward_return_label',input:'close',period:2,when:'event'},
];
const causalGraph={...composed,steps:causalSteps,result_steps:['positive','event','prior','outcome']};
fail(validTool('market',{...marketScreen,composition:causalGraph}), 'bounded event and future label graph passes strict public schema');
const priceCrossGraph={...composed,steps:[
 {id:'fast',op:'rolling_mean',input:'close',period:5},
 {id:'slow',op:'rolling_mean',input:'close',period:20},
 {id:'above',op:'greater_than_series',input:'fast',other:'slow'},
 {id:'cross',op:'rising_edge',input:'above'},
],result_steps:['above','cross']};
fail(validTool('market',{...marketScreen,composition:priceCrossGraph}),
 'T03/T15 independently parametrized price windows compose into a crossing event');
const ohlcGraph={...composed,steps:[
 {id:'body',op:'greater_than_series',input:'close',other:'open'},
 {id:'range',op:'greater_than_series',input:'high',other:'low'},
 {id:'confirmed',op:'and',input:'body',other:'range'},
],result_steps:['confirmed']};
fail(validTool('market',{...marketScreen,composition:ohlcGraph}),
 'T02/T04 four closed-candle inputs can compose into a typed signal');
const volumeGraph={...composed,steps:[
 {id:'fast_volume',op:'rolling_mean',input:'volume',period:2},
 {id:'slow_volume',op:'rolling_mean',input:'volume',period:3},
 {id:'lift',op:'greater_than_series',input:'fast_volume',other:'slow_volume'},
 {id:'entry',op:'rising_edge',input:'lift'},
 {id:'volume_change',op:'returns_series',input:'volume'},
],result_steps:['fast_volume','lift','entry','volume_change']};
fail(validTool('market',{...marketScreen,composition:volumeGraph}),
 'T02/T07 venue-native volume can feed causal typed series composition');
for(const bad of [
 {id:'above',op:'greater_than_series',input:'fast'},
 {id:'above',op:'greater_than_series',input:'fast',other:'slow',period:2},
 {id:'above',op:'greater_than_series',input:'fast',other:'slow',threshold:0},
])fail(!validTool('market',{...marketScreen,composition:{...priceCrossGraph,
 steps:[...priceCrossGraph.steps.slice(0,2),bad],result_steps:['above']}}),
 'T02/T03 invalid binary price comparison rejected by public schema');
const booleanGraph={...composed,steps:[
 {id:'returns',op:'returns_series',input:'close'},
 {id:'positive',op:'greater_than',input:'returns',threshold:0},
 {id:'prior',op:'lag',input:'positive',period:1},
 {id:'both',op:'and',input:'positive',other:'prior'},
 {id:'either',op:'or',input:'positive',other:'prior'},
 {id:'event',op:'rising_edge',input:'both'},
],result_steps:['both','either','event']};
fail(validTool('market',{...marketScreen,composition:booleanGraph}),
 'T07/T15 boolean conjunction and disjunction accepted through public market schema');
for(const op of ['and','or']){
 for(const bad of [
  {id:'both',op,input:'positive'},
  {id:'both',op,input:'positive',other:'prior',period:2},
  {id:'both',op,input:'positive',other:'prior',threshold:1},
 ]){
  fail(!validTool('market',{...marketScreen,composition:{...booleanGraph,steps:[...booleanGraph.steps.slice(0,3),bad]}}),
   `${op} rejects missing other or unrelated parameter`);
 }
}
for(const invalid of [
 {...causalSteps[1],threshold:undefined},
 {...causalSteps[1],threshold:Infinity},
 {...causalSteps[2],period:1},
 {...causalSteps[3],period:0},
 {...causalSteps[4],when:undefined},
 {...causalSteps[4],threshold:0},
]){
 const altered=[...causalSteps];
 const pos=causalSteps.findIndex(s=>s.id===invalid.id);
 altered[pos]=invalid as any;
 fail(!validTool('market',{...marketScreen,composition:{...causalGraph,steps:altered}}),
  `causal graph rejects unexpected or invalid parameter ${invalid.id}`);
}
fail(!validTool('market',{...marketScreen,fields:Array.from({length:14},(_,i)=>`field_${i}`),composition:composed}), 'combined projection count bounded');
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
const {strategy_id:_graphStrategy,parameters:_graphParameters,...graphBase}=run;
const graphSignal={version:'okx.graph-signal/v1',graph:{version:'okx.market.composition/v1',steps:[
 {id:'change',op:'returns_series',input:'close'},
 {id:'up',op:'greater_than',input:'change',threshold:0},
],result_steps:['up']},signal_step:'up',target_if_true:1,target_if_false:0,missing:'hold'};
const graphPayload={...graphBase,action:'graph_backtest',graph_signal:graphSignal};
fail(validTool('research',graphPayload),'research graph backtest is a typed causal operation');
fail(isExecutionMutation('research','graph_backtest'),'graph research jobs are correctly marked mutations');
const graphInvalid=[
 {...graphPayload,strategy_id:'ema_cross_v1'},
 {...graphPayload,graph_signal:{...graphSignal,version:'unknown'}},
 {...graphPayload,graph_signal:{...graphSignal,target_if_true:2}},
 {...graphPayload,graph_signal:{...graphSignal,signal_step:'missing'}},
 {...graphPayload,graph_signal:{...graphSignal,missing:'buy'}},
 {...graphPayload,graph_signal:{...graphSignal,raw_source:'not_typed'}},
 {...graphPayload,graph_signal:{...graphSignal,graph:{...graphSignal.graph,steps:[...graphSignal.graph.steps,{id:'future',op:'forward_return_label',input:'close',period:3,when:'up'}]}}},
 {...graphPayload,graph_signal:{...graphSignal,graph:{...graphSignal.graph,steps:[{id:'change',op:'returns_series',input:'close',period:2},graphSignal.graph.steps[1]]}}},
 {...graphPayload,raw_rpc:'forbidden'},
];
for(const bad of graphInvalid)
 fail(!validTool('research',bad),'research graph rejects malformed or noncausal input before native dispatch');
payloadFixtures.push({tool:'research',action:'graph_backtest_causal',positive:graphPayload,negative:graphInvalid});
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
const catalogPage={action:'list',limit:50,cursor:'a'.repeat(64)};
fail(validTool('research',catalogPage),'research list accepts last-seen job ID cursor');
fail(validTool('research',{action:'list',limit:1}),'research list accepts first page without cursor');
fail(!isExecutionMutation('research','list'),'listing research jobs has no mutation authority');
for(const invalid of [
 {...catalogPage,limit:0},{...catalogPage,limit:51},{...catalogPage,limit:1.5},
 {...catalogPage,cursor:'A'.repeat(64)}, {...catalogPage,cursor:'a'.repeat(63)},
 {...catalogPage,cursor:'z'.repeat(64)}, {...catalogPage,cursor:'../catalog'},
 {...catalogPage,cursor:''}, {...catalogPage,account:'foreign'},
 {action:'list',cursor:'a'.repeat(64)},
])fail(!validTool('research',invalid),'research list strict bounds, cursor, no account or extra fields');
for(const view of ['summary','trades','equity','variants'])
 fail(validTool('research',{...researchResult,view}),`research result view ${view} accepted`);
fail(!validTool('research',{...researchResult,view:'unsupported'}),'research view strictly enumerated');
console.log('live_strategy_v1_and_research_views PASS');
const start=payloadFixtures.find(x=>x.tool==='strategies'&&x.action==='start')!.positive;
const {expected_revision,configuration_digest,...stop}=start;
fail(validTool('strategies',{...stop,action:'stop'}),'strategy stop uses instance identity');
fail(!validTool('strategies',{...stop,action:'stop',raw_rpc:'forbidden'}),'strict stop rejects extra');
fail(!validTool('strategies',{...stop,action:'start'}),'start requires revision and digest');
fail(!validTool('strategies',{...start,configuration_digest:'bad'}),'start rejects malformed digest');
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
const restore=payloadFixtures.find(x=>x.tool==='strategies'&&x.action==='restore_configuration')!;
for(const invalid of [
 {...restore.positive,source_revision:restore.positive.expected_revision},
 {...restore.positive,instance_id:'manual'},
 {...restore.positive,source_instance:'other'},
]){
 fail(!validTool('strategies',invalid),'invalid or foreign strategy restoration must be rejected');
 restore.negative.push(invalid);
}
for(const invalid of [
 {...capitalAction,amount_usd:'0'},
 {...capitalAction,amount_usd:'-10'},
 {...capitalAction,direction:'override'},
 {...capitalAction,expected_authority_revision:0},
])fail(!validTool('strategies',invalid),'P0.2 invalid capital assignment passed schema');
const strategiesTool=PUBLIC_TOOLS.find((t:any)=>t.name==='strategies') as any;
fail(!!strategiesTool?.inputSchema?.$defs?.d,'P0.2 shared public decimal schema missing');
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
 if(tool.name==='accounts'){
  fail(expanded.type==='object' && expanded.required.length===1 && expanded.required[0]==='action'
    && expanded.additionalProperties===false,'linked accounts root is strict object');
  fail(expanded.properties.link_id.pattern==='^link_[a-f0-9]{32}$',
    'host-only linked account selector is bounded');
  fail(expanded.oneOf.length===ACTION_SCHEMAS.accounts.length
    && expanded.oneOf.every((branch:any)=>branch.additionalProperties===false
      && Object.keys(branch.properties?.link_id??{}).length===0
      && (branch.properties?.action?.const||branch.properties?.action?.enum)),
    'every account action retains strict allowed fields');
  fail(!validTool('accounts',{action:'list',link_id:'link_fixture'}),
    'native protocol never accepts host-only link selector');
 }else{
  const strategyBranches=tool.name==='strategies'?expanded.oneOf.flatMap((branch:any)=>
   branch.oneOf?branch.oneOf.map((action:any)=>({
    properties:{action:action.properties.action,...branch.properties,...action.properties},
    required:['action',...branch.required,...action.required.filter((key:string)=>key!=='action')],
   })): [branch]):[];
  const variants=tool.name==='strategies'?strategyBranches.map((branch:any)=>({
    properties:{action:branch.properties.action,account:expanded.properties.account,...branch.properties},
    required:['action','account',...branch.required.filter((key:string)=>key!=='action')],
    additionalProperties:false,
  })):tool.name==='research'?expanded.oneOf.map((branch:any)=>({
    properties:Object.fromEntries(Object.entries(branch.properties).map(([field,rule])=>
      [field,field==='graph_signal'?
        ACTION_SCHEMAS.research.find((s:any)=>s.properties.action.const==='graph_backtest')!.properties.graph_signal:
        (Object.keys(rule as object).length===0?expanded.properties[field]:rule)])),
    // Restore the discriminator requirement factored to the public root;
    // compare the reconstructed strict branch byte-for-byte to its source.
    ...Object.fromEntries(Object.entries(branch).filter(([key])=>key!=='properties')
      .map(([key,value])=>[key,key==='required'?
        ['action',...(value as string[]).filter(field=>field!=='action')]:value])),
  })):expanded.oneOf;
  if(tool.name==='strategies')
    fail(expanded.type==='object' && expanded.unevaluatedProperties===false
      && expanded.required.length===1 && expanded.required[0]==='account',
      'factored strategies schema stays strict at the root');
  if(tool.name==='research')
    fail(Object.keys(expanded.properties).length>7&&
      expanded.oneOf.find((branch:any)=>branch.properties.action.const==='graph_backtest')?.properties.graph_signal?.type==='object'&&
      variants.every((branch:any)=>branch.additionalProperties===false),
      'research shared types preserve action-specific closed objects');
  const comparable=tool.name==='strategies'||tool.name==='research'?{oneOf:variants}:expanded;
  fail(JSON.stringify(comparable)===JSON.stringify({oneOf:ACTION_SCHEMAS[tool.name]}),
    `exact public schema factoring ${tool.name}`);
  for(const fixture of payloadFixtures.filter(x=>x.tool===tool.name))
   for(const input of [fixture.positive,...fixture.negative])
    fail(matches(comparable,input)===matches({oneOf:ACTION_SCHEMAS[tool.name]},input),
      `factored positive/negative semantics ${tool.name}.${fixture.action}`);
 }
}
for(const action of ['backtest','grid','validate']){
 const base=payloadFixtures.find(x=>x.tool==='research'&&x.action===action)!.positive;
 const positive={...base,strategy_id:'ema_momentum_confirm_v1'};
 const negative={...positive,strategy_id:'unregistered_source_code'};
 fail(validTool('research',positive),`registered research composition ${action}`);
 fail(!validTool('research',negative),'unregistered strategy cannot cross research boundary');
 payloadFixtures.push({tool:'research',action:action+'_composition',positive,negative:[negative]});
}

for(const action of ['backtest','grid','validate']){
 const base=payloadFixtures.find(x=>x.tool==='research'&&x.action===action)!.positive;
 const withLookback=(lookback:number)=>action==='grid'
   ?{...base,strategy_id:'close_breakout_v1',variants:[{lookback}]}
   :{...base,strategy_id:'close_breakout_v1',parameters:{lookback}};
 const positive=withLookback(20);
 const wrongType=action==='grid'?{...positive,variants:[{fast:9,slow:21}]}:{...positive,parameters:{fast:9,slow:21}};
 const mismatch=action==='grid'?{...base,variants:[{lookback:20}]}:{...base,parameters:{lookback:20}};
 const invalid=[withLookback(1),withLookback(1001),wrongType,mismatch];
 fail(validTool('research',positive),`research ${action} accepts own breakout parameters`);
 for(const bad of invalid)fail(!validTool('research',bad),`research ${action} rejects malformed or mismatched breakout`);
 payloadFixtures.push({tool:'research',action:action+'_breakout',positive,negative:invalid});
}

for(const action of ['backtest','grid','validate']){
 const base=payloadFixtures.find(x=>x.tool==='research'&&x.action===action)!.positive;
 const withPeriods=(fast:number,slow:number)=>action==='grid'?{...base,variants:[{fast,slow}]}:{...base,parameters:{fast,slow}};
 const positive=withPeriods(999,1000),negative=[withPeriods(999,1001),withPeriods(1000,1000)];
 fail(validTool('research',positive),`research ${action} maximum supported periods`);
 for(const bad of negative)fail(!validTool('research',bad),'research EMA periods match Native bounds');
 payloadFixtures.push({tool:'research',action:action+'_period_bounds',positive,negative});
}
