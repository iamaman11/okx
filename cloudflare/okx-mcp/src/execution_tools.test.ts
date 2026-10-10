import { buildExecutionOperation, EXECUTION_TOOLS, executionProfilePermitsOperation, isExecutionMutation } from "./execution_tools.js";
function assert(condition: boolean, detail: string): void {
  if (!condition) throw Error(detail);
}
const INTENT="intent_stage4c_matrix_0123456789";
const uidAction=(action:string,rest:Record<string,unknown>={})=>({action,intent_id:INTENT,...rest});
const policy={
  version:"okx.demo.hard-risk/v1",
  max_account_gross_notional_usd:"100",
  max_instrument_gross_notional_usd:"100",
  max_margin_utilization_ratio:"0.1",
  max_loss_per_trade_usd:"5",
  max_daily_realized_loss_usd:"10",
  max_drawdown_ratio:"0.1",
  max_leverage:"2",
  allowed_instruments:["BTC-USDT-SWAP"],
  minimum_quality:"fresh",degraded_mode:"reject",correlated_clusters:[],
};
const mandate={
  version:"okx.demo.mandate/v1",capital_base_usd:"100",
  decision_horizon_hours:1,allowed_instruments:["BTC-USDT-SWAP"],
  max_drawdown_ratio:"0.1",leverage_ceiling:"2",
  minimum_liquidity_notional_usd:"0",max_turnover_ratio:"1",
};
const entry={
  entry_price:"80000",stop_price:"79000",max_settle_notional:"100",
  max_loss_settle:"5",target_rr:"2",
  entry_liquidity_role:"maker",exit_liquidity_role:"taker",
};
const basePrepare={
  instrument:"BTC-USDT-SWAP",trade_mode:"cross",order_type:"limit",
  risk:{mandate,policy},
};
const c=buildExecutionOperation(uidAction("prepare",{
  ...basePrepare,spec:{action:"open",position_side:"long",entry},
}));
assert(c?.type==="prepare_execution", "risk-bound open must be typed");
assert(buildExecutionOperation(uidAction("prepare",{
  ...basePrepare,spec:{action:"close",position_side:"long",size:"0.02",price:"80000"},
}))?.type==="prepare_execution","risk-bound reduce/close must be typed");
assert(buildExecutionOperation(uidAction("submit"))?.type==="submit_prepared_execution","submit must be typed");
assert(buildExecutionOperation(uidAction("abort_reverse"))?.type==="abort_reverse_execution","reverse abort must be typed");
assert(buildExecutionOperation(uidAction("amend",{mutation_id:"mutation_demo_012345",new_price:"79999"}))?.type==="mutate_execution","amend must be typed");
assert(buildExecutionOperation(uidAction("cancel",{mutation_id:"mutation_demo_012345"}))?.type==="mutate_execution","cancel must be typed");
assert(buildExecutionOperation(uidAction("cancel_protection",{mutation_id:"mutation_demo_012345"}))?.type==="mutate_execution","protection cancel must be typed");
for(const x of [
  uidAction("prepare",{...basePrepare,spec:{action:"open",position_side:"long",entry},risk:undefined}),
  uidAction("prepare",{...basePrepare,spec:{action:"open",position_side:"long",entry:{...entry,arbitrary:"do-not-forward"}}}),
  uidAction("prepare",{...basePrepare,spec:{action:"close",position_side:"long",size:"0",price:"80000"}}),
  uidAction("prepare",{...basePrepare,spec:{action:"close",position_side:"long",size:"0.02",price:"80000"}, unexpected:"value"}),
  uidAction("prepare",{...basePrepare,spec:{action:"close",position_side:"long",size:"0.02",price:"80000"}, risk:{mandate,policy:{...policy,minimum_quality:"unknown"}}}),
  uidAction("submit",{new_price:"90000"}),
  uidAction("amend",{mutation_id:"mutation_demo_012345"}),
  uidAction("cancel",{mutation_id:"invalid!"}),
  uidAction("submit",{intent_id:"bad"}),
  uidAction("execute_batch",{orders:[]}),
]) {
  const result=buildExecutionOperation(x);
  // Rust independently revalidates policy semantics, but Worker must reject
  // malformed or foreign fields before sending a corresponding native request.
  if(result!==null)throw Error("malformed execution action reached runtime: "+JSON.stringify(x));
}
for (const op of ["prepare_execution","submit_prepared_execution","mutate_execution","abort_reverse_execution"]) {
  assert(isExecutionMutation(op), "all trading writes classify as uncertain on lost ACK");
  assert(executionProfilePermitsOperation("demo_acceptance",op), "verified Demo write allowed");
  for (const profile of ["production","unverified","",undefined]) {
    assert(!executionProfilePermitsOperation(profile,op), "non-Demo profile must deny write");
  }
}
assert(executionProfilePermitsOperation("production","account_summary"),"read is allowed");
assert(!isExecutionMutation("account_summary"),"read must not be misclassified as exchange mutation");
assert(EXECUTION_TOOLS.length===2,"one read preflight + one explicit write surface");
assert(EXECUTION_TOOLS[1] !== undefined && (EXECUTION_TOOLS[1] as any).name==="execution_action","typed write name");
