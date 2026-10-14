import { buildExecutionOperation, EXECUTION_TOOLS, executionProfilePermitsOperation, isExecutionMutation } from "./execution_tools.js";
import { TOOL_CONTRACT_VERSION } from "./shared.js";
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
const validLineage = {
  origin_evidence_id:"sha256:"+"a".repeat(64),
  origin_schema:"stage4c.demo/v1",
  origin_version:"v1",
  decision_reference:{
    decision_time_ms:1791763200000,
    price:"80000",
    price_basis:"limit_price",
    price_policy_version:"stage4c/v1",
  },
};
assert(buildExecutionOperation(uidAction("prepare",{
  ...basePrepare,spec:{action:"open",position_side:"long",entry},lineage:validLineage,
}))?.type==="prepare_execution","correct native sha256 lineage must pass unchanged");
for(const bad of [
  {origin_evidence_id:"physical-demo-btc-20261012a"},
  {origin_evidence_id:"sha256:"+"A".repeat(64)},
  {origin_evidence_id:"sha256:"+"a".repeat(63)},
  {authority_evidence_id:"external-note"},
  {decision_reference:{...validLineage.decision_reference,price_basis:"external"}},
  {decision_reference:{...validLineage.decision_reference,decision_time_ms:0}},
  {decision_reference:{...validLineage.decision_reference,price:"0"}},
]) {
  const attempt = buildExecutionOperation(uidAction("prepare",{
    ...basePrepare,
    spec:{action:"open",position_side:"long",entry},
    lineage:{...validLineage,...bad},
  }));
  assert(attempt===null,"malformed lineage must fail at MCP ingress before transport: "+JSON.stringify(bad));
}
const schemaLineage = (EXECUTION_TOOLS[1] as any).inputSchema.properties.lineage;
assert(schemaLineage.properties.origin_evidence_id.pattern==="^sha256:[0-9a-f]{64}$",
  "advertised MCP schema must match the native SHA256 evidence contract");
assert(buildExecutionOperation(uidAction("prepare",{
  ...basePrepare,spec:{action:"close",position_side:"long",size:"0.02",price:"80000"},
}))?.type==="prepare_execution","risk-bound reduce/close must be typed");
const begin = uidAction("begin_strategy_run",{
  origin_schema:"okx.research.live-decision/v1",origin_version:"run-2026-10-13",
  mandate_version:"demo-mandate-v1",allocated_usdt:"100",
  operator_authority_id:"sha256:"+"f".repeat(64),
});
const beginOperation = buildExecutionOperation(begin);
assert(beginOperation?.type==="begin_strategy_run" &&
  beginOperation.allocation_id===INTENT &&
  beginOperation.allocated_usdt==="100","capital run is typed and has no gateway order fields");
for (const invalid of [
  {...begin,allocated_usdt:"0"},
  {...begin,operator_authority_id:"not-a-proof"},
  {...begin,origin_schema:"bad !!"},
  {...begin,spec:{action:"open"}},
]) {
  assert(buildExecutionOperation(invalid)===null,
    "malformed run must fail before runtime admission");
}
const explicitRearm = {
  expected_stopped_at_ms:1792017080203,
  prior_finalization_sha256:"f".repeat(64),
};
const newRunWithRearm = buildExecutionOperation({...begin,rearm:explicitRearm});
assert(newRunWithRearm?.type==="begin_strategy_run"
  && (newRunWithRearm.rearm as {expected_stopped_at_ms:number}|undefined)?.expected_stopped_at_ms===1792017080203,
  "explicit rearm must stay on the same typed opening and never become an untyped RPC");
for(const rearm of [
  {expected_stopped_at_ms:0,prior_finalization_sha256:"f".repeat(64)},
  {expected_stopped_at_ms:1792017080203,prior_finalization_sha256:"bad"},
  {expected_stopped_at_ms:1792017080203,prior_finalization_sha256:"f".repeat(64),override_stop:true},
]){
  assert(buildExecutionOperation({...begin,rearm})===null,
    "malformed or forged rearm rejected before native runtime");
}
assert(buildExecutionOperation({
  action:"set_leverage",intent_id:"stage5a_leverage_20261014",instrument:"BTC-USDT-SWAP",
  trade_mode:"cross",leverage:"2",
})?.type==="set_leverage", "typed leverage reduction lives in existing write surface");
for (const invalid of [
  {trade_mode:"isolated",leverage:"2",instrument:"BTC-USDT-SWAP"},
  {trade_mode:"cross",leverage:"-2",instrument:"BTC-USDT-SWAP"},
  {trade_mode:"cross",leverage:"2",instrument:"BTC-USD-SWAP"},
]) {
  assert(buildExecutionOperation({action:"set_leverage",intent_id:"stage5a_leverage_20261014",...invalid})===null,
    "unsafe leverage wire shape rejected");
}
assert(isExecutionMutation("set_leverage"),"leverage POST must require Demo profile");
assert(!executionProfilePermitsOperation("production","set_leverage"),"Production leverage always read-only");
assert(isExecutionMutation("begin_strategy_run"),"capital mutation requires Demo-only owner");
assert(!executionProfilePermitsOperation("production","begin_strategy_run"),"Production cannot allocate Demo strategy capital");
assert(buildExecutionOperation(uidAction("finalize_strategy_run"))?.type==="finalize_strategy_run",
  "finalization is one typed local Demo-only ledger operation");
assert(buildExecutionOperation(uidAction("finalize_strategy_run",{new_price:"0.1"}))===null,
  "finalization cannot accept caller asserted exchange proof or trading fields");
assert(isExecutionMutation("finalize_strategy_run"),"finalization is a protected local ledger write");
assert(buildExecutionOperation(uidAction("quarantine_unverified_strategy_run"))?.type==="quarantine_unverified_strategy_run",
  "UNVERIFIED closure must use an exact typed non-financial local operation");
assert(buildExecutionOperation(uidAction("quarantine_unverified_strategy_run",{new_price:"99"}))===null,
  "caller cannot inject synthetic NAV or any trading fields in UNVERIFIED");
assert(!executionProfilePermitsOperation("production","quarantine_unverified_strategy_run"),
  "Production cannot quarantine Demo strategy runs");
assert(!executionProfilePermitsOperation("production","finalize_strategy_run"),
  "Production cannot finalize a Demo run");
assert(buildExecutionOperation(uidAction("submit"))?.type==="submit_prepared_execution","submit must be typed");
assert(buildExecutionOperation(uidAction("abort_reverse"))?.type==="abort_reverse_execution","reverse abort must be typed");
assert(buildExecutionOperation(uidAction("abandon_prepared"))?.type==="abandon_prepared_execution","only local unsent abandonment admitted");
assert(buildExecutionOperation(uidAction("amend",{mutation_id:"mutation_demo_012345",new_price:"79999"}))?.type==="mutate_execution","amend must be typed");
assert(buildExecutionOperation(uidAction("cancel",{mutation_id:"mutation_demo_012345"}))?.type==="mutate_execution","cancel must be typed");
assert(buildExecutionOperation(uidAction("cancel_protection",{mutation_id:"mutation_demo_012345"}))?.type==="mutate_execution","protection cancel must be typed");
const shrink=buildExecutionOperation(uidAction("shrink_protection_size",{
  mutation_id:"mutation_demo_012345",new_size:"0.5",
}));
assert(shrink?.type==="mutate_execution" && (shrink as any).mutation.type==="shrink_protection"
  && (shrink as any).mutation.new_size==="0.5",
  "shrink must preserve exact typed size without granting another mutation");

const confirm=buildExecutionOperation(uidAction("confirm_protection_size",{mutation_id:"mutation_demo_012345"}));
assert(confirm?.type==="mutate_execution" && (confirm as any).mutation.type==="confirm_protection_size",
  "fenced venue readback must use existing Demo-only mutation transport");
for(const x of [
  uidAction("prepare",{...basePrepare,spec:{action:"open",position_side:"long",entry},risk:undefined}),
  uidAction("prepare",{...basePrepare,spec:{action:"open",position_side:"long",entry:{...entry,arbitrary:"do-not-forward"}}}),
  uidAction("prepare",{...basePrepare,spec:{action:"close",position_side:"long",size:"0",price:"80000"}}),
  uidAction("prepare",{...basePrepare,spec:{action:"close",position_side:"long",size:"0.02",price:"80000"}, unexpected:"value"}),
  uidAction("prepare",{...basePrepare,spec:{action:"close",position_side:"long",size:"0.02",price:"80000"}, risk:{mandate,policy:{...policy,minimum_quality:"unknown"}}}),
  uidAction("submit",{new_price:"90000"}),
  uidAction("abandon_prepared",{mutation_id:"mutation_demo_012345"}),
  uidAction("abandon_prepared",{new_price:"90000"}),
  uidAction("amend",{mutation_id:"mutation_demo_012345"}),
  uidAction("cancel",{mutation_id:"invalid!"}),
  uidAction("confirm_protection_size",{mutation_id:"invalid!"}),
  uidAction("confirm_protection_size",{mutation_id:"mutation_demo_012345",new_size:"0.5"}),
  uidAction("confirm_protection_size",{mutation_id:"mutation_demo_012345",raw_rpc:"POST"}),
  uidAction("shrink_protection_size",{mutation_id:"mutation_demo_012345"}),
  uidAction("shrink_protection_size",{mutation_id:"invalid!",new_size:"0.5"}),
  uidAction("shrink_protection_size",{mutation_id:"mutation_demo_012345",new_size:"0"}),
  uidAction("shrink_protection_size",{mutation_id:"mutation_demo_012345",new_size:"0.5",raw_rpc:"POST"}),
  uidAction("shrink_protection_size",{mutation_id:"mutation_demo_012345",new_size:"0.5",new_price:"123"}),
  uidAction("submit",{intent_id:"bad"}),
  uidAction("execute_batch",{orders:[]}),
]) {
  const result=buildExecutionOperation(x);
  // Rust independently revalidates policy semantics, but Worker must reject
  // malformed or foreign fields before sending a corresponding native request.
  if(result!==null)throw Error("malformed execution action reached runtime: "+JSON.stringify(x));
}
for (const op of ["prepare_execution","submit_prepared_execution","mutate_execution","abort_reverse_execution","abandon_prepared_execution","begin_strategy_run","finalize_strategy_run","quarantine_unverified_strategy_run","set_leverage"]) {
  assert(isExecutionMutation(op), "all trading writes classify as uncertain on lost ACK");
  assert(executionProfilePermitsOperation("demo_acceptance",op), "verified Demo write allowed");
  for (const profile of ["production","unverified","",undefined]) {
    assert(!executionProfilePermitsOperation(profile,op), "non-Demo profile must deny write");
  }
}
assert(executionProfilePermitsOperation("production","account_summary"),"read is allowed");
assert(!isExecutionMutation("account_summary"),"read must not be misclassified as exchange mutation");
assert(EXECUTION_TOOLS.length===2,"one read preflight + one explicit write surface");
const advertisedActions = (EXECUTION_TOOLS[1] as any).inputSchema.properties.action.enum as string[];
assert(TOOL_CONTRACT_VERSION === "okx.mcp.tools/2026-10-15.1",
  "typed action schema changed; the advertised MCP contract version must advance");
assert(advertisedActions.includes("quarantine_unverified_strategy_run"),
  "UNVERIFIED must be distinct from financial FINALIZED");
assert(advertisedActions.includes("finalize_strategy_run"),
  "finalization must be advertised via existing Demo-only typed operation");
assert(advertisedActions.includes("set_leverage"),"native leverage lowering is exposed via same typed action");
assert(advertisedActions.includes("begin_strategy_run"),"native capital opening is typed and Demo only");
assert(advertisedActions.includes("abandon_prepared"),"advertised writable action list must include the typed unsent PREPARED release");
assert(advertisedActions.includes("confirm_protection_size"),"existing tool schema advertises exact-ID safe local confirmation");
assert(advertisedActions.includes("shrink_protection_size"),"existing tool advertises narrow typed single-OCO shrink");
assert(new Set(advertisedActions).size===advertisedActions.length,"advertised actions must be unique");
assert(EXECUTION_TOOLS[1] !== undefined && (EXECUTION_TOOLS[1] as any).name==="execution_action","typed write name");
