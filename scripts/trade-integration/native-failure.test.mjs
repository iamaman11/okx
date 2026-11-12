import test from "node:test";
import assert from "node:assert/strict";
import { nativeDriverPrimaryCode, nativeDriverTeardownFailure, summarizeNativeWorkerdFailure, summarizeRustPanics, signedGetSourceFailure } from "./native-failure.mjs";

test("signed T09.2 source denial keeps the original typed cause without private routes", () => {
  const privateValue="PRIVATE_SENTINEL_87654321";
  const payload = {
    outcome:{status:"completed",data:{
      primary_reason:"COHORT_FINANCIAL_SOURCE_INCOMPLETE",
      state:"REJECTED", send_state:"NOT_STARTED", risk_release_state:"RELEASED",
    }},
    outbound_posts:3,
    denied_routes:[
      "TRANSCRIPT_SOURCE_PAGE_UNEXPECTED_OR_EXHAUSTED:fixture-executor-a:/api/v5/trade/order?clOrdId="+privateValue,
      "TRANSCRIPT_SOURCE_PAGE_UNEXPECTED_OR_EXHAUSTED:fixture-executor-a:/api/v5/trade/fills-history?instType=SPOT",
    ],
  };
  const source = "T09_2_SIGNED_GET_DENIAL_V1:"+JSON.stringify(payload)+"\n";
  const safe = signedGetSourceFailure(source);
  assert.equal(safe.parse_state,"VALID");
  assert.equal(safe.origin_case,"T09.2");
  assert.equal(safe.reason_code,"COHORT_FINANCIAL_SOURCE_INCOMPLETE");
  assert.equal(safe.send_state,"NOT_STARTED");
  assert.equal(safe.risk_release_state,"RELEASED");
  assert.equal(safe.outbound_posts,3);
  assert.equal(safe.denial_count,2);
  assert.deepEqual(safe.source_denial_codes,["TRANSCRIPT_SOURCE_PAGE_UNEXPECTED_OR_EXHAUSTED"]);
  assert.equal(JSON.stringify(safe).includes(privateValue),false);
  for(const malformed of [
    source.repeat(2),
    "T09_2_SIGNED_GET_DENIAL_V1:{not-json}",
    "T09_2_SIGNED_GET_DENIAL_V1:"+JSON.stringify({
      ...payload,outcome:{data:{primary_reason:privateValue}}
    }),
    "T09_2_SIGNED_GET_DENIAL_V1:"+JSON.stringify({...payload,denied_routes:[privateValue]})
  ]) {
    const refused=signedGetSourceFailure(malformed);
    assert.equal(refused.parse_state,"INVALID");
    assert.equal(JSON.stringify(refused).includes(privateValue),false);
  }
  assert.equal(signedGetSourceFailure("ordinary Native failure"),null);
});

const teardownFixture = () => ({schema:"okx.native-driver-teardown-failure/v1",
  primary_code:"LIFECYCLE_PROBE_PRIMARY_ERROR",
  failures:[{owner:"second",code:"LIFECYCLE_PROBE_POST_DISPOSE_INJECTED_ERROR"}],
  owners_closed:true,root_retained:false});
const teardownWire = value => "NATIVE_DRIVER_TEARDOWN_FAILURE_V1:"+JSON.stringify(value)+"\n";
test("Native teardown marker preserves only typed primary and cleanup facts", () => {
  const value=nativeDriverTeardownFailure({stderr:teardownWire(teardownFixture())});
  assert.equal(value.parse_state,"VALID");
  assert.equal(value.reason_code,"NATIVE_DRIVER_TEARDOWN_FAILED");
  assert.equal(value.primary_code,"LIFECYCLE_PROBE_PRIMARY_ERROR");
  assert.deepEqual(value.failures,[{owner:"second",code:"LIFECYCLE_PROBE_POST_DISPOSE_INJECTED_ERROR"}]);
  assert.equal(value.owners_closed,true);
  assert.equal(value.root_retained,false);
  const unjoined={...teardownFixture(),primary_code:null,owners_closed:false,root_retained:true,
    failures:[{owner:"native",code:"CHILD_JOIN_TIMEOUT"}]};
  assert.equal(nativeDriverTeardownFailure({stdout:teardownWire(unjoined)}).parse_state,"VALID");
  assert.equal(nativeDriverTeardownFailure({stderr:"ordinary scenario assertion"}),null);
  assert.equal(nativeDriverPrimaryCode(Error("WORKER_FRAME_TIMEOUT")),"WORKER_FRAME_TIMEOUT");
  assert.equal(nativeDriverPrimaryCode(Error("PRIVATE_SENTINEL_87654321")),"DRIVER_ASSERTION_OR_IO_FAILURE");
});
test("malformed duplicate oversized and untyped Native teardown markers fail closed privately", () => {
  const sentinel="PRIVATE_SENTINEL_87654321";
  const cases=[
    "NATIVE_DRIVER_TEARDOWN_FAILURE_V1:{invalid="+sentinel,
    teardownWire(teardownFixture()).repeat(2),
    "NATIVE_DRIVER_TEARDOWN_FAILURE_V1:"+"x".repeat(4096)+sentinel,
    teardownWire({...teardownFixture(),primary_code:sentinel}),
    teardownWire({...teardownFixture(),schema:sentinel}),
    teardownWire({...teardownFixture(),raw_body:{token:sentinel}}),
    teardownWire({...teardownFixture(),owners_closed:false,root_retained:false}),
    teardownWire({...teardownFixture(),failures:[]}),
    teardownWire({...teardownFixture(),failures:Array(6).fill({owner:"native",code:"CHILD_JOIN_TIMEOUT"})}),
    teardownWire({...teardownFixture(),failures:[{owner:sentinel,code:"CHILD_JOIN_TIMEOUT"}]}),
    teardownWire({...teardownFixture(),failures:[{owner:"second",code:sentinel}]}),
    teardownWire({...teardownFixture(),failures:[{owner:"second",code:"DRIVER_OWNER_CLOSE_FAILED",path:sentinel}]}),
    teardownWire({...teardownFixture(),failures:Array(2).fill({owner:"native",code:"CHILD_JOIN_TIMEOUT"})}),
  ];
  for(const stderr of cases){
    const result=nativeDriverTeardownFailure({stderr});
    assert.equal(result.parse_state,"INVALID");
    assert.equal(result.reason_code,"NATIVE_DRIVER_TEARDOWN_MARKER_INVALID");
    assert.equal(result.owners_closed,false);
    assert.equal(result.root_retained,true);
    assert.deepEqual(result.failures,[]);
    assert.ok(!JSON.stringify(result).includes(sentinel));
    assert.ok(!JSON.stringify(result).includes("raw_body"));
  }
});

test("a recovery-task panic survives the later public Status timeout", () => {
  const result = summarizeRustPanics({stderr:
    "thread 'tokio-runtime-worker' (10) panicked at crates\\okx-agent\\src\\trade_integration_t10_12.rs:381:5:\n" +
    "assertion failed: OLD_REPLY_PROBE_LOST\n\n" +
    "thread 'cloudflare_transport::tests::campaign' (11) panicked at crates\\okx-agent\\src\\cloudflare_transport.rs:2943:14:\n" +
    "T10-12 original public Status never reconciled after Worker reconnect: Elapsed(())\n"});
  assert.equal(result.count, 2);
  assert.equal(result.panics[0].location, "crates/okx-agent/src/trade_integration_t10_12.rs:381:5");
  assert.match(result.panics[0].reason, /OLD_REPLY_PROBE_LOST/);
  assert.match(result.panics[1].reason, /Elapsed/);
  assert.equal(result.cross_stream_order, "UNKNOWN");
});

test("Rust failure evidence is bounded and never emits private assertion payloads", () => {
  const sentinel = "PRIVATE_SENTINEL_87654321";
  const header = "thread 'worker' panicked at C:\\private\\repo\\crates\\okx-agent\\src\\risk_bridge.rs:39:9:\n";
  const result = summarizeRustPanics({stdout:(header + "raw_response={\"token\":\"" + sentinel + "\"}\n").repeat(30)});
  assert.equal(result.count, 30);
  assert.equal(result.panics.length, 16);
  assert.equal(result.omitted, 14);
  assert.equal(result.panics[0].reason, "DETAIL_REDACTED_OR_MISSING");
  assert.equal(result.panics.at(-1).ordinal, 29);
  assert.ok(!JSON.stringify(result).includes(sentinel));
  assert.ok(!JSON.stringify(result).includes("private"));
});

test("separate stdout and stderr failures do not acquire invented causal ordering", () => {
  const result = summarizeRustPanics({stdout:"thread 'first' panicked at crates/a.rs:1:2:\nFIRST_FAILURE\n",
    stderr:"thread 'other' panicked at crates/b.rs:3:4:\nOTHER_FAILURE\n"});
  assert.deepEqual(result.panics.map(x => [x.stream, x.ordinal]), [["stdout", 0], ["stderr", 0]]);
  assert.equal(result.cross_stream_order, "UNKNOWN");
  assert.equal(summarizeRustPanics({}).count, 0);
});

test("redacted source payload retains its fixed diagnostic code, never the route or body", () => {
  const x=summarizeRustPanics({stderr:
    "thread 'campaign' panicked at crates/okx-agent/src/trade_integration_t10_09.rs:400:13:\n"+
    // OkxError::Response Display in crates/okx-api/src/error.rs and the
    // RecordedAccountHttp exhausted-source error in accounts.rs.
    'T10-09 authenticated terminal B order readback: "OKX response error: TRANSCRIPT_SOURCE_PAGE_UNEXPECTED_OR_EXHAUSTED:fixture-observer-b:/api/v5/account/balance?ccy=USDT:method=GET:task=Some(Id(42)):clOrdId=None"\n'});
  assert.equal(x.panics[0].reason,"TRANSCRIPT_SOURCE_PAGE_UNEXPECTED_OR_EXHAUSTED");
  assert.ok(!JSON.stringify(x).includes("/api/v5/"));
  const privateError=summarizeRustPanics({stderr:
    "thread 'campaign' panicked at crates/a.rs:1:1:\n"+
    'raw_response={"token":"TRANSCRIPT_EXHAUSTED:PRIVATE_VALUE"}\n'});
  assert.equal(privateError.panics[0].reason,"DETAIL_REDACTED_OR_MISSING");
});

test("nested assertion survives generic Command failed envelope", () => {
  const x = summarizeNativeWorkerdFailure({code:1,
    stdout:'T09_1_NATIVE_SQL_RESTART_V1:{"state":"ok"}',
    stderr:"Command failed: node test-runtime-do.mjs\nAssertionError [ERR_ASSERTION]: T09.7 Worker must reload exact settled budget\n    at file:///C:/sensitive/private/test-runtime-do.mjs:608:14"});
  assert.equal(x.error_class, "NODE_ASSERTION");
  assert.match(x.first_visible_error, /T09.7 Worker must reload/);
  assert.equal(x.phase, "T09_1_NATIVE_SQL_RESTART_V1");
  assert.equal(x.workerd_test_line, 608);
  assert.ok(!JSON.stringify(x).includes("sensitive/private"));
  assert.equal(x.outer_exit_code, "1");
});
test("secret and source-bearing diagnostic lines must not be emitted", () => {
  const sentinel="SENTINEL_PRIVATE_SIGNATURE_87654321";
  const x=summarizeNativeWorkerdFailure({code:1,
    stderr:"Error: Authorization: Bearer "+sentinel+"\nAssertionError [ERR_ASSERTION]: raw_response={\"api_key\":\""+sentinel+"\"}",
    stdout:"T09_2_PHASE_V1:AFTER_SQL_READBACK\nT09_6_NATIVE_TERMINAL_V1:{\"signed_header\":\""+sentinel+"\"}"});
  assert.equal(x.error_class,"UNCLASSIFIED");
  assert.equal(x.first_visible_error,"INNER_CHILD_EXIT_WITHOUT_SAFE_ASSERTION");
  assert.equal(x.phase,"T09_6_NATIVE_TERMINAL_V1");
  assert.ok(!JSON.stringify(x).includes(sentinel));
  assert.ok(!JSON.stringify(x).includes("Authorization"));
  assert.equal(x.workerd_test_line, null);
  assert.match(x.output_sha256,/^[a-f0-9]{64}$/);
});
test("a wrapper-only exit is not mistaken for a Rust failure", () => {
  const x=summarizeNativeWorkerdFailure({code:"ETIMEDOUT",
    stderr:"Command failed: C:\\Program Files\\nodejs\\node.exe\nCompiling okx-agent v0.1",
    stdout:"T09_2_PHASE_V1:AFTER_WORKER_READBACK"});
  assert.equal(x.error_class,"UNCLASSIFIED");
  assert.equal(x.outer_exit_code,"UNKNOWN");
  assert.equal(x.phase,"T09_2_PHASE_V1");
});

test("managed Add missing intent retains the exact safe refusal without response payload", () => {
  const response = {status:"completed", data:{state:"REJECTED", primary_reason:"BUDGET_BOUND_BREACH",
    rejection_origin:"CENTRAL",send_state:"NOT_STARTED",intent_id:"a".repeat(64)},
    raw_response:{api_key:"PRIVATE_SENTINEL_87654321"}};
  const result=summarizeRustPanics({stderr:
    "thread 'campaign' panicked at crates/okx-agent/src/trade_integration_t09_4.rs:959:13:\n"+
    "T09_4_MANAGED_ADD_MISSING_INTENT_V1:"+JSON.stringify(response)+"\n"});
  assert.match(result.panics[0].reason,/REJECTED:BUDGET_BOUND_BREACH:CENTRAL:NOT_STARTED/);
  assert.deepEqual(result.panics[0].detail,{marker:"T09_4_MANAGED_ADD_MISSING_INTENT_V1",parse_state:"PARSED",
    status:"completed",state:"REJECTED",reason_code:"BUDGET_BOUND_BREACH",origin:"CENTRAL",send_state:"NOT_STARTED",
    intent_id_present:true,intent_id_length:64,intent_id_format:"HEX_64"});
  assert.ok(!JSON.stringify(result).includes("PRIVATE_SENTINEL"));
  assert.ok(!JSON.stringify(result).includes("a".repeat(64)));
});

test("managed Add diagnostic rejects arbitrary values even in selected fields", () => {
  const sentinel="PRIVATE_SENTINEL_87654321";
  const result=summarizeRustPanics({stderr:
    "thread 'campaign' panicked at crates/a.rs:1:2:\nT09_4_MANAGED_ADD_MISSING_INTENT_V1:"+
    JSON.stringify({status:sentinel,data:{state:sentinel,reason_code:sentinel,origin:sentinel,send_state:sentinel,
      intent_id:sentinel,request:{credential:sentinel}}})+"\n"});
  const detail=result.panics[0].detail;
  assert.equal(detail.status,null);assert.equal(detail.state,null);assert.equal(detail.reason_code,null);
  assert.equal(detail.origin,null);assert.equal(detail.send_state,null);
  assert.equal(detail.intent_id_format,"OTHER");assert.equal(detail.intent_id_length,sentinel.length);
  assert.ok(!JSON.stringify(result).includes(sentinel));
  const malformed=summarizeRustPanics({stderr:
    "thread 'campaign' panicked at crates/a.rs:1:2:\nT09_4_MANAGED_ADD_MISSING_INTENT_V1:{raw_response="+sentinel+"\n"});
  assert.equal(malformed.panics[0].detail.parse_state,"INVALID_JSON");
  assert.ok(!JSON.stringify(malformed).includes(sentinel));
});

test("Rust count assertion preserves integers but never string assertion payloads", () => {
  const header="thread 'campaign' panicked at crates/okx-agent/src/trade_integration_t10_02.rs:123:5:\n";
  const numeric=summarizeRustPanics({stderr:header+
    "assertion `left == right` failed: T10-02 follows one real managed Add\n  left: 6\n right: 5\n"});
  assert.deepEqual(numeric.panics[0].integer_comparison,{left:6,right:5});
  const privateValues=summarizeRustPanics({stderr:header+
    'assertion `left == right` failed\n left: "SECRET_SENTINEL"\n right: "5"\n'});
  assert.equal(privateValues.panics[0].integer_comparison,undefined);
  assert.ok(!JSON.stringify(privateValues).includes("SECRET_SENTINEL"));
});

test("unexpected T10-02 posts preserve only bounded typed identities before the panic", () => {
  const record={posts_before:5,posts_after:6,reason_code:"UNATTRIBUTED_EXCHANGE_POSITION",state:"REJECTED",origin:"LOCAL",send_state:"NOT_STARTED",
    posts:[{ordinal:6,route:"/api/v5/trade/cancel-order",client_id_sha256:"a".repeat(64),algo_id_sha256:null,
      reduce_only:true,side:"sell",size:"0.20",raw_body:{api_key:"SECRET_SENTINEL"}}]};
  const result=summarizeRustPanics({stderr:"T10_02_UNEXPECTED_POSTS_V1:"+JSON.stringify(record)+"\n"+
    "thread 'campaign' panicked at crates/okx-agent/src/trade_integration_t10_02.rs:168:5:\n"+
    "assertion `left == right` failed\n left: 6\n right: 5\n"});
  const detail=result.diagnostics[0].detail;
  assert.equal(detail.posts_before,5);assert.equal(detail.posts_after,6);
  assert.equal(detail.posts[0].route,"/api/v5/trade/cancel-order");
  assert.equal(detail.posts[0].client_id_sha256,"a".repeat(64));
  assert.equal(detail.posts[0].reduce_only,true);assert.equal(detail.posts[0].size,"0.20");
  assert.equal(detail.reason_code,"UNATTRIBUTED_EXCHANGE_POSITION");
  assert.ok(!JSON.stringify(result).includes("SECRET_SENTINEL"));
  assert.ok(!JSON.stringify(result).includes("raw_body"));
});

test("unexpected post diagnostics reject free text and bound arrays and repeated markers", () => {
  const sentinel="SECRET_SENTINEL";
  const record={posts_before:-1,posts_after:Infinity,reason_code:sentinel,state:sentinel,origin:sentinel,send_state:sentinel,
    posts:Array.from({length:20},()=>({route:"https://private.invalid/"+sentinel,client_id_sha256:sentinel,
      algo_id_sha256:sentinel,reduce_only:sentinel,side:sentinel,size:sentinel}))};
  const result=summarizeRustPanics({stdout:("T10_02_UNEXPECTED_POSTS_V1:"+JSON.stringify(record)+"\n").repeat(10)});
  assert.equal(result.diagnostics.length,8);assert.equal(result.diagnostics_omitted,2);
  const detail=result.diagnostics[0].detail;
  assert.equal(detail.posts.length,12);assert.equal(detail.posts_omitted,8);
  assert.equal(detail.posts_before,null);assert.equal(detail.posts_after,null);
  assert.equal(detail.posts[0].route,null);assert.equal(detail.posts[0].client_id_sha256,null);
  assert.equal(detail.posts[0].reduce_only,null);assert.equal(detail.posts[0].size,null);
  assert.ok(!JSON.stringify(result).includes(sentinel));
});

test("T09.6 reports the exact bounded POST delta and causal boundary verdict", () => {
  const marker="T09_6_STATUS_POSTS_V1:";
  const record={posts_before:7,posts_after:8,phase:"CLOSE_TERMINAL_STATUS",
    boundary_verdict:"EXPECTED_CANCEL",reason_code:"TRANSCRIPT_STATUS_POST_EXPECTED_CANCEL",
    posts:[{ordinal:8,actor_scope:"ACCOUNT_A_EXECUTOR",route:"/api/v5/trade/cancel-algos",
      algo_id_sha256:"b".repeat(64),client_id_sha256:null,
      raw_body:{credential:"SECRET_SENTINEL"}}]};
  const result=summarizeRustPanics({stderr:marker+JSON.stringify(record)});
  const detail=result.diagnostics[0].detail;
  assert.equal(detail.phase,"CLOSE_TERMINAL_STATUS");
  assert.equal(detail.boundary_verdict,"EXPECTED_CANCEL");
  assert.equal(detail.posts[0].actor_scope,"ACCOUNT_A_EXECUTOR");
  assert.equal(detail.posts[0].algo_id_sha256,"b".repeat(64));
  assert.equal(detail.posts[0].route,"/api/v5/trade/cancel-algos");
  assert.equal(detail.reason_code,"TRANSCRIPT_STATUS_POST_EXPECTED_CANCEL");
  assert.ok(!JSON.stringify(result).includes("SECRET_SENTINEL"));
  const hostile=summarizeRustPanics({stderr:marker+JSON.stringify({...record,
    phase:"SECRET_SENTINEL",boundary_verdict:"SECRET_SENTINEL",
    posts:[{actor_scope:"SECRET_SENTINEL"}]})});
  assert.equal(hostile.diagnostics[0].detail.phase,null);
  assert.equal(hostile.diagnostics[0].detail.boundary_verdict,null);
  assert.equal(hostile.diagnostics[0].detail.posts[0].actor_scope,null);
  assert.ok(!JSON.stringify(hostile).includes("SECRET_SENTINEL"));
});
