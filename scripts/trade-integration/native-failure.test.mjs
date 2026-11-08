import test from "node:test";
import assert from "node:assert/strict";
import { summarizeNativeWorkerdFailure, summarizeRustPanics } from "./native-failure.mjs";

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
