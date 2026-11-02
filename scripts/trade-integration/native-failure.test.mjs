import test from "node:test";
import assert from "node:assert/strict";
import { summarizeNativeWorkerdFailure } from "./native-failure.mjs";

test("nested assertion survives generic Command failed envelope", () => {
  const x = summarizeNativeWorkerdFailure({code:1,
    stdout:'T09_1_NATIVE_SQL_RESTART_V1:{"state":"ok"}',
    stderr:"Command failed: node test-runtime-do.mjs\nAssertionError [ERR_ASSERTION]: T09.7 Worker must reload exact settled budget\n    at file:///script.mjs:608:14"});
  assert.equal(x.error_class, "NODE_ASSERTION");
  assert.match(x.first_visible_error, /T09.7 Worker must reload/);
  assert.equal(x.phase, "T09_1_NATIVE_SQL_RESTART_V1");
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
