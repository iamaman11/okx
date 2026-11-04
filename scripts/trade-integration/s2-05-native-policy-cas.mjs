#!/usr/bin/env node
// #449 S2-05: one real Native SQL + persisted Worker governance CAS check.
// No live credentials, risk-policy override, test exchange or extra POST.
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { mkdir, writeFile } from "node:fs/promises";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { actualNativeWorker } from "./native-workerd.mjs";

const root = fileURLToPath(new URL("../../", import.meta.url));
const index = process.argv.indexOf("--artifacts");
const dir = index >= 0 ? process.argv[index + 1] : null;
if (index >= 0) assert.ok(dir, "artifacts directory required");
if (dir) await mkdir(dir, { recursive: true });
process.env.OKX_S2_05_POLICY_CAS_PROBE = "1";

try {
  // Standalone T09.5 transport, NOT statusProbe: Rust asserts !status_probe
  // and publishes ACK + semantic replay BEFORE Worker policy CAS.
  const transcript = await actualNativeWorker(root, AbortSignal.timeout(160_000));
  const native = transcript.nativePolicyReplay;
  const worker = transcript.workerPolicyCas;
  assert.ok(native && worker, "both Native and Worker S2-05 receipts required");
  assert.equal(native.source, "runtime_session_governance_cas_over_wss");
  assert.equal(native.original_intent_id, native.replayed_intent_id);
  assert.equal(native.original_request_hash, native.persisted_request_hash);
  assert.equal(native.original_deadline_ms, native.persisted_deadline_ms);
  assert.equal(native.new_governance_revision, 3);
  assert.equal(native.native_sql_revision, 3);
  assert.equal(native.native_sql_matches_governance, true);
  assert.equal(native.new_key_reason, "ACCOUNT_NEW_RISK_PAUSED");
  assert.equal(native.posts_before, 1);
  assert.equal(native.posts_after, 1);
  assert.equal(worker.account_paused, true);
  assert.equal(worker.persisted_worker_durable_object, true);
  assert.equal(worker.new_revision, worker.prior_revision + 1);
  assert.equal(worker.new_revision, native.new_governance_revision);
  const evidence = {
    schema: "okx.s2-05-standalone-native-worker/v1",
    status: "PASS",
    exact_requirement: "#449:S2-05",
    actual_native_sql: true,
    actual_worker_policy_cas: true,
    old_idempotency_replay_preserved: true,
    new_policy_key_denied: true,
    venue_posts_added_after_cas: 0,
    native, worker, transcript_sha256: transcript.transcript_sha256,
  };
  if (dir) await writeFile(resolve(dir, "s2-05-policy-cas.json"),
    JSON.stringify(evidence, null, 2) + "\n");
  process.stdout.write(JSON.stringify({
    schema: evidence.schema, status: evidence.status,
    old_replay: true, new_key_denied: true,
    post_count: native.posts_after, policy_revision: native.new_governance_revision,
  }) + "\n");
} catch (error) {
  if (dir) {
    const message = String(error?.message ?? "");
    await writeFile(resolve(dir, "s2-05-failure.json"),
      JSON.stringify({
        schema: "okx.s2-05-failure/v1", status: "FAIL",
        phase: "native_worker_policy_cas",
        error_sha256: createHash("sha256").update(message).digest("hex"),
        commit_sha: process.env.GITHUB_SHA ?? null,
      }, null, 2) + "\n");
  }
  throw error;
}
