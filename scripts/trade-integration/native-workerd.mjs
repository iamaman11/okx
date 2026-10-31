// One immutable integration transcript per CI run, shared by T09.0 and
// T09.5. This starts the EXISTING Rust Native + production RuntimeSession/DO
// test. No exchange service, risk authority or monetary truth is implemented
// in this file. The child only emits receipts after its own assertions pass.
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { resolve } from "node:path";

const exec = promisify(execFile);
const hash = value => createHash("sha256").update(JSON.stringify(value)).digest("hex");
let memo;
function receipt(stdout, prefix) {
  const matches = [...stdout.matchAll(new RegExp(prefix + ":(\\{[^\\r\\n]+\\})", "g"))];
  assert.equal(matches.length, 1, prefix + " must have one authentic child readback");
  return JSON.parse(matches[0][1]);
}
export async function actualNativeWorker(root, signal) {
  if (!memo) memo = (async () => {
    const { stdout, stderr } = await exec(
      process.execPath,
      [resolve(root, "cloudflare", "okx-mcp", "scripts", "test-runtime-do.mjs")],
      {
        cwd: root,
        env: { ...process.env, OKX_NATIVE_WORKER_INTEGRATION: "1" },
        signal,
        timeout: 115_000,
        maxBuffer: 12 * 1024 * 1024,
        windowsHide: true,
      },
    );
    assert.match(stdout, /t09_public_trade_execute_open_ack_and_idempotency PASS: posts=1/);
    assert.match(stdout, /native_to_persisted_workerd_riskbook PASS/);
    const terminal = [...stdout.matchAll(/\\{"suite":"runtime-session-sqlite-do","status":"PASS"[^\\r\\n]*\\}/g)];
    assert.equal(terminal.length, 1, "production Durable Object test terminal PASS missing");
    const done = JSON.parse(terminal[0][0]);
    assert.equal(done.real_durable_object, true);
    assert.equal(done.persisted_generation, true);
    return {
      nativeBootstrap: receipt(stdout, "T09_0_NATIVE_SQL_EVIDENCE_V1"),
      workerBootstrap: receipt(stdout, "T09_0_WORKER_EVIDENCE_V1"),
      nativeReplay: receipt(stdout, "T09_5_NATIVE_IDEMPOTENCY_EVIDENCE_V1"),
      workerReplay: receipt(stdout, "T09_5_WORKER_IDEMPOTENCY_EVIDENCE_V1"),
      transcript_sha256: createHash("sha256").update(stdout + stderr).digest("hex"),
    };
  })();
  return memo;
}
export { hash };
