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
const memo = new Map();
function receipt(stdout, prefix) {
  const matches = [...stdout.matchAll(new RegExp(prefix + ":(\\{[^\\r\\n]+\\})", "g"))];
  assert.equal(matches.length, 1, prefix + " must have one authentic child readback");
  return JSON.parse(matches[0][1]);
}
export async function actualNativeWorker(root, signal, { statusProbe = false, secondAdmission = false } = {}) {
  assert.ok(!secondAdmission || statusProbe, "T09.2 needs the completed terminal T09.1 readback");
  const key = secondAdmission ? "second-admission" : statusProbe ? "post-fill-status" : "baseline";
  if (!memo.has(key)) memo.set(key, (async () => {
    const { stdout, stderr } = await exec(
      process.execPath,
      [resolve(root, "cloudflare", "okx-mcp", "scripts", "test-runtime-do.mjs")],
      {
        cwd: root,
        env: {
          ...process.env, OKX_NATIVE_WORKER_INTEGRATION: "1",
          OKX_T09_1_STATUS_PROBE: statusProbe ? "1" : "0",
          OKX_T09_2_ADMISSION_PROBE: secondAdmission ? "1" : "0",
        },
        signal,
        timeout: 115_000,
        maxBuffer: 12 * 1024 * 1024,
        windowsHide: true,
      },
    ).catch(error => {
      // The Rust child prints its exact failing assertion to stderr. The
      // integration runner retains only 200 chars of an error reason, so
      // forwarding "Command failed: node ..." discards the actual blocker.
      // Prefer the final production assertion, then a Rust panic line.
      const stderr = String(error.stderr ?? "");
      const recorded = stderr.match(/T09_1_CLOSE_ERROR_FROM_RUST_V1:(\{[^\r\n]+\})/);
      if (recorded) {
        const source = JSON.parse(recorded[1]);
        // Preserve the exact typed rate domain, local defer and request-sent
        // fields in the CI log. They decide whether a SOURCE GET or venue
        // ORDER was throttled; a 90-byte reason hid this distinction.
        const detail = {
          schema: "okx.t09-close-denial/v1",
          reason_code: String(source.reason_code).slice(0, 1200),
          source_reason_code: source.source_reason_code ?? null,
          outbound_posts: source.outbound_posts,
          unconsumed_recorded_posts: source.unconsumed_recorded_posts ?? null,
          last_outbound_post: source.last_outbound_post ?? null,
          close_status: source.close_result?.status ?? null,
          close_reason_code: source.close_result?.reason_code ?? null,
        };
        process.stderr.write("T09_1_CLOSE_DENIAL_EVIDENCE_V1:" +
          JSON.stringify(detail) + "\n");
        throw Error("T09_1_PRODUCTION_CLOSE_DENIED:" +
          String(source.reason_code).slice(0, 90) +
          ":posts=" + source.outbound_posts +
          ":capture=" + String(source.source_reason_code ?? "NONE").slice(0, 45));
      }
      const terminal = stderr.match(/T09_1_TERMINAL_CAPTURE_FAILURE_V1:(\{[^\r\n]+\})/);
      if (terminal) {
        const detail = JSON.parse(terminal[1]);
        process.stderr.write("T09_1_TERMINAL_CAPTURE_DENIAL_V1:" +
          JSON.stringify(detail) + "\n");
        throw Error("T09_1_TERMINAL_CAPTURE_FAILED:" +
          String(detail.source_reason).slice(0, 110));
      }
      // The nested workerd script has access to cargo's stdout AND stderr.
      // Preserve its bounded last Rust assertion instead of re-wrapping the
      // unhelpful Node "Command failed" prefix. No raw HTTP bodies or secrets.
      const childFailure = stderr.match(/T09_1_NATIVE_CHILD_FAILURE_V2:(\{[^\r\n]+\})/);
      if (childFailure) {
        const detail = JSON.parse(childFailure[1]);
        process.stderr.write("T09_1_NATIVE_CHILD_FAILURE_V2:" +
          JSON.stringify(detail) + "\n");
        throw Error("NATIVE_RUST_ASSERTION:" +
          String(detail.rust_assertion).slice(0, 1200));
      }
      // Preserve deterministic Native phase markers and the complete final
      // Rust assertion in the CI job log. The short summary reason alone
      // previously reduced typed no-send evidence to "max_requests: 10".
      // Do not include raw HTTP bodies, signed headers, or environment data.
      const stdout = String(error.stdout ?? "");
      const phaseMarkers = [...stdout.matchAll(
        /(T09_1_(?:NATIVE_[A-Z0-9_]+|TERMINAL_[A-Z0-9_]+|RECORDED_[A-Z0-9_]+))_V1:/g,
      )].map(m => m[1]).slice(-15);
      const lines = stderr.split(/\r?\n/).map(line => line.trim()).filter(Boolean);
      const panic = lines.findLastIndex(line => /panicked at|thread '.+' panicked/.test(line));
      const failure = panic >= 0
        ? lines.slice(panic, panic + 5).join(" ")
        : lines.findLast(line => /T09_1_NATIVE_CHILD_EXCEPTION_V1:|Error:|error:|FAILED/.test(line))
          ?? error.message;
      process.stderr.write("T09_1_NATIVE_FAILURE_PHASE_V1:" + JSON.stringify({
        phases: phaseMarkers,
        cause: String(failure).slice(0, 2000),
      }) + "\n");
      throw Error("NATIVE_WORKER_CHILD_FAILED:" + String(failure).slice(0, 135));
    });
    assert.match(stdout, /t09_public_trade_execute_open_ack_and_idempotency PASS: posts=1/);
    assert.match(stdout, /native_to_persisted_workerd_riskbook PASS/);
    const terminal = [...stdout.matchAll(/\{"suite":"runtime-session-sqlite-do","status":"PASS"[^\r\n]*\}/g)];
    assert.equal(terminal.length, 1, "production Durable Object test terminal PASS missing");
    const done = JSON.parse(terminal[0][0]);
    assert.equal(done.real_durable_object, true);
    assert.equal(done.persisted_generation, true);
    return {
      nativeBootstrap: receipt(stdout, "T09_0_NATIVE_SQL_EVIDENCE_V1"),
      workerBootstrap: receipt(stdout, "T09_0_WORKER_EVIDENCE_V1"),
      nativeReplay: receipt(stdout, "T09_5_NATIVE_IDEMPOTENCY_EVIDENCE_V1"),
      nativeFinancial: receipt(stdout, "T09_1_NATIVE_FILL_BILL_EVIDENCE_V1"),
      ...(statusProbe
        ? {
          nativePostFill: receipt(stdout, "T09_1_NATIVE_POST_FILL_STATUS_V1"),
          nativeClose: receipt(stdout, "T09_1_NATIVE_CLOSE_ACK_V1"),
          nativeTerminal: receipt(stdout, "T09_1_NATIVE_TERMINAL_CLOSE_V1"),
          nativeRestart: receipt(stdout, "T09_1_NATIVE_SQL_RESTART_V1"),
          ...(secondAdmission ? {
            nativeSecondAdmission: receipt(stdout, "T09_2_NATIVE_ADMISSION_V1"),
            workerSecondAdmission: receipt(stdout, "T09_2_WORKER_AFTER_RECONNECT_V1"),
          } : { workerPostFill: receipt(stdout, "T09_1_WORKER_POST_FILL_EVIDENCE_V1") }),
        }
        : { workerReplay: receipt(stdout, "T09_5_WORKER_IDEMPOTENCY_EVIDENCE_V1") }),
      transcript_sha256: createHash("sha256").update(stdout + stderr).digest("hex"),
    };
  })());
  return memo.get(key);
}
export { hash };
