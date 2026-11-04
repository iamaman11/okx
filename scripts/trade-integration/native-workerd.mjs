// One immutable integration transcript per CI run, shared by T09.0 and
// T09.5. This starts the EXISTING Rust Native + production RuntimeSession/DO
// test. No exchange service, risk authority or monetary truth is implemented
// in this file. The child only emits receipts after its own assertions pass.
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { resolve } from "node:path";
import { summarizeNativeWorkerdFailure } from "./native-failure.mjs";

const exec = promisify(execFile);
const hash = value => createHash("sha256").update(JSON.stringify(value)).digest("hex");
const memo = new Map();
const cohortMetrics = new Map();
function receipt(stdout, prefix) {
  const matches = [...stdout.matchAll(new RegExp(prefix + ":(\\{[^\\r\\n]+\\})", "g"))];
  assert.equal(matches.length, 1, prefix + " must have one authentic child readback");
  return JSON.parse(matches[0][1]);
}
export async function actualNativeWorker(root, signal, { statusProbe = false, secondAdmission = false, partialFills = false, managedAdd = false, closeMode = false, budgetDenial = false } = {}) {
  assert.ok(!secondAdmission || statusProbe, "T09.2 needs the completed terminal T09.1 readback");
  if (partialFills && !secondAdmission) throw Error("T09.3_REQUIRES_T09.2");
  if (managedAdd && !partialFills) throw Error("T09.4_REQUIRES_T09.3");
  if (closeMode && !managedAdd) throw Error("T09.6_REQUIRES_T09.4");
  if (budgetDenial && !closeMode) throw Error("T09.8_REQUIRES_T09.6");
  // A failing T09.8 must not invalidate independently completed T09.6/7
  // receipts. Both modes still execute the complete Native+Worker cohort.
  const mode = closeMode ? (budgetDenial ? "managed-close-denial" : "managed-close") : managedAdd ? "managed-add" : partialFills ? "partial-fills" : secondAdmission ? "second-admission" : statusProbe ? "post-fill-status" : "baseline";
  // Financial evidence must never be memoized across different opt-in probes.
  const key = [mode,
    process.env.OKX_S2_05_POLICY_CAS_PROBE === "1" ? "policy-cas" : "normal-policy",
    process.env.OKX_B1_NATIVE_CAP_PROBE === "1" ? "b1-cap" : "normal-cap",
    process.env.OKX_B1_SECOND_POSITIVE_PROBE === "1" ? "b1-positive" : "normal-b",
    process.env.OKX_T10_14_NATIVE_PROBE === "1" ? "t10-14" : "no-t10-14",
    process.env.OKX_T10_09_LATE_FILL_PROBE === "1" ? "t10-09" : "no-t10-09",
    process.env.OKX_T10_06_CONCURRENT_PROBE === "1" ? "t10-06" : "no-t10-06",
    process.env.OKX_T10_07_NATIVE_PROBE === "1" ? "t10-07" : "no-t10-07",
    process.env.OKX_T09_MA_CONCURRENT_PROBE === "1" ? "t09-ma" : "no-t09-ma",
    process.env.OKX_T10_02_EXTERNAL_EXPOSURE_PROBE === "1" ? "t10-02" : "no-t10-02",
    process.env.OKX_T10_04_FOREIGN_WORKER_REPLY_PROBE === "1" ? "t10-04" : "no-t10-04",
    process.env.OKX_T09_SPEC_SECOND_LINEAR_PROBE === "1" ? "t09-spec" : "no-t09-spec",
    process.env.OKX_T10_05_LOST_ACK_PROBE === "1" ? "t10-05-lost" : "no-t10-05-lost",
    process.env.OKX_T10_05_EXPLICIT_REJECT_PROBE === "1" ? "t10-05-reject" : "no-t10-05-reject",
    process.env.OKX_T10_15_POST_HELD_SOURCE_PROBE === "1" ? "t10-15" : "no-t10-15",
    process.env.OKX_T10_03_POST_AUTH_REPLY_LOST_PROBE === "1" ? "t10-03" : "no-t10-03",
    process.env.OKX_T10_01_PARTIAL_SOURCE_RECOVERY_PROBE === "1" ? "t10-01" : "no-t10-01",
    process.env.OKX_T10_13_STOP_PROBE === "1" ? "t10-13" : "no-t10-13",
    process.env.OKX_T10_10_FINANCE_REPLAY_PROBE === "1" ? "t10-10" : "no-t10-10",
  ].join(":");
  const reused = memo.has(key);
  if (!reused) {
    const startedAt = Date.now();
    memo.set(key, (async () => {
    const { stdout, stderr } = await exec(
      process.execPath,
      [resolve(root, "cloudflare", "okx-mcp", "scripts", "test-runtime-do.mjs")],
      {
        cwd: root,
        env: {
          ...process.env, OKX_NATIVE_WORKER_INTEGRATION: "1",
          OKX_T09_1_STATUS_PROBE: statusProbe ? "1" : "0",
          OKX_T09_2_ADMISSION_PROBE: secondAdmission ? "1" : "0",
          OKX_T09_3_PARTIAL_PROBE: partialFills ? "1" : "0",
          OKX_T09_4_MANAGED_ADD_PROBE: managedAdd ? "1" : "0",
          OKX_T09_6_CLOSE_PROBE: closeMode ? "1" : "0",
          OKX_T09_8_DENIAL_PROBE: budgetDenial ? "1" : "0",
        },
        signal,
        timeout: closeMode ? 240_000 : 115_000,
        maxBuffer: 12 * 1024 * 1024,
        windowsHide: true,
      },
    ).catch(error => {
      // The Rust child prints its exact failing assertion to stderr. The
      // integration runner retains only 200 chars of an error reason, so
      // forwarding "Command failed: node ..." discards the actual blocker.
      // Prefer the final production assertion, then a Rust panic line.
      const stderr = String(error.stderr ?? "");
      const signedGetDenial = stderr.match(/T09_2_SIGNED_GET_DENIAL_V1:(\{[^\r\n]+\})/);
      if (signedGetDenial) {
        const diagnostic = JSON.parse(signedGetDenial[1]);
        process.stderr.write("T09_2_SIGNED_GET_DENIAL_V1:" +
          JSON.stringify(diagnostic) + "\n");
      }
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
      const b1Transport = stderr.match(/B1_POSITIVE_TRANSPORT_FAILURE_V1:(\{[^\r\n]+\})/);
      if (b1Transport) {
        const diagnostic = JSON.parse(b1Transport[1]);
        process.stderr.write("B1_POSITIVE_TRANSPORT_FAILURE_V1:" +
          JSON.stringify(diagnostic) + "\n");
        const failure = Error("B1_TRANSPORT_FIRST_CAUSE:" +
          String(diagnostic.reason_code).slice(0, 90));
        failure.b1Diagnostic = diagnostic;
        throw failure;
      }
      const b1Cause = stderr.match(/B1_POSITIVE_DENIAL_EVIDENCE_V1:(\{[^\r\n]+\})/);
      if (b1Cause) {
        const detail = JSON.parse(b1Cause[1]);
        process.stderr.write("B1_POSITIVE_DENIAL_EVIDENCE_V1:" +
          JSON.stringify(detail) + "\n");
        throw Error("B1_POSITIVE_REFUSED:" + String(detail.reason).slice(0,170) +
          ":origin=" + String(detail.origin) + ":send=" + String(detail.send_state) +
          ":posts=" + String(detail.posts));
      }
      const childFailure = stderr.match(/T09_1_NATIVE_CHILD_FAILURE_V2:(\{[^\r\n]+\})/);
      if (childFailure) {
        const detail = JSON.parse(childFailure[1]);
        process.stderr.write("T09_1_NATIVE_CHILD_FAILURE_V2:" +
          JSON.stringify(detail) + "\n");
        throw Error("NATIVE_RUST_ASSERTION:" +
          String(detail.rust_assertion).slice(0, 1200));
      }
      // Extract only bounded economic facts from the real persisted Worker
      // mismatch. Previously double-escaped braces prevented this receipt
      // from matching; the outer runner hid the true monetary divergence.
      const moneyMismatch = stderr.match(
        /T09_2_POST_NATIVE_BUDGET_MISMATCH_V1:(\{[^\r\n]+\})/
      );
      if (moneyMismatch) {
        const evidence = JSON.parse(moneyMismatch[1]);
        const decimal = value => typeof value === "string" &&
          /^(?:0|[1-9][0-9]{0,10})(?:\.[0-9]{1,9})?$/.test(value)
          ? value : null;
        const nativeEntry = evidence.native_worker_budget?.entry_budget ?? {};
        const detail = {
          schema: "okx.t09-worker-budget-mismatch/v1",
          expected_reserved_usd: decimal(evidence.expected_reserved_usd),
          actual_reserved_usd: decimal(evidence.actual_reserved_usd),
          expected_spent_usd: decimal(evidence.expected_spent_usd),
          actual_spent_usd: decimal(evidence.actual_spent_usd),
          native_reserved_usd: decimal(nativeEntry.reserved_usd),
          native_spent_usd: decimal(nativeEntry.consumed_usd),
          worker_risk_book_sequence: evidence.worker_risk_book_sequence ?? null,
          native_risk_book_sequence: evidence.native_risk_book_sequence ?? null,
        };
        process.stderr.write("T09_2_POST_NATIVE_BUDGET_MISMATCH_V1:" +
          JSON.stringify(detail) + "\n");
        throw Error("T09_2_POST_NATIVE_BUDGET_MISMATCH:" +
          String(detail.actual_reserved_usd) + ":expected=" +
          String(detail.expected_reserved_usd) + ":spent=" +
          String(detail.actual_spent_usd));
      }
      // Preserve only bounded, secret-free evidence from the nested child.
      // The previous fallback hid its assertion behind "Command failed".
      const evidence = summarizeNativeWorkerdFailure(error);
      process.stderr.write("T09_NATIVE_NESTED_FAILURE_V1:" +
        JSON.stringify(evidence) + "\n");
      throw Error("NATIVE_WORKER_CHILD_FAILED:" + evidence.error_class +
        ":" + evidence.phase + ":" + evidence.first_visible_error.slice(0, 70));
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
      ...(process.env.OKX_T10_06_CONCURRENT_PROBE === "1" ? {
        nativeT10Cold: receipt(stdout, "T10_06_NATIVE_COLD_CONCURRENT_V1"),
        nativeT10Concurrency: receipt(stdout, "T10_06_NATIVE_CONCURRENT_V1"),
      } : {}),
      nativeAccountScope: receipt(stdout, "S2_07_NATIVE_ACCOUNT_SCOPE_V1"),
      ...(process.env.OKX_S2_05_POLICY_CAS_PROBE === "1" ? {
        nativePolicyReplay: receipt(stdout, "S2_05_NATIVE_POLICY_REPLAY_V1"),
        workerPolicyCas: receipt(stdout, "S2_05_WORKER_POLICY_CAS_V1"),
      } : {}),
      nativeFinancial: receipt(stdout, "T09_1_NATIVE_FILL_BILL_EVIDENCE_V1"),
      ...(statusProbe
        ? {
          nativePostFill: receipt(stdout, "T09_1_NATIVE_POST_FILL_STATUS_V1"),
          nativeClose: receipt(stdout, "T09_1_NATIVE_CLOSE_ACK_V1"),
          nativeTerminal: receipt(stdout, "T09_1_NATIVE_TERMINAL_CLOSE_V1"),
          nativeRestart: receipt(stdout, "T09_1_NATIVE_SQL_RESTART_V1"),
          ...(secondAdmission ? {
            nativeSecondAdmission: receipt(stdout, "T09_2_NATIVE_ADMISSION_V1"),
            nativeCrossUidDenial: receipt(stdout, "T09_MA_CROSS_UID_DENIAL_V1"),
            nativeStrategyAccountPreflight: receipt(stdout, "T09_MA_NATIVE_SQL_STRATEGY_PREFLIGHT_V1"),
            workerSecondAdmission: receipt(stdout, "T09_2_WORKER_AFTER_RECONNECT_V1"),
            ...(process.env.OKX_B1_NATIVE_CAP_PROBE === "1" ? {
              nativeB1Cap: receipt(stdout, "B1_NATIVE_SHARED_CAP_DENIAL_V1"),
            } : {}),
            ...(process.env.OKX_B1_SECOND_POSITIVE_PROBE === "1" ? {
              nativeB1Positive: receipt(stdout, "B1_NATIVE_SECOND_ACCOUNT_ACCEPTED_V1"),
            } : {}),
            ...(process.env.OKX_T09_MA_CONCURRENT_PROBE === "1" ? {
              nativeMAConcurrent: receipt(stdout, "T09_MA_CONCURRENT_PUBLIC_V1"),
            } : {}),
            ...(process.env.OKX_T10_14_NATIVE_PROBE === "1" ? {
              nativeT10Isolation: receipt(stdout, "T10_14_NATIVE_ISOLATION_V1"),
            } : {}),
            ...(process.env.OKX_T10_09_LATE_FILL_PROBE === "1" ? {
              nativeT10LateFill: receipt(stdout, "T10_09_NATIVE_LATE_B_FILL_V1"),
            } : {}),
            ...(partialFills ? {
              nativePartialFills: receipt(stdout, "T09_3_NATIVE_PARTIAL_TERMINAL_V1"),
              nativeTerminalSettlement: receipt(stdout, "T09_3_TERMINAL_SETTLEMENT_RECONCILED_V1"),
            } : {}),
            ...(managedAdd ? { nativeManagedAdd: receipt(stdout, "T09_4_NATIVE_MANAGED_ADD_V1") } : {}),
            ...(process.env.OKX_T10_02_EXTERNAL_EXPOSURE_PROBE === "1" ? {
              nativeT10Unowned: receipt(stdout, "T10_02_NATIVE_UNOWNED_EXPOSURE_V1"),
            } : {}),
            ...(process.env.OKX_T10_04_FOREIGN_WORKER_REPLY_PROBE === "1" ? {
              nativeT10ForeignReply: receipt(stdout, "T10_04_NATIVE_FOREIGN_WORKER_REPLY_V1"),
              workerT10ForeignReply: receipt(stdout, "T10_04_WORKER_FOREIGN_REPLY_V1"),
            } : {}),
            ...(process.env.OKX_T09_SPEC_SECOND_LINEAR_PROBE === "1" ? {
              nativeSecondarySpec: receipt(stdout, "T09_SPEC_NATIVE_SECOND_LINEAR_V1"),
              workerSecondarySpec: receipt(stdout, "T09_SPEC_WORKER_SECOND_LINEAR_V1"),
            } : {}),
            ...((process.env.OKX_T10_05_LOST_ACK_PROBE === "1"
              || process.env.OKX_T10_05_EXPLICIT_REJECT_PROBE === "1") ? {
              nativeT10PostOutcome: receipt(stdout, "T10_05_NATIVE_POST_OUTCOME_V1"),
              workerT10PostOutcome: receipt(stdout, "T10_05_WORKER_AFTER_NATIVE_V1"),
            } : {}),
            ...(process.env.OKX_T10_15_POST_HELD_SOURCE_PROBE === "1" ? {
              nativeT10SourceChanged: receipt(stdout, "T10_15_NATIVE_CAUSAL_SOURCE_V1"),
              workerT10SourceChanged: receipt(stdout, "T10_15_WORKER_AFTER_NATIVE_V1"),
            } : {}),
            ...(process.env.OKX_T10_03_POST_AUTH_REPLY_LOST_PROBE === "1" ? {
              nativeT10PostReplyLost: receipt(stdout, "T10_03_NATIVE_POST_AUTH_REPLY_LOST_V1"),
              workerT10PostReplyLost: receipt(stdout, "T10_03_WORKER_POST_AUTH_RECONCILED_V1"),
            } : {}),
            ...(process.env.OKX_T10_01_PARTIAL_SOURCE_RECOVERY_PROBE === "1" ? {
              nativeT10SourceRecovery: receipt(stdout, "T10_01_NATIVE_SOURCE_RECOVERY_V1"),
              workerT10SourceRecovery: receipt(stdout, "T10_01_WORKER_SOURCE_RECOVERY_V1"),
            } : {}),
            ...(process.env.OKX_T10_10_FINANCE_REPLAY_PROBE === "1" ? {
              nativeT10FinanceReplay: receipt(stdout, "T10_10_NATIVE_SIGNED_REPLAY_V1"),
            } : {}),
            ...(process.env.OKX_T10_13_STOP_PROBE === "1" ? {
              nativeT10Stop: receipt(stdout, "T10_13_NATIVE_STOP_AFTER_FILL_V1"),
              workerT10Stop: receipt(stdout, "T10_13_WORKER_STOP_V1"),
            } : {}),
            ...(closeMode ? { nativeCloseAck: receipt(stdout, "T09_6_NATIVE_CLOSE_ACK_V1") } : {}),
            ...(closeMode ? { nativeCloseTerminal: receipt(stdout, "T09_6_NATIVE_TERMINAL_V1"), nativeCloseFinancialSources:receipt(stdout,"T09_6_NATIVE_FINANCIAL_SOURCES_V1"), workerCloseReconciled:receipt(stdout,"T09_6_WORKER_CLOSE_RECONCILED_V1") } : {}),
            ...(closeMode ? {
              nativeProcessRestart:receipt(stdout,"T09_7_NATIVE_PROCESS_RESTART_V1"),
              ...(budgetDenial ? {
                nativeBudgetDenial:receipt(stdout,"T09_8_NATIVE_CAMPAIGN_DENIAL_V1"),
              } : {}),
              workerTerminalRestart:receipt(stdout,"T09_7_WORKER_TERMINAL_RESTART_V1"),
            } : {}),
          } : { workerPostFill: receipt(stdout, "T09_1_WORKER_POST_FILL_EVIDENCE_V1") }),
        }
        : { workerReplay: receipt(stdout, "T09_5_WORKER_IDEMPOTENCY_EVIDENCE_V1") }),
      transcript_sha256: createHash("sha256").update(stdout + stderr).digest("hex"),
      cohort_execution: { key, elapsed_ms: Date.now() - startedAt,
        source: "actual-native-worker-child" },
    };
    })());
  }
  const evidence = await memo.get(key);
  const previous = cohortMetrics.get(key);
  cohortMetrics.set(key, {
    ...evidence.cohort_execution,
    calls: (previous?.calls ?? 0) + 1,
  });
  return { ...evidence,
    cohort_execution: { ...evidence.cohort_execution, reused },
  };
}
// Driver-level metrics come from the real child even when individual case
// return objects expose only independently verified financial assertions.
// These measurements do not imply independent repeat executions.
export function nativeCohortSnapshot() {
  return [...cohortMetrics.values()].map(value => ({ ...value }));
}
export { hash };
