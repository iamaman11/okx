// Bounded diagnostic projection for the nested Windows Native/Workerd test.
// Never print raw HTTP/WS, child stdout/stderr, credentials, or stacks.
import { createHash } from "node:crypto";

const secretIndicator = /authorization|bearer|api[_-]?key|passphrase|secret|signature|credential|access[_-]?key|\btoken\b|signed[_ -]?header|raw[_ -]?(?:body|response)|set-cookie|private[_ -]?key/i;
const ansi = /\x1b\[[0-9;]*m/g;
function sanitized(line) {
  const text = String(line).replace(ansi, "").trim();
  if (!text || secretIndicator.test(text) || /[{}\\]/.test(text) ||
      /https?:\/\/|\/api\/v5\/|[A-Za-z]:[\\/]/.test(text) ||
      /[A-Za-z0-9+/=_-]{48,}/.test(text)) return null;
  return text.slice(0, 160);
}
function panicReason(line) {
  const safe = sanitized(line);
  if (safe) return safe;
  // A wrapped typed fixture error may contain a route, causing whole-line
  // redaction. Keep only its code at the known error boundary, never values.
  const text=String(line).replace(ansi, "");
  if (secretIndicator.test(text)) return "DETAIL_REDACTED_OR_MISSING";
  const code=text.match(/(?:OKX API error: |: ")((?:TRANSCRIPT|HTTP_TRANSCRIPT|ACCOUNT|RISK|ORDER|ENTRY|PROTECTION|CAMPAIGN|SOURCE|SQL)_[A-Z0-9_]{1,90})(?=[:"])/);
  return code?.[1] ?? "DETAIL_REDACTED_OR_MISSING";
}
const diagnosticLine = line =>
  /^(?:AssertionError(?: \[ERR_[A-Z_]+\])?:|Error \[ERR_[A-Z_]+\]:|Error: (?!Command failed)[A-Z][A-Z0-9_ :.\-]+$|thread '[^']+' panicked at|T09_[A-Z0-9_]+(?:DENIAL|FAILURE|FAILED)_V1:)/.test(line);

// A task panic may precede the main test's timeout. Retain both boundaries;
// never call the last assertion the first cause. Pipes preserve order only
// within one stream, so this projection makes no cross-stream time claim.
export function summarizeRustPanics(error) {
  const panics = [];
  let count = 0;
  for (const stream of ["stdout", "stderr"]) {
    const lines = String(error?.[stream] ?? "").replace(ansi, "").split(/\r?\n/);
    let ordinal = 0;
    for (let i = 0; i < lines.length; i++) {
      if (!/thread '.+'(?: \(\d+\))? panicked at/.test(lines[i])) continue;
      const location = lines[i].replaceAll("\\", "/")
        .match(/\b(crates\/[A-Za-z0-9_./-]+\.rs:\d{1,6}:\d{1,6})/);
      const reason = panicReason(lines[i + 1] ?? "");
      panics.push({stream, ordinal:ordinal++, location:location?.[1] ?? "RUST_LOCATION_UNKNOWN", reason});
      count++;
    }
  }
  const bounded = panics.length <= 16 ? panics : [...panics.slice(0, 8), ...panics.slice(-8)];
  return {panics:bounded, count, omitted:count - bounded.length, cross_stream_order:"UNKNOWN"};
}
export function summarizeNativeWorkerdFailure(error) {
  const stderr = String(error?.stderr ?? "");
  const stdout = String(error?.stdout ?? "");
  const combined = (stderr + "\n" + stdout).replace(ansi, "");
  const relevant = combined.split(/\r?\n/).map(x => x.trim())
    .filter(diagnosticLine).map(sanitized).filter(Boolean);
  const phases = [...combined.matchAll(/\b(T09_(?:[0-9]+_)?[A-Z0-9_]+_V1):/g)]
    .map(m => m[1]).slice(-12);
  const assertion = relevant.at(-1) ?? "INNER_CHILD_EXIT_WITHOUT_SAFE_ASSERTION";
  // Only reveal a numeric line from our fixed, known test module. Never
  // echo the OS path, Node stack, source bytes, signed URL or credentials.
  const localFrame = combined.match(/test-runtime-do\.mjs:(\d{1,5}):\d{1,5}/);
  const workerd_test_line = localFrame ? Number(localFrame[1]) : null;
  const exit = String(error?.code ?? "UNKNOWN");
  return {
    schema: "okx.t09-nested-failure/v1",
    phase: phases.at(-1) ?? "NO_RECORDED_PHASE",
    error_class: assertion.startsWith("AssertionError") ? "NODE_ASSERTION"
      : assertion.includes("panicked at") ? "RUST_PANIC"
      : assertion === "INNER_CHILD_EXIT_WITHOUT_SAFE_ASSERTION" ? "UNCLASSIFIED" : "NESTED_ERROR",
    first_visible_error: assertion,
    workerd_test_line,
    error_count: relevant.length,
    outer_exit_code: /^[0-9]{1,8}$/.test(exit) ? exit : "UNKNOWN",
    stdout_bytes: stdout.length,
    stderr_bytes: stderr.length,
    output_sha256: createHash("sha256").update(combined).digest("hex"),
  };
}
