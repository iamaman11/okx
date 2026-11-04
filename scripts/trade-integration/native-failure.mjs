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
const diagnosticLine = line =>
  /^(?:AssertionError(?: \[ERR_[A-Z_]+\])?:|Error \[ERR_[A-Z_]+\]:|Error: (?!Command failed)[A-Z][A-Z0-9_ :.\-]+$|thread '[^']+' panicked at|T09_[A-Z0-9_]+(?:DENIAL|FAILURE|FAILED)_V1:)/.test(line);
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
