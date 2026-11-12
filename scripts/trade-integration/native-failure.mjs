// Bounded diagnostic projection for the nested Windows Native/Workerd test.
// Never print raw HTTP/WS, child stdout/stderr, credentials, or stacks.
import { createHash } from "node:crypto";

const secretIndicator = /authorization|bearer|api[_-]?key|passphrase|secret|signature|credential|access[_-]?key|\btoken\b|signed[_ -]?header|raw[_ -]?(?:body|response)|set-cookie|private[_ -]?key/i;
const ansi = /\x1b\[[0-9;]*m/g;
const managedAddMarker = "T09_4_MANAGED_ADD_MISSING_INTENT_V1";
const nativeTeardownMarker = "NATIVE_DRIVER_TEARDOWN_FAILURE_V1:";
const nativeTeardownOwners = new Set(["native", "native_crash_host", "first", "second", "runtime_root"]);
const nativeTeardownCodes = new Set([
  "CHILD_TEARDOWN_TIMEOUT", "CHILD_JOIN_TIMEOUT", "CHILD_TREE_TERMINATION_FAILED",
  "DRIVER_OWNER_CLOSE_TIMEOUT", "DRIVER_OWNER_CLOSE_FAILED",
  "LIFECYCLE_PROBE_POST_DISPOSE_INJECTED_ERROR",
]);
const nativePrimaryCodes = new Set([
  "LIFECYCLE_PROBE_PRIMARY_ERROR", "LIFECYCLE_PROBE_PRIMARY_TIMEOUT",
  "NATIVE_SQL_BOOTSTRAP_DEADLINE", "NATIVE_EXITED_BEFORE_WORKER_CAMPAIGN",
  "WORKER_FRAME_TIMEOUT", "DRIVER_ASSERTION_OR_IO_FAILURE",
]);
export function nativeDriverPrimaryCode(error) {
  return nativePrimaryCodes.has(error?.message) ? error.message : "DRIVER_ASSERTION_OR_IO_FAILURE";
}
// A present malformed marker is fatal too. No payload text survives rejection.
export function nativeDriverTeardownFailure(error) {
  const lines = [error?.stderr, error?.stdout].flatMap(stream =>
    String(stream ?? "").split(/\r?\n/).filter(line => line.startsWith(nativeTeardownMarker)));
  if (!lines.length) return null;
  const invalid = () => ({schema:"okx.native-driver-teardown-failure/v1", parse_state:"INVALID",
    reason_code:"NATIVE_DRIVER_TEARDOWN_MARKER_INVALID", primary_code:null,
    failures:[], owners_closed:false, root_retained:true});
  if (lines.length !== 1 || lines[0].length > 4096) return invalid();
  let value;
  try { value=JSON.parse(lines[0].slice(nativeTeardownMarker.length)); }
  catch { return invalid(); }
  if (!value || Object.keys(value).sort().join(",") !==
      "failures,owners_closed,primary_code,root_retained,schema" ||
      value.schema !== "okx.native-driver-teardown-failure/v1" ||
      !(value.primary_code === null || nativePrimaryCodes.has(value.primary_code)) ||
      typeof value.owners_closed !== "boolean" || typeof value.root_retained !== "boolean" ||
      (!value.owners_closed && !value.root_retained) ||
      !Array.isArray(value.failures) || value.failures.length < 1 || value.failures.length > 5 ||
      value.failures.some(failure => !failure || Object.keys(failure).sort().join(",") !== "code,owner" ||
        !nativeTeardownOwners.has(failure.owner) || !nativeTeardownCodes.has(failure.code)) ||
      new Set(value.failures.map(failure => failure.owner)).size !== value.failures.length)
    return invalid();
  return {schema:value.schema, parse_state:"VALID", reason_code:"NATIVE_DRIVER_TEARDOWN_FAILED",
    primary_code:value.primary_code,
    failures:value.failures.map(({owner, code}) => ({owner, code})),
    owners_closed:value.owners_closed, root_retained:value.root_retained};
}
const fixedState = value => typeof value === "string" &&
  /^(?:completed|accepted|rejected|pending|unknown|failed|queued|running|PREPARED|RESERVED|POSTING|ACKED|OPEN|FILLED|CANCELLED|REJECTED|UNKNOWN|PENDING)$/.test(value)
  ? value : null;
function fixedCode(value) {
  if (typeof value !== "string" || secretIndicator.test(value)) return null;
  if (value.startsWith("OKX rate/backpressure defer:")) return "OKX_RATE_BACKPRESSURE_DEFER";
  return /^(?:BUDGET|CENTRAL|LOCAL|ENTRY|STRATEGY|ACCOUNT|CAMPAIGN|INTENT|PROTECTION|SOURCE|SQL|ORDER|RISK|POLICY|FEE|LEVERAGE|NAV|UNATTRIBUTED|COHORT|TRANSCRIPT)_[A-Z0-9_]{1,80}$/.test(value)
    ? value : null;
}
// The Rust producer already emits typed outcome fields at the original
// T09.2 refusal. Keep these bounded fields instead of reconstructing a
// business reason from panic text or forwarding private HTTP query values.
export function signedGetSourceFailure(stderr) {
  const marker = "T09_2_SIGNED_GET_DENIAL_V1:";
  const lines = String(stderr ?? "").replace(ansi, "").split(/\r?\n/)
    .map(line => line.trim()).filter(line => line.startsWith(marker));
  if (!lines.length) return null;
  const invalid = () => ({
    schema:"okx.t09-signed-source-failure/v1", parse_state:"INVALID",
    origin_case:"T09.2", stage:"EXECUTE",
    reason_code:"T09_2_SOURCE_DIAGNOSTIC_INVALID",
  });
  if (lines.length !== 1 || lines[0].length > 64*1024) return invalid();
  let value;
  try { value = JSON.parse(lines[0].slice(marker.length)); }
  catch { return invalid(); }
  const code = fixedCode(value?.outcome?.data?.primary_reason);
  const routes = value?.denied_routes;
  const posts = value?.outbound_posts;
  if (!code || !Array.isArray(routes) || routes.length > 32 ||
      routes.some(route => typeof route !== "string" || route.length > 1024) ||
      !Number.isSafeInteger(posts) || posts < 0 || posts > 1000)
    return invalid();
  const causes = routes.map(route => fixedCode(route.split(":")[0]));
  if (causes.some(cause => !cause)) return invalid();
  return {
    schema:"okx.t09-signed-source-failure/v1", parse_state:"VALID",
    origin_case:"T09.2", stage:"EXECUTE",
    reason_code:code,
    state:fixedState(value.outcome.data.state),
    send_state:["NOT_STARTED","POSTING","UNKNOWN","ACKED","SENT","NOT_SENT"]
      .includes(value.outcome.data.send_state) ? value.outcome.data.send_state : null,
    risk_release_state:["RELEASED","HELD","PENDING","UNKNOWN"]
      .includes(value.outcome.data.risk_release_state) ? value.outcome.data.risk_release_state : null,
    denial_count:routes.length, source_denial_codes:[...new Set(causes)],
    outbound_posts:posts,
  };
}

function unexpectedPostsDetail(line) {
  const text=String(line).replace(ansi, "").trim();
  const marker=["T10_02_UNEXPECTED_POSTS_V1","T09_6_STATUS_POSTS_V1"]
    .find(marker=>text.startsWith(marker+":"));
  if (!marker) return null;
  const payload=text.slice(marker.length+1);
  if (payload.length>64*1024) return {marker,parse_state:"OVERSIZE"};
  let value;
  try { value=JSON.parse(payload); }
  catch { return {marker,parse_state:"INVALID_JSON"}; }
  const count=n=>Number.isSafeInteger(n)&&n>=0&&n<=1_000_000_000?n:null;
  const hash=s=>typeof s==="string"&&/^[a-f0-9]{64}$/.test(s)?s:null;
  const routes=new Set(["/api/v5/trade/order","/api/v5/trade/cancel-order","/api/v5/trade/amend-order",
    "/api/v5/trade/order-algo","/api/v5/trade/cancel-algos","/api/v5/trade/amend-algos"]);
  const posts=Array.isArray(value?.posts)?value.posts:[];
  return {marker,parse_state:"PARSED",posts_before:count(value?.posts_before),posts_after:count(value?.posts_after),
    phase:["CLOSE_TERMINAL_STATUS","ENTRY_CANCEL_STOP_STATUS","POST_CANCEL_RELEASE_STATUS","POST_PEER_RELEASE_STATUS","T10_08_CLOSE_TERMINAL","T10_08_ORIGINAL_FLAT"].includes(value?.phase)?value.phase:null,
    boundary_verdict:["EXPECTED_CANCEL","FORBIDDEN_EFFECT"].includes(value?.boundary_verdict)?value.boundary_verdict:null,
    state:fixedState(value?.state),reason_code:fixedCode(value?.reason_code),
    origin:["LOCAL","CENTRAL","VENUE","NATIVE","SQL"].includes(value?.origin)?value.origin:null,
    send_state:["NOT_STARTED","POSTING","UNKNOWN","ACKED","SENT","NOT_SENT"].includes(value?.send_state)?value.send_state:null,
    posts:posts.slice(0,12).map(post=>({
      ordinal:count(post?.ordinal),route:routes.has(post?.route)?post.route:null,
      actor_scope:["ACCOUNT_A_EXECUTOR","ACCOUNT_A_OBSERVER","ACCOUNT_B_EXECUTOR","ACCOUNT_B_OBSERVER","UNKNOWN"].includes(post?.actor_scope)?post.actor_scope:null,
      client_id_sha256:hash(post?.client_id_sha256),algo_id_sha256:hash(post?.algo_id_sha256),
      reduce_only:typeof post?.reduce_only==="boolean"?post.reduce_only:null,
      side:["buy","sell"].includes(post?.side)?post.side:null,
      size:typeof post?.size==="string"&&/^(?:0|[1-9][0-9]{0,29})(?:\.[0-9]{1,18})?$/.test(post.size)?post.size:null,
    })),posts_omitted:Math.max(0,posts.length-12)};
}
function managedAddDetail(line) {
  const text = String(line).replace(ansi, "").trim();
  if (!text.startsWith(managedAddMarker + ":")) return null;
  const payload = text.slice(managedAddMarker.length + 1);
  if (payload.length > 64 * 1024) return { marker:managedAddMarker, parse_state:"OVERSIZE" };
  let value;
  try { value = JSON.parse(payload); }
  catch { return { marker:managedAddMarker, parse_state:"INVALID_JSON" }; }
  const data = value?.data;
  const intentId = data?.intent?.intent_id ?? data?.intent_id ?? value?.intent_id;
  return {
    marker:managedAddMarker, parse_state:"PARSED",
    status:fixedState(value?.status), state:fixedState(data?.state ?? value?.state),
    reason_code:fixedCode(data?.primary_reason ?? data?.reason_code ?? value?.reason_code ?? value?.failure?.code),
    origin:["LOCAL","CENTRAL","VENUE","NATIVE","SQL"].includes(data?.rejection_origin ?? data?.origin)
      ? (data.rejection_origin ?? data.origin) : null,
    send_state:["NOT_STARTED","POSTING","UNKNOWN","ACKED","SENT","NOT_SENT"].includes(data?.send_state)
      ? data.send_state : null,
    intent_id_present:typeof intentId === "string", intent_id_length:typeof intentId === "string" ? intentId.length : null,
    intent_id_format:typeof intentId !== "string" ? "MISSING"
      : /^[a-f0-9]{64}$/.test(intentId) ? "HEX_64" : "OTHER",
  };
}
function sanitized(line) {
  const text = String(line).replace(ansi, "").trim();
  if (!text || secretIndicator.test(text) || /[{}\\]/.test(text) ||
      /https?:\/\/|\/api\/v5\/|[A-Za-z]:[\\/]/.test(text) ||
      /[A-Za-z0-9+/=_-]{48,}/.test(text)) return null;
  return text.slice(0, 160);
}
function panicReason(line) {
  const detail = managedAddDetail(line);
  if (detail) return managedAddMarker + ":" +
    [detail.state, detail.reason_code, detail.origin, detail.send_state].filter(Boolean).join(":");
  const safe = sanitized(line);
  if (safe) return safe;
  // A wrapped typed fixture error may contain a route, causing whole-line
  // redaction. Keep only its code at the known error boundary, never values.
  const text=String(line).replace(ansi, "");
  if (secretIndicator.test(text)) return "DETAIL_REDACTED_OR_MISSING";
  const code=text.match(/(?:OKX (?:API|response|clock) error: |: ")((?:TRANSCRIPT|HTTP_TRANSCRIPT|ACCOUNT|RISK|ORDER|ENTRY|PROTECTION|CAMPAIGN|SOURCE|SQL)_[A-Z0-9_]{1,90})(?=[:"])/);
  return code?.[1] ?? "DETAIL_REDACTED_OR_MISSING";
}
const diagnosticLine = line =>
  /^(?:AssertionError(?: \[ERR_[A-Z_]+\])?:|Error \[ERR_[A-Z_]+\]:|Error: (?!Command failed)[A-Z][A-Z0-9_ :.\-]+$|thread '[^']+' panicked at|T09_[A-Z0-9_]+(?:DENIAL|FAILURE|FAILED)_V1:)/.test(line);

// A task panic may precede the main test's timeout. Retain both boundaries;
// never call the last assertion the first cause. Pipes preserve order only
// within one stream, so this projection makes no cross-stream time claim.
export function summarizeRustPanics(error) {
  const panics = [];
  const diagnostics=[];
  let count = 0;
  for (const stream of ["stdout", "stderr"]) {
    const lines = String(error?.[stream] ?? "").replace(ansi, "").split(/\r?\n/);
    let ordinal = 0;
    for (let i = 0; i < lines.length; i++) {
      const postDetail=unexpectedPostsDetail(lines[i]);
      if(postDetail) diagnostics.push({stream,line_index:i,detail:postDetail});
      if (!/thread '.+'(?: \(\d+\))? panicked at/.test(lines[i])) continue;
      const location = lines[i].replaceAll("\\", "/")
        .match(/\b(crates\/[A-Za-z0-9_./-]+\.rs:\d{1,6}:\d{1,6})/);
      const reason = panicReason(lines[i + 1] ?? "");
      const detail = managedAddDetail(lines[i + 1] ?? "");
      // Rust's count assertion puts its actual/expected on following lines.
      // Keep integers only; strings/Debug payloads may contain credentials.
      const comparison = /assertion `left == right` failed/.test(lines[i + 1] ?? "")
        ? lines.slice(i + 2, i + 6).map(line => line.match(/^\s*(left|right): ([0-9]{1,10})\s*$/)).filter(Boolean) : [];
      const counts = Object.fromEntries(comparison.map(match => [match[1], Number(match[2])]));
      panics.push({stream, ordinal:ordinal++, location:location?.[1] ?? "RUST_LOCATION_UNKNOWN", reason,
        ...(detail ? {detail} : {}),
        ...(Object.hasOwn(counts,"left") && Object.hasOwn(counts,"right") ? {integer_comparison:counts} : {})});
      count++;
    }
  }
  const bounded = panics.length <= 16 ? panics : [...panics.slice(0, 8), ...panics.slice(-8)];
  const retainedDiagnostics=diagnostics.length<=8?diagnostics:[...diagnostics.slice(0,4),...diagnostics.slice(-4)];
  return {panics:bounded, count, omitted:count - bounded.length, cross_stream_order:"UNKNOWN",
    ...(diagnostics.length?{diagnostics:retainedDiagnostics,diagnostic_count:diagnostics.length,
      diagnostics_omitted:diagnostics.length-retainedDiagnostics.length}:{})};
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
