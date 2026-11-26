# Operation diagnostics: one read-only entry point

Use the existing MCP tool `diagnostics` with `action: "explain"`. No additional
public tool, log database, financial permission or Engine process is required.

| Known reference | MCP arguments | Evidence owner |
| --- | --- | --- |
| Research job ID (64 lowercase hex) | `{"action":"explain","view":"research","reference":"<job_id>"}` | Research job catalog |
| Original Edge request ID for Research | `{"action":"explain","view":"research","reference":"request:<original_request_id>"}` | Research job catalog |
| Trading intent | `{"action":"explain","view":"operation","account":"<authorized_account>","reference":"intent:<intent_id>"}` | Engine account SQL |
| Original Edge request ID for a new direct `trade.execute` intent | `{"action":"explain","view":"operation","account":"<authorized_account>","reference":"request:<original_request_id>"}` | Engine account SQL |
| Terminal release | `{"action":"explain","view":"operation","account":"<authorized_account>","reference":"terminal_release:<id>"}` | Engine terminal-release trace |
| Protection operation | `{"action":"explain","view":"operation","account":"<authorized_account>","reference":"<operation_reference>"}` | Engine protection trace |

Research stores the original Edge `request_id` in `origin_invocation` when it
admits a new job. Searching `request:<id>` reads the existing 128-entry bounded
job diagnostics projection, including the latest durable subset restored on
startup. It does **not** scan SQL, block the catalog writer, or reconstruct
historical events. `NOT_OBSERVED / UNKNOWN_NOT_RETAINED` means *not in that
window*, **not** “the request had no effect.” If more than one job matches,
`NOT_EVALUATED / AMBIGUOUS_REQUEST_ORIGIN` contains no selected job; use an
exact `job_id`. Older jobs can still be inspected by their ID if retained in
the projection; otherwise the diagnostic cannot infer their state.

For Research and Engine `operation` explain reads, Edge adds
`operation_trace` (`okx.operation-trace-summary/v1`) alongside the untouched
owner `data`. It contains the owner, typed reference kind, current observed
stage, first unproven fact, and safe next action. It is always read-only and
partial: neither a recorded job state nor an Engine SQL intent snapshot proves
complete causal history, exchange execution, or delivery acknowledgments.
It rejects foreign job/intent references instead of summarizing them as
observed.

**Identity semantics:** `operation_trace.diagnostic_request_id` is the new
read-only diagnostic call. For a matched Research job,
`operation_trace.origin_request_id` is the **original** request that admitted
the job. For a newly prepared **direct** Engine `trade.execute` intent, the
account SQLite owner commits `edge-request:<id> → intent_id` as a typed
`EDGE_TRADE_PREPARE_ORIGIN` receipt in the **same transaction** as intent
preparation. Engine can therefore resolve `request:<id>` after restart, even
when the original Edge response was lost. The read-only diagnostic then
inspects the durable intent and exposes `resolved_intent_reference`.

`origin_request_id` is `null` for an Engine query by `intent:<id>` because its
original Edge request is not inferred. Automatic strategy execution, legacy
intents, `replace`, `close` and `cancel` do **not** create this one-to-one
request receipt; they require their explicit domain reference. A missing
`request:<id>` receipt is `NOT_OBSERVED / REQUEST_ORIGIN_NOT_RECORDED`, not a
proof of no financial effect. Reuse of one request ID for a different intent
is rejected, never relinked. The receipt is observational only: it does not
authorize a new trade or replace financial idempotency. Do not equate a
diagnostic request ID with the original operation ID.

Recommended incident sequence:

1. Keep the exact original `request_id`, `job_id` or `intent_id` from the
   returned receipt. Do not resubmit an uncertain mutation.
2. Call `diagnostics.explain` with the appropriate owner view, account (for
   trading) and reference. Read `operation_trace.first_unproven_fact` and
   `next_action`, then inspect the owner-specific `data` for exact evidence.
3. If `state` is `NOT_EVALUATED`, treat the outcome as unknown and continue
   with read-only owner-specific diagnostics. This endpoint never authorizes
   trading or retries a Research mutation.

Routing and the existing 13 MCP tool descriptors remain unchanged.
