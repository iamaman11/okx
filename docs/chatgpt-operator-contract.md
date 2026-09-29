# ChatGPT operator contract

This document defines the normal operating discipline for ChatGPT over the existing GitHub transport.

The goal is to keep queries attributable, replay-safe, context-efficient and fail-closed without adding a new backend, MCP server, Worker, database or transport owner.

## Transport split

- CONTROL uses GitHub issue #12 and plaintext strongly typed control requests.
- DATA uses GitHub issue #10 and encrypted strongly typed request/result envelopes.
- The repository is public, so account/risk DATA must never be published as plaintext.
- The DATA cryptographic contract remains X25519 -> HKDF-SHA256 -> ChaCha20-Poly1305.

## Context-budget invariants

Transport correctness is not enough: normal operation must also protect the ChatGPT context window from avoidable bulk evidence.

Runtime bounds:

- DATA plaintext responses are operation-budgeted at 8/12/16/32 KiB with a 40 KiB global ceiling;
- predicted encrypted GitHub comments must remain within the GitHub comment-body ceiling or terminalize as `RESPONSE_TOO_LARGE`;
- CONTROL request bodies are limited to 4 KiB;
- CONTROL terminal results are limited to 16 KiB and oversized diagnostics terminalize as `CONTROL_RESPONSE_TOO_LARGE`;
- `workspace_status` returns at most 16 changed-path entries and at most 512 bytes per entry;
- DATA and CONTROL runtime polling use persisted cursors, post-cursor retrieval, ETag/304 support and a bounded 10-page / 1000-comment recovery ceiling.

ChatGPT-side rules:

1. Do not fetch the complete comment history of #10 or #12 during normal operation.
2. Read issue metadata for counts/capacity, then retrieve only the exact request tail/result needed.
3. If a connector call returns many records, reduce/filter inside the tool orchestration before emitting anything into conversational context.
4. Never copy ciphertext, complete successful workflow logs, complete PR diffs or complete mailbox history into normal context.
5. Prefer structured workflow/job/artifact metadata; fetch only the failing log section when diagnosing CI.
6. Prefer Level 2 application operations for ordinary analysis. Level 1 bulk/detail operations are forensic tools and should be used only when the extra detail is necessary.
7. Decrypt a DATA terminal once, validate it, reduce it immediately to compact evidence, and discard the large plaintext from the working conversational summary.
8. Reuse the compact evidence record rather than repeatedly reopening the same mailbox terminal.

A ChatGPT stream/tool timeout must therefore be diagnosed separately from application transport health. DATA/CONTROL evidence is considered implicated only when their own typed telemetry/status shows a transport failure.

## DATA publication preflight

Normal operation must never hand-edit Base64, ciphertext, nonce or ephemeral public-key fields.

Before publishing a DATA request, the client must:

1. build the complete typed `AgentRequest`;
2. build and encrypt the complete `MailboxEnvelope` programmatically;
3. call `okx_protocol::crypto::preflight_client_request_envelope` over the exact envelope that will be posted;
4. require the helper to prove:
   - client-to-agent direction and envelope schema;
   - valid typed request;
   - exact inner/outer request-id equality;
   - valid Base64 transport fields;
   - exact 32-byte client ephemeral public key;
   - exact 12-byte nonce;
   - client public/private key consistency;
   - successful authenticated decryption of the exact ciphertext;
   - decrypted request equality with the original typed request;
5. serialize the validated envelope once and publish that immutable serialization.

If preflight fails, nothing is posted.

Permanent malformed/unauthenticated DATA input is not a terminal request and must not starve the mailbox cursor. Internal/runtime failures remain retryable.

## Exact-request retrieval

Normal operation must not repeatedly read the full DATA mailbox.

For every published request keep:

- request id;
- request comment id;
- publication timestamp;
- client ephemeral private key only in request-scoped private client state.

Retrieve only the bounded comment tail after the known request publication point, and stop as soon as the exact matching terminal `request_id` is found.

Do not treat another request's terminal as evidence for the current request.

If a terminal is not yet present, preserve the same request id and continue bounded retrieval. Do not repost a duplicate request merely to poll.

## One-shot decrypt / validate / reduce

For a terminal DATA response:

1. decrypt once;
2. validate envelope direction, request id and typed `AgentResponse`;
3. validate the expected result schema;
4. reduce the result immediately to the minimum evidence required for the user's question;
5. keep the compact evidence in ChatGPT context rather than the complete mailbox response.

Do not repeatedly decrypt or restate the same large terminal payload during one analytical turn.

## Evidence summaries

For normal application queries retain a compact evidence record containing only fields needed to answer and audit the conclusion.

Recommended shape:

```text
request_id
operation
terminal_status
result_schema
generated_at
quality
bounded key facts / computed evidence
warnings or structured failure
provenance / generations required by the operation
response-size telemetry when relevant
```

Raw low-level payloads remain available for forensic use but are not copied into normal conversational context.

## CI / artifact discipline

For successful changes, normal acceptance should use:

- exact tested head SHA;
- exact tested tree;
- workflow run id and terminal conclusion;
- job-level conclusions;
- exact artifact id/name/digest;
- merge tree equality.

Do not expand full successful CI logs.

Fetch step/job logs only when a job fails or when a specific acceptance fact is unavailable from structured metadata. Read the minimum failing section needed to classify and repair the defect.

## Normal vs forensic operations

Normal research should prefer bounded application-level operations such as MarketOverview, MarketResearch, PortfolioRisk and deterministic scenario operations.

Low-level operations such as raw market/reference/history snapshots remain supported for forensic diagnosis and contract inspection.

Do not replace the typed operation surface with a generic batch language, expression engine or arbitrary request executor.

## Context budget rule

The expensive work belongs below ChatGPT:

```text
OKX REST/WS
  -> immutable observation state
  -> quality / generation gates
  -> deterministic local Rust analysis
  -> bounded typed query result
  -> compact encrypted DATA evidence
  -> ChatGPT semantic interpretation
```

ChatGPT should not reproduce local deterministic arithmetic by orchestrating many small raw requests when an application-level operation already owns that composition.

## H1-F acceptance

H1-F is accepted when:

- malformed client envelope construction is prevented by programmatic preflight;
- tests cover valid preflight, malformed transport encoding and request-id mismatch;
- normal DATA retrieval is exact-request / bounded-tail based;
- one-shot decrypt/validate/reduce is documented and used in acceptance;
- successful CI is accepted from structured metadata without whole-log expansion;
- canonical evidence summaries are used for physical H1-G acceptance;
- no new backend, MCP, Worker, database or transport owner is introduced.
