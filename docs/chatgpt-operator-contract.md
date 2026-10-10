# ChatGPT operator contract

This document defines the normal operating discipline for ChatGPT over the accepted Cloudflare-primary / GitHub-fallback architecture.

The goal is to keep queries attributable, context-efficient and fail-closed while allowing broad analytical question coverage without turning every natural-language question into a new backend endpoint.

## Current authority and G1 safety — 2026-10-10

The existing **primary Cloudflare MCP includes a typed Demo-only `execution_action`**, alongside read-only `executor_preflight`, `execution_status` and `account_summary`. Main source `5e3acf5c` already contains the six original operations; [Stage 4C-G1 PR #273](https://github.com/iamaman11/okx/pull/273) proposes the seventh, `abandon_prepared`. No other trade transport or executor is allowed. Production is **always read-only**. The active Demo carries one long and an ACTIVE exchange OCO; its earlier CLOSE is still unsent PREPARED. Only a verified PREPARED-only durable local abandonment may release that reservation; exchange CLOSE, protection cleanup, fills and bills require independent admission/reconciliation. SOURCE PR/CI alone is not installed/physical PASS. [#160](https://github.com/iamaman11/okx/issues/160) is the current canonical cursor; supersede inconsistent historical text below.

## Transport split

- **Primary DATA:** ChatGPT -> OAuth `okx-cloudflare-mcp` -> one Durable Object rendezvous -> one outbound authenticated Windows WSS -> existing typed `AgentRequest/AgentResponse` path.
- **Fallback DATA:** GitHub issue #10 with encrypted strongly typed request/result envelopes. It is parity/recovery only, not the normal product path.
- **CONTROL:** GitHub issue #12 with plaintext strongly typed lifecycle/deploy/diagnostic requests.
- Cloudflare owns authentication/rendezvous/correlation only; Windows/Rust remains the product authority.
- The repository is public, so private account/risk DATA must never be published as plaintext.
- The GitHub DATA cryptographic contract remains X25519 -> HKDF-SHA256 -> ChaCha20-Poly1305.
- Every **read-only** product capability exposed on the Cloudflare primary must be black-box accepted through the actual connected MCP tool. A healthy runtime or newer Worker contract is not enough; refresh tools if stale and re-run that specific primary operation before treating GitHub as parity evidence. **Stage-4C Demo exchange mutations** use the single explicitly typed `execution_action` on the Cloudflare primary when authenticated active Demo and native Rust authority permit. GitHub encrypted DATA remains an independent recovery/diagnostic transport, **not** a replacement for a refused primary mutation. No second mutation gateway, hidden RPC or real-money write authority is introduced.

## Production-intended operator contract; Demo as venue acceptance, not product fork

**The destination is real OKX trading.** Demo is a bounded exchange integration / fault-injection test mode using **the exact same** typed prepare/submit/cancel/amend/close/protective-cleanup contracts, one Rust executor/risk owner, durable ledger, uncertain ACK/restart discipline, fee and bill reconciliation, and TCA intended for eventual live use. No separate Demo strategy, trading state machine, executor or permanent Demo-only mutation MCP API is permitted. The environment, credential-bound account, durable root, margin/position mode and explicit authority gate are different, and are tested for non-interchangeability.

**Currently proven versus targeted:** `ExecutionRuntime::new` is constructed as production `ReadOnly`, while Demo acceptance uses a guarded `DemoAcceptance` mode and `enable_demo_acceptance` around actual mutation sends. The shared `OrderExecutor` is a reusable core; there is **not yet an approved real-money mutation admission path**. Stage-5/live activation must add separate environment-bound policy/permission proof within the same owner; never loosen or reuse the Demo-only mutation guard as a shortcut.

**Live admission is not Demo admission:** production is currently read-only; final live trading requires Stages 1–5 acceptance, separate explicit operator activation, scoped production Trade/no-Withdraw credentials, risk/governance and rollback/canary proof. Demo physical PASS must never enable the real-money write gate.

**Cloudflare is an operator observation plane, not the source of real-time trading safety.** Stage-4C active-profile continuity candidate (single PR) makes the **same already-authenticated** Cloudflare WSS available to whichever one Job-owned production/Demo agent is active. The `Hello` binds an explicit profile to the session/generation; missing profile in legacy agent is `unverified`, invalid profiles are rejected, and a switch fences the previous generation. The existing `account_summary` still binds UID and coherent exchange truth. `demo_acceptance` retains its separate credentials, durable root and encrypted DATA #234; Rust `direct_transport_read_only` rejects mutation operations **as read-only requests**; the separate `direct_transport_demo_execution` closed set can admit them only under authenticated `demo_acceptance` at both Worker and native Rust gates. Profile continuity alone never grants trade authority; require Worker deploy, exact Windows binary, and black-box profile/FRESH/exposure and risk tests. Production exposure requiring ongoing protection must not be blinded by a Demo switch; reject transition or prove an independently accepted safe supervision path before any live risk.

**Typed trade commands are already on the primary Demo-only surface.** They invoke the existing Rust owner with immutable risk, UID, journal and per-intent admission; this is not a new Demo executor. A future separately authorized **real-money** admission path is still unimplemented and must not reuse Demo authority. Never camouflage a mutation as a read or bypass a tool refusal. Autonomous safety and recovery may not depend on ChatGPT being online.

**Current Stage-4C physical proof:** `submit_long`/`submit_close` matched encrypted #234 response envelopes but lack an accepted independent exact-account post-CLOSE venue position, pending ordinary/protective order, fill/bill/fee and ledger reconciliation. Until resolved, exposure status is `UNVERIFIED`, not FLAT; do not blindly retry or abandon the active Demo owner. Stage 5 P7/P8/P9 still mandatory and independently tracked in #160. Exact operational cursor: `docs/ROADMAP.md`, execution evidence: #223.

## First-party typed execution on the primary transport (Stage-4C candidate)

The product-intended `execution_action` MCP surface is deliberately **not** an arbitrary RPC: main accepts six named operations; PR #273 proposes a seventh strictly local `abandon_prepared` on the existing typed Rust execution contract. `executor_preflight` is a separate read-only call; `execution_status` and `account_summary` are read-only acceptance truth. The active one-owner Cloudflare Hello profile is a hard security input, **not** a user-selectable tool argument. The Worker must reject every mutation unless `runtime_profile=demo_acceptance`; Rust independently rechecks the same profile and active Demo mode, and only then passes the request to the same existing credential/UID/freshness/risk/ledger pre-send gate. Production remains hard ReadOnly regardless of API permission, OAuth client or user authorization.

**No opaque batch mutations or automatic retries.** For a complete user-authorized Demo acceptance run, ChatGPT drives the ordered matrix as sequential *durable* intents: one prepare, one submit, one exact-ID readback and independent same-UID venue reconciliation at each checkpoint. A lost tool response is **UNKNOWN**, never a reason to make a fresh request ID or send again. Any platform/connector refusal is respected; GitHub encrypted DATA is not a route around a refused primary write. The admitted Demo operations are mechanical exchange tests and are not strategy-directed live trades. Update tools once after verified Worker schema deployment; absence of the tool is not permission to send via an alternate transport.

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

1. Prefer the primary Cloudflare MCP path for normal product questions; use GitHub DATA only for explicit fallback/parity/recovery proof.
2. Do not fetch the complete comment history of #10 or #12 during fallback/control operation.
3. Read issue metadata for counts/capacity, then retrieve only the exact request tail/result needed.
4. If a connector call returns many records, reduce/filter inside the backend/tool orchestration before emitting anything into conversational context.
5. Never copy ciphertext, complete successful workflow logs, complete PR diffs or complete mailbox history into normal context.
6. Prefer structured workflow/job/artifact metadata; fetch only the failing log section when diagnosing CI.
7. Prefer bounded application/query-plan operations for ordinary analysis. Low-level bulk/detail operations are forensic tools and should be used only when the extra detail is necessary.
8. Decrypt a fallback DATA terminal once, validate it, reduce it immediately to compact evidence, and discard the large plaintext from the working conversational summary.
9. Reuse compact evidence rather than repeatedly reopening the same mailbox terminal or re-fetching the same large primary result.
10. Treat the normal result as a self-contained decision packet: identity/version, status/quality, provenance/coverage, assumptions, bounded key metrics, risk/policy outcome, warnings/blockers/invalidation conditions, and artifact/continuation ids when needed.
11. Default interaction budget for an ordinary analytical/research question is one coarse MCP operation and <= 12,288 bytes of result evidence once the capability contract is known. Extra calls must be justified by a typed incomplete/degraded result, explicit forensic need, or bounded continuation.
12. Never request raw candles/trades/L2/replay traces merely to reproduce deterministic Rust calculations in ChatGPT.

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

## CONTROL publication preflight

A CONTROL `request_id` is an immutable single-publication identity for one logical operation. Controller-side terminal-id deduplication is defense in depth; it must not be used as the normal way to make duplicate publication safe.

Before posting any `okx.windows.control/v1` request to issue #12, ChatGPT must:

1. construct the complete typed request and its candidate `request_id`;
2. perform an exact GitHub issue search for that id in repository comments, for example `"<request_id>" in:comments`;
3. inspect the bounded search results and **abort publication if issue #12 is present**, whether the existing comment is the request or its terminal result;
4. if the operation was already published, retrieve/reuse its existing terminal evidence instead of posting again;
5. if the prior publication state is uncertain, resolve that state before any new CONTROL mutation; do not create a fresh id merely to bypass uncertainty about whether the original mutation ran;
6. use a fresh `request_id` only for a genuinely new logical CONTROL operation.

A request already known from compact conversation/operator evidence is also treated as used and must not be reposted. Polling/retrieval never republishes the request.

If a duplicate publication is discovered after the fact, convert the accidental comment into a non-request audit note when possible, preserve the original terminal as canonical evidence, and verify that no second terminal execution occurred.

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

Raw low-level payloads remain available for forensic use but are not copied into normal conversational context. A fresh chat must be able to continue from the compact record plus immutable artifact/experiment ids rather than depending on prior conversational history.

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

Normal research should prefer bounded application-level operations such as MarketOverview, MarketResearch, AccountSummary, PortfolioRisk and deterministic scenario operations.

Low-level operations such as raw market/reference/history snapshots remain supported for forensic diagnosis and contract inspection.

Do not replace the typed operation surface with arbitrary SQL, a string expression engine, user-supplied code or an arbitrary OKX request executor.

## Universal analytical question rule

ChatGPT owns **semantic planning**, not numerical truth. For a broad read-only question, ChatGPT should map the request onto the smallest supported **versioned bounded typed analytical plan** rather than asking for a new endpoint whose name mirrors the wording of the question.

A plan may specify only accepted primitives such as:
- normalized universe selectors;
- allowlisted factual fields;
- typed filters;
- stable sort and top/bottom-K;
- bounded grouping/aggregation;
- supported time-window comparisons;
- versioned deterministic metrics owned by Rust;
- freshness/coherence policy and hard response limits.

Examples that should share one capability rather than become separate MCP tools:
- top 10 derivatives by 24h change;
- bottom 10 USDT swaps by 24h change;
- highest-volume live swaps;
- highest/lowest supported funding metric;
- rank a bounded universe by an already-supported derived metric.

Backend-change rule:
- if the question only recombines existing facts/operators, **no backend code change**;
- if it needs a new factual primitive, implement that primitive once in the factual layer;
- if it needs a new deterministic metric, implement/version/test it once in `okx-analysis`;
- if it changes mutation/risk/governance semantics, use a dedicated explicit capability rather than the generic read plan.

This keeps the MCP surface small while making the answer space broad. The preferred stable read surface is:
- `query_capabilities`: fetch the current catalog version, fields, metrics, operators, units and hard bounds;
- `query`: submit a bounded plan that names the catalog version it was built from.

ChatGPT should refresh the **capability catalog**, not require a new MCP tool schema for every new metric. Unknown/stale field or metric IDs fail closed. The runtime maps IDs to typed internal implementations; free-form formulas are never accepted.

The query-plan evaluator owns no durable business state and must not create a second collector, cache, scheduler or formula owner.

## Context budget rule

The expensive work belongs below ChatGPT:

```text
OKX REST/WS
  -> immutable observation state
  -> quality / generation gates
  -> deterministic local Rust analysis
  -> bounded typed query result
  -> compact Cloudflare MCP result (or encrypted GitHub fallback evidence)
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
- no extra backend/service/database/transport owner is introduced; primary Cloudflare MCP and GitHub fallback reuse the accepted owners.
