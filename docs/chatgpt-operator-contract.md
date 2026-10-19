# ChatGPT operator contract

> **Canonical future architecture (2026-10-20):** [Architecture Contract v1](ARCHITECTURE_CONTRACT_V1.md). This is the accepted target modularity/extension boundary, **not** proof that simultaneous accounts/venues or live trading are already deployed. Dated checklists and states below are historical unless independently reverified.

This document defines the normal operating discipline for ChatGPT over the accepted Cloudflare-primary / GitHub read-only-diagnostic architecture.

The goal is to keep queries attributable, context-efficient and fail-closed while allowing broad analytical question coverage without turning every natural-language question into a new backend endpoint.

## Current operator authority — 2026-10-17

**Active source of truth is [Issue #160](https://github.com/iamaman11/okx/issues/160), not the dated historical snapshots below.** The main Rust owner now contains scoped Production CONTROL policy, guarded native FOK OPEN/CLOSE and financial close/quarantine (#365–#367). The single MCP Worker transport can dispatch ONLY those five supported typed Production operations after verified runtime Hello; this is never permission to POST. The native executor independently checks ENABLED/STOPPED, exact account/run/UID, STOP, UNKNOWN, modelled loss <= 10 USDT, and fresh venue evidence. No Production Live physical acceptance or installed Windows parity has been claimed. This note supersedes old assertions below that the *code* always forces Production ReadOnly; old historical acceptance evidence remains historical.

**Verified release and account:** `main eb1da12add28f3bdcee909c09339f6515e2558ce`; PR #297 6/6 CI PASS; exact Windows artifact `11713135973` installed with CONTROL `READY_RUNNING`; primary Cloudflare Demo MCP `PASS/session_fresh`, tool contract `okx.mcp.tools/2026-10-11.2`. Authenticated account FRESH FLAT (positions=0, regular orders=0, native OCO=0, unresolved=0, reconciled). One FOK OPEN plus attached protection and one FOK CLOSE physically passed. ADD/HEDGE/REDUCE/REVERSE physical PASS **not proven**. Live **OFF**.

**NEXT = Stage 4C-R1:** exact independently confirmed exchange FLAT/0 pending OCO plus durable cleanup of old OPEN `protection=ACTIVE`, same UID, negative foreign/incomplete/UNKNOWN tests, no exchange POST; then 4C-R2 risk oracle/market/OCO race, 4C-D physical FOK ADD/HEDGE, 4C-E REDUCE/REVERSE, G3-F/G4, Stage 5A/B/C, separate Live gate. [#160](https://github.com/iamaman11/okx/issues/160) is the only acceptance authority. All legacy D1/NEXT and artifact text below is dated history.

**Operator GO/NO-GO:** a PASS connector does NOT mean FRESH market/account, accepted hard risk or permission for the specific mutation. Prefer one bounded typed reply with profile, UID, source generations/timestamps, oracle agreement, ledger and venue OCO/UNKNOWN, artifact provenance, next action. Never submit new risk to diagnose a failing risk/OCO check; never retry an UNKNOWN exchange effect. One Rust executor, ledger, Windows owner, primary Cloudflare MCP Demo trading ingress. GitHub DATA read-only diagnosis only; CONTROL typed lifecycle only.

## Transport split

- **Primary DATA:** ChatGPT -> OAuth `okx-cloudflare-mcp` -> one Durable Object rendezvous -> one outbound authenticated Windows WSS -> existing typed `AgentRequest/AgentResponse` path.
- **Read-only diagnostic DATA:** GitHub issue #10 defines encrypted strongly typed diagnostics; issue #234 contains historical Demo acceptance evidence. Use only for diagnostic/parity/recovery reads, never trading writes or a path around refused primary mutations.
- **CONTROL:** GitHub issue #12 with plaintext strongly typed lifecycle/deploy/diagnostic requests.
- Cloudflare owns authentication/rendezvous/correlation only; Windows/Rust remains the product authority.
- The repository is public, so private account/risk DATA must never be published as plaintext.
- The GitHub DATA cryptographic contract remains X25519 -> HKDF-SHA256 -> ChaCha20-Poly1305.
- Every **read-only** product capability exposed on the Cloudflare primary must be black-box accepted through the actual connected MCP tool. A healthy runtime or newer Worker contract is not enough; refresh tools if stale and re-run that specific primary operation before treating GitHub as parity evidence. **Stage-4C Demo exchange mutations** use the single explicitly typed `execution_action` on the Cloudflare primary when authenticated active Demo and native Rust authority permit. GitHub encrypted DATA remains an independent recovery/diagnostic transport, **not** a replacement for a refused primary mutation. No second mutation gateway, hidden RPC or real-money write authority is introduced.

## Production-intended operator contract; Demo as venue acceptance, not product fork

**The destination is real OKX trading.** Demo is a bounded exchange integration / fault-injection test mode using **the exact same** typed prepare/submit/cancel/amend/close/protective-cleanup contracts, one Rust executor/risk owner, durable ledger, uncertain ACK/restart discipline, fee and bill reconciliation, and TCA intended for eventual live use. No separate Demo strategy, trading state machine, executor or permanent Demo-only mutation MCP API is permitted. The environment, credential-bound account, durable root, margin/position mode and explicit authority gate are different, and are tested for non-interchangeability.

**Current code versus physical acceptance:** The shared Rust executor admits supported Production FOK OPEN/CLOSE only after native CONTROL policy and independent live account/source/risk checks, never by reusing Demo-only mutation authority. A code-level Production path is NOT a verified live-money trade; until exact Windows CONTROL install, enabled real-account proof and independent venue readback, Production Live is NOT ACCEPTED.

**Live admission is not Demo admission:** the installed runtime remains Demo READ_ONLY; a future Production session can only trade after completed Stage 1–5 acceptance, explicit durable per-account/run/instrument policy, scoped Trade/no-Withdraw credentials, independent source and risk governance, physical recovery and native venue proof. Demo PASS and the Worker routing gate never enable real money.

**Cloudflare is an operator transport plane, not the source of trading safety.** The physically accepted single authenticated WSS serves the **one active** production or Demo profile, with a Hello/generation binding, isolated credentials/root and cross-profile fencing. Worker allows Demo typed mutation routing and only the five native-supported Production operations. Native Rust independently checks environment, CONTROL policy, UID, clock, rate, STOP/UNKNOWN, risk and ledger. A user or UI cannot confer Production authority by selecting a profile. Switching away from a profile with protected exposure must preserve independent supervision or reject. ChatGPT availability is never a prerequisite for protecting an open position.

**Typed trade commands stay on one primary MCP surface.** They invoke the existing Rust owner with immutable plan, risk, UID and ledger admission. The Production native path exists for the five narrowly scoped operations, but remains unaccepted until immutable Windows install and independently attested venue source and live execution. No second trading transport or Demo authority reuse exists. Never camouflage a mutation as a read or bypass a tool refusal. Autonomous safety and recovery may not depend on ChatGPT being online.

**Current Stage-4C physical proof (2026-10-11):** same-UID Demo G1 locally abandoned unsent PREPARED without exchange send; G2 preflight accepted; G3-A/B protected LONG/SHORT venue OPEN→fills→CLOSE with no residual ordinary/OCO and complete bills/TCA. **G3C also passed**: actual same-order LIVE unprotected reducing CLOSE AMEND 82700.20→82800.14, cancel with zero fills, separate fully FILLED IOC exit, exact parent OCO venue absent and `CLEANED_UP` without exchange POST, then one typed CONTROL restart preserving exact ledger states and zero positions/orders/OCO. Market WS recovered from fail-closed NOT_READY to FRESH. [Exact record #223](https://github.com/iamaman11/okx/issues/223#issuecomment-6103793654). G3-D/E/F, G4 and Live remain OPEN; a prior GH same-intent conflict was a rejection, not a successful duplicate prepare.

## First-party typed execution on the primary transport (Demo G3C physically accepted)

The current `execution_action` is a finite typed MCP tool, **not** arbitrary JSON/RPC or a second exchange owner; `abandon_prepared` works only for an exact unsent local PREPARED intent with no exchange effect. `executor_preflight`, `execution_status` and `account_summary` remain read-only. The bound Hello profile is never a user-selectable argument. Production Worker dispatch is limited to begin, prepare, submit, finalize and quarantine; all other Production mutations are refused before Rust. Even an allowed dispatch is NOT order authorization: every exchange-affecting operation still requires native policy, per-intent risk and independent venue readback.

**No opaque batch mutations or automatic retries.** For a complete user-authorized Demo acceptance run, ChatGPT drives the ordered matrix as sequential *durable* intents: one prepare, one submit, one exact-ID readback and independent same-UID venue reconciliation at each checkpoint. A lost tool response is **UNKNOWN**, never a reason to make a fresh request ID or send again. Any platform/connector refusal is respected; GitHub encrypted DATA is not a route around a refused primary write. The admitted Demo operations are mechanical exchange tests and are not strategy-directed live trades. Update tools once after verified Worker schema deployment; absence of the tool is not permission to send via an alternate transport.

## MCP tool schema and permission preflight

1. After **every deployed Worker change that modifies tool names, parameters, enum values or descriptions** check `runtime_status.tool_contract_version`, deployed Worker version and the *ChatGPT-visible* tool/schema. If equal, do not require a ritual refresh; if unavailable, stale or mismatched, use **Update tools** once through ChatGPT UI, then re-check the actual offered enum and perform a read-only black-box probe.
2. This is distinct from `query_capabilities`: changing only the versioned analytical **data catalog** should be discovered through that read-only catalog, **not** by forcing an MCP tool registration update. Record tool-version/visible-actions/deploy evidence in #160/#223.
3. For a refused action, classify independently: (a) ChatGPT/connector denies **before tool delivery** (no runtime request_id), (b) rejected tool schema/arguments, (c) transport/runtime/profile/auth, (d) Rust policy/venue outcome. Permissions such as **Allow all actions** do not guarantee every specific platform call is admitted. The 2026-10-10/11 `prepare` began succeeding after the user reported a refresh, but the previous client-side schema snapshot and rejection reason were not retained: **likely schema drift, not proven causality**.
4. No platform/connector safety refusal may be retried via renamed action, fresh request ID or GitHub mailbox. A delivered write that loses ACK is **UNKNOWN, non-replayable** until exact intent, ledger and exchange reconciliation; `retryable=true` on an old generic transport envelope must never authorize replay of a mutation.
5. The operator is responsible for deciding what to send; the same Rust execution core is responsible for admission, durable status, exchange effects and recovery. Use no second executor/daemon/polling loop.

## Deferred Stage 5C-2: one environment choice, no G3-D trading dependency

The eventual trader/user choice is `Торговая среда: Демо | Реальная`. **Do not implement or exercise it during G3-D1/D2/D3:** those tasks operate safely inside the already-running Demo profile. This Stage 5C-2 acceptance is an **operator-level choice**, not a new UI/backend or additional MCP mutation tool. The native authorized owner still uses existing typed CONTROL `start_demo_acceptance` / `restore_production_agent` **only after** both direct and automatic desired-profile changes enforce the same verified flat/quiescence guard. The current raw commands are **not** accepted as safe unrestricted toggles and should never be used to evade a failed gate.

- **First read-only:** current CONTROL owner/PID/desired and running profiles plus bound runtime status/UID, FRESH account positions, all ordinary and complete SWAP/FUTURES conditional/OCO inventory, coherent ledger, all pending/UNKNOWN/prepared/reverse/mutation state. Report `environment`, `trading_permission`, `quality`, `positions/orders/OCO`, `unresolved_intents`, and `switch_allowed`/one exact blocker (bounded packet). A profile or MCP session `PASS` alone is not venue flat proof.
- **One native guarded handoff:** quiesce new mutation admission, prove flat/no unknown/complete same UID with no intervening state changes; then persist destination and switch once, verifying new runtime profile/account/environment, correct separate secrets and durable single owner. Controller restart, `reconcile_desired`, startup restoration and manual CONTROL path must share the same guard. If any source or destination proof fails, report typed `BLOCKED/UNKNOWN`, leave or restore the original safe owner rather than silently abandoning exposure.
- **Semantics:** Demo is permitted only through current Demo-specific Rust preflight/mandate; `Реальная` means Production **ReadOnly** now. The environmental selector NEVER grants Live trading. A later separate explicitly human-authorized Live activation requires Stage 5 acceptance, keys scoped without Withdraw, live-specific risk admission, revocation and canary proof.
- **Acceptance:** T1/T2 include LIVE/partially filled order, guarded ACTIVE/PENDING/FAILED OCO, PREPARED/UNKNOWN, opposite UID, stale or incomplete inventory, crash after desired-state persistence, profile mismatch, lost connection and duplicate target. Every unsafe case denies **before stopping the current owner**, and does not send an exchange POST. T3 full CI and verified exact Windows install; T4/T5 bounded **FLAT Demo → Production ReadOnly → FLAT Demo** with independent before/after venue truth and native restart, never exposure. [Canonical Stage 5C-2 acceptance scope and current D1–D3 critical path](ROADMAP.md).
- **Trade mechanic boundary:** D1 read-only account-mode/protection capability; D2 existing Rust ledger exact protective coverage for ADD/REDUCE/HEDGE, never delete `InstrumentBusy` on hope; D3 Demo physical low-risk fills/bills/OCO/flat. These are the **sole current** execution acceptance tasks. The selector/ownership-fence is 5C-2, not a prerequisite or a second trading owner.

## Trader/analyst decision packet and measurable user experience

- For normal questions prefer one bounded high-level MCP operation (`query` catalog, `market_overview`, `market_intelligence`, `account_summary`, `portfolio_risk`, `execution_status`); do not fetch raw book/history or expand entire mailbox/CI logs unless an exact forensic question requires it.
- Minimum answer fields when relevant: instrument, account **scope/profile UID**, status, **transport/session** generation separate from **market/account source** time/age/coherence, factual observed vs modelled vs unavailable, risks/constraints, active/nonterminal intent IDs, reconciliation coverage, blockers and **next permitted action**. `DEGRADED` aggregate market research is not automatically trade-ready even if WSS session is `FRESH`.
- Payload target: **one coarse response <=12,288 bytes** (not a guaranteed hard cap for every operation); prefer summary counts and targeted exact-ID detail/continuation for old managed intents and orders, without suppressing any unresolved exposure or incomplete inventory. Source-only reconciliation for the authenticated UID must not be mistaken for main+all-subaccounts truth.
- Record for Stage-5C acceptance: scenario, run N, p50/p95/p99 end-to-end/tool latency, runtime/venue/source ages and recovery gaps, payload bytes/number of calls, errors/unknown/denial classes, measured tokens **only if a client exposes them**. Character count is not token count; a fast single read sample does not prove p99 or reliability.
- The checkpoint for a new ChatGPT conversation is a **compact** status from #160: current single ID, installed tested tree/artifact, current profile/UID/source quality, exact unresolved intent(s), allowed next operation and evidence IDs. Do not depend on retained ChatGPT context as an execution journal. User intervention only for an explicitly unobservable local/credential/UI gate; coding/testing/CI/evidence ownership remains with the agent.

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
