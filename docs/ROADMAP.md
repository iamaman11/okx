# Roadmap and acceptance cursor

## Single canonical plan

**#160 — Industrial trading platform roadmap**

Issue #160 is the authoritative detailed roadmap and acceptance contract. This file is the repository-side summary/cursor only; it must not become a second competing plan.

**Executable Stage-5 and future live-admission checklist:** [STAGE5_IMPLEMENTATION.md](STAGE5_IMPLEMENTATION.md), subordinate to #160. It maps existing module owners, concrete changes, positive/negative tests, T1–T5 evidence and terminal PASS/FAIL. Stage 4C exchange acceptance stays in #223 and this ROADMAP; it is not replaced by that checklist.

The platform follows exactly five development stages:

```text
Repository Guard v1
    ->
Stage 1  TRUTH
    ->
Stage 2  INTELLIGENCE + RISK
    ->
Stage 3  SCIENTIFIC RESEARCH + REPLAY
    ->
Stage 4  EXECUTION + TCA
    ->
Stage 5  GOVERNANCE + OPERATIONS + LEARNING
    ->
explicit optional live activation gate
```

The final live activation gate is not a sixth development stage. Production live trading remains fail-closed until separately authorized after Stages 1–5 pass.


## CURRENT: one sequential stage gate and verified acceptance — 2026-10-10

**Canonical authority #160; this is its operative summary, not another plan.** Exactly ONE active work ID, completed and evidenced before opening the next. Historical CURRENT/NEXT or "parallel" instructions elsewhere are superseded. A blocked step remains blocked; do not jump to Stage 5B/C, an independent research task or optional Stage-4D feature. Safety restoration is part of the same step. An OpenAI/connector safety refusal must NOT be bypassed by renaming the request or using another tool/transport. No new daemon, scheduler, executor, arbitrary RPC or mutation MCP endpoint.

**Uniform status:** ACCEPTED requires all applicable T1 deterministic positive/negative, T2 ownership/freshness/integration/restart, T3 six-job exact-head CI and tested-tree=merged-tree plus installed agent/controller hash (if changed), T4 actual authorized primary Cloudflare read-only or supported encrypted Demo physical exchange result with correct environment/UID, and T5 negative/fault recovery. SOURCE_ACCEPTED is not physical PASS; NOT_ACCEPTED/BLOCKED_EXTERNAL are not PASS. Capture exact test name, CI run, source tree, artifact/installed SHA, current process, UID fingerprint, request/ordId/tradeId/billId, bounded coverage, no-blind-replay and independent restore. No historical error, ciphertext-only ACK or a zero production-subaccount balance proves Demo flatness.

| Sequence / owner | Concrete requirements, T1–T5 tests and terminal result |
| --- | --- |
| **Stage 1 TRUTH — ACCEPTED/CLOSED** | WSS443, clock/rate, instrument/venue/reference, account + fills/bills and reconciling ledger. T1 parser/clock/changed-rule fixtures; T2 WS/REST/private convergence/backpressure; T3 exact Windows release; T4 primary FRESH market/account plus DATA parity; T5 stale/reconnect/rate/clock rejects. Accepted for **one authenticated execution account**, not main + all subaccounts. Existing closure #160 remains the source record |
| **Stage 2 INTELLIGENCE/RISK — ACCEPTED/CLOSED** | Market liquidity/derivatives, deterministic portfolio and immutable mandate/risk pre-send. T1 pricing/size/stress/hard-policy fixtures; T2 generation/coherence; T3 CI 37160080767 exact tree b63f98d57df6e9e1e1b6fb1c24c949d580717603 and verified Windows artifact 11287008643; T4 non-zero Futures virtual risk, OKX account-position-risk and primary allow/reject; T5 stale/policy/leverage fail before send. Closed #197 |
| **Stage 3A DATA, 3B REPLAY, 3C SCIENCE ENGINE — SOFTWARE_ACCEPTED** | T1 canonical hash/no-lookahead/funding/cost/split/holdout and anti-overfit fixtures; T2 identical replay/lineage after restart; T3 immutable source and version IDs; T4 bounded real dataset/replay and honest insufficient/REJECT through primary research MCP; T5 corrupt/gapped/reused OOS cannot promote or trade. H1 rejected; no alpha claims |
| **Stage 3D PAPER/SHADOW scientific exit — OPEN** | T1 state transitions/parity; T2 one agent event session and stale-data denial; T3 exact installed evidence; T4 *eligible* frozen BACKTESTED to operator-approved PAPER/SHADOW real session; T5 corrupted/stale/insufficient input blocks promotion with zero orders. H2 VALIDATION 22 < 30, FINAL_OOS sealed/unconsumed (#216). Research software completion is not strategy approval. Earlier authority permits synthetic Stage-4 Demo mechanical testing without profitable alpha, but **not simultaneous work**. This candidate-specific scientific exit is mandatory before enabling **that** strategy in live trading, but is not a prerequisite for Stage-5 software or synthetic Demo engineering |
| **Stage 4A/4B engine, Stage 4D MCP context — SOFTWARE_ACCEPTED** | T1/T2 one Rust execution owner, protected intent/unknown, AMEND/CANCEL/CLOSE/TCA and bounded packets; T3 verified accepted software; T4 supported primary read-only observation; T5 duplicate/recovery/payload rejection. Does not imply Stage-4C physical fills. Optional read-only Demo MCP must not be built as workaround for a blocked call |
| **4C-G0 diagnostic — production ACCEPTED, Demo OPEN** | PR #268 exact CI 38057028443, tested=merged tree c434e7b2ce225e9739155aa7ae83fdd838b94c61; controller bundle 11672008469 physically installed with verified launcher rollback. T1/T2 old-vs-current log, rotate/overflow and PID lease; T4 production owned current PID/complete launch/fatal=null; T5 old Demo instId log is historical, not active. Current Demo PID read-only result still unaccepted |
| **NEXT 4C-G1 — sole active slice; NOT_ACCEPTED** | Prove **previous Demo exposure**, not a new trade. Required T1/T2 stale/truncated/duplicate/exact-ID reject; T3 exact installed agent/controller; T4 authenticated same-Demo-UID current PID plus decrypted same-request positions/ordinary + all four SWAP/FUTURES conditional/OCO pending scopes, fill/bill/ledger and original submit_long/submit_close client/ordId/fill identity reconciliation; T5 unknown results produce ZERO new sends and keep EXPOSURE_UNVERIFIED. One Demo STARTED PID 20312, but next read-only CONTROL status was blocked by ChatGPT **before GitHub publication**. Legitimately determine whether approved read-only observation is possible; **do not retry blocked control by changing request ID/tool/channel**. Production restore PASS PID 13984, Cloudflare fresh generation 284, zero exposure only in production authenticated subaccount; Demo remains UNKNOWN |
| **4C-G2 — after G1 PASS** | Fresh same-session Demo preflight with authenticated UID, credentials, margin/collateral/clock/private WS/reference and risk. T1 malformed LIVE instId/state/lot/price rejects; T2 exact generation; T3 installed artifact; T4 fresh accepted preflight; T5 stale/wrong UID/unknown exposure => 0 gateway sends. Historical preflight invalid after profile switch |
| **4C-G3 — after G2 PASS** | Tiny Demo protected OPEN→actual fill→risk-reducing CLOSE→terminal flat; separate *unprotected* reducing CLOSE AMEND, hedge long/short, bounded ADD/REDUCE/REVERSE where supported, TP/SL and partials. T1/T2 partial/race/hedge fixtures; T3 exact installed engine; T4 ordId/tradeId/billId, fee currencies, VWAP/TCA, four-scope no-orphan; T5 unsupported modes/protected OPEN amend reject, no blind replay or exaggerated size |
| **4C-G4 — after G3 PASS** | T1/T2 uncertain ACK/restart/dedup/ledger; T3 exact artifacts; T4 no orphan and same-account reconciled Demo final plus production read-only restored with Cloudflare FRESH; T5 negative fault/recovery without duplicate sends/unknown exposures. Only then Stage 4 T1–T5 CLOSE |
| **Stage-3 candidate-specific science — separate activation gate** | Respect frozen #216 or a distinct preregistered version on independent eligible data; T1–T5 positive BACKTESTED/PAPER/SHADOW proof is required for STRATEGY_APPROVED, **not** a prerequisite for Stage-5 engineering. REJECT/INSUFFICIENT remains nonpromotion; no false alpha or auto-promotion |
| **5A governance — after 4C** | Existing 5A-1 policy → 5A-2 durable STOP_NEW_RISK → 5A-3 complete admission. T1/T2 day boundary, partial SWAP/FUTURES history, breach/candidate-only, restart, close/cancel preserve exits; T3 *installed* exact agent; T4 bounded same-UID Demo accept/deny; T5 no forbidden exchange send. #263/#264/#267 merely source accepted, not installed/physical |
| **5B learning — after 5A** | 5B-1 exact intent→order→fill/bill/fee/funding/TCA residuals; 5B-2 immutable KEEP/DEMOTE/RETIRE/NEW_VERSION. T1/T2 partial/duplicate/non-settle fees/manual/negative OOS; T3 immutable lineage; T4 matched real venue outcome and compact primary research review; T5 missing evidence INCOMPLETE, no imaginary profit/promotion |
| **5C industrial operation — after 5B** | 5C-1 source age vs MCP freshness and measured p50/p95/p99 with sample N and context budget; 5C-2 lost-ACK/reboot/CAA or physically tested alternative protection; 5C-3 credential/provenance/rollback. T1/T2 fault fixtures; T3 installed exact SHA; T4 primary product workflow measured; T5 crash/unknown/recover without orphan, second owner or blind send |
| **FINAL LIVE gate — separate, NOT Stage 6** | After all mandatory engineering, physical Demo and governance gates PASS, and separately approved science for strategy-directed trading, plus human-approved production account-bound Trade/no-Withdraw credential/expiry/capital cap/authorized intent. T1/T2 deny wrong scope/revoked/stale; T3 trusted install; T4 tiny preauthorized live canary with exact fills/bills/close; T5 stop-new-risk/protection/recovery. Presently DISABLED |

**NEXT SINGLE ID: 4C-G1-READONLY-DEMO-EVIDENCE.** Resolve availability/permission for the previously blocked **read-only** Demo diagnosis and exact-account exchange/ledger observation. If still not supported/permitted, record BLOCKED_EXTERNAL and STOP; do not switch workstreams or invent a bypass. If supported, prove current PID and old intent effects before any fresh preflight or Demo order.

**Every terminal entry in #160:** slice ID/status; owner; exact tested head/tree/six CI jobs; merged-tree parity; installed relevant SHA; T1..T5 stimulus/assertion/evidence; profile/UID/generation/freshness; exact request/ord/fill/bill IDs; negative recovery; residual uncertainty; operator usability bytes/tool calls; only one NEXT ID. The detailed 4C-G0..G4 and Stage5 tables below specify test fixtures; this matrix owns the *order and current state*, not a new capability authority.

## Product mission: live-ready design, Demo as exchange acceptance only

**Target product is real OKX trading.** Both Demo and eventual live trading must use the **same typed trade-intent contract, same Rust `okx-execution` owner, mathematical/risk policy, order prepare/submit/cancel/amend/close/protective-cleanup logic, persisted uncertainty/idempotency model, exchange fill/bill/TCA accounting and recovery code**. Venue environment is an explicitly bound runtime parameter, not a second execution architecture. Reuse the same production-intended code in Demo to verify mechanics safely; do not develop or maintain an independent Demo trading product.

**CURRENT implementation gap, not accepted live parity:** `crates/okx-agent/src/execution_runtime.rs` currently constructs `ExecutionRuntimeMode::ReadOnly` for production and `DemoAcceptance` for Demo; mutation-capable wrappers explicitly require `DemoAcceptance` and accepted Demo preflight, and `OrderExecutor::enable_demo_acceptance` is the only narrow temporary enable path observed. These wrappers are **not** proof that a safely authorized production write path already exists. Future Stage-5/live-gate engineering must introduce a **single typed environment-scoped admission policy** into the existing execution owner with production credential/UID, explicit operator authorization and fail-closed tests; reuse common order/risk/ledger mechanics, never copy the Demo authorization path or simply remove its environment guard. Until then: `PRODUCTION_MUTATION=UNAVAILABLE`, `ALLOW_LIVE_TRADING=false`. This gap must be resolved and T1–T5 proven before any live canary.

**Different admission, never different business rules:** Demo binds its own immutable environment/account UID, Trade-scoped Demo credentials, separate durable state and acceptance budget; it may perform bounded physical venue tests once fresh preflight/risk/protection/restore conditions hold. Production begins read-only; **actual live writes require separate least-privilege credentials, operator approval, accepted Stages 1–5, exchange-native disconnect protection decision, irreversible live authority enable and tiny canary/rollback proof**. A successful Demo order or compatible API shape cannot bypass this live gate. Every acceptance test must assert that a Demo flag/credential/ledger cannot be silently substituted into live.

**User-facing transport and local trading engine are separate concerns:** primary Cloudflare MCP provides compact authenticated observations and discretionary operator workflows; ChatGPT is *not* the low-latency live order controller or an always-on monitor. Any eventual supported explicit trader action must cross typed authenticated authorization into the **same** Rust owner, regardless of whether its authenticated delivery is MCP or another already-approved control boundary. Do not add another order engine or route trade writes through a read-only tool. Autonomous/recovery-critical protection must remain safe if ChatGPT/Cloudflare is unavailable. No new transport is authorized here.

**No Demo-MCP detour:** the *useful* 4D follow-up is read-only visibility of whichever environment is actively selected using the **existing** Cloudflare WSS, one agent and fenced profile generation. A new Demo-only MCP mutation API (**old optional P2**) is **NOT a prerequisite** for Stage 4C, Stage 5 or live activation; implement no such API without a proven production use-case, genuine authorization and security review. Keep supported encrypted GitHub #234 venue acceptance during transition. If one active Demo profile would blind production while real-money exposure needs monitoring, profile switch must be refused or guarded by an independently accepted safety model before any live use; do not quietly trade unobserved.

## HISTORICAL source/deployment checkpoint — 2026-10-10 (superseded by serial matrix)

This dated checkpoint supersedes the old PR #260-only cursor below; historical notes are retained as evidence, not active instructions. The single detailed acceptance authority remains #160; Demo exchange evidence is #223; subordinate Stage-5 work is [STAGE5_IMPLEMENTATION.md](STAGE5_IMPLEMENTATION.md).

- **Merged source vs deployed binary:** canonical GitHub `main=43c00ebffdf328518b15de6eac92426dc0c9dfc0` includes #260–#265 plus **#267**. #263 durably records one-way `STOP_NEW_RISK`; #264 latches independently observed account breaches; #267 tightens daily-loss evidence to require exactly one complete SWAP **and** FUTURES positions-history scope, rejecting missing/duplicate/unexpected ones. #267 exact-head native/Linux/architecture/Cloudflare CI `38055176808` is **6/6 SUCCESS**, and tested head/merged tree both `05fd2f8b204c426174a3737951d8a8fa9d8dd561`. Code source T1–T3 progress does **not** imply installed physical Stage-5 acceptance. **Last verified installed Windows artifact remains #260** `11650383653`, tree `2ab5652c8346ebb745b9681690b9fd2c82a05a39`; do not equate current source, installed binary, and Demo exchange acceptance.
- **Fresh independent production proof:** read-only CONTROL #12 `ctl_stage4c5_readonly_progress_20261010_1320z` terminal [PASS](https://github.com/iamaman11/okx/issues/12#issuecomment-6097917548) at 2026-10-10 13:19:17 UTC: one Job-owned `production` profile, exact installed #260 artifact SHA. Cloudflare MCP `runtime_status` **PASS/connected/session_fresh**, generation **281**; authenticated `account_summary COMPLETED/FRESH`, strictly `read_only`, 0 positions, 0 ordinary pending and 0 covered conditional/OCO pending with complete four-scope coverage and `reconciliation.consistent=true`. This scope is **authenticated subaccount only**, not all main/subaccounts and **not Demo**. BTC-USDT-SWAP WS had a transient `DEGRADED/WS_SUBSCRIPTIONS_INCOMPLETE` REST fallback, then recovered to `FRESH/WS_CURRENT_GENERATION_COMPLETE`; don't treat transport freshness as data freshness.
- **Stage 4C safety gate:** #223's Demo `reference_data/missing_required_field/instId` and credential markers are from **retained profile logs**, not proven current-PID evidence. Current CONTROLLER code explicitly labels such tail evidence not current-process-scoped. Treat the live failure cause as **UNPROVEN** until a bounded new Demo process/provenance attribution, not as a confirmed parsing bug. Independent authenticated **Demo** fill/bill/order/protective/ledger reconciliation for earlier `submit_long`/`submit_close` ciphertext remains unaccepted: **Demo exposure UNVERIFIED**, not FLAT. No new exposure, replay or parser relaxation until G0–G2 are proved.
- **Next Stage 4C physical T1–T5:** on fresh same-session Demo UID/environment/collateral/preflight and trustworthy reference/risk, obtain exact-id fill/CLOSE/flat plus full ordinary/protective inventory and fills/bills/fees/TCA; separately prove unprotected risk-reducing CLOSE AMEND, hedge/ADD/REDUCE/REVERSE where supported, controlled partial/race/UNKNOWN/restart and durable protective cleanup. Production live writes stay disabled and are never a recovery shortcut.
- **Stage 5:** #263/#264/#267 are **source/CI progress within 5A**, not complete 5A nor Stage 5. Prove installation, historical negative cases, persisted stop under restart, permitted safe reducing actions, and zero unintended sends; complete 5A-1, 5B-1/2, 5C-1/2/3 with exact test/evidence matrix. Production-scoped write admission and an actually supported authenticated operator intent surface remain missing; future activation requires a separate explicit live gate.
- **Operator usability:** preserve one Cloudflare MCP read-only primary and one Rust agent; improve **read-only** active-profile Demo visibility only after resolving potential Demo exposure, with account/profile-generation fencing. Current Demo launcher intentionally omits Cloudflare WSS and forces encrypted GitHub #234 for Demo DATA. Never add Demo-only mutation MCP tools, arbitrary RPC, second executor/daemon or extra scheduler solely to hide this limitation. Measure source age, WS readiness, p50/p95/p99 request/recovery latency, payload/context budget, and restore/no-blind-mutation success across T4/T5. Target normal operator response <=12,288 bytes.

## Detailed Stage 4C G0–G4 test contract — status and next action from serial matrix

**Authority / status:** subordinate to #160, evidence in #223. Code merged through #267 plus documentation #265 does not establish a new installed agent, Demo exposure state, or Stage 4C T4. A controller `runtime_diagnostics.fatal_error` may be **retained historical evidence** (see `current_process_scoped=false`); the reported `reference_data/missing_required_field/instId` and absent Demo credential startup markers **must be classified against one fresh active process before describing them as today's failure**. The normal production Cloudflare session was separately read-only/FRESH; it cannot establish Demo flatness. Do not patch away missing `instId` or create synthetic reference rules without exchange row provenance. Existing `okx-observation/src/reference.rs` intentionally excludes documented identity-free preopen FUTURES announcements but rejects all other missing IDs.

**Single logical owner sequence; each gate records PASS | NOT_ACCEPTED | BLOCKED_EXTERNAL and exact source/request/time/account/profile evidence.** Do not skip a gate because a later CI passes. Stop conditions are explicit; no live orders in any gate.

| ID | Concrete action / existing owner | Repeatable test and ACCEPT only if | If failed / safety boundary |
| --- | --- | --- | --- |
| **4C-G0: controller + source truth** | Read-only CONTROL #12 `status` and bounded current-process diagnostics; inspect installed binary source tree/agent SHA, one Job-Object agent, desired/running profile, current-versus-retained fatal timestamp/scoping, startup readiness flags. Existing `okx-host-control`, `okx-agent` | T4: exact controller terminal result and one Job-owned installed artifact; **current-process** fatal attribution additionally requires process PID/launch identity, not presently exposed by ordinary `status`. Until supported by bounded existing CONTROL diagnostics, mark current-error proof `NOT_ACCEPTED`; a minimal typed read-only process-identity field may be added in the existing host controller with T1/T2 if necessary. T5: retained fatal/stale marker does **not** fail a healthy active process; reproduced current-process fatal does fail readiness | If only retained `instId` evidence, classify **HISTORICAL/UNPROVEN**, not reproduced bug. If fresh current-process failure, capture a bounded non-secret sanitized instrument `instType/state/instFamily_present/instId_present` class, endpoint and environment (not public raw dump); reject a live malformed row. No speculative parser relaxation |
| **4C-G1: previous Demo exposure** | Under one authorized Demo owner/profile and supported typed encrypted GitHub DATA #234 or approved read-only active-profile Cloudflare when actually available: authenticate Demo UID/environment/observer permissions; retrieve coherent positions, ordinary order inventory, all four SWAP/FUTURES × conditional/OCO pending scopes, fill/order/bill history and durable exact intent IDs for earlier `submit_long`/`submit_close` | T4: AEAD-authenticated **plaintext result**, matched request ID, exact Demo UID fingerprint, FRESH/coherent snapshots and complete bounded nontruncated scopes; all earlier submitted intents resolved with exact exchange identities. Flat only if position 0, ordinary pending 0, covered pending protective 0, unmatched managed/unmanaged risk 0 and no unknown effects. T5: truncated history, wrong UID, incomplete scope, stale generation or unavailable decryption returns **EXPOSURE_UNVERIFIED**, never FLAT | Do not place new exposure, replay old submit, or assert terminal flat. If Demo is not running, determine bounded CONTROL handoff/recovery readiness and independent monitoring first; no unconditional switch if it could abandon exposure |
| **4C-G2: Demo bootstrap and safe admission** | If G1 is independently reconciled, run fresh same-session Demo preflight on the **installed** exact artifact; verify reference/market identity and viable price/size/fees, credentials observer Read-only/executor Read+Trade/no Withdraw, long-short mode, collateral, private WS, clock, owner, recovery and protective cleanup | T1/T2: malformed **live** `instId`/tick/lot still rejects; documented preopen FUTURES announcement omitted, not made tradeable; no credentials or UID substitution across Demo/prod. T4: current-session preflight `accepted=true`, all required source freshness and zero unknown exposure. T5: stale preflight, wrong Demo UID, missing collateral or bootstrap incomplete generates zero gateway sends | Reproduce `instId` failure with row-class metadata before any narrow code fix. Re-run T1–T3 + installed T4 after fix, never bypass validation |
| **4C-G3: exchange mechanics** | Single `okx-execution` owner runs tiny versioned Demo test matrix: protected OPEN→actual fill→risk-reducing CLOSE→terminal flat; separate unprotected reducing CLOSE AMEND; long/short and bounded ADD/REDUCE/REVERSE where venue supports; attached TP/SL exact-owned cleanup | T1/T2 fixtures cover full and partial fills, cancels, STP, hedge/reverse and protected-OPEN amendment rejection. T4: exchange ordId/tradeId/billId, fee currency/maker-taker, fill VWAP, TCA and decision/risk/reference lineage, exact order/algo identity; after all tests account flat and four-scope orphan-free. T5: unsupported hedge/size/mode rejects before send | If fill/ACK uncertain, stop new risk and reconcile exact identities without blind replay; never artificially increase size to force partial fills |
| **4C-G4: recovery + exit** | Same owner + existing Windows CONTROL: controlled nonterminal restart/lost-ACK/duplicate-intent tests, bounded restore to strictly read-only production, fresh Cloudflare production FRESH/account check | T1/T2 restart ledger tests and T3 tested-tree=merged-tree=installed-artifact provenance. T4/T5: single agent, no duplicated exchange send, known terminal Demo intent/protection outcome, no abandoned exposure; restoration terminal PASS, production read_only and no unexpected mutation; produce **one** 4C T1–T5 acceptance matrix with exact evidence ids | Any unknown effect = NOT_ACCEPTED; preserve ability to reduce/manage exposure. NEVER claim physical PASS from ciphertext envelope or CI alone |

**Manual operator intervention:** only when the bounded tools explicitly cannot prove a required local credential/UI fact. Specify one non-secret observation, not a long Windows command sequence. The coding, policy, CI review, and acceptance decision remain with ChatGPT. Operational fallback is deliberately `STOP/READ_ONLY`, not a new transport or bypass.

**Operator full-cycle acceptance (Stage 4C→5C):** for each representative event `read-only diagnose → reproduce → smallest existing-owner correction → exact-head CI → no-rebuild tree proof → verified CONTROL install → same-binary T4 → T5/recover → normal primary MCP fresh/read_only`, record source/tree/build/run, Windows PID/SHA, exact request and terminal result and reason if blocked. A claimed “full ChatGPT lifecycle” needs one completed real sequence including a negative recovery case; separate successful CONTROL operations do not add up automatically.

## HISTORICAL checkpoint — PR #260 installed; Demo position outcome unverified (2026-10-10)

**Historical snapshot only; the dated serial matrix above takes precedence.** #160 remains the acceptance authority and #223 the execution evidence ledger. Older dated sections below are historical and do not override this checkpoint.

- **Source and deployment, accepted:** remote main `b95d39c95a92118b6ccec27fe17ed44d9761cd8d` after merged PR #260; tested PR head `cae27995b6b24a1f8ffa135e39a1851a5e3e0404`; **tested/merged/installed source tree** `2ab5652c8346ebb745b9681690b9fd2c82a05a39`. PR CI run `38004372067` and post-merge `38004988598` each have six passing jobs including native Windows. Verified Windows artifact `11650383653` deployed via CONTROL `ctl_stage4c_big_slice_deploy_20261010a` terminal PASS; installed agent SHA256 `263f87c1ffbaff159e8bff416180378fd5a03e910a2d7a9380411cdab5760985`, persisted provenance.
- **Actual runtime, not yet closed:** CONTROL `ctl_stage4c_readonly_audit_20261010a` at 2026-10-10 00:20:46 UTC returned PASS, one Job-Object-owned Windows agent, `desired_profile=running_profile=demo_acceptance`, exact deployed binary/hash. In Demo the controller deliberately omits Cloudflare WSS attachment; hence normal primary MCP reports OFFLINE while Demo is selected. This is intentional launch configuration, not evidence of a Cloudflare transport defect.
- **Safety-critical open evidence:** encrypted Demo #234 has same-request response envelopes for `prepare_long`, `submit_long`, read-only account/order checks, `prepare_close`, `submit_close`. Their existence proves delivery/response correlation, **not** venue fill/CLOSE success or terminal flatness. No authenticated plaintext result/independent post-CLOSE exchange/bill/position/protective-inventory reconciliation for this latest sequence has been accepted here. `EXPOSURE=UNVERIFIED`; **NO blind retry, new OPEN, unconditional profile switch/restore, or Stage-4C PASS** based only on envelopes. First obtain new supported authenticated read-only exact-account exchange/ledger evidence while the existing Demo owner is active, settle uncertain effects through exact IDs, then bounded restore and fresh production MCP/account verification.
- **Scope:** Stage 4C.1 non-fill accepted historically; #260 is engineering CI/installation accepted, **not yet fully physical filled-position/cleanup acceptance**. Stage 4C.2–4C.3, Stage 5 and Stage 3 scientific strategy promotion remain OPEN. H2 FINAL_OOS remains sealed. Production live authority stays FALSE.
- **Historical optional usability proposal, not active work:** profile-aware **read-only** Cloudflare MCP for whichever *single* agent profile is active, including Demo. The architecture/acceptance proposal and its explicit no-mutation first phase are in the next section; implement only after current Demo exposure is safely reconciled. Update this CURRENT on subsequent proofs rather than treating historical snapshots as latest.

## Stage 4 scope: production-intended execution, Demo physical proof, operator transport

**Canonical work is Stage 4C exchange execution and Stage 4D operator DATA efficiency**, not a new P0/P1/P2 project. Original Stage 4D compact transport/context slice was already accepted (#241/#242); these are follow-on acceptance gaps within the existing stages.

**First — Stage 4C current exchange safety:** the installed Demo agent received encrypted correlated response envelopes for `submit_long` and `submit_close`, but a terminal **independent venue/execution-ledger flat, fill/bill/fee and full pending ordinary+protective reconciliation is not yet accepted**. Collect fresh authenticated exact-account evidence, resolve unknown outcomes by original exchange/order/intent IDs and manage any still-open exposure. Never blind-replay a mutation, assert CLOSE from an envelope, or unconditionally restore/abandon a profile responsible for live exposure.

**Core deliverable — one real-trading execution system, Demo-proven:** complete physical Stage 4C T1–T5 for the same order contracts we intend to use live: protected OPEN→fill→managed CLOSE→clean-flat; unprotected risk-reducing CLOSE AMEND; long/short/ADD/REDUCE/REVERSE when supported; partial fills and cancel races; exact pending TP/SL and owned cleanup; durable ACK/UNKNOWN restart and duplicate-intent fences; fills, bills, fee currency, TCA, account and decision lineage. *No* Demo-only business logic or duplicate order handler may substitute for production-intended code. No live account mutation yet.

**Secondary operator improvement — Stage 4D follow-up, read-only Demo visibility:** attach whichever **one active** production/Demo profile to the already-established Cloudflare WSS for *authenticated read-only* market/account/risk/order/execution observation, never a new Worker, MCP service, scheduler, state owner or runtime. Preserve `direct_transport_read_only` rejection for mutation-capable operations. Require exact environment/hashed credential-bound account ID, fresh source and session generation, profile-switch fencing and negative cross-profile/stale-result tests. A Demo profile cannot be switched to at the cost of unmanaged or unobserved live risk.

**Not on the critical path — dedicated Demo MCP mutation commands:** do not build these just to replace GitHub #234. An eventual **production-intended** typed operator trading authority should be evaluated only against a concrete user workflow, available connector scopes and platform permissions, complete risk/approval model and reuse of the *same* Rust executor. If not demonstrably useful and supported, leave existing read-only MCP intact and perform Demo physical acceptance through supported encrypted GitHub DATA. Never bypass tool refusals or piggyback mutations onto read-only queries.

**Stage 5 remains mandatory but strictly serial after Stage 4C:** canonical #160 P7/5A risk/governance, P8/5B attribution/learning, P9/5C reliability/recovery/security; they are neither replaced nor deferred by read-only Demo visibility. The only final live activation authority is the independent gate **after** Stages 1–5 PASS. The research H2 promotion status (#216) is separate, and no unsupported strategy may become live merely because the order engine works.

**Stage-4D read-only Demo follow-up acceptance:** T1/T2 single owner/isolated Demo credentials and root, unchanged mutation guard, wrong-profile and stale-generation rejection; T3 exact-head artifact/installed provenance; T4 live connected Cloudflare MCP reads in Demo with authenticated account and protected-pending coverage and successful production-return verification; T5 reconnect/restore/cross-profile/no-second-agent negative tests. Record latency, payload bytes, context impact and source quality; no new risk exposure is required for this transport test.

## Stage 5 remains independent: consumer-grade acceptance and critical path (2026-10-10)

**Canonical mapping:** #160 retains exactly **Stage 5 P7 governance/live safety (5A), P8 attributable performance and learning (5B), P9 observable/recoverable operation and release security (5C)**. The previous local labels `P0/P1/P2` were not stages and are retired here because they obscured the live-first product goal. The detailed T1–T5 Stage 5 requirements in #160 and the original Stage 5 section later in this roadmap remain authoritative.

| Work | Role | Required acceptance |
| --- | --- | --- |
| 4C: uncertain Demo execution and remaining filled order mechanics | **Stage 4 mandatory**; Demo is the venue safety acceptance environment for the **same future live engine** | Independent exact-ID flat/exposure proof first; then filled round-trips, amend, long/short, TP/SL, bills/TCA and fault tests |
| 4D: currently selected Demo account read-only via Cloudflare | **Stage 4 operator convenience**; follow-up to accepted context-size hardening | Exact profile/source generation and account identity; no mutation authority, no second runtime |
| Future operator-initiated order submission interface | **Not independently required for Demo**; product use-case must justify any new surface | Authenticated explicit intent/approval/policy/exchange constraints, single Rust execution owner, connector/platform acceptance; no read-only bypass |
| 5A / P7: live governance and protective stop | **Stage 5 mandatory** | Durable STOP_NEW_RISK, max daily loss/drawdown/exposure, safe cancel-entry/preserve-exit and guarded flatten; fail-closed restart |
| 5B / P8: attributable outcomes and learning | **Stage 5 mandatory** | Immutable decision→order→fill/bill/fee/TCA→strategy scorecard and `KEEP/DEMOTE/RETIRE/NEW_VERSION`; manual activity accounted separately |
| 5C / P9: dependable operations | **Stage 5 mandatory** | Auth/credential isolation, reboot/WS/Cloudflare/GitHub fault recovery, freshness/latency SLO evidence, CAA protectiveness, traceable release and rollback |

**Strict serial engineering critical path:** 4C-G1 → G2 → G3 → G4 → 5A → 5B → 5C → separate live gate. A positive Stage-3 candidate remains a separate required precondition for **strategy-directed** activation, not for engineering work. Never work on two slices at once; any blocked active gate means STOP with evidence. Stage-4D read-only Demo profile is useful for operator efficiency but must not block unrelated Stage-5 implementation. A dedicated Demo MCP order sender is unnecessary unless it serves a clearly validated future live workflow. The independent live-activation gate remains OFF until all mandatory stage evidence and explicit authorization pass.

### Trader/analyst usability scorecard — required observable fields, not invented PASSes

Every measure below must identify *scope + generation + source + collection window + sample count* and distinguish current measured evidence from desired budgets. Do **not** assign arbitrary universal latency/freshness thresholds: choose limits by instrument liquidity, exchange semantics, strategy time horizon and measured distributions. Missing coverage returns `UNKNOWN/NOT_READY`, never `PASS`.

| Consumer dimension | One decision-grade observable | Acceptance test and owner |
| --- | --- | --- |
| Transport availability and profile continuity | Authenticated primary session status, active Demo/production, hashed account UID, WSS generation, disconnect count and session-switch gap | Existing Cloudflare/agent/controller: profile switch and reconnect, stale-generation/cross-profile rejection; p50/p95/p99 and recovery objective measured across representative samples |
| Market data usefulness, not just connection | Source WS/REST, exchange event time, observed/available time, age/skew, sequence-contiguous order book, coverage, bid/ask/spread/depth, mark/index/funding | Existing observation: unready/stale/gapped feeds fail closed; quotes and reference data must be fresh for the trade horizon |
| Account/risk truth | Exact account/environment, positions, free collateral, leverage/margin, normal+four-scope pending protective orders, coherent ledger, risk/stop constraints | Existing observation/risk/execution: same-generation reconcile with exchange, independent unknown/unmanaged counts, stale/risk-increase reject |
| Trade execution quality | Intent→prepared→sent→ACK/UNKNOWN→exchange-verified identity, partial fill, VWAP, maker/taker fees and fee currency, bills, realized slippage and TCA | Sole Rust execution owner; no blind replay; exact identity, no orphan, bounded decision/preflight validity and reconciliation latency |
| Research decision validity | Timestamp/provenance, versioned hypothesis/mandate/cost, selection lineage, OOS/walk-forward, paper/shadow drift, alternative/no-trade baseline | Research/analysis; reject lookahead/insufficient evidence, no promotion on failed holdout |
| Resilience and governance | Lost transport/host reboot/WS gap behavior, STOP_NEW_RISK, capital-at-risk, CAA protective safety, last good truth and reasoned alerts | Stage 5A/5C failure injection + Windows physical recovery; use explicit safety-state rather than optimistic reconnect |
| Context and operator cost | End-to-end question→decision steps, p50/p95 tool latency, failed call rate, response bytes, schema footprint, tool count, tokens *actually measured by client*, redundant history read count | Stage 4D operator/MCP tests, coarse query and bounded evidence; do not present character counts as tokens or historical benchmarks as live metrics |
| Commercial and operational practicality | Expected edge net of spread/impact/fees/funding, position capacity, slippage sensitivity, session uptime, maintenance/release rollback, operator effort per safe task | Stage 2/3/4 analytics + Stage 5 review; no profitability/capacity claims without empirical evidence |

**Context baseline (already measured/accepted in #241/#242; not a live measurement today):** the removed duplicate full text JSON made sampled `account_summary` total output **9361 → 4590 characters**, with structured payload **4484 → 4483** characters; `market_overview` **6809 → 3228**; `market_intelligence` **5607 → 2765**. The fallback is **72 characters**. CI guardrails: text fallback ≤96 bytes, wrapper overhead ≤256 bytes, owned `tools/list` schema ≤24 KiB, coarse tool count ≤16 (then 13), ordinary decision packet target ≤12,288 bytes. These are **payload/schema budgets**, not proof of latency, tokens consumed, session uptime, fresh market data or trade safety. Review measured distribution and any changed tool set before declaring success.

**Operator baseline experiment after optional Stage-4D Demo read-only primary acceptance:** with exact installed source/tree and stable active profile, use matched representative real questions (market status, position/portfolio risk, candidate-order decision, execution audit, research decision) and record: direct MCP calls/round trips, bytes per call, reported timing and sample count, source freshness/skew, result coverage, whether one decision was possible without a separate status call, typed failure rate/recovery and actual tool-schema/answer-context tokens when measurable. Compare isolated Demo/prod only after verifying account fingerprints; never compare real and virtual balances as equivalent risk capital. T1/T2 deterministic boundary tests and bounded T4 black-box each have own evidence artifact. No extra daemon/telemetry DB/background polling is authorized.

## Historical operational cursor — 2026-10-10 (superseded by CURRENT above)

This compact snapshot reconciles the latest **physical evidence** with canonical authority #160 and execution acceptance #223. Historical stage notes below are retained as dated evidence, not competing CURRENT/NEXT instructions.

- **Source/CI (CURRENT as observed 2026-10-10):** merged `main=cb08ef57aa72c34c77b9f4f1ce57021e614fecbf`; installed PR #259 exact source tree `0760483f103b6ee3811c44805ea1a3173c0d8905`, Windows release artifact `11649852489`, agent SHA256 `8ac19247488ca52c0871da94b2c8b1109fba0c3cc3afb28b8a07df892b05d03f`. PR #257/#258/#259 merged and physically accepted for narrow scopes (#223 latest comment); **PR #260 candidate not yet merged/deployed or physically accepted**. Historical prior-release: production GitHub `main=3ea0b49662c1296e78ef24ba91e8a2c1e8e32c79`, exact installed source tree `a2d5f0bf7392a7186df3a03fcb5e1e2ef14cf51d`, Windows release artifact `11647077099`, installed agent SHA256 `5dfad4e1160bfda24648d0b0c05d21ef50818cc1ad1868d65927f8251b95c54b`. PR #254/#255 merged with exact tested-tree == merged-tree; PR CI runs #37993324848/#37995390048 six gates SUCCESS each, corresponding post-merge no-rebuild CI runs #37994901496/#37996089646 SUCCESS; typed CONTROL deploy terminal PASS. Exact provenance and physical Demo proof in #223 comment 6089930662.
- **Stages:** Stage 1–2 CLOSED; Stage 3A/3B/3C infrastructure accepted, Stage 3D PAPER/SHADOW infrastructure available, **final Stage 3 strategy promotion remains OPEN** (#216); Stage 4A/4B CLOSED; **Stage 4C physical Demo venue acceptance OPEN** (#223); Stage 4D MCP/context slice CLOSED (#241/#242); Stage 5 and optional explicit production-live gate pending.
- **Stage 4C.1 accepted (2026-10-09 21:56 UTC):** one real Demo-only minimum post-only protected OPEN was exchange-ACKed once, independently reconciled/canceled and terminally verified with zero fills/positions/ordinary pending orders and `NOT_ACTIVATED` TP/SL; no protected OPEN amend. Two archive-only summaries initially missed the canceled order (managed unresolved=1), so `okx-api` now merges bounded recent+archive history with exact-ID validation (#254), and uses the documented 40/2s per-user recent-history rate budget (#255). **Physical after-deploy Demo `account_summary COMPLETED/FRESH`: SWAP order history rows=1, managed_intents=1/matched=1/unresolved=0, no unknown orders/fills, coherent/reconciled; production restored read-only with 0 positions/orders, MCP PASS/FRESH generation 270.** This accepts *the exercised 4C.1 lifecycle and its durable history*, not account-wide pending-algo inventory, Stage 4C.2 fill or overall Stage 4C.
- **Demo #245 (historical):** exact installed product code passed authenticated Futures/long-short/permission/private-WS/clock/account-rate-budget `executor_preflight=accepted`; at 2026-10-09 ~09:35 UTC Demo equity was **0**, so no order was submitted. This historical blocker was **superseded by a fresh positive balance at 14:04 UTC**.
- **Account identity (operator-confirmed labels):** production/live OKX subaccount = `Succession`; distinct OKX Demo Trading account = `demoAccount08102026`. The two balances, credentials, permissions, environment flags, ledgers and order identities must **never** be conflated. Names here are operator-reported identification aids; native credential fingerprints + Demo environment binding must establish exact runtime identity before any Demo order.
- **2026-10-09 14:04 UTC — Demo account and equity VERIFIED:** Browser profile in Demo Trading showed `demoAccount08102026` (Sub-account) with 5,000 USDT and `OKX-Demo-Observer` Read / `OKX-Demo-Executor` Read+Trade. Fresh authenticated encrypted Demo `account_snapshot` (`req_stage4c_ui_uid_crosscheck_20261009a`) returned `COMPLETED/FRESH`, `account_uid_fingerprint` **matching SHA256 of browser UID**, **USDT available_balance=5000, frozen=0**, total equity approximately USD 102,598.65, Futures/long-short, observer read_only, 0 positions/orders. Fresh `executor_preflight` (`req_stage4c_ui_executor_preflight_20261009a`) returned **accepted=true**, executor same account, Read+Trade/no Withdraw, private WS/clock PASS, Demo-only rate fallback. The earlier zero API balance was historical; **why it transitioned to 5000 is unproven**. Production was restored by typed CONTROL terminal PASS; Cloudflare generation 257 PASS/FRESH and read-only production account reconciled with zero positions/orders. **No exchange mutation executed**. Details and request ids: #223.
- **Historical 4C.1 proposal (superseded):** the earlier zero-fill checklist and one-off Demo DATA transport blocker have been physically resolved as recorded in #223. Its old preflight evidence is historical only; never reuse a preflight after switching profiles. Stage 4C.2 requires an independent same-session Demo preflight and protected real-fill safety review.
- **2026-10-09 20:14–20:20 UTC — encrypted Demo DATA recovered and scientifically decoded (read-only):** Two new requests using the original X25519/HKDF-SHA256/ChaCha20-Poly1305 #234 mailbox published normally, received matching agent responses and passed local per-request AEAD authentication/decryption; see #223 [checkpoint comment 6088619251]. `account_snapshot` returned COMPLETED/DEGRADED on cold private-WS REST bootstrap with USDT 5000 available, Futures/long_short_mode and zero positions/orders; **not order-ready** until convergence. A second freshly scoped `executor_preflight` returned `okx.executor-preflight/v3`, `COMPLETED/FRESH`, **accepted=true**, matched Demo account fingerprint, executor Read+Trade/no Withdraw, private WS converged, account mode and exchange clock PASS, labelled Demo 1000/2s fallback. Each Demo test was explicitly paired with native CONTROL restore of production (terminal PASS), Cloudflare primary converged PASS/FRESH generation 264. **No exchange mutations/orders.** `DEMO_DATA_DELIVERY=PASS`, while `STAGE4C_T4=OPEN`; the accepted preflight cannot be reused after the profile switch. The earlier ChatGPT pre-GitHub refusal remains a historical intermittent platform error of unknown cause, not a reproduced failure of the exchange, controller or Rust mailbox code.
- **Independent research gate (post-window 2026-10-09, #216):** The frozen H2 `two_bar_momentum` 96x1H FINAL_OOS window has matured, but two scheduled primary-MCP scientific re-entry attempts stopped **before consuming it**: new VALIDATION evidence had **22 trades**, below the immutable pre-holdout `MIN_VALIDATION_TRADES_V1=30`, and robustness was not `ReadyForFinalOos`. Existing checkpoint/spec/robustness evidence is immutable; full artifact IDs need independent retrieval before stronger provenance claims. `FINAL_OOS=SEALED_UNCONSUMED`; no consumption intent, PromotionBundle, BACKTESTED decision or PAPER/SHADOW authority. **Do not** retune H2, change split length after inspecting results, reduce sample thresholds, open the sealed holdout or silently re-run the same acceptance loop. Mark this evaluated H2 lineage `NOT_PROMOTABLE / PRE_HOLDOUT_INSUFFICIENT_EVIDENCE`; any future candidate must have a separately frozen hypothesis/version, validation plan and fresh independent OOS. Stage-4 *Demo execution-mechanics acceptance* may proceed independently, but production authority and strategy promotion remain blocked.
- **Primary transport and quality:** direct Cloudflare MCP operational `PASS/connected/session_fresh`; its freshness **does not imply market readiness**. An observed BTC WebSocket cold subscription state `WS_SUBSCRIPTIONS_INCOMPLETE/DEGRADED` returned an explicitly labelled bounded REST fallback; subsequent same-instrument WS state recovered `FRESH`, 8 subscriptions and sequence-contiguous order book. Record convergence-time/fault-budget evidence if recurring; do not add a new poller/retry/market-state owner without a reproduced defect.
- **Trader/operator gaps (prioritized):** (P0) Stage-4C physical Demo venue evidence; (P0 independent) #216 scientific holdout; (P1) Stage-5 safety, exchange-native disconnect/protection policy, recovery, fill/bill/TCA lineage and operational budgets; (P1) keep proving supported **ChatGPT -> GitHub typed CONTROL -> controller -> terminal** resilience under connector safety limits. This session physically proved read-only `status`, Demo switch and production restore terminal PASS; one earlier generic comment write was platform-blocked, which is not a demonstrated controller defect. Do not create a second CONTROL transport/daemon/mutation MCP tool without a reproducible requirement and explicit architecture approval. (P2) measured multi-factor screening (`return + liquidity + spread`) using existing bounded query ownership, not generic new tools.
- **Context discipline:** Stage-4D #242 removed duplicate JSON serialization: typed `structuredContent` remains canonical; 72-character text fallback observed, and `account_summary` was approximately 4.5 kB structured / 4.6 kB total. Keep self-contained <=12,288-byte decision packets, typed source/artifact/generation IDs, measured tool-schema budgets, no raw historical dumps and no chat memory as an operational store.
- **Architecture invariants unchanged:** one OS supervisor/controller/Job-Object agent/Tokio runtime, existing observation/analysis/research/execution owners, Cloudflare primary read-only DATA, GitHub encrypted DATA parity and typed CONTROL, no generic shell/OKX RPC, no duplicate state or blind mutation replay, production live authority **disabled**. Changes require T1–T5 plus exact-tree CI/artifact/physical proofs where applicable.

Acceptance classification: **Stage 4C.1 non-fill + durable exchange-history parity ACCEPTED; Stage 4C.2, 4C.3, overall Stage 4C, Stage 3 scientific strategy promotion and Stage 5 remain OPEN.** No production live trading authority.

## Stage 4C large-slice closure contract — implementation and proof matrix (2026-10-10)

**Authority:** #160 remains the canonical product acceptance contract, #223 the venue execution evidence ledger. This is the reproducible implementation/test sequence, **not** a self-certified Stage 4C PASS. Slices #257 (protected terminal-close reservation), #258/#259 (four exact pending `SWAP/FUTURES × conditional/OCO` inventory with `pause` and complete-page detection), and #260 (exact-owned durable `cancel-algos`, currently candidate) share the sole existing Rust `okx-execution` owner. No additional transport, controller, worker, mutable policy owner or exchange order path.

**Sequence and prerequisites:** (A) finish and review the exact-owned protective cleanup candidate #260 with `PREPARED → SUBMITTING → ACK/UNKNOWN → independently CONFIRMED_ABSENT`; reject unknown-effect replay, mismatched owner, extra pending algos, incomplete inventory, non-flat account and non-Demo authority; T1/T2 negative tests first. (B) exact-head Linux/Windows CI all required jobs SUCCESS, release Windows artifact and `tested tree == merged tree`; typed CONTROL deploy with SHA256/provenance and read-only production/isolated Demo smoke. (C) a new single-owner Demo session: exact credential UID/environment/mode, Trade permission/no Withdraw, collateral, account/WS/clock/fees/reference/market freshness, risk/lot/tick/size policy, pending ordinary/covered-algo inventory, private WS synchronization. **Old preflight must never authorize a new Demo mutation.** (D) perform minimal, bounded explicitly versioned Demo scenarios only after both a working restore route and owned TP/SL cleanup are proved. (E) return to production read-only and collect the following terminal T1–T5 matrix. A failure at any gate is `NOT_ACCEPTED` with immutable proof IDs, not an excuse to remove checks.

| Requirement | Deterministic/in-process T1–T2 evidence | Physical Demo/T4 or negative/T5 acceptance |
| --- | --- | --- |
| Unprotected CLOSE AMEND | Exact client/mutation IDs; validate reduce-only size, tick, post-only/limit, state transitions, monotonic durable revisions; reject amend of protected risk-increasing OPEN | One live unprotected **risk-reducing CLOSE** amended once; exact ACK and independently reconciled final order details/fills; never expand risk |
| Hedge long/short and OPEN/ADD/REDUCE/REVERSE | Hedge-mode positionSide long/short rules, directional size bounds, reverse CLOSE→fresh OPEN lease; same-instrument contention, STP and no cross-intent over-allocation | Minimum supported Demo long→flat and short→flat round-trip; separately bounded ADD/REDUCE and REVERSE proof where actual venue support permits; reject unsupported mode with exact reason, no naked exposure |
| Exchange fills/bills/fees | Stub mixed maker/taker, partial-fill+cancel, identical `ordId/tradeId/billId` dedup and missing/external fills, maker/taker sign; no dependence on VIP fills WS | Exchange `accFillSz`/trade IDs, **fills + bills**, fee currency/rate, commissions and position delta cross-check durable ledger; final ordinary and algo pending zero |
| TCA and decision lineage | VWAP, arrival/decision reference price basis, implementation shortfall, markouts when observed, time basis/clock/gateway latency, funding, explicit incomplete/unknown fee and source errors; immutable strategy/mandate/risk/reference generation IDs | Reconcile actual Demo fill VWAP and fees with exchange ledger/bills to declared tolerance; return bounded `okx.execution-tca` proof with exact artifact/id and attribution, no synthetic numbers |
| Concurrency / negative tests | Duplicate logical intent across GitHub DATA/direct rejection, ACK lost, crash after durable SUBMITTING, restart with nonterminal, old/stale preflight, external manual balance/position change, race cancel-vs-fill, STP and unsupported fills channel | One controlled restart while Demo order is still live or pending then **read-only** exact-ID reconciliation, never blind resubmit; at least one fail-closed negative proof per risk boundary |
| Exact-owned TP/SL lifecycle | Four independent pending-list scope coverage; `pause` state, conflicting ownership and no-serial-retry, partial-fill→cancel coverage; deterministic cleanup transitions survive restart | Independently verify attached algoId/client ID/covered size against filled parent, exact one-owner cleanup, exchange no-algo pending after close and durable `CONFIRMED_ABSENT` |
| T3/T4/T5 final exit | all PR/job IDs, tested/merged tree equality, no extra owners/services, deterministic fixtures T1/T2, negative T5 matrix | One installed hash/provenance, Demo `FRESH` and account-flat, positions=0/ordinary pending=0/protective pending=0, unknown/unmanaged exchange identities=0, reconciliation PASS; production never left read-only, primary MCP PASS/FRESH after restore |

**Do not mistake** post-only zero-fill 4C.1 for a filled lifecycle; terminal order state for fully attributed fills; ordinary pending=0 for protective cleanup; a successful `cancel-algos` ACK for verified absence; empty/truncated history for no fills; or a production read-only status for Demo T4. Risk budget stays intentionally minimal despite operator-authorized unrestricted *number of Demo tests*. No force-filling larger notional amounts simply to manufacture partial-fill races.

**One Stage-4 terminal acceptance record:** include source tree, exact CI run/artifact binary SHA, 4C.1/4C.2/4C.3 T1–T5 evidence by name, authenticated Demo request/response IDs, exchange order/trade/bill/algo identities (bounded fingerprints), cost/TCA residuals, unknown/unmanaged counts, negative/reboot tests, production return, and classification `PASS | NOT_ACCEPTED`. Stage 3 scientific promotion, Stage 5 governance and production-live authorization remain separately gated; H2 FINAL_OOS remains sealed.

## Stage 4C physical acceptance and operator recovery — exact proof contract

**Status: Stage 4C.1 ACCEPTED / Stage 4C.2 & 4C.3 OPEN** (#223). One genuine Demo-only non-fill place/cancel was physically proved and its former history gap fixed; no Demo fill yet. This is an operational proof checklist, not an instruction to bypass ChatGPT connector controls. Legacy #223 §4C.1's `OPEN -> amend` is superseded by this and the 2026-10-09 accepted cursor above; protected risk-increasing OPEN is intentionally not amendable. No strategy promotion is required for synthetic/versioned Demo mechanics.

1. **Read-only preflight immediately before every mutation:** controller status confirms exact installed Windows agent SHA/provenance, one owned profile, no overlapping Demo/production runtime; fresh authenticated Demo UID fingerprint and demo=true environment, Read+Trade/no Withdraw executor credentials, Futures/long-short mode, available-margin, no unknown/unmanaged exposure, coherent private WS and account state, current reference/fees/clock/risk-policy version, supported instrument/tick/lot and bounded rate budget. A historical successful preflight is never reusable authority. If GitHub encrypted Demo DATA is blocked before GitHub, classify `OPERATOR_DELIVERY_BLOCKED`, restore/retain production read-only, and **do not submit or route via a new channel**.
2. **4C.1 zero-fill:** one minimal explicitly authorized Demo-only post-only protected `OPEN -> exchange exact-id reconcile -> CANCEL -> terminal reconcile`; require zero fill, zero position and both ordinary/algo orphan counts zero. If any fill occurs, stop the zero-fill claim and switch to bounded protected exposure reconciliation/controlled closing; never misclassify a fill as cancel success.
3. **4C.2 fill + amend (OPEN, gated):** before creating exposure, prove a reachable single-owner Demo CONTROL/DATA path, fresh bound Demo identity/balance/private-WS/clock/fees/risk gates, no other exposure, and a bounded independent source of **all relevant pending protective algo orders** plus safe exact-owned cancel/cleanup or a demonstrated exchange-native terminal cleanup. Existing `okx-execution` resolves attached algo by exact client ID; `account_summary` **now independently reports bounded four-scope pending conditional/OCO inventory** (#257–#259 physically accepted). Durable exact-owner cancel-algos cleanup remains a #260 candidate until CI/merged tree/deploy/Demo accepted; inventory alone does not prove cleanup. The OKX read-only `orders-algo-pending` and exact `cancel-algos` exist, but do not blindly expose a generic trade API; implement any missing bounded native contract in the existing API/execution owners only after a concrete testable requirement. Without recovery/cleanup readiness, **NO FILLED OPEN**. Then one minimum risk-capped Demo `OPEN -> exact fills/bills/fees -> risk-reducing CLOSE`; AMEND only on a non-protected CLOSE, reconcile every uncertain ACK, verify zero positions, ordinary and algo orders, ledger/orphan/TCA completeness. Never amend protected OPEN or blindly replay a mutation.
**2026-10-10 CONTROL delivery checkpoint:** single-owner production status `ctl_stage4c2_baseline_status_20261010a` terminal PASS (installed artifact/hash/tree exact, desired/running production, job object owned). Demo switch `ctl_stage4c2_start_demo_20261010a` was blocked twice by ChatGPT connector safety *before GitHub publication*; exact request-ID search and issue #12 tail confirmed no request comment, and production stayed running. Treat as `OPERATOR_CONTROL_DELIVERY_BLOCKED`, **not** proof of controller or OKX malfunction. Do not spin alternative shell transport, replace request IDs to bypass rejection, or place any order while switching/restore path is not available. Resume with supported typed CONTROL only when available and with fresh same-session checks.

4. **4C.3 resilience/lineage:** simulate or observe lost ACK, duplicate submit, partial-fill/cancel race; one nonterminal Demo execution crosses a controlled agent restart and converges through authoritative identity/order/fill/bill lookup, **never blind mutation replay**. Collect exact fee/fill VWAP/TCA, protective covered quantity/cleanup and app-vs-exchange reconciliation evidence. Partial fills need not be forced at venue when T1/T2 deterministic race tests exist.
5. **4C final acceptance:** exact source tree/CI/artifact SHA, all T1–T5 fixtures, real Demo venue order and fill evidence, clean managed + unmanaged ledger/venue accounting and no stray protective algo; verified production `Succession` stayed read-only, no mutations or unexpected exposure, primary Cloudflare MCP `PASS/FRESH`. The approved artifact may have historical log errors; controller `runtime_diagnostics.*.log_evidence.current_process_scoped=false` explicitly means `fatal_error` is **retained diagnostic history**, not proof of the current agent fault.

**Windows workspace hygiene before any local sync:** typed `workspace_status` is read-only except a Git fetch. The observed old `C:\okx` has `?? Cargo.lock`; current remote `main` tracks `Cargo.lock`. A clean fast-forward may overwrite an unknown local lockfile. Preserve its exact bytes + hash through an operator-approved bounded maintenance procedure **before** cleaning it. The existing `sync` intentionally requires an agent-stopped, clean workspace, so never force it against a running verified production agent; check controller/agent recovery and artifact provenance after bounded maintenance. Keep the installed production artifact independent of workspace HEAD. Do not read or move credentials, change secret ACLs, run a generic shell, or touch unrelated applications.

**Scientific distinction:** #216 H2 pre-holdout VALIDATION insufficiency is a separate research result. It never blocks Stage4C synthetic Demo mechanics nor authorizes PAPER/SHADOW/real-money trading. Keep both the historical negative evidence and the untouched H2 FINAL_OOS sealed.

---

## Code-complete track (product engineering independent of strategy profitability)

**Current objective:** achieve **CODE_COMPLETE as an engineering candidate** on the existing Rust/Tokio product, as soon as testable, without waiting for a profitable strategy, new 96-hour holdout or successful positive H2 promotion. The Stage-3 research *engine* is implemented; the H2 *candidate* is not promotable on the observed validation lineage. Do not fabricate scientific acceptance.

Four different statuses must never be conflated:

- `CODE_COMPLETE`: all approved product workflows and safety policies are implemented in accepted owners with T1/T2 deterministic and integration tests, exact-head CI, no missing internal executable capability. Can be evaluated without a promising strategy or OKX trading access.
- `VENUE_ACCEPTED`: separately require physical OKX Demo intent -> place -> reconcile -> cancel/fill/flat/protection/restart/TCA evidence on the **same** execution owner. It is not obtained by CI or old preflight.
- `STRATEGY_APPROVED`: one independently frozen hypothesis passes the locked scientific criteria and is authorized through positive BACKTESTED -> PAPER/SHADOW. This is a *strategy-instance authority*, not a prerequisite to compiling, testing or completing the product. Failed/insufficient candidates stay recorded and must not be rebranded as profitable.
- `LIVE_AUTHORIZED`: stages and safety accepted, separate least-privilege production Trade credential and **explicit** owner approval. Current production live authority remains disabled.

**Same-day code-work critical path, no clock-time guarantee and no architecture expansion:**

1. **Freeze scope / verify already accepted foundation.** Stage 1–2, Stage 3A–3D *research infrastructure*, Stage 4A/4B execution kernel, Stage 4D transport/context are retained. CI/source-tree/installed artifact are authoritative; do not reopen implemented features merely to add activity. H2 validation 22 < 30 is an independent scientific failure of evidence, not a missing Rust feature.
2. **Stage 5A: operator/governance gate** in existing `okx-analysis`/sole `okx-execution` owner. Reuse `HardRiskPolicy`, `CandidateRiskGate`, the existing durable ledger and mutation revalidation: fail-closed `STOP_NEW_RISK`, cancel-entry policy, preserving protected exits and controlled close, default-disabled production live. Test stale/unknown mode, daily loss/drawdown persistence/rollover, crash/restart and operator stops. Do **not** introduce a second policy/calculation/executor/daemon/transport or a generic `ALLOW_LIVE_TRADING` endpoint.
3. **Stage 5B: audit + attribution + review** using existing read-only account/fill/bill/TCA/research artifact identities. A bounded, versioned immutable scorecard and `KEEP | DEMOTE | RETIRE | NEW_VERSION` review with explicit insufficient-evidence/drift reasons; external/manual effects must be labelled unattributed. No auto-promotion or self-modifying strategy. Test synthetic/negative/no-trade inputs without requiring positive H2 edge.
4. **Stage 5C: operation/recovery/canary-readiness**. Review exchange-native Cancel All After (CAA) for protective-exit compatibility; implement only if a provably safe policy requires it, otherwise document a tested alternative. Reuse current Windows supervisor, Cloudflare primary, GitHub fallback and exact artifact provenance; T1/T2 fault tests, T3 CI, T4 read-only product/operability checks, T5 shutdown/recovery. No new scheduler, mutable DB, process, MCP mutation tool or raw-RPC escape hatch.
5. **Code-complete decision:** one explicit reviewed checklist maps each Stage-3/4/5 software deliverable to accepted code, tests and exact source tree. `CODE_COMPLETE=PASS` only with no unimplemented required software behavior. `BLOCKED_EXTERNAL` / `NOT_ACCEPTED` remain visible for real Demo venue tests, PAPER/SHADOW scientific promotion and production canary. An unfinished CI or still-missing kill/review kernel is **not** CODE_COMPLETE.

**Superseded code-only proposal:** do not pursue Stage 5A/5B/5C or another research task while Stage 4C-G1 remains open. No parallel engineering. Do not stall software delivery waiting for H3. Do not weaken or prematurely close the existing Stage 3 scientific or Stage 4C venue acceptance requirements.

**Trader/ChatGPT product check at each slice:** ordinary market/account/risk/research/execution inspection works through coarse read-only primary Cloudflare MCP packets; preserve ≤12,288-byte normal response and typed quality/provenance, supported tool schemas, operator-readable failed-gate reasons, and immutable ids across chats. No chat-context state owner or low-level MCP micro-tool proliferation.

---

## Historical Stage-1/2 closure and original Stage-3 cursor — evidence only

Canonical execution order inside Stage 1:

1. **P0.1 WebSocket 443 compatibility** — ACCEPTED.
2. **P0.2 exchange clock discipline** — ACCEPTED.
3. **Venue/instrument-state execution gate** — ACCEPTED.
4. **Account + ledger truth** — ACCEPTED.
5. **P0.3 named rate/backpressure domains** — ACCEPTED.
6. **Stage-1 final T1–T5 acceptance** — ACCEPTED.

The P0/P1 labels are capability groups, not a competing execution order.

Stage 1 final acceptance is **ACCEPTED/CLOSED** on the exact tested/deployed tree:
- final tested head: `ec4359d388b8a71fba90fe360b986974f1d4fe96`;
- current merged main at acceptance: `6b7519e158207943d1cedce92d302e3c2d7d3c42`;
- compare tested head -> merged main: 0 changed files;
- final source tree: `481a9f2b4a9e4cf97b921dbe55c7e8be1f32924b`;
- final Windows artifact: `11200168727` from CI run `36941520981`;
- exact artifact installed and provenance verified through CONTROL;
- Cloudflare-primary market/account/capability paths physically passed on the installed binary;
- independent encrypted GitHub DATA fallback parity passed on the same binary;
- exact-tree restart advanced direct transport generation and market/account state reconverged to FRESH.

Stage-1 acceptance scope is the authenticated production execution account and the factual surfaces available through its least-privilege observer credential. A separate Main-account Read-only credential now exists and authenticates, but it is intentionally not connected to the production runtime; full main+subaccounts treasury inventory remains a deferred capability and `multi_account_inventory_complete=false` stays explicit until that capability is deliberately integrated and accepted.

**Stage 2 — INTELLIGENCE + RISK: ACCEPTED/CLOSED. Historical next cursor at that time: Stage 3; current NEXT is 4C-G1 in the serial matrix above.**

Stage 2 progress:

- **bounded universal analytical query — ACCEPTED/CLOSED**: `query_capabilities` + `query`, bounded whole-derivatives scan, stable top/bottom-K, explicit missing/excluded/truncation/coherence/provenance;
- **live microstructure core — ACCEPTED**: spread/depth/impact and sequence-contiguous live evidence through the existing bounded public-WS owner;
- **derivatives + history intelligence — ACCEPTED**: recent trades, realized-volatility/volume/OI change, funding history/regime, basis/term-structure evidence, plus production fixes for live OKX wire/rate-domain behavior;
- **portfolio risk mandate/hard-policy primary T4 — ACCEPTED**: PR #184 is on main `fe67d3495479e72a56c20c08fc5c231ed0670225`; exact tested head `74e14e5b1ff09c4fe27b1e7579eee9e905c2d843`; CI `37073209534` PASS; artifact `11256197440` deployed; Worker contract `okx.mcp.tools/2026-10-03.1`. After refreshing the connected tool schema, primary Cloudflare allow + intentional reject cases both passed with FRESH coherent evidence, typed policy rejection, non-null BTC reference generation in the reject case, and zero-residual OKX account-position-risk oracle comparison. GitHub fallback was not used as a substitute.
- **portfolio-risk correctness — ACCEPTED**: end-of-evaluation generation/coherence admission, explicit source-skew/oracle semantics, fail-closed unsupported daily-loss currencies and explicit mandate field semantics are merged/deployed;
- **statistical portfolio risk — ACCEPTED**: #187 adds explicit-sample covariance/correlation, volatility contribution, deterministic scenario/historical stress; Expected Shortfall remains explicitly NOT_COMPUTED until a declared valid tail-sample contract exists;
- **pre-mutation hard-policy ownership — ACCEPTED for implementation/fail-closed boundary**: #188 merged as main `1e7b37a7705d1925d9979e4c20cb63fdff4ae903`; tested head `093f489b07b894ada932b0f393c6f9ae1f2bf583`; tested/merged tree `9eafeaca84c3080edfcae813901c9b7b38d4eaaa`; CI `37087545928` PASS; artifact `11261092800` deployed/restarted with provenance; primary runtime and `portfolio_risk/v3` regression remained PASS/FRESH. This does not claim a live production mutation.
- **account-mode-aware Futures virtual risk + Stage-2 final acceptance — ACCEPTED/CLOSED**: PR #197 tested head `efa6f8dec36c3abde0d9abc69f749d4b75c0b05c`, tested/merged tree `b63f98d57df6e9e1e1b6fb1c24c949d580717603`, CI #843 / `37160080767` PASS, Windows artifact `11287008643` deployed with persisted provenance. Primary Cloudflare `portfolio_risk/v6` physically passed on Futures mode with a non-zero BTC+ETH COUNTERFACTUAL portfolio, FRESH mark/reference evidence, exchange-published contract constraints, statistical evidence, independent observed-account oracle consistency and zero exchange mutation.

Canonical Stage-2 closure order — **DONE / ACCEPTED**:

1. **DONE** — primary `portfolio_risk` allow + intentional reject through connected Cloudflare MCP.
2. **DONE** — portfolio-risk correctness: generation/coherence/skew/oracle semantics and explicit mandate fields.
3. **DONE** — statistical portfolio risk with explicit samples/windows, covariance/correlation, volatility contribution and deterministic stress.
4. **DONE** — immutable mandate/hard-policy revalidation immediately before the existing mutation boundary; production live trading remains disabled.
5. **DONE** — account-mode-aware exchange-oracle correction:
   - authenticated `acctLv=2` Futures mode no longer requires Position Builder;
   - COUNTERFACTUAL multi-instrument risk remains deterministic in `okx-analysis` over FRESH OKX mark/reference facts and exchange-published contract/lot/min/max/leverage constraints;
   - the real observed account remains independently checked against `account-position-risk`;
   - Position Builder remains typed for `acctLv=3/4` only;
   - no account-mode change, extra account/subaccount, Demo subsystem, second observer, fallback calculator, new transport or new MCP tool was introduced.
6. **DONE** — final Stage-2 T1–T5 acceptance:
   - tested head `efa6f8dec36c3abde0d9abc69f749d4b75c0b05c`;
   - tested/merged tree `b63f98d57df6e9e1e1b6fb1c24c949d580717603`;
   - CI #843 / run `37160080767` all jobs PASS;
   - artifact `11287008643`, digest `sha256:52d3521259ab2109f9a0d9781002f59c30272315d4771476e0a7603fc6010da2`;
   - merged main `f1028e97f984e313f9c0b702010ebeca4518a088`, merge tree == tested tree;
   - CONTROL deploy PASS, installed agent SHA-256 `1bdba04deb1f9a93761f7521279341d32dfaca883046df49da378aa9ac2b02d6`;
   - primary runtime PASS/session_fresh generation 114;
   - primary `portfolio_risk/v6` non-zero Futures proof PASS; hard-policy and exchange-constraint negative cases PASS; post-check account remains FRESH/coherent with zero positions/orders and consistent reconciliation.

Structural delta for the corrective slice: **0** new crates, long-lived tasks, mutable state owners, stores, schedulers/poll loops, transports, mutation authorities, MCP tools, credential models and third-party dependencies.

Cross-cutting CONTROL reliability debt discovered during item 5 is **DONE / ACCEPTED** in #193:
- merged main `239232ad9ef6308893c86c4afa1fbcf9fc6b6690`;
- exact tested head `053cb0213f4367113dd32aada83e9704e0a9b1cd`, tested/merged tree `b16c3d56438b01402f09a051b8092a43a3d22145`;
- CI #832 / run `37125627129` PASS;
- controller artifact `11274982057`, controller SHA-256 `10784b32ba24dc65058ff3b7c7c03783b28c1ae6e3726b2c85eeecdfc1d0004f`;
- existing `okx-github` now owns bounded 10s connect / 60s whole-request timeouts plus typed Timeout/Decode classification; no new owner/task/transport was introduced;
- controller stage/handoff committed through the immutable launcher, and `acceptance_crash_controller` physically proved durable-terminal-before-exit, automatic Scheduler/launcher recovery, no replay and post-recovery PASS/FRESH runtime;
- post-recovery production account remained FRESH/coherent with 0 positions, 0 pending orders and consistent ledger reconciliation.

Historical operational baseline (superseded; source/date retained only for provenance):
- canonical main `b2688e1c2186e83b9c47cf513668e53a034fe891`;
- Worker contract `okx.mcp.tools/2026-10-05.9`;
- research catalog `okx.research.catalog/2026-10-05.8`;
- direct runtime PASS / connected / session_fresh; connection generation is telemetry rather than roadmap identity;
- Stage 1 and Stage 2 PASS/CLOSED;
- Stage 3A/3B/3C infrastructure PASS/CLOSED;
- H1 `close_momentum` REJECTED;
- H2 `two_bar_momentum` development screen PASS and frozen pending fresh independent FINAL_OOS in #216;
- Stage-3D immutable promotion gate ACCEPTED in #217;
- Stage-3D causal live-decision parity kernel ACCEPTED in #218;
- production live trading remains disabled.

**Historical Stage-3D implementation cursor (superseded by CURRENT 4C-G1).** The admitted delta is exactly one agent-root task, one single-writer session owner and one bounded command/wakeup channel over the existing public observation owner; no second market connection, timer/poller or mutation authority. Stage-3 final acceptance still requires a future BACKTESTED candidate plus real PAPER/SHADOW evidence; Stage 4 remains gated behind that exit.

## Repository Guard v1

The guard exists to preserve the accepted architecture while keeping development fast.

Machine-enforced:
- protected `main` keeps the required contexts;
- CI validates architecture dependency direction, portable Rust and Cloudflare MCP;
- `Cargo.lock` + `--locked` remain mandatory;
- the Cloudflare package graph is locked and CI/deploy uses the committed lock;
- exact tested source tree and Windows artifact SHA-256 provenance remain mandatory.

Review-enforced:
- every nontrivial change names one concrete need/failure;
- an existing authoritative owner is preferred;
- structural complexity delta defaults to zero;
- every non-zero crate/task/store/transport/dependency/tool delta is justified;
- superseded paths are removed rather than retained indefinitely;
- tests name the product/architecture failure they protect.

The executable dependency DAG lives in `scripts/check_architecture.py`. The current ownership model remains canonical in `docs/ARCHITECTURE.md`.

## Cross-stage evidence rules

Professional outputs are decision-grade evidence, not raw API dumps.

Required principles:
- CONTROL `request_id` values are immutable single-publication identities: before any issue #12 POST, exact-search the candidate id in comments and abort if #12 already contains the request or terminal; controller deduplication remains defense in depth, not a retry mechanism;
- multi-source facts carry explicit `as_of`, provenance/generation and coherence/skew diagnostics;
- older/out-of-order REST evidence cannot regress newer accepted state;
- evidence distinguishes **OBSERVED**, **MODELLED** and **COUNTERFACTUAL** values;
- replay uses point-in-time and availability-time semantics;
- analytical outputs carry assumptions, uncertainty/sensitivity where meaningful, quality and invalidation conditions;
- exchange risk/max-size/precheck surfaces may validate local calculations but never replace deterministic local ownership;
- manual/external/exchange-system actions remain separately attributed unless managed lineage exists;
- one logical mutation identity is transport-independent across Cloudflare primary and GitHub fallback.

## Universal question coverage rule

Natural-language questions are **not** backend/API methods. The platform must not add one Rust operation or one MCP tool for every wording such as “top gainers”, “highest volume”, “largest funding”, “compare these markets”, or “which contracts changed most”.

The reusable read path is a **bounded typed analytical plan** executed by the existing Rust owners:

```text
user question
 -> ChatGPT semantic planning
 -> versioned bounded typed query plan
 -> existing observation/runtime facts
 -> allowlisted deterministic analysis operators
 -> bounded evidence result
 -> ChatGPT interpretation/explanation
```

The typed plan may compose only explicit product primitives:
- universe selection: instrument type, settlement currency, lifecycle/state and other normalized factual selectors;
- factual projection: allowlisted normalized market/reference/account fields;
- deterministic operators: filter, stable sort, top/bottom-K, bounded grouping/aggregation and explicitly supported time-window comparisons;
- versioned derived metrics owned by `okx-analysis`;
- explicit `as_of`, freshness/coherence policy, result limit and response budget.

This is **not** SQL, an arbitrary expression language, a raw OKX endpoint proxy or executable code. Every field/operator/metric is an enum/versioned contract with deterministic validation and hard bounds.

Architecture rule:
- a **new wording or combination** of already-supported facts/operators requires no backend change;
- a **new factual source** is added once to the factual layer;
- a **new calculation/metric** is added once to `okx-analysis` with deterministic tests;
- a **new mutation/safety semantic** remains an explicit dedicated capability and never enters the generic read planner.

The external MCP surface stays deliberately small. Prefer one stable universal read contract rather than metric-specific tools:
- `query_capabilities` returns the current query-contract/catalog version, supported field IDs, metric IDs + versions/units, operators and hard limits;
- `query` accepts one bounded analytical plan that declares the catalog version it was built against and fails closed on an unknown/stale capability or unsupported primitive.

This keeps MCP tool discovery stable as the metric catalog grows: adding a metric does not require adding a new MCP method. Existing coarse tools may remain as convenience/compatibility recipes, but should compile onto the same factual/analysis owners rather than duplicate formulas.

The first broad plan variant after Stage 1 should be a bounded market-universe scan (for example top/bottom-N by supported price change, volume, funding, basis or another accepted metric), rather than separate `top_gainers`, `top_losers`, `top_volume`, etc. tools. Coarse mutation/risk-policy/scenario capabilities remain dedicated where their contracts carry materially different authority or safety semantics.

Acceptance for a general analytical query capability must prove:
- whole-universe selection is bounded and rate-budget aware;
- stable deterministic ranking/tie behavior;
- no silent missing instruments or unreported truncation;
- freshness/provenance/metric version is present;
- result cardinality and payload size are bounded;
- unsupported fields/operators fail closed;
- no second collector, cache owner, scheduler, database or formula owner is introduced.

## Five development stages

### Stage 1 — TRUTH

Compatibility, clock/rate correctness, venue/reference safety, coherent account hierarchy, balances, positions, orders, fills, bills, PnL/funding/fees/interest and reconciliation.

Important proof includes:
- WS 443 and reconnect;
- exchange clock gate;
- venue/instrument/reference pre-mutation gate;
- account/ledger history coverage and no double counting;
- named rate domains/backpressure;
- direct Cloudflare MCP plus independent GitHub fallback.

Exit: factual market/account/reference truth for the authenticated production execution account is coherent, fresh/provenanced and black-box accepted. Treasury-wide main+subaccounts aggregation remains a separate external/deferred capability until a master read credential exists.

### Stage 2 — INTELLIGENCE + RISK

Market microstructure, derivatives structure, portfolio exposures, stress/scenario evidence, a versioned trading/research mandate and deterministic hard risk policy.

Important proof includes:
- decision-grade evidence with explicit coherence;
- one bounded typed market-universe scan that can answer cross-universe ranking/filtering questions without endpoint-per-question growth;
- depth/spread/impact/basis/carry;
- exact sizing/margin/risk boundaries;
- exchange-oracle differential checks against supported account-position-risk / max-size / position-builder evidence;
- end-of-evaluation reference-generation and bounded multi-source skew admission;
- hard daily-loss policy never silently disappears when currency conversion evidence is unavailable;
- mandate fields are either active constraints/derived evidence or explicitly context-only;
- the same hard policy is revalidated on fresh accepted evidence immediately before future mutation inside the existing execution owner;
- representative non-zero risk/oracle proof without enabling production live trading;
- no duplicate venue/reference/risk owner.

Exit: professional decision evidence is calculated locally in Rust, the same hard policy is enforceable at the mutation boundary, and ChatGPT does not become the financial calculator or safety authority.

### Stage 3 — SCIENTIFIC RESEARCH + REPLAY

Current development cursor. Stage 3 turns accepted Stage-1/2 truth/risk into a reproducible research system without creating a second platform.

Core architecture:
- one admitted new domain crate at most: `okx-research`;
- `okx-observation` remains the current factual owner;
- `okx-analysis` remains the deterministic feature/statistics/risk/strategy-formula owner;
- `okx-research` may own immutable datasets, replay, experiment lineage, validation and promotion artifacts only;
- no second Tokio runtime, daemon, scheduler/poll loop, operational database, transport or exchange mutation authority;
- no research-only duplicate implementation of a strategy intended for paper/live;
- a local content-addressed immutable artifact repository is permitted and is not a mutable live-status database.

Historical-data rule:
- prefer official OKX historical data/query sources where they provide the required facts;
- Tier A = candles/funding + explicit modelled execution costs;
- Tier B = provenance-locked trades/order-book/reference events;
- never claim historical L2 execution from Tier A;
- unavailable required event/reference history => `INSUFFICIENT_DATA`;
- historical replay must carry point-in-time universe/reference facts and availability-time semantics; the current `ReferenceRegistry` must not be used as a shortcut for historical eligibility/rules.

ChatGPT/product rule:
- ChatGPT formulates hypotheses and interprets compact evidence; Rust owns data/replay/statistics/risk;
- raw candles/trades/L2 do not traverse MCP for chat-side calculation;
- preserve current direct-transport limits (64 KiB frames, 20 s response deadline, existing bounded inflight);
- normal research summary target <= 12,288 bytes;
- long acquisition/replay uses deterministic bounded steps + immutable continuation/artifact ids, not a background job queue/poller;
- a fresh chat must be able to inspect/resume by experiment/artifact id;
- external research surface should remain at most `research_capabilities` + `research` unless a concrete typed-authority boundary proves another tool necessary.

Final pre-implementation invariants:
- causal timing is explicit: event -> availability -> decision -> earliest execution;
- Tier-A intrabar ambiguity is conservative/`AMBIGUOUS_FILL`/Tier-B, never guessed favorably;
- signal/mark/index/execution prices are distinct roles;
- funding is applied as historical events; fee/cost/capacity assumptions carry provenance;
- every human/ChatGPT/sweep trial belongs to immutable research-family lineage;
- final OOS is sealed before opening; descendants cannot reuse consumed holdout as pristine;
- sample adequacy can produce `INSUFFICIENT_EVIDENCE`;
- null/simple baselines prove replay accounting before alpha claims;
- dataset identity records canonical raw + normalized hashes;
- immutable pinned EvidenceStore is separate from quota-bounded evictable SourceCache;
- archive ingestion is bounded/untrusted/allowlisted;
- MCP freshness is separate from research-input freshness; ChatGPT/Cloudflare is never in local paper/shadow decision latency;
- PAPER = live facts + simulated execution; SHADOW = production-intended decision/risk path stopping at `WOULD_SUBMIT`;
- positive promotion to PAPER/SHADOW requires explicit operator authorization.

Canonical Stage-3 execution order — exactly four large slices:

1. **3A Research Data Foundation**
   - **v1 scope:** USDT linear perpetuals; BTC/ETH/DOGE; Tier-A 1H candles + funding + point-in-time reference, mark/index history only where required; one bounded BTC Tier-B source-schema proof;
   - explicitly not all OKX instruments/cadences/L2 in v1;
   - unknown historical reference coverage -> `INSUFFICIENT_REFERENCE_HISTORY`;
   - official-source historical acquisition/chunking;
   - immutable dataset/chunk hashes, gap/duplicate/out-of-order evidence;
   - Tier A/Tier B classification;
   - event-time + availability-time semantics;
   - point-in-time universe/reference timeline;
   - deterministic bounded resume/checkpoint semantics;
   - raw + normalized canonical hashes, pinned-vs-cache storage quotas and hardened archive ingestion.
   - **T1:** parser/raw+normalized canonical hash/gap/current-reference-poison/chunk-boundary/storage-quota/hostile-archive fixtures.
   - **T2:** identical source -> identical dataset hash; restart/resume equivalence; no second live owner; existing rate/backpressure preserved.
   - **T3:** exact build provenance + dataset binding to parser/normalization/source versions.
   - **T4:** primary MCP acquires/inspects one bounded real Tier-A dataset and, where available, one bounded Tier-B sample without bulk payload.
   - **T5:** corrupt hash/gap/unsupported source/stale transport fail closed; resume remains idempotent.
   - Exit: the application can prove exactly what historical data exists, provenance, availability and gaps.

2. **3B Deterministic Replay Kernel**
   - one `okx-research` replay owner;
   - versioned hypothesis + experiment spec;
   - deterministic event ordering/no-lookahead plus explicit event/availability/decision/earliest-fill causality;
   - typed signal/mark/index/execution price roles and conservative Tier-A intrabar ambiguity;
   - event-based funding and versioned fee/funding/spread/slippage/capacity model with provenance;
   - same `okx-analysis` features/strategy/risk semantics intended for shadow/live;
   - baseline experiment/profile construction (declared assumptions, cost model values, mandate/policy values) is owned by `okx-research`, not `okx-agent`; `okx-analysis` remains the owner of policy/formula semantics;
   - `okx-agent` remains composition-only: validate/load/invoke/project;
   - deterministic experiment hash/result.
   - **T1:** feature golden vectors, poisoned-future/same-close isolation, ordering, intrabar ambiguity, price-role, variable-funding, fee provenance, quantization/cost/capacity monotonicity and repeatability.
   - **T2:** captured-live replay parity where semantics match; no out-of-split reads; restart/resume same terminal hash; Stage-2 risk remains authoritative.
   - **T3:** experiment binds exact source/dataset/algorithm/mandate/risk/cost versions; dataset acquisition provenance and replay-algorithm provenance are distinct (`dataset_source_tree` vs `replay_source_tree`), and the replay source tree participates in immutable experiment identity.
   - **T4:** primary MCP proves `NO_TRADE` + one manually auditable baseline before a real Tier-A experiment; Tier-B claims require inspected compatible Tier-B source semantics; the normal replay response is a self-contained <=12,288-byte decision packet and returns no bulk events/traces.
   - **T5:** future poison/gaps/unsupported Tier-B return typed failure or `INSUFFICIENT_DATA`; exchange mutations = 0.
   - Exit: one strategy can be replayed reproducibly without hidden future data or duplicate production formulas.

3. **3C Scientific Validation & Promotion**
   - chronological train/validation/untouched final OOS;
   - walk-forward;
   - purge/embargo only where overlapping horizons require it;
   - physically sealed/consumed final-holdout lineage;
   - immutable research-family lineage for human/ChatGPT/sweep trials;
   - sample/evidence adequacy;
   - cost/capacity and applicable parameter sensitivity plus regime breakdown;
   - immutable negative-trial retention;
   - DSR only where its Sharpe-like selection assumptions and sample/trial inputs are satisfied; PBO/CSCV only where a genuine comparable candidate family and valid block structure exist; unsupported diagnostics are `NOT_APPLICABLE`;
   - immutable promotion + demotion/invalidation criteria and `PromotionBundle`.

   **3C v1 implementation contract (preflight 2026-10-04):**
   - 3C adds **no new crate, long-lived task, state owner, DB, scheduler/poll loop, transport, mutation authority or MCP tool**. Default dependency delta is zero.
   - Current Stage-3A primary acquisition is latest-page bounded (<=100 1H candles), which is adequate for 3A/3B proof but not scientific train/validation/OOS. 3C first wires bounded exact-range/multi-page acquisition through the **existing** cursor-capable OKX history adapters and the existing immutable `ResearchCheckpoint` chain; it must not create a second collector/job engine.
   - A frozen `ValidationSpec` binds the parent dataset, exact chronological split/fold boundaries, applicable purge/embargo, evidence-adequacy policy, diagnostics/applicability rules, cost/capacity sensitivity grid, regime definitions and promotion/invalidation criteria **before** final holdout consumption. This artifact is the holdout seal; no separate mutable holdout service exists.
   - Validation derives immutable time-slice replay artifacts from the parent dataset and reuses the accepted Stage-3B replay kernel unchanged. Split/OOS semantics must not create a second replay implementation or duplicate strategy/risk/accounting formulas.
   - `ResearchFamily` is an immutable/versioned manifest over already executed trial artifacts (including negative/failed human, ChatGPT and bounded sweep variants). 3C v1 does **not** add an autonomous optimizer, sweep scheduler or background search engine.
   - `okx-analysis` owns pure validation/statistical math and strategy research metadata (lookback/forward holding horizon/applicable parameter surface); `okx-research` owns family/split/holdout/validation/promotion orchestration; `okx-agent` remains load/invoke/project only; Cloudflare remains validation/bounds/thin mapping only.
   - Statistical outputs that participate in artifact identity use explicit algorithm versions and canonical bounded precision with cross-platform golden vectors. Do not add a statistics dependency unless a reproduced formula/portability gap proves it necessary.
   - Promotion decision is exactly `REJECT | BACKTESTED | INSUFFICIENT_DATA | INSUFFICIENT_EVIDENCE`. `NOT_APPLICABLE` belongs to individual diagnostics, not the terminal promotion state.
   - Real T4 is not required to manufacture a `BACKTESTED` result. If the real family/sample is inadequate, `INSUFFICIENT_EVIDENCE` is the scientifically correct PASS behavior.
   - Normal MCP output remains one self-contained decision packet <=12,288 bytes; folds/trials/raw series stay in immutable local evidence and are referenced by ids.

   - **T1:** exact split-boundary/future-poison fixtures; derived-slice identity; walk-forward ordering; strategy-metadata-driven purge/embargo; sealed-holdout and descendant `POST_SELECTION` rules; family all-trials/negative-trials lineage; sample-adequacy verdicts; cost monotonicity; parameter-sensitivity `NOT_APPLICABLE` for a parameterless strategy; DSR reference vectors and PBO/CSCV small-matrix reference vectors only under applicable assumptions; deliberately overfit synthetic family cannot promote; all deterministic outputs match on Linux/Windows golden fixtures.
   - **T2:** bounded multi-page acquisition -> immutable checkpoint -> restart/resume -> identical terminal parent dataset; same immutable family/spec -> identical validation decision; negative trials remain queryable; lineage cannot be rewritten; validation work does not create a second market/reference owner or starve existing runtime/control paths.
   - **T3:** `ValidationSpec`, family/result and `PromotionBundle` bind exact parent/slice artifact ids, source trees, strategy/analysis/replay algorithm versions, mandate/risk/cost versions, statistical algorithm versions, criteria and holdout-consumption lineage.
   - **T4:** primary MCP performs bounded continuation to prepare/inspect one sufficiently long real Tier-A parent dataset, evaluates one real Stage-3 family through the existing `research` tool, and returns one compact `REJECT | BACKTESTED | INSUFFICIENT_DATA | INSUFFICIENT_EVIDENCE` decision with OOS/walk-forward/cost/applicable anti-overfit evidence; no bulk folds/traces.
   - **T5:** future-partition poison; deliberately overfit family; missing/corrupt parent/slice/trial artifacts; reused final holdout after candidate change; missing **applicable** anti-overfit evidence; invalid family rewrite; and any attempt to reach exchange mutation all fail closed.
   - Exit: `BACKTESTED` requires a frozen reproducible validation spec and sufficient predeclared scientific evidence; otherwise the typed non-promotion state is preserved.

4. **3D Paper/Shadow + Product UX + final Stage-3 acceptance**
   - states `RESEARCH -> BACKTESTED -> PAPER -> SHADOW`, with explicit operator authorization for positive promotion;
   - PAPER = live facts + simulated execution/virtual PnL; SHADOW = production-intended candidate/risk/pre-execution path ending at `WOULD_SUBMIT`;
   - one bounded event-driven research-session component inside the existing agent/runtime chain;
   - consumes existing accepted live observation; no second WS collector/full-market subscription/polling loop;
   - same feature/strategy/risk functions as replay;
   - append-only/content-addressed shadow evidence + compact status;
   - bounded/resumable MCP research UX, safe across transport loss and independent of chat history;
   - local paper/shadow remains outside MCP/ChatGPT latency and distinguishes transport freshness from research-input freshness.
   - **T1:** state transitions, replay/live-decision parity, <=12,288-byte normal summary, continuation idempotency.
   - **T2:** live observation feeds shadow without starving heartbeat/control/reconciliation; restart restores lineage; no exchange mutation path.
   - **T3:** exact Windows artifact/deploy provenance when agent changes; artifacts bind exact source/strategy/dataset versions.
   - **T4:** primary Cloudflare capabilities/run/resume/inspect-by-id + one real paper/shadow session; GitHub DATA parity only, never primary substitute.
   - **T5:** SESSION_STALE/reconnect, lost response, payload/time budget overrun, absent event history and corrupt artifact all fail/resume safely; production account unchanged.
   - Exit: one strategy lineage is traceable from falsifiable hypothesis through immutable data/replay/OOS/anti-overfit evidence to live paper/shadow with zero exchange mutation.

Stage-3 final acceptance must emit one bounded `okx.stage-acceptance/v1` over one exact accepted tree and include a simplicity review proving:
- <=1 new research crate;
- no new runtime/daemon/scheduler/operational DB/transport/mutation authority;
- no duplicate market/reference/formula/risk owner;
- direct transport limits were not enlarged;
- compact MCP evidence remains the chat boundary;
- production live trading remains disabled.

Exit: strategies are scientifically testable/reproducible and can progress through BACKTESTED/PAPER/SHADOW without live money or chat-history dependence.

**Stage 3A acceptance — PASS/CLOSED (2026-10-04).**

Accepted evidence:
- implementation PR #201; tested head `ab4ace3f3c46fdb414625660346587ae852fe912`;
- accepted source tree `5d9a73e385022e10792d9fa75b1c5b70f6aecdb6`, identical in merged main `2c248b945494e6012e22b3dd2b313fd87dfab3d5`;
- PR CI run `37206445119`: all required Linux, Windows, architecture and Cloudflare MCP jobs PASS;
- exact Windows bundle artifact `11305160431` installed by the existing controller; provenance persisted; agent restart PASS;
- production Cloudflare Worker deploy from merged main PASS; runtime remained PASS/connected/session-fresh on tool contract `okx.mcp.tools/2026-10-04.1`;
- primary Cloudflare MCP black-box `research_capabilities` PASS/FRESH and bound to the accepted source tree;
- primary Tier-A BTC/ETH/DOGE 1H inspections completed with immutable candle/funding/reference artifacts, zero explicit candle gaps, and the expected fail-closed `INSUFFICIENT_REFERENCE_HISTORY` verdict because a current instrument snapshot is not silently treated as historical point-in-time truth;
- primary Tier-B BTC historical-trades probe PASS/FRESH with 20 observed events, exact raw and normalized hashes, separate capture/chunk identities, explicit availability/continuity semantics, and no bulk rows returned through MCP;
- invalid Tier-B shape was rejected by the Worker and an out-of-range `trade_limit=101` was rejected by the connector schema;
- post-acceptance runtime remained PASS/FRESH; account remained coherent/reconciled with zero open positions and zero pending orders; Stage 3A has no exchange mutation authority.

Stage 3A exit is satisfied because the application can now prove what admitted historical data exists, its source/provenance/availability semantics, and where historical reference coverage is insufficient without inventing missing truth.

### Stage 3B acceptance — 2026-10-04

Stage 3B — Deterministic Replay Kernel is **ACCEPTED/CLOSED**.

Accepted evidence:
- PR #203 squash-merged the deterministic replay kernel; exact-head CI #925 passed architecture, Cloudflare typecheck, Linux fmt/clippy/tests and Windows native clippy/tests/release artifact build;
- the accepted Windows agent artifact was deployed through CONTROL with persisted provenance and restarted successfully; runtime advertised Stage `3B_V1` and the exact accepted replay source tree;
- primary Cloudflare MCP produced a bounded BTC Tier-A replay artifact `sha256:578b3cb3918416a8374b1cfc57f1df163851d7e87c8f9ca0e30c9b9971c4c3a0` while preserving the typed `INSUFFICIENT_REFERENCE_HISTORY` blocker instead of inventing point-in-time reference truth;
- primary `NO_TRADE` replay completed with 23 candles, 22 decisions, zero trades, zero gross/net/cost/funding, no bulk events and no exchange mutation authority;
- primary `close_momentum` replay completed on the same immutable artifact with explicit `COUNTERFACTUAL_MECHANICS` evidence, 20 trades, gross PnL `-0.05931`, trading cost `0.170007765`, funding cost `-0.000526213889483853697`, and net PnL `-0.228791551110516146303`; accounting reconciled exactly as gross minus trading cost minus funding cost;
- the compact replay decision packet was about 2.5 kB, well below the 12,288-byte normal-result budget, and returned no replay trace/bulk events;
- repeated replay before restart produced the same hypothesis/spec/experiment/result artifact identities and numerical results;
- after a controlled agent restart, direct-transport generation advanced from 138 to 139 and replaying the same artifact again produced the same immutable identities and numerical results;
- historical-observed mechanics on insufficient point-in-time reference coverage failed closed as `INSUFFICIENT_REFERENCE_HISTORY` with zero decisions/trades;
- a syntactically valid but missing replay artifact failed typed/non-retryable as `RESEARCH_ARTIFACT_FAILURE`;
- Worker boundary/version mismatches discovered during acceptance were fixed narrowly in PRs #204/#205; Cloudflare deploys #21/#22 passed typecheck/deploy/health, and the live contract is `okx.mcp.tools/2026-10-04.3` with research catalog `okx.research.catalog/2026-10-04.2`.

Stage 3B exit is satisfied: one strategy is replayed reproducibly without hidden future data, duplicate production formulas, bulk MCP traces or exchange mutation authority.

Stage 3B exit remains satisfied. Stage 3C scientific-validation infrastructure has since been accepted, including sealed FINAL_OOS consumption and immutable promotion evidence. Stage 3D has also accepted the immutable positive-promotion gate and causal live-decision parity kernel. The current implementation cursor is the bounded event-driven PAPER/SHADOW session owner; H2 final scientific acceptance is deferred to #216 until the fresh independent holdout exists.

The Stage-3 design freeze remains in force. Stage 3D must build on the accepted 3A/3B/3C owners and artifacts; new framework layers or methods still require a reproduced source/runtime/test failure, not speculative completeness.

### Stage 4 — EXECUTION + TCA

Complete order/position lifecycle, single mutation authority, uncertain-submit recovery, partial fills, amend/cancel/protective orders, intent arbitration and execution-quality attribution.

Important proof includes:
- idempotent client identity and transport-independent duplicate protection;
- reduce-only/hedge/reverse/STP semantics;
- deterministic arbitration for simultaneous same-instrument managed intents;
- manual/external changes force fresh reconciliation;
- pre-trade exchange/risk/reference revalidation;
- Demo Trading place/observe/amend/cancel/close;
- TCA with fills/bills, latency and modelled-vs-realized cost residuals.

Exit: mutation mechanics are physically accepted in OKX Demo Trading while production live trading remains disabled.

### Stage 5 — GOVERNANCE + OPERATIONS + LEARNING

Hard policy/kill behavior, attribution/scorecards, closed learning loop, operational budgets, recovery, security, release provenance and compatibility review.

The learning loop is:

```text
falsifiable hypothesis
 -> reproducible experiment
 -> promotion bundle
 -> paper/shadow observation
 -> controlled execution
 -> reconciled outcome + TCA
 -> attribution / live-vs-research residuals
 -> review
 -> KEEP | DEMOTE | RETIRE | NEW_VERSION
 -> next hypothesis/version
```

Important proof includes:
- no strategy self-promotion or self-modification;
- immutable review evidence;
- manual/external activity excluded from managed-strategy attribution unless lineage exists;
- explicit freshness/coherence/query/recovery/reconciliation budgets;
- durable execution/audit/research-artifact integrity and restore/reconcile path;
- Cloudflare-primary/GitHub-fallback independence;
- exchange-native disconnect protection such as Cancel All After is Demo-proven safe for the chosen protective-order topology or explicitly rejected with accepted alternative safety evidence;
- relevant OKX API compatibility/deprecation changes are reviewed at release/stage acceptance;
- source -> CI -> artifact -> installed binary provenance and supply-chain evidence.

Exit: the platform is industrial-ready with production live trading still disabled, and research/trading forms a closed auditable evidence loop.

## Stage acceptance contract

Every stage requires one bounded `okx.stage-acceptance/v1` record over one traceable accepted source tree.

Required proof layers:
- **T1** deterministic code/formula/parser/property proof;
- **T2** runtime/in-process ownership, generation, coherence and recovery proof;
- **T3** exact deployable artifact/provenance proof when runtime code changes;
- **T4** supported black-box product proof;
- **T5** negative/fault/recovery proof.

Primary/fallback black-box invariant, scoped by **authorized operation class**:
- For normal **read-only product and research capabilities**, **Cloudflare MCP is the mandatory primary T4 path** and must be exercised first. A healthy `runtime_status` or newer Worker contract alone does not prove a specific capability: that operation must actually be callable.
- **The narrow Stage-4C physical Demo exchange-mutation exception** uses the previously accepted encrypted GitHub DATA #234 path and **the same Rust executor** because the current primary Cloudflare MCP intentionally rejects mutation requests. This venue safety acceptance must have authenticated decrypted same-request results and independent exchange/ledger reconciliation, not just ciphertext envelopes; it is **not** Cloudflare read-only product acceptance and does not authorize a Demo MCP mutation tool or any production live write.
- GitHub DATA remains fallback/parity **for capabilities normally exposed through primary MCP**. Never substitute GitHub DATA for a missing/stale primary **read-only** capability, invent a second transport or evade a connector refusal.
- If a **read-only** Cloudflare tool is missing despite a newer Worker contract, T4 for that specific primary capability remains OPEN. Refresh `okx-cloudflare-mcp` tools (Обновить инструменты / Update tools) and re-exercise the direct operation before accepting it; do not treat Demo exchange proof as a substitute.
- Final Stage 4C terminal T4/T5 requires independent Demo exchange proof **and** successful read-only production restoration through the primary Cloudflare MCP; these are separate acceptance scopes.

A stage is PASS only when every deliverable maps to explicit evidence and no unresolved internal correctness/safety gap remains. `BLOCKED_EXTERNAL` is allowed only for a specifically named unavailable credential/venue capability/dataset and is not PASS.

## Final live activation gate

Only after Stages 1–5 PASS:
- explicit operator decision;
- separate least-privilege production Trade credential with `Withdraw=false`;
- credential authority/network/IP-binding posture explicitly accepted;
- live master gate explicitly enabled;
- all fresh clock/venue/reference/account/risk/governance gates accepted;
- tiny bounded canary on one allowed instrument/strategy version;
- successful open -> observe/reconcile -> close -> fills/bills/TCA/attribution cycle;
- immediate disable-new-risk path.

The canary proves production plumbing/safety, not statistical profitability. It cannot auto-promote a strategy or auto-increase capital. Any capital ramp remains a governed learning-loop decision.

No CI or test may silently cross the live gate.

## Accepted foundation retained

Do not reopen without concrete evidence:
- one Tokio runtime / one native agent owner chain;
- reference/public/private observation;
- deterministic analysis boundary;
- durable execution ledger and fail-closed pre-enable boundary;
- verified Windows lifecycle/artifact deployment;
- direct Cloudflare MCP primary transport;
- GitHub DATA/CONTROL fallback/recovery transport;
- bounded public-market working set and responsive direct transport.

## Non-goals

- no second runtime/daemon/watchdog/scheduler merely for a feature;
- no generic shell or arbitrary OKX RPC;
- no generic strategy/rules framework before a real boundary requires it;
- no database/service merely for governance or experiments;
- no microservice/Kubernetes split;
- no duplicate collector/formula/state authority;
- no permanent macro/news/on-chain/alternative-data ingestion unless a falsifiable hypothesis requires it and point-in-time provenance exists;
- no speculative SBE/multi-exchange migration without measured need;
- no live production trading before the explicit final gate.
