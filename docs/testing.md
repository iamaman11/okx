# Trade acceptance evidence map — #449 r9.1 (A0, active A1)

**Canonical:** [product #448](https://github.com/iamaman11/okx/issues/448), [execution #449 r9.1](https://github.com/iamaman11/okx/issues/449). This is an evidence map, **not** a new suite, an authorization to trade, or a PASS certificate.

**Current A1 working checkout:** `work/t10-07-one-campaign-native-20261105`. Starting Windows evidence: [18/26 on 82497aec](https://github.com/iamaman11/okx/actions/runs/42124898454), T09 core9/9 and T10-06 PASS; T10-03/05/15 executed FAIL; T10-08–12 NOT_PROVEN. Code fixes are WIP, not PASS until tests run on the exact new SHA. Current PR #493 remains on `b93be8e`, not this branch. The row-level table below is the **historical r8.3 evidence map**, not a new result on the active branch; row statuses and SHA must be replaced only when their actual receipts exist.

The 26 authoritative IDs are owned by `scripts/run-trade-integration.mjs --list`; all 26 `cases/<ID>.mjs` files were found on the work-branch snapshot. **File presence proves neither feature completeness nor scenario PASS.** All component/integration results below are **NOT_RUN on this exact branch SHA**; individual partial witnesses and deliberate fail-closed cases are documented rather than promoted. No production behavior was classified `NOT_FOUND` solely because it lacks a test.

**Existing command per row:** `node scripts/run-trade-integration.mjs --case <ID> --artifacts artifacts/a0-<ID>` (replace both placeholders with that row's ID); Windows Native acceptance remains required. Before the next full run: `node scripts/run-trade-integration.mjs --self-test` and `node scripts/run-trade-integration.mjs --list`. `CONNECT` means reuse an apparently sufficient existing adapter and verify on the exact SHA, not PASS in advance; `EXTEND` means a missing oracle/stimulus; `FIX` means inspect a reproduced failure. No IMPLEMENT decision is justified by the current search.

| ID | production path::symbol | existing test/command | CODE evidence | COMPONENT result | INTEGRATION result | exact gap / next observation | next action | SHA/run/artifact |
|---|---|---|---|---|---|---|---|---|
| `T09.0` | `crates/okx-agent/src/accounts.rs::AccountRuntimeRegistry::open; cloudflare/okx-mcp/src/risk_book.ts::installCampaignEntryProof` | [`cases/T09.0.mjs`](../scripts/trade-integration/cases/T09.0.mjs), `--case T09.0` | FOUND: native SQL A/B + risk opening | NOT_RUN | NOT_RUN | Confirm two account SQL owners, signed source and zero initial risk on this SHA | CONNECT | `f06383e` / no CI artifact |
| `T09.1` | `crates/okx-agent/src/trade_service/mod.rs::TradeService::trade; crates/okx-agent/src/trade_service/execute.rs::post_once` | [`cases/T09.1.mjs`](../scripts/trade-integration/cases/T09.1.mjs), `--case T09.1` | FOUND: public execute, status, fill/bill and Worker readback | NOT_RUN | NOT_RUN | One actual open/close/cancel route; signed fees and exactly three posts | CONNECT | `f06383e` / no CI artifact |
| `T09.2` | `crates/okx-agent/src/trade_service/plan.rs::plan; crates/okx-agent/src/trade_service/execute.rs::execute` | [`cases/T09.2.mjs`](../scripts/trade-integration/cases/T09.2.mjs), `--case T09.2` | FOUND: second native entry and Worker reserve | NOT_RUN | NOT_RUN | 0.30 lot admitted, upper 3.0015 and exact outbound count | CONNECT | `f06383e` / no CI artifact |
| `T09.3` | `crates/okx-agent/src/trade_service/reconcile.rs::settle_incremental_entry` | [`cases/T09.3.mjs`](../scripts/trade-integration/cases/T09.3.mjs), `--case T09.3` | FOUND: signed partial and terminal fill/bill sources | NOT_RUN | NOT_RUN | 0.12→0.30 fill, cumulative 4.002 and no retransmission | CONNECT | `f06383e` / no CI artifact |
| `T09.4` | `crates/okx-agent/src/trade_service/reconcile.rs::verified_live_protection; crates/okx-agent/src/trade_service/execute.rs::census_for_managed_protection_until` | [`cases/T09.4.mjs`](../scripts/trade-integration/cases/T09.4.mjs), `--case T09.4` | FOUND: managed add 0.20 and separate stop | NOT_RUN | NOT_RUN | Reproduce original protection incident with phase-aware inventory; prove owned/unowned stop distinction | EXTEND | `f06383e` / no CI artifact |
| `T09.5` | `crates/okx-execution/src/ledger/sql/intents.rs::by_idempotency; crates/okx-agent/src/trade_service/execute.rs::existing_request_hash_matches` | [`cases/T09.5.mjs`](../scripts/trade-integration/cases/T09.5.mjs), `--case T09.5` | FOUND: Native SQL + Worker semantic replay | NOT_RUN | NOT_RUN | Same-key replay/semantic changes preserve one decision/reservation/post | CONNECT | `f06383e` / no CI artifact |
| `T09.6` | `crates/okx-agent/src/trade_service/reconcile.rs::close; crates/okx-agent/src/trade_service/reconcile.rs::release_terminal` | [`cases/T09.6.mjs`](../scripts/trade-integration/cases/T09.6.mjs), `--case T09.6` | FOUND: close both lots; terminal and risk readbacks | NOT_RUN | NOT_RUN | Flat/orphan/held 0, entry and exit fees 0.003 each, 9 total posts | CONNECT | `f06383e` / no CI artifact |
| `T09.7` | `crates/okx-agent/src/trade_service/recovery.rs::reconcile_restored` | [`cases/T09.7.mjs`](../scripts/trade-integration/cases/T09.7.mjs), `--case T09.7` | FOUND: Native OS restart + persisted Worker readback | NOT_RUN | NOT_RUN | A99.994/B100 and no duplicate financial or venue mutation | CONNECT | `f06383e` / no CI artifact |
| `T09.8` | `crates/okx-agent/src/trade_service/execute.rs::revalidate_held_entry_quote; cloudflare/okx-mcp/src/risk_book.ts::reserveRisk` | [`cases/T09.8.mjs`](../scripts/trade-integration/cases/T09.8.mjs), `--case T09.8` | FOUND: budget denial after terminal cohort | NOT_RUN | NOT_RUN | Historical b93 failure in T09.4; distinguish source timing from actual stop; then upper4.002 vs remaining3.997 yields 0 new POST | FIX | `f06383e` / no CI artifact |
| `T09-MA` | `crates/okx-agent/src/trade_service/mod.rs::TradeService::trade; cloudflare/okx-mcp/src/runtime_session.ts::dispatch` | [`cases/T09-MA.mjs`](../scripts/trade-integration/cases/T09-MA.mjs), `--case T09-MA` | FOUND: A/B public barrier, scoped SQL and shared Worker | NOT_RUN | NOT_RUN | Confirm separate strategy/account identities, concurrency and shared limit without wrong-account mutations | EXTEND | `f06383e` / no CI artifact |
| `T09-SPEC` | `crates/okx-agent/src/trade_service/plan.rs::verify_linear_usdt_entry_units` | [`cases/T09-SPEC.mjs`](../scripts/trade-integration/cases/T09-SPEC.mjs), `--case T09-SPEC` | FOUND: second-spec public plan and Execute probe | NOT_RUN | NOT_RUN | ETH-USDT-SWAP alternate tick/lot/multiplier; invalid step 0 POST | CONNECT | `f06383e` / no CI artifact |
| `T10-01` | `crates/okx-agent/src/trade_service/execute.rs::verify_post_held_entry_sources` | [`cases/T10-01.mjs`](../scripts/trade-integration/cases/T10-01.mjs), `--case T10-01` | FOUND: stale/partial source plus fresh recovery probe | NOT_RUN | NOT_RUN | Complete source matrix (balance/history/algos/fee/FX/spec) before and after HELD with typed denial and recovery | EXTEND | `f06383e` / no CI artifact |
| `T10-02` | `crates/okx-agent/src/trade_service/execute.rs::certified_managed_external_exposure` | [`cases/T10-02.mjs`](../scripts/trade-integration/cases/T10-02.mjs), `--case T10-02` | FOUND: signed external 0.70 vs owned 0.50 | NOT_RUN | NOT_RUN | Positive owned exposure allowed; unowned/UNKNOWN blocks add and keeps money | CONNECT | `f06383e` / no CI artifact |
| `T10-03` | `crates/okx-agent/src/risk_bridge.rs::reserve_then_fence; cloudflare/okx-mcp/src/risk_authority.ts::handleNativeRiskCommand` | [`cases/T10-03.mjs`](../scripts/trade-integration/cases/T10-03.mjs), `--case T10-03` | FOUND: lost POST_AUTH reply, Worker INSPECT | NOT_RUN | NOT_RUN | Cover reserve-commit lost reply AND begin-post committed lost reply; same identity, no extra grant/send | EXTEND | `f06383e` / no CI artifact |
| `T10-04` | `crates/okx-agent/src/trade_service/execute.rs::risk_receipt_bound; crates/okx-agent/src/trade_service/execute.rs::post_receipt_bound` | [`cases/T10-04.mjs`](../scripts/trade-integration/cases/T10-04.mjs), `--case T10-04` | FOUND: foreign Worker command reply binding probe | NOT_RUN | NOT_RUN | Wrong command/account/reserve/plan/policy/CAS/generation all reject with 0 POST | EXTEND | `f06383e` / no CI artifact |
| `T10-05` | `crates/okx-agent/src/trade_service/execute.rs::post_once; crates/okx-agent/src/trade_service/reconcile.rs::status_intent` | [`cases/T10-05.mjs`](../scripts/trade-integration/cases/T10-05.mjs), `--case T10-05` | FOUND: lost ACK vs signed venue rejection cohorts | NOT_RUN | NOT_RUN | UNKNOWN then same clOrdId readback without resend, distinct explicit reject | CONNECT | `f06383e` / no CI artifact |
| `T10-06` | `crates/okx-execution/src/ledger/sql/intents.rs::prepare; crates/okx-execution/src/ledger/sql/intents.rs::by_idempotency` | [`cases/T10-06.mjs`](../scripts/trade-integration/cases/T10-06.mjs), `--case T10-06` | FOUND: 100 concurrent public identical-key requests | NOT_RUN | NOT_RUN | One intent/reserve and ≤1 create; changed payload conflicts | CONNECT | `f06383e` / no CI artifact |
| `T10-07` | `cloudflare/okx-mcp/src/risk_book.ts::reserveRisk; cloudflare/okx-mcp/src/risk_book.ts::releaseUnsentRisk` | [`cases/T10-07.mjs`](../scripts/trade-integration/cases/T10-07.mjs), `--case T10-07` | FOUND: Worker 3×4 plus Native shared-campaign prerequisite | NOT_RUN | NOT_RUN | Case intentionally FAIL: same-campaign Native source-backed 8→4 release, remaining 2→6 and no false SQL bridge | EXTEND | `f06383e` / no CI artifact |
| `T10-08` | `crates/okx-agent/src/trade_service/reconcile.rs::emergency_exit; crates/okx-agent/src/trade_service/reconcile.rs::verified_live_protection` | [`cases/T10-08.mjs`](../scripts/trade-integration/cases/T10-08.mjs), `--case T10-08` | FOUND: signed partial fill and protection first leg | NOT_RUN | NOT_RUN | Case intentionally FAIL: same-intent failed protection, late fill and bounded protective attempt | EXTEND | `f06383e` / no CI artifact |
| `T10-09` | `crates/okx-agent/src/trade_service/reconcile.rs::cancel; crates/okx-agent/src/trade_service/reconcile.rs::close` | [`cases/T10-09.mjs`](../scripts/trade-integration/cases/T10-09.mjs), `--case T10-09` | FOUND: B public Cancel ACK and persisted worker first leg | NOT_RUN | NOT_RUN | Case intentionally FAIL: same-intent post-ACK late fill, partial close and remaining protection | EXTEND | `f06383e` / no CI artifact |
| `T10-10` | `crates/okx-execution/src/ledger/sql/intents.rs::bind_late_entry_settlement; cloudflare/okx-mcp/src/risk_book.ts::settleEntrySpend` | [`cases/T10-10.mjs`](../scripts/trade-integration/cases/T10-10.mjs), `--case T10-10` | FOUND: duplicate fills/bills initial causal leg | NOT_RUN | NOT_RUN | Case intentionally FAIL: same-campaign late fee/rebate, replay revision and upper breach | EXTEND | `f06383e` / no CI artifact |
| `T10-11` | `crates/okx-execution/src/ledger/sql/intents.rs::recover_posting; crates/okx-agent/src/trade_service/recovery.rs::reconcile_restored` | [`cases/T10-11.mjs`](../scripts/trade-integration/cases/T10-11.mjs), `--case T10-11` | 40 connected F2/F3/F4/F8 kills; separate component evidence retained | PARTIAL | NOT_RUN | Case intentionally FAIL: complete F0..F8×10, same Native/Worker stores and no duplicated facts | EXTEND | `c239f8f7` / CI `42542429888`; scope below |
| `T10-12` | `cloudflare/okx-mcp/src/risk_book.ts::reserveRisk; cloudflare/okx-mcp/src/runtime_session.ts::dispatch` | [`cases/T10-12.mjs`](../scripts/trade-integration/cases/T10-12.mjs), `--case T10-12` | FOUND: Worker kill/reopen persisted DO after reserve | NOT_RUN | NOT_RUN | Case intentionally FAIL: after-settlement kill, fresh generation, Native binding/no double debit | EXTEND | `f06383e` / no CI artifact |
| `T10-13` | `crates/okx-agent/src/trade_service/reconcile.rs::close; cloudflare/okx-mcp/src/risk_authority.ts::handleNativeRiskCommand` | [`cases/T10-13.mjs`](../scripts/trade-integration/cases/T10-13.mjs), `--case T10-13` | FOUND: Worker STOP and protective close probe | NOT_RUN | NOT_RUN | Assert before-claim and post-fill pause ordering, stale reduce blocker and safe reduction | EXTEND | `f06383e` / no CI artifact |
| `T10-14` | `crates/okx-agent/src/trade_service/mod.rs::TradeService::trade; crates/okx-agent/src/trade_service/reconcile.rs::cancel` | [`cases/T10-14.mjs`](../scripts/trade-integration/cases/T10-14.mjs), `--case T10-14` | FOUND: A held SQL; B public Status/Cancel | NOT_RUN | NOT_RUN | Independent B progress under held A, then join A, 1 B cancel POST | CONNECT | `f06383e` / no CI artifact |
| `T10-15` | `crates/okx-agent/src/trade_service/execute.rs::revalidate_held_entry_quote; crates/okx-agent/src/trade_service/execute.rs::verify_post_held_entry_sources` | [`cases/T10-15.mjs`](../scripts/trade-integration/cases/T10-15.mjs), `--case T10-15` | FOUND: signed fee mutation after HELD | NOT_RUN | NOT_RUN | Check FX/spec/fee/policy changes and fixed plan binding before POST; 0 send on mismatch | EXTEND | `f06383e` / no CI artifact |

## A1 baseline and evidence boundary

The b93be8e Windows failure is in the T09.4 Add/Status prefix of T09.8: `INTENT_EXPOSURE_REMAINS`, correlated with `UNATTRIBUTED_CONDITIONAL_ORDER` and `COHORT_UNRESOLVED_POST_OUTCOME`. **Root cause not proven.** For A1 capture account/intent/source generation, route+query, phase ordering, two stop IDs, send/ACK, SQL/Worker revision; separate fixture publication timing from genuine unowned exposure. Use a deterministic I/O barrier; do not relax money checks, source freshness or production deadlines. The targeted positive must end in consumed 6.003, remaining 3.997, denied 4.002 and zero extra create POST. The negative must keep held/denied for incomplete evidence. Preserve baseline artifacts before changing code.

Known deliberate non-PASS cases on `f06383e`: `T10-07`, `T10-08`, `T10-09`, `T10-10`, `T10-11`, `T10-12`. Each writes or requests a partial prerequisite and throws a precise error until the integrated oracle is complete; a component probe cannot be substituted for this step. Remaining adapters also require runner validator/evidence sufficiency review and exact-SHA Windows execution.

## S1/S2 and USER acceptance (separate from 26 cases)

| Scope | Evidence already located | Exact outstanding / next verification | Status on `f06383e` |
|---|---|---|---|
| S1-01/02 | `accounts.rs::recorded_support`, `trade_integration_t09_4.rs`, `trade_integration_t09_6.rs` | Explicit pre-cancel and crossing-cancel GET barriers, stale then fresh inventory with original timestamp, source/budget accounting | NOT_PROVEN |
| S1-03 | Recorded support + transport invalid-request tests | Account/query/extra POST/missing event/expired source each independently fails with first reason and no mutation | NOT_PROVEN |
| S1-04 | Existing T09.0–.8 public suite; historical b93 CI | All nine T09 on one exact commit, no executed fail; exact source-backed budget/flat/restart | NOT_RUN |
| S1-05 | Existing Native test child lifecycle | Abort/join/close stores and preserve per-stage receipts on success/failure/timeout | NOT_PROVEN |
| S2-01/02/03/04/06/07/08 | Native semantic DTO + Worker `risk_book.ts::normalizeDecimal`, goldens, SQL replay tests | Locate exact current test filter and bind golden fixture + persisted replay to this SHA | NOT_RUN |
| S2-05 | Historical Windows policy-CAS artifact from b93 CI | Repeat actual immutable policy/strategy retry versus new-key decision on release SHA | NOT_RUN |
| R-UX01–08 / analyst / manager | `crates/okx-agent/src/analytics/*`; public MCP is the intended boundary | Run validate→backtest→grid/OOS→result→decision→export→reopen on one dataset and save checksum | NOT_RUN |
| Trader / INSTALLED / PHYSICAL / REAL | Existing P0 runtime, governance, release and backup owners | Only after CODE and applicable release/operational gates; REAL requires separate explicit authorization | NOT_RUN / NO-GO |

**Update rule:** refresh row evidence from the next exact SHA + CI/artifact; do not copy historical PASS across commits or turn a deliberate negative into a green gate. Required mandatory milestones remain 16→24→26 PASS with 0 executed failures. No full CI was triggered for this documentation/metadata update.
## #495 — entry settlement (bounded structural refactor only)

The coordinator now lives at `trade_service/entry_settlement.rs::settle_incremental_entry`,
with one production caller in `reconcile.rs::status_intent`. The old method
is removed; `release_terminal` remains the independent terminal owner. The
SQL owner still controls source-backed finance/bind/ack and NativeRiskClient
still carries authenticated Worker CAS. No changed FX, fees, risk limits,
network GET budgets, release ordering, or wire results.

**Direct Windows command** (run on final source SHA, require nonzero tests):

```text
cargo test --locked -p okx-agent --lib trade_service::entry_settlement:: -- --nocapture
```

R-01/R-02: `r01_r02_partial_settlement_and_same_revision_repeat_use_one_sql_ack`;
`r02_terminal_replay_reads_immutable_receipt_without_second_settle`.
R-03: `r03_released_late_bill_requires_same_reservation_new_revision_and_ack`.
R-04: `r04_missing_source_cannot_ack_and_identity_mismatch_preserves_first_reason`.
R-05: `r05_lost_settlement_reply_keeps_unacked_revision_and_replays_same_proof`.
R-06: `r06_three_cas_conflicts_never_ack_unaccepted_settlement`.
Additional tests prove demo/no-order fail-closed and typed error metadata.
All use real SQLite; programmable NativeRiskClient replies prove orchestration,
not Worker monetary arithmetic. No setup stamps successful settlement ACK
into a table.

[Windows run #42392615185](https://github.com/iamaman11/okx/actions/runs/42392615185)
at `6628b2acfb31d2f3bcdc8b4841760d5f366a2016` and
[run #42393552274](https://github.com/iamaman11/okx/actions/runs/42393552274)
at `6de0455f6930feced26a518df304945dfce657d1` both confirmed
**8 PASS, 0 FAIL** direct tests; the first timed **1.34s** after compilation.
Rust format/compile, architecture, Cloudflare and B1 prerequisites succeeded.
Later #499 run [#42403496577](https://github.com/iamaman11/okx/actions/runs/42403496577)
at `034f549cc9290e23479a8207dd2395ff1d2dbdc6` passed 8/8 direct
settlement and required Windows Clippy, but full trading acceptance was 20/26.
This does not establish release acceptance.

**Exact-parent address comparison**, original `3a892c4993e4a62a270197c52cd9d3c1afd00663`
against `6de0455f6930feced26a518df304945dfce657d1`:
- T09.3 PASS/PASS (run #42393552274, artifact 13582019224).
- T09.8 FAIL/FAIL at identical first
  `PROTECTION_CANCEL_OUTCOME_UNKNOWN_INSPECT` in `trade_integration_t09_6.rs:618`
  (artifact 13583265306).
- T10-09: first pair baseline PASS/candidate FAIL at recorded fixture
  `TRANSCRIPT_SOURCE_PAGE_UNEXPECTED_OR_EXHAUSTED` on
  `orders-algo-pending` (artifact 13583030900). On same SHA, one targeted job
  retry reproduced baseline FAIL/candidate FAIL with identical first
  `T10_09_AMEND_ACK_SQL_MISSING` and
  `PROTECTION_SHRINK_ORIGINAL_RISK_UNVERIFIED` in
  `trade_integration_t10_09.rs:846` (artifact 13582866530).
  Therefore the recorded integration path is **nondeterministic** even on
  the original SHA; it is **not** proven PASS or financial-complete.
- The operation bodies were compared with normalized whitespace after only
  replacing `self` dependencies, deadline provider, and successful typed
  outcomes; the transformed original and extracted source were identical.
  Counts before/after: 8 SQL calls, 5 risk action uses, 2 bounded retry loops,
  3 bind calls, 2 SQL ACK calls.

The temporary three-case Windows comparison matrix was intentionally removed
after preserving the two independent runs and artifacts. Only fast direct
production-boundary tests remain in the existing `windows-native` job.
Source-fixture stability and protection failures belong to #449; #495 cannot
be used to claim T10-09 PASS or bypass full26/required release gates.

**#500 protection cleanup — accepted bounded production operation.**

Acceptance proof is the completed Windows [run #42479784285](https://github.com/iamaman11/okx/actions/runs/42479784285)
on **source SHA `8cbcd554361455e80d51e69f564edc5c28820a89`**;
[26-case artifacts](https://github.com/iamaman11/okx/actions/runs/42479784285/artifacts/13617249020).
Exactly **23/26** are PASS on that SHA, including **T09.6, T09.7,
T09.8, T10-08 and T10-09**. Only T10-10, T10-11 and T10-12 FAIL;
these are subsequent #449 stages and are NOT acceptance criteria for #500.
Windows cargo tests, Clippy, Linux Rust/Core, Architecture, Cloudflare,
Windows B1 and both fast operation suites passed on the same source SHA.
Neither general CODE 26/26 nor REAL/installed release is accepted.

**One production operation, no shadow test algorithm.** Public `Status` and
PC-07 both invoke `trade_service/protection_cleanup.rs::cleanup_flat_protection`
with explicit SQL owner, authenticated observer, real REST adapter, semaphore,
UID and verified order facts. The old Status-local cancellation/ACK/readback
branch no longer acts as a second decision path; the duplicated ACK verdict
was consolidated in `send_protection_cleanup_after_admission`, and the same
SQL/observed readback/receipt path is called by the direct test. The existing
AccountStore remains the sole durable BEGIN/single-send/receipt owner;
release/census and money remain separate owners. A flat position with a
still-live original entry order is NOT cleanup permission. A lost ACK cannot
cause another POST; incomplete/stale, wrong-account or still-live algo reads
remain pending/blocked, not falsely confirmed. No financial formulas,
RiskBook authorization, TTL, wire/SQL schema or POST retry policy were relaxed.

**Causal RED → GREEN:** earlier [run #42417714196](https://github.com/iamaman11/okx/actions/runs/42417714196)
recorded `cleanup_<client-id>` SQL `POSTING` and an actual cancel attempt
for `900000002` **before the signed T09.6 cancel observation phase**. This
explained the later missing target ACK without granting resend permission.
The bounded fix aligned source/operation boundaries while preserving
independent signed pre-ACK and post-ACK observations, exact-algo POST/ACK
identity and the original durable operation. A production+fixture cause
must not be collapsed into permission to manufacture a successful readback.
The final T09.6/.7/.8 PASS plus PC-01–07 proves this bounded flow;
intermittent unrelated managed-Add readiness on other SHAs is not evidence
of an ACK or a financial regression in cleanup.

**Direct reproduction on the code SHA** (after building Rust on Windows):

```text
cargo test --locked -p okx-agent --lib trade_service::protection_cleanup::protection_cleanup_direct_tests:: -- --nocapture
cargo test --locked -p okx-agent --lib trade_service::entry_settlement:: -- --nocapture
node scripts/run-trade-integration.mjs --case T09.6 --artifacts <existing-artifact-dir>
node scripts/run-trade-integration.mjs --case T09.7 --artifacts <existing-artifact-dir>
node scripts/run-trade-integration.mjs --case T09.8 --artifacts <existing-artifact-dir>
node scripts/run-trade-integration.mjs --case T10-08 --artifacts <existing-artifact-dir>
node scripts/run-trade-integration.mjs --case T10-09 --artifacts <existing-artifact-dir>
```

In the exact CI run the direct protection tests **PC-01–07 = 7 PASS/0 FAIL
in 3.06s** *after compilation*. PC-01 covers one exact send/receipt and
replay; PC-02 lost ACK and no second send; PC-03 stale/incomplete sources;
PC-04 foreign identity/late exposure; PC-05 original SQL after restart;
PC-06 two callers and pre-ACK stale read; PC-07 full signed HTTP
cancel/ACK, independently read-back complete 16-scope inventory and same
production SQL receipt. Entry settlement **8 PASS/0 FAIL in 2.68s** after
compilation. The direct tests are registered in the existing Windows
`windows-native` CI job; no second harness or long campaign is required
to explain the cleanup operation.

Read-only `diagnostics view=operation` exposes a bounded account/operation
trace with operation identity, local BEGIN, attempted send, ACK/readback,
source timestamps, SQL receipt and specific blockers; the P95 <=100ms
warm-local test is a diagnostic target, not a venue SLO. This volatile
projection does NOT fabricate durable history after restart/eviction;
SQL remains the authority and an absent projection never proves no-send.

**Scope boundary:** #500 accepts only this operation and its directly
associated transitions; PR #499 remains Draft and **must not merge** until
full #449 CODE 26/26, S1/S2, required checks and branch integration evidence.
A documentation-only commit following the proven SHA does not change tested
Rust/JS behavior; it must not be represented as a new full-CODE PASS.

## #501 — unsent release: one production boundary

`trade_service/unsent_release.rs::release_unsent` owns the existing orchestration
for a durably rejected, never-sent intent. `TradeService::release_unsent` is a
thin adapter used by public Status, Execute rejection and maintenance through
Status. The former implementation in `reconcile.rs` is removed. Terminal
position release remains a different operation with its existing guards.

Dependencies are the account SQL handle, NativeRiskClient, governance lock and
per-call SQL deadline. The operation has no venue adapter. AccountStore still
owns no-send proof and acknowledgement; RiskBook still owns central money.
`UnsentReleaseStage` identifies `sql_proof`, `authority`, `inspect`, `release`
or `sql_ack` at the failing call, with the original reason code and known
intent/reservation identity. Public Status retains `central_release.reason_code`
and adds `central_release.failure`; Execute retains `risk_release_error` and
adds `risk_release_failure`. This is a result explanation, not a new persisted
event history or a claim that a pending operation is safe to resubmit.

```text
cargo test --locked -p okx-agent --lib trade_service::unsent_release::tests:: -- --nocapture
```

UR-01/02: missing durable proof/authority, no risk command.
UR-03: HELD → RELEASE_UNSENT → SQL ACK, repeat retains the first receipt.
UR-04: lost reply, actual SQL owner shutdown/reopen, INSPECT before acknowledgement.
UR-05: stale CAS reinspection and the unchanged four-attempt bound.
UR-06/07: denied, missing/invalid and unresolved replies; existing Posted/absent paths.
UR-08: real SQLite trigger rejects ACK; no false success, later readback recovers.
UR-09: local acknowledgement and Demo paths make no Worker call.
UR-10: two concurrent callers, ordered conflicting replies, one durable acknowledgement.

These tests use real SQLite and NativeRiskClient with scripted response facts.
They prove orchestration, not Worker economics, a real OS crash, or full CODE.
The first Windows run on `2c3f780c` passed **10/10 in 2.36s** after compilation
([CI 42519995459](https://github.com/iamaman11/okx/actions/runs/42519995459)).
The existing CI workflow supports `target_case=UnsentRelease` for these tests
without Workerd or the long trading campaign. The mandatory PR matrix and
full #449 release gates remain unchanged; this mode cannot certify 26/26.

## T10-11 — connected F2/F3/F4/F8 checkpoint, 2026-11-09

Code SHA `c239f8f7743b5598b4c0319c4d97fb90f6b0ebe7`,
[Windows CI 42542429888](https://github.com/iamaman11/okx/actions/runs/42542429888):
40 independent OS kills passed their assertions, ten per boundary:

- F2: Worker HELD committed, reply lost before local receipt.
- F3: local RESERVED committed, actual Worker INSPECT proves HELD, no BEGIN_POST.
- F4: actual Worker POST_AUTHORIZED committed, no local claim; replay of the old
  fence after restart is DENIED without changing the book sequence or budget.
- F8: Worker release committed, reply lost before local SQL acknowledgement;
  recovery reads RELEASED and writes one SQL receipt without a second mutation.

Every new Native process opens its original account SQL; public Cancel/Status
persists CENTRAL_RELEASE. Replay retains the same receipt. Venue POST count is
zero. Reopening the same Worker storage preserves RELEASED and the original
budget amounts, including the pre-existing foreign reserve.

Artifacts `T10-11/native-worker-fX-process-crashes.json` for X=2,3,4,8 and forty
individual `native-worker-fX-process-crash-N.json` bind each repetition to its code SHA,
original SQL root, killed/recovery PIDs and SQL/Worker readbacks. Each repetition
creates fresh stores, then retains those stores across its restart. Native
crash executions never reuse memoized evidence. Series durations were F2=49.180s,
F3=32.627s, F4=32.594s and F8=35.266s. Individual repetitions ranged from 2.794s
to 10.746s. The whole targeted case took 156.397s after compilation, including
the retained 60 component kills in 6.196s. These measurements are not a guarantee
for a different runner. Recalculate bounded child/case/job envelopes when adding
the remaining phases; do not alter production source TTLs or trading deadlines.

This is a direct boundary test with explicit SQL prepare/reserve and census/causal fixture inputs,
not proof of full public Execute/admission, automatic recovery scheduling or a
live exchange. The separate component kills still lack connected Worker proofs;
some overlap the accepted boundaries. **Do not sum them into integrated acceptance.**
Connected F0/F1/F5/F6/F7 remain incomplete; T10-11 intentionally
ends `T10_11_ALL_NINE_NATIVE_WORKER_PHASES_TEN_TIMES_NOT_YET_PROVEN`.
Neither T10-11 nor 26/26 is accepted.

On the same code SHA, [CI 42543265153](https://github.com/iamaman11/okx/actions/runs/42543265153)
passed Rust formatting, all ten direct unsent-release tests and
`cargo clippy --locked -p okx-agent --all-targets -- -D warnings`.

Direct account-reopen regression, before the actual crash scenario:

```text
cargo test --locked -p okx-agent --lib accounts::tests::recovery_prepared_account_reopens_catalog_and_original_sql -- --exact --nocapture
node scripts/run-trade-integration.mjs --case T10-11 --artifacts <artifact-dir>
```

The first command checks original persisted SQL, catalog identity, duplicate
owner rejection and no POST. It waits for the existing SQL termination signal,
not an arbitrary delay. The second command remains a partial/FAIL acceptance
until every required F0–F8 boundary is proved ten times. A subsequent docs-only
commit does not represent a new tested runtime SHA.
