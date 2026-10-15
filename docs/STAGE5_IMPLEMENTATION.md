# Stage 5 implementation checklist and live-admission contract


## Stage 5B / 5C: one-account compact trader view (PR #334, candidate)

The existing primary Cloudflare MCP `account_summary` tool supports an
optional `view: "trader"` that changes the native Rust read-only query to
`trader_snapshot`. `view: "full"` (or omitted view) preserves the old
diagnostic response. There is no second MCP tool, Windows agent, Demo
analytics engine, state database, timer, execution ingress, or transport.

One native account snapshot and one ledger reconciliation own **both** views:
authenticated account UID and native position/order/OCO counts, source
coverage/coherence, strategy STOP, terminal NAV/checksum, FINALIZED vs
UNVERIFIED count, and funding-bill attribution are taken from exactly the
same previously reconciled source. The compact view is bounded to 12,288
bytes of serialized result; it includes actual source history-read duration,
full-result serialized byte count and compact-result serialized byte count
so bandwidth/context reduction is measured rather than guessed.

**Critical safety:** the read-only view can return `NO_GO` on a source/risk
blocker or `NOT_ATTESTED` otherwise, **never `GO`**. Positive new-risk
permission requires the separate explicit, current executor preflight,
instrument-specific Production-public market FRESH, account/strategy NAV,
versioned risk policy, and immutable execution plan revalidation. An
allowed reduce/close under STOP still uses the existing native executor.
Fundamental strategy signals never come from Demo. Production Live OFF
until separately admitted.

Funding bills (OKX subTypes `173`/`174`) provide account-side PnL but
not a universal exact strategy/position-side order identity. The compact
packet reports ambiguous funding as **unallocated**, never silently
included in strategy NAV; positive assignment needs source-exact native
ownership and independent financial tests. One-symbol or time-window
coincidence is insufficient. Official source:
https://www.okx.com/docs-v5/en/ (Get bills details).

**Separate unaccepted physical test:** v14 cold on-disk migration after
first legitimate persisted ledger write, intact pre-v14 snapshot and
detached archived evidence, fail-closed recovery and performance. The
day-boundary STOP does not prevent continuing code or read-only tests.
Avoid artificial trading POST or bypass of account-safety STOP to create
a migration proof.


## Stage 5B native one-time v13 → v14 storage migration (PR #336)

On the **existing single Windows agent/executor** only, an explicit Demo-profile
`execution_action` with `action="migrate_finalized_ledger_storage"` migrates
a stopped FINALIZED v13 to its v14 manifest and detached archives. Inputs
are **only** `intent_id` (the precise finalized run),
`expected_manifest_sha256` (from a live `account_summary(view="storage")`),
`expected_finalized_checksum_sha256`, and `expected_stopped_at_ms`.
No generic shell, raw JSON RPC, extra agent, database, executor, polling loop,
exchange API call, strategy rearm, fresh run, PnL recalculation or trade.

Fail-closed predicates: authenticated direct transport Demo profile, local
Demo execution mode, exact original on-disk v13 hash, exact run-final checksum,
exact stored STOP timestamp, and **equal on-disk/in-memory financial state**.
The same original executor mutex serializes the operation with all other
ledger mutations. The original ledger writer first retains byte-identical
`.pre-v14.json`, seals old FINALIZED/UNVERIFIED runs in content-hashed
`.archive` files, then atomically publishes the new v14 manifest.

After writing, the same native owner verifies actual storage layout and
immutable STOP/finalization IDs again. The existing `view="storage"` supplies
independent read-only cold validation. A repeated matching request after lost
ACK is a read-only verification that the preserved pre-v14 SHA matches the
original hash; it never performs a second write. A mismatched request is
rejected rather than applying a stale snapshot. On uncertain failure **read
storage first**; never blindly repeat.

This is storage maintenance, not proof of a completed external backup or
live trading admission. The account-specific finalized NAV/checksum, durable
STOP and no-position state must match before and after actual CONTROL/MCP
acceptance. Production Live stays disabled. A new day is not a prerequisite.

## Stage 5B recovery audit without Demo trading (integrated acceptance slice)

`account_summary({view:"storage"})` calls a **distinct, explicitly requested
read-only Rust query**, inside the **same Windows agent and ledger owner**. No
new tool, service, scheduler, executor, trading endpoint or mutable fallback.
The query reads exact installed on-disk `execution-ledger.json` bytes,
verifies the legacy or v14 reader invariants (including detached archive
hashes and identities), and reports a manifest SHA256, actual on-disk schema,
verified sealed FINALIZED/UNVERIFIED counts, immutable pre-v14 original
snapshot SHA256 where present, durable STOP reason and final NAV checksum.

Status `LEGACY_PENDING_FIRST_AUTHORIZED_WRITE` means a v14-capable binary
**has not** converted the actual ledger. It is NOT a failure and MUST NOT
trigger a fake trade, reset STOP or convert the existing run. Status
`V14_SEALED_OR_EMPTY_ARCHIVE` means the manifest/archive payloads were
validated by a cold, non-mutating disk read; it is NOT an automated backup
or proof of off-host disaster recovery.

Native Linux + Windows tests perform the full legacy→v14 transition on a
synthetic fixture and then **copy** primary manifest, `.archive` directory
and `.pre-v14.json` original into a different temporary directory. The
copy is cold-loaded and checks all financial IDs/hashes. Missing or
tampered segments fail closed, and failed recovery never modifies the
manifest. This tests portable offline bundle semantics before any future
live migration, without new cloud hosts or Demo execution.

The operator must back up the entire directory with a consistent
single-owner quiescent boundary. Reading the audit result does **not** itself
freeze a filesystem, snapshot data or authorize binary rollback. Existing
UTC STOP remains live until a separately proven native rearm and separate
operator authorization; Production Live remains disabled.

## Stage 5B financial ledger v14: sealed archive / recovery invariant (PR #333, merged and installed)

This change is **NOT ACCEPTED until full CI, Windows CONTROL source-tree
parity, cold restart, and independent read-only MCP proof**. Production Live is
OFF. The one Rust `okx-execution` ledger remains the only financial/mutation
writer: there is no second agent, service, database or independent archive owner.

* Atomic ledger commit now means a **bundle** under the same runtime root:
  `execution-ledger.json` is the authoritative v14 manifest; adjacent
  `execution-ledger.archive/` contains immutable `finalized-<run-id>.json`
  and `unverified-<run-id>.json` source-evidence bodies. The manifest publishes
  SHA-256 content references only *after* each sealed body is durable.
* On startup, missing/mismatched/duplicated detached run bodies fail closed
  before execution resumes. Old v1–v13 inline archives remain readable and
  migrate only through the existing ledger writer. A crash before manifest
  publication can leave unreferenced staging data, never a false accepted proof.
* **Migration:** before its first v14 write, the same native ledger
  writer seals the exact original v1–v13 file as
  `execution-ledger.pre-v14.json`. A conflicting snapshot aborts admission
  without modifying the original ledger. This snapshot is NEVER a live
  runtime fallback or an authority to replay an uncertain trade.
* **Backup, transfer and restore the v14 manifest and archive directory
  together**, never the manifest alone. `execution-ledger.pre-v14.json`
  must also be preserved for operator-controlled disaster recovery. The
  old v13 binary CANNOT load v14; binary rollback after migration requires
  verified account FLAT/source, stopping the sole owner, and a separate
  consciously authorized legacy data restoration. Do NOT automatically
  restore a stale legacy file over later v14 financial events.
* A binary deployment must not erase financial files. Do not repair a bad
  archive by deleting a record, recomputing a checksum, inventing a fill,
  or silently falling back to a newer exchange REST window.
* The archive still has an explicit bounded ceiling (256 sealed periods); the
  response includes both FINALIZED and UNVERIFIED consumption. The standard
  readback returns a bounded sample rather than hundreds of proof records; the
  underlying history is not evicted. This is not a claim of infinite retention.
* Funding bills (OKX subtype 173/174) remain **UNALLOCATED** where a native
  instrument/side/time/position-owner proof is unavailable; no matching-by-symbol
  shortcut. Funding allocation is not accepted by this archive-only change.


**Stage 5A current recovery decision (2026-10-14, [PR #329](https://github.com/iamaman11/okx/pull/329), DRAFT):** The selected Demo run `stage5b_financial_demo_run_20261014_d` is irrecoverably `INCOMPLETE` after a 9h+ private source gap, despite current FLAT/0 orders/0 OCO. Never force `FINALIZED` or write fictitious NAV/fee/PnL. The single execution ledger v11 adds an explicit immutable `UNVERIFIED` *non-financial closure* containing run/account ID, exact source checksum, reason and cryptographic closure checksum, and archives the exact native evidence separately from financially finalized runs. This is an explicit Demo-only administrative action with current fresh independent REST/private-WS/OCO/FLAT proof; refuse if any run-owned execution, recent native fill/bill/order, active STOP or unknown account status is present. Only a separate independently attested new FLAT opening may start the next run with fresh cursors; prior run ID/history are never reused or silently purged. Read-only `account_summary` must surface both financially finalized and unverified proofs distinctly, the latter without NAV. Do not execute the action or claim Stage 5A acceptance until CI and Windows CONTROL/MCP acceptance prove these conditions. Production Live OFF, no new services/fallback.


**Stage 5A source continuity — single-owner quiet checkpoint (2026-10-14):** The private WS financial capture accepts real account/order/fill events and checks once at startup but cannot prove event-free periods if no events arrive. The functional slice in the existing native Windows runtime checks coverage due at its already-established mailbox heartbeat, only for a non-finalized Demo acceptance run and only after 1h without persisted authenticated source proof, with at most one failed REST/WS proof attempt per 5m. The same existing `capture_one_native_financial_source` verifies account snapshot, private WS generation/convergence and complete native REST orders/fills/bills, then appends through the one ledger lock. No new heartbeat, timer, Tokio task, daemon, scheduler or parallel source, no exchange POST, and no Demo analytical market. A stopped/unreachable agent over 2h cannot retroactively recover vanished canceled-order history; `run d` remains `INCOMPLETE` forever for that gap. Proof requires exact 6/6 CI, verified Windows CONTROL and MCP, real future run continuity, and independent NAV/high-water/loss STOP + restart/allowed exit; Stage 5A NOT_ACCEPTED before that.



**Active Stage 5A durable NAV slice (2026-10-14):** [#326](https://github.com/iamaman11/okx/pull/326) is MERGED and physically CONTROL/MCP ACCEPTED for single-source Production-public mark-to-market, with no Demo analytical feed. [#327](https://github.com/iamaman11/okx/pull/327) is one DRAFT functional PR using the existing atomic ledger for bounded, hash-linked NAV/high-water checkpoints and conservative UTC-day gross-realized-outflow STOP, with compact readback and no new service/agent/transport. A coherent source read may attest an unchanged quiet interval without forcing extra persistent cursor writes; missing native events, uncertain bills, stale Production mark or broken source coverage still fail closed. Neither sparse observed peaks nor a code-only STOP is full Stage 5A acceptance: require exact CI, verified Windows CONTROL/MCP, restart/tamper/day-boundary tests and physical OPEN valuation before Stage 5A can close. Production Live OFF, no new risk permissions.

**2026-10-14 proven native margin-boundary defect (single functional fix candidate):** PR #319 is merged and exact installed via GitHub CONTROL #12, artifact 11857091440, independently VERIFIED agent SHA256 479d581e65708814928886bf3a5d6dc03a3506bd66eb0dd1980f60e2e6ea2d07. After installation, read-only MCP `account_summary.strategy_run_opening_source.blockers` returned exactly `NON_USDT_TRADING_EQUITY`, with FLAT, 0 OCO and consistent immutable ledger. An independent read-only `trading_capabilities` returned `acctLv=2` Futures and `BTC-USDT-SWAP` linear/settled USDT. OKX Futures mode shares cross margin only *within* the margin currency, unlike multi-currency/portfolio margin. The prior blanket foreign-currency veto is therefore overbroad; the **candidate correction** treats non-USDT equity as separate from USDT margin only when both native authenticated account config and account snapshot attest `acctLv=2`. Any mismatch, absent/other account mode (including `3`/`4`), non-USDT-settled or nonlinear NEW-risk contract remains fail-closed. Begin, prepare/pre-submit initial NAV, and FINALIZED must all share the same verified source rule; amounts remain native `balance.details.USDT.eq`, not `totalEq` nor exchange-rate conversion. Mandate and 100 USDT reservation remain virtual and account-scoped, not a real sub-wallet; require FLAT, source-exact orders/fills/bills, no OCO, independent WS, stable UID, and post-run external-cash/foreign-bill checks. No native risk stop/financial high-water/day-loss relaxation. No new DB, worker, executor, transport or recurring task. **Full Stage 5B-1 NOT_ACCEPTED** until exact-head 6/6 CI, merged-tree equality, CONTROL-installed proof and one physical financial Demo run + restart/NEXT.


This is an **implementation checklist subordinate to [#160](https://github.com/iamaman11/okx/issues/160)**, not an additional plan or trading authority. Stage 4 exchange proof is [#223 G3C terminal acceptance](https://github.com/iamaman11/okx/issues/223#issuecomment-6103793654). As of 2026-10-13, **Stage 5B-1 is the active implementation cursor; Stage 5A full NAV/STOP is not accepted**. Stage 4C's supported execution/recovery subset is physically accepted through [#304](https://github.com/iamaman11/okx/pull/304) and [#305](https://github.com/iamaman11/okx/pull/305), but the product's complete protected ADD/HEDGE/partial REDUCE functionality remains UNSUPPORTED / NOT_ACCEPTED. This does not grant authority to enable Live. Existing Stage 5A source changes #263 durable STOP_NEW_RISK, #264 account-wide breach latch and #267 complete SWAP/FUTURES UTC-day evidence remain the single implementation; this stage must correct the candidate margin budget and prove the policy's admission boundaries rather than duplicate their owners. Stage 5B, 5C and final explicit Live gate are not yet accepted.

**5B-1 observed baseline:** [PR #308](https://github.com/iamaman11/okx/pull/308) verified one exact tested/merged tree, six CI jobs, one native Windows install and independent primary-MCP read-only result: 35 intents, 29 orders, 26 fills, 33 bills, 8 exact links, 3 funding bills not assigned, `strategy_nav_available=false`. The next same-stage functional slice uses native fill `billId` + `fillPnl` and trade bill `billId` + `fee` + `pnl` to reject fee/currency/PnL mismatches, account-external bills, missing/truncated evidence; it must never retroactively invent strategy ownership, funding attribution or mandate capital flows. **#308 is an accepted conservative projection, not full 5B-1 or 5A acceptance.**

**Installed #313 accepted:** exact 6/6 CI, tested==merged tree, native CONTROL install artifact 11818341485, running PID 19144 and independently VERIFIED installed SHA256 c64b1581ef1e8a64e74f0cbd9c893dfb338992e341dbf60637ac32b37eb2e57a. The Demo MCP is FRESH. The installed `begin_strategy_run` action is now visible in the ChatGPT tool schema; no run opening was invoked. The remaining Stage 5B-1 scope is ledger-owned run identity, complete durable event-source attribution, position marks, independent NAV and irreversible finalization; until accepted, venue new risk remains denied.

**Historical pre-merge scope of PR #315 (now MERGED and installed; Stage 5B-1 still NOT_ACCEPTED):** source lot accounting must reject foreign CLOSE, partial over-reduction, absent owned OPEN, wrong Hedge side, reversed timestamps, duplicate or missing exchange IDs/bills, funding with ambiguous ownership and truncated windows. The same execution ledger must persist source-exact events/cursors across restart, before short OKX history expires; FLAT closure must forbid next-run contamination; open NAV requires independently sourced current mark and position-owned instrument mechanics. This is not a new stage or module owner and cannot be merged while any of these conditions lacks passing tests/physical proof. Installed #314 remains production-intended Demo baseline; no new-risk permission or Live activation from PR #315 interim CI.


**Historical interim #315 source-evidence integration (superseded by installed v9):** The single `okx-execution` atomic ledger now writes `okx.execution-ledger/v8` including exact account-sourced order/fill/bill payloads, hash-checked append-only keys, source observation cursors and a compact evidence summary in the existing account reconciliation response. Complete fresh source coherence is required before journal mutation; no second persistence owner or exchange POST was added. Funding remains unallocated without proof. This does NOT establish strategy NAV for open positions, owned reduction PnL, finalize/next-run safety, archive policy or physical Demo/restart acceptance. When an exchange identity conflicts or the evidence cap is exceeded, persistence fails closed without data eviction. Continue T1–T5 in PR #315 and do not merge solely because CI succeeds.


**Persistent run finance projection, still NOT ACCEPTED:** The source-journal projection `okx.strategy-financial-period/v1` calculates FIFO run-owned opening/reducing lots, validates exact order/fill/bill IDs and USDT-only native fee/PnL agreement, and checks open linear-contract marked PnL against independent account position mark and reported UPL. External fills/bills, ambiguous funding, history gaps, unmatched positions, duplicate ownership, and incomplete native terms all force `INCOMPLETE` (NAV absent). All reports have `usable_for_new_risk=false` until time-series NAV/peak/daily STOP and finalization are independently tested. This is not shared account equity or trading authority.

**Installed #315 financial-period lifecycle (Stage 5B-1 still NOT_ACCEPTED):** The same atomic ledger now writes `okx.execution-ledger/v9` with an active run, source-exact journal, bounded finalized-period history and per-run immutable SHA256 finalization. `FINALIZED` denies new risk while preserving permitted reducing/protective operations and original evidence; opening another run archives the previous reservation/finalization/evidence under the same writer. Replay rejects duplicate IDs, overlapping periods and a corrupted final NAV without rewriting disk. Terminal order coverage checks the latest source-timestamped state, persists native order `sz` and `accFillSz`, and requires native fill sums to agree exactly for both FILLED and canceled-unfilled/partial orders. Missing quantities, orphan fills, conflicting source identities, and amended orders without independently proved final size remain `INCOMPLETE`, never synthetic zero. The existing typed Demo `execution_action.finalize_strategy_run` now gathers a fresh authenticated account, complete order/fill/bill history, private WS convergence and full native TP/SL/OCO absence inside Windows; only the one executor can atomically persist FINALIZED, with idempotence rechecked under fresh FLAT evidence. `account_summary` exposes the compact immutable finalized run and archived-run count. Existing ledger migrations advance to v9. **Not yet accepted:** independent current market mark and durable NAV peak/day-loss policy, physical Windows install and restart with exact binary/source proof, and a physical Demo OPEN→FLAT→FINALIZED→NEXT trade cycle. No Live authority or Stage acceptance from source/CI alone. No T3/T4 or Live assertion follows solely from Rust source and CI.

**Exact installed milestone / active gap (2026-10-13):** [PR #315](https://github.com/iamaman11/okx/pull/315) was merged (main `044b143a5aaf6565916c93dc20ad8c1de854a105`; tested/merged source tree `7f426523efeab160452eaff66fedbe03395cdd6e`). PR CI and postmerge CI SUCCESS, native MCP Worker deploy SUCCESS, Windows CONTROL exact artifact `11837773644` and independently VERIFIED installed SHA256 `dee19b2c57e85be180b26e20f9afb6141b00d0b24530a3f188a7d1b4d409fee8`. Runtime MCP PASS, Demo FLAT, but the **full financial run has not been physically closed or replayed**. Draft [PR #316](https://github.com/iamaman11/okx/pull/316) corrects unchanged MCP tool schema version and ensures transient incomplete REST history cannot poison immutable cursors; compact summary reports source gaps/capacity and checkpoint deferral. It does **not** guarantee unattended continuity across OKX's short cancellation history or more than 16 archived runs. No second service/scheduler/fallback or Live permission. Next: independent MCP tool schema visibility, exact CI/deploy, then measured long-run source continuity and full T1–T5 physical finance/restart.

**Current Stage 5B-1 financial durability work (after fully verified merged/installed #317):** The original native ledger used one cursor per complete REST fetch, including unchanged read-only account summaries and high-frequency private event readbacks. This can exhaust the 4096-cursor capacity before the 40000-source-event cap and obstruct a genuine run close. The current functional candidate coalesces only redundant quiet REST cursor writes into a bounded 5-minute interval; every new native order/fill/bill is still persisted immediately, while a typed FINALIZED forces a fresh exact REST cursor through the SAME execution ledger. Older and conflicting source timestamps fail closed. No periodic thread, polling, new service, raw transport, or source-window invention. **Still NOT_ACCEPTED** until exact-source CI/CONTROL, crash and capacity regression, full strategy NAV/high-water/day STOP, and physical Demo BEGIN→OPEN→CLOSE→FINALIZED→restart→NEXT. Repeated unchanged REST reads are not independent proof of a lost >2-hour quiet-period gap; delayed bills and external writers remain unsupported without new source evidence.

**Historical #317 work pointer (now merged, tested and CONTROL-installed; not the current task):** Source continuity is captured from authenticated nonempty private account, positions and orders WS events, not a ChatGPT request, by the existing Windows runtime. It re-reads complete native REST order/fill/bill and requires two unchanged private WS convergence fences and same account generation before persisting through the one ledger mutex. No new worker/service/poll, no exchange POST. The original server-side MCP tool contract is now `2026-10-13.2` and CONTROL verified #316 installed; the currently supplied ChatGPT execution tool schema still does not expose `finalize_strategy_run` despite backend support. No raw RPC/GitHub fallback may bypass this. **This event signal alone does not prove completeness for a long QUIET period or delayed bills without a subsequent private event.** Stage 5B-1 must remain NOT_ACCEPTED pending these tests, durable NAV/STOP and physical Demo BEGIN→OPEN→CLOSE→FINALIZED→NEXT/restart.

**One prior work pointer:** No account/mandate equity relationship authenticates a strategy NAV. Until the existing ledger provides source-verified run-owned NAV/mark/high-water/day-loss evidence at both prepare and pre-POST, candidate risk increases must be rejected even for equity <= declared mandate. This rejection is distinct from durable breach STOP and must not block legitimate protective exits. Preserve external OKX order/fill/bill limits and 2-hour canceled-unfilled gap behavior, with exact IDs in the existing durable owner; never assert historical source completeness beyond what was saved.


**2026-10-13 native opening (PR #312, source/CI/CONTROL accepted, no operator invocation):** one authenticated Demo-only `begin_strategy_run` operation on the existing MCP execution tool collects native UID, coherent WS account, orders/fills/bills coverage, USDT equity, current FLAT, and zero pending SWAP/FUTURES conditional/OCO *inside* Windows; the caller cannot assert these facts. The one existing executor persists an immutable virtual reserve and rejects duplicate/over-budget/foreign origin, preserving read-only Production. This is source admission only: funding attribution, owned active mark, durability across the short unfilled-order history horizon, strategy high-water/daily STOP and full positive NAV remain NOT_ACCEPTED. No automatic Demo allocation, exchange POST or Live authorization. Full test/CI/install independent proof required.


**5B-1 follow-up to installed #309:** exact eight fill/bill links are account evidence, not proof of strategy-only profit or NAV. Original immutable origin schema/version and mandate must partition reports into non-exclusive provenance cohorts. Missing or truncated order/fill/bill windows must be visible and fail closed. Neither TradingMandate.capital_base_usd nor shared Demo equity verifies initial cash allocation. OKX funding 173/174 remains unallocated without original position-side/strategy ownership. Negative tests must cover multiple cohorts, history truncation and no double count; exact CI, native CONTROL install and independent primary MCP evidence required. No Live authorization.

 [#160](https://github.com/iamaman11/okx/issues/160) → Stage 5A budget and conservative `STRATEGY_NAV_UNATTESTED` gate source/physical PASS through #307; full strategy NAV and physical irreversible STOP **NOT_ACCEPTED**. **NOW Stage 5B-1:** existing-owner exact intent→order→fill→bill/fee/funding attribution with no double count, no synthetic NAV, and explicit `INCOMPLETE` for historical lineage/strategy funding/capital-flow gaps. Stage 5B-1 is NOT ACCEPTED until exact CI, Windows install and independent primary-MCP exchange proof. Then revisit Stage 5A full NAV/STOP acceptance → 5C bounded ChatGPT operator snapshot, measured Cloudflare transport and safe Demo↔Production ReadOnly lifecycle → separately authorized Live gate. Historic G3-D1/D2, baseline OPEN/CLOSE and R2 unsafe venue-atomic research are not current tasks.

**Stage 5C-2 historical intent triage (15 October 2026):** Native read-only `account_summary(view="full")` and `view="handoff"` now carry an explicitly bounded `reconciliation.unmatched_historical` list (up to 16 deterministic exact intent IDs, local ledger state, terminality and recorded exchange order ID; total and truncation always reported). It distinguishes an unknown/submitting ledger state from an old local terminal state while **never** treating missing bounded OKX order history as venue absence. The original nonzero historical discrepancy still blocks handoff; triage only exposes which exact historical records require independent evidence. No synthetic resolution or new risk admission is possible from a diagnostic.

**Stage 5C-2 installed release parity (15 October 2026):** A verified agent update is NOT an implicit controller update. The original controller binary drift allowed an unsafe Demo→Production switch, fixed by the PR #338 A/B controller activation and negative physical proof (issue #160). The existing CONTROL `status` now reports `release_parity`: exact installed-agent provenance verified against its binary, active launcher/CONTROL release source tree, current executing controller SHA256 and no in-progress activation. `MATCHED` describes source/version consistency only, NOT an admissible profile switch. Any missing evidence, old controller release or binary/hash mismatch is `BLOCKED`. Existing same-profile recovery and STOP supervision are not disabled by a diagnostic failure. Do not use this observer packet as a one-time fence or an authorization: cross-profile handoff remains denied until agent-owned freeze/quiescence, exact immutable ledger/UID/source-generation proof, single-use CONTROL consumption, crash recovery and poststart Production ReadOnly UID are implemented and physically accepted. In production rollout require the exact controller and agent source tree from the same tested release before requesting an authorized profile transition; no automatic Demo→Production fallback. This release-parity update alone is NOT full Stage 5C-2 acceptance.

**Stage 5C-2 source handoff readiness (15 October 2026):** The one existing `account_summary(view="handoff")` query now reports a bounded *read-only* `okx.profile-handoff-readiness/v1`. It gathers the same single account, OCO, REST histories and WS convergence evidence as `view="full"`, then inspects **every** durable execution intent/mutation/protection in the original executor mutex (not just the first 16 of 45 diagnostics rows). A missing local ledger, nonterminal/UNKNOWN mutation, uncleared protective lifecycle, changed account UID, missing v14 storage audit, incomplete current history, residual position, unknown OCO or mismatched native intent count blocks readiness. The concrete historical discrepancy (three managed intents outside bounded exchange order evidence despite zero positions/orders/OCO) is shown as `MANAGED_INTENTS_OUTSIDE_EXCHANGE_EVIDENCE`, not silently treated as flat.

**Important trust boundary:** even when all read-only predicates pass, status is `SOURCE_QUIESCENT_NOT_AUTHORIZED`, and `handoff_admission="DENIED_NO_CONTROL_BOUND_ONE_TIME_FENCE"`. A transient account snapshot is **not** a one-time signed/native agent fencing ticket. Existing Windows CONTROL `start_demo_acceptance`/`restore_production_agent` must still reject a different profile. Do not bolt a controller change onto an unauthenticated ChatGPT JSON snapshot. A safe actual swap still requires current source owner freeze, single-use PID/generation-bound proof consumed by CONTROL, crash recovery and destination Production-ReadOnly poststart identity verification. No additional agent, daemon, scheduler, transport or live trading is authorized by this preliminary inspection.

**Stage 5C compact account response / latency slice (15 October 2026):** `account_summary(view="trader")` remains the sole typed compact projection of the same authoritative `account_summary`, NOT a second service, cache, trading authority or Demo market feed. Native account history/fills and pending OCO are independent REST GETs. They now execute concurrently through the existing `AccountBootstrapper` and native rate budget **inside the SAME private-WS convergence cursor fence**; either error fails closed before projection. No stale-data fallback, no optimistic `FRESH` or (GO) from shorter wall time. The full response `coherence` and compact `transport` report separately measured account bootstrap, ledger-history, protective-algo, reconciliation, and wall history-read milliseconds. Audit end-to-end Cloudflare latency independently; these are native intervals, not network percentiles. The live acceptance needs a representative request count and measured variance, not one cherry-picked response. Current STOP and `FUNDING_NOT_ATTRIBUTED` must remain `NO_GO`. No change to producer public market source.

**PERMANENT CROSS-STAGE ACCEPTANCE INVARIANT:** «Главный принцип дальнейшей разработки: уменьшать эксплуатационную сложность, а не добавлять новые компоненты». Minimize ChatGPT round trips, payload/context, active owners, recovery branches and opaque conditions. New code must close a demonstrated gap in existing Rust owners with negative regression / exact install, without new process, scheduler, data plane, fallback transport or new development stage. A larger shared OKX account balance is NOT measured strategy NAV; until strategy-specific lineage/PnL is source-complete, deny any unproven new-risk assumption and withhold Live acceptance. Stage 5B's attribution evidence is the prerequisite for final NAV acceptance, not a new stage. Test irreversible STOP in an isolated in-process durable ledger; do not poison the current shared Demo ledger for a test.

**No duplicated Stage-5 environment service:** Finish the already-existing controller's safe source-fenced handoff **inside 5C-2**, together with bounded restart, stop, install and protection continuity; do not start a new pre-G3-D switching subproject or build another process/transport. Existing 5A risk and 5C recovery components are reused, not reimplemented.

## Order of work and responsibility

All work reuses existing crates and the single Windows agent. Stages are a **strict sequential execution/acceptance order**, not parallel work. Stage 4C's supported execution scope is physically accepted, while unavailable protected ADD/HEDGE/partial REDUCE remain explicitly NOT_ACCEPTED. Stage 5A work may proceed on the safe subset, but Stage-4C full-feature acceptance and final Live authorization remain blocked; run the listed Stage-5 items sequentially with explicit terminal proofs. Stage-3 candidate-specific scientific acceptance is required for live strategy authorization, not for building the safe product. For each slice: PR must name the owner, changed files, negative cases, complete CI and installed proof where relevant. Do not add a scheduler, daemon, data service, mutation MCP, alternate executor or generic OKX RPC to deliver this checklist.

| ID / owner | Deliverable and minimal code location | T1–T2: deterministic and fault tests | T3–T5: physical evidence and acceptance | Depends on |
| --- | --- | --- | --- | --- |
| **5A-1 Policy** — `okx-analysis/src/risk.rs`, `okx-execution/src/validation.rs` | Single versioned capital/instrument/strategy and daily-loss/drawdown/open-risk/pending-intent admission policy; explicit treatment of partial coverage and stale evidence | Boundary values, precision, account-day reset, stale/frozen snapshot, external balance/position changes; fail closed before any SUBMITTING | Exact-head CI. On installed Demo, fresh risk evidence yields allow/reject for same intent; rejection causes **zero** exchange mutation | Stage 2 risk semantics |
| **5A-2 Stop and protect** — `okx-execution/src/state.rs`, `executor.rs`, `ledger.rs` | Durable STOP_NEW_RISK; separate cancel-entry vs protected-exit behavior; flatten only with explicit independent authority | Restart does not clear stop; rejected OPEN/ADD/REVERSE; valid risk reduction remains possible; preserved TP/SL and uncertainty do not silently disappear | Demonstrate Demo negative cases with exact IDs and stable ledger; fault/restart proof must not replay unknown mutation; terminal state records which exits survived | 5A-1; Stage 4 protection mechanics |
| **5B-1 Attribution** — `okx-analysis/src/tca.rs`, `okx-execution/src/reconciliation.rs`, `ledger.rs` | Link decision/intent/strategy version to exchange order, fill, bill, fee currency, funding and TCA; compare modelled vs realized | Multiple partial fills, repeated IDs, maker/taker sign, non-USD fees, manual/external orders, missing or truncated history; no double-counting | Reconstruct one already accepted Demo/paper outcome with exact source and artifact references. PASS only when measured residuals reconcile or are explicitly INCOMPLETE, never silently zero | Existing Stage 4C fills/bills; fixtures can precede them |
| **5B-2 Learning review** — `okx-research/src/promotion_lifecycle.rs` and accepted immutable artifact owner | Immutable review tying experiment/promotion version to executed outcome and KEEP, DEMOTE, RETIRE or NEW_VERSION | Tampered/missing family evidence, version rewrite, failed validation or drift cannot promote; no strategy self-modification | Black-box compact evidence links experiment -> eligible decision -> execution -> TCA -> independent review. Insufficient H2 validation remains NOT_PROMOTABLE | 5B-1 + Stage 3 accepted lineage |
| **5C-1 Measured operations** — `okx-agent/src/runtime.rs`, `cloudflare_transport.rs`, `okx-observation` | Bounded typed status for source age/skew, WS generation/gaps, queue pressure, query/reconcile/recovery latencies and account readiness | Inject stale/gapped/out-of-order market and account facts; exceed a declared strategy-specific budget -> DEGRADED/NOT_READY or deny, not optimistic FRESH | Representative real Cloudflare requests with p50/p95/p99, sample count, payload and schema budgets. Do not invent universal time thresholds or confuse transport FRESH with source FRESH | Stage 4D read-only baseline |
| **5C-2 Recovery, ownership, environment choice** — `okx-host-control/src/runtime.rs`, `executor.rs`, `okx-execution` | Restore one owner after process/reboot; prove exact-UID no-unknown flat/OCO source quiescence before one **Demo↔Production ReadOnly** handoff; guard STOP, RESTART and INSTALL when exposure might be unmanaged; independently verify destination profile, account and transport; evaluate OKX Cancel All After (CAA) effect on protective exits | Open/part-filled protected positions, pending/UNKNOWN intents, bad UID, stale/truncated source, corrupt desired state, lost ACK, interrupted install, controller crash/restart and duplicate requests cannot abandon source protection or create a second executor | Full native CI/source equality and verified Windows install; on truly FLAT Demo, independently prove Demo→Production ReadOnly→Demo, postrestart source and venue/ledger consistency, plus negative refusal cases. CAA-safe Demo proof or documented safe alternative; never assert Live approval | Stage 4C exchange/recovery + 5A |
| **5C-3 Release/security** — existing CI, `okx-host-control/src/provenance.rs` and credential owners | Exact source/CI/bundle/binary chain, permission isolation, bounded auth/tool scopes and release-time OKX compatibility review | Reject mismatched artifact/tree/credential environment; old/stale token or disabled gate cannot mutate | CI security/dependency evidence, verified Windows SHA and rollback; no Withdraw credential and no secret emitted to logs | Can run independently |

**Reuse audit before each PR:** Stage 2 already implements hard-risk policy arithmetic, drawdown and daily-realized-loss calculations in `okx-analysis/src/risk.rs`; `okx-execution/src/validation.rs` already tests rejection of risk-increasing OPEN while permitting a reducing CLOSE. `okx-analysis/src/tca.rs` already owns TCA arithmetic, and `okx-research` owns promotion artifacts. The Stage-5 work above means **close missing durable admission, recovery, attribution and physical acceptance gaps**, not implement duplicate formulas or duplicate state owners. For each work item record existing API/tests, the reproduced missing behavior, smallest diff and negative regression before writing code.


## Additional test cases per Stage-5 work ID — strictly after predecessors

This is a **coverage ledger**, not a Live authorization. G3C is physically accepted; G3-D/E/F and G4 must still independently close before Stage-5 acceptance. Already merged source work remains reused, not an additional executor or parallel project.

| ID / status | Smallest remaining deliverable and exact owner | Deterministic/fault test (T1/T2) | Black-box/installed acceptance (T3/T4/T5) |
| --- | --- | --- | --- |
| **5A-3 OPEN** (after #263/#264/#267 source PASS) | In existing `okx-execution` + `okx-agent`, complete bounded admission proof: independent observed account breaches durably stop new OPEN/ADD/REVERSE, while CLOSE/CANCEL and already necessary protective exits remain manageable | Inject UTC-day incomplete/history stale/account-level breach/candidate-only breach; assert only truly observed complete breaches latch durable stop; restart preserves stop; gateway sends for prohibited intents = 0; authorization/mandate mismatch rejects before latch. Explicit daily boundary, multi-intent and corrupted-ledger cases | Exact-head full Windows/Linux CI; installed version hash; Demo negative preflight with exact matched account, zero risk-increasing exchange sends; controlled restart and reducing CLOSE/CANCEL behavior. No physical PASS before actual Demo evidence |
| **5B-1 OPEN** | Same `okx-analysis::tca` and execution ledger: immutable lineage and observed trade/bill/fee reconciliation for a real accepted Demo lifecycle | Duplicate trade/bill IDs, non-USDT maker/taker fee, partial/missed fill, manual account activity, truncated history, unmatched funding return `INCOMPLETE` and no silent zero; reproducible exact decimals | Exact exchange fill/order/bill and risk/reference/strategy evidence, fee VWAP/slippage residuals with declared tolerance and provenance; no claim if 4C-G3 missing |
| **5B-2 OPEN** | Reuse `okx-research` promotion artifact owner for immutable KEEP/DEMOTE/RETIRE/NEW_VERSION review linked to 5B-1 | Missing/tampered OOS, 22<30 H2 validation, lookahead and changed strategy version fail closed; review never mutates strategy/executor; deterministic repeat | Versioned compact review packet references an accepted or explicitly `INCOMPLETE` outcome; positive strategy not required for engineering tests, but no strategy promotion on insufficient validation |
| **5C-1 OPEN** | Reuse `okx-agent`/observation/Cloudflare typed existing read-only path for measured operator reliability without new telemetry process | Stale/lagged/gapped WS, REST fallback, profile crossover, oversized payload, retained diagnostics test as non-current; test bounded reject/recover | Representative repeated calls with sample N, p50/p95/p99 **measured** transport & source age, p95 data acquisition/reconciliation and recovery gap; source quality separate from session freshness; packet normal target ≤12,288 bytes, no invented token statistics |
| **5C-2 OPEN** | One Windows ownership chain + existing executor for crash/unknown result/CAA protection evaluation | Lost ACK after SUBMITTING; duplicate logical request after restart; ledger corrupted; profile handoff while live exposure; CAA cancels entry vs protect exits counterexample all fail safely | Proven exact install/rollback/read-only status; one real bounded nonterminal Demo restart with exact identity reconcile and no orphan; accepted exchange-native CAA proof **or** documented physically tested alternate protection |
| **5C-3 OPEN** | Existing CI/credentials/controller security and exact-tree supply chain; no new authorizing tool | Reject wrong UID/credential/environment/tree/expired approval/disabled live; secret redaction, permissions, snapshot rollback and fail-closed rebuild | Same-source Windows/Linux CI, artifact SHA, CONTROL deploy/recover, primary MCP, no secret output, production live mutation still unavailable |

**Definition of done:** each row: `STATUS (PASS|NOT_ACCEPTED|BLOCKED_EXTERNAL) | code owner/file(s) | exact T1/T2 test and assertion | exact successful PR-head CI & merged-tree proof | installed artifact SHA where relevant | immutable T4/T5 request/order/incident IDs | unresolved risk | operator usability result`. A successful test on `main` with jobs SKIPPED is reuse evidence, not fresh native compilation. Source-complete != installed != Demo-exchange-accepted != strategy-approved != live-authorized.

## Standalone production-write readiness — code proof, NOT permission to trade

Current `okx-execution/src/executor.rs::MutationAuthority` is **Disabled | DemoAcceptance**, and `okx-agent/src/execution_runtime.rs` uses **ReadOnly** for production. `enable_demo_acceptance` correctly rejects non-Demo. Thus **a live-capable admission implementation is still missing**.

**Implementation owner:** the same `okx-execution` authority and agent runtime; define an explicitly scoped *production* admission with account/environment/credential UID binding, single approved mandate and risk policy version, audit identity, expiry/revocation and immediate STOP_NEW_RISK. Keep existing Demo guards. Reuse the exact order mechanics and durable ledger; never make live available by deleting `environment.demo` checks or exposing generic mutation through read-only MCP.

**Operator command ingress (explicit unresolved Live capability):** the existing Cloudflare MCP has one **Demo-only** typed `execution_action` and production is hard `ReadOnly`; it is incorrect to call the entire primary MCP read-only. Before *Live* activation separately prove supported, authenticated, scoped, revocable operator/strategy intent into the **same Rust executor**, with actual connector/platform permission and account/approval evidence. Do not assume Demo tool permission automatically extends to production; if unsupported, Live activation is `BLOCKED_EXTERNAL/NOT_ACCEPTED`. Continuous risk protection and recovery must remain local even when ChatGPT disconnects. Forbid switching away from live exposure unless independent safe supervision is proven.

**Pre-activation tests:** default and after restart remain read-only; bad account/UID/credential/permission, missing approval, revoked/expired intent, stale reference/market/account facts, altered policy, repeated requests and conflict with nonterminal order all cause **zero gateway sends**. Stage 5 policy remains authoritative immediately before send; no operator request may bypass it. Prove deterministic T1/T2, exact-source CI/installed T3, read-only production black-box T4 and fault T5 **without any live exchange mutation**.

**Activation is a separate, future operator decision**, only after Stage 1–5 terminal PASS, an eligible scientifically accepted strategy or explicitly approved bounded canary purpose, least-privilege Trade/no-Withdraw credential and authenticated UID, CAA/alternative decision, fresh risk/preflight and recovery readiness. A canary must have an explicit account/instrument/notional cap and evidence for real order -> fills/bills/fees -> managed close -> reconciled exposure and TCA, followed by an explicit decision to keep new risk disabled or to authorize a separately bounded next step. No CI or Demo acceptance authorizes it.

## Uniform acceptance record (each work ID)

The issue #160 terminal entry is `PASS | NOT_ACCEPTED | BLOCKED_EXTERNAL` and contains: (a) exact tested head and merged source **tree equality**, CI job IDs and result; (b) Windows artifact ID, installed binary hash and controller proof when runtime changes; (c) T1 positive/negative tests and T2 ownership/restart tests; (d) T4 versioned typed primary output or supported physical Demo evidence with environment, hashed account UID and exact bounded IDs; (e) T5 failure/denial/recovery proof; (f) residual gaps and decision. `BLOCKED_EXTERNAL` names an actual unsupported credential/connector/exchange facility and is never PASS.

Stage 5 closes only when every **5A/5B/5C** work item passes its applicable T1–T5 and one reconstructable closed learning-loop result is accepted; live mutations remain disabled. Do not claim platform industrial readiness while the required Stage 4C physical execution proof or Stage 5 record remains open. A positive Stage-3 strategy promotion is a **separate prerequisite for activating that strategy**, not permission to invent profitability or postpone independent execution/governance engineering. The already-existing Demo-only typed `execution_action` is a Stage-4 acceptance interface; Stage 5 must not create a second Demo sender, MCP bypass or parallel agent.


**Next 5B-1 code boundary:** persist at most one immutable virtual reservation in the existing ledger, not a second strategy bank account. Require source-fresh FLAT and zero OCO, native USDT equity and operator authority, reject over-allocation/duplicate reservation, enforce original source/mandate lineage at both preparation and submit while exits remain possible. In the same read-only account reconciliation, derive a closed (FLAT), short-history-window strategy NAV only when every order/fill/bill, source currency and account-equity residual reconciles exactly. Ambiguous funding remains unallocated; open positions require their own mark-to-market owner. This is a **partial 5B-1 functional gate** only, not accepted strategy-specific drawdown/daily-loss STOP or production Live enablement. Exact T1 negative tests, 6-job CI, Windows CONTROL installation and independent Cloudflare MCP non-regression are mandatory.
