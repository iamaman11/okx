# okx

Rust/Tokio OKX observation, analysis and execution-safety platform for the current Windows + GitHub environment.

The production architecture is deliberately small: one Windows supervisor chain, one observation runtime, one mutation owner, typed encrypted access, and hosted-CI artifacts as the only normal deployment authority.

## Product charter — permanent; not a parallel roadmap

**Goal:** one simple, understandable, modular, industrial OKX application for analysis and trading that ChatGPT can operate end-to-end **without manual user-side commands, guessing account state or blindly resending an uncertain order**. Demo is an exchange acceptance environment for **the same Rust executor intended for future real trading**, never a second application. Production remains **ReadOnly** until the independent explicit Live gate after Stage 5.

| Existing single owner | Responsibility; no replacement or duplicate |
| --- | --- |
| `okx-observation` | Fresh, coherent, attributable market and exact-account truth |
| `okx-analysis` | Deterministic risk, calculations and transaction-cost analysis (TCA) |
| `okx-execution` | Sole trade/risk-mutation owner, durable intent/ledger, protection, exact exchange reconciliation and **non-replay after UNKNOWN** |
| Windows `okx-host-control` + one agent | Native single-process ownership, lifecycle, verified installation and recovery |
| Cloudflare MCP | **Primary** typed ChatGPT data and permitted Demo-trade transport; no second execution owner |
| GitHub | Canonical code, CI, exact verified releases, typed CONTROL and encrypted read-only diagnostics/parity; **never** a workaround for a primary-tool refusal |

**PERMANENT INVARIANT:** «Главный принцип дальнейшей разработки: уменьшать эксплуатационную сложность, а не добавлять новые компоненты». Every Stage 5A–5C feature must reduce or at least not increase the operator's steps, ChatGPT context, failure modes or recovery burden. Reuse established owners and typed compact evidence; no separate service, mutation fallback, parallel executor or speculative refactor. This is a functional acceptance gate, not an extra stage.

**No code for code's sake:** no Demo fork, new watchdog/scheduler/daemon/polling loop, competing API/RPC, risk engine or execution owner. First identify an observable missing capability or defect, reuse an existing module, make the smallest safe change, test failure paths, prove exact CI and installed artifact, physically verify Demo exchange results if relevant, and **close the work item** before moving on. Existing protections cannot be removed to make a test pass. ChatGPT must use bounded status/decision packets with account UID/profile, source freshness, risk, unresolved intents and next safe action; never depend on a long chat history as the ledger.

**Exactly five product stages. ONE current critical path:** Stages 1–3 closed within documented scope → **4C-R1** durable FLAT/OCO ledger cleanup → **4C-R2** risk-oracle mark/time basis, explicit risk admission, market FRESH, OCO race bound → **4C-D** physical protected FOK OPEN/ADD/HEDGE/full exits → **4C-E** safe REDUCE/REVERSE or typed UNSUPPORTED → **G3-F/G4** lost ACK/crashes/non-replay and Windows recovery → **Stage 5A** portfolio risk governance → **5B** accounting/TCA/science → **5C** operator usability, exact deploy/restore, safe Demo↔Production ReadOnly → separately human-approved **Live gate**. See one canonical [#160 AC-01…AC-10](https://github.com/iamaman11/okx/issues/160), not another roadmap.

**Anti-loop / simplicity contract:** each reproducible invariant -> one same-owner causal change -> negative/race regression -> one coherent PR + six CI checks -> exact verified install if runtime changed -> physical Demo venue proof when applicable -> terminal PASS / FAIL-BLOCKED / UNSUPPORTED. After two unsuccessful bounded root-cause attempts, record a precise blocker/native unsupported premise, not a new transport/refactor. Keep one agent, executor, ledger, MCP Demo write ingress and Windows owner. Large files split only where it clarifies an actual changed responsibility; no code for code's sake.

**One operational cursor (2026-10-14):** Canonical [#160](https://github.com/iamaman11/okx/issues/160) is the acceptance authority. [PR #323](https://github.com/iamaman11/okx/pull/323) is MERGED, six-job CI and postmerge SUCCESS, Windows CONTROL exact artifact 11863333842 installed/provenance VERIFIED; read-only MCP exposes immutable archives b/c with stable hashes. Active Demo run d remains 100-USDT FLAT with zero positions/orders/OCO; strategy new-risk NAV remains `usable_for_risk=false`. **Current market provenance acceptance slice:** label existing Demo public REST (`x-simulated-trading: 1`) and `wspap` data explicitly as SIMULATED in market overview, microstructure and research, with production OI comparability `NOT_ATTESTED`; DO NOT scale the anomalous Demo OI number or relabel it production. Next: Stage 5A durable marked strategy NAV/high-water/daily-loss STOP and Stage 5C compact measured decision packet. Unwitnessed >2h source gaps and unowned funding remain INCOMPLETE; no new service, agent, market transport or Live authority. Historical sections below are superseded by #160.

## Current production state

Accepted:

- M1 reference data — PASS;
- M2 public REST market state — PASS;
- M3 persistent public WebSocket — PASS;
- M4 private read-only account/order state — PASS;
- M5 deterministic cost/risk/scenario analysis — PASS;
- M6 bounded application/research query surface — PASS;
- H1 GitHub transport/context hardening — PASS;
- A2 reboot + real external network-loss recovery — PASS;
- Phase 2 pre-enable execution boundary — PHYSICAL PASS with live writes hard-disabled;
- Stage 1 TRUTH — ACCEPTED/CLOSED;
- Stage 2 INTELLIGENCE + RISK — ACCEPTED/CLOSED on account-mode-aware Futures risk evidence;
- Stage 3A DATA FOUNDATION — ACCEPTED/CLOSED;
- Stage 3B DETERMINISTIC REPLAY KERNEL — ACCEPTED/CLOSED on primary MCP replay + restart/determinism + fail-closed evidence;
- Stage 3C SCIENTIFIC VALIDATION & PROMOTION — ACCEPTED/CLOSED as infrastructure; H1 `close_momentum` was scientifically REJECTED;
- H2 `two_bar_momentum` — development screen PASS, frozen for a fresh independent 96h FINAL_OOS tracked in #216;
- Stage 3D immutable promotion gate — ACCEPTED;
- Stage 3D causal live-decision parity kernel — ACCEPTED;
- Stage 3D bounded event-driven PAPER/SHADOW session owner — research implementation; independent H2 promotion remains gated by #216.

**Supported execution baseline (2026-10-12):** installed main through [PR #302](https://github.com/iamaman11/okx/pull/302), 6/6 CI, exact CONTROL rollout, Cloudflare MCP Demo PASS/FRESH; four physically verified protected FOK OPEN→CLOSE round-trips = **8 exchange FILLED orders** on BTC/ETH/DOGE. Independent account FLAT, 0 ordinary orders, 0 active OCO, 0 unresolved intents, consistent ledger; historical protections CLEANED_UP. This baseline is **ACCEPTED** and must not be repeatedly re-tested for unrelated changes. See the authoritative [#160 current cursor](https://github.com/iamaman11/okx/issues/160) for precise source tree, CI, installed hash and more recent evidence.

**Stage 4C R2 venue decision:** no documented guarantee that an existing exchange-native OCO cannot autonomously trigger between an exact REST GET/private-WS fence and a new protected ADD/HEDGE FOK. Local mutex and WebSocket ACK are not exchange atomicity. Therefore **protected ADD/HEDGE is explicitly UNSUPPORTED at prepare and submit**, without any exchange POST or new intent; the corresponding *functional* acceptance remains **NOT_ACCEPTED**, not a simulated PASS. No alternative unsafe trading route, never cancel a sole live SL to make the test pass.

**Accepted Stage 4C-E (2026-10-12):** [PR #304](https://github.com/iamaman11/okx/pull/304) passed exact six-job CI, merge-tree equality, native Windows install and physical Demo serial LONG → FLAT → linked SHORT with exact OCO; final Demo FLAT/zero OCO and ledger reconciliation PASS. Partial protected REDUCE in Hedge mode and ADD/HEDGE with existing OCO remain explicitly UNSUPPORTED, not functionally accepted. Existing fenced `shrink_protection_size` is repair for independently already-shrunken exposure, not an atomic REDUCE primitive. Ordinary reducing CLOSE AMEND remains accepted. [Authoritative details #160](https://github.com/iamaman11/okx/issues/160#issuecomment-6117035371).

**Accepted G3-F/G4 supported-scope evidence (2026-10-12):** [PR #305](https://github.com/iamaman11/okx/pull/305) exact six-job CI, tested=merged tree, Windows verified install, real agent kill/restart at FLAT and while BTC 0.01 was protected by the same native OCO; complete final FLAT/zero OCO/ledger PASS. Real lost-ACK exactly after venue POST remains **not physically proven**, but durable no-replay is tested by the same Rust executor. [Full G4 proof](https://github.com/iamaman11/okx/issues/160#issuecomment-6117330353). Full Stage 4C all-feature approval remains NOT_ACCEPTED because protected ADD/HEDGE and partial REDUCE have no accepted safe venue-atomic path.

**5A budget gate ACCEPTED:** [PR #306](https://github.com/iamaman11/okx/pull/306), six-job CI / verified Windows agent and read-only physical boundary: $100 allocated capital, 2x, $40.02 candidate violates 20% margin; $40.00 meets the exact margin limit. Admission is checked at prepare and again at submit.

**CURRENT ACCEPTANCE CURSOR — Stage 5B-1 after verified merged/installed [PR #315](https://github.com/iamaman11/okx/pull/315); Stage 5A full NAV/STOP NOT_ACCEPTED:** account equity is not independently attested strategy NAV. A larger shared Demo account must not silently prove the small mandate's zero drawdown: reject candidate risk with `STRATEGY_NAV_UNATTESTED` instead of optimistic account-equity drawdown; preserve existing margin arithmetic and risk-reducing exits. This is a conservative missing-evidence gate, **not** a strategy NAV calculation or full Stage 5A acceptance. The existing durable STOP_NEW_RISK must survive restart, block all prepared new-risk POSTs and leave a legitimate CLOSE usable once. Tests may stage synthetic daily breaches in a temporary ledger; never permanently latch the active common Demo ledger just to manufacture a demonstration. True strategy NAV/fill/bill attribution requires Stage 5B, so final Stage 5A/Live remains NOT_ACCEPTED until independently proven. No new service or transport. Stage 5B then 5C.

**Stage 5B-1 active same-owner event-driven continuity — draft [PR #317](https://github.com/iamaman11/okx/pull/317):** after merged/installed #316 (6/6 CI, exact verified Windows agent and Worker contract 2026-10-13.2), authenticated private-WS nonempty financial updates are wired to the existing Windows financial source owner: one fresh observer REST snapshot, complete order/fill/bill history and two WS generation fences before the single ledger appends exact immutable evidence. No scheduler, additional service, polling loop or trading POST. This is an event-triggered capture improvement, **not proof** of all quiet-period windows, missing final bills after a sole WS event, eventual >16 archived runs, or completed strategy-specific NAV/STOP. Keep 5B-1 NOT_ACCEPTED until those failure cases and physical full Demo run/restart are proven.

**Stage 5B-1 verified code/installation milestone:** [PR #315](https://github.com/iamaman11/okx/pull/315) was MERGED, 6/6 PR CI and postmerge CI SUCCESS, native Worker deploy SUCCESS, source tree `7f426523efeab160452eaff66fedbe03395cdd6e`, Windows CONTROL exact artifact `11837773644` installed SHA-256 `dee19b2c57e85be180b26e20f9afb6141b00d0b24530a3f188a7d1b4d409fee8` and independent status PASS. The existing ledger v9 stores exact run-owned order/fill/bill evidence, per-run financial projections and an immutable FINALIZED archive, and the sole Demo executor exposes native finalization. This is **not full Stage 5B-1 acceptance**: physical BEGIN→OPEN→CLOSE→FINALIZED→NEXT/restart, independently durable open NAV/highwater/STOP and gaps in unattended source capture remain unresolved. [PR #316](https://github.com/iamaman11/okx/pull/316) corrects MCP tool-schema versioning and incomplete-source checkpoint poisoning and exposes bounded continuity/capacity diagnostics; no new worker, service or trading owner. Production ReadOnly and Live OFF.

**October 13, 2026 API evidence boundary:** Exact Demo order/fill/bill IDs and fees can be read and independently reconciled, but history is bounded (in particular, canceled unfilled orders age out after about two hours), funding bills need not prove a unique strategy/position-side owner, and account equity is not strategy NAV. The one ledger must retain verified ownership/cursors without retroactive backfill. The Stage 5B no-NAV new-risk rule applies for account equity above, equal to, **or below** declared strategy capital until a source-attested per-run NAV is actually connected; this is not a permanent daily-loss STOP and must not block legitimate exits. The current ChatGPT `execution_action` schema now exposes `begin_strategy_run`; no physical run opening was requested because the durable run accounting/finalization is not yet accepted. The next functional proof binds new intents to the ledger-owned allocation ID; missing old/foreign lineage is NOT retrospectively owned. Canonical acceptance remains [#160](https://github.com/iamaman11/okx/issues/160).

Live trading is **not enabled**. One eventual user choice is **Demo / Real (Production ReadOnly)**; choosing a trading environment never grants real-money execution authority. [PR #283](https://github.com/iamaman11/okx/pull/283) merged source-only deny-by-default profile transition guards, **not yet installed or proven as a complete safe toggle**. Existing lifecycle shutdown/restart/install can still interrupt supervision with open exposure, so do not claim lifecycle safety from CI. A later Stage 5C-2 proof must cover fresh exact-account/ledger/OCO guard, single-owner handoff and negative crash/recovery cases; stay on the accepted Demo profile for G3-D1/D2/D3. The single typed Demo-only `execution_action` uses the same `okx-execution` owner; Production remains ReadOnly.

## Architecture

```text
GitHub-hosted CI
  -> locked Rust dependency graph
  -> Linux + Windows CI
  -> exact tested Windows bundle + manifest + SHA-256
  -> verified deploy / durable recovery release

Windows Task Scheduler
  ONE TimeTrigger / PT1M / StartWhenAvailable / IgnoreNew
        |
        v
ONE fixed Scheduler entrypoint: C:\okx-control\okx-host-control.exe
  immutable launcher binary after one-time root migration
  content-addressed controller selection
  transactional activation + rollback
        |
        v
ONE versioned okx-host-control.exe
  desired state + mutex + Job Object
  fixed typed CONTROL operations only
        |
        v
ONE okx-agent.exe
        |
        +--> Reference / Market / Account / Orders
        |      -> reconciliation / readiness
        |      -> immutable snapshots
        |
        +--> pure deterministic Analysis
        |
        +--> ONE production-disabled OrderExecutor
        |      -> durable mutation ledger
        |
        +--> encrypted typed DATA mailbox
```

## Ownership

- `okx-api`: typed OKX REST/auth/exchange primitives only;
- `okx-ws`: WebSocket protocol/transport only;
- `okx-runtime`: Tokio WS lifecycle/reconnect/subscription ownership;
- `okx-observation`: normalized reference/market/account/order state, reconciliation and readiness;
- `okx-analysis`: pure deterministic Decimal/fixed-point analysis;
- `okx-execution`: sole mutation owner, idempotency, durable ledger and UNKNOWN_SUBMISSION recovery;
- `okx-agent`: composition/query/access adapter only;
- `okx-github`: GitHub transport primitives;
- `okx-protocol`: versioned DATA/CONTROL contracts;
- `okx-host-launcher`: immutable one-shot controller root-of-trust; hash verification, activation and rollback only;
- `okx-host-control`: Windows lifecycle/deploy/diagnostics only.

No observation, analysis, transport or host-control component may directly become a trading engine.

## DATA and CONTROL

Normal ChatGPT product/research queries use the primary Cloudflare MCP path:

```text
ChatGPT -> OAuth MCP Worker -> Cloudflare RuntimeSession
        -> authenticated outbound Windows WSS -> okx-agent
        <- bounded typed AgentResponse <-
```

The direct path is generation-bound, heartbeat-probed and bounded by frame size, in-flight concurrency and response deadline. The Worker and Rust owner admit only the explicitly typed `execution_action` on an authenticated `demo_acceptance` profile; all production writes remain rejected. A healthy MCP session alone does not prove tool-schema freshness, account source freshness or permission for a specific mutation. GitHub DATA (#10 protocol; #234 historical Demo acceptance mailbox) remains an encrypted **read-only diagnosis/parity** path, NOT a fallback trading-write ingress:

```text
ChatGPT -> encrypted typed request -> GitHub #10 -> okx-agent
        <- encrypted terminal result <-
```

CONTROL #12 is the typed Windows lifecycle/deployment transport:

```text
ChatGPT -> fixed allowlisted control request -> GitHub #12 -> okx-host-control
        <- bounded typed result <-
```

CONTROL has no arbitrary shell/PowerShell/HTTP/path execution surface. Legacy remote `BuildAgent` is protocol-compatible only and fails closed; normal production deployment is verified hosted-CI artifact deployment.

Both transports have physically passed restart and real external GitHub/network-loss recovery. Controller replacement is being closed under #126: successful PR-CI bundle -> immutable content-addressed version -> durable CONTROL ACK -> unchanged Scheduler invokes the fixed launcher entrypoint -> exact old-process identity exit -> readiness proof -> commit or automatic rollback. The Scheduler definition is unchanged even by the one-time root migration.

## Account and execution boundary

Production account target: standard sub-account `Succession`.

- observer credential: Read only;
- executor credential: Read + Trade;
- Withdraw: never;
- Futures account mode;
- long/short position mode;
- executor IP binding: optional diagnostic evidence, not a production acceptance requirement;
- credentials remain outside Git and public GitHub plaintext.

A prepared execution plan is not a timeless permit. Any future live send must reacquire authoritative reference/account state and pass exact pre-send continuity checks.

Live-write enablement is a separate future authorization only after Stages 1–5 pass and the explicit final live-activation gate is approved.

## Supply chain

- Rust toolchain: 1.95.0;
- workspace `Cargo.lock` is committed;
- production CI/build paths use `--locked`;
- GitHub Actions used by canonical CI/account acceptance are pinned to immutable commit SHAs;
- successful PR acceptance may be reused after merge only when merged tree exactly equals the tested PR head tree;
- durable production recovery bundles are promoted to versioned GitHub Releases after exact-tree + manifest + binary-hash verification;
- normal local Windows build/install is not a production deployment authority.

`main` branch protection is enabled with required `classify`, `linux-core`, and `windows-native` checks; tested-tree/merged-tree equality remains the artifact reuse invariant.

## Windows filesystem boundaries

```text
C:\okx          mutable canonical Git workspace
C:\okx-control  immutable launcher + versioned controllers + activation state
C:\okx-runtime  installed agent + runtime state/logs/staging
C:\okx-upgrade  temporary bootstrap/upgrade staging only
```

Installed production binaries do not execute from the mutable source checkout.

## Canonical documentation

- [Architecture](docs/ARCHITECTURE.md)
- [Roadmap index (authority #160)](docs/ROADMAP.md)
- [Encrypted GitHub mailbox](docs/encrypted-github-mailbox.md)
- [ChatGPT operator contract](docs/chatgpt-operator-contract.md)

Canonical issues:

- #160 current industrial roadmap and acceptance authority;
- #216 deferred frozen H2 fresh 96h independent FINAL_OOS;
- #113 historical production-readiness closure baseline;
- #5 product/domain architecture;
- #7 Windows runtime/deployment;
- #10 encrypted DATA transport protocol; #234 bounded Demo DATA acceptance mailbox;
- #12 CONTROL transport;
- #3 execution boundary;
- #47 capability matrix;
- #126 immutable controller launcher/root-of-trust — ACTIVE until one-time root migration plus remote commit/rollback/reboot physical acceptance;
- #54 controller self-update — absorbed by #126 for final lifecycle closure;
- #58 mailbox compaction — trigger-based deferred maintenance.


### Windows desktop behavior

The production Windows control plane is intentionally background-only. The fixed Task Scheduler
entrypoint is a Windows GUI-subsystem launcher and all controller-owned child processes use
`CREATE_NO_WINDOW`. Normal polling, controller recovery, agent lifecycle, diagnostics, and
verified launcher-root upgrades must not create console or PowerShell windows on the interactive
desktop. Launcher-root upgrades are verified against an exact successful PR-CI artifact and the
expected current launcher SHA-256; they never mutate the Scheduler definition.


**Stage 5B-1 exchange proof:** #308 merged/installed (CI six jobs, exact tree and primary Demo read-only account summary). It proved 8 fill/bill links, but 3 funding bills are unallocated and 22/35 legacy intents have no original lineage; never equate the linked partial trade totals to a strategy NAV. The next functional gate cross-checks native fill `billId`, fee currency/amount and `fillPnl` against account bill `billId`, `fee`, `ccy`, `pnl`; mismatches are `INCOMPLETE`, funding without position-side ownership is unallocated, starting capital and external cashflows remain unattested. No extra data plane or trade engine.


**5B-1 provenance boundary after #309:** source-exact venue fees/PnL are verified only for 8 original lineage-bound fills; 35 durable intents do not identify one independently capitalized strategy. Immutable decision origin schema/version and the original mandate are evidence cohorts, not proof of capital ownership. New same-stage correction groups financial evidence by origin/mandate, with explicit refusal for truncated order/fill/bill histories. Funding 173/174 lacks independently proven strategy-side identity. Declared TradingMandate capital is a risk budget, not a funded allocation. No artificial strategy NAV or Live admission.


**Stage 5B-1 current bounded capital implementation (PR pending):** the existing execution-ledger v7 adds a single immutable, account-UID-scoped virtual USDT reservation, with fresh opening source, operator-authority identifier, no active orders/positions/native OCO, capital <= observed original native USDT equity, no double allocation and exact lineage/mandate gate at prepare and submit. This does NOT move funds, equalize USD to USDT, claim independent strategy cash ownership or enable Live. The existing account observation exposes native per-currency USDT equity to the read-only reconciliation. A new *FLAT-only*, short source-window NAV projection requires exact source-complete intent/order/fill/bill links, no foreign bills, no funding ownership heuristic, no non-USDT balances, no residual account-equity cashflow; otherwise `INCOMPLETE`. This is NOT an open-position NAV, time-series drawdown proof or Stage-5A admission gate. Risk remains fail-closed and no automatic reservation activation or new executor was added.


**Stage 5B-1 native-run admission follow-up (October 13):** the same authenticated Demo-only `execution_action` gains a narrowly typed `begin_strategy_run` administrative ledger operation. The caller chooses an allocation ID, origin, mandate, native-USDT amount and an operator-reference ID; the Windows runtime itself independently reads the fresh account, read-only order/fill/bill history, private-WS continuity and all pending SWAP/FUTURES OCO/conditional orders. No caller-provided FLAT, coverage or balance flags are accepted. The existing executor mutex then writes a single immutable reservation, rejects another reservation, and preserves the existing account-wide STOP. **This does not transfer capital, prove strategy cash/NAV or enable Live/new-risk entitlement.** The current NAV projection remains source-bounded/FLAT-only with `usable_for_risk=false`. Full owned mark-to-market, durable financial checkpoints and physical NAV/STOP acceptance remain open. Do not invoke an irreversible new Demo allocation until independently verified initial currency and operator authority are supplied.
