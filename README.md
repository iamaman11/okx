# OKX — one Engine, one installed Windows runtime

**Normative specification:** [#448](https://github.com/iamaman11/okx/issues/448) (P0 → P1 → P2; M/T/A; K1–K12; C01–C20).
**Single implementation cursor:** [#449](https://github.com/iamaman11/okx/issues/449).
Issue #398 records historical **read-only** acceptance; it is not a competing roadmap or proof of Real execution.

The product has one installed **`okx-agent` Engine** on `windows-primary` for accounts,
trading, research and finance. Cloudflare Worker provides the authenticated MCP/WSS
gateway and central account risk authority; native Windows CONTROL only owns
install/restart/diagnostics. GitHub CI builds signed, version-addressed release
artifacts. No separate Demo Agent, research benchmark executable, second financial
ledger, alternate transport or bypass for real orders.

## Modules and responsibility

| Module | Owns | Does not own |
| --- | --- | --- |
| `crates/okx-agent` | One runtime and request orchestration | A second venue execution state machine |
| `crates/okx-agent/src/analytics/market.rs` | Market use case, source reads and market computations | Trading decisions and account SQL |
| `crates/okx-agent/src/analytics/result.rs` | Verified immutable result reads, page cursors and export integrity | Research CPU execution or SQL mutations |
| `crates/okx-agent/src/analytics/{jobs,executor,budget,benchmark}.rs` | SQLite research jobs, shared CPU/memory admission, in-Agent K9 diagnostics | A separate benchmark service |
| `crates/okx-research` / `crates/okx-analysis` | Replay, research artifacts and analytics formulas | Transport permissions or exchange orders |
| `crates/okx-execution` | Account-owned order facts, fills and SQLite finance | Worker risk authority or display totals |
| `cloudflare/okx-mcp` | Public MCP schema, routing and central risk authority | A second native trading engine |
| `crates/okx-host-control` | Verified GitHub artifact install, lifecycle and Windows diagnostics | Research/strategy calculation or venue execution |

**Market:** `market` MCP → Worker/WSS → Agent `AnalyticsService.market`
→ K7 planner → `analytics/market.rs` → bounded result and immutable artifact.

**Research:** `research.validate/grid/backtest/benchmark` MCP → Agent
`AnalyticsService` → shared SQLite jobs and CPU/memory admission →
replay → `analytics/result.rs` summary/detail/export. The K9 fixed fixture
runs inside this same Engine; CI microbenchmarks are not physical acceptance.

**Trading:** an authorized `trade` MCP action → Worker risk authority →
the single Agent trade service / account SQLite → exchange API; reconcile
the venue outcome before changing financial state. External restrictions
are reported, never hidden behind strategy or read-only actions.

## Developer map for the first bounded real trading campaign

There is **one ownership chain**, not a controller for each product feature:

`Account/Source → Strategy Decision → Intent/SQL → RiskBook RESERVE →
BEGIN_POST → single venue POST → order/fill/bill finance →
NAV/STOP → protected position/terminal settlement → release`.

- **Adding an exchange capability:** normalize its authenticated source
  in the venue adapter and declare which order, fee, margin, FX, history
  and protective-order proofs it actually supports. An unsupported proof
  returns a typed reason, **never zero**. No direct Worker money calculator.
- **Adding a strategy:** emit the existing typed Decision/Intent and use the
  same SQL-owner, RiskBook and post-once execution path. Strategy budget is
  a risk policy, **not** newly created account capital.
- **Adding finance evidence:** write immutable venue fact + provenance into
  the single account SQL owner, with exact cursor/replay rules. The native
  `EntryCostProof` either stays `Unknown` or persists a quote-bound
  `ENTRY_ADMISSION_PROVEN` operation receipt *before* central `RESERVE`.
  After a terminal verified venue readback, the SAME SQL owner links
  fills to final fee bills, recomputes a conservative cumulative USD
  charge from the original source-bound FX upper, and persists
  `ENTRY_SETTLEMENT_PROVEN`. The existing WSS `SETTLE_ENTRY` moves that
  amount from reserved to consumed before `Resolve(RELEASED)` returns
  unused reserved capacity. `RELEASED` must exactly match the ALREADY
  settled amount, revision and digest; it cannot invent a new financial
  debit. Denied terminal proofs do not change the loss/entry budgets
  or advance RiskBook CAS. Lost replies replay the original receipt.
  A known `notional + fee` quote alone never proves margin/borrow/FX.
  `trade.status` reads the source-bound SQL admission and labels it as
  cost proof **only**, never POST permission. T0 creation and v2 reopening
  install the same immutable SQL receipt triggers; an ACK retry rechecks
  the originally covered fill/bill cut-off and content digest even after
  history cursors/generations advance. Capture watermarks remain mandatory
  coverage checks but are not part of the hash of immutable economic facts. With missing opening campaign coverage or an
  Unknown cost component, the Engine still fails closed.
- **Campaign opening history:** native fills and bills capture reject a
  history page whose beginning is outside OKX's last-three-calendar-months
  source window (including the boundary safety margin). An empty expired
  page is **NOT** evidence of zero expense. Existing `AccountHistoryApi`
  provides independently paginated *recent* and *archived* order-history
  sources: a single 100-row read or one archive alone is never complete
  order history. To install campaign opening, source coverage must run
  from independently proven actual campaign inception to a common cut-off
  over every managed real account. The on-demand typed
  `AccountHistoryApi::complete_campaign_orders` now exhausts BOTH order
  sources with a bounded page limit, verifies order scope/causality and
  returns a canonical content digest, only when even canceled-without-fill
  orders remain provable (under two hours with a margin). Equal-time
  contradictory economic facts from different archives are rejected.
  Crash recovery now uses the same complete order history, with separate
  exhaustive fills history for all instrument types and the unfiltered
  account-wide bill archive. Every fill must have its original fee bill
  with matching currency and economic identity; recovery also binds fills
  to the exact observed orders. Backups older than the provable venue
  horizon cannot establish absence of a lost POST. These facts are
  content-addressed venue evidence, NOT a certified USD expense or
  independently proven campaign inception. The native API now exposes
  one `CompleteCampaignAccountHistory` from independently exhausted recent
  and archive orders, instrument-partitioned fills, and account-wide bills
  at a common cut-off. It verifies exact order/fill quantity and bill/fee
  identity without creating a second ledger. A fresh authenticated Worker
  governance initialization records `campaign_origin` atomically in its
  existing RiskBook and exposes it through the existing typed native
  `SUMMARY`. The Rust protocol checks campaign identity, installation
  and causal opening timestamp. Legacy campaigns lacking the original
  inception receipt remain `NOT_PROVEN`, never receive a fabricated date. For older
  campaigns and recovery, sources fail closed pending independent proof.
  A complete native all-managed-account USD expense importer is still not
  connected, so RiskBook leaves admission blocked. The existing native
  RiskBook SUMMARY exposes authenticated campaign inception, but the
  historical venue content digest is deliberately NOT a USD debit proof.
  Authenticated OKX history timestamps and monetary amounts are parsed from
  their original string wire fields; native test fixtures preserve that wire
  representation. A malformed history field rejects the source instead of
  fabricating an opening campaign budget.
  Opening imports additionally require currency/contract valuation for
  every observed economic event, the SQL owner and all-managed-account
  aggregation. No unknown source may become `consumed_usd=0`.
  Missing all-in FX/margin/borrow sources now reject the prepared SQL intent
  as `ENTRY_ALL_IN_COST_UNPROVEN` BEFORE any central RESERVE attempt: known
  quote remains readable, and no orphaned risk marker can be created.
- **Handling net-mode positions:** exchange `posSide=net` uses a signed
  position quantity, while native SQL lots use `long` / `short`.
  `trade_service/reconcile.rs::verified_venue_position_direction` is the
  shared proof for status, pre-POST cohort and recovery. A different or
  malformed direction cannot be interpreted as flat. The same module's
  `verified_live_protection` binds each protection to actual venue
  side/size/trigger; recovery must read the *live* algo, not only its
  stored SQL receipt.
- **Diagnosing a denial:** inspect one `trade.status` or bounded
  `diagnostics.explain(view=finance)` result and its source/reason.
  Typical owners: `ENTRY_*_UNPROVEN` = native venue/SQL input;
  `COHORT_*` / `PROTECTION_*` = native account/venue census;
  `CAMPAIGN_*` / `ENTRY_SETTLEMENT_*` = central Worker RiskBook;
  `ACCOUNT_STORAGE_*` = sole SQLite owner. Never reset financial facts
  or reissue a POST merely to turn a denial into PASS.

**P0.2 release rule:** native quote, order protection and synthetic Worker
tests are separate evidence. A green CI proves the exact commit only; it
does not prove a campaign opening, all-in USD entry upper, protected
actual position, 600-second live financial window, or a real venue trade.
Unknown source coverage remains **NO-GO** until the source owner can
prove it. Existing standard lifecycle is CI → protected merge → CONTROL
Windows install → operator Cloudflare deploy → parity/PHYSICAL-F → bounded
real acceptance. No new scheduler, separate ledger or diagnostic service.

## Build, tests, and diagnosis

From the repository root on an installed Rust/PowerShell development machine:

```powershell
cargo fmt --all -- --check
python scripts/check_architecture.py --self-test
python scripts/check_architecture.py
python scripts/check_product_contract.py
cargo clippy --locked -p okx-agent -p okx-host-control -p okx-host-launcher -p okx-execution --all-targets -- -D warnings
cargo test --locked -p okx-agent -p okx-host-control -p okx-host-launcher -p okx-execution
```

On Linux the same canonical CI also checks portable crates and the Worker
contract; see `[.github/workflows/ci.yml](.github/workflows/ci.yml)`.
No production credentials are needed for the local fixture/unit tests.
Run public read-only diagnostics with the authenticated `diagnostics` MCP tool
or the existing typed `status` request to Windows CONTROL issue #12; follow
the returned request ID and source reason, not a manual SQL inspection.

**Known architectural gap, NOT_ACCEPTED:** the current architecture guard
checks direct local-crate dependencies, not transitive purity. In particular
`okx-analysis → okx-observation → okx-api` still couples analytics to API
types. #448 K10/C20 explicitly requires closing this gap and exercising
extension paths; a passing CI architecture job does not yet prove that target.

**Release evidence:** `main` merge and CI PASS are code proof only.
Physical acceptance additionally requires the *same* installed Agent/Worker
Git tree, native CONTROL binary SHA, MCP/WSS readback and scenario evidence
in #449. Do not initialize the account SQL ledger or reset campaign limits
to resolve a deployment mismatch. K9 workload and Real venue lifecycle are
NOT_ACCEPTED until their independent physical receipts exist.

[Architecture](docs/ARCHITECTURE_CONTRACT_V1.md) ·
[Operations and lifecycle](docs/PRODUCT_ACCEPTANCE.md) ·
[ChatGPT operator rules](docs/chatgpt-operator-contract.md)
