# MCP action evidence register — 2026-11-28

Source baseline: [`main` `16888173684e`](https://github.com/iamaman11/okx/commit/16888173684efcada4933df2a73fc2faa3a3f3e0), tree `111764096e09bc9ae5d85999eb13afa578501b7e`. This is a **snapshot of evidence**, not a release acceptance certificate. Change the evidence values only after recording new evidence; the CI roster check merely prevents actions from being added or removed silently.

| Column | Evidence meaning |
| --- | --- |
| Code | `Y`: typed public schema and dispatch exist in this source tree; **not** a runtime pass. |
| CI | `S`: positive/negative strict-schema fixtures run in `execution_tools.test.ts`. `F`: additionally, Worker → fake DO mutation route has both success and refusal in `mcp.test.ts`; **no exchange POST**. These are included in the successful Ready [#5293](https://github.com/iamaman11/okx/actions/runs/46091944154); merged-main [#5294](https://github.com/iamaman11/okx/actions/runs/46092428162) reused its identical tree. |
| Installed | `E`: production Edge matches this tree; Edge-owned governance store needs no changed Windows binary. `R`: Edge and Research runtime are source-compatible with this tree (Research-only diff is test code). `D`: Engine or CONTROL binary is **older** than this tree, so current source is not physically installed. `M`: depends on selected diagnostics view, and includes older owners. |
| Live | `Y`: positive call observed in production; `0`: empty-state success only; `P`: both fresh and partial outcomes were observed; `—`: not observed / not accepted. A read of an existing job is not proof that a new run or a mutation works. |
| Negative | `S`: schema malformed/unknown-field refusal in CI only; `F`: also fake DO owner-refusal route; `L`: specific live negative calls observed (in addition to schema tests). **None** means a destructive production negative was exercised. |

| Action | Code | CI | Installed | Live | Negative |
| --- | :---: | :---: | :---: | :---: | :---: |
| `accounts.list` | Y | S | E | 0 | S |
| `accounts.status` | Y | S | E | — | S |
| `accounts.connect` | Y | S+F | D | — | S+F |
| `accounts.pause` | Y | S+F | E | — | S+F |
| `accounts.resume` | Y | S+F | E | — | S+F |
| `accounts.set_limits` | Y | S+F | E | — | S+F |
| `strategies.list` | Y | S | D | — | S |
| `strategies.status` | Y | S | D | — | S |
| `strategies.create` | Y | S+F | D | — | S+F |
| `strategies.update` | Y | S+F | D | — | S+F |
| `strategies.restore_configuration` | Y | S+F | D | — | S+F |
| `strategies.start` | Y | S+F | D | — | S+F |
| `strategies.stop` | Y | S+F | D | — | S+F |
| `strategies.capital_assign` | Y | S | D | — | S |
| `market.screen` | Y | S | R | — | S |
| `market.compare` | Y | S | R | — | S |
| `market.instruments` | Y | S | R | Y | S |
| `market.snapshot` | Y | S | R | — | S |
| `market.microstructure` | Y | S | R | — | S |
| `market.capabilities` | Y | S | R | Y | S |
| `research.record_decision` | Y | S | R | — | S |
| `research.decision` | Y | S | R | Y | S |
| `research.list` | Y | S | R | Y | S+L |
| `research.backtest` | Y | S | R | — | S |
| `research.grid` | Y | S | R | — | S |
| `research.graph_backtest` | Y | S | R | — | S |
| `research.benchmark` | Y | S | R | — | S |
| `research.status` | Y | S | R | Y | S |
| `research.cancel` | Y | S | R | — | S |
| `research.result` | Y | S | R | Y | S |
| `research.capabilities` | Y | S | R | Y | S |
| `research.economics` | Y | S | R | — | S |
| `research.validate` | Y | S | R | Y | S |
| `trade.preview` | Y | S | D | — | S |
| `trade.execute` | Y | S+F | D | — | S+F |
| `trade.status` | Y | S | D | — | S |
| `trade.cancel` | Y | S+F | D | — | S+F |
| `trade.close` | Y | S+F | D | — | S+F |
| `trade.capabilities` | Y | S | D | — | S |
| `trade.preflight` | Y | S | D | — | S |
| `portfolio.summary` | Y | S | D | — | S |
| `portfolio.history` | Y | S | D | — | S |
| `portfolio.scenario` | Y | S | R | Y | S |
| `diagnostics.explain` | Y | S | M | P | S |
| `diagnostics.runtime` | Y | S | D | — | S |

**Exact inventory:** 13 MCP tool descriptors (six multi-action tools, seven standalone `trade.*` tools), 45 public action variants. The 45 rows above are checked against runtime `ACTION_SCHEMAS` by `cloudflare/okx-mcp/scripts/test-tool-result.mjs` in CI. `research.result` Live `Y` covers the existing pinned job's five views (`summary`, `trades`, `equity`, `variants`, `export`), **not a newly submitted backtest**. `research.list` Live `S+L` includes invalid limit/cursor/extra-parameter refusals. `diagnostics.explain` Live `P` records intermittent `CONTROL_ATTEST_OFFLINE`/`CONTROL_PID_SHA_GENERATION`, despite independently verified living processes; it is not stable production attestation.

Installed baseline: [CONTROL issue #12](https://github.com/iamaman11/okx/issues/12) established running Research executable tree `9652f03674790ade9f1d3f6dfffc71d4b9fa31cb`; Engine tree `ed0884d1402565378a7c61265700eeeb42be1ad0`, CONTROL tree `dad0fb6dfb26ebe62d662a1e8a95413704327880`. Production Edge [deploy #125](https://github.com/iamaman11/okx/actions/runs/46093524489) is the source tree above. Version drift is **not** equivalent to a proven failure of an old installed action.

## Acceptance still required

1. **P0 financial cutover:** connect a verified account and import complete all-managed-account USD economic expenses before real RiskBook admission. No live trading/settlement acceptance is claimed here; finance must remain fail-closed.
2. **P0 attestation:** explain and correct intermittent CONTROL proof failures without weakening the PID/SHA/generation checks; record stable on-wire acceptance separately.
3. **P1 analyst workflow:** controlled new `backtest`, `grid`, `graph_backtest`, `market.screen`/`compare`, `economics`, and non-trading decision write/read; verify progress, cancellation, pagination and crash/restart behavior on installed binaries. Existing completed-job reads are insufficient.
4. **P1 installed parity:** update Engine and CONTROL only through separately authorized pinned-artifact deployment; rerun relevant live/negative acceptance after parity.
5. **P1 observability:** extend saved causal provenance past the current bounded operation trace; record physical T10 latency/load/cancellation and resource SLO rather than treating mocked 100-call concurrency as a physical load result.

The CI files `acceptance_composed.test.ts`, `legacy_finance_cutover.test.ts`, and `risk_book.test.ts` are explicitly **retired** and not run as financial acceptance. The active Worker suite has ten test files plus workerd/SQLite integration; `mcp.test.ts` exercises 12 fake mutation routes, but fake financial success is **not real exchange or installed Engine proof**.
