# Current architecture

See [the implemented clean Engine contract](ARCHITECTURE_CONTRACT_V1.md), [#398](https://github.com/iamaman11/okx/issues/398) and [the operational runbook](PRODUCT_ACCEPTANCE.md).

[The earlier architecture document](history/ARCHITECTURE-before-398.md) is historical evidence only.

## Finding an operation's owner

`okx-protocol::AgentOperation` is the public command model. `query::dispatch`
validates it and routes to the existing account, trade, analytics or portfolio
owner. Background recovery uses those same operations. The executable mapping
and exact direct-test commands are in [testing.md](testing.md).

Trade decisions remain beside their operation in `trade_service`. Account
SQLite mutations belong to `AccountStoreHandle`; exchange sends belong to the
existing execution adapter; shared risk belongs to Worker RiskBook. A diagnostic
projection cannot grant permission, settle money or release risk.

## Read-only explanations

The existing `diagnostics.explain` tool has explicit views:

| View | Reference / owner | Evidence and limits |
|---|---|---|
| `operation` | account + cleanup operation/intent; protection operation trace | Local observed transitions and bounded startup SQL receipts; lost ACK/origin remain unknown |
| `finance` | account + optional cycle ID; finance trace | Capture phases, SQL timing and source errors; busy trace is unevaluated, not a missing cycle |
| `storage` | account; account SQL owner's published snapshot | Last policy/stops/coverage/storage facts, publication age and refresh state; no query is queued by the reader |
| `research` | job ID; installation's research catalog | Bounded committed metadata projection, progress/state/result reference and safe reason; no catalog lock wait |

The default detailed diagnostic keeps its existing SQL-reading semantics.
The snapshot views expose unknown history and publication gaps. They do not
replace current authoritative reads used by execution. Ordinary MCP/session
authorization and account routing still apply; a research job is installation
scoped and rejects account routing. A missing retained job is unknown, not proof
that the durable job does not exist.

The account SQL loop refreshes its diagnostic copy at most once per second
when dirty, using a separate short diagnostic query budget. Readers never
request refresh. Research catalog metadata is published after committed
changes under the existing catalog owner. Both projections have bounded data;
publication failure cannot turn a failed mutation into success or vice versa.

## Research result provenance

Grid selection uses train data and the fixed tie-break policy. Each candidate's
OOS and doubled-fee evaluation is report-only. Before computing the grid, the
existing content-addressed artifact store saves its dataset reference,
parameters, costs, split, seed and code version. The result links this manifest;
export validates its identity and retention follows its dataset references.
This does not establish a profitable strategy or authorize live execution.

## Research decisions and extensions

`AnalyticsService::research` routes `record_decision` and `decision` to
`analytics/decision.rs`. The existing Jobs SQLite row owns one annotation head;
the existing artifact store owns immutable revisions bound to the job, result
and experiment. Updating the hypothesis publishes a new revision. It never
changes the backtest or calls TradeService. Expected-head CAS detects concurrent
changes. Artifact verification/publication happens outside the catalog lock;
the short transaction rechecks the result, expiry and head before committing.
The existing bounded executor schedules this file I/O.

Decisions currently require one provenance-bearing experiment result. Legacy
and batch manifests are explicitly unsupported for this action. History is
limited to256 revisions; expiry/pinning follows the job and reachable graph.
An interrupted or losing publication can leave an unreachable metadata file;
the current reachable-root retention does not collect those orphan files.

For a calculation extension, start at `okx-kernel::operators`: ordinary Rust
types/functions own the rule, while `StrategyKernel::from_id` selects a named
composition. `ema_momentum_confirm_v1` reuses the EMA and momentum kernels;
`okx-research::strategy_registry` keeps the same closed-bar/next-open replay.
Its literal signal and exact-money tests are independent of live account data.

The typed Research contract and capability descriptor expose the supported ID;
Worker validates/routes that contract and performs no strategy calculation.
`analytics::sources::Sources` remains the data owner: real OKX normalization
rejects duplicate timestamps and excludes unconfirmed candles. The C20 test
delivers fixed raw responses through the existing HTTP boundary and then uses
the production market/research owners. Adding this composition changed no
financial schema, TradeService or risk authority.
