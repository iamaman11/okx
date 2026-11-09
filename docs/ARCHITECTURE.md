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
| `operation` | account + `terminal_release:<intent>`; terminal-release owner | Actual reached stage, last outcome and first observed blocker for that revision; volatile and never a fresh SQL/Worker read |
| `finance` | account + optional cycle ID; finance trace | Capture phases, SQL timing and source errors; busy trace is unevaluated, not a missing cycle |
| `storage` | account; account SQL owner's published snapshot | Last policy/stops/coverage/storage facts, publication age and refresh state; no query is queued by the reader |
| `research` | job ID; installation's research catalog | Bounded committed metadata projection, progress/state/result reference and safe reason; no catalog lock wait |

The default detailed diagnostic keeps its existing SQL-reading semantics.
The snapshot views expose unknown history and publication gaps. They do not
replace current authoritative reads used by execution. Ordinary MCP/session
authorization and account routing still apply; a research job is installation
scoped and rejects account routing. A missing retained job is unknown, not proof
that the durable job does not exist.

`trade.status.central_release.diagnostic_reference` identifies the terminal
projection when that owner was called. `terminal_release_trace.rs` observes
the existing release stages with non-blocking writes; `diagnostics.rs` reads
that account's projection after normal routing checks. It never invokes Status
or release. Each account retains at most128 terminal intents with bounded keys;
drop, eviction and superseded-attempt counters are visible. After restart an
unretained attempt is unknown. A successful function return is recorded as
`returned_ok`, not reconstructed as an exchange ACK or independent SQL proof.
Initiator is explicitly unrecorded; this does not close cross-family causal history.

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

## Research progress

`strategy_registry::grid_with_observer` reports completed replay units without
changing the existing grid result or train-only selection. Each instrument has
N training, N holdout, N fee-sensitivity and one selected replay:3N+1 units.
A candidate counts as complete after its holdout and fee replay finish. The
grid's CPU callback writes only bounded atomic observations. The existing job
task coalesces them and publishes through Jobs' existing SQLite owner; it starts
no detached progress worker. Multiple instruments aggregate their actual totals.

Research Status serializes optional `candidate_progress`; the existing research
diagnostic mirrors committed counters without waiting on the catalog. Running
percent stays below100 until a result artifact is published and the job commits
Completed. Phase describes the last observed work (`publishing` after all replay
units); terminal job state tells whether publication succeeded. Cancellation and
terminal states fence late observations. Reopen retains last committed counts
and marks unfinished jobs Interrupted; it does not pretend to resume compute.

## Source and installed version

A branch identifies a moving work line; an immutable checkpoint tag identifies
one commit. Neither identifies an installed executable on its own. For source
use `git rev-parse HEAD`, `git rev-parse 'HEAD^{tree}'` and `git status --short`;
dirty changes are not the tagged version. Continue the same line from the
checkpoint instead of creating another development branch for each slice.

For a CI bundle, compare `manifest.json` fields `source_head_sha`, `source_tree`
and the declared executable SHA256 hashes with the actual files. The existing
host-control artifact validator enforces these identities during installation;
CONTROL health/ACK belongs to that actual installed instance. Keep local-test,
GitHub-CI, bundle and installed evidence distinct in [testing.md](testing.md).
A local checkpoint archive is not a release or an acceptance certificate.

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
