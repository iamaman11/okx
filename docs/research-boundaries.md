# Research and live execution

This is a source map, not another execution model. The authoritative boundaries
are Cargo dependencies, Rust visibility, the IPC enums, and the SQL owners.

## Current source boundary and acceptance

The Engine has no normal, build **or dev** Cargo dependency on Research.
The former Engine `research_client.rs` and Research command tests have been
removed. A Research-owned WSS session accepts public market and Research
commands directly; CONTROL alone owns its verified Windows process lifecycle.
`scripts/check_architecture.py --strict-component-test-boundary` guards the
complete Engine/Research dependency closure, including test features.

Cross-owner load evidence remains necessary: the Engine test launches an
external, Research-owned `okx-research-load-fixture` through a versioned JSONL
interface. It does not compile or import the Research protocol. The Windows
acceptance still measures Research/Engine process isolation, no exchange POST,
and real MCP → Durable Object → WSS behavior. Source separation and green CI
are not proof that a particular installed Windows component is current or that
its physical load budget is accepted; record exact source tree, release,
installed binary hashes and live observations separately.

## Ownership

| Responsibility | Production entrypoint / owner |
|---|---|
| Authenticated trading/account command | `okx-trading-engine::query::dispatch_with_governance`; Research commands are rejected as `RESEARCH_OWNER_REQUIRED` |
| Available or unavailable finance | `okx-trading-engine::finance_dispatch::FinanceDispatch` |
| Existing finance startup and exact rejection cause | `okx-trading-engine::finance_startup::select` |
| Live execution, protection, reconciliation | `okx-trading-engine::trade_service::TradeService` |
| Financial persistence and RiskBook | Existing `UnifiedFinanceHandle`, one financial SQLite |
| Live closed candles | `okx-market-runtime::live_source::LiveCandleSource` |
| Public market HTTP and shared rate budget | `okx-market-runtime::market_bootstrap::MarketBootstrapper` |
| Engine delivery and financial observations | `okx-trading-engine::cloudflare_transport` |
| Research delivery and typed public-source requests | `okx-research-service::edge_runtime` |
| Research command handler | `okx-research-service::analytics::AnalyticsService` |
| Research jobs, identity, cancellation, root lease | Research `analytics::jobs::Jobs` |
| Pure calculations | Existing `okx-analysis` and `okx-research` libraries |
| Verified component staging | CONTROL `artifact::research_component` |
| Standalone Research process, Windows Job and desired state | CONTROL `research_supervisor::ResearchSupervisor` |
| Standalone Research Cloudflare credential | Research `edge_credentials`, separate Windows Credential Manager target |

## Independent Edge lane

The Research Service also has a **Research-owned** outbound WebSocket adapter
(`edge_runtime.rs`) behind the explicit `--edge-runtime` mode. It connects
with `hello_research`, the `research` runtime profile and a distinct bearer;
it binds requests and replies to an authenticated Edge session generation.
It does not import Engine's Cloudflare transport, private account credentials,
financial storage or exchange order-sending capability. This mode uses the
already implemented `IndependentOkxPublicSource` rather than Engine's public
market callback, and bounds frame size and concurrent work. The existing
legacy IPC runners remain available for historical acceptance tests while
their independent-process witnesses are migrated. This **does not** mean the
two launch modes satisfy S2: only CONTROL may own the eventual installed
Research lifecycle.

Research owns market queries with explicit instruments or a public universe,
market capabilities, instrument search, overview, microstructure, portfolio
scenarios, and Research job commands and diagnostics. Edge routes these
operations to Research even if its socket is offline. A Research deployment
diagnostic reports the Edge transport observation. CONTROL's
`StatusResearch` remains the source for locally verified image and Job state.
A Research socket refuses trading, account
selection, strategy mutation, risk commands
and policy snapshot frames. Edge's internal `target_owner` routing marker is
removed before constructing the strict native Rust request.

The standalone mode uses the existing CONTROL:

- `StartResearch` requires the exact SHA-256 of the installed, reverified
  content-addressed worker. CONTROL starts it in a **separate Windows Job**,
  sends a bounded startup receipt only after Job assignment, and persists an
  independent process generation. Research rejects startup if that receipt
  is absent or invalid; it does not race its own memory Job against CONTROL.
  Inside that process the Worker installs and reads back a nested Windows Job
  with a **4 GiB memory limit and 50% CPU hard cap**. Both are fail-closed:
  inability to enforce either limit prevents Research startup. Each
  application has its own desired state and can run concurrently. Before
  starting Engine, CONTROL verifies its installed SHA-256 and its local
  `runtime-contract` response. DeployAgent checks this contract on the staged
  image before stopping or replacing the current Engine. Older Engine images
  that can launch a Research child are refused. Windows environments with
  DFSS need physical verification of the CPU rate control.
- `StopResearch` requires the active worker SHA and matching process generation;
  status/desired state live in CONTROL's own `research-state.json` rather than
  Engine's `engine-state.pb`. A new installation uses
  `RUNTIME_ROOT/research/edge-catalog`. To retain a previous scoped catalog in
  `RUNTIME_ROOT/analytics`, the authorized operator supplies
  `existing_catalog_scope_sha256` on `StartResearch`. This is the SHA-256 of
  the **raw public key** matching the old catalog's scope, independent of the
  Research bearer and Edge host identity. The stopped Engine child releases
  the catalog lease. Research opens the same catalog and artifacts in place
  and verifies the existing scope under that lease before schema upgrades,
  job recovery or serving requests. CONTROL retains the selection for
  subsequent starts. A missing catalog, an unbound or mismatched scope,
  or data in both roots blocks selection or startup. No merging or
  replacement catalog is performed in these cases.
- `ProvisionResearchRuntimeToken` generates a separate secret and passes it to
  the verified Research executable by bounded stdin. Research writes it to
  its own Windows Credential Manager target; the CONTROL response contains
  only its SHA-256, never the bearer. Provisioning while Research is desired
  or observed running is rejected: the deliberate rotation order is **stop,
  provision, configure the matching Edge hash, start, authenticate and read
  back**. Windows standalone mode ignores bearer environment variables and
  pins the allowed WSS endpoint.
- `StatusResearch`, including the read-only MCP Bridge tool `research_status`,
  is **process evidence only**. Its `RUNNING_UNVERIFIED` phase does not mean an
  authenticated Edge session, loaded job catalog, or successful market read.
  `EXISTING_SCOPED` reports the selected catalog binding. Verify a successful
  Research status/result read on an existing job before switching routing.
- Edge authenticates each runtime against its own bearer fingerprint.
  Standalone Research uses its own delivery limit and read queue. Pending ACKs
  and responses are matched by socket generation and request ID.
- `standalone_catalog_reopens_through_real_mcp_do_websocket` uses the actual
  MCP router, local workerd Durable Object, and Rust Research session. It
  checks the original job status, result bytes, decision replay and the offline
  response with only Research configured. The local harness supplies a test
  MCP scope. The deployed OAuth and installed Windows checks remain required.

The deployed Edge, CONTROL, and Engine must be switched using an accepted
candidate. An offline Research owner returns its own unavailable status.
Independent Edge token registration, authenticated-session readback, clean
restart/reboot, job compatibility, independent update and rollback evidence
are required for installed acceptance.

### P0 operator runbook: activate the independent Research Edge owner

This is an explicit, one-time, typed CONTROL/accepted-release operation, not
an automatic side effect of merging a PR. Before starting, record the installed
Engine PID/SHA, Research SHA/process generation, CONTROL release parity, current
Edge source tree and the accepted native/Edge contract. No exchange POST,
Engine restart, manual EXE replacement or new process owner is authorized.

1. Invoke `StopResearch` with the observed Research image SHA and generation;
   verify Research is stopped and Engine PID/SHA are unchanged. Do not rotate
   credentials while Research is desired or observed running.
2. Invoke `ProvisionResearchRuntimeToken` through CONTROL. The new bearer is
   stored only under the Research Windows Credential Manager target. Capture
   **only** the returned `runtime_token_sha256`, never the bearer.
3. Configure that exact 64-character lowercase SHA-256 as the protected GitHub
   Actions **environment secret** `OKX_RESEARCH_RUNTIME_TOKEN_SHA256` in
   `okx-succession-global-executor`; it must differ from Engine's configured
   fingerprint. Do not commit it into `wrangler.jsonc`, echo it in logs or
   override the Engine token. The deploy preflight and release-config generation
   fail closed if the Research fingerprint is missing, malformed or equal.
4. Invoke `StartResearch` with the verified installed Research image SHA and,
   if required, its established catalog-scope fingerprint. Its
   `RUNNING_UNVERIFIED` process status is not yet proof of an
   authenticated session and may persist until Edge deployment completes.
5. Only after the matching native release is accepted, explicitly dispatch
   `cloudflare-mcp-deploy.yml` from exact `main` with
   `initialize_clean_v2=false`. Require checked Worker tests, `/health`
   `release_source_tree` equal to the deployed main tree and owner-authenticated
   `/installation/inspect`. Then verify separately the **Research** WSS owner
   is `PASS`/fresh, a read-only Research capabilities/status/result request
   succeeds, and Engine's PID/SHA and authenticated Engine WSS remain stable.

If an intermediate step fails or the fingerprint/provenance is uncertain, stop
at that step and report the owner, code and first missing evidence. Never claim
Research ready based only on its PID or a healthy Engine WSS. Do not deploy
Edge-first, bypass the protected fingerprint, restart Engine to repair Research
or use live financial operations as a connectivity probe.

Separate Credential Manager targets and distinct crates are a code-level
least-authority measure, **not** an OS security boundary when both processes
run as the same Windows user.

The Engine has no Research dependency, including dev/test dependency edges.
The historic `okx-ipc-contracts → okx-research-protocol` build coupling is also
removed. Research's standalone Edge runtime uses its own public source,
protocols and catalog; it does not import Engine, account SQL or exchange-order
capabilities. `scripts/check_architecture.py` validates both normal/build and
strict dev/test closures. The remaining cross-owner T10 fixtures use an
external Research process, not a shared Research library dependency.

The Research executable still supports legacy framed commands with
`--independent-source --root <research-dir> --scope <scope>`. Its local
provider reads public OKX candles, recent and historical trades, SWAP
reference data, tickers, historical funding and open interest. It rejects parent source replies
in that mode. Use `--market-request --root <research-dir> --scope <scope>`
to read one JSON `MarketRequest` from stdin and print the result. Explicit
instruments are required. These requests use the same analytics, coverage
and artifact code as Research Edge commands. Live user-facing Research
requests must be routed to the Research owner; Engine's authenticated product
dispatcher returns `RESEARCH_OWNER_REQUIRED`, not a fallback result.

The live strategy turn in `okx-trading-engine/src/trade_service/strategies.rs`
captures verified closed bars from Engine's source, **evaluates the installed
rule locally**, verifies freshness and seals the deterministic decision key
inside the account-scoped financial SQLite owner. Stop and automatic-send
claim are serialized against the same strategy activation revision. Research
does not calculate, approve or publish a live trading signal for Engine.
Manual trade, risk-reducing exits and reconciliation use the same financial
authority. Old `ResearchCommand::ProposeStrategySignal` and its Research SQLite
outbox are legacy code to remove after mandatory witnesses migrate; they are
not part of the desired production contract.

## Identity and recovery

`RequestId` belongs to one IPC session. `IdempotencyKey` identifies a durable
command in the authenticated host's research scope. `JobId` identifies its
persisted job. Reconnection does not manufacture a new idempotency key.

The existing research catalog owns semantic request digests, scope binding,
terminal state, and cursor identity. An exclusive root lease precedes recovery.
Interrupted work is reported as interrupted; it is not promoted to success.
Cancellation is terminal even when a previously issued source response arrives.

`InvocationId` links the originating product request to the IPC request and job;
it is not the durable idempotency key. The catalog records the creator's actual
Worker executable SHA-256 and available build-time source identity at admission.
Reopening under another executable preserves that identity. Result manifests
record the executable that computed the result. Legacy records retain unknown
provenance rather than inheriting the currently running Worker.

Financial startup retains a typed cause and returns the original safe rejection
code. Missing, corrupt, busy and foreign storage are not interchangeable. Research
does not require successful finance startup or an initial HTTP instrument fetch.
The existing public source owner initializes the reference only when a request
needs it; until then reference-dependent requests fail closed. Capabilities and
stored-job access do not perform that fetch.

## Installation versus activation

`HostControlOperation::DeployResearch` stages the verified, content-addressed
Research image and publishes `installed-research.json`; installation is not
activation. In the new architecture only CONTROL's `StartResearch` and
`StopResearch` own the process. Verify installed and running SHA, process
generation, Windows Job, authenticated Research Edge session and existing-job
readback separately. A failed activation preserves the existing Engine PID,
financial SQLite and trading authority; it never authorizes a new image by
inference. `StatusResearch` is read-only process evidence, not proof of a
network handshake. Installed update/rollback and shared-load measurements
remain acceptance prerequisites under #580 S3.

The historical Engine-hosted handoff/recovery procedure and its
`diagnostics.explain(view="research",reference="deployment")` observation
describe only an **older installed mode**. They are useful for verifying a
safe transition, but must not be represented as the final CONTROL-owned
runtime, a live Research-only user route or a completed physical cutover.

Account diagnostics read NAV completeness, raw coverage, source time and checked
time from one scoped financial snapshot, with a 250ms read deadline. They report
the current observed state, not an inferred historical admission cause, and
never refresh sources or grant a send. Coverage includes all five fill-history
partitions, bills and the account snapshot.

The financial collector retains verified earlier packages after a later
failure. Its property report distinguishes PASS, observed FAIL, NOT_PROVEN,
NOT_RUN and MISSING; the overall gate still fails on any incomplete execution.
Concurrent fixture tests report an early Execute reply immediately rather than
waiting for a phase the operation can no longer reach.
Finite fill/Close transcripts pause their timer producer only while explicitly
delivering history. Shutdown cancels a producer waiting for that test-only gate
before the gate is released. Final economic assertions inspect stored COMPLETE
coverage and ledger amounts; they do not assume a paused source stays fresh.
Admission freshness remains independently enforced and negatively tested.

## Direct checks after building

```powershell
# Research-owned tests: production singleton, no Engine child fixture.
cargo build --locked -p okx-research-service --bin okx-research-service
cargo test --locked -p okx-research-service --lib
cargo test --locked -p okx-research-service --test ipc_process -- --test-threads 1
cargo test --locked -p okx-research-service --test causal_release_smoke
# Explicitly ignored by default: requires Node, installed Miniflare and loopback.
cargo test --locked -p okx-research-service --lib standalone_catalog_reopens_through_real_mcp_do_websocket -- --ignored --nocapture
cargo test --locked -p okx-research-service --lib two_heavy_research_jobs_ingestion_stop_and_cancel_latency

# Cross-owner load acceptance uses externally launched Research-owned binaries
# with no Engine Research protocol linkage. Never ship these test fixtures.
cargo build --locked -p okx-research-service --bin okx-research-service --features test-isolated-singleton
Copy-Item target/debug/okx-research-service.exe target/debug/okx-research-service-test-isolated.exe
$env:OKX_RESEARCH_SERVICE_TEST_EXE = (Resolve-Path target/debug/okx-research-service-test-isolated.exe).Path
cargo build --locked -p okx-research-service --bin okx-research-load-fixture --features test-fixture
$env:OKX_RESEARCH_LOAD_FIXTURE_EXE = (Resolve-Path target/debug/okx-research-load-fixture.exe).Path
cargo test --locked -p okx-trading-engine --lib accounts::native_trade_contracts::native_engine_research::engine_maintenance_and_stop_remain_independent_of_heavy_research_and_disconnect -- --exact
cargo test --locked -p okx-trading-engine --lib cloudflare_transport::session_tests::
cargo test --locked -p okx-trading-engine --lib finance_startup::
cargo test --locked -p okx-market-runtime --lib public::reference_refresh::tests::
python scripts/check_architecture.py --self-test
python scripts/check_architecture.py
python scripts/check_architecture.py --strict-component-test-boundary
```

The dedicated Engine-under-Research-load check runs in its own test process so
Windows job accounting includes its descendants rather than unrelated tests.
Compilation, process startup, execution, and resource evidence are reported
separately. Measured resources do not by themselves establish a resource budget.
The first fully green four-component candidate was `dacab8c5` in
[CI #44222225744](https://github.com/iamaman11/okx/actions/runs/44222225744).
Its direct financial budget was verified, while the Research process-tree
resource budget was explicitly `NOT_PROVEN`. The latter is not a shipping resource
guarantee merely because the enclosing CI job succeeded.

In the full CI, Linux runs the Worker library, real IPC children and doctests
in the dedicated Research step; the portable workspace step excludes that
package. Windows runs the Worker library once, except for the K9 workload which
runs once in its dedicated evidence step. The Native regression skips only
exact property witnesses extracted from the current Rust source manifest.
The parallel financial collector must execute FULL_PASS on the same clean
SHA/tree and prove that its observed roster matches that manifest. Missing,
failed or mismatched collector evidence makes the protected Windows gate RED;
the source roster alone never grants financial acceptance. All remaining
regressions are mandatory. Compare local
and CI timings for the same command and include its build time; a financial
execution time is not the duration of a complete Windows job.

To add an experiment with existing rules, submit another validated
`ResearchRequest` specification. To add a calculation, extend its existing pure
operator/replay registry and its direct tests, then connect it through the worker
planner. Financial state and CONTROL do not belong to that calculation change.

Research-only Cargo tests run without rebuilding the financial Engine even if
the accepted binary base is unavailable. The separately specified
`okx.windows.research-component/v1` artifact contains exactly three files:
`Cargo.lock`, `manifest.json` and `okx-research-service.exe`. Its manifest
binds the exact repository head/tree, Rust 1.100.0, the complete Cargo lock
SHA-256, executable SHA-256/byte length, protocol and storage versions.
This byte/schema proof is **component-only**, not evidence of a four-binary
installed application, an authenticated GitHub source, or an authorized
update. Its producer/consumer and negative controls require their own CI
verification before stating CODE PASS.

Deployable **Research-only packaging** still needs a previously successful,
accepted and **merged** complete base, an unexpired exact GitHub bundle and
its explicit `.github/research-release-base.json` selection; otherwise
Ready fails closed.
The first full-build PR has no
such prior merged base, so its skipped `research-service-tests-windows` step does not
prove the next independent update. The separate Worker Cargo build, scope
negative controls, real child handoff and CONTROL staging checks provide CODE
evidence for the implemented route; actual use of a subsequent Worker release
is a separate acceptance event. No artificial second implementation is needed.

CODE acceptance remains governed by #512: C0–C13, U01–U05 and the exact-SHA
required gates are accepted only with their recorded evidence. Temporary fixture
process tests are not evidence of installation, real exchange connectivity, or
live trading. The [operation map](acceptance/512/operation-map.md) links the
current supported families to their owners and direct tests.
