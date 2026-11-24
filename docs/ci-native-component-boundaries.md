# Native CI ownership and build boundaries (#537)

The protected checks `classify`, `linux-core` and `windows-native` stay required. They
aggregate different **executed** jobs; a skipped financial matrix is not
reported as a successful financial test run.

**One automatic CI for `main`:** `.github/workflows/ci.yml` runs the protected
checks and, only after their success, the optional Native-base publication job.
No second `workflow_run` is started on completion. GitHub Actions has separate
manual operational workflows for protected account access and Cloudflare
deployment; those operations do not appear as permanently skipped CI jobs.
The historical All26 branch-only workflow and explicit recovery promotion
remain separate from the normal `main` CI path. Closed #510/#512 migration
jobs were removed from the main workflow.

| Changed files (complete GitHub diff) | Direct owner and consumers | Ready CI | Native release artifact |
| --- | --- | --- | --- |
| Only `crates/okx-host-launcher/src/**/*.rs` (+ docs) | Launcher, plus Host Controller consumer | `host-controller-launcher-checks`; no Trading Engine/SQL compilation | None: host identities are not an installable four-component bundle |
| Only `crates/okx-host-controller/src/**/*.rs` and optional Launcher Rust source (+ docs) | Host Controller and Launcher | `host-controller-launcher-checks`; no Trading Engine/SQL compilation | None |
| Trading Engine/TradeService, `okx-trade-execution` or financial SQL | Trading Engine, RiskBook, SQL and trade-side consumers | Mandatory financial `trading-engine-acceptance-windows`, independent `trading-engine-regressions-windows`, parallel `trading-engine-build-windows`, B1 and Linux financial regressions | Full verified Windows bundle on qualifying PR |
| Shared `okx-api`, `okx-market-runtime`, `okx-ipc-contracts`, root Cargo files, build scripts, installer/packaging scripts or CI workflow | All affected consumers | Full Native matrix (fail closed) | Full only after acceptance |
| Narrow Research/Host `Cargo.toml` with optional `Cargo.lock` | Only that owner and its actual consumers | Scoped **only after exact before/after Windows Cargo closure proof**, including versions/features/dev dependencies; otherwise full Native | Research accepted-base composition or host-only nondeployable evidence |
| Research-only + accepted immutable Native base | Research/IPC consumers | Separate Research gate with independently verified Cargo closure | Composed Research-only bundle |
| Documentation only / Cloudflare-only | Documentation / MCP | Existing scoped gates | No fresh Native build |
| Mixed host + Research or Cloudflare, unknown paths, renames, incomplete diff | Unknown/shared impact | Full Native matrix | Only if accepted |

The host-only source exception remains intentionally **narrow**: just Rust
implementation files inside the two host crates. A Host or Research manifest
change takes the corresponding scoped route **only if a separate resolver proof
establishes** that all protected Windows dependency closures are identical.
Unverified dependency changes and installation contracts stay full. The classifier reads every
GitHub diff page, rejects renames and ambiguous paths, and has negative tests
in `scripts/check_ci_native_scope.py --self-test`.
The same self-test checks that the shell scope guard accepts exactly the
classifier's output tokens; it runs on Draft PRs before Rust compilation.

`scripts/check_architecture.py` separately rejects direct and transitive
financial/market dependencies from CONTROL or Launcher, as well as a Launcher
dependency on CONTROL. CONTROL may depend on Launcher and the versioned
protocol. The financial SQLite owner remains in `okx-trade-execution` and its
TradeService/Engine consumers remain in `okx-trading-engine`; this stage does **not**
claim separate financial SQL and Engine release cycles.

For a full Native change, the protected `windows-native` aggregate requires
**all four** `trading-engine-acceptance-windows`, `trading-engine-regressions-windows`,
`trading-engine-build-windows` and `trading-safety-acceptance-windows` to succeed.
The complete budgeted financial collector and isolated OS/SQL/process fixtures
remain on one runner. Independent remaining regressions run on a separate
Windows runner, deriving their exact financial-witness exclusions from the
current collector manifest. The full Windows package is built in parallel
from the **same exact source tree**; publication waits for all four jobs.
Scoped Research, host,
Cloudflare and exact merged-tree reuse require the B1 prerequisite to be
`skipped`, never falsely counted as executed. The five recorded-source B1
fixtures are run sequentially to avoid unrelated fixture readiness timeouts;
the 100-caller CAS test keeps its internal concurrency.

The four serialized, clock-sensitive Trading Engine witnesses also record nine
monotonic lifecycle phase durations each in the existing financial JSON report
(`clock_sensitive_phase_timings`). The collector requires exact current-run
receipt identities and preserves its original resource-budget and witness
checks; these timings diagnose slow setup, entry, settlement, release or cold
reopen, but are not independent proof of financial correctness.

The host job runs the existing installation-root safety fixture, architecture
guard, formatting, clippy, Rust tests and Windows release builds for both
CONTROL and Launcher. Its exact source HEAD, tree, component scope and binary
hashes are retained as **non-deployable** evidence. It never publishes a new
Native base; `publish-accepted-windows-release` inside the **same** protected
`.github/workflows/ci.yml` runs after successful main checks and continues
to require fresh `trading-engine-acceptance-windows`, `trading-engine-regressions-windows`,
`trading-engine-build-windows`, B1 and a complete
four-component Windows artifact. It publishes a GitHub release but never
installs or restarts the Trading Engine.

To warm dependencies for the **next** PR, an explicit `workflow_dispatch` on
the accepted `main` can set `prime_windows_cargo_cache=true`. The classifier
must prove that `main` is the exact tree of a successful Ready PR. Two separate
Windows runners then seed the test/collector and release Cargo profiles using
their respective cache keys. This manual compilation executes **no financial
tests**, creates no candidate release artifact and deploys nothing. An ordinary
PR cache cannot prime the default branch; cold and warm PR timings remain
separate measurements for #560.

Draft PRs with only host Rust sources run direct Launcher and CONTROL Cargo
tests of library and binary targets via `scripts/check_p0.py`, reusing the
same host path classifier. Manifest, shared-code and CI changes retain the
expanded Draft checks; no host exception covers them.

Acceptance controls:

1. A CONTROL-only or Launcher-only Rust diff selects its own Windows job;
   that job compiles both host owner/consumer crates, never Agent/Execution.
2. A shared IPC, Cargo manifest, SQL or Engine diff selects full financial CI.
3. A mixed host and non-host diff cannot inherit the cheap host path.
4. Existing protected aggregate names remain, and the host-only branch fails
   if its specific Windows job fails or is skipped.
5. Changes to these classifier/workflow/architecture rules themselves select
   `full`; the first adoption therefore requires existing full acceptance.

Measured elapsed times and the possible future separation of Engine vs SQL
test jobs remain open acceptance work, not completed by this routing change.

## Cargo consumer inventory (#560, diagnostic phase)

The existing `dependency-audit` job already runs `cargo metadata --locked`. The
same resolved graph is now reused by `scripts/ci-component-impact.cjs` to record
which deployable Windows binaries consume each changed workspace crate via
normal/build dependencies. This includes *transitive* consumers; for example,
changing `okx-domain-core` impacts both the Engine/finance and Research release
roots. A Launcher implementation change reaches the Host Controller but not
the financial Engine. Dev-only dependencies are excluded from **release** impact,
not from the tests that may need to run.

The report lists actual dependency paths, affected binary names and separate
Cloudflare changes. It is retained in the existing dependency-audit artifact
and GitHub job summary; no new CI workflow, Rust compilation or Windows runner
is introduced for inventory. Any Cargo manifest, lock, build script, unknown
path, unverified rename or incomplete diff is reported as potentially affecting
all Windows binaries. An unavailable push diff is also explicitly unverified.

**The inventory is diagnostic-only, not a skip authorization.** The existing
fail-closed classifier and immutable-release checks still select tests and
builds. Narrow manifest candidates pass through a *separate* pre/post resolver
in the existing `classify` job: both Cargo versions use the same pinned toolchain,
`--locked`, and `--filter-platform x86_64-pc-windows-msvc`. The proof compares
the resolved package identities, checksums, enabled and declared features,
dependency aliases/kinds/targets and crate target definitions across complete
protected transitive closures. It conservatively includes dev dependencies to
protect Windows tests. A missing node, mixed owner, unsupported path, changed
protected dependency, failed resolver or stale PR HEAD/base means **full**.
No separate workflow or Windows runner is added to classify the change.

For Research release reuse, `prepare-research-release.ps1` repeats this proof
against the **accepted Native release's own merge commit** before composing its
immutable Engine/Host binaries with a fresh Research worker. The PR comparison
alone is not sufficient to trust older installed binaries. Financial tests and
full packaging still run whenever Engine's closure changes. Real CI evidence
for positive/negative manifest changes and warm-cache timings remains an open
acceptance item in #560.
