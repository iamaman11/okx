# Canonical component and CI names (#537)

Names describe **ownership**, not historical implementation details. A Cargo
package is a compilation boundary, a Windows executable is a release artifact,
and a CI job is evidence. They are not interchangeable.

## Four application owners

The workspace has four logical owners across 21 Cargo packages in PR #581. Shared
libraries are compile-time dependencies. The source paths describe ownership
in the workspace. Runtime boundaries are tracked separately.

| Owner | Code and durable state | Exclusive effect | Must not own |
| --- | --- | --- | --- |
| **Engine** | `okx-trading-engine`, `okx-trade-execution`; one financial SQLite/WAL, RiskBook, private account keys | Financial admission, durable intent, at-most-once venue send, reconciliation, protection | Research algorithms, hypothesis jobs, Research's dataset/catalog storage |
| **Research** | `okx-research-service`, `okx-research-domain`, `okx-research-protocol`, `okx-strategy-core`, `okx-analysis`, `okx-research`; own research jobs/data | Public-source ingestion, causal computation, replay, analysis and immutable artifacts | Private credentials, account risk, live strategy decisions, finance SQLite, exchange POST |
| **Control** | `okx-host-controller` and `okx-host-launcher`; installed component manifests and lifecycle evidence | Authorized start/stop/install/rollback/doctor | Trading decisions, finance ledger, research computations |
| **Edge** | `cloudflare/okx-mcp`, `okx-bridge-mcp`; transport/session/identity state | Authenticated MCP/API delivery to the matching owner | Financial or research source-of-truth state, venue mutation |

`okx-engine-domain` owns Engine facts and fee rules. `okx-research-domain`
owns analytical inputs and public market facts for Research.
`okx-ipc-contracts` carries the Engine-facing wire. Its historical import of
`okx-research-protocol` is an S1 build-coupling defect, not an accepted
cross-owner dependency: the target Engine normal/build closure must contain
**no Research protocol or domain**. The temporary `legacy-research-test`
feature is a **dev-only debt**, not a waiver for final S1: normal/build/dev
must all be clean after T06–T10 witnesses move to an independent test harness.
Research owns its separate authenticated Edge request types. Host-control mailbox messages live
in `okx-host-protocol`.
`okx-api`, `okx-market-runtime`, `okx-observation`, `okx-ws` and
`okx-github` contain current adapters/support code; their current broad
dependency fan-out is **not** a reason to grant another owner financial
authority. `okx-cli` is a tool, not a fifth continuously running application.

The [crate split map](crate-split-map.md) shows which modules moved, their
Cargo dependencies and the CI acceptance checks.

### Ownership is a runtime contract, not a crate count

`Research` being compiled as separate crates does **not** on its own mean it
can restart independently. The **previously installed** Engine-hosted mode
started a Research child through `ResearchClient` and provided the public
market source callback. This path is no longer the intended production
execution architecture; the remaining child IPC and Engine test fixtures are
explicit S2 deletion work. The new, opt-in CONTROL-owned mode has its own
desired-state file, content-verified Research image, PID/process generation,
Windows Job, restart policy, host-scoped SQLite catalog and Research-specific
Cloudflare credential. CONTROL's `StatusResearch` reports only observed process
state; `RUNNING_UNVERIFIED` explicitly **does not** attest an Edge handshake.
The new mode is not an automatic migration of the installed Engine child.

`DeployResearch` still publishes only a verified installed image and reports
`activation: requires_engine_observation`. It does not silently replace either
running Research process, migrate historical jobs, configure Edge or prove
cutover. Process supervision, accepted installation and live routing are
separate evidence requirements; a source change alone is not physical Windows
acceptance.

The independent runtime cutover has four separately provable boundaries:

1. **Code and capability:** Research owns public-source ingestion, simulation,
   analytic state and its own authenticated transport; it cannot access the
   finance ledger, private exchange credential or order-send API.
2. **Process and installation:** CONTROL independently supervises the verified
   Research image and records its PID, executable SHA, startup generation and
   health. Its update and rollback must not restart the Engine.
3. **Routing and wire:** Edge authenticates a distinct `research` socket,
   strips internal dispatch metadata from the strict Rust wire request and
   has no hidden Engine fallback when that owner is offline.
4. **Compilation and verification:** Cargo closure checks and CI classify
   individual owner changes; shared IPC, build configurations and unknown
   inputs remain on the complete financial route. A scope classification is
   never proof that an installed process is independently managed.

Do not switch production routing to the separate Research credential until
CONTROL-owned Research startup, credential provisioning and Windows
restart/rollback acceptance have been verified on the actual installation,
the independently provisioned token SHA has been configured at Edge, and
Edge authenticated-session health has been observed. Both processes currently
run under the **same Windows user identity**: separate Credential Manager
targets and Cargo graphs reduce accidental coupling but are **not** an OS
sandbox or malicious-code isolation. That requires distinct restricted Windows
identities and filesystem ACLs. The Engine independently calculates live
strategy decisions from installed rules and controls their financial SQL claim;
Research's analysis/results do not constitute trading authority.

Virtual portfolio stress scenarios run directly in Research through
`okx-analysis::portfolio_scenario`, using Research's typed Edge command.
Engine does not forward those requests. Actual account portfolio and NAV
remain Engine-owned financial reads.

### Confirmed remaining coupling and release rules

At PR #581, Engine's `trade_service/strategies.rs::strategy_turn` evaluates
its installed rules locally against verified closed bars, then submits the
decision to its own SQL/RiskBook authority. The Engine query dispatcher rejects
Research operations with `RESEARCH_OWNER_REQUIRED`; Edge routes those to
Research's separate socket. However, `research_client.rs` and historical
child-process acceptance fixtures still compile under `#[cfg(test)]`, and
the Research executable retains a legacy framed-IPC entrypoint. In addition,
the old shared `okx-ipc-contracts → okx-research-protocol` dependency must be
disabled for production, with a visibly unresolved dev-only exception until
witness migration. The exact transition is tracked
in [the source and transition map](research-boundaries.md).

An independent Research change is accepted only when it can compile/test,
run public-source requests and produce reproducible replay results while
Engine is stopped, with **zero Engine compilation**, no private credentials,
no exchange POST and no financial database access. An Engine change requires
the applicable financial invariants: one SQLite writer, RiskBook admission,
lost-ACK `UNKNOWN`, no optimistic retry, crash recovery and protective exits
without Research. Shared wire/contract changes require both owners' checks.
Control and Edge changes require their own native/transport evidence and
cross-owner checks only for actually affected boundaries.

Keep one mandatory CI orchestration in `.github/workflows/ci.yml`: classify
actual transitive impact, run the affected owner jobs, and require one
fail-closed result. A post-merge `main` run that reuses exact accepted tree
evidence must **not** be described as a fresh financial acceptance run. The
target warmed 60–120-second ready cycle includes the relevant financial
checks and usable artifact, and is not yet proved for Engine changes.

The classifier follows **runtime effects as well as Cargo imports**. Research
analytics and its own Edge adapter can use the Research-only Windows route;
`research_supervisor.rs` can use Host owner tests. While the old Engine-child
mode still exists and its process tests remain mandatory, changes to Research's
`main.rs` or `process_memory.rs` conservatively require full Engine/Finance
acceptance. Likewise,
Host `job.rs` and `lifecycle_executor.rs` own the Engine process and its
Windows Job as well as Research: changing either must run the full native
matrix. Shared IPC, Cargo lockfiles and CI build inputs remain full-scope.
This is a temporary **explicit ownership boundary**, not a claim that all
files under a crate belong to one runtime owner.

The trusted `main` Windows release compiler cache and the full PR release
builder must agree on Cargo profile environment values, or an otherwise
unchanged Engine recompiles despite a successful main cache prime. Owner-only
Windows cache reuse is a separate, unproven performance task: combining two
`rust-cache` restores on one target can erase the first restore on a partial
cache hit. Scope selection must also be verified against actual Windows jobs:
a green classifier unit test alone does not prove a 1–2 minute owner-only run.

## Rust packages

| Canonical Cargo package / directory | Previously called | Responsibility |
| --- | --- | --- |
| `okx-trading-engine` | `okx-agent` | Trading process: accounts, positions, financial decisions, exchange sends and recovery |
| `okx-trade-execution` | `okx-execution` | Engine-owned RiskBook, finance/SQLite and order execution; **not** another process |
| `okx-native-finance-check` | Engine's `native_finance_acceptance` example | CI-only executable that runs the financial witness binaries and verifies exact-tree receipts |
| `okx-engine-domain` | `okx-domain-core` | Engine facts, fee rules and exact numeric inputs |
| `okx-research-domain` | `okx-domain-core` | Public market facts, analytical inputs and numeric validation owned by Research |
| `okx-ipc-contracts` | `okx-protocol` | Engine wire and migration-era compatibility; Research-protocol linkage must be removed for final S1 |
| `okx-research-protocol` | part of `okx-ipc-contracts` | Research commands, Edge frames and source data contracts |
| `okx-host-protocol` | new | Host installation/control command and result protocol for CONTROL and MCP bridge; no trading ownership |
| `okx-market-runtime` | `okx-runtime` | Runtime market sources, venue API, WebSocket and observation integration |
| `okx-research-service` | `okx-research-worker` | Separate research process and IPC implementation; no trading authority |
| `okx-host-controller` | `okx-host-control` | Windows installation, version activation, rollback, lifecycle and diagnostics |
| `okx-host-launcher` | unchanged | Verified native Windows process startup |
| `okx-strategy-core` | unchanged | Research-owned pure strategy calculations, causal plans and exact time-series grids |
| `okx-research`, `okx-analysis` | unchanged | Research and analytical functions |

Inside those packages, `lifecycle_executor.rs` is the Host Controller's
operation runner; `analytics/cpu_executor.rs` is Research's bounded CPU
scheduler. They are not financial executors. The account authorization role
`Executor` is a separate, existing permission and is not another executable.

## Installed Windows compatibility identities

Renaming the Cargo package does **not** rename an installed service or the
artifact it consumes. Until a separate, explicitly verified controller/host
migration, the v2 manifest and installed installation keep these names:

| Process responsibility | Current artifact filename / compatible identity |
| --- | --- |
| Trading Engine (`okx-trading-engine`) | `okx-agent.exe`; `okx-agent.stdout.log`, `okx-agent.stderr.log` |
| Research Service (`okx-research-service`) | `okx-research-service.exe` |
| Host Controller (`okx-host-controller`) | `okx-host-control.exe`; `iamaman11-okx-host-control` task/mutex |
| Host Launcher | `okx-host-launcher.exe` |

The existing non-Windows default data directory remains `.okx-agent`, and
the Windows scheduled-task XML remains
`C:\okx\control\okx-host-control-task.xml`.

Windows Credential Manager targets are also persistent installation identities,
**not** Cargo package names. An installed token or signing key must remain
readable after a source-only rename:

| Owner | Stable Credential Manager service |
| --- | --- |
| Trading Engine identity/signing key | `iamaman11.okx-agent` |
| Trading Engine GitHub mailbox token | `iamaman11.okx-agent.github` |
| Trading Engine Cloudflare runtime token | `iamaman11.okx-agent.cloudflare` |
| Trading Engine account credentials | `iamaman11.okx-agent.account.<reference>` |
| Host Controller GitHub token | `iamaman11.okx-host-control.github` |
| Standalone Research Cloudflare runtime token | `runtime-token.iamaman11.okx-research-service.cloudflare` |

Any future credential target migration requires explicit, separately accepted
secret-preserving migration and rollback; never silently write a renamed target.

Historic release manifests, stored source SHA256, previously installed roots
(`C:\okx-runtime`) and other provenance records are facts, not identifiers to
rewrite. Do not silently rename them or fake a new accepted binary. There is
one financial SQLite owner **inside** Trading Engine, not a SQL service.

The accepted **Windows binary baseline** (formerly called “Native base”)
means proven reusable binary bytes, not a runtime library. Existing
`native-base-<sha>` release tags remain immutable compatibility identifiers.

## CI job names and exact ownership

One `.github/workflows/ci.yml` contains **16 job definitions**. GitHub renders
all of their names even when the selected component route correctly skips a
job. Four Windows regression matrix cells and at most two manual cache
profiles can add displayed rows; they are **not extra application components**.
The `on: push` trigger applies only to `main` (in addition to PR and manual runs).
The GitHub job's YAML identifier may differ from its displayed name; branch
protection and accepted-artifact verification use the **displayed names**.

| Job displayed by GitHub | When it runs / what it verifies |
| --- | --- |
| `classify` | Every event, Ubuntu: immutable change scope, fail-closed owner routing and Windows Cargo closure |
| `draft-owner-feedback` | Draft PR, Ubuntu: selected owner/consumer feedback (`draft-component-tests` YAML ID); does **not** compile Windows-only code |
| `dependency-audit` | Ready/main for Rust owners: locked dependency graph, advisories, licences and sources; an applicable failure makes protected `linux-core` fail |
| `architecture-contracts` | Fresh non-Edge Rust changes: ownership, schema, product contracts and negative CI/release checks |
| `rust-tests-linux` | Full/Engine route only: portable Rust Clippy, tests, Research IPC and dev-isolated dispatch; **not** a Windows runtime test |
| `cloudflare-mcp-contracts` | Edge affected: TypeScript/MCP/DO/public-contract checks on Ubuntu |
| `linux-core` | Non-Draft, Ubuntu: protected result aggregator, **not** another Rust build; requires `dependency-audit` success for applicable Rust scopes, exact skipped status for docs/Edge-only |
| `host-controller-launcher-checks` | Ready Host/CONTROL or mixed Host+Research, Windows: Clippy, tests and two Host EXE builds, no Engine build |
| `trading-engine-acceptance-windows` | Fresh Full/Engine route, Windows: exact 62 financial contracts / 107 witnesses, recovery and resource proof |
| `trading-engine-regressions-windows (suite)` | Fresh Full/Engine route: four parallel Windows suites for Engine, Host, Execution and Research; Host also runs the unchanged native `--all-targets` Clippy with `-D warnings` after its tests |
| `research-service-tests-windows` | Fresh Research-only or mixed Host+Research PR, Windows: Research owner/IPC/WSS tests and scoped Research release evidence; no Engine build on Research-only |
| `windows-native` | Non-Draft, Ubuntu: protected aggregator of the **actual** required Windows jobs, not a Windows runner itself |
| `trading-engine-build-windows` | Fresh Full/Engine route, Windows: Engine-only or full four-EXE release artifact with exact SHA/tree |
| `warm-windows-cargo-cache (profile)` | **Manual** accepted-main cache seed (`trusted_main_cache_prime` YAML ID); choose `tests`, `release` (one Windows runner) or `both` (two runners, default). Compilation only, never acceptance/publish |
| `publish-accepted-windows-release` | Accepted Full main tree, isolated write permission: publish immutable verified four-EXE PR artifact without rebuilding or installing |
| `publish-accepted-research-component` | Accepted Research-only main tree, isolated write permission: publish exact three-file Research artifact and origin/readback without rebuilding |

The old `all26-slice-targeted.yml` workflow and Native/Worker JS case adapters
were retired: their `--case` invocation was already guaranteed to fail after
the Native finance cutover. The exact 26-ID archive manifest and offline
negative evidence verifier remain. Their removal does **not** delete any
current Rust financial witness.

The Linux-only, ignored million-terminal-intent `AccountStore` benchmark is
still runnable manually from source but **no longer runs on every Full PR**.
Its historical RSS/latency/index performance threshold is **not PR-gated**;
this is distinct from the Windows 62/107 financial acceptance and is not a
Windows performance measurement. A missing 100-parallel SQL test filter that
previously reported `0 passed` was also removed (no real test was skipped).

The required check **display names** `classify`, `linux-core` and `windows-native`
are retained exactly as configured in GitHub `main` branch protection.
`classify` is the actual `component_impact` job, without a redundant alias;
the aggregators keep YAML IDs `required_linux` and `required_windows`.
`linux-core` explicitly depends on `dependency-audit`: an applicable audit
failure or cancellation is **not** compatible with protected success, including
the accepted-`main` reuse route. Docs-only and Edge-only changes instead
require that audit job to be **skipped**, not falsely report a passing Rust audit.
Together, the protected aggregate statuses enforce the applicable Linux and
Windows evidence. On a fresh Full Native route, `windows-native` requires
finance acceptance, all four independent Windows regression suites (including
Host Clippy) and release build; `linux-core` requires Linux Rust, architecture,
dependency and Edge checks. On accepted-`main` reuse they verify the exact
previously successful PR tree instead. Neither claims skipped financial tests ran.
The old branch-only Draft jobs for closed issues #510 and #512 have been removed;
normal Draft owner feedback remains under its new display name.

Several additional job names are security contracts even though they are not
direct branch-protection contexts. The two main publishers verify exact
first-attempt Ready PR job rosters, including `dependency-audit` and all four
regression suites; installed CONTROL independently requires the existing
seven-job Research Ready roster (`classify`, `architecture-contracts`,
`dependency-audit`, `cloudflare-mcp-contracts`,
`research-service-tests-windows`, `linux-core`, `windows-native`). Removing or
renaming these statuses requires a separately versioned provenance policy,
negative controls and migration of the **installed** CONTROL verifier first.
Adding synthetic green alias checks or accepting only a workflow conclusion
would not be equivalent evidence.

CI classifies the diff for the exact commits in the GitHub event. Docs-only
changes use the protected classification and aggregate checks. Edge changes
also run the Worker contract job. Both routes skip Rust setup, Cargo audit,
Windows jobs and Native release publication; the protected Linux aggregate
checks these intentional skips. Research and Host changes run their owner
jobs. A Host and Edge change runs its affected component checks without
rebuilding the Engine. A Research-only PR builds its own three-file component;
the accepted `main` publisher reuses those exact bytes without fetching or
repackaging an Engine/Native base for the Research release. An automatic
accepted-`main` push does not claim or perform fresh Engine tests or
compilation. The separate, explicit manual cache-seed dispatch may compile
Engine **for cache only**; it cannot authorize publication. A changed/unknown
tree must take the appropriate fresh PR route; there is no
publish-by-recompile fallback on `main`.

Moving Clippy into the existing Host regression runner removes one Windows
matrix cell and one runner setup. It does **not** prove a shorter total CI
wall-clock: the Host tests and Clippy now execute sequentially on that runner.
Compare equivalent Ready-run queue, cache, build and test timings before
asserting any performance improvement. The optional manual cache seed is not
part of Ready acceptance: selecting one profile avoids running the other
Windows compiler job, while selecting `both` may start two. Cache warmup is
neither a financial test nor release evidence.

On the Windows production dependency graph, `okx-analysis` and
`okx-strategy-core` are outside Trading Engine, and Research builds
independently of `okx-engine-domain` and the venue execution adapters. The
remaining shared Engine IPC import of `okx-research-protocol` is explicitly
**not** accepted for production S1: the architecture guard must reject Research
protocol/domain in Engine's normal/build Cargo closure and record
`legacy-research-test` as open dev debt. The full CI route
also covers changes to CI policy or inputs whose impact the classifier cannot
verify.

The financial collector runs the seven wall-clock scenarios in an isolated
two-thread test group. Each test that expects a fresh public-time response
queues one response, timestamped at delivery by the recorded venue. Tests for
stale inputs still use their fixed recorded values. Production freshness,
SQLite deadlines and the collector's financial resource budgets apply as before.

The exact source tree is embedded in the Engine executable. Changing the
release label relinks the executable while Cargo can reuse the Engine library
when its inputs match. The running executable still rejects storage commands
that name a different release tree.

## Release acceptance for a future executable rename

1. Keep binary names and v2 archive/manifest schema stable in this code-name
   migration; compile and test the renamed Cargo packages and both old EXE names.
2. Require the full Windows finance (including B1), Linux, architecture and MCP acceptance
   once for this shared-dependency/workflow change. The Draft route is scoped.
3. Verify the exact immutable HEAD/tree, all four EXE hashes, and the installed
   CONTROL/Launcher/Engine/Research compatibility before deploying anything.
4. A later Windows-identity migration needs a typed, rollback-capable
   controller transition and explicit approval. Never manually replace the
   installed Engine, restart trading or repack mixed provenance.
5. After branch protection is migrated to new aggregate display names, remove
   the old CI aliases in their own independently checked change.
