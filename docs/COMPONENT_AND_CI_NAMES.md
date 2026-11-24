# Canonical component and CI names (#537)

Names describe **ownership**, not historical implementation details. A Cargo
package is a compilation boundary, a Windows executable is a release artifact,
and a CI job is evidence. They are not interchangeable.

## Four application owners (current `main`)

The deployed product has four logical owners; **17 Cargo packages do not mean
17 services**. Shared libraries are compile-time dependencies, never extra
financial authorities. Source paths in this table refer to the current code,
not evidence that all intended runtime boundaries have already been achieved.

| Owner | Code and durable state | Exclusive effect | Must not own |
| --- | --- | --- | --- |
| **Engine** | `okx-trading-engine`, `okx-trade-execution`; one financial SQLite/WAL, RiskBook, private account keys | Financial admission, durable intent, at-most-once venue send, reconciliation, protection | Research algorithms, hypothesis jobs, Research's dataset/catalog storage |
| **Research** | `okx-research-service`, `okx-strategy-core`, `okx-analysis`, `okx-research`; own research jobs/data | Public-source ingestion, causal computation, replay, typed proposed signal | Private credentials, account risk, finance SQLite, exchange POST |
| **Control** | `okx-host-controller` and `okx-host-launcher`; installed component manifests and lifecycle evidence | Authorized start/stop/install/rollback/doctor | Trading decisions, finance ledger, research computations |
| **Edge** | `cloudflare/okx-mcp`, `okx-bridge-mcp`; transport/session/identity state | Authenticated MCP/API delivery to the matching owner | Financial or research source-of-truth state, venue mutation |

`okx-domain-core` and `okx-ipc-contracts` are shared pure facts and typed
Engine/Research contracts. Host-control messages live in `okx-host-protocol`;
only the child shutdown request/reply remain in the shared Engine/Host IPC.
`okx-api`, `okx-market-runtime`, `okx-observation`, `okx-ws` and
`okx-github` contain current adapters/support code; their current broad
dependency fan-out is **not** a reason to grant another owner financial
authority. `okx-cli` is a tool, not a fifth continuously running application.

### Confirmed remaining coupling and release rules

The Engine does **not** compile the Research crate, but
`trade_service/strategies.rs::strategy_turn` currently awaits Research's
`evaluate_signal` after loading candles. `research_client.rs` also proxies
public-source requests through Engine's `MarketBootstrapper`, and the Edge
MCP transport currently routes research commands through Agent dispatch.
These are real runtime couplings. A diagram showing four boxes is insufficient
evidence of independence; [the live source and transition map](research-boundaries.md)
records the actual code-level boundary.

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

## Rust packages

| Canonical Cargo package / directory | Previously called | Responsibility |
| --- | --- | --- |
| `okx-trading-engine` | `okx-agent` | Trading process: accounts, positions, financial decisions, exchange sends and recovery |
| `okx-trade-execution` | `okx-execution` | Engine-owned RiskBook, finance/SQLite and order execution; **not** another process |
| `okx-domain-core` | `okx-kernel` | Shared financial/domain facts and deterministic rules; no I/O or Research time-series implementation |
| `okx-ipc-contracts` | `okx-protocol` | Shared versioned Engine/Research IPC and typed command contracts |
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

Any future credential target migration requires explicit, separately accepted
secret-preserving migration and rollback; never silently write a renamed target.

Historic release manifests, stored source SHA256, previously installed roots
(`C:\okx-runtime`) and other provenance records are facts, not identifiers to
rewrite. Do not silently rename them or fake a new accepted binary. There is
one financial SQLite owner **inside** Trading Engine, not a SQL service.

The accepted **Windows binary baseline** (formerly called “Native base”)
means proven reusable binary bytes, not a runtime library. Existing
`native-base-<sha>` release tags remain immutable compatibility identifiers.

## CI job names

| Job | Purpose |
| --- | --- |
| `classify` | Protected fail-closed changed-file boundary plus before/after Windows Cargo closure proof for narrow Research/Host manifest candidates |
| `draft-component-tests` | Scoped feedback for Draft pull requests |
| `trading-engine-acceptance-windows` | Mandatory complete financial collector/resource-budget acceptance and isolated Engine, SQLite, clock/process recovery tests |
| `trading-engine-regressions-windows` | Parallel independent Engine/Execution/Host/Research regressions, Windows Clippy and K9; never substitutes for financial acceptance |
| `trading-engine-build-windows` | Parallel fresh four-component Windows release build and artifact on full Ready PRs; manual accepted-`main` release cache seed |
| `trusted-main-windows-test-cache-prime` | Optional manual compile-only test/collector cache seed from accepted `main`; no financial acceptance or deployment |
| `research-service-tests-windows` | Research Service tests and build against the accepted unchanged Trading Engine |
| `host-controller-launcher-checks` | Host Controller and Host Launcher tests/build without the Trading Engine |
| `rust-tests-linux` | Rust library, Research Service IPC and financial SQLite tests executed on Linux (not a Linux trading service) |
| `architecture-contracts` | Dependency, architecture, product and acceptance contract guards |
| `dependency-audit` | Dependency security, licence and origin checks |
| `cloudflare-mcp-contracts` | MCP transport and public contract checks |
| `research-package-proof` | Draft proof of a composed accepted Research candidate, not a deploy |
| `publish-accepted-windows-release` | After successful protected main CI, publish only the exact verified Windows artifact; no installation |

The required check **display names** `classify`, `windows-native` and `linux-core`
are retained for branch protection and accepted-CI reuse. `classify` is the
actual `component_impact` job, without a redundant alias; the platform
aggregators keep implementation IDs `required_windows` and `required_linux`.
They require finance acceptance, independent regressions and release build on the
full Native route; on reuse they verify the accepted exact tree instead. They
never claim that skipped financial tests ran. The old branch-only Draft jobs for closed
issues #510 and #512 have been removed; normal Draft owner tests remain.

CI classifies the diff for the exact commits in the GitHub event. Docs-only
changes use the protected classification and aggregate checks. Edge changes
also run the Worker contract job. Both routes skip Rust setup, Cargo audit,
Windows jobs and Native release publication. Research and Host changes run
their owner jobs. A Host and Edge change runs their two component jobs without
rebuilding the Engine. A Research release verifies and fetches the accepted Native
base once, in the job that assembles the artifact.

On the Windows production dependency graph, `okx-analysis`, `okx-strategy-core`
and the Research crates are outside Trading Engine. Changes to `okx-domain-core`,
`okx-ipc-contracts`, `okx-market-runtime` and other Engine inputs still affect
its binary or the contract it implements. The full route also covers changes
to CI policy or inputs whose impact the classifier cannot verify.

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
