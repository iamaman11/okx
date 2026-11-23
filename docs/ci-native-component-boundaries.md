# Native CI ownership and build boundaries (#537)

The protected checks `linux-core` and `windows-native` stay required. They
aggregate different **executed** jobs; a skipped financial matrix is not
reported as a successful financial test run.

| Changed files (complete GitHub diff) | Direct owner and consumers | Ready CI | Native release artifact |
| --- | --- | --- | --- |
| Only `crates/okx-host-launcher/src/**/*.rs` (+ docs) | Launcher, plus Host Controller consumer | `host-lifecycle-windows`; no Engine/SQL compilation | None: host identities are not an installable four-EXE bundle |
| Only `crates/okx-host-controller/src/**/*.rs` and optional Launcher Rust source (+ docs) | Host Controller and Launcher | `host-lifecycle-windows`; no Engine/SQL compilation | None |
| Trading Engine/TradeService, `okx-trade-execution` or financial SQL | Engine, RiskBook, SQL and trade-side consumers | Mandatory `trading-engine-windows-full`, B1, Linux financial regressions | Full verified Windows bundle on qualifying PR |
| Shared `okx-api`, `okx-market-runtime`, `okx-ipc-contracts`, root Cargo files, any `Cargo.toml`, build scripts, installer/packaging scripts or CI workflow | All affected consumers | Full Native matrix (fail closed) | Full only after acceptance |
| Research-only + accepted immutable Native base | Research/IPC consumers | Separate Research gate with independently verified Cargo closure | Composed Research-only bundle |
| Documentation only / Cloudflare-only | Documentation / MCP | Existing scoped gates | No fresh Native build |
| Mixed host + Research or Cloudflare, unknown paths, renames, incomplete diff | Unknown/shared impact | Full Native matrix | Only if accepted |

The host-only exception is intentionally **narrow**: just Rust implementation
files inside the two host crates. Changes to dependencies or installation
contracts cannot claim source-only independence. The classifier reads every
GitHub diff page, rejects renames and ambiguous paths, and has negative tests
in `scripts/check_ci_native_scope.py --self-test`.

`scripts/check_architecture.py` separately rejects direct and transitive
financial/market dependencies from CONTROL or Launcher, as well as a Launcher
dependency on CONTROL. CONTROL may depend on Launcher and the versioned
protocol. The financial SQLite owner remains in `okx-trade-execution` and its
TradeService/Engine consumers remain in `okx-trading-engine`; this stage does **not**
claim separate financial SQL and Engine release cycles.

The host job runs the existing installation-root safety fixture, architecture
guard, formatting, clippy, Rust tests and Windows release builds for both
CONTROL and Launcher. Its exact source HEAD, tree, component scope and binary
hashes are retained as **non-deployable** evidence. It never publishes a new
Native base; `.github/workflows/publish-native-base.yml` continues to require
fresh `trading-engine-windows-full` plus B1 and a complete four-EXE artifact.

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
