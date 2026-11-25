# Windows installation and GitHub updates

One product root, independent owners:

```text
C:\okx\
  control\      immutable Launcher, controller versions, activation/lifecycle state
  runtime\      installed Agent, catalog, account SQL/WAL, research, backups
  maintenance\  migration inventory and previous Scheduler definition
```

`C:\okx\workspace` is an optional developer checkout. It is not created by
installation. Neither installed Agent startup nor artifact verification requires
Git, Cargo, MSVC or this directory. Do not initialize `.git` at `C:\okx`: a
checkout must never contain the product's control or financial data.

## GitHub is the distribution authority

The source owner is `iamaman11/okx`, pinned repository ID `1388071566`.
`.github/workflows/ci.yml` builds all four EXEs (`okx-trading-engine`, `okx-host-controller`,
`okx-host-launcher`, `okx-research-service`) in one Windows bundle and records
their SHA-256, exact source HEAD/tree and Rust version in `manifest.json`.
The existing CONTROL operations deploy Agent, stage/activate controller and
upgrade Launcher from an explicitly identified GitHub run/artifact. **This is
the full Native release path, not the Research-only release format.**

### Research-only component update

The standalone Research artifact is **not** a four-EXE composition and does not
borrow the installed Engine or a pinned Native-base archive. The Research-only
Windows job builds only `okx-research-service.exe` and uploads the raw GitHub
PR `CI` Actions artifact named `okx-research-component-<PR_HEAD_SHA>`, with
exactly these three root files:

- `Cargo.lock`
- `manifest.json` (`okx.windows.research-component/v1`)
- `okx-research-service.exe`

The strict manifest binds repository ID `1388071566`, exact tested head/tree,
Rust 1.100.0, complete Cargo.lock SHA-256, Research executable SHA-256 and
length, protocol version 1 and storage version 1. Extra entries, absent files,
unbounded bytes and unknown manifest fields fail closed. **No Agent, Engine,
CONTROL or Launcher executable belongs in this component artifact.**

On a Research-only merge with an identical tested/merged source tree, the
protected `main` CI job rechecks the same accepted Ready PR run attempt,
required job results, immutable raw Actions ZIP digest and source provenance.
It publishes the unchanged bytes under the append-only GitHub release tag
`research-component-<MERGE_SHA>` with exactly
`okx-research-component.zip`, `published-research-origin.json` and
`SHA256SUMS.txt`. An existing tag is never overwritten. This public Release
is immutable distribution/provenance, **not** a deployment or a second build.

`HostControlOperation::DeployResearch {run_id, artifact_id,
expected_source_tree}` authorizes CONTROL to fetch the **raw accepted PR CI
artifact**, not to silently substitute Release ZIP bytes. CONTROL checks the
same-attempt Ready gates, artifact name/digest, exact GitHub tested and current
main trees, three-file format, manifest compatibility and active Controller
identity. It stages the Research executable in the existing content-addressed
Research storage and records the selected image. The separate CONTROL
`StartResearch`/`StopResearch` and Research-owned Edge WSS path own the process
lifecycle; no Engine `ResearchClient`, Engine restart, trading authority or SQL
writer is involved. A deployed image or `StatusResearch` process receipt alone
does not prove a successful authenticated Edge session or completed job replay.

Research-only installation **requires a previously installed, verified
CONTROL + Launcher root** and its matching active Controller version. It does
not install those prerequisites. In particular the virgin-host path is
**NOT_PROVEN**: `scripts/windows-bootstrap-release.ps1` currently recovers
CONTROL from a full Native bundle, while `InstallLauncherRoot`/root migration
requires a valid existing Scheduler policy; on a clean Windows host that task
may be absent. Do not bypass the Scheduler checks, install an unverified EXE,
create a new Engine/account SQL installation, or claim physical S2 acceptance
until a disposable Windows bootstrap and a real installed handoff have passed.

Preserve prior Research scope, job IDs, catalog/artifacts and stopped-worker
rollback provenance. Verify CONTROL's installed and active Research hashes,
process generation, Windows Job/CPU/RAM limits, authenticated WSS readback,
existing-job status/result, stopped/restarted behavior and a genuine
Research-only update/rollback **on the exact accepted installed candidate**.
The artifact and CI controls above establish CODE paths only; physical Windows
and user-facing Edge acceptance remain **NOT_PROVEN**.

Full Native installation/update must retain all of these checks (Research-only
component validation follows the distinct three-file rules above):

1. Canonical repository/owner identity; successful, completed PR `CI` run.
2. Unexpired artifact named `okx-windows-bundle-<exact-tested-head>` in that run.
3. Manifest repository, source SHA and requested tree match that run.
4. Authenticated GitHub commit-tree readback for both tested HEAD and current
   `main` equals the manifest tree. A Draft PR PASS alone is not production
   promotion. No local checkout/fetch is involved in this check.
5. Hash each component before installation. Launcher-root bootstrap also
   requires the executing trusted controller to match the accepted bundle.
6. Preserve the existing activation/terminal-ACK/readiness/rollback protocol;
   use one fixed Scheduler entrypoint `C:\okx\control\okx-host-control.exe`.
7. Read installed provenance and actual executable hashes back; controller and
   Agent must belong to the accepted release set. Recover from failures before
   reporting the update successful.

`.github/workflows/promote-production-recovery.yml` publishes a versioned GitHub
release from an already tested artifact after main promotion with equal trees.
It does not rebuild or overwrite an existing release tag. Initial bootstrap must
download the explicitly accepted GitHub package and verify its manifest/hashes
before invoking the trusted controller; the bare `install` command is a copy
operation, not an artifact verifier. Do not install arbitrary local build output,
pick an old `latest` release automatically, or run copied legacy binaries.

## Migration from the legacy three-root installation

Run PowerShell 7 from the reviewed source of this change:

```powershell
./scripts/windows-unify-root.ps1          # plan and inventory only
./scripts/windows-unify-root.ps1 -Apply   # maintenance cutover, does not start OKX
```

The script requires all Agent/Control/Launcher processes stopped, real directories
without reparse points, no pending controller handoff/update, and no existing
checkout at the product root. It disables and archives the old scheduled task,
records file lengths and SHA-256, then renames the two legacy directories on the
same volume. SQL, WAL, SHM, backups, hidden files and empty directories are verified
at the destination. Conflicting destinations are never merged or overwritten.
The fixed-map journal lets a repeated invocation finish after a directory rename
without redoing it. Verification failures leave scheduling disabled.

Successful data migration reports `DATA_VERIFIED_AWAITING_GITHUB_INSTALL`.
This is deliberately not `READY`: the old EXEs still target the legacy paths.
Install the GitHub-built unified-root release before restoring scheduling.
Do not create compatibility junctions or run old/new versions concurrently:
their account-lease namespaces differ. Credential Manager records and account
identity remain unchanged. A missing catalog is a recovery blocker, not permission
to initialize a new installation over existing SQL.

The migration receipt proves the bytes moved; it does not prove a successful
installation, OAuth login, connected Engine, restored account identity or trading.
Those need separate readbacks after the accepted GitHub release is installed.

## One-time recovery when CONTROL cannot answer

Use the existing GitHub CONTROL for normal upgrades. If its entrypoint is
missing and Scheduler targets a missing EXE, ChatGPT cannot repair that host
remotely. Perform one bounded local bootstrap, not another permanent service.

1. Review and run scripts/windows-install-preflight.ps1 in PowerShell 7.
   The output shows only root/EXE/Scheduler/process presence; it never reads
   SQL contents or secrets, and it changes nothing.
2. If legacy roots remain, run scripts/windows-unify-root.ps1 first in plan
   mode. Use -Apply only after stopping the installed processes and verifying
   the physical inventory and journal. Preserve existing SQL/WAL/SHM; do not
   initialize another account over it or delete conflicting state.
3. After protected merge to main, and a matching published production recovery
   release from the accepted tree, execute from the reviewed source checkout:

       pwsh -NoProfile -File .\scripts\windows-bootstrap-release.ps1 -MainSha <accepted-40-character-main-sha>

   The script checks the canonical current main, release tag/assets, archive
   SHA-256, bundle manifest, all three binaries and tested source tree. It
   refuses legacy roots, running processes, existing entrypoint, active
   Launcher state and enabled Scheduler. It uses only the existing native
   controller install operation and returns
   CONTROLLER_ENTRYPOINT_RESTORED_NOT_RUNNING.
4. Start C:\okx\control\okx-host-control.exe run as a temporary foreground
   operation on the workstation. Once it responds, use existing typed GitHub
   CONTROL to install the verified launcher/Agent, enable one canonical
   Scheduler task and read back the live PID, hashes and release identity.
5. Deploy the same accepted main tree using the existing main-only
   cloudflare-mcp-deploy.yml and verify OAuth, MCP tools/list, WSS, Windows
   read-only account and research workflows. CI or /health alone is not
   installed/physical acceptance.

### One-time isolated Cloudflare governance for clean native v2

The new native v2 Windows installation uses a new empty account catalog and
one native financial SQLite owner. The Cloudflare Worker routes both runtime
WSS and MCP governance to the distinct named Durable Object
`primary-clean-v2`; the prior `primary` object is **preserved**, neither reset
nor copied. Deploying a Worker to that empty new object leaves tools fail-closed
until a new governance installation is explicitly initialized.

For the first v2 deployment only, dispatch `cloudflare-mcp-deploy.yml` on
`main` with `initialize_clean_v2=true`, the exact installation ID read back
from Windows CONTROL, and `accepted_bundle_sha256` from the verified GitHub
Windows artifact. The protected GitHub environment provides the existing owner
secret; never paste it into dispatch inputs or logs. The workflow first checks
those inputs, builds and deploys exact-main Worker configuration, waits for its
source-tree health evidence, then performs a single owner-authenticated
zero-account initialization and strict readback. A changed body cannot reuse
the idempotency operation; an already initialized or inconsistent DO refuses
replacement. For later deployments, leave `initialize_clean_v2=false` and
require the existing read-only owner inspection to pass.

This cutover is **not** account enrollment, a financial state migration,
authorization to trade, or proof of a fresh WSS session. A clean initial
governance configuration has zero linked accounts, and old real account access
stays confined to the preserved legacy DO. Validate the new native installation
ID, fresh transport, read-only account list and research workflows separately.

These scripts do not authorize exchange POST, reset RiskBook, clear old
campaign spending, or prove REAL readiness. There is no destructive fallback
for unknown or inconsistent host state.

## Acceptance

- `scripts/test-windows-unify-root.ps1`: exact byte/empty-directory preservation,
  tamper/conflict rejection, restart after rename, path and reparse-point guards.
- Rust launcher/controller/GitHub tests: unified root, isolated optional checkout,
  installed Agent runtime working directory, Scheduler policy, commit-tree binding.
- On the workstation: old roots absent; destination inventory equals the journal;
  Scheduler disabled until replacement; no old EXE process running.
- After release install: one fixed task under `control`, verified active version
  and Agent provenance, no checkout required; account catalog matches existing SQL.
- MCP acceptance: OAuth initialize/tools/list, connected runtime diagnostics,
  account read-only state, research run/result/export/reopen. No exchange POST
  belongs to this migration or initial acceptance.

## Native whole-finance maintenance (#510 candidate)

The new Native format has one `runtime/finance/finance.sqlite` for all accounts
and campaigns. A verified release provides `okx-trading-engine --root <runtime-root>
backup-finance` and `restore-finance --snapshot-id finance-<32-lowercase-hex>
--expected-release <accepted-release> --operation-id <unique-operation>`.
These are stopped-Engine maintenance operations, not install/update commands.
The Draft #510 candidate is not permission to run them on an existing host.

Backup requires the existing verified catalog/initialization metadata, no Engine
or financial writer, and a recognized root. It writes one whole-finance snapshot
under `finance-backups`; SQLite backup includes committed WAL. It never copies
credentials or creates account/installation identity. The original DB/WAL bytes
remain unchanged. The archived DB carries a checksummed recovery-history cutoff.

Restore requires the same existing identity/catalog and an ABSENT financial DB;
existing DB, orphan WAL/SHM, unknown files, active writers, invalid manifest,
schema or cohort refuse. It reads the published archive without modifying it,
validates a private staging copy, and never replaces an existing target. An
interrupted staging file remains an explicit blocker. This is not automatic
repair or rollback of a corrupt/current financial database. Restored accounts
remain fenced until the original exchange/storage audit proves the missing
history; ordinary NAV capture alone cannot clear that audit. An archive older
than the supported exchange order-history window can remain unproven.
