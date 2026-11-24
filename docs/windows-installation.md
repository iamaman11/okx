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
upgrade Launcher from an explicitly identified GitHub run/artifact.

### Research-only component update

**One-time migration from the installed legacy CONTROL:** The #545 controller
requires the v2 archive's Research entry to be named
`okx-research-worker.exe`, while the new native release names it
`okx-research-service.exe`. It therefore rejects the new full v2 archive with
`ARTIFACT_VERIFICATION_FAILED: missing component` before staging its
replacement. Do not retry the incompatible release, rename files in the
archive, or replace installed binaries manually. The existing CI packager
supports a single tree-pinned `.github/windows-legacy-control-bridge.txt`
marker that emits a verified *v1 three-EXE* bundle for the old CONTROL to
stage the updated controller. Agent/Launcher can be deferred until the next
accepted full v2 release, avoiding a redundant Agent installation. This bridge
contains no Research executable and cannot certify Research activation. After
the Controller handoff is physically confirmed, remove the marker on a newly
tested source tree to restore the ordinary v2 Service artifact. That release
must pass the full Native/financial gate; stage its matching Controller, then
install Launcher, Agent, and Research through authenticated CONTROL and verify
all four installed identities. Finally test a separate Research-only update
and rollback. Engine PID continuity begins at the post-migration baseline,
not at the pre-migration process: a Controller handoff can replace the Agent's
owned process even without changing Agent executable bytes.

`HostControlOperation::DeployResearch` takes `run_id`, `artifact_id`, and
`expected_source_tree`. CONTROL fetches the accepted GitHub archive and verifies
its v2 composition, component bytes and origins, source tree, and compatibility.
It checks the reused Agent and Controller against their installed provenance and
executable hashes, and checks Launcher bytes. A verified Worker is staged in a
content-addressed location and `installed-research.json` records the candidate.
The running Agent's `ResearchClient` separately drains the old child and activates
the verified candidate; installation and active worker are distinct states. A
failed validation does not authorize activation or a financial Engine restart.
Use the existing authenticated CONTROL operation, not an unverified local EXE.

When an already verified Research image is replaced, CONTROL first validates
its saved provenance and content-addressed binary, then persists
`runtime/rollback-research.json` before atomically publishing the new
`runtime/installed-research.json`. An exact redeploy retains the rollback slot.
The typed `RollbackResearch` operation requires both the exact current Worker
SHA-256 and the exact saved rollback Worker SHA-256. It validates both images,
rejects an absent, corrupted, linked, or mismatched rollback image, and
atomically restores the saved pointer without touching Agent, Controller,
Launcher, Engine lifecycle, or financial state. This operation intentionally
does not compare the old image's tree with the *current* `main`: older verified
Research versions would otherwise become impossible to restore. CONTROL
records a terminal ACK; the Agent independently verifies the selected bytes
before the next child activation. A rollback ACK alone does **not** prove that
the old child became active, nor does it certify unchanged running Engine
SHA-256/PID: obtain both physical readbacks before and after a Research-only
update and its rollback.

Successful Research deploy and rollback receipts include `timing_ms` for CONTROL
verification and pointer publication. Deploy also measures the initial artifact
fetch. `activation: "requires_engine_observation"` marks the next step. Measure
time to the expected active PID/SHA separately through the Engine's
[Research deployment diagnostic](research-boundaries.md#installation-versus-activation).

For a captured update or rollback, save the CONTROL `status` result and the
MCP diagnostic `structuredContent` both before and after the operation. Save
the terminal CONTROL deploy or rollback result as `change.json`. Check them
from the repository with:

```powershell
python scripts/check-research-handoff.py before-control.json before-research.json change.json after-control.json after-research.json
```

This checks the selected Research SHA against the active post-switch child and
compares the Engine PID, incarnation and verified binary SHA across the change.
The report covers the captured handoff. Separate workload and financial tests
cover position protection and SQLite continuity.

This describes the implemented path, not installed acceptance. #512 r15 checks
the separate Worker build and existing verification/staging/handoff contracts.
The first release requires full CI, merge and the existing accepted manifest;
later Research-only releases reuse that verified, merged base. The successful
first full-build CI on `dacab8c5` does not itself exercise the separate
`research-service-tests-windows` branch, which requires an explicitly selected accepted
base. Actual host installation, process/PID continuity and physical/live acceptance
remain under #449. An expired or missing origin artifact is rejected; there is
no additional credential-backed archive cache or recovery installer.

Production installation/update must retain all of these checks:

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
