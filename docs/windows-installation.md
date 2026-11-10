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
`.github/workflows/ci.yml` builds all three EXEs in one Windows bundle and records
their SHA-256, exact source HEAD/tree and Rust version in `manifest.json`.
The existing CONTROL operations deploy Agent, stage/activate controller and
upgrade Launcher from an explicitly identified GitHub run/artifact.

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
