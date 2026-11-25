# #537 / #518 T10 — accepted-origin Research v2 candidate

> **Historical evidence only (superseded by #580 S2/S3).** The mixed-origin
> v2 composer and its negative fixture remain available for audit, but CI no
> longer runs the `research-composition-evidence` Draft job, and CONTROL no
> longer accepts a v2 bundle for `DeployResearch`. Current Research deployment
> requires the strict three-file `okx.windows.research-component/v1` PR Ready
> artifact; see `docs/windows-installation.md`. Nothing in this archived T10
> candidate constitutes present-day deployment authorization.

This is a **candidate-only** acceptance. It does not install software, restart
Agent/Engine, activate Worker, alter accounts, or certify physical T10. Its top
source identity was the candidate PR head/tree. Its then-proposed CONTROL
promotion mechanism has since been retired; the modern accepted-artifact
boundary is described separately in #580.

## Fixed accepted origins (22 November 2026)

| Component | Accepted run | Artifact | Original source HEAD | SHA256 |
|---|---:|---:|---|---|
| Installed Agent | 44912048553 | 14710029752 | `36d79e634f59066943e58b9a8a017a15670caef7` | `3d2672e42e8795295bb5d43a95feb1f40e6d4e654e3c93b7d512f821347540e2` |
| Installed Controller | 44912048553 | 14710029752 | same | `e210ed72a496571a09d0534af57dc3d160466e585877104ad1fbea433c4cc825` |
| Installed Launcher | 44912048553 | 14710029752 | same | `29434651745d0064e3a0cdf43c32b093a6615f3af841c77359d15b4b6e79707b` |
| T10 Research Worker | 45057675543 | 14769181785 | `73ba2a88b85c1fff017c118e65c2449d2efe570a` | `d9e413499736edd527c31b56e693e2d818ddb89f9b61e36eb88dd2750a4b548e` |

The three installed Native hashes were read back on Windows and the archived
component bytes were independently hashed against each original manifest.
The installed Worker is still `0efef9de575f87e23703cb94d92ec4b6fd38292a0577f13a2084fb524035d0f5`.
Both source commits are merged into `main`; the T10 PR tree and merge tree are
`35d7308da5653ca1ca6963683c64f60183c2f1ca`.

## One existing packaging entrypoint, explicit base

`scripts/prepare-research-release.ps1 -AcceptedCompositionSpec` selects a
reviewed installed base and one separately accepted Research origin. The
existing default `-VerifyOnly` and ordinary Research-only release path remain
unchanged. The composition helper downloads both exact GitHub CI artifacts,
checks CI event/status/conclusion, canonical repository, artifact identity,
merged PR ancestry, source Git tree, all six exact archive entries and every
component SHA256/size/IPC/storage version. It emits a v2 bundle of **exactly**
`manifest.json`, `Cargo.lock`, `okx-agent.exe`, `okx-host-control.exe`,
`okx-host-launcher.exe` and `okx-research-worker.exe`.

Each Native component retains its `44912048553/14710029752` source origin;
Worker retains `45057675543/14769181785`. The top-level candidate source
HEAD/tree is the tested composition PR HEAD/tree. This was the mixed-origin
format proposed for the former `DeployResearch` path; **current CONTROL
rejects it**. It must not be passed to a full-Agent deployment operation as a
substitute for a verified Agent origin.

Historically, the `research-composition-evidence` PR label triggered an isolated
Windows candidate job in Draft CI, producing `okx-windows-bundle-<PR_HEAD>`.
That job and artifact path have been **removed**. The historical verification
scripts and proof fixtures remain as regression evidence, but are not a
deployable Research component or a durable independent release.

## Negative acceptance and exact boundary

`node scripts/test-compose-verified-research-v2.cjs` proves that the candidate
validator rejects a different installed base, matching bytes with forged
artifact/source origin, damaged EXE, unknown Research filename, altered source
tree, incompatible protocol/storage version and mismatched `Cargo.lock`.
Real downloads add the GitHub/merged-tree readback unavailable to pure fixtures.
The packager then repeats five negative substitutions against the **actual
downloaded component bytes** and records each typed refusal in the proof file.

The test uses the **historical** CONTROL v2 schema and per-component rules,
but does not call the installed CONTROL operation. Final installed verification
requires a separately accepted/merged **v1 standalone Research** release and
readback of Agent PID/SHA, Research PID/SHA, source/storage I/O, peak RSS,
Windows committed-memory pressure, cancellation and recovery under load.
