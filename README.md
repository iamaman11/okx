# okx

Rust/Tokio OKX observation, analysis and execution-safety platform for the current Windows + GitHub environment.

The production architecture is deliberately small: one Windows supervisor chain, one observation runtime, one mutation owner, typed encrypted access, and hosted-CI artifacts as the only normal deployment authority.

## Current production state

Accepted:

- M1 reference data — PASS;
- M2 public REST market state — PASS;
- M3 persistent public WebSocket — PASS;
- M4 private read-only account/order state — PASS;
- M5 deterministic cost/risk/scenario analysis — PASS;
- M6 bounded application/research query surface — PASS;
- H1 GitHub transport/context hardening — PASS;
- A2 reboot + real external network-loss recovery — PASS;
- Phase 2 pre-enable execution boundary — PHYSICAL PASS with live writes hard-disabled.

Current canonical work cursor: **#113 Production Readiness Closure**.

Live trading is **not enabled**. Production construction of the executor remains disabled before SUBMITTING persistence and before any exchange mutation.

## Architecture

```text
GitHub-hosted CI
  -> locked Rust dependency graph
  -> Linux + Windows CI
  -> exact tested Windows bundle + manifest + SHA-256
  -> verified deploy / durable recovery release

Windows Task Scheduler
  ONE TimeTrigger / PT1M / StartWhenAvailable / IgnoreNew
        |
        v
ONE fixed Scheduler entrypoint: C:\okx-control\okx-host-control.exe
  immutable launcher binary after one-time root migration
  content-addressed controller selection
  transactional activation + rollback
        |
        v
ONE versioned okx-host-control.exe
  desired state + mutex + Job Object
  fixed typed CONTROL operations only
        |
        v
ONE okx-agent.exe
        |
        +--> Reference / Market / Account / Orders
        |      -> reconciliation / readiness
        |      -> immutable snapshots
        |
        +--> pure deterministic Analysis
        |
        +--> ONE hard-disabled OrderExecutor
        |      -> durable mutation ledger
        |
        +--> encrypted typed DATA mailbox
```

## Ownership

- `okx-api`: typed OKX REST/auth/exchange primitives only;
- `okx-ws`: WebSocket protocol/transport only;
- `okx-runtime`: Tokio WS lifecycle/reconnect/subscription ownership;
- `okx-observation`: normalized reference/market/account/order state, reconciliation and readiness;
- `okx-analysis`: pure deterministic Decimal/fixed-point analysis;
- `okx-execution`: sole mutation owner, idempotency, durable ledger and UNKNOWN_SUBMISSION recovery;
- `okx-agent`: composition/query/access adapter only;
- `okx-github`: GitHub transport primitives;
- `okx-protocol`: versioned DATA/CONTROL contracts;
- `okx-host-launcher`: immutable one-shot controller root-of-trust; hash verification, activation and rollback only;
- `okx-host-control`: Windows lifecycle/deploy/diagnostics only.

No observation, analysis, transport or host-control component may directly become a trading engine.

## DATA and CONTROL

DATA #10 is the encrypted application/query transport:

```text
ChatGPT -> encrypted typed request -> GitHub #10 -> okx-agent
        <- encrypted terminal result <-
```

CONTROL #12 is the typed Windows lifecycle/deployment transport:

```text
ChatGPT -> fixed allowlisted control request -> GitHub #12 -> okx-host-control
        <- bounded typed result <-
```

CONTROL has no arbitrary shell/PowerShell/HTTP/path execution surface. Legacy remote `BuildAgent` is protocol-compatible only and fails closed; normal production deployment is verified hosted-CI artifact deployment.

Both transports have physically passed restart and real external GitHub/network-loss recovery. Controller replacement is being closed under #126: successful PR-CI bundle -> immutable content-addressed version -> durable CONTROL ACK -> unchanged Scheduler invokes the fixed launcher entrypoint -> exact old-process identity exit -> readiness proof -> commit or automatic rollback. The Scheduler definition is unchanged even by the one-time root migration.

## Account and execution boundary

Production account target: standard sub-account `Succession`.

- observer credential: Read only;
- executor credential: Read + Trade;
- Withdraw: never;
- Futures account mode;
- long/short position mode;
- executor IP binding: optional diagnostic evidence, not a production acceptance requirement;
- credentials remain outside Git and public GitHub plaintext.

A prepared execution plan is not a timeless permit. Any future live send must reacquire authoritative reference/account state and pass exact pre-send continuity checks.

Live-write enablement is a separate future authorization after #113 closes.

## Supply chain

- Rust toolchain: 1.95.0;
- workspace `Cargo.lock` is committed;
- production CI/build paths use `--locked`;
- GitHub Actions used by canonical CI/account acceptance are pinned to immutable commit SHAs;
- successful PR acceptance may be reused after merge only when merged tree exactly equals the tested PR head tree;
- durable production recovery bundles are promoted to versioned GitHub Releases after exact-tree + manifest + binary-hash verification;
- normal local Windows build/install is not a production deployment authority.

`main` branch protection is enabled with required `classify`, `linux-core`, and `windows-native` checks; tested-tree/merged-tree equality remains the artifact reuse invariant.

## Windows filesystem boundaries

```text
C:\okx          mutable canonical Git workspace
C:\okx-control  immutable launcher + versioned controllers + activation state
C:\okx-runtime  installed agent + runtime state/logs/staging
C:\okx-upgrade  temporary bootstrap/upgrade staging only
```

Installed production binaries do not execute from the mutable source checkout.

## Canonical documentation

- [Architecture](docs/ARCHITECTURE.md)
- [Roadmap and production-closure cursor](docs/ROADMAP.md)
- [Encrypted GitHub mailbox](docs/encrypted-github-mailbox.md)
- [ChatGPT operator contract](docs/chatgpt-operator-contract.md)

Canonical issues:

- #113 current production-closure cursor;
- #5 product/domain architecture;
- #7 Windows runtime/deployment;
- #10 encrypted DATA transport;
- #12 CONTROL transport;
- #3 execution boundary;
- #47 capability matrix;
- #126 immutable controller launcher/root-of-trust — ACTIVE until one-time root migration plus remote commit/rollback/reboot physical acceptance;
- #54 controller self-update — absorbed by #126 for final lifecycle closure;
- #58 mailbox compaction — trigger-based deferred maintenance.


### Windows desktop behavior

The production Windows control plane is intentionally background-only. The fixed Task Scheduler
entrypoint is a Windows GUI-subsystem launcher and all controller-owned child processes use
`CREATE_NO_WINDOW`. Normal polling, controller recovery, agent lifecycle, diagnostics, and
verified launcher-root upgrades must not create console or PowerShell windows on the interactive
desktop. Launcher-root upgrades are verified against an exact successful PR-CI artifact and the
expected current launcher SHA-256; they never mutate the Scheduler definition.
