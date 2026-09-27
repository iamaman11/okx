# okx

Rust/Tokio read-only observation and analysis platform for OKX.

The project is built in small, physically verifiable layers. The current production direction is a native Windows runtime, not workflow-per-query GitHub execution.

## Current architecture

```text
GitHub-hosted CI
  -> tested Windows bundle
  -> verified deployment

Windows Task Scheduler
  ONE TimeTrigger
        |
        v
okx-host-control.exe
  single controller
  desired state + Job Object
        |
        v
okx-agent.exe
        |
        v
Rust/Tokio observation runtime
  Reference Data
  Market State
  Reconciliation / Readiness
  later Account + Orders
        |
        v
typed Query API
  encrypted GitHub mailbox #10 now
  MCP adapter later
```

Ownership is strict:

- `okx-api`: typed OKX exchange primitives;
- `okx-ws`: OKX WebSocket protocol/transport only;
- `okx-runtime`: one Tokio owner for WS lifecycle/reconnect/subscriptions;
- `okx-observation`: normalized reference/market/account/order state, reconciliation and readiness;
- `okx-agent`: composition and access transport;
- `okx-host-control`: Windows lifecycle/deploy/diagnostics only;
- GitHub Actions: CI/release/acceptance only, never the production market-data runtime.

## Implementation status

- M1 Reference Data Registry — PASS / closed (#23).
- M2 Public REST Market State — PASS / closed (#26).
- M3 Persistent Public OKX WebSocket — PASS / closed (#30).
- Post-M3 runtime readability cleanup — PASS / closed (#42).
- M4 Private read-only Account + Order State — CURRENT.
- M5 Deterministic Cost/Risk/Scenario — planned.
- M6 MCP adapter — planned.

M2 intentionally reports `DEGRADED`: request-time REST bootstrap is attributable but is not persistent realtime state.

M3 proved `FRESH` market readiness from persistent WebSocket subscriptions, generation/reconciliation evidence and order-book `seqId/prevSeqId` continuity. The remaining physical forced network-loss test is explicitly deferred with #16 R3.

## Safety boundary

M1–M6 are read-only observation/analysis work:

- no order placement/cancel/amend;
- no withdrawals/transfers;
- no executor credential in the observation runtime;
- no raw API secrets in GitHub/logs/query responses;
- missing or inconsistent state fails closed.

## Windows lifecycle

Canonical runtime supervision:

```text
Task Scheduler TimeTrigger
  -> one okx-host-control
  -> one Job-Object-owned okx-agent
```

No SCM service, PowerShell watchdog, RestartOnFailure, LogonTrigger recovery, or second custom watchdog.

R4 (Windows reboot + sign-in) is PASS. R3 (external network/GitHub loss) remains DEFERRED; see #16.

## Windows filesystem layout

The installed Windows system intentionally separates mutable source, controller, and runtime binary:

```text
C:\okx
  canonical Git checkout / workspace / working directory

C:\okx-control
  installed okx-host-control.exe
  desired.json and controller-owned lifecycle state

C:\okx-runtime
  installed okx-agent.exe
  previous/staging agent binaries
  agent stdout/stderr logs

C:\okx-upgrade
  temporary manual bootstrap/upgrade staging only
  NOT part of the runtime architecture
```

`C:\okx-upgrade` was created by earlier one-time manual controller-upgrade instructions. The current Scheduler action points to `C:\okx-control\okx-host-control.exe`, and the agent is installed under `C:\okx-runtime`; therefore the upgrade directory is not required for normal operation and may be removed after confirming no manual process is running from it.

Keeping the first three directories separate is intentional: a mutable Git checkout must not be the installed controller or agent binary.

## Documentation

- [Architecture](docs/ARCHITECTURE.md)
- [Roadmap and acceptance cursor](docs/ROADMAP.md)

Canonical GitHub issues:

- #5 product/domain architecture;
- #7 native Windows runtime/deployment;
- #10 encrypted temporary analytical transport;
- #16 deferred Windows recovery acceptance debt;
- #30 M3 persistent public WebSocket — closed/PASS;
- #42 post-M3 runtime readability cleanup — closed/PASS.
