# Roadmap and acceptance cursor

## Single current cursor

**#113 — Production Readiness Closure**

The product is no longer in M/Q/H feature staging. Read-only observation/analysis and the hard-disabled execution boundary are accepted. Current work is limited to closing concrete production-baseline gaps.

```text
accepted runtime
    ->
#113 Production Readiness Closure
    ->
frozen production baseline
    ->
ONLY THEN optional explicit live-write acceptance
```

## Accepted product/runtime foundation

- [x] M1 Reference Data Registry — #23 PASS;
- [x] M2 Public REST Market State — #26 PASS;
- [x] M3 Persistent Public WebSocket — #30 PASS;
- [x] Q0 query/transport efficiency — #47 PASS;
- [x] M4 private read-only Account + Order State — #59 PASS;
- [x] M5 deterministic cost/risk/scenario analysis — #65 PASS;
- [x] M6 bounded HistoryBehavior / PositionScenario / CurrentCost / MarketResearch — #72 PASS;
- [x] H1 GitHub transport / response-shaping / context efficiency — #83 PASS;
- [x] A2 unattended Windows lifecycle / reboot / real external network-loss recovery — #16 PASS.

The current observation/analysis platform is production-accepted in the present Windows + GitHub environment.

## Phase 2 pre-enable execution boundary — PHYSICAL PASS

Authority: #3.

Accepted:

- [x] pure typed ExecutionIntent -> ExecutionPlan;
- [x] durable bounded mutation ledger;
- [x] deterministic `clOrdId` identity/idempotency;
- [x] typed place/amend/cancel + exact lookup primitives;
- [x] one OrderExecutor as sole mutation owner;
- [x] UNKNOWN_SUBMISSION persistence/reconciliation;
- [x] semantic account/reference/fee pre-send continuity;
- [x] deterministic prepare conflicts terminalized;
- [x] bounded public/private OKX REST deadlines;
- [x] isolated executor credential custody;
- [x] authenticated executor preflight: Read+Trade, no Withdraw, intended sub-account/mode;
- [x] encrypted read-only ExecutionStatus;
- [x] physical disabled-submit proof;
- [x] restart persistence proof;
- [x] live orders sent during acceptance: 0.

Production live writes remain disabled before SUBMITTING persistence and before network send. No production enable setter/mechanism is accepted.

Executor IP allowlisting is optional diagnostic evidence, not an acceptance gate.

## #113 closure checkpoints

### Checkpoint 1 — reproducible dependency authority — PASS

- [x] committed workspace `Cargo.lock`;
- [x] canonical compile/test/release paths use `--locked`;
- [x] Linux + Windows CI PASS from the locked graph;
- [x] exact tested-tree == merged-tree acceptance recorded.

### Checkpoint 2 — CI/supply-chain immutability — PASS

Completed:

- [x] canonical GitHub Actions pinned to immutable commit SHAs;
- [x] account acceptance Actions pinned;
- [x] exact merge-tree reuse rule retained;
- [x] remote local-build deployment bypass removed;
- [x] legacy CONTROL `BuildAgent` fails closed;
- [x] verified hosted-CI artifact remains the normal agent deployment authority.

Completed repository enforcement:

- [x] `main` branch protection enabled;
- [x] required checks: `classify`, `linux-core`, `windows-native`;
- [x] enforcement applies to the protected production branch.

### Checkpoint 3 — durable recovery artifact — PASS

- [x] exact successful PR CI artifact resolved;
- [x] merged tree must equal tested PR head tree;
- [x] embedded bundle manifest verified;
- [x] agent/controller SHA-256 verified;
- [x] versioned production recovery Release published;
- [x] `SHA256SUMS.txt` published;
- [x] no updater/service/runtime owner added.

Current accepted durable release lineage began with:
`production-recovery-3ef505d7ec7a`.

### Checkpoint 4 — canonical architecture/documentation — PASS on merge of the canonical-docs slice

- [x] one canonical `docs/ARCHITECTURE.md`;
- [x] remove case-conflicting `docs/architecture.md`;
- [x] current README;
- [x] current ROADMAP;
- [x] account credential policy synchronized;
- [x] Phase 2 pre-enable status synchronized;
- [x] live-write status explicitly separated from production baseline.

### Checkpoint 5 — final exact production-baseline acceptance — runtime/execution PASS; controller lifecycle + repository enforcement remain

One bounded acceptance on the exact final artifact must prove:

- final main SHA/tree + CI PASS;
- durable recovery bundle identity/hash;
- installed agent provenance VERIFIED/HASH_MATCH;
- Scheduler policy valid;
- exactly one controller and one Job-Object-owned agent;
- READY_RUNNING;
- bounded restart PASS;
- one compact public/market DATA query PASS;
- one private account DATA query PASS;
- executor preflight PASS;
- ExecutionStatus before/after restart PASS;
- fresh prepared execution plan;
- submit -> `LIVE_TRADING_DISABLED`;
- ledger never enters SUBMITTING;
- no exchange mutation;
- state survives restart;
- live orders sent = 0;
- no second supervisor/runtime/transport introduced.

Exit state:

**PRODUCTION BASELINE — CLOSED/PASS**

## Deferred, non-blocking maintenance

### #126 immutable controller launcher/root-of-trust — ACTIVE / required

#126 supersedes the self-replacing controller design tracked by #54.

Required acceptance:

- [ ] build and merge the independent `okx-host-launcher` root-of-trust;
- [ ] bundle/manifest/recovery artifacts carry launcher SHA-256;
- [ ] perform the one-time root migration from an exact accepted artifact;
- [ ] canonical Scheduler action becomes permanently `okx-host-launcher.exe run`;
- [ ] normal controller update never changes Scheduler and never overwrites a running executable;
- [ ] remotely stage a distinct controller version;
- [ ] durable CONTROL terminal PASS precedes old controller exit;
- [ ] launcher waits the exact old PID and candidate readiness event;
- [ ] successful candidate commits atomically;
- [ ] failed candidate automatically rolls back to the previous confirmed version;
- [ ] crash/reboot boundary converges without local operator intervention;
- [ ] fresh CONTROL + DATA + Job-owned agent recovery PASS;
- [ ] live trading remains disabled and exchange mutations remain zero.

The migration-only legacy activator is allowed solely before launcher-root exists and must fail closed afterward.

### #54 controller self-update — ABSORBED BY #126

No separate closure path remains. #54 closes only when #126 physical acceptance closes the controller lifecycle permanently.

### #58 DATA mailbox compaction

Remain deferred until the existing 800/900 capacity trigger or real recovery evidence requires it. Do not add speculative deletion/rotation machinery.

## Later explicit live-write acceptance

Not part of #113.

If explicitly authorized after the production baseline closes, use one separate bounded live-write acceptance cursor proving:

- explicit enable authority;
- fresh pre-send state/risk checks;
- one deliberately bounded real order;
- durable ACK or UNKNOWN_SUBMISSION;
- exact exchange/private-state reconciliation;
- restart/idempotency;
- amend/cancel/close semantics as applicable;
- no duplicate mutation after uncertainty.

No autonomous strategy engine or automatic trade selection is implied.
