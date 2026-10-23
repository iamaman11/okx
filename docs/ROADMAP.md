# #398 progress: implementation versus physical acceptance

This is a **dated status ledger (23 October 2026)**, not a work plan. All tasks and tests are defined in [issue #398](https://github.com/iamaman11/okx/issues/398) and its [acceptance criteria](https://github.com/iamaman11/okx/issues/398#issuecomment-6373732525). Historical Stage 5 and W01–W10 directions are not executable.

| Area | Proven | Still pending |
| --- | --- | --- |
| F01 policy | PHYSICAL PASS after restart: central and native account SQL USD10/revision2, consumed/reserved=0 | Verify no reset after final deployment |
| F02 execution price | PR #403 exact CI and tick/slippage regressions PASS | Final release read-only planning and safe admission acceptance |
| F03 result semantics | **LIVE FAIL**: native `JOB_NOT_FOUND`/`ORDER_NOTIONAL_LIMIT` projected as `REQUEST_FAILED`; queued job projected completed | Fix DO-wrapped Worker projection and run live negatives |
| F04 transient source | #403 regression committed | Concurrent source defer/retry/recovery physical test |
| F05 analyst views | **LIVE FAIL**: grid selected/train/test/equity incomplete; no full log-return correlation matrix or interior gap guard | Complete user projections and input completeness |
| F06 portfolio coverage | Before T0 physically shows partial as required | After-T0 financial reconciliation and aggregate quality |
| F07 strategies | Instances and shared kernel exist | Crash-safe durable decision→intent; two-instance acceptance |
| F08 units/jobs | SQLite finance and central reservations exist; research metadata still JSON | Risk-budget/notional split, indexed SQLite jobs |
| F09 final acceptance | #403 Windows CONTROL Agent/Controller/Launcher `MATCHED`; [6/6 CI](https://github.com/iamaman11/okx/actions/runs/40010720157) | Final exact Windows+Worker release, comprehensive 7-tool/16-capability matrix, gated Real cycles |

Old sixteen-tool functionality is **not yet fully preserved**. Restore business use cases as typed actions inside the seven, not legacy endpoints. The second physical Real account does not exist: `PHYSICAL_MULTI_ACCOUNT=BLOCKED`, regardless of fixture coverage.

Evidence: [installed CONTROL](https://github.com/iamaman11/okx/issues/12#issuecomment-6373212927), [live MCP audit](https://github.com/iamaman11/okx/issues/398#issuecomment-6373565941). Real POST remains blocked on account evidence freshness and individual risk admission; code green is not physical PASS.
