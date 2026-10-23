# Analytics evidence

Final prepared release measurement recorded at 2026-10-22T23:59:50.981044+00:00; Git HEAD `86f2273b9d481a0ae60b76e64a059061a4b290b6`. The component source snapshot was checked unchanged before/after the run; exact source hashes, Cargo.lock, binary hashes, raw log hashes, hardware and measurement counts are in `../acceptance/398/analytics-performance.json` and `398-analytics-source-snapshot.json`. This records prepared native component performance, not installed ReleaseSet parity or physical MCP/network acceptance.

Commands ran sequentially in the existing target directory:

```
cargo run --locked -p okx-research --release --example analytics_bench
cargo test --locked -p okx-agent --release --lib analytics:: -- --nocapture --test-threads=1
```

The actual release fixture run passed 16 tests with zero failures, including immutable pages/restart, two-instrument independent grid/artifacts/cursors, market artifact publication/cache/result roundtrip, metric extension, source quality, bounded CPU/cancellation and planner regressions. Raw logs: `398-analytics-benchmark-release.log` and `398-analytics-release.log`.

| Prepared fixture | Actual measurement |
|---|---|
| EMA20/50, 90 days × 15 minutes, 8,640 closed bars | p50 27.0703 ms; p95 35.9249 ms; max 50.4655 ms; summary 395 bytes |
| Grid, 100 variants × 100,000 bars | 7.0917798 s; 1,410,083 evaluated bars/s; peak working set 14,622,720 bytes; incremental peak 7,901,184 bytes |
| Screen, 8 instruments × 100 bars | p50 0.2713 ms; p95 0.5033 ms; summary 3166 bytes |
| Two real causal grids plus simulated typed ingestion | STOP scheduling p95 11.9897 ms; cancellation p95 12.8409 ms; cancellation max 16.9845 ms |

EMA, screen and STOP/cancel measurements each use five warmups and thirty samples. The 100-variant grid is one timed full completion. Peak memory comes from Windows process counters in the benchmark process. This host reports Intel64 Family 6 Model 60 Stepping 3, GenuineIntel, 8 logical CPUs, Windows-11-10.0.26200-SP0, 34,278,834,176 bytes physical RAM. Compilation/other host activity can affect wall time. The screen metric measures native fixture compute/projection/serialization; the concurrent fixture simulates ingestion and does not contact an exchange. Both immutable dataset/result hashes match the previous prepared fixture run.

A prior release-only regression exposed cancellation delay during whole-dataset JSON hashing. Current code hashes bounded 4,096-element chunks with cooperative cancellation and shares the immutable dataset hash across grid variants. The actual measurements above exercise that implementation after batch and artifact-I/O offload changes.
