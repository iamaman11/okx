# Analytics evidence

Executable benchmark: `cargo run -p okx-research --release --example analytics_bench`.
Raw measurements: `../acceptance/398/analytics-performance.json`.
The machine reports Intel64 Family 6 Model 60 Stepping 3, eight logical CPUs, Windows x86_64, 34,278,834,176 bytes RAM.

The checked measurement uses five warmups and thirty EMA 20/50 replay measurements on a prepared 90-day 15-minute fixture. The 100-variant/100,000-bar job is timed to completion and Windows peak RSS is read from the process counters. See JSON for actual values and fixture/result hashes. Compiler/test contention may affect wall times; do not replace actual values with target thresholds.

Native screen and concurrent STOP/cancel metrics are emitted by actual executable agent tests as ANALYTICS_PERF_JSON and included by scripts/acceptance_product.py from full test logs. No physical MCP/public-network/deployment result is claimed by these compute fixtures.
The final recorded release fixture run passed 13 analytics tests. Prepared EMA: p50 21.8754 ms, p95 28.7802 ms, max 34.5614 ms, summary 395 bytes. The shared-hash 100-variant grid over 100,000 bars completed in 4.1519558 seconds (2,408,503 bars/second), peak working set 14,761,984 bytes and incremental peak 7,966,720 bytes. Native eight-instrument screen: p95 0.4072 ms, summary 3,166 bytes. Two actual heavy grid jobs with simulated ingestion: STOP scheduling p95 10.0713 ms; cancellation max 11.7414 ms. All warm measurements use five warmups and thirty samples. The raw log is `398-analytics-release.log`; the performance JSON includes recording time, exact benchmark/test binary SHA-256, component file hashes, fixture hash and result hashes. These are working-tree component measurements, not final installed ReleaseSet parity.

A release-only regression initially exposed a real cancellation stall during whole-dataset JSON hashing. The correction hashes bounded 4,096-element chunks with cancellation checks and shares the immutable dataset hash across grid variants. Repeated successful release fixtures and the completed benchmark above measure the corrected implementation.
