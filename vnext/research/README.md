# Research replay

This workspace replays a checked, close-only causal graph from recorded closed
bars on standard input and emits a JSON array of timestamped results. It can be
built and tested from a copy of this directory alone.

```sh
cargo fmt --all -- --check
cargo clippy --locked --workspace --all-targets -- -D warnings
cargo test --locked --workspace
cargo build --locked --release --bin okx-research-replay
```

The kernel is extracted from the installed version for the first vNext slice.
The legacy copy serves existing callers until their migration. New Research
work belongs in this workspace. The recorded replay consumes a supplied dataset. The public adapter has no
trading authority and accepts no credentials.

Recorded replay requires a supported `timeframe_ms` and a closed-bar grid.
Every input bar carries candle-start `timestamp_ms` and `available_at_ms`
(observed source availability). Availability must not precede candle close;
it cannot be inferred merely from event time. Gaps, duplicates, reordered
availability and off-grid observations fail before any result is published.

```json
{
  "timeframe_ms": 60000,
  "graph": {
    "version": "okx.market.composition/v1",
    "steps": [{"id": "average", "op": "rolling_mean", "input": "close", "period": 2}],
    "result_steps": ["average"]
  },
  "bars": [
    {"timestamp_ms": 60000, "available_at_ms": 120000, "close": 2.0},
    {"timestamp_ms": 120000, "available_at_ms": 180000, "close": 4.0}
  ]
}
```

For public `--okx` replay the recorded acquisition time is the conservative
availability of every observed bar; a confirmed candle is never presumed
available at its nominal close. This does not replace an independently
attested historical publication-time dataset.

Use `okx-research-replay --okx` with a JSON object containing `graph`,
`instrument`, `interval` (`1m`, `3m`, `5m`, `15m`, `30m`, `1H`, `4H` or `1Dutc`) and `limit` (1–100).
This mode fetches one public OKX history page and returns a `source` receipt
and `bars`. The receipt identifies the acquisition time and hashes the exact
response. The provider discards still-open bars and rejects gaps and duplicates.
Recorded replay remains available without network access.
