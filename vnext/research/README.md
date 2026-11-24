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

Use `okx-research-replay --okx` with a JSON object containing `graph`,
`instrument`, `interval` (`1m`, `3m`, `5m`, `15m`, `30m`, `1H`, `4H` or `1Dutc`) and `limit` (1–100).
This mode fetches one public OKX history page and returns a `source` receipt
and `bars`. The receipt identifies the acquisition time and hashes the exact
response. The provider discards still-open bars and rejects gaps and duplicates.
Recorded replay remains available without network access.
