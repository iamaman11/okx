# S0 local measurements

This small standard-library tool measures an explicitly chosen owner's build
and test commands for the two topology prototypes required by [#569](https://github.com/iamaman11/okx/issues/569).
It is **not** a Ready CI gate or proof of the ≤120-second requirement.

Run the same commands against two independently prepared, isolated workspace
directories (e.g. archive exports of two candidate revisions). The script
never deletes build caches or starts installation, Control or live trading. It
does not invoke a shell for the measured commands.

```sh
python3 -B vnext/s0/measure.py run \
  --repository . \
  --workspace /tmp/s0-variant-a/engine \
  --owner engine --name compact \
  --output /tmp/s0-compact.json \
  --command 'cargo test --locked --workspace' \
  --command 'cargo build --locked --release --workspace'

python3 -B vnext/s0/measure.py run \
  --repository . \
  --workspace /tmp/s0-variant-b/engine \
  --owner engine --name separated \
  --output /tmp/s0-separated.json \
  --command 'cargo test --locked --workspace' \
  --command 'cargo build --locked --release --workspace'

python3 -B vnext/s0/measure.py compare \
  --baseline /tmp/s0-compact.json \
  --candidate /tmp/s0-separated.json \
  --output /tmp/s0-comparison.json
```

Replace the workspace paths with actual exported candidate directories.
Research and Control accept the same Cargo commands; Edge accepts explicit
`npm ci`, `npm run check`, `npm test` or `node scripts/test-name.mjs` commands.
Use an owner-specific lockfile and comparable runner class, toolchains,
features, target, environment and cache policy. A changed SHA-256 of workspace
source inputs is **necessary but not sufficient**: independently document the
meaningful behavior-changing source diff and its failing-before/passing-after
oracle. Use actual isolated copies to avoid reading sibling source files.

Each JSON sample contains source fingerprint, Git HEAD/tree and dirty flag (if
`--repository` points at a checkout), platform/toolchain, Cargo metadata
closure, build-affecting environment input names (never secrets), phase durations,
Cargo artifact events, fresh/nonfresh counts, and
basic test summaries. `cargo --message-format=json` compiler-artifact events are
*not* exact rustc invocations; `rustc_verbose_invocations` is unknown without
Cargo verbose logs. Actual linker invocations are also **not instrumented**.
Do not treat executable artifacts as a one-to-one count of linker processes.

`compare` rejects unchanged source fingerprints, mismatched command lists or
toolchains and failed phases. It reports only sequential local phase deltas.
The full Ready budget requires separate evidence for real Windows/Linux
compiler/link/test events, finance/OS/B1 witnesses, production artifacts,
provenance, queues and the protected gate's `t0→t1` time as specified by #569.

Check the tool without Rust installed:

```sh
python3 -B -m unittest discover -s vnext/s0 -p 'test_*.py'
```

## Executable topology experiments

The S0 candidate comparison and its current evidence limits are tracked in
[topology_decision.md](topology_decision.md). The functional-equivalence checklist
is in [functional_coverage.md](functional_coverage.md). The two alternatives
are temporary **owner-only** exports; they never edit production source, install
an EXE, restart an account owner, or call a live trading endpoint.

Run the Engine comparison on a machine with sufficient temporary disk space
(the command compiles two independent Cargo targets and deletes its own exports
when it finishes):

```sh
python3 -B vnext/s0/engine_topology_ab.py \
  --engine vnext/engine \
  --probe command \
  --output /tmp/engine-command-topology.json

python3 -B vnext/s0/engine_topology_ab.py \
  --engine vnext/engine \
  --probe finance \
  --output /tmp/engine-finance-topology.json
```

Both variants now run the connected **offline** typed command → actual
SQLite/WAL PREPARE → synthetic reserve → durable POST fence → one simulated
venue send → lost ACK → UNKNOWN → reopen → no repost → readback scenario,
as well as the existing production financial-owner fake-venue
lost-ACK/single-send witnesses. They then rebuild after the same temporary
production-source change. The **command** probe changes validated identifiers;
the separately exported **finance** probe first proves its PREPARE-idempotency
test fails against the unchanged source, then hardens the original financial
writer implementation and measures the green test and release rebuild. Each
probe runs the fake-venue witnesses again after its source change. The new
connected scenario has a **synthetic CentralReceipt**, not a RiskBook
authorization or a real `CommittedVenueSend`; it is an S0 topology oracle,
not a new Engine HTTP execution E2E or a financial SQL-schema migration. The first
non-authoritative Linux **command** sample is preserved
in [engine_ab_local_sample.json](engine_ab_local_sample.json).

The Research export is deliberately separate from the fast oracle and the
source-diff benchmark, allowing the same exact topology and data to be tested
without copying any Engine code:

```sh
python3 -B vnext/s0/research_topology.py \
  --source vnext/research --output-dir /tmp/research-s0-variants
python3 -B vnext/s0/research_oracle.py \
  --workspace-root /tmp/research-s0-variants
python3 -B vnext/s0/research_diff.py \
  --source vnext/research --workspace-root /tmp/research-s0-variants \
  --probe kernel-only
```

Use a fresh export for the alternative `--probe adapter-only`. Both probe
classes intentionally modify **only the disposable copies**, not the canonical
Research source. The Research oracle executes three typed QueryPlan hypotheses
**A/B/C on the same six immutable closed bars**, records the common source
SHA-256 and checks three distinct complete numerical series, deterministic
replays and fail-closed negative paths for both topologies. It does not prove
the external MCP/API route (U01). Run the same `measure.py run --command ...` list *before and
after* each probe and compare records only when host, toolchain, cache policy,
commands and semantic oracle match. The first local measurements of both
classes are recorded in [research_ab_local_sample.json](research_ab_local_sample.json).
Do not run the heavy Engine and Research
experiments concurrently on a shared small `/tmp` filesystem: `SQLITE_FULL`
invalidates a finance result, it does not demonstrate a financial regression.

Neither one successful isolated workflow nor a single warm local sample proves
the protected Ready gate's ≤120 seconds, live install/rollback, Finance/OS B1 or
four independent production runtimes. These remain **OPEN** in #569.
