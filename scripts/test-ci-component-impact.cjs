'use strict';

const assert = require('node:assert/strict');
const path = require('node:path');
const {analyze, changedFiles, markdown} = require('./ci-component-impact.cjs');

const cwd = process.cwd();
const names = [
  'okx-trading-engine', 'okx-trade-execution', 'okx-domain-core',
  'okx-ipc-contracts', 'okx-research-service', 'okx-research-protocol', 'okx-strategy-core',
  'okx-research', 'okx-analysis', 'okx-host-controller', 'okx-host-launcher', 'okx-api',
  'okx-host-protocol', 'okx-bridge-mcp',
];
const e = (pkg, kind = null) => ({pkg, dep_kinds: [{kind, target: null}]});
const edges = {
  'okx-trading-engine': [e('okx-trade-execution'), e('okx-ipc-contracts'), e('okx-api')],
  'okx-ipc-contracts': [e('okx-domain-core'), e('okx-research-protocol')],
  'okx-research-service': [e('okx-research'), e('okx-strategy-core'), e('okx-research-protocol'), e('okx-api', 'dev')],
  'okx-research': [e('okx-analysis')],
  'okx-analysis': [e('okx-strategy-core')],
  'okx-strategy-core': [e('okx-research-protocol')],
  'okx-research-protocol': [e('okx-domain-core')],
  'okx-host-controller': [e('okx-host-launcher'), e('okx-host-protocol'), e('okx-ipc-contracts')],
  'okx-host-protocol': [e('okx-ipc-contracts')],
  'okx-bridge-mcp': [e('okx-host-protocol'), e('okx-ipc-contracts')],
};
const metadata = {
  workspace_root: cwd,
  workspace_members: names,
  packages: names.map(id => ({id, name: id, manifest_path: path.join(cwd, 'crates', id, 'Cargo.toml')})),
  resolve: {nodes: names.map(id => ({id, deps: edges[id] ?? []}))},
};
const files = (...paths) => [[...paths.map(filename => ({filename, status: 'modified'}))]];
const binaries = report => report.potentially_affected_windows_binaries.map(x => x.binary).sort();
const all = ['okx-agent.exe', 'okx-host-control.exe', 'okx-host-launcher.exe', 'okx-research-service.exe'].sort();

const research = analyze(metadata, files('crates/okx-strategy-core/src/operators.rs'));
assert.deepEqual(binaries(research), ['okx-research-service.exe']);
assert.equal(research.uncertainty, null);
assert.deepEqual(research.potentially_affected_windows_binaries[0].paths[0],
  ['okx-research-service', 'okx-strategy-core']);
assert.match(markdown(research), /DIAGNOSTIC_ONLY_NOT_A_CI_SKIP_PROOF|Diagnostic only/);
assert.deepEqual(binaries(analyze(metadata, files('crates/okx-research-service/src/source_io.rs'))),
  ['okx-research-service.exe']);
assert.deepEqual(binaries(analyze(metadata, files('crates/okx-host-launcher/src/main.rs'))),
  ['okx-host-control.exe', 'okx-host-launcher.exe']);
assert.deepEqual(binaries(analyze(metadata, files('crates/okx-host-controller/src/autostart.rs'))),
  ['okx-host-control.exe']);
assert.deepEqual(binaries(analyze(metadata, files('crates/okx-host-protocol/src/lib.rs'))),
  ['okx-host-control.exe']);
assert.deepEqual(binaries(analyze(metadata, files('crates/okx-trade-execution/src/lib.rs'))),
  ['okx-agent.exe']);
assert.deepEqual(binaries(analyze(metadata, files('crates/okx-strategy-core/src/time_series.rs'))),
  ['okx-research-service.exe']);
assert.deepEqual(binaries(analyze(metadata, files('crates/okx-trade-execution/src/risk/money.rs'))),
  ['okx-agent.exe']);
assert.deepEqual(binaries(analyze(metadata, files('crates/okx-domain-core/src/fee_model.rs'))),
  ['okx-agent.exe', 'okx-host-control.exe', 'okx-research-service.exe']);
assert.deepEqual(binaries(analyze(metadata, files('crates/okx-ipc-contracts/src/lib.rs'))),
  ['okx-agent.exe', 'okx-host-control.exe']);
assert.deepEqual(binaries(analyze(metadata, files('crates/okx-research-protocol/src/strategy_contract.rs'))),
  ['okx-agent.exe', 'okx-host-control.exe', 'okx-research-service.exe']);
// A dev-only Research dependency must not be attributed to its release EXE.
assert.deepEqual(binaries(analyze(metadata, files('crates/okx-api/src/lib.rs'))), ['okx-agent.exe']);

const cloudflare = analyze(metadata, files('cloudflare/okx-mcp/src/mcp.ts', 'docs/a.md'));
assert.deepEqual(binaries(cloudflare), []);
assert.deepEqual(cloudflare.other_components, ['cloudflare-mcp']);
assert.deepEqual(binaries(analyze(metadata, files('docs/research-boundaries.md'))), []);
for (const changed of [
  files('Cargo.lock'), files('crates/okx-research-service/Cargo.toml'),
  files('crates/okx-host-launcher/build.rs'), files('.github/workflows/ci.yml'),
  files('crates/okx-trading-engine/src/main.rs', 'scripts/research-release-scope.cjs'),
]) {
  const report = analyze(metadata, changed);
  assert.deepEqual(binaries(report), all);
  assert.equal(report.uncertainty, 'UNKNOWN_OR_DEPENDENCY_CONFIGURATION_CHANGE');
}
for (const untrusted of [null, [], [[]], [[{}]],
  [[{filename: 'docs/a.md', status: 'renamed', previous_filename: 'docs/b.md'}]],
  [[{filename: 'docs/a.md', status: 'modified'}, {filename: 'docs/a.md', status: 'modified'}]],
  [[{filename: '../secret', status: 'modified'}]],
  [[{filename: 'docs/a.md', status: 'modified'}].concat(Array(100).fill({filename: 'docs/b.md', status: 'modified'}))],
]) {
  const report = analyze(metadata, untrusted);
  assert.deepEqual(binaries(report), all);
  assert.notEqual(report.uncertainty, null);
}
assert.equal(changedFiles(null).reason, 'PUSH_DIFF_NOT_SUPPLIED');
assert.deepEqual(binaries(analyze(metadata, null)), all);

const missing = structuredClone(metadata);
missing.resolve.nodes = [];
assert.throws(() => analyze(missing, files('crates/okx-strategy-core/src/operators.rs')),
  /WORKSPACE_MEMBER_GRAPH_MISSING/);
const badDeps = structuredClone(metadata);
badDeps.resolve.nodes.find(x => x.id === 'okx-research-service').deps[0].dep_kinds = null;
assert.throws(() => analyze(badDeps, files('crates/okx-research/src/lib.rs')),
  /CARGO_DEPENDENCY_KIND_MISSING/);
const extra = structuredClone(metadata);
extra.resolve.nodes.find(x => x.id === 'okx-trading-engine').deps.push(e('okx-research-service', 'build'));
assert.deepEqual(binaries(analyze(extra, files('crates/okx-strategy-core/src/operators.rs'))),
  ['okx-agent.exe', 'okx-research-service.exe']);

console.log('CI_CARGO_IMPACT_DIAGNOSTIC_CONTROLS=PASS');
