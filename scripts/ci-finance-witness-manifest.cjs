'use strict';

// Extract the finance witness exclusion set from the reviewed Rust source,
// without compiling the entire Engine just to invoke `--list` in the parallel
// regressions runner. This does NOT replace the executing finance collector:
// the isolated acceptance job checks the exact same source-derived set against
// every observed witness after its mandatory FULL_PASS on the current tree.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const sourcePath = path.join(__dirname, '..', 'crates', 'okx-trading-engine',
  'examples', 'native_finance_acceptance', 'manifest.rs');
const packages = new Set(['okx-domain-core', 'okx-trade-execution',
  'okx-trading-engine', 'okx-ipc-contracts', 'okx-market-runtime', 'repository']);

function extract(source) {
  assert.equal(typeof source, 'string', 'FINANCE_MANIFEST_NOT_TEXT');
  // The two accepted syntax forms are intentionally narrow: immutable Witness
  // constants and an explicit CONTRACTS array containing literal references.
  // A Rust refactor of the manifest must be reviewed rather than guessed.
  const declarations = [...source.matchAll(/const\s+([A-Z][A-Z0-9_]*):\s*Witness\s*=\s*Witness\s*\{\s*package:\s*"([^"]+)"\s*,\s*test:\s*"([^"]+)"\s*,\s*\};/g)];
  const starts = [...source.matchAll(/\bconst\s+[A-Z][A-Z0-9_]*:\s*Witness\s*=\s*Witness\s*\{/g)];
  assert.ok(starts.length > 20 && declarations.length === starts.length,
    'FINANCE_WITNESS_DECLARATION_UNRECOGNIZED');

  const byName = new Map();
  for (const [, name, owner, test] of declarations) {
    assert.ok(!byName.has(name), `FINANCE_WITNESS_DUPLICATE_NAME:${name}`);
    assert.ok(packages.has(owner), `FINANCE_WITNESS_UNKNOWN_PACKAGE:${owner}`);
    assert.match(test, /^[A-Za-z0-9_:]+$/, `FINANCE_WITNESS_UNSAFE_TEST:${name}`);
    byName.set(name, { package: owner, test });
  }

  const anchor = 'pub const CONTRACTS: &[Contract] = &[';
  const first = source.indexOf(anchor);
  assert.ok(first >= 0 && source.indexOf(anchor, first + anchor.length) < 0,
    'FINANCE_CONTRACTS_ARRAY_AMBIGUOUS');
  const contracts = source.slice(first + anchor.length).replace(/\/\/[^\n]*/g, '');
  // There must be precisely one literal witness array per Contract object.
  const rows = [...contracts.matchAll(/\bContract\s*\{/g)];
  const ids = [...contracts.matchAll(/\bid:\s*"([A-Za-z0-9_.]+)"/g)].map(match => match[1]).sort();
  const refs = [...contracts.matchAll(/\bwitnesses:\s*&\[([\s\S]*?)\]/g)];
  assert.ok(rows.length > 20 && refs.length === rows.length,
    'FINANCE_CONTRACT_WITNESS_ARRAY_UNRECOGNIZED');
  assert.equal(ids.length, rows.length, 'FINANCE_CONTRACT_IDS_MISSING');
  assert.equal(new Set(ids).size, ids.length, 'FINANCE_CONTRACT_IDS_DUPLICATED');
  const selected = new Set();
  for (const [, body] of refs) {
    assert.match(body, /^[A-Z0-9_,\s]+$/, 'FINANCE_CONTRACT_WITNESS_NOT_LITERAL');
    const names = body.split(',').map(x => x.trim()).filter(Boolean);
    assert.ok(names.length > 0, 'FINANCE_CONTRACT_EMPTY_WITNESSES');
    for (const name of names) {
      assert.ok(byName.has(name), `FINANCE_CONTRACT_UNDECLARED_WITNESS:${name}`);
      selected.add(name);
    }
  }
  const all = [...selected].map(name => byName.get(name))
    .sort((a, b) => `${a.package}:${a.test}`.localeCompare(`${b.package}:${b.test}`));
  const witnesses = all.filter(w => w.package !== 'repository');
  const repository_witnesses = all.filter(w => w.package === 'repository');
  assert.ok(witnesses.length > 20, 'FINANCE_MANIFEST_TOO_SMALL');
  assert.ok(repository_witnesses.length >= 3, 'FINANCE_REPOSITORY_GUARDS_MISSING');
  const keys = all.map(w => `${w.package}\0${w.test}`);
  assert.equal(new Set(keys).size, keys.length, 'FINANCE_MANIFEST_DUPLICATE_TEST');
  return { contracts: ids, witnesses, repository_witnesses };
}

function verify(report, sourceManifest, head, tree) {
  assert.equal(report?.schema, 'okx.native-finance-acceptance/v1', 'FINANCE_REPORT_SCHEMA_INVALID');
  assert.equal(report.mode, 'full', 'FINANCE_REPORT_NOT_FULL_MODE');
  assert.equal(report.overall, 'FULL_PASS', 'FINANCE_REPORT_NOT_FULL_PASS');
  assert.equal(report.collector_source_binding, 'VERIFIED', 'FINANCE_COLLECTOR_SOURCE_UNVERIFIED');
  assert.match(report.head, /^[0-9a-f]{40}$/, 'FINANCE_REPORT_HEAD_INVALID');
  assert.match(report.tree, /^[0-9a-f]{40}$/, 'FINANCE_REPORT_TREE_INVALID');
  assert.equal(report.head, head, 'FINANCE_REPORT_HEAD_MISMATCH');
  assert.equal(report.tree, tree, 'FINANCE_REPORT_TREE_MISMATCH');
  assert.equal(report.git_clean, true, 'FINANCE_REPORT_GIT_DIRTY');
  const executedContracts = report.contracts.map(row => {
    assert.equal(row.status, 'PASS', 'FINANCE_REPORT_CONTRACT_FAILED');
    return row.id;
  }).sort();
  assert.deepEqual(executedContracts, sourceManifest.contracts, 'FINANCE_CONTRACT_IDS_DIFFER');
  const actual = report.observed_witnesses;
  assert.ok(Array.isArray(actual), 'FINANCE_REPORT_MISSING_OBSERVED');
  const actualKeys = actual.map(w => {
    assert.equal(w.status, 'ok', 'FINANCE_REPORT_WITNESS_NOT_OK');
    assert.ok(packages.has(w.package), 'FINANCE_REPORT_WITNESS_UNKNOWN_PACKAGE');
    assert.match(w.test, /^[A-Za-z0-9_:]+$/, 'FINANCE_REPORT_TEST_INVALID');
    return `${w.package}\0${w.test}`;
  }).sort();
  const expectedKeys = [...sourceManifest.witnesses, ...sourceManifest.repository_witnesses]
    .map(w => `${w.package}\0${w.test}`).sort();
  assert.deepEqual(actualKeys, expectedKeys, 'FINANCE_SOURCE_AND_EXECUTION_ROSTERS_DIFFER');
  return true;
}

function selfTest(source) {
  const expected = extract(source);
  assert.throws(() => extract(source.replace('pub const CONTRACTS:', 'pub const CONTRACTS_RENAMED:')));
  assert.throws(() => extract(source.replace('package: "okx-trading-engine"', 'package: "unknown-owner"')));
  assert.throws(() => extract(source.replace('witnesses: &[', 'witnesses: &dynamic_witnesses(')));
  assert.throws(() => extract(source.replace('test: "', 'test: dynamic_test_name("')));
  const report = { schema: 'okx.native-finance-acceptance/v1',
    mode: 'full', overall: 'FULL_PASS', collector_source_binding: 'VERIFIED',
    head: 'a'.repeat(40), tree: 'b'.repeat(40), git_clean: true,
    contracts: expected.contracts.map(id => ({ id, status: 'PASS' })),
    observed_witnesses: [...expected.witnesses, ...expected.repository_witnesses]
      .map(w => ({ ...w, status: 'ok' })) };
  verify(report, expected, report.head, report.tree);
  assert.throws(() => verify(report, expected, report.head, 'c'.repeat(40)), /FINANCE_REPORT_TREE_MISMATCH/);
  assert.throws(() => verify({ ...report, contracts: report.contracts.slice(1) },
    expected, report.head, report.tree), /FINANCE_CONTRACT_IDS_DIFFER/);
  assert.throws(() => verify({ ...report, observed_witnesses: report.observed_witnesses.slice(1) },
    expected, report.head, report.tree), /FINANCE_SOURCE_AND_EXECUTION_ROSTERS_DIFFER/);
  process.stdout.write('FINANCE_SOURCE_ROSTER_STRICT_CONTROLS=PASS\n');
}

function main() {
  const args = process.argv.slice(2);
  const source = fs.readFileSync(sourcePath, 'utf8');
  if (args.length === 1 && args[0] === '--self-test') {
    selfTest(source);
    return;
  }
  const manifest = extract(source);
  if (args.length === 4 && args[0] === '--verify-report') {
    verify(JSON.parse(fs.readFileSync(args[1], 'utf8')), manifest, args[2], args[3]);
    process.stdout.write(`FINANCE_COLLECTOR_ROSTER_MATCH=${manifest.witnesses.length}\n`);
    return;
  }
  assert.equal(args.length, 0, 'FINANCE_MANIFEST_ARGUMENTS_INVALID');
  process.stdout.write(`${JSON.stringify(manifest)}\n`);
}

try {
  main();
} catch (error) {
  process.stderr.write(`${error?.message ?? error}\n`);
  process.exitCode = 1;
}
