'use strict';

// A single automatic main CI: preserve three protected status names, keep
// optional operations out of the test graph, and never publish before gates.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const root = path.join(__dirname, '..', '.github', 'workflows');
const workflows = fs.readdirSync(root).filter(x => /\.ya?ml$/.test(x));
const source = fs.readFileSync(path.join(root, 'ci.yml'), 'utf8');

function validate(yaml, fileNames) {
  assert.ok(!fileNames.includes('publish-native-base.yml'), 'SECOND_PUBLICATION_WORKFLOW');
  const parts = yaml.split(/^jobs:\s*$/m);
  assert.equal(parts.length, 2, 'MALFORMED_CI_JOBS');
  const [header, jobs] = parts;
  assert.match(header, /^name: CI$/m);
  assert.match(header, /^  push:$/m);
  assert.match(header, /^      - main$/m);
  assert.match(header, /^  pull_request:$/m);
  assert.doesNotMatch(header, /^  workflow_run:$/m);
  assert.match(header, /^  workflow_dispatch:$/m);
  assert.match(header, /^      prime_windows_cargo_cache:$/m);
  assert.match(header, /^        default: false$/m);
  assert.doesNotMatch(header, /^  contents: write$/m);
  assert.match(header, /cancel-in-progress: \$\{\{ github\.event_name == 'pull_request' \}\}/);

  const entries = [...jobs.matchAll(/^  ([a-zA-Z0-9_-]+):\s*$/gm)];
  const map = new Map(entries.map((m, i) => [m[1], jobs.slice(m.index,
    i + 1 < entries.length ? entries[i + 1].index : undefined)]));
  assert.equal(map.size, entries.length, 'DUPLICATE_JOB_ID');
  // The 15th job is only an explicitly requested, trusted-main cache seed.
  // It never executes as part of ordinary PR/main CI or financial acceptance.
  assert.ok(map.size <= 15, 'OBSOLETE_JOB_GRAPH_REGRESSION');
  for (const unused of ['required_classify', 'draft-trading-migration-512',
    'draft-riskbook-migration-510', 'cloudflare_deploy', 'readonly_observer',
    'production_recovery_promote', 'named_validate', 'all26_windows_targeted']) {
    assert.ok(!map.has(unused), `UNNECESSARY_CI_JOB:${unused}`);
  }
  function job(id) {
    assert.ok(map.has(id), `MISSING_JOB:${id}`);
    return map.get(id);
  }

  // Keep the existing branch-protection display name without an alias runner.
  assert.match(job('component_impact'), /^    name: classify$/m);
  for (const [id, display] of [['required_linux', 'linux-core'],
    ['required_windows', 'windows-native']]) {
    assert.match(job(id), new RegExp(`^    name: ${display}$`, 'm'));
    assert.match(job(id), /needs: \[component_impact,/);
  }
  const names = {
    rust_tests_linux: 'rust-tests-linux',
    trading_engine_acceptance_windows: 'trading-engine-acceptance-windows',
    trading_safety_acceptance_windows: 'trading-safety-acceptance-windows',
    research_service_tests_windows: 'research-service-tests-windows',
    host_controller_launcher_checks: 'host-controller-launcher-checks',
    'research_package_proof': 'research-package-proof',
  };
  for (const [id, display] of Object.entries(names)) {
    assert.match(job(id), new RegExp(`^    name: ${display}$`, 'm'),
      `AMBIGUOUS_CI_JOB_NAME:${id}`);
  }
  const engine = job('trading_engine_acceptance_windows');
  assert.match(engine, /\$collector = Join-Path \$targetDir 'debug\\examples\\native_finance_acceptance\.exe'/);
  assert.match(engine, /\$manifestJson = & \$collector --list/);
  assert.doesNotMatch(engine, /cargo run[^\n]*-- --list/, 'REDUNDANT_CARGO_FINANCIAL_LIST');
  assert.match(engine, /shared-key: trading-engine-windows/);
  const prime = job('trusted_main_cache_prime');
  assert.match(prime, /^    name: trusted-main-windows-cargo-cache-prime$/m);
  assert.match(prime, /github.event_name == 'workflow_dispatch' &&/);
  assert.match(prime, /inputs.prime_windows_cargo_cache == true &&/);
  assert.match(prime, /github.ref == 'refs\/heads\/main' &&/);
  assert.match(prime, /needs.component_impact.outputs.build_required == 'false'/);
  assert.match(prime, /merged tree exactly matches successful PR head tree/);
  assert.match(prime, /shared-key: trading-engine-windows/);
  assert.match(prime, /cache-on-failure: false/);
  assert.match(prime, /cargo test --locked --no-run --all-targets/);
  assert.match(prime, /cargo build --locked --release/);
  assert.doesNotMatch(prime, /gh release|actions\/upload-artifact|cargo run|cargo test --locked .* -- --nocapture/);
  assert.match(prime, /FINANCIAL_TESTS=NOT_EXECUTED; RELEASE=NOT_PUBLISHED; ENGINE=NOT_INSTALLED/);
  const publish = job('publish_accepted_windows_release');
  assert.match(publish, /^    name: publish-accepted-windows-release$/m);
  assert.match(publish,
    /needs: \[dependency-audit, component_impact, required_linux, required_windows\]/);
  assert.match(publish, /if: >-\n\s+\$\{\{\n\s+always\(\)/,
    'MAIN_PUBLICATION_MUST_IGNORE_TRANSITIVE_SKIPS');
  assert.match(publish, /github\.event_name == 'push' &&/);
  assert.match(publish, /github\.ref == 'refs\/heads\/main' &&/);
  for (const name of ["['dependency-audit']", '.component_impact', '.required_linux', '.required_windows']) {
    assert.ok(publish.includes(`needs${name}.result == 'success'`),
      `PUBLICATION_NEEDS_SUCCESS:${name}`);
  }
  assert.match(publish, /^    permissions:\n      contents: write$/m);
  assert.match(publish, /group: native-base-publication/);
  assert.match(publish, /ACCEPTED_MAIN: \$\{\{ github\.sha \}\}/);
  assert.doesNotMatch(publish, /github\.event\.workflow_run/);
  assert.match(publish, /"trading-engine-acceptance-windows","trading-safety-acceptance-windows","linux-core","windows-native"/);

  for (const [id, block] of map) {
    if (id !== 'publish_accepted_windows_release') {
      assert.doesNotMatch(block, /^      contents: write$/m, `UNEXPECTED_WRITE_SCOPE:${id}`);
    }
  }
}

validate(source, workflows);
assert.throws(() => validate(source, [...workflows, 'publish-native-base.yml']));
assert.throws(() => validate(source.replace(
  'needs: [dependency-audit, component_impact, required_linux, required_windows]',
  'needs: [component_impact]'), workflows));
assert.throws(() => validate(source.replace('name: classify', 'name: impact-only'), workflows));
assert.throws(() => validate(source.replace('name: rust-tests-linux', 'name: linux-trader'), workflows));
assert.throws(() => validate(source.replace('always() &&\n        github.event_name',
  'github.event_name'), workflows));
assert.throws(() => validate(source.replace("inputs.prime_windows_cargo_cache == true &&", "true &&"), workflows));
assert.throws(() => validate(source.replace("github.event_name == 'workflow_dispatch' &&", "github.event_name == 'push' &&"), workflows));
assert.throws(() => validate(source.replace('$manifestJson = & $collector --list',
  '$manifestJson = & cargo run --locked -p okx-trading-engine --example native_finance_acceptance -- --list'), workflows));
console.log('CI_LEAN_MAIN_ROUTING_CONTROLS=PASS');
