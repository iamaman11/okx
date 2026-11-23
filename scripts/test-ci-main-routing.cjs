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
  assert.doesNotMatch(header, /^  workflow_dispatch:$/m);
  assert.doesNotMatch(header, /^  contents: write$/m);
  assert.match(header, /cancel-in-progress: \$\{\{ github\.event_name == 'pull_request' \}\}/);

  const entries = [...jobs.matchAll(/^  ([a-zA-Z0-9_-]+):\s*$/gm)];
  const map = new Map(entries.map((m, i) => [m[1], jobs.slice(m.index,
    i + 1 < entries.length ? entries[i + 1].index : undefined)]));
  assert.equal(map.size, entries.length, 'DUPLICATE_JOB_ID');
  assert.ok(map.size <= 14, 'OBSOLETE_JOB_GRAPH_REGRESSION');
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
  const publish = job('publish_accepted_windows_release');
  assert.match(publish, /^    name: publish-accepted-windows-release$/m);
  assert.match(publish,
    /needs: \[dependency-audit, component_impact, required_linux, required_windows\]/);
  assert.match(publish,
    /if: github\.event_name == 'push' && github\.ref == 'refs\/heads\/main'/);
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
console.log('CI_LEAN_MAIN_ROUTING_CONTROLS=PASS');
