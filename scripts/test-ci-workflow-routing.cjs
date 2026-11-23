'use strict';

// #560: a single Actions workflow must not turn a manual production operation
// into a PR/push side effect or grant release permissions to test jobs.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const workflowDir = path.join(__dirname, '..', '.github', 'workflows');
const fileNames = fs.readdirSync(workflowDir)
  .filter(name => /\.ya?ml$/.test(name)).sort();
const workflow = fs.readFileSync(path.join(workflowDir, 'ci.yml'), 'utf8');

function check(source, files) {
  assert.deepEqual(files, ['ci.yml'], 'ONLY_ONE_ACTIONS_WORKFLOW_FILE');
  const [header, jobsSource] = source.split(/^jobs:\s*$/m);
  assert.ok(header && jobsSource, 'CI_JOBS_MISSING');
  assert.match(header, /^name: CI$/m);
  assert.match(header, /^  pull_request:$/m);
  assert.match(header, /^  push:$/m);
  assert.match(header, /^  workflow_dispatch:$/m);
  assert.doesNotMatch(header, /^  workflow_run:$/m);
  assert.doesNotMatch(header, /^  contents: write$/m);

  const headings = [...jobsSource.matchAll(/^  ([a-zA-Z0-9_-]+):\s*$/gm)];
  const blocks = new Map(headings.map((head, index) => [
    head[1], jobsSource.slice(head.index,
      index + 1 < headings.length ? headings[index + 1].index : undefined),
  ]));
  assert.equal(blocks.size, headings.length, 'DUPLICATE_JOB_ID');
  function job(name) {
    assert.ok(blocks.has(name), `MISSING_JOB:${name}`);
    return blocks.get(name);
  }
  function guard(name, expected) {
    const match = job(name).match(/^    if: (.+)$/m);
    assert.ok(match, `MISSING_JOB_GUARD:${name}`);
    assert.ok(match[1].includes(expected), `UNSAFE_JOB_GUARD:${name}`);
  }

  for (const name of ['dependency-audit', 'component_impact', 'required_classify',
    'required_linux', 'required_windows']) {
    guard(name, "github.event_name == 'push' && github.ref == 'refs/heads/main'");
  }
  guard('all26_windows_targeted', "github.event_name == 'push'");
  assert.match(job('all26_windows_targeted'), /contains\(fromJSON\('/);

  for (const [operation, jobs] of Object.entries({
    'credentials-check': ['credentials_observer', 'credentials_executor'],
    'readonly-account-acceptance': ['readonly_observer', 'readonly_executor',
      'readonly_identity_match', 'readonly_opening_inventory'],
    'named-account-attestation': ['named_validate', 'named_observer'],
    'cloudflare-mcp-deploy': ['cloudflare_deploy'],
  })) {
    for (const name of jobs) {
      guard(name, `github.event_name == 'workflow_dispatch' && inputs.operation == '${operation}'`);
    }
    assert.ok(header.includes(`          - ${operation}`), `MISSING_DISPATCH_OPTION:${operation}`);
  }
  assert.match(job('cloudflare_deploy'), /environment: okx-succession-global-executor/);
  assert.match(job('cloudflare_deploy'), /group: cloudflare-mcp-production/);
  assert.match(job('named_observer'), /needs: named_validate/);
  assert.match(job('readonly_identity_match'), /needs: \[readonly_observer, readonly_executor\]/);

  for (const [name, expectedNeeds] of [
    ['native_base_publish', 'needs: [dependency-audit, required_classify, required_linux, required_windows]'],
    ['production_recovery_promote', 'needs: [recovery_request_change, required_linux, required_windows, dependency-audit]'],
  ]) {
    const block = job(name);
    guard(name, "github.event_name == 'push' && github.ref == 'refs/heads/main'");
    assert.ok(block.includes(expectedNeeds), `MISSING_SAFETY_GATE:${name}`);
    assert.match(block, /^    permissions:\n      contents: write$/m);
  }
  for (const [name, block] of blocks) {
    if (!['native_base_publish', 'production_recovery_promote'].includes(name)) {
      assert.doesNotMatch(block, /^      contents: write$/m,
        `UNEXPECTED_WRITE_TOKEN:${name}`);
    }
  }
  guard('production_recovery_promote', "needs.recovery_request_change.outputs.changed == 'true'");
  guard('recovery_request_change', "github.event_name == 'push' && github.ref == 'refs/heads/main'");
  assert.match(job('recovery_request_change'),
    /git diff --name-only "\$PREVIOUS_MAIN" "\$GITHUB_SHA" -- \.github\/production-recovery-request\.json/);
  assert.match(job('native_base_publish'), /ACCEPTED_MAIN: \$\{\{ github\.sha \}\}/);
  assert.match(job('native_base_publish'), /group: native-base-publication/);
  assert.doesNotMatch(job('native_base_publish'), /github\.event\.workflow_run/);
}

check(workflow, fileNames);

// Negative controls: regressions in the release dependencies, manual-only
// deployment guard, or workflow count must fail this test immediately.
assert.throws(() => check(workflow, ['ci.yml', 'publish-native-base.yml']));
assert.throws(() => check(workflow.replace(
  'needs: [dependency-audit, required_classify, required_linux, required_windows]',
  'needs: [required_classify]'), fileNames));
assert.throws(() => check(workflow.replace(
  "github.event_name == 'workflow_dispatch' && inputs.operation == 'cloudflare-mcp-deploy'",
  "github.event_name == 'push'"), fileNames));

console.log('CI_SINGLE_WORKFLOW_ROUTING_CONTROLS=PASS');
