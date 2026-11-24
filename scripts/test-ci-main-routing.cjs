'use strict';

// A single automatic main CI: preserve three protected status names, keep
// optional operations out of the test graph, and never publish before gates.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const root = path.join(__dirname, '..', '.github', 'workflows');
const workflows = fs.readdirSync(root).filter(x => /\.ya?ml$/.test(x));
const source = fs.readFileSync(path.join(root, 'ci.yml'), 'utf8');
const researchRelease = fs.readFileSync(path.join(root, '..', '..', 'scripts', 'prepare-research-release.ps1'), 'utf8');

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
  assert.match(header, /^        default: true$/m);
  assert.doesNotMatch(header, /^  contents: write$/m);
  assert.match(header, /cancel-in-progress: \$\{\{ github\.event_name == 'pull_request' \}\}/);

  const entries = [...jobs.matchAll(/^  ([a-zA-Z0-9_-]+):\s*$/gm)];
  const map = new Map(entries.map((m, i) => [m[1], jobs.slice(m.index,
    i + 1 < entries.length ? entries[i + 1].index : undefined)]));
  assert.equal(map.size, entries.length, 'DUPLICATE_JOB_ID');
  // Finance, independent regressions, B1 and release share a single workflow.
  // The only cache-only job is the explicit trusted-main seed.
  assert.ok(map.size <= 17, 'OBSOLETE_JOB_GRAPH_REGRESSION');
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
  const classify = job('component_impact');
  assert.match(classify, /^    name: classify$/m);
  assert.match(classify, /component_scope: \$\{\{ steps.proven_scope.outputs.scope \}\}/);
  assert.match(classify, /--dependency-candidate < "\$RUNNER_TEMP\/ci-files.json"/);
  assert.match(classify, /if: github.event_name == 'pull_request' && github.event.pull_request.draft != true && steps.file_scope.outputs.manifest_candidate != 'none'/);
  assert.match(classify, /git fetch --no-tags --depth=1 origin "\$BASE_SHA"/);
  assert.match(classify, /git worktree add --detach "\$base" "\$BASE_SHA"/);
  assert.equal((classify.match(/cargo metadata --locked --format-version 1 --filter-platform x86_64-pc-windows-msvc/g) || []).length, 2);
  assert.match(classify, /node scripts\/ci-cargo-closure-proof.cjs "\$temp\/base.json" "\$temp\/head.json"/);
  assert.match(classify, /if \[\[ -n '\$\{\{ steps.cargo_closure.outputs.scope \}\}' \]\]; then/);
  assert.match(classify, /scope="\$\{\{ steps.proven_scope.outputs.scope \}\}"/);
  assert.match(classify, /steps.proven_scope.outputs.scope == 'research'/);
  assert.doesNotMatch(classify, /native_required=false[^\n]*CARGO/);
  assert.match(researchRelease, /git worktree add --detach \$baselineDir \$mergeSha/);
  assert.match(researchRelease, /cargo metadata --locked --format-version 1 --filter-platform x86_64-pc-windows-msvc/);
  assert.match(researchRelease, /node scripts\/research-release-scope.cjs @scopeArgs/);
  for (const [id, display] of [['required_linux', 'linux-core'],
    ['required_windows', 'windows-native']]) {
    assert.match(job(id), new RegExp(`^    name: ${display}$`, 'm'));
    assert.match(job(id), /needs: \[component_impact,/);
  }
  const names = {
    rust_tests_linux: 'rust-tests-linux',
    trading_engine_acceptance_windows: 'trading-engine-acceptance-windows',
    trading_engine_regressions_windows: 'trading-engine-regressions-windows',
    trading_engine_build_windows: 'trading-engine-build-windows',
    trusted_main_test_cache_prime: 'trusted-main-windows-test-cache-prime',
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
  assert.match(engine, /shared-key: trading-engine-tests-windows/);
  assert.doesNotMatch(engine, /cargo build --locked --release|name: okx-windows-bundle-/);
  // The real budgeted financial collector remains on the isolated runner.
  assert.match(engine, /Mandatory current Rust financial property acceptance/);
  assert.match(engine, /Isolated Native pending-fill crash recovery/);
  assert.match(engine, /MCP DO WSS Native finance under isolated Research load/);
  assert.doesNotMatch(engine, /K9 worker two-heavy cancellation latency evidence/);
  const regression = job('trading_engine_regressions_windows');
  assert.match(regression, /needs: component_impact/);
  assert.match(regression, /if: needs\.component_impact\.outputs\.build_required == 'true' && needs\.component_impact\.outputs\.native_required != 'false' && needs\.component_impact\.outputs\.research_only != 'true'/);
  assert.match(regression, /shared-key: trading-engine-tests-windows/);
  assert.match(regression, /Clippy native Windows crates/);
  assert.match(regression, /Strategy Core and Research owner regressions on full Windows/);
  assert.match(regression, /Native trading, SQL, CONTROL and Launcher remaining regression/);
  // Never make timed terminal-source frontiers vanish into --skip. Their
  // two original Engine tests are instead required to pass serially here.
  for (const witness of [
    'stale_fill_coverage_blocks_release_before_worker',
    'stale_bill_coverage_blocks_settlement_after_inspect',
  ]) {
    assert.match(regression, new RegExp(`'--skip','trade_service::terminal_release::tests::${witness}'`));
  }
  assert.match(regression, /name: Serial terminal release source-frontier regression/);
  assert.match(regression, /cargo test --locked -p okx-trading-engine --lib trade_service::terminal_release::tests::stale_ -- --test-threads 1/);
  assert.match(regression, /2 passed; 0 failed;/);
  assert.match(regression, /BOTH_TERMINAL_SOURCE_FRONTIER_WITNESSES_REQUIRED/);
  assert.match(regression, /cargo run --locked -p okx-trading-engine --example native_finance_acceptance -- --list/);
  assert.match(regression, /K9 worker two-heavy cancellation latency evidence/);
  assert.doesNotMatch(regression, /Mandatory current Rust financial property acceptance|Isolated Native pending-fill crash recovery|name: okx-windows-bundle-/);
  const build = job('trading_engine_build_windows');
  assert.match(build, /\(github.event_name == 'workflow_dispatch' &&\n         inputs.prime_windows_cargo_cache == true &&/);
  assert.match(build, /inputs.prime_windows_cargo_cache == true &&/);
  assert.match(build, /github.ref == 'refs\/heads\/main' &&/);
  assert.match(build, /needs.component_impact.outputs.build_required == 'false'/);
  assert.match(build, /merged tree exactly matches successful PR head tree/);
  assert.match(build, /needs.component_impact.outputs.native_required != 'false'/);
  assert.match(build, /shared-key: trading-engine-release-windows/);
  assert.doesNotMatch(build, /shared-key: trading-engine-tests-windows/);
  assert.match(build, /cache-on-failure: false/);
  assert.match(build, /cargo build --locked --release/);
  assert.match(build, /name: okx-windows-bundle-\$\{\{ github.event.pull_request.head.sha \}\}/);
  assert.match(build, /if: github.event_name == 'pull_request'\n        uses: actions\/upload-artifact/);
  assert.match(build, /source_head_sha = \$sourceHead/);
  assert.doesNotMatch(build, /gh release|cargo run|cargo test --locked .* -- --nocapture/);
  assert.match(build, /FINANCIAL_TESTS=NOT_EXECUTED; RELEASE=NOT_PUBLISHED; ENGINE=NOT_INSTALLED/);
  const prime = job('trusted_main_test_cache_prime');
  assert.match(prime, /github.event_name == 'workflow_dispatch' &&/);
  assert.match(prime, /inputs.prime_windows_cargo_cache == true &&/);
  assert.match(prime, /github.ref == 'refs\/heads\/main' &&/);
  assert.match(prime, /needs.component_impact.outputs.build_required == 'false'/);
  assert.match(prime, /merged tree exactly matches successful PR head tree/);
  assert.match(prime, /shared-key: trading-engine-tests-windows/);
  assert.doesNotMatch(prime, /shared-key: trading-engine-release-windows/);
  assert.match(prime, /cache-on-failure: false/);
  assert.match(prime, /cargo test --locked --no-run --all-targets/);
  assert.match(prime, /cargo build --locked -p okx-trading-engine --example native_finance_acceptance/);
  assert.doesNotMatch(prime, /gh release|actions\/upload-artifact|cargo run|cargo build --locked --release/);
  assert.match(prime, /FINANCIAL_TESTS=NOT_EXECUTED; RELEASE=NOT_PUBLISHED; ENGINE=NOT_INSTALLED/);
  const b1 = job('trading_safety_acceptance_windows');
  assert.match(b1, /shared-key: trading-engine-tests-windows/);
  assert.doesNotMatch(b1, /shared-key: trading-engine-release-windows/);
  assert.match(b1, /Precompile current Rust property targets/);
  assert.match(b1, /CURRENT_NATIVE_B1_PUBLIC_WITNESSES_MISSING/);
  const windows = job('required_windows');
  assert.match(windows, /needs: \[component_impact, trading_engine_acceptance_windows, trading_engine_regressions_windows, trading_engine_build_windows,/);
  assert.match(windows, /test "\$\{\{ needs\.trading_engine_regressions_windows\.result \}\}" = "success"/);
  assert.equal((windows.match(/test "\$\{\{ needs\.trading_engine_regressions_windows\.result \}\}" = "skipped"/g) || []).length, 4,
    'REGRESSION_JOB_MUST_BE_SKIPPED_ON_EVERY_NON_FINANCE_PATH');
  assert.match(windows, /test "\$\{\{ needs.trading_engine_build_windows.result \}\}" = "success"/);
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
  assert.match(publish, /"trading-engine-acceptance-windows","trading-engine-regressions-windows","trading-engine-build-windows","trading-safety-acceptance-windows","linux-core","windows-native"/);

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
assert.throws(() => validate(source.replace('component_scope: ${{ steps.proven_scope.outputs.scope }}',
  'component_scope: ${{ steps.file_scope.outputs.scope }}'), workflows));
assert.throws(() => validate(source.replace('if [[ -n \'${{ steps.cargo_closure.outputs.scope }}\' ]]; then',
  'if [[ -n \'${{ steps.file_scope.outputs.scope }}\' ]]; then'), workflows));
assert.throws(() => validate(source.replace('git fetch --no-tags --depth=1 origin "$BASE_SHA"',
  'git fetch --no-tags --depth=1 origin main'), workflows));
assert.throws(() => validate(source.replace('name: rust-tests-linux', 'name: linux-trader'), workflows));
assert.throws(() => validate(source.replace('        default: true', '        default: false'), workflows));
assert.throws(() => validate(source.replace('BOTH_TERMINAL_SOURCE_FRONTIER_WITNESSES_REQUIRED',
  'WE_DID_NOT_VERIFY_THE_TWO_TESTS'), workflows));
assert.throws(() => validate(source.replace('stale_bill_coverage_blocks_settlement_after_inspect',
  'stale_bill_not_covered'), workflows));
assert.throws(() => validate(source.replace(/(name: trading-safety-acceptance-windows[\s\S]*?shared-key: )trading-engine-tests-windows/,
  '$1trading-engine-release-windows'), workflows));
assert.throws(() => validate(source.replace('always() &&\n        github.event_name',
  'github.event_name'), workflows));
assert.throws(() => validate(source.replace("inputs.prime_windows_cargo_cache == true &&", "true &&"), workflows));
assert.throws(() => validate(source.replace("github.event_name == 'workflow_dispatch' &&", "github.event_name == 'push' &&"), workflows));
assert.throws(() => validate(source.replace('test "${{ needs.trading_engine_build_windows.result }}" = "success"', 'true'), workflows));
assert.throws(() => validate(source.replace('test "${{ needs.trading_engine_regressions_windows.result }}" = "success"', 'true'), workflows));
assert.throws(() => validate(source.replace('test "${{ needs.trading_engine_regressions_windows.result }}" = "skipped"', 'true'), workflows));
assert.throws(() => validate(source.replace('needs: [component_impact, trading_engine_acceptance_windows, trading_engine_regressions_windows,', 'needs: [component_impact, trading_engine_acceptance_windows,'), workflows));
assert.throws(() => validate(source.replace('"trading-engine-acceptance-windows","trading-engine-regressions-windows",', '"trading-engine-acceptance-windows",'), workflows));
assert.throws(() => validate(source.replace('shared-key: trading-engine-release-windows', 'shared-key: trading-engine-tests-windows'), workflows));
assert.throws(() => validate(source.replace('shared-key: trading-engine-tests-windows', 'shared-key: trading-engine-release-windows'), workflows));
assert.throws(() => validate(source.replace('$manifestJson = & $collector --list',
  '$manifestJson = & cargo run --locked -p okx-trading-engine --example native_finance_acceptance -- --list'), workflows));
console.log('CI_LEAN_MAIN_ROUTING_CONTROLS=PASS');
