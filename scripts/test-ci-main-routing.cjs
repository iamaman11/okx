'use strict';

// A single automatic main CI: preserve three protected status names, keep
// optional operations out of the test graph, and never publish before gates.
const assert = require('node:assert/strict');
const {execFileSync} = require('node:child_process');
const fs = require('node:fs');
const path = require('node:path');

const root = path.join(__dirname, '..', '.github', 'workflows');
const workflows = fs.readdirSync(root).filter(x => /\.ya?ml$/.test(x));
const source = fs.readFileSync(path.join(root, 'ci.yml'), 'utf8').replaceAll('\r\n', '\n');
const researchRelease = fs.readFileSync(path.join(root, '..', '..', 'scripts', 'prepare-research-release.ps1'), 'utf8').replaceAll('\r\n', '\n');
const mainCacheWriter = "${{ github.ref == 'refs/heads/main' }}";
const warmPrefix = "${{ matrix.profile == 'tests' && 'v2-rust' || 'v1-rust' }}";
const regressionSuites = ['engine', 'host', 'execution', 'research', 'clippy'];
const releaseGates = ['trading-engine-acceptance-windows',
  ...regressionSuites.map(suite => `trading-engine-regressions-windows (${suite})`),
  'trading-engine-build-windows', 'linux-core', 'windows-native'];
const nativeCacheJobs = [
  ['trading_engine_acceptance_windows', 'v2-rust', 'trading-engine-tests-windows', 'false'],
  ['trading_engine_regressions_windows', 'v2-rust', 'trading-engine-tests-windows', 'false'],
  ['trading_engine_build_windows', 'v1-rust', 'trading-engine-release-windows', 'false'],
  ['trusted_main_cache_prime', warmPrefix, 'trading-engine-${{ matrix.profile }}-windows', mainCacheWriter],
];

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
  // Finance, independent regressions and release share a single workflow.
  // The only cache-only job primes exact accepted main, outside the PR gate.
  assert.ok(map.size <= 17, 'OBSOLETE_JOB_GRAPH_REGRESSION');
  for (const unused of ['required_classify', 'draft-trading-migration-512',
    'draft-riskbook-migration-510', 'trading_safety_acceptance_windows', 'cloudflare_deploy', 'readonly_observer',
    'production_recovery_promote', 'named_validate', 'all26_windows_targeted']) {
    assert.ok(!map.has(unused), `UNNECESSARY_CI_JOB:${unused}`);
  }
  function job(id) {
    assert.ok(map.has(id), `MISSING_JOB:${id}`);
    return map.get(id);
  }
  // Cache writes stay on exact accepted main. Tests and shipping builds
  // use separate target caches.
  for (const [id, prefix, shared, save] of nativeCacheJobs) {
    const block = job(id);
    const starts = [...block.matchAll(/^      - uses: Swatinem\/rust-cache@[0-9a-f]{40}(?: #[^\n]+)?$/gm)];
    assert.equal(starts.length, 1, `EXACTLY_ONE_NATIVE_RUST_CACHE:${id}`);
    const end = block.indexOf('\n      - ', starts[0].index + starts[0][0].length);
    const step = block.slice(starts[0].index, end < 0 ? undefined : end);
    assert.ok(step.split('\n').includes(`          prefix-key: ${prefix}`), `NATIVE_CACHE_GENERATION:${id}`);
    assert.match(step, /^          cache-workspace-crates: true$/m, `NATIVE_WORKSPACE_CACHE:${id}`);
    assert.ok(step.split('\n').includes(`          shared-key: ${shared}`), `NATIVE_CACHE_PROFILE:${id}`);
    assert.ok(step.split('\n').includes(`          save-if: ${save}`), `SCOPED_CACHE_WRITER:${id}`);
    assert.match(step, /^          cache-on-failure: false$/m);
    if (['trading_engine_acceptance_windows', 'trading_engine_regressions_windows',
      'trading_engine_build_windows'].includes(id)) {
      for (const profile of ['DEV', 'TEST'])
        assert.match(block, new RegExp(`^      CARGO_PROFILE_${profile}_DEBUG: 0$`, 'm'));
    }
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
  assert.match(classify, /compare\/\$base\.\.\.\$CHECKED_HEAD/);
  assert.match(classify, /\.commits\[-1\]\.sha == \$head/);
  assert.match(classify, /host-controller\|host-launcher\) edge_required="\$\{\{ steps\.file_scope\.outputs\.edge_changed \}\}"/);
  assert.doesNotMatch(classify, /pulls\/\$PR_NUMBER\/files|prepare-research-release\.ps1/);
  assert.match(classify, /"research_only=true" >> "\$GITHUB_OUTPUT"/);
  assert.match(classify, /native_proof=RESEARCH_OWNER_ONLY/);
  const research = job('research_service_tests_windows');
  assert.match(research, /base_head: \$\{\{ steps\.package\.outputs\.base_head \}\}/);
  assert.equal((research.match(/prepare-research-release\.ps1 -Dist/g) || []).length, 1);
  assert.match(research, /RESEARCH_BASE_PROOF_REQUIRED/);
  // Research-only changes must not resurrect the four real-child IPC timeouts
  // previously observed when nine Windows processes started in parallel. The
  // dedicated harness retains the original 10s receive deadline.
  assert.match(research, /cargo test --locked -p okx-research-service --test ipc_process -- --nocapture --test-threads 1/);
  assert.match(research, /WORKER_IPC_TESTS_MISSING|RESEARCH_IPC_ALL_NINE_TESTS_REQUIRED/);
  assert.doesNotMatch(research, /cargo test --locked -p okx-strategy-core -p okx-research -p okx-research-service -p okx-analysis\s/,
    'SCOPED_RESEARCH_MUST_NOT_RUN_UNBOUNDED_WORKER_INTEGRATIONS');
  assert.doesNotMatch(classify, /native_required=false[^\n]*CARGO/);
  assert.match(researchRelease, /git worktree add --detach \$baselineDir \$mergeSha/);
  assert.match(researchRelease, /cargo metadata --locked --format-version 1 --filter-platform x86_64-pc-windows-msvc/);
  assert.match(researchRelease, /node scripts\/research-release-scope.cjs @scopeArgs/);
  const host = job('host_controller_launcher_checks');
  assert.match(host, /cargo clippy --locked -p okx-host-protocol -p okx-host-launcher -p okx-host-controller -p okx-bridge-mcp/);
  assert.match(host, /cargo test --locked -p okx-host-protocol -p okx-host-launcher -p okx-host-controller -p okx-bridge-mcp/);
  assert.doesNotMatch(host, /cargo build --locked --release -p okx-trading-engine/);
  for (const [id, display] of [['required_linux', 'linux-core'],
    ['required_windows', 'windows-native']]) {
    assert.match(job(id), new RegExp(`^    name: ${display}$`, 'm'));
    assert.match(job(id), /needs: \[component_impact,/);
  }
  const names = {
    rust_tests_linux: 'rust-tests-linux',
    trading_engine_acceptance_windows: 'trading-engine-acceptance-windows',
    trading_engine_build_windows: 'trading-engine-build-windows',
    research_service_tests_windows: 'research-service-tests-windows',
    host_controller_launcher_checks: 'host-controller-launcher-checks',
    'research_package_proof': 'research-package-proof',
  };
  for (const [id, display] of Object.entries(names)) {
    assert.match(job(id), new RegExp(`^    name: ${display}$`, 'm'),
      `AMBIGUOUS_CI_JOB_NAME:${id}`);
  }
  // Edge transport has one required owner. Running the same npm/Miniflare
  // checks again inside portable Rust CI doubles setup without new evidence.
  const linux = job('rust_tests_linux');
  const edge = job('cloudflare_mcp_contracts');
  assert.doesNotMatch(linux, /npm ci|npm run check|test-native-finance-retirement-do\.mjs|actions\/setup-node@/);
  assert.match(edge, /npm ci --ignore-scripts --no-audit --no-fund/);
  assert.match(edge, /needs\.component_impact\.outputs\.edge_required == 'true'/);
  assert.match(edge, /npm run check/);
  assert.match(edge, /node scripts\/test-native-finance-retirement-do\.mjs/);
  const engine = job('trading_engine_acceptance_windows');
  assert.doesNotMatch(engine, /cargo run[^\n]*-- --list/, 'REDUNDANT_CARGO_FINANCIAL_LIST');
  assert.doesNotMatch(engine, /id: native-properties\s+if:/, 'FULL_MAIN_FALLBACK_MUST_RUN_FINANCE');
  assert.match(engine, /shared-key: trading-engine-tests-windows/);
  assert.doesNotMatch(engine, /cargo build --locked --release|name: okx-windows-bundle-/);
  // The real budgeted financial collector remains on the isolated runner.
  assert.match(engine, /Mandatory current Rust financial property acceptance/);
  assert.match(engine, /node scripts\/ci-finance-witness-manifest\.cjs --verify-report/);
  assert.match(engine, /Isolated Native pending-fill crash recovery/);
  assert.match(engine, /MCP DO WSS Native finance under isolated Research load/);
  assert.match(engine, /standalone_catalog_reopens_through_real_mcp_do_websocket -- --exact --ignored --nocapture/);
  assert.match(engine, /STANDALONE_RESEARCH_MCP_WITNESS_NOT_RUN/);
  assert.doesNotMatch(engine, /K9 worker two-heavy cancellation latency evidence/);
  const regression = job('trading_engine_regressions_windows');
  assert.match(regression, /needs: component_impact/);
  assert.match(regression, /if: needs\.component_impact\.outputs\.build_required == 'true' && needs\.component_impact\.outputs\.native_required != 'false' && needs\.component_impact\.outputs\.research_only != 'true'/);
  assert.ok(regression.includes('    name: trading-engine-regressions-windows (${{ matrix.suite }})'));
  assert.ok(regression.includes(`      fail-fast: false\n      matrix:\n        suite: [${regressionSuites.join(', ')}]`));
  for (const [name, scope] of [
    ['Clippy native Windows crates', "matrix.suite == 'clippy'"],
    ['Strategy Core and Research owner regressions on full Windows', "matrix.suite == 'research'"],
    ['Build research worker for regression process contracts', "matrix.suite == 'engine'"],
    ['Remaining native regression tests for this suite', "matrix.suite == 'engine' || matrix.suite == 'host' || matrix.suite == 'execution'"],
    ['Serial native account catalog and writer-termination regressions', "matrix.suite == 'engine'"],
    ['Serial terminal release source-frontier regression', "matrix.suite == 'engine'"],
    ['Agent dispatch to actual research child', "matrix.suite == 'engine'"],
    ['Worker library regression and actual IPC child', "matrix.suite == 'research'"],
    ['K9 worker two-heavy cancellation latency evidence', "matrix.suite == 'research'"],
    ['Upload K9 native latency evidence', "matrix.suite == 'research'"],
  ]) assert.ok(regression.includes(`      - name: ${name}\n        if: ${scope}\n`), `NATIVE_REGRESSION_SUITE:${name}`);
  assert.match(regression, /'host' \{ 'okx-host-controller'; 'okx-host-launcher'; 'okx-host-protocol'; 'okx-bridge-mcp' \}/);
  assert.match(regression, /FULL_HOST_RESEARCH_WITNESS_NOT_RUN_\$witness/);
  for (const witness of [
    'one_process_job_can_be_terminated_without_touching_the_other',
    'nested_job_assignment_waits_for_parent_ready_receipt',
    'research_control_root_is_independent_and_start_requires_verified_installation',
    'corrupted_and_hash_mismatched_research_image_never_launch',
    'standalone_research_lifecycle_requires_exact_image_and_process_generation',
    'research_token_provision_command_has_no_bearer_fields',
    'typed_research_rollback_requires_two_distinct_exact_hashes',
    'control_bridge_denies_research_mutations_before_network',
  ]) assert.match(regression, new RegExp(witness));
  assert.match(regression, /WINDOWS_RESEARCH_SECRET_OWNER_WITNESS_NOT_RUN_\$witness/);
  for (const witness of [
    'research_token_validation_is_bounded_and_ascii_only',
    'research_native_target_is_distinct_from_engine',
    'mock_store_never_exposes_research_bearer_to_engine_target',
    'research_starts_only_after_finite_supervisor_job_receipt',
    'installed_research_bearer_is_pinned_to_control_edge_origin',
  ]) assert.match(regression, new RegExp(witness));
  assert.match(regression, /cargo test --locked -p okx-research-service --test ipc_process -- --nocapture --test-threads 1/);
  assert.match(regression, /'execution' \{ 'okx-trade-execution' \}/);
  // The six 3s SQLite-writer/catalog fixtures are still mandatory, just not
  // contending with the remaining 300 Engine libtests for the test-harness clock.
  assert.match(regression, /\$testArgs \+= @\('--skip','accounts::native_connect_contracts::'\)/);
  assert.match(regression, /name: Serial native account catalog and writer-termination regressions/);
  assert.match(regression, /cargo test --locked -p okx-trading-engine --lib accounts::native_connect_contracts:: -- --test-threads 1/);
  assert.match(regression, /NATIVE_ACCOUNT_CATALOG_ALL_SIX_TESTS_REQUIRED/);
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
  assert.match(regression, /\$manifestJson = & node scripts\/ci-finance-witness-manifest\.cjs/);
  assert.doesNotMatch(regression, /cargo run[^\n]*(?:native_finance_acceptance|okx-native-finance-check) -- --list/);
  assert.match(regression, /K9 worker two-heavy cancellation latency evidence/);
  assert.doesNotMatch(regression, /Mandatory current Rust financial property acceptance|Isolated Native pending-fill crash recovery|name: okx-windows-bundle-/);
  const build = job('trading_engine_build_windows');
  assert.match(build, /needs.component_impact.outputs.build_required == 'true' && needs.component_impact.outputs.native_required != 'false'/);
  assert.doesNotMatch(build, /workflow_dispatch|name: Seed|CACHE_SEED/);
  assert.match(build, /shared-key: trading-engine-release-windows/);
  assert.doesNotMatch(build, /shared-key: trading-engine-tests-windows/);
  assert.match(build, /cache-on-failure: false/);
  assert.match(build, /cargo build --locked --release/);
  assert.match(build, /name: okx-windows-bundle-\$\{\{ github.event.pull_request.head.sha \}\}/);
  assert.match(build, /if: github.event_name == 'pull_request'\n        uses: actions\/upload-artifact/);
  assert.match(build, /source_head_sha = \$sourceHead/);
  assert.doesNotMatch(build, /gh release|cargo run|cargo test --locked .* -- --nocapture/);
  const prime = job('trusted_main_cache_prime');
  assert.ok(prime.includes("name: trusted-main-windows-cache-prime (${{ matrix.profile }})"));
  assert.match(prime, /^        profile: \[tests, release\]$/m);
  assert.match(prime, /github.event_name == 'push' && needs.component_impact.outputs.component_scope == 'full'/);
  assert.match(prime, /github.event_name == 'workflow_dispatch' && inputs.prime_windows_cargo_cache == true/);
  assert.match(prime, /github.ref == 'refs\/heads\/main' &&/);
  assert.match(prime, /needs.component_impact.outputs.build_required == 'false'/);
  assert.match(prime, /merged tree exactly matches successful PR head tree/);
  for (const profile of ['DEV', 'TEST']) {
    assert.match(prime, new RegExp(`^      CARGO_PROFILE_${profile}_DEBUG: 0$`, 'm'));
    assert.doesNotMatch(prime, new RegExp(`CARGO_PROFILE_${profile}_DEBUG:.*\\|\\|`),
      'CACHE_PRIME_MUST_NOT_PROVIDE_AN_EMPTY_PROFILE_ENV_ON_RELEASE');
  }
  assert.match(prime, /cache-on-failure: false/);
  assert.match(prime, /cargo test --locked --no-run --all-targets/);
  assert.match(prime, /cargo build --locked -p okx-native-finance-check/);
  assert.match(prime, /matrix.profile == 'release' && steps.prime-cache.outputs.cache-hit != 'true'/);
  assert.match(prime, /cargo build --locked --release -p okx-trading-engine -p okx-host-controller -p okx-host-launcher -p okx-research-service/);
  assert.doesNotMatch(prime, /gh release|actions\/upload-artifact|cargo run/);
  assert.match(prime, /FINANCIAL_TESTS=NOT_EXECUTED; RELEASE=NOT_PUBLISHED; ENGINE=NOT_INSTALLED/);
  // Unlike the Debug/Test matrix, the PR release builder used to omit the
  // profile-debug env that the trusted-main release cache prime had set. That
  // changed rustc cache fingerprints and recompiles identical Engine code.
  const release = job('trading_engine_build_windows');
  for (const profile of ['DEV', 'TEST']) {
    const directive = new RegExp(`^      CARGO_PROFILE_${profile}_DEBUG: 0$`, 'm');
    assert.match(release, directive);
    assert.match(prime, directive);
  }
  const windows = job('required_windows');
  assert.match(windows, /needs: \[component_impact, trading_engine_acceptance_windows, trading_engine_regressions_windows, trading_engine_build_windows,/);
  assert.match(windows, /test "\$\{\{ needs\.component_impact\.outputs\.native_proof \}\}" = "RESEARCH_OWNER_ONLY"/);
  assert.match(windows, /test "\$\{\{ needs\.trading_engine_regressions_windows\.result \}\}" = "success"/);
  assert.equal((windows.match(/test "\$\{\{ needs\.trading_engine_regressions_windows\.result \}\}" = "skipped"/g) || []).length, 5,
    'REGRESSION_JOB_MUST_BE_SKIPPED_ON_EVERY_NON_FINANCE_PATH');
  const mixed = windows.slice(windows.indexOf('= "RESEARCH_HOST_OWNER_CHECKS"'),
    windows.indexOf('WINDOWS_NATIVE_PROOF=MIXED_OWNER_CHECKS_WITH_VERIFIED_ENGINE'));
  assert.ok(mixed.length > 0);
  assert.match(mixed, /mixed_base_head.*\^\[0-9a-f\]\{40\}\$/);
  for (const job of ['acceptance', 'regressions', 'build'])
    assert.ok(mixed.includes('needs.trading_engine_' + job + '_windows.result }}" = "skipped"'));
  for (const job of ['host_controller_launcher_checks', 'research_service_tests_windows'])
    assert.ok(mixed.includes('needs.' + job + '.result }}" = "success"'));
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
  assert.ok(publish.includes(JSON.stringify(releaseGates)), 'PUBLICATION_REQUIRES_EACH_WINDOWS_SUITE');

  for (const [id, block] of map) {
    if (id !== 'publish_accepted_windows_release') {
      assert.doesNotMatch(block, /^      contents: write$/m, `UNEXPECTED_WRITE_SCOPE:${id}`);
    }
  }
}

validate(source, workflows);
// A Research-owned OS resource policy can change native Finance latency even
// without touching Engine source while the Engine-hosted Worker still exists.
// Route that rare change through the real Windows financial acceptance gate.
function scopeFor(filename) {
  const python = process.platform === 'win32' ? 'python' : 'python3';
  return execFileSync(python, ['-B', 'scripts/check_ci_native_scope.py'], {
    cwd: path.join(__dirname, '..'), encoding: 'utf8',
    input: JSON.stringify([[{filename, status: 'modified'}]]),
  }).trim();
}
assert.equal(scopeFor('crates/okx-research-service/src/process_memory.rs'), 'full');
assert.equal(scopeFor('crates/okx-research-service/src/main.rs'), 'full');
assert.equal(scopeFor('crates/okx-research-service/src/analytics/jobs.rs'), 'research');
assert.equal(scopeFor('crates/okx-host-controller/src/job.rs'), 'full');
assert.equal(scopeFor('crates/okx-host-controller/src/lifecycle_executor.rs'), 'full');
assert.equal(scopeFor('crates/okx-host-controller/src/research_supervisor.rs'), 'host-controller');
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
assert.throws(() => validate(source.replace('      - name: Test portable crates',
  '      - run: npm ci --ignore-scripts --no-audit --no-fund\n      - name: Test portable crates'), workflows));
assert.throws(() => validate(source.replace('        default: true', '        default: false'), workflows));
assert.throws(() => validate(source.replace('BOTH_TERMINAL_SOURCE_FRONTIER_WITNESSES_REQUIRED',
  'WE_DID_NOT_VERIFY_THE_TWO_TESTS'), workflows));
assert.throws(() => validate(source.replace('NATIVE_ACCOUNT_CATALOG_ALL_SIX_TESTS_REQUIRED',
  'WE_DID_NOT_VERIFY_THE_SIX_TESTS'), workflows));
assert.throws(() => validate(source.replace("$testArgs += @('--skip','accounts::native_connect_contracts::')",
  "# no catalog exclusion"), workflows));
assert.throws(() => validate(source.replace('stale_bill_coverage_blocks_settlement_after_inspect',
  'stale_bill_not_covered'), workflows));
assert.throws(() => validate(source.replace('always() &&\n        github.event_name',
  'github.event_name'), workflows));
assert.throws(() => validate(source.replace("inputs.prime_windows_cargo_cache == true", "true"), workflows));
assert.throws(() => validate(source.replace("github.event_name == 'workflow_dispatch' && inputs.prime_windows_cargo_cache == true", "github.event_name == 'pull_request'"), workflows));
assert.throws(() => validate(source.replace("github.event_name == 'push' && needs.component_impact.outputs.component_scope == 'full'", "github.event_name == 'push'"), workflows));
assert.throws(() => validate(source.replace('test "${{ needs.trading_engine_build_windows.result }}" = "success"', 'true'), workflows));
assert.throws(() => validate(source.replace('test "${{ needs.trading_engine_regressions_windows.result }}" = "success"', 'true'), workflows));
assert.throws(() => validate(source.replace('test "${{ needs.trading_engine_regressions_windows.result }}" = "skipped"', 'true'), workflows));
assert.throws(() => validate(source.replace('"research_only=true" >> "$GITHUB_OUTPUT"', 'true'), workflows));
assert.throws(() => validate(source.replace('RESEARCH_BASE_PROOF_REQUIRED', 'BYPASS'), workflows));
assert.throws(() => validate(source.replace('.commits[-1].sha == $head', 'true'), workflows));
assert.throws(() => validate(source.replace('test "${{ needs.component_impact.outputs.native_proof }}" = "RESEARCH_OWNER_ONLY"', 'true'), workflows));
assert.throws(() => validate(source.replace('node scripts/ci-finance-witness-manifest.cjs --verify-report', 'echo skipped'), workflows));
assert.throws(() => validate(source.replace('$manifestJson = & node scripts/ci-finance-witness-manifest.cjs', '$manifestJson = & cargo run --locked -p okx-native-finance-check -- --list'), workflows));
assert.throws(() => validate(source.replace('needs: [component_impact, trading_engine_acceptance_windows, trading_engine_regressions_windows,', 'needs: [component_impact, trading_engine_acceptance_windows,'), workflows));
assert.throws(() => validate(source.replace('"trading-engine-regressions-windows (engine)",', ''), workflows));
assert.throws(() => validate(source.replace('suite: [engine, host, execution, research, clippy]', 'suite: [engine, host, execution, research]'), workflows));
assert.throws(() => validate(changeJobInput(source, 'trading_engine_build_windows',
  'shared-key: trading-engine-release-windows', 'shared-key: trading-engine-tests-windows'), workflows));
assert.throws(() => validate(changeJobInput(source, 'trading_engine_acceptance_windows',
  'shared-key: trading-engine-tests-windows', 'shared-key: trading-engine-release-windows'), workflows));
// Scope mutations to named native jobs: unrelated future Rust caches must not
// create false positives, and YAML input order is irrelevant.
function changeJobInput(yaml, id, before, after) {
  const entries = [...yaml.matchAll(/^  ([a-zA-Z0-9_-]+):[ \t]*$/gm)];
  const index = entries.findIndex(entry => entry[1] === id);
  assert.ok(index >= 0, `MISSING_CACHE_JOB:${id}`);
  const start = entries[index].index;
  const end = entries[index + 1]?.index ?? yaml.length;
  const block = yaml.slice(start, end);
  assert.ok(block.includes(before), `MISSING_CACHE_INPUT:${id}`);
  return yaml.slice(0, start) + block.replace(before, after) + yaml.slice(end);
}
for (const [id, prefix, , save] of nativeCacheJobs) {
  for (const [before, after] of [
    [`prefix-key: ${prefix}`, 'prefix-key: v0-rust'],
    ['cache-workspace-crates: true', 'cache-workspace-crates: false'],
    [`save-if: ${save}`, 'save-if: true'],
  ]) {
    assert.throws(() => validate(changeJobInput(source, id, before, after), workflows));
  }
}
assert.throws(() => validate(changeJobInput(source, 'research_service_tests_windows',
  'ipc_process -- --nocapture --test-threads 1',
  'ipc_process -- --nocapture --test-threads 8'), workflows));
assert.throws(() => validate(changeJobInput(source, 'research_service_tests_windows',
  'cargo test --locked -p okx-strategy-core -p okx-research -p okx-analysis',
  'cargo test --locked -p okx-strategy-core -p okx-research -p okx-research-service -p okx-analysis'), workflows));
for (const profile of ['DEV', 'TEST']) {
  assert.throws(() => validate(changeJobInput(source, 'trading_engine_build_windows',
    `CARGO_PROFILE_${profile}_DEBUG: 0`, `CARGO_PROFILE_${profile}_DEBUG: ''`), workflows));
}
console.log('CI_LEAN_MAIN_ROUTING_CONTROLS=PASS');
