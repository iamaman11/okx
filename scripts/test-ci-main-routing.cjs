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
const disposableScript = fs.readFileSync(path.join(root, '..', '..', 'scripts',
  'ci-s2-disposable-windows.ps1'), 'utf8').replaceAll('\r\n', '\n');
const researchRelease = fs.readFileSync(path.join(root, '..', '..', 'scripts', 'prepare-research-release.ps1'), 'utf8').replaceAll('\r\n', '\n');
const ipcProcess = fs.readFileSync(path.join(root, '..', '..', 'crates',
  'okx-research-service', 'tests', 'ipc_process.rs'), 'utf8').replaceAll('\r\n', '\n');
const mainCacheWriter = "${{ github.ref == 'refs/heads/main' }}";
const warmPrefix = "${{ matrix.profile == 'tests' && 'v2-rust' || 'v1-rust' }}";
// Unknown/missing manual choices produce an empty matrix (hard CI failure),
// never an implicit two-runner cache warmup.
const selectedCacheMatrix = "        profile: ${{ fromJSON(inputs.cache_profile == 'tests' && '[\"tests\"]' || inputs.cache_profile == 'release' && '[\"release\"]' || inputs.cache_profile == 'both' && '[\"tests\",\"release\"]' || '[]') }}";
// Windows Clippy runs on the existing host regression runner. It is not a
// separately accepted matrix result, but it remains a required -D warnings
// command inside the host cell.
const regressionSuites = ['engine', 'host', 'execution', 'research'];
const releaseGates = ['trading-engine-acceptance-windows',
  ...regressionSuites.map(suite => `trading-engine-regressions-windows (${suite})`),
  'trading-engine-build-windows', 'linux-core', 'windows-native'];
// GitHub REST currently returns the bare workflow path; some GitHub response
// forms append @ref. Both are canonical, but another workflow is never one.
const acceptedWorkflowPath = String.raw`test("^\\.github/workflows/ci\\.yml(@[A-Za-z0-9_][A-Za-z0-9_.-]*(/[A-Za-z0-9_][A-Za-z0-9_.-]*)*)?$")`;
const windowsIpcSummary = String.raw`test result: ok\. 12 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out;`;
const nativeCacheJobs = [
  ['trading_engine_acceptance_windows', 'v2-rust', 'trading-engine-tests-windows', 'false'],
  ['trading_engine_regressions_windows', 'v2-rust', 'trading-engine-tests-windows', 'false'],
  ['trading_engine_build_windows', 'v1-rust', 'trading-engine-release-windows', 'false'],
  ['trusted_main_cache_prime', warmPrefix, 'trading-engine-${{ matrix.profile }}-windows', mainCacheWriter],
];
const s2ExactTests = [
  'research_sandbox::native::tests::disposable_real_research_lpac_ready_job_and_bearer',
  'research_acl::native::tests::disposable_windows_acl_image_package_rx_and_inventory',
  'research_acl::native::tests::disposable_windows_acl_unsealed_catalog_is_rejected',
  'research_acl::native::tests::disposable_windows_acl_reparse_and_hardlink_rejected',
  'research_acl::native::tests::disposable_windows_acl_catalog_sqlite_sidecars_inherit_low_il',
  'research_acl::native::tests::disposable_windows_acl_finance_sqlite_locks_untouched',
];
const s2JobGate = `    if: >-
      github.event_name == 'pull_request' &&
      github.event.pull_request.draft == true &&
      github.event.pull_request.head.repo.full_name == github.repository &&
      contains(github.event.pull_request.labels.*.name, 's2-disposable-windows') &&
      (github.event.action == 'synchronize' ||
       (github.event.action == 'labeled' && github.event.label.name == 's2-disposable-windows'))`;

function validate(yaml, fileNames, runnerScript = disposableScript) {
  assert.ok(!fileNames.includes('publish-native-base.yml'), 'SECOND_PUBLICATION_WORKFLOW');
  assert.ok(!fileNames.includes('all26-slice-targeted.yml'), 'RETIRED_FINANCE_WORKFLOW_RESURRECTED');
  const parts = yaml.split(/^jobs:\s*$/m);
  assert.equal(parts.length, 2, 'MALFORMED_CI_JOBS');
  const [header, jobs] = parts;
  assert.match(header, /^name: CI$/m);
  assert.match(header, /^  push:$/m);
  assert.match(header, /^      - main$/m);
  assert.match(header, /^  pull_request:$/m);
  assert.match(header, /^    types: \[opened, reopened, synchronize, ready_for_review, labeled\]$/m,
    'EXPLICIT_LABELED_EVENT_REQUIRED_WITHOUT_CHANGING_READY_TRIGGERS');
  assert.doesNotMatch(header, /^  pull_request_target:$/m);
  assert.doesNotMatch(header, /^  workflow_run:$/m);
  assert.match(header, /^  workflow_dispatch:$/m);
  assert.match(header, /^      prime_windows_cargo_cache:$/m);
  assert.match(header, /^        default: true$/m);
  assert.match(header, /^      cache_profile:\n        description: 'Select which trusted main Windows Cargo cache to warm'\n        required: true\n        type: choice\n        options: \[tests, release, both\]\n        default: both$/m,
    'MANUAL_CACHE_MUST_EXPOSE_ONE_OR_BOTH_PROFILES_WITH_SAFE_BOTH_DEFAULT');
  assert.doesNotMatch(header, /^  contents: write$/m);
  assert.match(header, /cancel-in-progress: \$\{\{ github\.event_name == 'pull_request' \}\}/);

  const entries = [...jobs.matchAll(/^  ([a-zA-Z0-9_-]+):\s*$/gm)];
  const map = new Map(entries.map((m, i) => [m[1], jobs.slice(m.index,
    i + 1 < entries.length ? entries[i + 1].index : undefined)]));
  assert.equal(map.size, entries.length, 'DUPLICATE_JOB_ID');
  // Finance, independent regressions and release share a single workflow.
  // The only cache-only job primes exact accepted main, outside the PR gate.
  assert.ok(map.size <= 17, 'OBSOLETE_JOB_GRAPH_REGRESSION');
  for (const unused of ['required_classify', 'research_package_proof', 'draft-trading-migration-512',
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
  assert.match(classify, /python3 -B scripts\/check_ci_native_scope\.py --github-plan/);
  assert.match(classify, /plan_args\+=\(--allow-dependency-candidate\)/);
  assert.match(classify, /if: github.event_name == 'pull_request' && github.event.pull_request.draft != true && steps.file_scope.outputs.manifest_candidate != 'none'/);
  assert.match(classify, /git fetch --no-tags --depth=1 origin "\$BASE_SHA"/);
  assert.match(classify, /git worktree add --detach "\$base" "\$BASE_SHA"/);
  assert.equal((classify.match(/cargo metadata --locked --format-version 1 --filter-platform x86_64-pc-windows-msvc/g) || []).length, 2);
  assert.match(classify, /node scripts\/ci-cargo-closure-proof.cjs "\$temp\/base.json" "\$temp\/head.json"/);
  assert.match(classify, /if \[\[ -n '\$\{\{ steps.cargo_closure.outputs.scope \}\}' \]\]; then/);
  assert.match(classify, /scope="\$\{\{ steps.proven_scope.outputs.scope \}\}"/);
  assert.match(classify, /compare\/\$base\.\.\.\$CHECKED_HEAD/);
  assert.match(classify, /\.commits\[-1\]\.sha == \$head/);
  assert.ok(classify.includes(acceptedWorkflowPath), 'MAIN_REUSE_MUST_MATCH_EXACT_CI_WORKFLOW_PATH');
  assert.match(classify, /\.run_attempt == 1\) \| \[\.id, \.run_attempt\] \| @tsv/,
    'UNREVIEWED_RERUN_CANNOT_AUTHORIZE_MAIN_REUSE');
  assert.match(classify, /host-controller\|host-launcher\) edge_required="\$\{\{ steps\.file_scope\.outputs\.edge_changed \}\}"/);
  assert.match(classify, /research-host\) edge_required=true/,
    'MIXED_RESEARCH_CONTROL_WSS_MUST_RUN_EDGE_CONTRACTS');
  assert.doesNotMatch(classify, /pulls\/\$PR_NUMBER\/files|prepare-research-release\.ps1/);
  assert.match(classify, /"research_only=true" >> "\$GITHUB_OUTPUT"/);
  assert.match(classify, /native_proof=RESEARCH_OWNER_ONLY/);
  const draft = job('draft-component-tests');
  assert.match(draft, /^    name: draft-owner-feedback$/m);
  assert.match(draft, /needs: component_impact/);
  assert.match(draft, /DRAFT_CHECKS: \$\{\{ needs\.component_impact\.outputs\.draft_checks \}\}/);
  assert.match(draft, /check_p0\.py --checks "\$DRAFT_CHECKS"/);
  assert.match(draft,
    /if: github.event_name == 'pull_request' && github.event.pull_request.draft && github.event.action != 'labeled'/,
    'LABEL_ONLY_EVENT_MUST_NOT_RECOMPILE_UNCHANGED_DRAFT_OWNER_TESTS');
  assert.doesNotMatch(draft, /check_p0\.py --base|fetch-depth: 0/);
  // The sole additional Windows job is a same-repo Draft S2 experiment, not
  // a finance Ready alias, a release, or a runner for every normal Draft.
  const s2 = job('research-lpac-disposable-windows');
  assert.match(s2, /^    name: research-lpac-disposable-windows$/m);
  assert.match(s2, /^    needs: component_impact$/m);
  assert.ok(s2.includes(s2JobGate), 'S2_EXACT_DRAFT_LABEL_SAME_REPO_EVENT_GATE_REQUIRED');
  assert.match(s2, /^    runs-on: windows-latest$/m);
  assert.match(s2, /^    timeout-minutes: 45$/m);
  assert.match(s2, /^      OKX_DISPOSABLE_WINDOWS_RUNNER: '1'$/m);
  assert.match(s2, /^      OKX_S2_EXPECTED_HEAD: \$\{\{ github.event.pull_request.head.sha \}\}$/m);
  assert.match(s2, /ref: \$\{\{ github.event.pull_request.head.sha \}\}/);
  assert.match(s2, /persist-credentials: false/);
  assert.match(s2, /run: \.\/scripts\/ci-s2-disposable-windows\.ps1/);
  assert.match(s2, /if: always\(\)\n        uses: actions\/upload-artifact@[0-9a-f]{40}/);
  assert.match(s2, /name: okx-s2-disposable-windows-\$\{\{ github.event.pull_request.head.sha \}\}-\$\{\{ github.run_attempt \}\}/);
  assert.match(s2, /path: \$\{\{ runner.temp \}\}\/okx-s2-disposable-evidence-\$\{\{ github.run_id \}\}-\$\{\{ github.run_attempt \}\}\//);
  assert.match(s2, /if-no-files-found: error/);
  assert.match(s2, /save-if: false/);
  assert.doesNotMatch(s2, /gh release|actions\/attest|cargo build --locked --release|okx-windows-bundle-/);
  assert.match(runnerScript, /schema = 'okx\.s2\.disposable-windows\/v1'/);
  assert.match(runnerScript, /acceptance_status = 'NOT_PROVEN_UNTIL_INDEPENDENT_REVIEW'/);
  assert.match(runnerScript, /\$env:RUNNER_ENVIRONMENT -ne 'github-hosted'/);
  assert.match(runnerScript, /\$env:RUNNER_OS -ne 'Windows'/);
  assert.match(runnerScript, /\$env:OKX_DISPOSABLE_WINDOWS_RUNNER -cne '1'/);
  assert.match(runnerScript, /\$head -cne \$env:OKX_S2_EXPECTED_HEAD/);
  assert.match(runnerScript, /git rev-parse 'HEAD\^\{tree\}'/);
  assert.match(runnerScript, /Join-Path \$runnerTemp "okx-s2-disposable-\$suffix"/);
  assert.match(runnerScript, /\.okx-disposable-windows-runner/);
  assert.match(runnerScript, /okx-s2-disposable-v1`n/);
  assert.match(runnerScript, /\$env:OKX_DISPOSABLE_WINDOWS_ROOT = \$root/);
  assert.match(runnerScript,
    /cargo build --locked -p okx-research-service --bin okx-research-service --features s2-disposable-windows/,
    'S2_LOOPBACK_WORKER_MUST_REQUIRE_NON_SHIPPING_CARGO_FEATURE');
  assert.match(runnerScript, /research_worker_build_feature = 's2-disposable-windows'/,
    'S2_RECEIPT_MUST_DISCLOSE_NON_SHIPPING_WORKER_BUILD_FEATURE');
  assert.match(runnerScript, /\$env:OKX_DISPOSABLE_RESEARCH_EXE = /);
  assert.match(runnerScript, /cargo test --locked -p okx-host-controller --lib \$testName -- --ignored --exact --show-output --test-threads 1/,
    'S2_CAPTURED_SUCCESS_STDOUT_MUST_KEEP_NAMED_RESULT_LINE_ATOMIC');
  assert.match(runnerScript, /\$namedPass = \$log -cmatch/);
  assert.match(runnerScript, /test result: ok\\\. 1 passed; 0 failed; 0 ignored; 0 measured; \[1-9\]\[0-9\]\* filtered out;/);
  assert.match(runnerScript, /\$hasWitness = \$log\.Contains\(\$witness\)/);
  assert.match(runnerScript, /\$testPassed = \$testExit -eq 0 -and \$null -eq \$invokeError -and\n\s+\$namedPass -and \$onePassed -and \$hasWitness/);
  assert.match(runnerScript, /\$namedExecuted = \$log -cmatch.*\(\?:ok\|FAILED\)/);
  assert.match(runnerScript, /\$receipt.attempted_exact_test_count \+= 1/);
  assert.match(runnerScript, /\$receipt.executed_exact_test_count \+= 1/);
  assert.match(runnerScript, /\$receipt.passed_exact_test_count \+= 1/);
  assert.match(runnerScript, /\$failedTests.Add\(\$testName\)/);
  assert.match(runnerScript, /\$receipt.failed_exact_test_names = \$failedTests.ToArray\(\)/);
  assert.match(runnerScript,
    /\$receipt.attempted_exact_test_count -ne \$tests.Count -or\n\s+\$receipt.executed_exact_test_count -ne \$tests.Count -or\n\s+\$receipt.passed_exact_test_count -ne \$tests.Count -or\n\s+\$failedTests.Count -ne 0 -or \$tests.Count -ne 6/,
    'S2_ALL_SIX_MUST_BE_ATTEMPTED_EXECUTED_AND_PASSED_WITH_NO_FAILING_WITNESSES');
  const probeLoopStart = runnerScript.indexOf('    foreach ($testName in $tests) {');
  const probeLoopEnd = runnerScript.indexOf('    $receipt.failed_exact_test_names =', probeLoopStart);
  assert.ok(probeLoopStart > 0 && probeLoopEnd > probeLoopStart, 'S2_PROBE_LOOP_REQUIRED');
  const probeLoop = runnerScript.slice(probeLoopStart, probeLoopEnd);
  assert.doesNotMatch(probeLoop, /\bthrow\b|\bbreak\b|\bcontinue\b/,
    'S2_ONE_FAILED_PROBE_MUST_NOT_ABORT_THE_REMAINING_FIVE');
  assert.match(probeLoop, /\$receipt.tests \+= \[ordered\]@\{/,
    'S2_EVERY_PROBE_MUST_HAVE_ITS_OWN_RECEIPT');
  assert.match(probeLoop, /\$testExit = -1[\s\S]*try \{\n\s+& cargo test[\s\S]*\} catch \{/,
    'S2_NATIVE_CARGO_FAILURE_MUST_NOT_ABORT_ALL_PROBES');
  assert.match(runnerScript, /Get-FileHash -Algorithm SHA256 -LiteralPath \$testLog/);
  assert.match(runnerScript, /\$receipt \| ConvertTo-Json -Depth 8 \| Set-Content/);
  assert.doesNotMatch(runnerScript, /gh release|cargo build --locked --release|C:\\okx|gh api --method POST/);
  const quotedDeclared = [...runnerScript.matchAll(/^    '(research_(?:sandbox|acl)::[^']+)'[,]?$/gm)]
    .map(match => match[1]);
  assert.deepEqual(quotedDeclared, s2ExactTests, 'EXACT_SIX_IN_CRATE_IGNORED_WINDOWS_TEST_NAMES');
  assert.equal(new Set(quotedDeclared).size, 6, 'S2_TEST_NAMES_NOT_UNIQUE');
  assert.match(job('dependency-audit'), /if: \(github.event_name != 'pull_request' \|\| github.event.pull_request.draft != true\)/);
  const research = job('research_service_tests_windows');
  assert.match(research, /if: github.event_name == 'pull_request' && needs.component_impact.outputs.build_required == 'true'/,
    'MAIN_MUST_NOT_BUILD_UNREVIEWED_RESEARCH_ORIGIN');
  assert.match(research, /research_component_sha256: \$\{\{ steps\.package\.outputs\.research_component_sha256 \}\}/);
  assert.match(research, /RESEARCH_READY_STATUS_POLICY=okx\.github\.actions\.research-ready\/v1/,
    'ACTIVE_RESEARCH_READY_POLICY_MUST_RETAIN_ACCEPTED_V1_NAMES');
  assert.doesNotMatch(research, /RESEARCH_READY_STATUS_POLICY=okx\.github\.actions\.research-ready\/v2/,
    'UNACCEPTED_RESEARCH_V2_STATUS_POLICY_CANNOT_REPLACE_V1');
  assert.doesNotMatch(research, /prepare-research-release\.ps1 -Dist/,
    'RESEARCH_ONLY_MUST_NOT_DOWNLOAD_NATIVE_BASE_OR_COPY_ENGINE_BYTES');
  assert.match(research, /Build self-contained Research-only Windows component/);
  assert.match(research, /schema = 'okx\.windows\.research-component\/v1'/);
  assert.match(research, /cargo_lock_sha256 = \(Get-FileHash/);
  assert.match(research, /node scripts\/research-component-artifact\.cjs \$dist \$sourceHead \$sourceTree/);
  assert.match(research, /RESEARCH_COMPONENT_EXACT_THREE_FILE_PROOF_FAILED/);
  assert.match(research, /name: okx-research-component-\$\{\{ github\.event\.pull_request\.head\.sha \}\}/);
  assert.doesNotMatch(research, /cargo build --locked --release -p okx-trading-engine|actions\/attest/);
  assert.match(research, /edge_runtime::standalone_mcp_tests::standalone_catalog_reopens_through_real_mcp_do_websocket -- --exact --ignored --nocapture --test-threads 1/);
  assert.match(research, /RESEARCH_ONLY_REAL_MCP_DO_WSS_WITNESS_NOT_EXECUTED/);
  // Research-only changes must not resurrect the four real-child IPC timeouts
  // previously observed when the Windows processes started in parallel. The
  // dedicated harness retains the original 10s receive deadline. One test is
  // One #[cfg(windows)] and one #[cfg(not(windows))] test are mutually
  // exclusive; two new cross-platform negatives require twelve on Windows.
  assert.match(research, /cargo test --locked -p okx-research-service --test ipc_process -- --nocapture --test-threads 1/);
  assert.ok(research.includes(windowsIpcSummary), 'RESEARCH_WINDOWS_IPC_EXACT_TWELVE_ZERO_IGNORED_REQUIRED');
  assert.match(research, /RESEARCH_IPC_ALL_TWELVE_TESTS_REQUIRED/);
  assert.match(research, /RESEARCH_PRIVILEGED_CLI_DENIAL_NOT_RUN_\$witness/);
  for (const witness of [
    'research_worker_has_no_privileged_credential_store_api_or_dependency',
    'privileged_edge_token_cli_is_disabled_before_stdin_or_catalog_on_all_platforms',
  ]) assert.match(research, new RegExp(witness));
  assert.doesNotMatch(research, /RESEARCH_IPC_ALL_(?:TEN|NINE)_TESTS_REQUIRED|test result: ok\\\. (?:10|9) passed/);
  assert.doesNotMatch(research, /cargo test --locked -p okx-strategy-core -p okx-research -p okx-research-service -p okx-analysis\s/,
    'SCOPED_RESEARCH_MUST_NOT_RUN_UNBOUNDED_WORKER_INTEGRATIONS');
  assert.doesNotMatch(classify, /native_required=false[^\n]*CARGO/);
  assert.match(researchRelease, /git worktree add --detach \$baselineDir \$mergeSha/);
  assert.match(researchRelease, /cargo metadata --locked --format-version 1 --filter-platform x86_64-pc-windows-msvc/);
  assert.match(researchRelease, /node scripts\/research-release-scope.cjs @scopeArgs/);
  assert.match(researchRelease, /\$candidate\.tag_name\.Substring\('native-base-'\.Length\)/);
  assert.match(researchRelease, /compare\/\$candidateSha\.\.\.\$baseSha/);
  assert.match(researchRelease, /\$relation\.status -cin @\('ahead', 'identical'\)/);
  assert.match(researchRelease, /Sort-Object distance/);
  assert.match(researchRelease, /AMBIGUOUS_NEAREST_NATIVE_BASE/);
  assert.match(researchRelease, /\$scopeArgs \+= '--engine-only'/);
  assert.doesNotMatch(researchRelease, /gh release delete|--cleanup-tag/);
  const host = job('host_controller_launcher_checks');
  assert.match(host, /cargo clippy --locked -p okx-host-protocol -p okx-host-launcher -p okx-host-controller -p okx-bridge-mcp/);
  assert.match(host, /cargo test --locked -p okx-host-protocol -p okx-host-launcher -p okx-host-controller -p okx-bridge-mcp/);
  assert.doesNotMatch(host, /cargo build --locked --release -p okx-trading-engine/);
  for (const [id, display] of [['required_linux', 'linux-core'],
    ['required_windows', 'windows-native']]) {
    assert.match(job(id), new RegExp(`^    name: ${display}$`, 'm'));
    assert.match(job(id), /needs: \[component_impact,/);
  }
  // Cargo advisories/license/source policy must participate in the protected
  // PR merge decision, not merely in post-merge release publication. GitHub
  // treats conditional skipped jobs as successful required checks; the always
  // running aggregate must explicitly require a real audit on Rust scopes.
  const requiredLinux = job('required_linux');
  assert.match(requiredLinux, /^    needs: \[component_impact, dependency-audit, /m,
    'LINUX_AGGREGATE_MUST_WAIT_FOR_DEPENDENCY_AUDIT');
  assert.match(requiredLinux,
    /case "\$\{\{ needs\.component_impact\.outputs\.component_scope \}\}" in\n\s+docs\|cloudflare\) test "\$\{\{ needs\['dependency-audit'\]\.result \}\}" = "skipped" ;;\n\s+\*\) test "\$\{\{ needs\['dependency-audit'\]\.result \}\}" = "success" ;;\n\s+esac/,
    'RUST_READY_MUST_NOT_ACCEPT_SKIPPED_OR_FAILED_DEPENDENCY_AUDIT');
  assert.match(requiredLinux, /if: always\(\) && \(github\.event_name != 'pull_request' \|\| github\.event\.pull_request\.draft != true\)/);
  const auditGate = requiredLinux.indexOf('docs|cloudflare) test "${{ needs[\'dependency-audit\'].result }}" = "skipped"');
  const mainReuseExit = requiredLinux.indexOf('REUSED_SUCCESSFUL_PR_CI=true');
  assert.ok(auditGate >= 0 && mainReuseExit > auditGate,
    'MAIN_EXACT_TREE_REUSE_MUST_NOT_SKIP_FRESH_DEPENDENCY_AUDIT');
  const names = {
    rust_tests_linux: 'rust-tests-linux',
    trading_engine_acceptance_windows: 'trading-engine-acceptance-windows',
    trading_engine_build_windows: 'trading-engine-build-windows',
    research_service_tests_windows: 'research-service-tests-windows',
    host_controller_launcher_checks: 'host-controller-launcher-checks',
  };
  for (const [id, display] of Object.entries(names)) {
    assert.match(job(id), new RegExp(`^    name: ${display}$`, 'm'),
      `AMBIGUOUS_CI_JOB_NAME:${id}`);
  }
  // Edge transport has one required owner. Running the same npm/Miniflare
  // checks again inside portable Rust CI doubles setup without new evidence.
  const linux = job('rust_tests_linux');
  const edge = job('cloudflare_mcp_contracts');
  const architecture = job('architecture_contracts');
  assert.match(linux, /cargo fmt --all -- --check/);
  assert.match(linux, /cargo clippy --locked --workspace --exclude okx-trading-engine --exclude okx-host-controller --all-targets -- -D warnings/);
  assert.match(linux, /cargo test --locked --workspace --exclude okx-trading-engine --exclude okx-host-controller --exclude okx-research-service/);
  assert.doesNotMatch(linux, /sqlite_million_history_bounded_rss|two_account_100_parallel_sql_readonly_isolation/,
    'RETIRED_OR_ZERO_TEST_LINUX_STEP');
  assert.doesNotMatch(linux, /name: Native policy and original financial invariants|native-policy\.log/,
    'DUPLICATED_PORTABLE_FINANCE_STEP');
  assert.match(architecture, /node scripts\/run-trade-integration\.mjs --list/);
  assert.match(architecture, /node scripts\/run-trade-integration\.mjs --self-test/);
  assert.match(architecture, /node scripts\/run-trade-integration\.mjs --case T09\.0 --artifacts/);
  assert.match(architecture, /RETIRED_REMOTE_FINANCE_HARNESS_EXECUTED/);
  assert.doesNotMatch(linux, /npm ci|npm run check|test-native-finance-retirement-do\.mjs|actions\/setup-node@/);
  assert.match(edge, /npm ci --ignore-scripts --no-audit --no-fund/);
  assert.match(edge, /needs\.component_impact\.outputs\.edge_required == 'true'/);
  assert.match(edge, /npm run check/);
  assert.match(edge, /python3 \.\.\/\.\.\/scripts\/check_product_contract\.py/,
    'EDGE_ONLY_CHANGES_MUST_KEEP_PUBLIC_PRODUCT_CONTRACT');
  assert.match(edge, /node scripts\/test-native-finance-retirement-do\.mjs/);
  assert.match(linux, /cargo build --locked -p okx-research-service --bin okx-research-service/);
  assert.match(linux, /cargo test --locked -p okx-research-service --test ipc_process -- --nocapture/);
  assert.doesNotMatch(linux, /research_dispatch_tests::|test-isolated-singleton|okx-research-service-test-isolated/,
    'LINUX_MUST_NOT_RESURRECT_ENGINE_TO_RESEARCH_DEV_TEST_COUPLING');
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
  assert.match(engine, /cargo build --locked -p okx-research-service --bin okx-research-service --features test-isolated-singleton/);
  assert.match(engine, /Resolve-Path 'target\/debug\/okx-research-service-test-isolated\.exe'/);
  assert.match(engine, /cargo build --locked -p okx-research-service --bin okx-research-load-fixture --features test-fixture/);
  assert.match(engine, /RESEARCH_TEST_LOAD_FIXTURE_BINARY_MISSING/);
  const fixtureEnvironment = /OKX_RESEARCH_LOAD_FIXTURE_EXE = \(Resolve-Path 'target\/debug\/okx-research-load-fixture\.exe'\)\.Path/;
  for (const [start, end] of [
    ['Engine autonomy under heavy research process tree', 'Upload Engine research process-tree observation'],
    ['MCP DO WSS Native finance under isolated Research load', 'Upload isolated MCP DO WSS Native load observation'],
  ]) {
    const first = engine.indexOf(`      - name: ${start}`);
    const last = engine.indexOf(`      - name: ${end}`);
    assert.ok(first >= 0 && last > first, `ISOLATED_ENGINE_T10_STEP_MISSING:${start}`);
    assert.match(engine.slice(first, last), fixtureEnvironment,
      `ENGINE_T10_STEP_REQUIRES_OWN_LOAD_FIXTURE:${start}`);
  }
  assert.doesNotMatch(engine, /K9 worker two-heavy cancellation latency evidence/);
  const regression = job('trading_engine_regressions_windows');
  assert.match(regression, /needs: component_impact/);
  assert.match(regression, /if: needs\.component_impact\.outputs\.build_required == 'true' && needs\.component_impact\.outputs\.native_required != 'false' && needs\.component_impact\.outputs\.research_only != 'true'/);
  assert.ok(regression.includes('    name: trading-engine-regressions-windows (${{ matrix.suite }})'));
  assert.ok(regression.includes(`      fail-fast: false\n      matrix:\n        suite: [${regressionSuites.join(', ')}]`));
  for (const [name, scope] of [
    ['Clippy native Windows crates', "matrix.suite == 'host'"],
    ['Strategy Core and Research owner regressions on full Windows', "matrix.suite == 'research'"],
    ['Build research worker for regression process contracts', "matrix.suite == 'engine'"],
    ['Remaining native regression tests for this suite', "matrix.suite == 'engine' || matrix.suite == 'host' || matrix.suite == 'execution'"],
    ['Serial native account catalog and writer-termination regressions', "matrix.suite == 'engine'"],
    ['Serial terminal release source-frontier regression', "matrix.suite == 'engine'"],
    ['Engine rejects Research owner attempts to mutate Native finance', "matrix.suite == 'engine'"],
    ['Worker library regression and actual IPC child', "matrix.suite == 'research'"],
    ['K9 worker two-heavy cancellation latency evidence', "matrix.suite == 'research'"],
    ['Upload K9 native latency evidence', "matrix.suite == 'research'"],
  ]) assert.ok(regression.includes(`      - name: ${name}\n        if: ${scope}\n`), `NATIVE_REGRESSION_SUITE:${name}`);
  assert.match(regression,
    /cargo clippy --locked -p okx-trading-engine -p okx-host-controller -p okx-host-launcher -p okx-trade-execution --all-targets -- -D warnings/,
    'WINDOWS_CLIPPY_MUST_RETAIN_ALL_NATIVE_OWNERS_AND_DENY_WARNINGS');
  assert.doesNotMatch(regression, /matrix\.suite == 'clippy'|suite: \[[^\]\n]*clippy/,
    'RETIRED_CLIPPY_MATRIX_MUST_NOT_RETURN');
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
    'sandbox_ready_attests_only_disposable_canaries_without_secret_material',
    'research_bearer_requires_bounded_one_shot_supervisor_envelope_and_eof',
    'installed_research_bearer_is_pinned_to_control_edge_origin',
  ]) assert.match(regression, new RegExp(witness));
  assert.match(regression, /cargo test --locked -p okx-research-service --test ipc_process -- --nocapture --test-threads 1/);
  assert.ok(regression.includes(windowsIpcSummary), 'FULL_WINDOWS_RESEARCH_IPC_EXACT_TWELVE_ZERO_IGNORED_REQUIRED');
  assert.match(regression, /WORKER_IPC_ALL_TWELVE_TESTS_REQUIRED/);
  assert.match(regression, /WINDOWS_RESEARCH_PRIVILEGED_CLI_DENIAL_NOT_RUN_\$witness/);
  for (const witness of [
    'research_worker_has_no_privileged_credential_store_api_or_dependency',
    'privileged_edge_token_cli_is_disabled_before_stdin_or_catalog_on_all_platforms',
  ]) assert.match(regression, new RegExp(witness));
  assert.match(regression, /cargo build --locked -p okx-research-service --bin okx-research-service --features test-isolated-singleton/);
  assert.match(regression, /Resolve-Path 'target\/debug\/okx-research-service-test-isolated\.exe'/);
  assert.match(regression, /cargo build --locked -p okx-research-service --bin okx-research-load-fixture --features test-fixture/);
  assert.match(regression, /RESEARCH_TEST_LOAD_FIXTURE_BINARY_MISSING/);
  assert.match(regression, /OKX_RESEARCH_LOAD_FIXTURE_EXE = \(Resolve-Path 'target\/debug\/okx-research-load-fixture\.exe'\)\.Path/);
  assert.doesNotMatch(regression, /research_dispatch_tests::|Agent dispatch to actual research child/,
    'WINDOWS_MUST_NOT_RESTART_LEGACY_ENGINE_RESEARCH_PROTOCOL_DISPATCH_TESTS');
  const financeDenialName = 'Engine rejects Research owner attempts to mutate Native finance';
  const financeDenialStart = regression.indexOf(`      - name: ${financeDenialName}\n`);
  assert.ok(financeDenialStart >= 0, 'ENGINE_RESEARCH_FINANCE_DENIAL_STEP_REQUIRED');
  const financeDenialNext = regression.indexOf('\n      - name:', financeDenialStart + 1);
  const financeDenial = regression.slice(financeDenialStart,
    financeDenialNext < 0 ? regression.length : financeDenialNext);
  assert.ok(financeDenial.includes('cargo test --locked -p okx-trading-engine --lib query::public_dispatch_tests::research_owner_handoff_cannot_change_native_finance_files_or_enqueue_writes -- --exact --test-threads 1'),
    'ENGINE_MUST_RUN_EXACT_RESEARCH_FINANCE_DENIAL_TEST');
  assert.ok(financeDenial.includes('if ($code -ne 0) { exit $code }'),
    'ENGINE_FINANCE_DENIAL_MUST_FAIL_ON_PROCESS_ERROR');
  assert.ok(financeDenial.includes("$testName = 'query::public_dispatch_tests::research_owner_handoff_cannot_change_native_finance_files_or_enqueue_writes'"),
    'ENGINE_FINANCE_DENIAL_MUST_NAME_THE_ACTUAL_EXECUTED_TEST');
  assert.ok(financeDenial.includes("$log -notmatch ('(?m)^test ' + [regex]::Escape($testName) + ' \\.\\.\\. ok$') -or"),
    'ENGINE_FINANCE_DENIAL_MUST_PROVE_THE_EXACT_NAMED_TEST_PASSED');
  assert.ok(financeDenial.includes("$log -notmatch 'test result: ok\\. 1 passed; 0 failed; 0 ignored; 0 measured; [1-9][0-9]* filtered out;'"),
    'ENGINE_FINANCE_DENIAL_MUST_REJECT_ZERO_PASSED_BUT_ALLOW_NONZERO_FILTERED');
  assert.ok(financeDenial.includes("throw 'ENGINE_RESEARCH_OWNER_FINANCE_DENIAL_EXACT_TEST_REQUIRED'"),
    'ENGINE_FINANCE_DENIAL_MUST_FAIL_CLOSED_ON_MISSING_TEST');
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
  assert.doesNotMatch(build, /--features test-isolated-singleton/,
    'SHIPPING_ENGINE_BUNDLE_MUST_NOT_USE_TEST_SINGLETON');
  assert.doesNotMatch(build, /--features test-fixture|okx-research-load-fixture/,
    'SHIPPING_WINDOWS_BUNDLE_MUST_NOT_PACKAGE_RESEARCH_LOAD_FIXTURE');
  assert.match(build, /PACKAGED_ENGINE_RUNTIME_CONTRACT_INVALID/);
  assert.match(build, /name: okx-windows-bundle-\$\{\{ github.event.pull_request.head.sha \}\}/);
  assert.match(build, /if: github.event_name == 'pull_request' && needs.component_impact.outputs.component_scope != 'engine'\n        uses: actions\/upload-artifact/);
  assert.match(build, /Build only Engine against verified unchanged Windows owners/);
  assert.match(build, /prepare-research-release\.ps1 -VerifyOnly -EngineOnly/);
  assert.match(build, /cargo build --locked --release -p okx-trading-engine/);
  assert.match(build, /okx.windows.engine-component\/v1/);
  assert.match(build, /reused_verified_components = \$reused/);
  assert.match(build, /release_is_complete = \$false/);
  assert.match(build, /ENGINE_COMPONENT_RUNTIME_CONTRACT_INVALID/);
  assert.match(build, /MAIN_NATIVE_REQUIRES_EXACT_ACCEPTED_READY_ARTIFACT/);
  assert.match(build, /name: okx-engine-component-\$\{\{ github.event.pull_request.head.sha \}\}/);
  assert.match(build, /source_head_sha = \$sourceHead/);
  assert.doesNotMatch(build, /gh release|cargo run|cargo test --locked .* -- --nocapture/);
  const prime = job('trusted_main_cache_prime');
  assert.ok(prime.includes("name: warm-windows-cargo-cache (${{ matrix.profile }})"));
  assert.ok(prime.includes(selectedCacheMatrix), 'CACHE_MATRIX_MUST_CREATE_ONLY_REQUESTED_TRUSTED_MAIN_RUNNERS');
  assert.doesNotMatch(prime, /^        profile: \[tests, release\]$/m);
  assert.match(prime, /github.event_name == 'workflow_dispatch' &&\n        inputs.prime_windows_cargo_cache == true &&/);
  assert.doesNotMatch(prime, /github.event_name == 'push'/,
    'ACCEPTED_MAIN_MUST_NOT_RECOMPILE_ENGINE_TO_WARM_WINDOWS_CACHE');
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
  assert.match(windows, /research_component_sha256.*\^\[0-9a-f\]\{64\}\$/);
  assert.match(windows, /WINDOWS_NATIVE_PROOF=RESEARCH_COMPONENT_VERIFIED_/);
  assert.doesNotMatch(windows, /REUSED_ACCEPTED_BASE_\$\{\{ needs\.research_service_tests_windows/);
  assert.match(windows, /test "\$\{\{ needs\.trading_engine_regressions_windows\.result \}\}" = "success"/);
  assert.match(windows, /native_proof \}\}" = "ENGINE_OWNER_ONLY"/);
  assert.match(windows, /WINDOWS_NATIVE_PROOF=FRESH_ENGINE_AND_FULL_FINANCIAL_SUCCESS/);
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
  assert.match(publish, /needs\.component_impact\.outputs\.build_required == 'false' &&/);
  assert.match(publish, /needs\.component_impact\.outputs\.reason == 'merged tree exactly matches successful PR head tree'/);
  for (const name of ["['dependency-audit']", '.component_impact', '.required_linux', '.required_windows']) {
    assert.ok(publish.includes(`needs${name}.result == 'success'`),
      `PUBLICATION_NEEDS_SUCCESS:${name}`);
  }
  assert.match(publish, /^    permissions:\n      contents: write$/m);
  assert.match(publish, /group: native-base-publication/);
  assert.match(publish, /ACCEPTED_MAIN: \$\{\{ github\.sha \}\}/);
  assert.doesNotMatch(publish, /github\.event\.workflow_run/);
  assert.doesNotMatch(publish, /gh release delete|--cleanup-tag/, 'ACCEPTED_ROLLBACK_BASES_MUST_NOT_BE_PRUNED_BLINDLY');
  assert.match(publish, /NATIVE_BASE_RETENTION=KEEP_ACCEPTED_ROLLBACK_ORIGINS/);
  assert.ok(publish.includes(acceptedWorkflowPath), 'NATIVE_PUBLISH_MUST_MATCH_EXACT_CI_WORKFLOW_PATH');
  assert.match(publish, /\.head_sha == \$head and \.run_attempt == 1 and/);
  assert.match(publish, /"\$attempt" == 1/);
  assert.match(publish, /runs\/\$candidate\/attempts\/\$attempt\/jobs\?per_page=100/);
  assert.match(publish, /all\(\.jobs\[\]; \.run_id == \$run and \.head_sha == \$head and \.run_attempt == 1\)/);
  assert.match(publish, /\.total_count == \(\.jobs \| length\)/);
  assert.match(publish, /\.workflow_run\.id == \$run and \.workflow_run\.repository_id == 1388071566/);
  assert.match(publish, /\.workflow_run\.head_repository_id == 1388071566/);
  assert.match(publish, /\.workflow_run\.head_sha == \$head/);
  assert.match(publish, /NO_EXACT_FIRST_ATTEMPT_FULL_NATIVE_READY_PR_ARTIFACT/);
  for (const name of [...releaseGates, 'classify', 'dependency-audit',
    'architecture-contracts', 'cloudflare-mcp-contracts', 'rust-tests-linux'])
    assert.ok(publish.includes(`"${name}"`), `NATIVE_PUBLICATION_MISSING_READY_GATE:${name}`);

  const researchPublish = job('publish_accepted_research_component');
  assert.match(researchPublish, /^    name: publish-accepted-research-component$/m);
  assert.match(researchPublish, /group: research-component-publication/);
  assert.match(researchPublish, /^      contents: write$/m);
  assert.match(researchPublish, /github.event_name == 'push' &&/);
  assert.match(researchPublish, /needs.component_impact.outputs.component_scope == 'research' &&/);
  assert.match(researchPublish, /needs.component_impact.outputs.build_required == 'false' &&/);
  assert.match(researchPublish, /needs.component_impact.outputs.reason == 'merged tree exactly matches successful PR head tree'/);
  assert.match(researchPublish, /needs.required_linux.result == 'success'/);
  assert.match(researchPublish, /needs.required_windows.result == 'success'/);
  assert.match(researchPublish, /\.merge_commit_sha == \$main and \.base.ref == "main"/);
  assert.match(researchPublish, /test "\$main_tree" = "\$head_tree"/);
  for (const name of ['classify', 'dependency-audit', 'architecture-contracts',
    'research-service-tests-windows', 'cloudflare-mcp-contracts', 'linux-core', 'windows-native'])
    assert.ok(researchPublish.includes(`"${name}"`), `RESEARCH_PUBLICATION_MISSING_READY_GATE:${name}`);
  assert.match(researchPublish, /runs\/\$candidate\/attempts\/\$attempt\/jobs\?per_page=100/);
  assert.match(researchPublish, /"\$attempt" == 1/);
  assert.match(researchPublish, /\.head_sha == \$head and \.run_attempt == 1 and/);
  assert.match(researchPublish, /all\(\.jobs\[\]; \.run_id == \$run and \.head_sha == \$head and \.run_attempt == 1\)/,
    'RESEARCH_READY_JOBS_MUST_ALL_BE_FROM_FIRST_ATTEMPT');
  assert.ok(researchPublish.includes(acceptedWorkflowPath), 'RESEARCH_PUBLISH_MUST_MATCH_EXACT_CI_WORKFLOW_PATH');
  assert.match(researchPublish, /\.head_repository\.id == 1388071566/);
  assert.match(researchPublish, /\.workflow_run\.id == \$run and \.workflow_run\.repository_id == 1388071566/);
  assert.match(researchPublish, /\.workflow_run\.head_repository_id == 1388071566/);
  assert.match(researchPublish, /\.workflow_run\.head_sha == \$head/);
  assert.match(research, /RESEARCH_COMPONENT_NOT_EXACT_PR_HEAD/);
  assert.match(research, /RESEARCH_COMPONENT_PR_HEAD_CHANGED_BEFORE_BUILD/);
  assert.match(researchPublish, /okx-research-component-\$head/);
  assert.match(researchPublish, /test "sha256:\$digest" = '\$\{\{ steps.select.outputs.digest \}\}'/);
  assert.match(researchPublish, /verify_research_component_release\.py "\$zip" "\$digest"/);
  assert.match(researchPublish, /tag="research-component-\$ACCEPTED_MAIN"/);
  assert.match(researchPublish, /https:\/\/api\.github\.com\/repos\/\$GITHUB_REPOSITORY\/git\/ref\/tags\/\$tag/);
  assert.match(researchPublish, /object="\$\(gh api "\/repos\/\$GITHUB_REPOSITORY\/git\/tags\/\$sha"\)"/,
    'ANNOTATED_TAGS_MUST_BE_PEELED_TO_COMMIT');
  assert.match(researchPublish, /test "\$sha" = "\$ACCEPTED_MAIN"/);
  assert.match(researchPublish, /verify_tag_target optional/);
  assert.match(researchPublish, /verify_tag_target required/);
  assert.match(researchPublish, /RESEARCH_TAG_REF_WRONG_COMMIT/);
  assert.match(researchPublish, /RESEARCH_PUBLISHED_TAG_REF_MISSING/);
  assert.match(researchPublish, /gh api --paginate "\/repos\/\$GITHUB_REPOSITORY\/releases\?per_page=100" \|/);
  assert.match(researchPublish, /jq -sc --arg tag "\$tag"/,
    'JQ_SLURP_MUST_SUPPORT_OLDER_GH_API_CLIENTS');
  assert.match(researchPublish, /if \(\$matches \| length\) > 1 then error\("RESEARCH_RELEASE_TAG_COLLISION"\)/,
    'DUPLICATE_DRAFT_TAG_MUST_FAIL_CLOSED');
  assert.match(researchPublish, /if ! gh api --method POST "\/repos\/\$GITHUB_REPOSITORY\/releases"/);
  assert.match(researchPublish, /-F draft=true -F prerelease=false/,
    'RESEARCH_RELEASE_ASSETS_MUST_BE_VERIFIED_BEFORE_PUBLISHING');
  assert.match(researchPublish, /RESEARCH_DRAFT_CREATE_AMBIGUOUS_VERIFY_ORIGIN/);
  assert.match(researchPublish, /RESEARCH_DRAFT_ORIGIN_NOT_FOUND/);
  assert.match(researchPublish, /\[\[ "\$release_id" =~ \^\[1-9\]\[0-9\]\*\$ \]\]/);
  assert.match(researchPublish, /gh api "\/repos\/\$GITHUB_REPOSITORY\/releases\/\$release_id"/,
    'UNPUBLISHED_DRAFT_MUST_BE_READ_BY_RELEASE_ID');
  assert.match(researchPublish, /\.id == \$id and \.tag_name == \$tag and \.target_commitish == \$main/);
  assert.match(researchPublish, /if jq -e '\.draft == true' <<<"\$release"/);
  assert.equal((researchPublish.match(/-H "Authorization: Bearer \$GH_TOKEN"/g) || []).length, 2,
    'GITHUB_TOKEN_REQUIRED_FOR_TAG_REF_AND_ASSET_UPLOAD');
  assert.match(researchPublish, /--data-binary "@\$RUNNER_TEMP\/\$name"/);
  assert.match(researchPublish, /https:\/\/uploads\.github\.com\/repos\/\$GITHUB_REPOSITORY\/releases\/\$release_id\/assets\?name=\$name/,
    'UNPUBLISHED_DRAFT_ASSETS_MUST_UPLOAD_BY_RELEASE_ID');
  assert.match(researchPublish, /test "\$present" = "\$expected"/,
    'RESEARCH_DUPLICATE_UPLOAD_MUST_PROVE_EXACT_EXISTING_SHA');
  assert.match(researchPublish, /RESEARCH_ASSET_UPLOAD_AMBIGUOUS_VERIFY_SHA:/);
  assert.match(researchPublish, /if ! gh api --method PATCH "\/repos\/\$GITHUB_REPOSITORY\/releases\/\$release_id"/);
  assert.match(researchPublish, /-F draft=false > "\$RUNNER_TEMP\/research-release-publish\.json"/);
  assert.match(researchPublish, /RESEARCH_DRAFT_PUBLISH_AMBIGUOUS_VERIFY_RELEASE/);
  assert.match(researchPublish, /\(\[\.assets\[\]\.name\] \| sort\) ==/,
    'RESEARCH_DRAFT_MUST_CONTAIN_EXACT_THREE_ASSETS');
  const shaReadback = researchPublish.indexOf('test "$present" = "$expected"');
  const completeDraft = researchPublish.indexOf('jq -e \'([.assets[].name] | sort) ==');
  const publishDraft = researchPublish.indexOf('gh api --method PATCH "/repos/$GITHUB_REPOSITORY/releases/$release_id"');
  assert.ok(shaReadback > 0 && completeDraft > shaReadback && publishDraft > completeDraft,
    'RESEARCH_ASSET_SHA_AND_EXACT_SET_REQUIRED_BEFORE_PUBLICATION');
  assert.match(researchPublish, /\.tag_name == \$tag and \.target_commitish == \$main and \.prerelease == false and/);
  assert.match(researchPublish, /\(\[\.assets\[\]\.name\] \| unique \| length\)/);
  for (const [proof, count] of [['all(.assets[]; .state == "uploaded")', 2],
    ['.assets[] | select(.name == $n) | .size', 3],
    ['asset_id="$(jq -r --arg n "$name"', 3],
    ['[[ "$asset_id" =~ ^[1-9][0-9]*$ ]]', 3]])
    assert.equal(researchPublish.split(proof).length - 1, count,
      `RESEARCH_ASSET_${proof}_REQUIRED_PRE_AND_POST_PUBLISH`);
  assert.match(researchPublish, /published="\$\(gh api "\/repos\/\$GITHUB_REPOSITORY\/releases\/tags\/\$tag" 2>\/dev\/null\)"/);
  assert.match(researchPublish, /RESEARCH_PUBLISHED_TAG_READBACK_NOT_VISIBLE/);
  assert.match(researchPublish, /test "\$\(jq -r \.id <<<"\$published"\)" = "\$release_id"/,
    'PUBLISHED_TAG_MUST_RESOLVE_SAME_RELEASE_ID');
  assert.ok(researchPublish.indexOf('published="$(gh api "/repos/$GITHUB_REPOSITORY/releases/tags/$tag" 2>/dev/null)"') > publishDraft,
    'DRAFT_MUST_NEVER_BE_READ_BY_PUBLISHED_TAG_ENDPOINT');
  assert.doesNotMatch(researchPublish, /^\s*(?:if ! )?gh release (?:upload|edit|create|view)/m,
    'DRAFT_GH_TAG_COMMANDS_ARE_UNRELIABLE');
  assert.doesNotMatch(researchPublish, /^\s+(?:if ! )?gh release upload[^\n]*(?:--clobber|--force)/m,
    'RESEARCH_EXISTING_ASSET_MUST_NEVER_BE_DELETED_OR_REPLACED');
  assert.match(researchPublish, /RESEARCH_COMPONENT_RELEASE=VERIFIED_APPEND_ONLY/);
  assert.match(researchPublish, /RESEARCH_ROLLBACK_RELEASE_RETENTION=KEEP_ALL_ACCEPTED/);
  assert.doesNotMatch(researchPublish, /cargo build|native-base-|gh release delete|--cleanup-tag|actions\/attest/);

  for (const [id, block] of map) {
    if (!['publish_accepted_windows_release', 'publish_accepted_research_component'].includes(id)) {
      assert.doesNotMatch(block, /^      contents: write$/m, `UNEXPECTED_WRITE_SCOPE:${id}`);
    }
  }
}

validate(source, workflows);
const ipcDeclaredTests = (ipcProcess.match(/^#\[tokio::test\]$/gm) || []).length;
const ipcPlainTests = (ipcProcess.match(/^#\[test\]$/gm) || []).length;
const ipcWindowsOnly = (ipcProcess.match(/^#\[cfg\(windows\)\]\n#\[tokio::test\]$/gm) || []).length;
const ipcNonWindowsOnly = (ipcProcess.match(/^#\[cfg\(not\(windows\)\)\]\n#\[tokio::test\]$/gm) || []).length;
assert.equal(ipcDeclaredTests, 12, 'RESEARCH_IPC_SOURCE_MUST_DECLARE_EXACT_TWELVE_ASYNC_TESTS');
assert.equal(ipcPlainTests, 1, 'RESEARCH_IPC_SOURCE_MUST_DECLARE_EXACT_ONE_SYNC_TEST');
assert.equal(ipcNonWindowsOnly, 1, 'RESEARCH_IPC_MUST_ISOLATE_EXACT_ONE_NON_WINDOWS_TEST');
assert.equal(ipcDeclaredTests + ipcPlainTests - ipcNonWindowsOnly, 12,
  'WINDOWS_RESEARCH_IPC_MUST_RUN_EXACT_TWELVE_TESTS');
assert.equal(ipcWindowsOnly, 1, 'RESEARCH_IPC_MUST_ISOLATE_EXACT_ONE_WINDOWS_TEST');
assert.doesNotMatch(ipcProcess, /^#\[ignore\]$/m, 'RESEARCH_IPC_CANNOT_CLAIM_IGNORED_TESTS');
// Exercise the actual path expression extracted independently from each job.
// In particular, the live 2026-11-25 Actions REST `/actions/runs` response
// for 45641162031 has path `.github/workflows/ci.yml` (without @ref).
const mainBlock = source.split('\n  component_impact:\n', 2)[1].split('\n  draft-component-tests:\n', 1)[0];
const nativePublisherBlock = source.split('\n  publish_accepted_windows_release:\n', 2)[1]
  .split('\n  publish_accepted_research_component:\n', 1)[0];
const publisherBlock = source.split('\n  publish_accepted_research_component:\n', 2)[1];
function workflowPathMatcher(block) {
  const found = block.match(/\.path \| (test\("(?:\\.|[^"])+"\))/g) || [];
  assert.equal(found.length, 1, 'EXACTLY_ONE_WORKFLOW_PATH_TRUST_FILTER');
  const expression = found[0].slice('.path | '.length);
  assert.equal(expression, acceptedWorkflowPath, 'WORKFLOW_PATH_FILTER_DRIFT');
  const encoded = expression.slice('test('.length, -1);
  return new RegExp(JSON.parse(encoded));
}
for (const [name, block] of [['main successful_pr_ci', mainBlock],
  ['Native-base Release publisher', nativePublisherBlock],
  ['Research Release publisher', publisherBlock]]) {
  const accepts = workflowPathMatcher(block);
  for (const valid of ['.github/workflows/ci.yml', '.github/workflows/ci.yml@main',
    '.github/workflows/ci.yml@refs/heads/main',
    '.github/workflows/ci.yml@work/580-runtime-separation',
    '.github/workflows/ci.yml@release/v1.2'])
    assert.ok(accepts.test(valid), `${name}:VALID_WORKFLOW_PATH:${valid}`);
  for (const invalid of ['', '.github/workflows/ci.yml@',
    '.github/workflows/other.yml', '.github/workflows/other.yml@main',
    '.github/workflows/ci.yml2', '.github/workflows/ci.yml@ main',
    '.github/workflows/ci.yml@refs//heads/main',
    '.github/workflows/ci.yml@../main',
    '.github/workflows/ci.yml@main\nmalicious'])
    assert.equal(accepts.test(invalid), false, `${name}:UNSAFE_WORKFLOW_PATH:${invalid}`);
  // All three selectors also have mandatory first-attempt gates, independently
  // enforced by validate() and the mutation controls below.
}
// Research's OS resource policy can change the measured Native Finance latency
// without touching Engine source.
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
// Research protocol/domain may be test/dev dependencies of Engine. The
// category itself must remain FULL until pinned Windows Cargo closure proof
// promotes a specific Ready PR after independent Engine decoupling.
assert.equal(scopeFor('crates/okx-research-protocol/src/analytics.rs'), 'full');
assert.equal(scopeFor('crates/okx-research-domain/src/numeric.rs'), 'full');
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
assert.throws(() => validate(source.replace('research-host) edge_required=true',
  'research-host) edge_required="${{ steps.file_scope.outputs.edge_changed }}"'), workflows),
  'MIXED_RESEARCH_CONTROL_WSS_MUST_NOT_SKIP_EDGE_CONTRACTS');
assert.throws(() => validate(source.replace('if [[ -n \'${{ steps.cargo_closure.outputs.scope }}\' ]]; then',
  'if [[ -n \'${{ steps.file_scope.outputs.scope }}\' ]]; then'), workflows));
assert.throws(() => validate(source.replace('git fetch --no-tags --depth=1 origin "$BASE_SHA"',
  'git fetch --no-tags --depth=1 origin main'), workflows));
assert.throws(() => validate(source.replace('name: rust-tests-linux', 'name: linux-trader'), workflows));
assert.throws(() => validate(source.replace('name: research-service-tests-windows',
  'name: research-owner-v2-windows'), workflows),
  'RESEARCH_READY_V2_DISPLAY_RENAME_CANNOT_REPLACE_ACCEPTED_V1_JOB');
assert.throws(() => validate(changeJobInput(source, 'required_linux',
  'needs: [component_impact, dependency-audit,', 'needs: [component_impact,'), workflows),
  'DEPENDENCY_AUDIT_CANNOT_BE_DROPPED_FROM_PROTECTED_LINUX_NEEDS');
assert.throws(() => validate(changeJobInput(source, 'required_linux',
  "            *) test \"${{ needs['dependency-audit'].result }}\" = \"success\" ;;",
  '            *) true ;;'), workflows),
  'RUST_DEPENDENCY_AUDIT_FAILURE_CANNOT_BE_BYPASSED');
assert.throws(() => validate(changeJobInput(source, 'required_linux',
  "            *) test \"${{ needs['dependency-audit'].result }}\" = \"success\" ;;",
  "            *) test \"${{ needs['dependency-audit'].result }}\" = \"skipped\" ;;"), workflows),
  'RUST_SCOPE_MUST_REQUIRE_EXECUTED_AUDIT_NOT_SKIPPED');
assert.throws(() => validate(changeJobInput(source, 'required_linux',
  "            docs|cloudflare) test \"${{ needs['dependency-audit'].result }}\" = \"skipped\" ;;",
  '            docs|cloudflare) true ;;'), workflows),
  'DOCS_EDGE_SCOPE_MUST_NOT_HIDE_UNEXPECTED_AUDIT_EXECUTION');
assert.throws(() => validate(source.replace('      - name: Format\n        run: cargo fmt --all -- --check',
  '      - name: Format\n        run: echo skipped-format'), workflows));
assert.throws(() => validate(source.replace('cargo test --locked --workspace --exclude okx-trading-engine --exclude okx-host-controller --exclude okx-research-service', 'echo skipped-portable-tests'), workflows));
assert.throws(() => validate(changeJobInput(source, 'rust_tests_linux',
  'cargo test --locked -p okx-research-service --test ipc_process -- --nocapture',
  'echo skipped-research-ipc'), workflows),
  'STANDALONE_RESEARCH_IPC_WITNESS_MUST_RUN_ON_LINUX');
assert.throws(() => validate(changeJobInput(source, 'rust_tests_linux',
  '# Former Engine T06–T10 IPC/causality witnesses now live in the',
  'cargo test --locked -p okx-trading-engine --lib research_dispatch_tests::\n          # Former Engine T06–T10 IPC/causality witnesses now live in the'), workflows),
  'LEGACY_ENGINE_RESEARCH_DEV_DISPATCH_MUST_NOT_RETURN_TO_LINUX');
assert.throws(() => validate(source.replace('      - name: Test portable crates',
  '      - run: cargo test -p okx-trade-execution --lib two_account_100_parallel_sql_readonly_isolation\n      - name: Test portable crates'), workflows));
assert.throws(() => validate(source.replace('      - name: Test portable crates',
  '      - run: npm ci --ignore-scripts --no-audit --no-fund\n      - name: Test portable crates'), workflows));
assert.throws(() => validate(source.replace('        default: true', '        default: false'), workflows));
assert.throws(() => validate(source.replace('options: [tests, release, both]', 'options: [tests, release]'), workflows),
  'MANUAL_CACHE_INPUT_MUST_RETAIN_BOTH_CHOICE');
assert.throws(() => validate(source.replace('        default: both', '        default: tests'), workflows),
  'MANUAL_CACHE_PROFILE_DEFAULT_MUST_WARM_BOTH');
assert.throws(() => validate(source.replace(selectedCacheMatrix,
  '        profile: [tests, release]'), workflows),
  'MANUAL_SINGLE_PROFILE_MUST_NOT_LAUNCH_BOTH_RUNNERS');
assert.throws(() => validate(source.replace("inputs.cache_profile == 'tests' && '[\"tests\"]'", 'false'), workflows),
  'MANUAL_TESTS_CACHE_PROFILE_MUST_NOT_BE_IGNORED');
assert.throws(() => validate(source.replace("inputs.cache_profile == 'release' && '[\"release\"]'", 'false'), workflows),
  'MANUAL_RELEASE_CACHE_PROFILE_MUST_NOT_BE_IGNORED');
assert.throws(() => validate(source.replace("inputs.cache_profile == 'both' && '[\"tests\",\"release\"]'", 'false'), workflows),
  'MANUAL_BOTH_CACHE_PROFILE_MUST_INCLUDE_RELEASE');
assert.throws(() => validate(source.replace("|| '[]') }}", "|| '[\"tests\",\"release\"]') }}"), workflows),
  'UNKNOWN_MANUAL_CACHE_PROFILE_MUST_NOT_START_WINDOWS_RUNNERS');
assert.throws(() => validate(source.replace('BOTH_TERMINAL_SOURCE_FRONTIER_WITNESSES_REQUIRED',
  'WE_DID_NOT_VERIFY_THE_TWO_TESTS'), workflows));
assert.throws(() => validate(source.replace('NATIVE_BASE_RETENTION=KEEP_ACCEPTED_ROLLBACK_ORIGINS',
  'gh release delete "$old" --cleanup-tag --yes'), workflows));
assert.throws(() => validate(source.replace('NATIVE_ACCOUNT_CATALOG_ALL_SIX_TESTS_REQUIRED',
  'WE_DID_NOT_VERIFY_THE_SIX_TESTS'), workflows));
assert.throws(() => validate(source.replace("$testArgs += @('--skip','accounts::native_connect_contracts::')",
  "# no catalog exclusion"), workflows));
assert.throws(() => validate(source.replace('stale_bill_coverage_blocks_settlement_after_inspect',
  'stale_bill_not_covered'), workflows));
assert.throws(() => validate(source.replace('always() &&\n        github.event_name',
  'github.event_name'), workflows));
assert.throws(() => validate(source.replace("inputs.prime_windows_cargo_cache == true", "true"), workflows));
assert.throws(() => validate(changeJobInput(source, 'trusted_main_cache_prime',
  "github.event_name == 'workflow_dispatch' &&", "github.event_name == 'pull_request' &&"), workflows));
assert.throws(() => validate(changeJobInput(source, 'trusted_main_cache_prime',
  "github.event_name == 'workflow_dispatch' &&", "github.event_name == 'push' &&"), workflows),
  'MAIN_PUSH_MUST_NOT_TRIGGER_COMPILER_CACHE_PRIME');
assert.throws(() => validate(source.replaceAll('test "${{ needs.trading_engine_build_windows.result }}" = "success"', 'true'), workflows));
assert.throws(() => validate(source.replaceAll('test "${{ needs.trading_engine_regressions_windows.result }}" = "success"', 'true'), workflows));
assert.throws(() => validate(source.replace('test "${{ needs.trading_engine_regressions_windows.result }}" = "skipped"', 'true'), workflows));
assert.throws(() => validate(source.replace('"research_only=true" >> "$GITHUB_OUTPUT"', 'true'), workflows));
assert.throws(() => validate(source.replace('RESEARCH_COMPONENT_EXACT_THREE_FILE_PROOF_FAILED', 'BYPASS'), workflows));
assert.throws(() => validate(source.replace(acceptedWorkflowPath,
  String.raw`test("^\\.github/workflows/ci\\.yml$")`), workflows),
  'MAIN_MUST_ACCEPT_GITHUB_REF_WORKFLOW_PATH');
assert.throws(() => validate(source.replace(acceptedWorkflowPath,
  String.raw`test("^\\.github/workflows/ci\\.yml.*$")`), workflows),
  'MAIN_MUST_REJECT_OTHER_WORKFLOWS');
const publisherIndex = source.lastIndexOf(acceptedWorkflowPath);
assert.ok(publisherIndex > source.indexOf(acceptedWorkflowPath), 'TWO_INDEPENDENT_WORKFLOW_TRUST_FILTERS');
const nativePublisherIndex = source.indexOf(acceptedWorkflowPath, source.indexOf('\n  publish_accepted_windows_release:\n'));
assert.ok(nativePublisherIndex > source.indexOf(acceptedWorkflowPath), 'NATIVE_PUBLICATION_HAS_OWN_TRUST_FILTER');
assert.ok(nativePublisherIndex < publisherIndex, 'PUBLISHER_PATH_FILTERS_DISTINCT');
assert.throws(() => validate(source.slice(0, nativePublisherIndex) +
  String.raw`test("^\\.github/workflows/ci\\.yml.*$")` +
  source.slice(nativePublisherIndex + acceptedWorkflowPath.length), workflows),
  'NATIVE_PUBLISHER_MUST_REJECT_OTHER_WORKFLOWS');
assert.throws(() => validate(source.slice(0, publisherIndex) +
  String.raw`test("^\\.github/workflows/ci\\.yml$")` +
  source.slice(publisherIndex + acceptedWorkflowPath.length), workflows),
  'PUBLISHER_MUST_ACCEPT_GITHUB_REF_WORKFLOW_PATH');
assert.throws(() => validate(source.slice(0, publisherIndex) +
  String.raw`test("^\\.github/workflows/ci\\.yml.*$")` +
  source.slice(publisherIndex + acceptedWorkflowPath.length), workflows),
  'PUBLISHER_MUST_REJECT_OTHER_WORKFLOWS');
assert.throws(() => validate(source.replace('.run_attempt == 1) | [.id, .run_attempt]',
  '.run_attempt > 0) | [.id, .run_attempt]'), workflows));
assert.throws(() => validate(source.replace('all(.jobs[]; .run_id == $run and .head_sha == $head and .run_attempt == 1)',
  'all(.jobs[]; .run_id == $run)'), workflows),
  'NATIVE_PUBLISHER_REQUIRES_SAME_HEAD_AND_ATTEMPT_JOBS');
assert.throws(() => validate(changeJobInput(source, 'publish_accepted_research_component',
  'all(.jobs[]; .run_id == $run and .head_sha == $head and .run_attempt == 1)',
  'all(.jobs[]; .run_id == $run and .head_sha == $head)'), workflows),
  'RESEARCH_PUBLISHER_REJECTS_MIXED_FIRST_AND_RERUN_JOB_EVIDENCE');
assert.throws(() => validate(changeJobInput(source, 'publish_accepted_research_component',
  '"research-service-tests-windows"', '"research-owner-v2-windows"'), workflows),
  'RESEARCH_PUBLISHER_CANNOT_ACCEPT_UNPROVEN_V2_JOB_ALIAS');
assert.throws(() => validate(source.replace(
  'RESEARCH_READY_STATUS_POLICY=okx.github.actions.research-ready/v1',
  'RESEARCH_READY_STATUS_POLICY=okx.github.actions.research-ready/v2'), workflows),
  'UNACCEPTED_RESEARCH_V2_READY_POLICY_CANNOT_HIDE_V1_PROTECTED_NAMES');
assert.throws(() => validate(source.replace('"$attempt" == 1', '"$attempt" =~ ^[1-9]'), workflows),
  'NATIVE_PUBLISHER_MUST_REJECT_SECOND_ATTEMPT');
assert.throws(() => validate(source.replace('runs/$candidate/attempts/$attempt/jobs?per_page=100',
  'runs/$candidate/jobs?per_page=100'), workflows),
  'NATIVE_PUBLISHER_MUST_USE_EXACT_ATTEMPT_JOB_LIST');
assert.throws(() => validate(changeJobInput(source, 'publish_accepted_windows_release',
  '.head_sha == $head and .run_attempt == 1 and', '.head_sha == $head and .run_attempt > 0 and'),
  workflows), 'NATIVE_PUBLISHER_REQUIRES_FIRST_ATTEMPT_RUN');
assert.throws(() => validate(changeJobInput(source, 'publish_accepted_windows_release',
  '"trading-engine-acceptance-windows"', '"trading-engine-acceptance-SKIPPED"'),
  workflows), 'NATIVE_PUBLISHER_MUST_REQUIRE_REAL_FINANCIAL_ACCEPTANCE');
assert.throws(() => validate(source.replace('.workflow_run.head_repository_id == 1388071566 and',
  'true and'), workflows),
  'NATIVE_PUBLISHER_MUST_MATCH_ARTIFACT_ORIGIN');
assert.throws(() => validate(source.replace('NO_EXACT_FIRST_ATTEMPT_FULL_NATIVE_READY_PR_ARTIFACT',
  'No fresh bundle, skipping publication'), workflows),
  'NATIVE_PUBLISHER_MUST_NOT_FAKE_GREEN_MISSING_RELEASE');
assert.throws(() => validate(source.replace('RESEARCH_ONLY_REAL_MCP_DO_WSS_WITNESS_NOT_EXECUTED', 'BYPASS'), workflows));
assert.throws(() => validate(source.replace('name: okx-research-component-${{ github.event.pull_request.head.sha }}',
  'name: okx-windows-bundle-${{ github.event.pull_request.head.sha }}'), workflows));
assert.throws(() => validate(source.replace('test "sha256:$digest" = \'${{ steps.select.outputs.digest }}\'',
  'true'), workflows));
assert.throws(() => validate(source.replace('RESEARCH_ROLLBACK_RELEASE_RETENTION=KEEP_ALL_ACCEPTED',
  'gh release delete "$old" --cleanup-tag --yes'), workflows));
assert.throws(() => validate(changeJobInput(source, 'publish_accepted_research_component',
  '-F draft=true -F prerelease=false', '-F draft=false -F prerelease=false'), workflows),
  'RESEARCH_RELEASE_MUST_STAGE_DRAFT_FIRST');
assert.throws(() => validate(changeJobInput(source, 'publish_accepted_research_component',
  'gh api --paginate "/repos/$GITHUB_REPOSITORY/releases?per_page=100"',
  'gh api "/repos/$GITHUB_REPOSITORY/releases/tags/$tag"'), workflows),
  'RESEARCH_DRAFT_MUST_BE_LOCATED_IN_AUTHENTICATED_RELEASE_LIST');
assert.throws(() => validate(changeJobInput(source, 'publish_accepted_research_component',
  'if ($matches | length) > 1 then error("RESEARCH_RELEASE_TAG_COLLISION")',
  'if ($matches | length) > 1 then $matches[0]'), workflows),
  'RESEARCH_RELEASE_TAG_COLLISION_MUST_FAIL_CLOSED');
assert.throws(() => validate(changeJobInput(source, 'publish_accepted_research_component',
  'gh api "/repos/$GITHUB_REPOSITORY/releases/$release_id"',
  'gh api "/repos/$GITHUB_REPOSITORY/releases/tags/$tag"'), workflows),
  'DRAFT_CANNOT_BE_READ_BY_PUBLISHED_TAG_ENDPOINT');
assert.throws(() => validate(changeJobInput(source, 'publish_accepted_research_component',
  '.id == $id and .tag_name == $tag and .target_commitish == $main',
  '.tag_name == $tag and .target_commitish == $main'), workflows),
  'RESEARCH_RELEASE_READBACK_REQUIRES_NUMERIC_ID_MATCH');
assert.throws(() => validate(changeJobInput(source, 'publish_accepted_research_component',
  'RESEARCH_DRAFT_ORIGIN_NOT_FOUND', 'DRAFT_ORIGIN_UNKNOWN_IGNORED'), workflows),
  'RESEARCH_RELEASE_MUST_REJECT_ABSENT_DRAFT_ORIGIN');
assert.throws(() => validate(changeJobInput(source, 'publish_accepted_research_component',
  'test "$sha" = "$ACCEPTED_MAIN"', 'test "$sha" != "$ACCEPTED_MAIN"'), workflows),
  'RESEARCH_PUBLISHED_TAG_CANNOT_POINT_TO_DIFFERENT_SOURCE');
assert.throws(() => validate(changeJobInput(source, 'publish_accepted_research_component',
  'verify_tag_target required', 'verify_tag_target optional'), workflows),
  'RESEARCH_PUBLISHED_RELEASE_MUST_HAVE_AN_ACTUAL_TAG_REF');
assert.throws(() => validate(changeJobInput(source, 'publish_accepted_research_component',
  'object="$(gh api "/repos/$GITHUB_REPOSITORY/git/tags/$sha")"',
  'object="$(gh api "/repos/$GITHUB_REPOSITORY/git/commits/$sha")"'), workflows),
  'RESEARCH_ANNOTATED_TAG_TARGET_MUST_BE_PEELED');
assert.throws(() => validate(changeJobInput(source, 'publish_accepted_research_component',
  'test "$present" = "$expected"', 'true'), workflows),
  'RESEARCH_UPLOAD_422_MUST_NOT_BYPASS_DIGEST');
assert.throws(() => validate(source.replaceAll(
  'all(.assets[]; .state == "uploaded")', 'all(.assets[]; .state == "starter")'), workflows),
  'RESEARCH_PARTIAL_STARTER_ASSETS_MUST_NOT_PUBLISH');
assert.throws(() => validate(source.replaceAll(
  '[[ "$asset_id" =~ ^[1-9][0-9]*$ ]]', 'true'), workflows),
  'RESEARCH_ASSET_IDS_MUST_BE_POSITIVE');
assert.throws(() => validate(source.replaceAll(
  '.assets[] | select(.name == $n) | .size', '.assets[] | select(.name == $n) | .url'), workflows),
  'RESEARCH_RELEASE_ASSET_SIZE_MUST_MATCH_LOCAL_BYTES');
assert.throws(() => validate(changeJobInput(source, 'publish_accepted_research_component',
  'https://uploads.github.com/repos/$GITHUB_REPOSITORY/releases/$release_id/assets?name=$name',
  'https://uploads.github.com/repos/$GITHUB_REPOSITORY/releases/999/assets?name=$name'), workflows),
  'RESEARCH_DRAFT_UPLOAD_MUST_TARGET_SELECTED_RELEASE_ID');
assert.throws(() => validate(changeJobInput(source, 'publish_accepted_research_component',
  '--data-binary "@$RUNNER_TEMP/$name"',
  '--data-binary @"$RUNNER_TEMP/unverified-file"'), workflows),
  'RESEARCH_RELEASE_MUST_UPLOAD_EXACT_VERIFIED_BYTES');
assert.throws(() => validate(changeJobInput(source, 'publish_accepted_research_component',
  '-H "Authorization: Bearer $GH_TOKEN"', '-H "Authorization: Bearer invalid"'), workflows),
  'RESEARCH_DRAFT_UPLOAD_REQUIRES_GITHUB_TOKEN');
assert.throws(() => validate(changeJobInput(source, 'publish_accepted_research_component',
  'test "$present" = "$expected"', 'gh release upload "$tag" "$RUNNER_TEMP/$name" --clobber'), workflows),
  'RESEARCH_RELEASE_NEVER_CLOBBERS_EXISTING_ASSET');
assert.throws(() => validate(changeJobInput(source, 'publish_accepted_research_component',
  'if ! gh api --method PATCH "/repos/$GITHUB_REPOSITORY/releases/$release_id"',
  'if ! gh release edit "$tag" --draft=false'), workflows),
  'RESEARCH_DRAFT_MUST_PUBLISH_BY_SELECTED_NUMERIC_ID');
assert.throws(() => validate(changeJobInput(source, 'publish_accepted_research_component',
  '-F draft=false > "$RUNNER_TEMP/research-release-publish.json"',
  '-F draft=true > "$RUNNER_TEMP/research-release-publish.json"'), workflows),
  'RESEARCH_RELEASE_ONLY_PUBLISHED_AFTER_COMPLETE_PROOF');
assert.throws(() => validate(changeJobInput(source, 'publish_accepted_research_component',
  'RESEARCH_DRAFT_CREATE_AMBIGUOUS_VERIFY_ORIGIN', 'DRAFT_CREATE_ERROR_IGNORED'), workflows),
  'AMBIGUOUS_RELEASE_CREATE_MUST_TRIGGER_PROVENANCE_READBACK');
assert.throws(() => validate(changeJobInput(source, 'publish_accepted_research_component',
  'RESEARCH_DRAFT_PUBLISH_AMBIGUOUS_VERIFY_RELEASE', 'DRAFT_PUBLISH_ERROR_IGNORED'), workflows),
  'AMBIGUOUS_DRAFT_PUBLISH_MUST_TRIGGER_FINAL_READBACK');
assert.throws(() => validate(changeJobInput(source, 'publish_accepted_research_component',
  'jq -e \'([.assets[].name] | sort) ==', 'jq -e \'true =='), workflows),
  'RESEARCH_RELEASE_MUST_NOT_PUBLISH_INCOMPLETE_DRAFT');
assert.throws(() => validate(changeJobInput(source, 'publish_accepted_research_component',
  'if jq -e \'.draft == true\' <<<"$release"; then',
  'if true; then'), workflows),
  'PUBLISHED_RESEARCH_RELEASE_MUST_NOT_ACCEPT_NEW_ASSETS');
assert.throws(() => validate(changeJobInput(source, 'publish_accepted_research_component',
  'test "$(jq -r .id <<<"$published")" = "$release_id"',
  'true'), workflows),
  'RESEARCH_PUBLISHED_TAG_MUST_BIND_NUMERIC_RELEASE_ID');
assert.throws(() => validate(changeJobInput(source, 'publish_accepted_research_component',
  '.workflow_run.head_repository_id == 1388071566 and', 'true and'), workflows));
assert.throws(() => validate(changeJobInput(source, 'publish_accepted_research_component',
  '.head_repository.id == 1388071566) |', 'true) |'), workflows));
assert.throws(() => validate(source.replace('.commits[-1].sha == $head', 'true'), workflows));
assert.throws(() => validate(source.replace('test "${{ needs.component_impact.outputs.native_proof }}" = "RESEARCH_OWNER_ONLY"', 'true'), workflows));
assert.throws(() => validate(source.replace('node scripts/ci-finance-witness-manifest.cjs --verify-report', 'echo skipped'), workflows));
assert.throws(() => validate(changeJobInput(source, 'trading_engine_acceptance_windows',
  'cargo build --locked -p okx-research-service --bin okx-research-load-fixture --features test-fixture',
  'echo fake-research-load-fixture'), workflows),
  'REAL_ENGINE_RESEARCH_T10_FIXTURE_MUST_BE_COMPILED');
assert.throws(() => validate(changeJobInput(source, 'trading_engine_acceptance_windows',
  "$env:OKX_RESEARCH_LOAD_FIXTURE_EXE = (Resolve-Path 'target/debug/okx-research-load-fixture.exe').Path",
  '$env:OKX_RESEARCH_LOAD_FIXTURE_EXE = $null'), workflows),
  'ENGINE_RESEARCH_T10_TESTS_MUST_HAVE_TWO_REAL_FIXTURE_PATHS');
assert.throws(() => validate(changeJobInput(source, 'trading_engine_regressions_windows',
  'cargo build --locked -p okx-research-service --bin okx-research-load-fixture --features test-fixture',
  'echo fake-research-load-fixture'), workflows),
  'ENGINE_REGRESSION_FIXTURE_BUILD_MUST_NOT_DISAPPEAR');
assert.throws(() => validate(changeJobInput(source, 'trading_engine_regressions_windows',
  "$env:OKX_RESEARCH_LOAD_FIXTURE_EXE = (Resolve-Path 'target/debug/okx-research-load-fixture.exe').Path",
  '$env:OKX_RESEARCH_LOAD_FIXTURE_EXE = $null'), workflows),
  'ENGINE_REGRESSION_TESTS_MUST_USE_REAL_FIXTURE_PATH');
// Apply each mutation *before* asserting failure: a stale `before` substring
// must not count as a successfully exercised negative control.
function rejectFinanceDenialMutation(before, after, reason) {
  const mutated = changeJobInput(source, 'trading_engine_regressions_windows', before, after);
  assert.notEqual(mutated, source, `MISSING_FINANCE_DENIAL_MUTATION:${reason}`);
  assert.throws(() => validate(mutated, workflows), reason);
}
rejectFinanceDenialMutation(
  '      - name: Engine rejects Research owner attempts to mutate Native finance',
  '      - name: Engine finance denial skipped',
  'ENGINE_RESEARCH_FINANCE_DENIAL_STEP_CANNOT_DISAPPEAR');
rejectFinanceDenialMutation(
  'query::public_dispatch_tests::research_owner_handoff_cannot_change_native_finance_files_or_enqueue_writes',
  'query::public_dispatch_tests::irrelevant_readonly_test',
  'ENGINE_RESEARCH_FINANCE_DENIAL_MUST_RUN_EXACT_TEST_NAME');
rejectFinanceDenialMutation(
  "$testName = 'query::public_dispatch_tests::research_owner_handoff_cannot_change_native_finance_files_or_enqueue_writes'",
  "$testName = 'query::public_dispatch_tests::irrelevant_readonly_test'",
  'ENGINE_RESEARCH_FINANCE_DENIAL_MUST_VALIDATE_THE_ACTUAL_TEST_LINE');
rejectFinanceDenialMutation(
  'research_owner_handoff_cannot_change_native_finance_files_or_enqueue_writes -- --exact --test-threads 1',
  'research_owner_handoff_cannot_change_native_finance_files_or_enqueue_writes -- --test-threads 1',
  'ENGINE_RESEARCH_FINANCE_DENIAL_MUST_RETAIN_EXACT_MATCH');
rejectFinanceDenialMutation(
  'test result: ok\\. 1 passed; 0 failed; 0 ignored; 0 measured; [1-9][0-9]* filtered out;',
  'test result: ok\\. 0 passed; 0 failed; 0 ignored; 0 measured; [1-9][0-9]* filtered out;',
  'ENGINE_RESEARCH_FINANCE_DENIAL_MUST_FAIL_ON_ZERO_MATCHED_TESTS');
rejectFinanceDenialMutation(
  'test result: ok\\. 1 passed; 0 failed; 0 ignored; 0 measured; [1-9][0-9]* filtered out;',
  'test result: ok\\. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out;',
  'ENGINE_RESEARCH_FINANCE_DENIAL_MUST_REQUIRE_REAL_FILTERED_ROSTER');
rejectFinanceDenialMutation(
  "(?m)^test ' + [regex]::Escape($testName) + ' \\.\\.\\. ok$",
  "(?m)^test ' + [regex]::Escape($testName) + ' .*",
  'ENGINE_FINANCE_DENIAL_MUST_PROVE_NAMED_TEST_PASS_LINE');
rejectFinanceDenialMutation(
  "(?m)^test ' + [regex]::Escape($testName) + ' \\.\\.\\. ok$') -or",
  "(?m)^test ' + [regex]::Escape($testName) + ' \\.\\.\\. ok$') -and",
  'ENGINE_FINANCE_DENIAL_MUST_REQUIRE_BOTH_NAMED_TEST_AND_RESULT');
assert.throws(() => validate(source.replace('$manifestJson = & node scripts/ci-finance-witness-manifest.cjs', '$manifestJson = & cargo run --locked -p okx-native-finance-check -- --list'), workflows));
assert.throws(() => validate(source.replace('needs: [component_impact, trading_engine_acceptance_windows, trading_engine_regressions_windows,', 'needs: [component_impact, trading_engine_acceptance_windows,'), workflows));
assert.throws(() => validate(source.replace('"trading-engine-regressions-windows (engine)",', ''), workflows));
assert.throws(() => validate(source.replace('suite: [engine, host, execution, research]', 'suite: [engine, host, execution]'), workflows),
  'RESEARCH_REGRESSION_MATRIX_CANNOT_BE_DROPPED');
assert.throws(() => validate(changeJobInput(source, 'trading_engine_regressions_windows',
  "if: matrix.suite == 'host'", "if: matrix.suite == 'clippy'"), workflows),
  'WINDOWS_CLIPPY_MUST_RUN_IN_THE_HOST_MATRIX');
assert.throws(() => validate(source.replace(
  'cargo clippy --locked -p okx-trading-engine -p okx-host-controller -p okx-host-launcher -p okx-trade-execution --all-targets -- -D warnings',
  'echo skipped-windows-clippy'), workflows), 'WINDOWS_CLIPPY_CANNOT_BE_REMOVED');
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
// Exact GitHub event eligibility is independently modeled and checked against
// the mandatory literal workflow gate above. These cases catch an accidental
// switch to a generic Draft, an unrelated label, a fork or a Ready run.
function s2Eligible(event) {
  return event.name === 'pull_request' && event.draft === true &&
    event.headRepo === event.repository && event.labels.includes('s2-disposable-windows') &&
    (event.action === 'synchronize' ||
      (event.action === 'labeled' && event.appliedLabel === 's2-disposable-windows'));
}
const allowedS2 = {name: 'pull_request', action: 'labeled', draft: true,
  headRepo: 'iamaman11/okx', repository: 'iamaman11/okx',
  labels: ['s2-disposable-windows'], appliedLabel: 's2-disposable-windows'};
assert.equal(s2Eligible(allowedS2), true, 'S2_EXPLICIT_LABEL_EVENT_MUST_ROUTE');
assert.equal(s2Eligible({...allowedS2, action: 'synchronize', appliedLabel: ''}), true,
  'S2_SYNC_WITH_EXISTING_LABEL_MUST_ROUTE');
for (const [name, event] of [
  ['ORDINARY_DRAFT', {...allowedS2, labels: []}],
  ['UNRELATED_LABEL_EVENT', {...allowedS2, appliedLabel: 'enhancement'}],
  ['READY_PR', {...allowedS2, draft: false}],
  ['FORK', {...allowedS2, headRepo: 'someone-else/okx'}],
  ['MAIN_PUSH', {...allowedS2, name: 'push'}],
  ['DISPATCH', {...allowedS2, name: 'workflow_dispatch'}],
  ['REOPENED', {...allowedS2, action: 'reopened'}],
  ['READY_FOR_REVIEW', {...allowedS2, action: 'ready_for_review', draft: false}],
  ['TITLE_EDIT', {...allowedS2, action: 'edited'}],
]) assert.equal(s2Eligible(event), false, `S2_MUST_NOT_START_WINDOWS:${name}`);

function rejectS2RoutingMutation(before, after, reason) {
  const mutated = changeJobInput(source, 'research-lpac-disposable-windows', before, after);
  assert.notEqual(mutated, source, `S2_ROUTING_MUTATION_NOT_APPLIED:${reason}`);
  assert.throws(() => validate(mutated, workflows), reason);
}
for (const [before, after, reason] of [
  ["github.event.pull_request.draft == true", 'true', 'S2_READY_CANNOT_EXECUTE'],
  ["github.event.pull_request.head.repo.full_name == github.repository", 'true', 'S2_FORK_CANNOT_EXECUTE'],
  ["contains(github.event.pull_request.labels.*.name, 's2-disposable-windows')", 'true',
    'S2_UNLABELED_CANNOT_EXECUTE'],
  ["github.event.action == 'labeled' && github.event.label.name == 's2-disposable-windows'",
    "github.event.action == 'labeled'", 'S2_UNRELATED_LABEL_CANNOT_EXECUTE'],
  ["github.event.action == 'synchronize'", "github.event.action == 'opened'", 'S2_SYNC_MUST_RETAIN_OPT_IN'],
  ['runs-on: windows-latest', 'runs-on: self-hosted', 'S2_RUNNER_MUST_BE_EPHEMERAL'],
  ["OKX_DISPOSABLE_WINDOWS_RUNNER: '1'", "OKX_DISPOSABLE_WINDOWS_RUNNER: '0'",
    'S2_GUARD_CANNOT_BE_DISABLED'],
  ['persist-credentials: false', 'persist-credentials: true', 'S2_CHECKOUT_CANNOT_RETAIN_WRITE_CREDENTIALS'],
  ['save-if: false', 'save-if: true', 'S2_UNREVIEWED_CARGO_CACHE_CANNOT_BE_SAVED'],
  ['if: always()', 'if: success()', 'S2_FAILED_RUN_MUST_ATTEMPT_RECEIPT_UPLOAD'],
  ['if-no-files-found: error', 'if-no-files-found: warn', 'S2_MISSING_RECEIPT_CANNOT_BE_SILENT'],
  ['run: ./scripts/ci-s2-disposable-windows.ps1', 'run: echo skipped', 'S2_PHYSICAL_RUNNER_REQUIRED'],
]) rejectS2RoutingMutation(before, after, reason);

function rejectS2ScriptMutation(before, after, reason) {
  assert.ok(disposableScript.includes(before), `S2_SCRIPT_MUTATION_NOT_APPLIED:${reason}`);
  assert.throws(() => validate(source, workflows, disposableScript.replace(before, after)), reason);
}
for (const [before, after, reason] of [
  ["$env:RUNNER_ENVIRONMENT -ne 'github-hosted'", '$false', 'S2_HOSTED_WINDOWS_ONLY'],
  ["$env:OKX_DISPOSABLE_WINDOWS_RUNNER -cne '1'", '$false', 'S2_RUNTIME_GUARD_REQUIRED'],
  ["$head -cne $env:OKX_S2_EXPECTED_HEAD", '$false', 'S2_IMMUTABLE_HEAD_REQUIRED'],
  ["git rev-parse 'HEAD^{tree}'", 'git rev-parse HEAD', 'S2_TREE_PROVENANCE_REQUIRED'],
  ["$env:OKX_DISPOSABLE_WINDOWS_ROOT = $root", '$env:OKX_DISPOSABLE_WINDOWS_ROOT = "C:\\okx"',
    'S2_TEMP_ROOT_REQUIRED'],
  ['.okx-disposable-windows-runner', '.unverified-runner-marker', 'S2_DISPOSABLE_MARKER_REQUIRED'],
  ['--bin okx-research-service --features s2-disposable-windows', '--bin okx-research-service',
    'S2_WORKER_MUST_NOT_USE_PRODUCTION_ORIGIN_PINNING'],
  ["research_worker_build_feature = 's2-disposable-windows'", "research_worker_build_feature = 'none'",
    'S2_FEATURE_MUST_BE_ATTESTED_IN_SMOKE_RECEIPT'],
  ['-- --ignored --exact --show-output --test-threads 1', '-- --show-output', 'S2_IGNORED_EXACT_ONLY'],
  ['-- --ignored --exact --show-output --test-threads 1', '-- --ignored --exact --nocapture --test-threads 1',
    'S2_UNCAPTURED_STDOUT_MAY_INTERLEAVE_NAMED_PASS_LINE'],
  ['$namedPass = $log -cmatch', '$namedPass = $true #', 'S2_NAMED_PASS_REQUIRED'],
  ['$hasWitness = $log.Contains($witness)', '$hasWitness = $true #', 'S2_NATIVE_WITNESS_REQUIRED'],
  ['1 passed; 0 failed;', '0 passed; 0 failed;', 'S2_NONZERO_EXECUTED_CASE_REQUIRED'],
  ['$failedTests.Add($testName)', 'throw "ABORT_AFTER_FIRST_FAILED_PROBE"',
    'S2_FIRST_FAILED_LPAC_MUST_NOT_HIDE_FIVE_ACL_CASES'],
  ['$failedTests.Add($testName)', 'break', 'S2_CANNOT_STOP_AFTER_FIRST_FAILED_TEST'],
  ['$receipt.tests += [ordered]@{', '$null = [ordered]@{', 'S2_EACH_FAILED_TEST_NEEDS_RECEIPT'],
  ['$receipt.attempted_exact_test_count += 1', '$null = 1',
    'S2_ALL_ATTEMPTED_CASES_MUST_BE_COUNTED'],
  ['$receipt.executed_exact_test_count += 1', '$null = 1',
    'S2_FAILED_BUT_EXECUTED_NATIVE_TEST_MUST_BE_COUNTED'],
  ['$receipt.passed_exact_test_count += 1', '$null = 1',
    'S2_PASSED_CASES_MUST_BE_COUNTED_INDEPENDENTLY'],
  ['$receipt.passed_exact_test_count -ne $tests.Count', '$false',
    'S2_ANY_FAILING_CASE_MUST_FAIL_WHOLE_JOB'],
  ['$failedTests.Count -ne 0 -or $tests.Count -ne 6', '$false -or $tests.Count -ne 6',
    'S2_ANY_FAILED_CASE_MUST_FAIL_AGGREGATE'],
  ['$receipt.failed_exact_test_names = $failedTests.ToArray()', '$receipt.failed_exact_test_names = @()',
    'S2_FAILED_CASE_NAMES_MUST_APPEAR_IN_RECEIPT'],
  ['(?:ok|FAILED)', '(?:ok)', 'S2_FAILED_NATIVE_EXECUTION_MUST_BE_RECORDED'],
  ["$tests.Count -ne 6", '$tests.Count -ne 0', 'S2_SIX_PHYSICAL_TESTS_REQUIRED'],
  ['Get-FileHash -Algorithm SHA256 -LiteralPath $testLog', '"unverified"', 'S2_LOG_SHA256_REQUIRED'],
  ['acceptance_status = \'NOT_PROVEN_UNTIL_INDEPENDENT_REVIEW\'',
    "acceptance_status = 'READY'", 'S2_CANNOT_DECLARE_READY'],
]) rejectS2ScriptMutation(before, after, reason);
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
  windowsIpcSummary, String.raw`test result: ok\. 9 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out;`), workflows),
  'WINDOWS_RESEARCH_IPC_STALE_NINE_MUST_FAIL');
assert.throws(() => validate(changeJobInput(source, 'research_service_tests_windows',
  windowsIpcSummary, String.raw`test result: ok\. 10 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out;`), workflows),
  'WINDOWS_RESEARCH_IPC_STALE_TEN_MUST_FAIL');
assert.throws(() => validate(changeJobInput(source, 'research_service_tests_windows',
  windowsIpcSummary, String.raw`test result: ok\. 11 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out;`), workflows),
  'WINDOWS_RESEARCH_IPC_UNDERCOUNT_ELEVEN_MUST_FAIL');
assert.throws(() => validate(changeJobInput(source, 'research_service_tests_windows',
  windowsIpcSummary, String.raw`test result: ok\. 12 passed; 0 failed;`), workflows),
  'WINDOWS_RESEARCH_IPC_IGNORED_OR_FILTERED_NOT_ACCEPTED');
assert.throws(() => validate(changeJobInput(source, 'research_service_tests_windows',
  'RESEARCH_IPC_ALL_TWELVE_TESTS_REQUIRED', 'RESEARCH_IPC_ALL_TEN_TESTS_REQUIRED'), workflows),
  'WINDOWS_RESEARCH_IPC_FAILURE_CODE_CANNOT_DRIFT');
assert.throws(() => validate(changeJobInput(source, 'trading_engine_regressions_windows',
  windowsIpcSummary, String.raw`test result: ok\. 10 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out;`), workflows),
  'FULL_WINDOWS_RESEARCH_IPC_MUST_EXECUTE_ALL_TWELVE');
for (const id of ['research_service_tests_windows', 'trading_engine_regressions_windows']) {
  for (const witness of [
    'research_worker_has_no_privileged_credential_store_api_or_dependency',
    'privileged_edge_token_cli_is_disabled_before_stdin_or_catalog_on_all_platforms',
  ]) {
    assert.throws(() => validate(changeJobInput(source, id, witness, 'REMOVED_PRIVILEGED_CLI_NEGATIVE'), workflows),
      `RESEARCH_PRIVILEGED_CLI_NEGATIVE_REQUIRED:${id}:${witness}`);
  }
}
assert.throws(() => validate(changeJobInput(source, 'research_service_tests_windows',
  'cargo test --locked -p okx-strategy-core -p okx-research -p okx-analysis',
  'cargo test --locked -p okx-strategy-core -p okx-research -p okx-research-service -p okx-analysis'), workflows));
for (const profile of ['DEV', 'TEST']) {
  assert.throws(() => validate(changeJobInput(source, 'trading_engine_build_windows',
    `CARGO_PROFILE_${profile}_DEBUG: 0`, `CARGO_PROFILE_${profile}_DEBUG: ''`), workflows));
}
console.log('CI_LEAN_MAIN_ROUTING_CONTROLS=PASS');
