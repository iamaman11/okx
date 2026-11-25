# Physical S2 probes on a disposable GitHub-hosted Windows runner only.
# This is deliberately separate from Ready/CONTROL and financial releases.
$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$tests = @(
    'research_sandbox::native::tests::disposable_real_research_lpac_ready_job_and_bearer',
    'research_acl::native::tests::disposable_windows_acl_image_package_rx_and_inventory',
    'research_acl::native::tests::disposable_windows_acl_unsealed_catalog_is_rejected',
    'research_acl::native::tests::disposable_windows_acl_reparse_and_hardlink_rejected',
    'research_acl::native::tests::disposable_windows_acl_catalog_sqlite_sidecars_inherit_low_il',
    'research_acl::native::tests::disposable_windows_acl_finance_sqlite_locks_untouched'
)

if ([string]::IsNullOrWhiteSpace($env:RUNNER_TEMP) -or
    [string]::IsNullOrWhiteSpace($env:GITHUB_RUN_ID) -or
    [string]::IsNullOrWhiteSpace($env:GITHUB_RUN_ATTEMPT)) {
    throw 'S2_DISPOSABLE_MISSING_GITHUB_RUNNER_TEMP_OR_RUN_ID'
}
$runnerTemp = (Resolve-Path -LiteralPath $env:RUNNER_TEMP).Path.TrimEnd('\', '/')
$suffix = "$($env:GITHUB_RUN_ID)-$($env:GITHUB_RUN_ATTEMPT)"
$evidenceDir = Join-Path $runnerTemp "okx-s2-disposable-evidence-$suffix"
$root = Join-Path $runnerTemp "okx-s2-disposable-$suffix"
$receiptPath = Join-Path $evidenceDir 'receipt.json'
New-Item -ItemType Directory -Path $evidenceDir -ErrorAction Stop | Out-Null

$receipt = [ordered]@{
    schema = 'okx.s2.disposable-windows/v1'
    purpose = 'isolated physical smoke; never authorizes Ready, release or installation'
    acceptance_status = 'NOT_PROVEN_UNTIL_INDEPENDENT_REVIEW'
    result = 'FAILED'
    started_at_utc = [DateTime]::UtcNow.ToString('o')
    completed_at_utc = $null
    source_head_sha = $null
    source_tree_sha = $null
    expected_head_sha = $env:OKX_S2_EXPECTED_HEAD
    github_repository = $env:GITHUB_REPOSITORY
    run_id = $env:GITHUB_RUN_ID
    run_attempt = $env:GITHUB_RUN_ATTEMPT
    runner_os = $env:RUNNER_OS
    runner_environment = $env:RUNNER_ENVIRONMENT
    windows_version = [Environment]::OSVersion.VersionString
    runner_image = $env:ImageOS
    runner_image_version = $env:ImageVersion
    disposable_root = $root
    research_worker_sha256 = $null
    expected_exact_test_count = $tests.Count
    executed_exact_test_count = 0
    tests = @()
    build_log_sha256 = $null
    failure = $null
}

$exitCode = 1
try {
    # Fail closed even if this script is invoked outside the opt-in workflow.
    if ($env:GITHUB_ACTIONS -ne 'true' -or $env:RUNNER_OS -ne 'Windows' -or
        $env:RUNNER_ENVIRONMENT -ne 'github-hosted' -or
        $env:OKX_DISPOSABLE_WINDOWS_RUNNER -cne '1') {
        throw 'S2_DISPOSABLE_REQUIRES_GITHUB_HOSTED_WINDOWS_OPT_IN'
    }
    if ($env:OKX_S2_EXPECTED_HEAD -cnotmatch '^[0-9a-f]{40}$') {
        throw 'S2_DISPOSABLE_EXPECTED_HEAD_NOT_IMMUTABLE_SHA'
    }
    $head = (& git rev-parse --verify HEAD).Trim()
    if ($LASTEXITCODE -ne 0 -or $head -cne $env:OKX_S2_EXPECTED_HEAD) {
        throw 'S2_DISPOSABLE_CHECKOUT_NOT_EXACT_PR_HEAD'
    }
    $tree = (& git rev-parse 'HEAD^{tree}').Trim()
    if ($LASTEXITCODE -ne 0 -or $tree -cnotmatch '^[0-9a-f]{40}$') {
        throw 'S2_DISPOSABLE_TREE_PROVENANCE_MISSING'
    }
    $receipt.source_head_sha = $head
    $receipt.source_tree_sha = $tree

    if (-not $root.StartsWith($runnerTemp + [IO.Path]::DirectorySeparatorChar,
            [StringComparison]::OrdinalIgnoreCase) -or
        $root -match '(?i)^[a-z]:\\okx(?:\\|$)' -or
        (Test-Path -LiteralPath $root)) {
        throw 'S2_DISPOSABLE_ROOT_NOT_FRESH_RUNNER_TEMP_CHILD'
    }
    New-Item -ItemType Directory -Path $root -ErrorAction Stop | Out-Null
    $marker = Join-Path $root '.okx-disposable-windows-runner'
    [IO.File]::WriteAllText($marker, "okx-s2-disposable-v1`n", [Text.UTF8Encoding]::new($false))
    $env:OKX_DISPOSABLE_WINDOWS_ROOT = $root

    # The LPAC test launches only this freshly built non-release worker.
    $buildLog = Join-Path $evidenceDir 'research-worker-build.log'
    & cargo build --locked -p okx-research-service --bin okx-research-service 2>&1 |
        Tee-Object -FilePath $buildLog
    $buildExit = $LASTEXITCODE
    $receipt.build_log_sha256 = (Get-FileHash -Algorithm SHA256 -LiteralPath $buildLog).Hash.ToLowerInvariant()
    if ($buildExit -ne 0) {
        throw "S2_DISPOSABLE_RESEARCH_WORKER_BUILD_FAILED:$buildExit"
    }
    $workerPath = Join-Path (Get-Location).Path 'target/debug/okx-research-service.exe'
    if (-not (Test-Path -LiteralPath $workerPath -PathType Leaf)) {
        throw 'S2_DISPOSABLE_WORKER_EXE_MISSING'
    }
    $env:OKX_DISPOSABLE_RESEARCH_EXE = (Resolve-Path -LiteralPath $workerPath).Path
    $receipt.research_worker_sha256 = (Get-FileHash -Algorithm SHA256 -LiteralPath $env:OKX_DISPOSABLE_RESEARCH_EXE).Hash.ToLowerInvariant()

    foreach ($testName in $tests) {
        $shortName = ($testName -split '::')[-1]
        $testLog = Join-Path $evidenceDir "$shortName.log"
        # One exact, explicitly ignored #[cfg(test)] in-crate Win32 proof per
        # cargo invocation. `--ignored` without `--exact` is not sufficient.
        & cargo test --locked -p okx-host-controller --lib $testName -- --ignored --exact --nocapture --test-threads 1 2>&1 |
            Tee-Object -FilePath $testLog
        $testExit = $LASTEXITCODE
        $log = Get-Content -LiteralPath $testLog -Raw
        $namedPass = $log -cmatch ('(?m)^test ' + [regex]::Escape($testName) + ' \.\.\. ok$')
        $onePassed = $log -cmatch 'test result: ok\. 1 passed; 0 failed; 0 ignored; 0 measured; [1-9][0-9]* filtered out;'
        # Printed only after the assertions inside the respective Win32 test;
        # the Cargo result line by itself is not a physical-witness receipt.
        $witness = "OKX_S2_WITNESS=$shortName"
        $hasWitness = $log.Contains($witness)
        $receipt.tests += [ordered]@{
            name = $testName
            exit_code = $testExit
            exact_named_pass = $namedPass
            exactly_one_passed = $onePassed
            native_witness = $hasWitness
            witness_marker = $witness
            log_file = "$shortName.log"
            log_sha256 = (Get-FileHash -Algorithm SHA256 -LiteralPath $testLog).Hash.ToLowerInvariant()
        }
        if ($testExit -ne 0 -or -not $namedPass -or -not $onePassed -or -not $hasWitness) {
            throw "S2_DISPOSABLE_EXACT_NATIVE_WITNESS_MISSING:$shortName"
        }
        $receipt.executed_exact_test_count += 1
    }
    if ($receipt.executed_exact_test_count -ne $tests.Count -or $tests.Count -ne 6) {
        throw 'S2_DISPOSABLE_NONZERO_FULL_SIX_TEST_SET_REQUIRED'
    }
    $receipt.result = 'PHYSICAL_SMOKE_EXECUTED_PENDING_INDEPENDENT_ACCEPTANCE'
    $exitCode = 0
    Write-Host "S2_DISPOSABLE_SMOKE=EXECUTED; EXACT_CASES=$($tests.Count); ACCEPTANCE=NOT_PROVEN"
} catch {
    $receipt.failure = $_.Exception.Message
    Write-Host "S2_DISPOSABLE_SMOKE=FAILED; REASON=$($receipt.failure)"
} finally {
    $receipt.completed_at_utc = [DateTime]::UtcNow.ToString('o')
    $receipt | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $receiptPath -Encoding utf8
    Write-Host "S2_DISPOSABLE_RECEIPT=$receiptPath"
    if (-not [string]::IsNullOrWhiteSpace($env:GITHUB_STEP_SUMMARY)) {
        @(
            '### S2 disposable Windows smoke (not a Ready/release proof)'
            "- Result: $($receipt.result)"
            "- Exact physical cases: $($receipt.executed_exact_test_count) / $($tests.Count)"
            "- Source head: $($receipt.source_head_sha)"
            "- Source tree: $($receipt.source_tree_sha)"
            '- Receipt: artifact okx-s2-disposable-windows-*'
            '- Acceptance remains NOT_PROVEN until independent review of physical evidence.'
        ) | Out-File -LiteralPath $env:GITHUB_STEP_SUMMARY -Append -Encoding utf8
    }
}
exit $exitCode
