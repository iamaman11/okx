# CI-CLEAN: exercise the shipped Agent executable against isolated temporary roots.
# No GitHub CONTROL polling, credentials, venue calls, Scheduler or production SQL.
[CmdletBinding()]
param(
    [Parameter(Mandatory)]
    [string]$AgentPath,
    [Parameter(Mandatory)]
    [string]$ControlPath,
    [Parameter(Mandatory)]
    [string]$LauncherPath,
    [Parameter(Mandatory)]
    [string]$ManifestPath,
    [Parameter(Mandatory)]
    [ValidatePattern('^[0-9a-f]{40} = 'Stop'
foreach ($source in @($AgentPath, $ControlPath, $LauncherPath, $ManifestPath)) {
    if (-not (Test-Path -LiteralPath $source -PathType Leaf)) { throw "RELEASE_TEST_INPUT_MISSING: $source" }
}

$fixture = Join-Path ([IO.Path]::GetTempPath()) ('okx-ci-clean-' + [guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $fixture | Out-Null
$installation = 'ci-clean-installation-01'
$operation = 'ci-clean-operation-01'

function Invoke-CleanInitialize {
    param([string]$Root, [string]$Operation, [bool]$ShouldPass)
    $output = & $AgentPath --root $Root initialize-installation --installation-id $installation --expected-release $SourceTree --operation-id $Operation 2>&1
    $exitCode = $LASTEXITCODE
    $message = ($output -join "`n")
    if ($ShouldPass) {
        if ($exitCode -ne 0) { throw "Unexpected clean initialize failure: $message" }
        $receipt = $message | ConvertFrom-Json
        if ($receipt.status -ne 'completed' -or $receipt.installation_id -cne $installation -or
            $receipt.operation_id -cne $Operation -or $receipt.release -cne $SourceTree) {
            throw 'CLEAN_INSTALL_RECEIPT_MISMATCH'
        }
    } else {
        if ($exitCode -eq 0) { throw 'CLEAN_INSTALL_UNEXPECTED_SUCCESS' }
        if ($message -notmatch 'INSTALLATION_ROOT_NOT_CLEAN') {
            throw "CLEAN_INSTALL_WRONG_ERROR: $message"
        }
    }
}

try {
    # The real CONTROL executable must report incomplete or unverified local
    # state without starting services, querying GitHub or interpreting SQL.
    $control = Join-Path $fixture 'control'
    $runtime = Join-Path $fixture 'runtime'
    New-Item -ItemType Directory -Path $control | Out-Null
    New-Item -ItemType Directory -Path $runtime | Out-Null
    $empty = & $ControlPath inspect --control-root $control --runtime-root $runtime | ConvertFrom-Json
    if ($LASTEXITCODE -ne 0 -or $empty.state -cne 'BLOCKED_RELEASE_FILES_MISSING' -or
        $empty.trading_permission -cne 'NEW_RISK_BLOCKED') {
        throw 'CLEAN_INSPECT_EMPTY_ROOT_NOT_BLOCKED'
    }
    Copy-Item -LiteralPath $AgentPath -Destination (Join-Path $runtime 'okx-agent.exe')
    Copy-Item -LiteralPath $ControlPath -Destination (Join-Path $control 'okx-host-control.exe')
    Copy-Item -LiteralPath $LauncherPath -Destination (Join-Path $control 'okx-host-launcher.exe')
    Copy-Item -LiteralPath $ManifestPath -Destination (Join-Path $control 'manifest.json')
    $manifest = Get-Content -LiteralPath $ManifestPath -Raw | ConvertFrom-Json
    if ($manifest.source_tree -cne $SourceTree -or [string]$manifest.repository_id -cne '1388071566') {
        throw 'CLEAN_INSPECT_MANIFEST_SOURCE_MISMATCH'
    }
    $initial = & $ControlPath inspect --control-root $control --runtime-root $runtime | ConvertFrom-Json
    if ($LASTEXITCODE -ne 0 -or $initial.local_release_integrity -cne 'LOCAL_HASHES_MATCH_NOT_REMOTE_ATTESTED' -or
        $initial.state -cne 'BLOCKED_AGENT_PROVENANCE' -or $initial.central_risk -cne 'UNKNOWN' -or
        $initial.safe_start -cne 'READ_ONLY_INSPECTION_ONLY') {
        throw 'CLEAN_INSPECT_RELEASE_OR_RISK_GATE_INVALID'
    }
    # A matching local manifest never grants risk; changed bytes must be refused.
    [IO.File]::WriteAllBytes((Join-Path $runtime 'okx-agent.exe'), [byte[]](1..64))
    $tampered = & $ControlPath inspect --control-root $control --runtime-root $runtime | ConvertFrom-Json
    if ($LASTEXITCODE -ne 0 -or $tampered.state -cne 'BLOCKED_RELEASE_NOT_VERIFIED' -or
        $tampered.trading_permission -cne 'NEW_RISK_BLOCKED') {
        throw 'CLEAN_INSPECT_TAMPERED_AGENT_NOT_BLOCKED'
    }

    # Fresh installation and repeat of the exact durable operation.
    $clean = Join-Path $fixture 'clean'
    New-Item -ItemType Directory -Path $clean | Out-Null
    Invoke-CleanInitialize -Root $clean -Operation $operation -ShouldPass $true
    $catalog = Join-Path $clean 'account-catalog.pb'
    $op = Join-Path $clean 'installation-operation.json'
    if (!(Test-Path -LiteralPath $catalog) -or !(Test-Path -LiteralPath $op)) {
        throw 'CLEAN_INSTALL_MISSING_DURABLE_IDENTITY'
    }
    $firstHash = (Get-FileHash -LiteralPath $catalog -Algorithm SHA256).Hash
    Invoke-CleanInitialize -Root $clean -Operation $operation -ShouldPass $true
    if ((Get-FileHash -LiteralPath $catalog -Algorithm SHA256).Hash -cne $firstHash) {
        throw 'CLEAN_INSTALL_RETRY_CHANGED_CATALOG'
    }

    # Old financial bytes must prohibit new local identity before any receipt.
    $dirty = Join-Path $fixture 'old-financial-state'
    $database = Join-Path $dirty 'accounts\prior\account.sqlite'
    New-Item -ItemType Directory -Path (Split-Path $database -Parent) -Force | Out-Null
    [IO.File]::WriteAllBytes($database, [byte[]](1..64))
    $sqlHash = (Get-FileHash -LiteralPath $database -Algorithm SHA256).Hash
    Invoke-CleanInitialize -Root $dirty -Operation $operation -ShouldPass $false
    if ((Test-Path -LiteralPath (Join-Path $dirty 'installation-operation.json')) -or
        (Test-Path -LiteralPath (Join-Path $dirty 'account-catalog.pb')) -or
        (Get-FileHash -LiteralPath $database -Algorithm SHA256).Hash -cne $sqlHash) {
        throw 'CLEAN_INSTALL_DIRTY_ROOT_MUTATED'
    }

    # A crash after operation-receipt publication must not bypass the guard.
    $interrupted = Join-Path $fixture 'interrupted'
    New-Item -ItemType Directory -Path (Join-Path $interrupted 'accounts') | Out-Null
    $receiptPath = Join-Path $interrupted 'installation-operation.json'
    @{ installation_id=$installation; operation_id=$operation; release=$SourceTree } |
        ConvertTo-Json -Compress | Set-Content -LiteralPath $receiptPath -Encoding utf8NoBOM
    $receiptHash = (Get-FileHash -LiteralPath $receiptPath -Algorithm SHA256).Hash
    Invoke-CleanInitialize -Root $interrupted -Operation $operation -ShouldPass $false
    if ((Get-FileHash -LiteralPath $receiptPath -Algorithm SHA256).Hash -cne $receiptHash -or
        (Test-Path -LiteralPath (Join-Path $interrupted 'account-catalog.pb'))) {
        throw 'CLEAN_INSTALL_INTERRUPTED_RETRY_MUTATED'
    }

    [ordered]@{
        schema = 'okx.ci-clean-install/v1'
        source_tree = $SourceTree
        clean_creation_and_exact_retry = 'PASS'
        legacy_sql_rejected_without_mutation = 'PASS'
        interrupted_dirty_retry_rejected = 'PASS'
        control_inspect_incomplete_release = 'PASS'
        control_inspect_missing_provenance = 'PASS'
        control_inspect_tampered_agent = 'PASS'
        trading = 'NOT_STARTED'
    } | ConvertTo-Json
} finally {
    # Only this isolated test-owned directory; never installed C:\okx data.
    if (Test-Path -LiteralPath $fixture) { [IO.Directory]::Delete($fixture, $true) }
}
# Expected negative native invocations leave LASTEXITCODE=1. All contracts above
# passed; return success explicitly rather than leaking that intentional failure.
exit 0
)]
    [string]$SourceTree
)
$ErrorActionPreference = 'Stop'
if (-not (Test-Path -LiteralPath $AgentPath -PathType Leaf)) { throw 'AGENT_BINARY_MISSING' }

$fixture = Join-Path ([IO.Path]::GetTempPath()) ('okx-ci-clean-' + [guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $fixture | Out-Null
$installation = 'ci-clean-installation-01'
$operation = 'ci-clean-operation-01'

function Invoke-CleanInitialize {
    param([string]$Root, [string]$Operation, [bool]$ShouldPass)
    $output = & $AgentPath --root $Root initialize-installation --installation-id $installation --expected-release $SourceTree --operation-id $Operation 2>&1
    $exitCode = $LASTEXITCODE
    $message = ($output -join "`n")
    if ($ShouldPass) {
        if ($exitCode -ne 0) { throw "Unexpected clean initialize failure: $message" }
        $receipt = $message | ConvertFrom-Json
        if ($receipt.status -ne 'completed' -or $receipt.installation_id -cne $installation -or
            $receipt.operation_id -cne $Operation -or $receipt.release -cne $SourceTree) {
            throw 'CLEAN_INSTALL_RECEIPT_MISMATCH'
        }
    } else {
        if ($exitCode -eq 0) { throw 'CLEAN_INSTALL_UNEXPECTED_SUCCESS' }
        if ($message -notmatch 'INSTALLATION_ROOT_NOT_CLEAN') {
            throw "CLEAN_INSTALL_WRONG_ERROR: $message"
        }
    }
}

try {
    # Fresh installation and repeat of the exact durable operation.
    $clean = Join-Path $fixture 'clean'
    New-Item -ItemType Directory -Path $clean | Out-Null
    Invoke-CleanInitialize -Root $clean -Operation $operation -ShouldPass $true
    $catalog = Join-Path $clean 'account-catalog.pb'
    $op = Join-Path $clean 'installation-operation.json'
    if (!(Test-Path -LiteralPath $catalog) -or !(Test-Path -LiteralPath $op)) {
        throw 'CLEAN_INSTALL_MISSING_DURABLE_IDENTITY'
    }
    $firstHash = (Get-FileHash -LiteralPath $catalog -Algorithm SHA256).Hash
    Invoke-CleanInitialize -Root $clean -Operation $operation -ShouldPass $true
    if ((Get-FileHash -LiteralPath $catalog -Algorithm SHA256).Hash -cne $firstHash) {
        throw 'CLEAN_INSTALL_RETRY_CHANGED_CATALOG'
    }

    # Old financial bytes must prohibit new local identity before any receipt.
    $dirty = Join-Path $fixture 'old-financial-state'
    $database = Join-Path $dirty 'accounts\prior\account.sqlite'
    New-Item -ItemType Directory -Path (Split-Path $database -Parent) -Force | Out-Null
    [IO.File]::WriteAllBytes($database, [byte[]](1..64))
    $sqlHash = (Get-FileHash -LiteralPath $database -Algorithm SHA256).Hash
    Invoke-CleanInitialize -Root $dirty -Operation $operation -ShouldPass $false
    if ((Test-Path -LiteralPath (Join-Path $dirty 'installation-operation.json')) -or
        (Test-Path -LiteralPath (Join-Path $dirty 'account-catalog.pb')) -or
        (Get-FileHash -LiteralPath $database -Algorithm SHA256).Hash -cne $sqlHash) {
        throw 'CLEAN_INSTALL_DIRTY_ROOT_MUTATED'
    }

    # A crash after operation-receipt publication must not bypass the guard.
    $interrupted = Join-Path $fixture 'interrupted'
    New-Item -ItemType Directory -Path (Join-Path $interrupted 'accounts') | Out-Null
    $receiptPath = Join-Path $interrupted 'installation-operation.json'
    @{ installation_id=$installation; operation_id=$operation; release=$SourceTree } |
        ConvertTo-Json -Compress | Set-Content -LiteralPath $receiptPath -Encoding utf8NoBOM
    $receiptHash = (Get-FileHash -LiteralPath $receiptPath -Algorithm SHA256).Hash
    Invoke-CleanInitialize -Root $interrupted -Operation $operation -ShouldPass $false
    if ((Get-FileHash -LiteralPath $receiptPath -Algorithm SHA256).Hash -cne $receiptHash -or
        (Test-Path -LiteralPath (Join-Path $interrupted 'account-catalog.pb'))) {
        throw 'CLEAN_INSTALL_INTERRUPTED_RETRY_MUTATED'
    }

    [ordered]@{
        schema = 'okx.ci-clean-install/v1'
        source_tree = $SourceTree
        clean_creation_and_exact_retry = 'PASS'
        legacy_sql_rejected_without_mutation = 'PASS'
        interrupted_dirty_retry_rejected = 'PASS'
        trading = 'NOT_STARTED'
    } | ConvertTo-Json
} finally {
    # Only this isolated test-owned directory; never installed C:\okx data.
    if (Test-Path -LiteralPath $fixture) { [IO.Directory]::Delete($fixture, $true) }
}
# Expected negative native invocations leave LASTEXITCODE=1. All contracts above
# passed; return success explicitly rather than leaking that intentional failure.
exit 0
)]
    [string]$SourceTree
)
$ErrorActionPreference = 'Stop'
foreach ($source in @($AgentPath, $ControlPath, $LauncherPath, $ManifestPath)) {
    if (-not (Test-Path -LiteralPath $source -PathType Leaf)) { throw "RELEASE_TEST_INPUT_MISSING: $source" }
}

$fixture = Join-Path ([IO.Path]::GetTempPath()) ('okx-ci-clean-' + [guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $fixture | Out-Null
$installation = 'ci-clean-installation-01'
$operation = 'ci-clean-operation-01'

function Invoke-CleanInitialize {
    param([string]$Root, [string]$Operation, [bool]$ShouldPass)
    $output = & $AgentPath --root $Root initialize-installation --installation-id $installation --expected-release $SourceTree --operation-id $Operation 2>&1
    $exitCode = $LASTEXITCODE
    $message = ($output -join "`n")
    if ($ShouldPass) {
        if ($exitCode -ne 0) { throw "Unexpected clean initialize failure: $message" }
        $receipt = $message | ConvertFrom-Json
        if ($receipt.status -ne 'completed' -or $receipt.installation_id -cne $installation -or
            $receipt.operation_id -cne $Operation -or $receipt.release -cne $SourceTree) {
            throw 'CLEAN_INSTALL_RECEIPT_MISMATCH'
        }
    } else {
        if ($exitCode -eq 0) { throw 'CLEAN_INSTALL_UNEXPECTED_SUCCESS' }
        if ($message -notmatch 'INSTALLATION_ROOT_NOT_CLEAN') {
            throw "CLEAN_INSTALL_WRONG_ERROR: $message"
        }
    }
}

try {
    # The real CONTROL executable must report incomplete or unverified local
    # state without starting services, querying GitHub or interpreting SQL.
    $control = Join-Path $fixture 'control'
    $runtime = Join-Path $fixture 'runtime'
    New-Item -ItemType Directory -Path $control | Out-Null
    New-Item -ItemType Directory -Path $runtime | Out-Null
    $empty = & $ControlPath inspect --control-root $control --runtime-root $runtime | ConvertFrom-Json
    if ($LASTEXITCODE -ne 0 -or $empty.state -cne 'BLOCKED_RELEASE_FILES_MISSING' -or
        $empty.trading_permission -cne 'NEW_RISK_BLOCKED') {
        throw 'CLEAN_INSPECT_EMPTY_ROOT_NOT_BLOCKED'
    }
    Copy-Item -LiteralPath $AgentPath -Destination (Join-Path $runtime 'okx-agent.exe')
    Copy-Item -LiteralPath $ControlPath -Destination (Join-Path $control 'okx-host-control.exe')
    Copy-Item -LiteralPath $LauncherPath -Destination (Join-Path $control 'okx-host-launcher.exe')
    Copy-Item -LiteralPath $ManifestPath -Destination (Join-Path $control 'manifest.json')
    $manifest = Get-Content -LiteralPath $ManifestPath -Raw | ConvertFrom-Json
    if ($manifest.source_tree -cne $SourceTree -or [string]$manifest.repository_id -cne '1388071566') {
        throw 'CLEAN_INSPECT_MANIFEST_SOURCE_MISMATCH'
    }
    $initial = & $ControlPath inspect --control-root $control --runtime-root $runtime | ConvertFrom-Json
    if ($LASTEXITCODE -ne 0 -or $initial.local_release_integrity -cne 'LOCAL_HASHES_MATCH_NOT_REMOTE_ATTESTED' -or
        $initial.state -cne 'BLOCKED_AGENT_PROVENANCE' -or $initial.central_risk -cne 'UNKNOWN' -or
        $initial.safe_start -cne 'READ_ONLY_INSPECTION_ONLY') {
        throw 'CLEAN_INSPECT_RELEASE_OR_RISK_GATE_INVALID'
    }
    # A matching local manifest never grants risk; changed bytes must be refused.
    [IO.File]::WriteAllBytes((Join-Path $runtime 'okx-agent.exe'), [byte[]](1..64))
    $tampered = & $ControlPath inspect --control-root $control --runtime-root $runtime | ConvertFrom-Json
    if ($LASTEXITCODE -ne 0 -or $tampered.state -cne 'BLOCKED_RELEASE_NOT_VERIFIED' -or
        $tampered.trading_permission -cne 'NEW_RISK_BLOCKED') {
        throw 'CLEAN_INSPECT_TAMPERED_AGENT_NOT_BLOCKED'
    }

    # Fresh installation and repeat of the exact durable operation.
    $clean = Join-Path $fixture 'clean'
    New-Item -ItemType Directory -Path $clean | Out-Null
    Invoke-CleanInitialize -Root $clean -Operation $operation -ShouldPass $true
    $catalog = Join-Path $clean 'account-catalog.pb'
    $op = Join-Path $clean 'installation-operation.json'
    if (!(Test-Path -LiteralPath $catalog) -or !(Test-Path -LiteralPath $op)) {
        throw 'CLEAN_INSTALL_MISSING_DURABLE_IDENTITY'
    }
    $firstHash = (Get-FileHash -LiteralPath $catalog -Algorithm SHA256).Hash
    Invoke-CleanInitialize -Root $clean -Operation $operation -ShouldPass $true
    if ((Get-FileHash -LiteralPath $catalog -Algorithm SHA256).Hash -cne $firstHash) {
        throw 'CLEAN_INSTALL_RETRY_CHANGED_CATALOG'
    }

    # Old financial bytes must prohibit new local identity before any receipt.
    $dirty = Join-Path $fixture 'old-financial-state'
    $database = Join-Path $dirty 'accounts\prior\account.sqlite'
    New-Item -ItemType Directory -Path (Split-Path $database -Parent) -Force | Out-Null
    [IO.File]::WriteAllBytes($database, [byte[]](1..64))
    $sqlHash = (Get-FileHash -LiteralPath $database -Algorithm SHA256).Hash
    Invoke-CleanInitialize -Root $dirty -Operation $operation -ShouldPass $false
    if ((Test-Path -LiteralPath (Join-Path $dirty 'installation-operation.json')) -or
        (Test-Path -LiteralPath (Join-Path $dirty 'account-catalog.pb')) -or
        (Get-FileHash -LiteralPath $database -Algorithm SHA256).Hash -cne $sqlHash) {
        throw 'CLEAN_INSTALL_DIRTY_ROOT_MUTATED'
    }

    # A crash after operation-receipt publication must not bypass the guard.
    $interrupted = Join-Path $fixture 'interrupted'
    New-Item -ItemType Directory -Path (Join-Path $interrupted 'accounts') | Out-Null
    $receiptPath = Join-Path $interrupted 'installation-operation.json'
    @{ installation_id=$installation; operation_id=$operation; release=$SourceTree } |
        ConvertTo-Json -Compress | Set-Content -LiteralPath $receiptPath -Encoding utf8NoBOM
    $receiptHash = (Get-FileHash -LiteralPath $receiptPath -Algorithm SHA256).Hash
    Invoke-CleanInitialize -Root $interrupted -Operation $operation -ShouldPass $false
    if ((Get-FileHash -LiteralPath $receiptPath -Algorithm SHA256).Hash -cne $receiptHash -or
        (Test-Path -LiteralPath (Join-Path $interrupted 'account-catalog.pb'))) {
        throw 'CLEAN_INSTALL_INTERRUPTED_RETRY_MUTATED'
    }

    [ordered]@{
        schema = 'okx.ci-clean-install/v1'
        source_tree = $SourceTree
        clean_creation_and_exact_retry = 'PASS'
        legacy_sql_rejected_without_mutation = 'PASS'
        interrupted_dirty_retry_rejected = 'PASS'
        control_inspect_incomplete_release = 'PASS'
        control_inspect_missing_provenance = 'PASS'
        control_inspect_tampered_agent = 'PASS'
        trading = 'NOT_STARTED'
    } | ConvertTo-Json
} finally {
    # Only this isolated test-owned directory; never installed C:\okx data.
    if (Test-Path -LiteralPath $fixture) { [IO.Directory]::Delete($fixture, $true) }
}
# Expected negative native invocations leave LASTEXITCODE=1. All contracts above
# passed; return success explicitly rather than leaking that intentional failure.
exit 0
)]
    [string]$SourceTree
)
$ErrorActionPreference = 'Stop'
if (-not (Test-Path -LiteralPath $AgentPath -PathType Leaf)) { throw 'AGENT_BINARY_MISSING' }

$fixture = Join-Path ([IO.Path]::GetTempPath()) ('okx-ci-clean-' + [guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $fixture | Out-Null
$installation = 'ci-clean-installation-01'
$operation = 'ci-clean-operation-01'

function Invoke-CleanInitialize {
    param([string]$Root, [string]$Operation, [bool]$ShouldPass)
    $output = & $AgentPath --root $Root initialize-installation --installation-id $installation --expected-release $SourceTree --operation-id $Operation 2>&1
    $exitCode = $LASTEXITCODE
    $message = ($output -join "`n")
    if ($ShouldPass) {
        if ($exitCode -ne 0) { throw "Unexpected clean initialize failure: $message" }
        $receipt = $message | ConvertFrom-Json
        if ($receipt.status -ne 'completed' -or $receipt.installation_id -cne $installation -or
            $receipt.operation_id -cne $Operation -or $receipt.release -cne $SourceTree) {
            throw 'CLEAN_INSTALL_RECEIPT_MISMATCH'
        }
    } else {
        if ($exitCode -eq 0) { throw 'CLEAN_INSTALL_UNEXPECTED_SUCCESS' }
        if ($message -notmatch 'INSTALLATION_ROOT_NOT_CLEAN') {
            throw "CLEAN_INSTALL_WRONG_ERROR: $message"
        }
    }
}

try {
    # Fresh installation and repeat of the exact durable operation.
    $clean = Join-Path $fixture 'clean'
    New-Item -ItemType Directory -Path $clean | Out-Null
    Invoke-CleanInitialize -Root $clean -Operation $operation -ShouldPass $true
    $catalog = Join-Path $clean 'account-catalog.pb'
    $op = Join-Path $clean 'installation-operation.json'
    if (!(Test-Path -LiteralPath $catalog) -or !(Test-Path -LiteralPath $op)) {
        throw 'CLEAN_INSTALL_MISSING_DURABLE_IDENTITY'
    }
    $firstHash = (Get-FileHash -LiteralPath $catalog -Algorithm SHA256).Hash
    Invoke-CleanInitialize -Root $clean -Operation $operation -ShouldPass $true
    if ((Get-FileHash -LiteralPath $catalog -Algorithm SHA256).Hash -cne $firstHash) {
        throw 'CLEAN_INSTALL_RETRY_CHANGED_CATALOG'
    }

    # Old financial bytes must prohibit new local identity before any receipt.
    $dirty = Join-Path $fixture 'old-financial-state'
    $database = Join-Path $dirty 'accounts\prior\account.sqlite'
    New-Item -ItemType Directory -Path (Split-Path $database -Parent) -Force | Out-Null
    [IO.File]::WriteAllBytes($database, [byte[]](1..64))
    $sqlHash = (Get-FileHash -LiteralPath $database -Algorithm SHA256).Hash
    Invoke-CleanInitialize -Root $dirty -Operation $operation -ShouldPass $false
    if ((Test-Path -LiteralPath (Join-Path $dirty 'installation-operation.json')) -or
        (Test-Path -LiteralPath (Join-Path $dirty 'account-catalog.pb')) -or
        (Get-FileHash -LiteralPath $database -Algorithm SHA256).Hash -cne $sqlHash) {
        throw 'CLEAN_INSTALL_DIRTY_ROOT_MUTATED'
    }

    # A crash after operation-receipt publication must not bypass the guard.
    $interrupted = Join-Path $fixture 'interrupted'
    New-Item -ItemType Directory -Path (Join-Path $interrupted 'accounts') | Out-Null
    $receiptPath = Join-Path $interrupted 'installation-operation.json'
    @{ installation_id=$installation; operation_id=$operation; release=$SourceTree } |
        ConvertTo-Json -Compress | Set-Content -LiteralPath $receiptPath -Encoding utf8NoBOM
    $receiptHash = (Get-FileHash -LiteralPath $receiptPath -Algorithm SHA256).Hash
    Invoke-CleanInitialize -Root $interrupted -Operation $operation -ShouldPass $false
    if ((Get-FileHash -LiteralPath $receiptPath -Algorithm SHA256).Hash -cne $receiptHash -or
        (Test-Path -LiteralPath (Join-Path $interrupted 'account-catalog.pb'))) {
        throw 'CLEAN_INSTALL_INTERRUPTED_RETRY_MUTATED'
    }

    [ordered]@{
        schema = 'okx.ci-clean-install/v1'
        source_tree = $SourceTree
        clean_creation_and_exact_retry = 'PASS'
        legacy_sql_rejected_without_mutation = 'PASS'
        interrupted_dirty_retry_rejected = 'PASS'
        trading = 'NOT_STARTED'
    } | ConvertTo-Json
} finally {
    # Only this isolated test-owned directory; never installed C:\okx data.
    if (Test-Path -LiteralPath $fixture) { [IO.Directory]::Delete($fixture, $true) }
}
# Expected negative native invocations leave LASTEXITCODE=1. All contracts above
# passed; return success explicitly rather than leaking that intentional failure.
exit 0
