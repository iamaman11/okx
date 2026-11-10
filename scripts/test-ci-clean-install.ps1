# CI-CLEAN: exercise the shipped Agent executable against isolated temporary roots.
# No GitHub CONTROL polling, credentials, venue calls, Scheduler or production SQL.
[CmdletBinding()]
param(
    [Parameter(Mandatory)]
    [string]$AgentPath,
    [Parameter(Mandatory)]
    [ValidatePattern('^[0-9a-f]{40}$')]
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
