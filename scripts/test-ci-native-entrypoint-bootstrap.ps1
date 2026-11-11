# CI-Bootstrap: exercise the production CONTROL "install" command on a
# separate GitHub-hosted, disposable Windows runner using the exact CI bundle.
# This is an entrypoint test, NOT an accepted-main release attestation, Agent
# service start, Scheduler enrollment or central RiskBook acceptance.
[CmdletBinding()]
param(
    [Parameter(Mandatory)]
    [string]$BundleDirectory,
    [Parameter(Mandatory)]
    [ValidatePattern('^[0-9a-f]{40}$')]
    [string]$ExpectedHead,
    [Parameter(Mandatory)]
    [ValidatePattern('^[0-9a-f]{40}$')]
    [string]$ExpectedTree
)
$ErrorActionPreference = 'Stop'
if (-not $IsWindows -or $env:GITHUB_ACTIONS -cne 'true' -or $env:CI -cne 'true') {
    throw 'CI_BOOTSTRAP_REQUIRES_DISPOSABLE_WINDOWS_ACTIONS_RUNNER'
}
$root = $env:SystemDrive + '\okx'
if (Test-Path -LiteralPath $root) {
    throw 'CI_BOOTSTRAP_TARGET_NOT_EMPTY_OR_UNKNOWN'
}

$manifestPath = Join-Path $BundleDirectory 'manifest.json'
if (-not (Test-Path -LiteralPath $manifestPath -PathType Leaf)) {
    throw 'CI_BOOTSTRAP_MANIFEST_MISSING'
}
$manifest = Get-Content -LiteralPath $manifestPath -Raw | ConvertFrom-Json
if ($manifest.schema -cne 'okx.windows.bundle/v1' -or
    [string]$manifest.repository_id -cne '1388071566' -or
    $manifest.source_head_sha -cne $ExpectedHead -or
    $manifest.source_tree -cne $ExpectedTree) {
    throw 'CI_BOOTSTRAP_RELEASE_IDENTITY_INVALID'
}
foreach ($name in @('okx-agent.exe', 'okx-host-control.exe', 'okx-host-launcher.exe')) {
    $path = Join-Path $BundleDirectory $name
    if (-not (Test-Path -LiteralPath $path -PathType Leaf)) {
        throw "CI_BOOTSTRAP_MISSING_BINARY: $name"
    }
    $expected = [string]$manifest.files.PSObject.Properties[$name].Value.sha256
    if ($expected -cnotmatch '^[0-9a-f]{64}$' -or
        (Get-FileHash -LiteralPath $path -Algorithm SHA256).Hash.ToLowerInvariant() -cne $expected) {
        throw "CI_BOOTSTRAP_BINARY_HASH_MISMATCH: $name"
    }
}

$controller = Join-Path $BundleDirectory 'okx-host-control.exe'
$entrypoint = Join-Path $root 'control/okx-host-control.exe'
$created = & $controller install | ConvertFrom-Json
if ($LASTEXITCODE -ne 0 -or $created.installed -ne $true -or
    $created.disposition -cne 'CREATED') {
    throw 'CI_BOOTSTRAP_NATIVE_CONTROLLER_NOT_CREATED'
}
$controllerHash = [string]$manifest.files.PSObject.Properties['okx-host-control.exe'].Value.sha256
if (-not (Test-Path -LiteralPath $entrypoint -PathType Leaf) -or
    (Get-FileHash -LiteralPath $entrypoint -Algorithm SHA256).Hash.ToLowerInvariant() -cne $controllerHash) {
    throw 'CI_BOOTSTRAP_INSTALLED_HASH_MISMATCH'
}
$inspection = & $entrypoint inspect | ConvertFrom-Json
if ($LASTEXITCODE -ne 0 -or
    $inspection.state -cne 'BLOCKED_RELEASE_FILES_MISSING' -or
    $inspection.trading_permission -cne 'NEW_RISK_BLOCKED' -or
    $inspection.central_risk -cne 'UNKNOWN' -or
    $inspection.safe_start -cne 'READ_ONLY_INSPECTION_ONLY') {
    throw 'CI_BOOTSTRAP_INCOMPLETE_RELEASE_NOT_FAIL_CLOSED'
}
$repeat = & $controller install | ConvertFrom-Json
if ($LASTEXITCODE -ne 0 -or $repeat.disposition -cne 'EXISTING') {
    throw 'CI_BOOTSTRAP_NOT_IDEMPOTENT'
}

# Mismatched already installed bytes must NEVER be overwritten by install.
# This negative probe changes only the disposable runner's copied entrypoint.
[IO.File]::WriteAllBytes($entrypoint, [byte[]](1..64))
$corruptHash = (Get-FileHash -LiteralPath $entrypoint -Algorithm SHA256).Hash
$PSNativeCommandUseErrorActionPreference = $false
$negativeOutput = & $controller install 2>&1
$negativeExit = $LASTEXITCODE
if ($negativeExit -eq 0 -or
    (Get-FileHash -LiteralPath $entrypoint -Algorithm SHA256).Hash -cne $corruptHash) {
    throw 'CI_BOOTSTRAP_CONFLICTING_ENTRYPOINT_OVERWRITTEN'
}
# The normal install remains immutable. Explicit expected-old-hash recovery
# is a separate native path; repeat must preserve the original old backup.
$oldHash = $corruptHash.ToLowerInvariant()
$wrongHash = '0' * 64
$negativeRecovery = & $controller install --recover-existing-sha256 $wrongHash 2>&1
if ($LASTEXITCODE -eq 0 -or
    (Get-FileHash -LiteralPath $entrypoint -Algorithm SHA256).Hash.ToLowerInvariant() -cne $oldHash) {
    throw 'CI_BOOTSTRAP_WRONG_OLD_SHA_NOT_REJECTED'
}
# Interrupted staging must not damage installed bytes or defeat safe retry.
[IO.File]::WriteAllBytes("$entrypoint.recovery-new", [byte[]](1..16))
[IO.File]::WriteAllBytes("$entrypoint.recovery-previous.staging", [byte[]](1..16))
$restored = & $controller install --recover-existing-sha256 $oldHash | ConvertFrom-Json
if ($LASTEXITCODE -ne 0 -or $restored.disposition -cne 'RECOVERED_LEGACY_CONTROLLER') {
    throw 'CI_BOOTSTRAP_NATIVE_LEGACY_RECOVERY_FAILED'
}
if ((Get-FileHash -LiteralPath $entrypoint -Algorithm SHA256).Hash.ToLowerInvariant() -cne $controllerHash) {
    throw 'CI_BOOTSTRAP_RECOVERED_HASH_MISMATCH'
}
$previous = "$entrypoint.recovery-previous"
if ((Get-FileHash -LiteralPath $previous -Algorithm SHA256).Hash.ToLowerInvariant() -cne $oldHash) {
    throw 'CI_BOOTSTRAP_LEGACY_BACKUP_MISSING'
}
$retryRecovery = & $controller install --recover-existing-sha256 $oldHash | ConvertFrom-Json
if ($LASTEXITCODE -ne 0 -or $retryRecovery.disposition -cne 'EXISTING_RECOVERED') {
    throw 'CI_BOOTSTRAP_RECOVERY_RETRY_NOT_IDEMPOTENT'
}
if (Test-Path -LiteralPath (Join-Path $root 'runtime')) {
    throw 'CI_BOOTSTRAP_UNEXPECTED_RUNTIME_CREATION'
}
if (Get-ScheduledTask -TaskName 'iamaman11-okx-host-control' -ErrorAction SilentlyContinue) {
    throw 'CI_BOOTSTRAP_UNEXPECTED_SCHEDULER_TASK'
}
[ordered]@{
    schema = 'okx.ci-native-entrypoint-bootstrap/v1'
    source_head_sha = $ExpectedHead
    source_tree = $ExpectedTree
    native_control_install = 'PASS'
    installed_controller_sha256 = $controllerHash
    exact_retry = 'PASS'
    preexisting_conflict_rejected_unchanged = 'PASS'
    wrong_recovery_hash_rejected = 'PASS'
    interrupted_staging_recovered = 'PASS'
    legacy_backup_preserved = 'PASS'
    recovery_exact_retry = 'PASS'
    incomplete_release_read_only_blocked = 'PASS'
    agent_launcher_installed = $false
    trading = 'NOT_STARTED'
    release_trust = 'CI_SOURCE_TREE_ONLY_NOT_ACCEPTED_MAIN'
} | ConvertTo-Json -Depth 3
exit 0
