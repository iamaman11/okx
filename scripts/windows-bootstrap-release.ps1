# Recover a missing or stopped legacy CONTROL via one accepted main ReleaseSet.
# PowerShell 7. No Agent start, SQL access, credential read or trading.
# Usage: pwsh -NoProfile -File .\scripts\windows-bootstrap-release.ps1 -MainSha <accepted-full-main-sha> [-ExpectedLegacyControlSha256 <independently-verified-old-sha>]
[CmdletBinding()]
param(
    [Parameter(Mandatory)]
    [ValidatePattern('^[0-9a-f]{40}$')]
    [string]$MainSha,

    [ValidatePattern('^[0-9a-f]{64}$')]
    [string]$ExpectedLegacyControlSha256
)
$ErrorActionPreference = 'Stop'
if (-not $IsWindows) { throw 'Windows only.' }

function Get-GitHubJson {
    param([string]$ApiPath)
    Invoke-RestMethod -Uri ("https://api.github.com/repos/iamaman11/okx/" + $ApiPath) -Headers @{ 'User-Agent' = 'okx-verified-bootstrap'; 'Accept' = 'application/vnd.github+json' }
}
function Assert-PlainDirectory {
    param([string]$Path)
    $item = Get-Item -LiteralPath $Path -Force
    if (-not $item.PSIsContainer -or $item.Attributes -band [IO.FileAttributes]::ReparsePoint) {
        throw "Unsafe directory: $Path"
    }
}
function Assert-Sha256 {
    param([string]$Path, [string]$Expected)
    if ($Expected -cnotmatch '^[0-9a-f]{64}$') { throw 'Invalid expected SHA-256.' }
    $actual = (Get-FileHash -LiteralPath $Path -Algorithm SHA256).Hash.ToLowerInvariant()
    if ($actual -cne $Expected) { throw "SHA-256 mismatch: $([IO.Path]::GetFileName($Path))" }
}
function Get-ExpectedChecksum {
    param([string]$Text, [string]$File)
    $pattern = '^([0-9a-f]{64})  ' + [regex]::Escape($File) + '$'
    $hits = @($Text -split '\r?\n' | Where-Object { $_ -cmatch $pattern })
    if ($hits.Count -ne 1) { throw "Missing or duplicate checksum for $File" }
    return [regex]::Match($hits[0], $pattern).Groups[1].Value
}

# Only the existing verified migration may move legacy SQL, WAL and control state.
foreach ($legacy in @('C:\okx-control', 'C:\okx-runtime')) {
    if (Test-Path -LiteralPath $legacy) { throw "Legacy root remains; migrate first: $legacy" }
}
Assert-PlainDirectory 'C:\okx'
Assert-PlainDirectory 'C:\okx\control'
if (Test-Path -LiteralPath 'C:\okx\.git') { throw 'Product root is still a checkout.' }
$entrypoint = 'C:\okx\control\okx-host-control.exe'
$entrypointExists = Test-Path -LiteralPath $entrypoint
$previousEntrypointHash = $null
if ($entrypointExists) {
    $item = Get-Item -LiteralPath $entrypoint -Force
    if ($item.PSIsContainer -or $item.Attributes -band [IO.FileAttributes]::ReparsePoint) {
        throw 'Existing CONTROL entrypoint is not a plain file.'
    }
    $previousEntrypointHash = (Get-FileHash -LiteralPath $entrypoint -Algorithm SHA256).Hash.ToLowerInvariant()
}
if (Test-Path -LiteralPath 'C:\okx\control\active-controller.json') {
    throw 'Active launcher root exists; use native update/rollback instead.'
}
foreach ($name in @('okx-agent', 'okx-host-control', 'okx-host-launcher')) {
    if (@(Get-Process -Name $name -ErrorAction SilentlyContinue).Count -ne 0) {
        throw "Running OKX process: $name"
    }
}
$task = Get-ScheduledTask -TaskName 'iamaman11-okx-host-control' -ErrorAction SilentlyContinue
if ($null -ne $task -and [string]$task.State -ne 'Disabled') {
    throw 'Scheduler is not disabled; refusing to replace an active installation.'
}

# Bind the recovery package to the current canonical main, not to a Draft PR or old latest.
$main = Get-GitHubJson 'branches/main'
if ($main.commit.sha -cne $MainSha) { throw 'Selected SHA is not current main.' }
$mainGit = Get-GitHubJson ("git/commits/" + $MainSha)
$sourceTree = [string]$mainGit.tree.sha
if ($sourceTree -cnotmatch '^[0-9a-f]{40}$') { throw 'Invalid source tree.' }
$tag = 'production-recovery-' + $MainSha.Substring(0, 12)
$release = Get-GitHubJson ("releases/tags/" + $tag)
if ($release.draft -or $release.prerelease -or $release.tag_name -cne $tag -or $release.target_commitish -cne $MainSha) {
    throw 'No exact accepted main recovery release.'
}
$zipName = 'okx-windows-recovery-' + $MainSha + '.zip'
$assets = @($zipName, 'SHA256SUMS.txt')
foreach ($name in $assets) {
    $hits = @($release.assets | Where-Object { $_.name -ceq $name })
    if ($hits.Count -ne 1) { throw "Missing/duplicate release asset: $name" }
    $limit = if ($name -ceq $zipName) { 268435456 } else { 16384 }
    if ([long]$hits[0].size -le 0 -or [long]$hits[0].size -gt $limit) { throw 'Release asset size is invalid.' }
    $expectedUrl = "https://github.com/iamaman11/okx/releases/download/$tag/$name"
    if ($hits[0].browser_download_url -cne $expectedUrl) { throw 'Unexpected asset URL.' }
}

$temp = Join-Path ([IO.Path]::GetTempPath()) ('okx-bootstrap-' + [guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $temp | Out-Null
try {
    foreach ($name in $assets) {
        Invoke-WebRequest -Uri "https://github.com/iamaman11/okx/releases/download/$tag/$name" -OutFile (Join-Path $temp $name)
    }
    $checksums = Get-Content -LiteralPath (Join-Path $temp 'SHA256SUMS.txt') -Raw
    $zip = Join-Path $temp $zipName
    Assert-Sha256 $zip (Get-ExpectedChecksum $checksums $zipName)

    Add-Type -AssemblyName System.IO.Compression.ZipFile
    $archive = [IO.Compression.ZipFile]::OpenRead($zip)
    $expectedFiles = @('okx-agent.exe','okx-host-control.exe','okx-host-launcher.exe','Cargo.lock','manifest.json')
    try {
        $actualFiles = @($archive.Entries | ForEach-Object { $_.FullName })
        if ($actualFiles.Count -ne $expectedFiles.Count -or
            @($actualFiles | Where-Object { $_ -cnotin $expectedFiles }).Count -gt 0 -or
            @($expectedFiles | Where-Object { $_ -cnotin $actualFiles }).Count -gt 0) {
            throw 'Unexpected or unsafe recovery ZIP entries.'
        }
        if (@($archive.Entries | Where-Object { $_.Length -gt 134217728 }).Count -gt 0) {
            throw 'Oversized recovery ZIP entry.'
        }
    } finally { $archive.Dispose() }

    $extract = Join-Path $temp 'bundle'
    Expand-Archive -LiteralPath $zip -DestinationPath $extract
    foreach ($name in $expectedFiles) {
        Assert-Sha256 (Join-Path $extract $name) (Get-ExpectedChecksum $checksums $name)
    }
    $manifest = Get-Content -LiteralPath (Join-Path $extract 'manifest.json') -Raw | ConvertFrom-Json
    if ($manifest.schema -cne 'okx.windows.bundle/v1' -or [string]$manifest.repository_id -cne '1388071566' -or
        $manifest.source_tree -cne $sourceTree -or $manifest.source_head_sha -cnotmatch '^[0-9a-f]{40}$') {
        throw 'Manifest does not match accepted main.'
    }
    $prGit = Get-GitHubJson ("git/commits/" + $manifest.source_head_sha)
    if ($prGit.tree.sha -cne $sourceTree) { throw 'Tested PR and merged main trees differ.' }
    foreach ($name in @('okx-agent.exe','okx-host-control.exe','okx-host-launcher.exe')) {
        $declared = [string]$manifest.files.PSObject.Properties[$name].Value.sha256
        Assert-Sha256 (Join-Path $extract $name) $declared
    }

    # Recheck state; then call the existing native install COPY, not another updater.
    $freshMain = Get-GitHubJson 'branches/main'
    Assert-PlainDirectory 'C:\okx'
    Assert-PlainDirectory 'C:\okx\control'
    foreach ($name in @('okx-agent', 'okx-host-control', 'okx-host-launcher')) {
        if (@(Get-Process -Name $name -ErrorAction SilentlyContinue).Count -ne 0) { throw "Running OKX process: $name" }
    }
    $task = Get-ScheduledTask -TaskName 'iamaman11-okx-host-control' -ErrorAction SilentlyContinue
    if ($null -ne $task -and [string]$task.State -ne 'Disabled') { throw 'Scheduler state changed during verification.' }
    if ($freshMain.commit.sha -cne $MainSha -or
        (Test-Path -LiteralPath $entrypoint) -ne $entrypointExists -or
        (Test-Path -LiteralPath 'C:\okx\control\active-controller.json') -or
        (Test-Path -LiteralPath 'C:\okx-control') -or (Test-Path -LiteralPath 'C:\okx-runtime')) {
        throw 'Main or local installation state changed; refusing install.'
    }
    # Check both conflicting provenance files BEFORE altering the binary.
    $installedManifest = 'C:\okx\control\manifest.json'
    $previousManifest = 'C:\okx\control\manifest.json.recovery-previous'
    $manifestHash = Get-ExpectedChecksum $checksums 'manifest.json'
    $existingHash = $null
    if (Test-Path -LiteralPath $installedManifest) {
        $existingManifest = Get-Item -LiteralPath $installedManifest -Force
        if ($existingManifest.PSIsContainer -or
            $existingManifest.Attributes -band [IO.FileAttributes]::ReparsePoint) {
            throw 'Existing release manifest is not a plain file.'
        }
        $existingHash = (Get-FileHash -LiteralPath $installedManifest -Algorithm SHA256).Hash.ToLowerInvariant()
    }
    if ($existingHash -and $existingHash -cne $manifestHash -and
        (Test-Path -LiteralPath $previousManifest)) {
        $previousItem = Get-Item -LiteralPath $previousManifest -Force
        if ($previousItem.PSIsContainer -or
            $previousItem.Attributes -band [IO.FileAttributes]::ReparsePoint) {
            throw 'Existing manifest backup is not a plain file.'
        }
        Assert-Sha256 $previousManifest $existingHash
    }
    $controllerHash = [string]$manifest.files.PSObject.Properties['okx-host-control.exe'].Value.sha256
    # Explicit old-SHA authorization prevents adopting an unknown executable.
    if ($entrypointExists -and $previousEntrypointHash -cne $controllerHash -and
        $previousEntrypointHash -cne $ExpectedLegacyControlSha256) {
        throw 'Legacy CONTROL SHA is not independently authorized for recovery.'
    }
    $native = Join-Path $extract 'okx-host-control.exe'
    if ($entrypointExists) {
        $freshHash = (Get-FileHash -LiteralPath $entrypoint -Algorithm SHA256).Hash.ToLowerInvariant()
        if ($freshHash -cne $previousEntrypointHash) {
            throw 'Installed CONTROL changed during verification.'
        }
        if ($previousEntrypointHash -cne $controllerHash) {
            & $native install --recover-existing-sha256 $previousEntrypointHash
        } else {
            & $native install
        }
    } else {
        & $native install
    }
    if ($LASTEXITCODE -ne 0) { throw 'Native CONTROL install/recovery failed.' }
    Assert-Sha256 $entrypoint $controllerHash

    # Keep historical provenance; publish the independently verified new
    # manifest. Interruption is safe: re-running with the same main SHA
    # completes either the binary step or this manifest-only step.
    if ($existingHash -and $existingHash -cne $manifestHash) {
        if (-not (Test-Path -LiteralPath $previousManifest)) {
            [IO.File]::Copy($installedManifest, $previousManifest, $false)
            Assert-Sha256 $previousManifest $existingHash
        }
        $stageManifest = 'C:\okx\control\manifest.json.recovery-new'
        if (Test-Path -LiteralPath $stageManifest) {
            $stageItem = Get-Item -LiteralPath $stageManifest -Force
            if ($stageItem.PSIsContainer -or
                $stageItem.Attributes -band [IO.FileAttributes]::ReparsePoint) {
                throw 'Manifest staging path is not a plain file.'
            }
        }
        [IO.File]::Copy((Join-Path $extract 'manifest.json'), $stageManifest, $true)
        Assert-Sha256 $stageManifest $manifestHash
        [IO.File]::Move($stageManifest, $installedManifest, $true)
    } elseif (-not $existingHash) {
        [IO.File]::Copy((Join-Path $extract 'manifest.json'), $installedManifest, $false)
    }
    Assert-Sha256 $installedManifest $manifestHash
    [ordered]@{
        schema = 'okx.windows.bootstrap-result/v1'
        state = 'CONTROLLER_ENTRYPOINT_RESTORED_NOT_RUNNING'
        main_sha = $MainSha
        source_tree = $sourceTree
        controller_sha256 = $controllerHash
        manifest_sha256 = $manifestHash
        next = 'Use native CONTROL to install the launcher/autostart/Agent; then verify MCP.'
        financial_state = 'NOT_TOUCHED_OR_VERIFIED'
        cloudflare = 'NOT_DEPLOYED_OR_VERIFIED'
    } | ConvertTo-Json -Depth 4
} finally {
    # Only this invocation's unique staging directory is removed; never product data.
    if (Test-Path -LiteralPath $temp) { [IO.Directory]::Delete($temp, $true) }
}
