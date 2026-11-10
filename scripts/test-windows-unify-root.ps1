$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot 'windows-unify-root.ps1')

function Expect-Failure {
    param([scriptblock]$Action, [string]$Name)
    $failed = $false
    try { & $Action } catch { $failed = $true }
    if (!$failed) { throw "Expected rejection: $Name" }
}

$fixture = Join-Path ([IO.Path]::GetTempPath()) ('okx-root-test-' + [Guid]::NewGuid().ToString('N'))
$product = Join-Path $fixture 'product'
$source = Join-Path $fixture 'legacy-runtime'
$target = Join-Path $product 'runtime'
New-Item -ItemType Directory -Path $product,(Join-Path $source 'accounts\one\backups'),(Join-Path $source 'empty') -Force | Out-Null
[IO.File]::WriteAllBytes((Join-Path $source 'accounts\one\account.sqlite'), [byte[]](0..255))
[IO.File]::WriteAllBytes((Join-Path $source 'accounts\one\account.sqlite-wal'), [byte[]](255..0))
[IO.File]::WriteAllText((Join-Path $source 'accounts\one\backups\backup.sqlite'), 'preserve-backup')
[IO.File]::WriteAllText((Join-Path $source '.hidden-state'), 'preserve-hidden-state')
$inventory = @(Get-RootInventory $source)
$move = { Move-VerifiedRoot -Source $source -Destination $target -ProductRoot $product -Inventory $inventory }

# Tampering after planning is rejected before moving either directory.
[IO.File]::AppendAllText((Join-Path $source '.hidden-state'), 'changed')
Expect-Failure $move 'changed source'
if (!(Test-Path -LiteralPath $source) -or (Test-Path -LiteralPath $target)) { throw 'Changed source was moved.' }
[IO.File]::WriteAllText((Join-Path $source '.hidden-state'), 'preserve-hidden-state')

# Occupied destinations are never merged or overwritten.
New-Item -ItemType Directory -Path $target | Out-Null
Expect-Failure $move 'occupied target'
Remove-Item -LiteralPath $target # Exact empty fixture directory; no recursive deletion.

& $move
if (Test-Path -LiteralPath $source) { throw 'Legacy source remains after same-volume rename.' }
Assert-InventoryEqual $inventory @(Get-RootInventory $target)
& $move # Resume after rename but before final journal update; no second move.

# A resumed migration must also reject an altered destination.
[IO.File]::AppendAllText((Join-Path $target '.hidden-state'), 'changed')
Expect-Failure $move 'changed resumed target'
Expect-Failure { Move-VerifiedRoot -Source $target -Destination $target -ProductRoot $product -Inventory $inventory } 'self move'
Expect-Failure { Move-VerifiedRoot -Source $source -Destination (Join-Path $fixture 'outside') -ProductRoot $product -Inventory $inventory } 'target outside product'

if ($IsWindows) {
    $linked = Join-Path $fixture 'linked'
    New-Item -ItemType Junction -Path $linked -Target $product | Out-Null
    Expect-Failure { Get-RootInventory $linked } 'linked root'
    $nested = Join-Path $product 'nested-link'
    New-Item -ItemType Junction -Path $nested -Target $fixture | Out-Null
    Expect-Failure { Get-RootInventory $product } 'nested reparse point'
    Remove-Item -LiteralPath $nested
    Remove-Item -LiteralPath $linked
}


# Keep the one-shot OPS-0 inventory tool parseable without executing it on CI hosts.
$tokens = $null
$parseErrors = $null
[System.Management.Automation.Language.Parser]::ParseFile(
    (Join-Path $PSScriptRoot 'windows-install-preflight.ps1'),
    [ref]$tokens, [ref]$parseErrors
) | Out-Null
if ($null -ne $parseErrors -and $parseErrors.Count -gt 0) {
    throw "Windows install preflight parse failed: $($parseErrors -join '; ')"
}

Write-Output "PASS root migration: exact SQL/WAL/backup/hidden bytes, empty directories, conflict/tamper rejection, resume, path/link guards. Fixture: $fixture"
