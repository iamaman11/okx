[CmdletBinding()]
param([switch]$Apply)

$ErrorActionPreference = 'Stop'

function Assert-PlainDirectory {
    param([Parameter(Mandatory)][string]$Path)
    $item = Get-Item -LiteralPath $Path -Force
    if (!$item.PSIsContainer -or ($item.Attributes -band [IO.FileAttributes]::ReparsePoint)) {
        throw "Expected a real directory, not a link: $Path"
    }
}

function Get-RootInventory {
    param([Parameter(Mandatory)][string]$Path)
    Assert-PlainDirectory $Path
    $root = [IO.Path]::GetFullPath($Path).TrimEnd('\', '/')
    # Walk without following links. Reject links before descending into a directory.
    $pending = [Collections.Generic.Stack[string]]::new()
    $pending.Push($root)
    $rows = [Collections.Generic.List[object]]::new()
    while ($pending.Count -gt 0) {
        foreach ($item in Get-ChildItem -LiteralPath $pending.Pop() -Force) {
            if ($item.Attributes -band [IO.FileAttributes]::ReparsePoint) {
                throw "Refusing a reparse point: $($item.FullName)"
            }
            $relative = [IO.Path]::GetRelativePath($root, $item.FullName)
            if ($item.PSIsContainer) {
                $rows.Add([pscustomobject]@{path=$relative;kind='directory';length=0;sha256=''})
                $pending.Push($item.FullName)
            } else {
                $hash = (Get-FileHash -LiteralPath $item.FullName -Algorithm SHA256).Hash.ToLowerInvariant()
                $rows.Add([pscustomobject]@{path=$relative;kind='file';length=$item.Length;sha256=$hash})
            }
        }
    }
    @($rows | Sort-Object path)
}

function Assert-InventoryEqual {
    param([AllowEmptyCollection()][object[]]$Expected, [AllowEmptyCollection()][object[]]$Actual)
    $left = ConvertTo-Json -InputObject @($Expected) -Depth 4 -Compress
    $right = ConvertTo-Json -InputObject @($Actual) -Depth 4 -Compress
    if ($left -cne $right) { throw 'Directory inventory differs; no cleanup or activation is permitted.' }
}

function Move-VerifiedRoot {
    param(
        [Parameter(Mandatory)][string]$Source,
        [Parameter(Mandatory)][string]$Destination,
        [Parameter(Mandatory)][string]$ProductRoot,
        [AllowEmptyCollection()][object[]]$Inventory
    )
    $sourcePath = [IO.Path]::GetFullPath($Source).TrimEnd('\', '/')
    $targetPath = [IO.Path]::GetFullPath($Destination).TrimEnd('\', '/')
    $productPath = [IO.Path]::GetFullPath($ProductRoot).TrimEnd('\', '/')
    if ([IO.Path]::GetDirectoryName($targetPath) -ine $productPath -or
        $sourcePath -ieq $targetPath -or
        $sourcePath.StartsWith($productPath + [IO.Path]::DirectorySeparatorChar, [StringComparison]::OrdinalIgnoreCase) -or
        [IO.Path]::GetPathRoot($sourcePath) -ine [IO.Path]::GetPathRoot($targetPath)) {
        throw 'Migration paths must be distinct, on the same volume, with a direct product child destination.'
    }
    Assert-PlainDirectory $productPath
    if (Test-Path -LiteralPath $sourcePath) {
        if (Test-Path -LiteralPath $targetPath) { throw 'Both source and target exist; refusing to merge or overwrite.' }
        Assert-InventoryEqual $Inventory @(Get-RootInventory $sourcePath)
        # Same-volume directory rename preserves SQL, WAL, backups, ACLs and hidden files.
        Move-Item -LiteralPath $sourcePath -Destination $targetPath
    }
    if (!(Test-Path -LiteralPath $targetPath)) { throw 'Neither source nor completed target exists.' }
    Assert-InventoryEqual $Inventory @(Get-RootInventory $targetPath)
}

function Save-MigrationJournal {
    param([string]$Path, [object]$Journal)
    $temporary = $Path + '.tmp'
    $Journal | ConvertTo-Json -Depth 9 | Set-Content -LiteralPath $temporary -Encoding utf8NoBOM
    Move-Item -LiteralPath $temporary -Destination $Path -Force
}

function Assert-ApplicationStopped {
    $processes = @(Get-CimInstance Win32_Process -Filter "Name='okx-agent.exe' OR Name='okx-host-control.exe' OR Name='okx-host-launcher.exe'")
    if ($processes.Count -gt 0) { throw 'Stop all installed OKX processes before migration; this script never force-kills them.' }
}

function Invoke-CanonicalRootMigration {
    param([bool]$Commit)
    $product = 'C:\okx'
    $maintenance = 'C:\okx\maintenance'
    $journalPath = Join-Path $maintenance 'root-migration.json'
    $mappings = @(
        @{source='C:\okx-control';destination='C:\okx\control'},
        @{source='C:\okx-runtime';destination='C:\okx\runtime'}
    )
    Assert-ApplicationStopped
    Assert-PlainDirectory $product
    # Product contents from a prior checkout may never be overwritten.
    if (Test-Path -LiteralPath (Join-Path $product '.git')) { throw 'C:\okx is still a checkout; preserve it before migration.' }
    $allowed = @('maintenance','control','runtime')
    foreach ($entry in Get-ChildItem -LiteralPath $product -Force) {
        if ($entry.Name -notin $allowed) { throw "Unexpected product-root entry: $($entry.Name)" }
        if ($entry.Attributes -band [IO.FileAttributes]::ReparsePoint) { throw 'Product children must not be reparse points.' }
    }
    $task = Get-ScheduledTask -TaskName 'iamaman11-okx-host-control' -ErrorAction SilentlyContinue
    if (Test-Path -LiteralPath $journalPath) {
        $journal = Get-Content -LiteralPath $journalPath -Raw | ConvertFrom-Json
        if ($journal.schema -ne 'okx.windows.root-migration/v1' -or @($journal.roots).Count -ne 2) {
            throw 'Invalid migration journal.'
        }
        for ($i=0; $i -lt 2; $i++) {
            if ($journal.roots[$i].source -cne $mappings[$i].source -or $journal.roots[$i].destination -cne $mappings[$i].destination) {
                throw 'Migration journal paths do not match the fixed canonical mapping.'
            }
        }
    } else {
        $roots = foreach ($mapping in $mappings) {
            if (Test-Path -LiteralPath $mapping.destination) { throw 'Occupied destination without a migration journal.' }
            $inventory = @(Get-RootInventory $mapping.source)
            [pscustomobject]@{source=$mapping.source;destination=$mapping.destination;inventory=$inventory}
        }
        $journal = [pscustomobject]@{
            schema='okx.windows.root-migration/v1';state='PLANNED';created_at=[DateTime]::UtcNow.ToString('o')
            task_was_enabled=($null -ne $task -and [string]$task.State -ne 'Disabled');roots=@($roots)
        }
    }
    # A pending update may contain absolute paths or an exiting-process handoff.
    foreach ($root in @('C:\okx-control','C:\okx\control')) {
        foreach ($relative in @('activation\pending.json','staged-controller.json','update\pending-controller-update.json','root-migration.json')) {
            if (Test-Path -LiteralPath (Join-Path $root $relative)) { throw "Resolve pending CONTROL state before migration: $relative" }
        }
    }
    if (!$Commit) {
        return [pscustomobject]@{state='PLAN_ONLY';roots=@($journal.roots | Select-Object source,destination);application_started=$false}
    }
    New-Item -ItemType Directory -Path $maintenance -Force | Out-Null
    if ($task) {
        $backup = Join-Path $maintenance 'previous-control-task.xml'
        if (!(Test-Path -LiteralPath $backup)) {
            $task | Export-ScheduledTask | Set-Content -LiteralPath $backup -Encoding utf8NoBOM
        }
        $task | Disable-ScheduledTask | Out-Null
    }
    Assert-ApplicationStopped
    Save-MigrationJournal $journalPath $journal
    foreach ($row in $journal.roots) {
        Assert-ApplicationStopped
        Move-VerifiedRoot -Source $row.source -Destination $row.destination -ProductRoot $product -Inventory @($row.inventory)
    }
    $journal.state = 'DATA_VERIFIED_AWAITING_GITHUB_INSTALL'
    Save-MigrationJournal $journalPath $journal
    [pscustomobject]@{
        state=$journal.state;product_root=$product;roots=@($journal.roots | Select-Object source,destination)
        files_verified=@($journal.roots.inventory | Where-Object kind -eq 'file').Count
        scheduler='DISABLED';application_started=$false;journal=$journalPath
    }
}

if ($MyInvocation.InvocationName -ne '.') {
    Invoke-CanonicalRootMigration -Commit $Apply.IsPresent | ConvertTo-Json -Depth 5
}
