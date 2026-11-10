# OPS-0: read-only Windows installation preflight. No secrets, SQL contents or mutations.
# Run on the OKX workstation using PowerShell 7:
#   pwsh -NoProfile -File .\scripts\windows-install-preflight.ps1
# This does NOT install, repair, start or stop anything.
[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
if (-not $IsWindows) { throw 'This preflight must run on the Windows workstation.' }

function Test-FilePresent {
    param([string]$Path)
    Test-Path -LiteralPath $Path -PathType Leaf
}

function Get-RootPresence {
    param([string]$Path)
    $item = Get-Item -LiteralPath $Path -Force -ErrorAction SilentlyContinue
    [ordered]@{
        exists = ($null -ne $item)
        directory = ($null -ne $item -and $item.PSIsContainer)
        reparse_point = ($null -ne $item -and [bool]($item.Attributes -band [IO.FileAttributes]::ReparsePoint))
    }
}

$control = 'C:\okx\control'
$runtime = 'C:\okx\runtime'
$expectedTaskPath = Join-Path $control 'okx-host-control.exe'
$legacyTaskPath = 'C:\okx-control\okx-host-control.exe'
$tasks = @(Get-ScheduledTask -TaskName 'iamaman11-okx-host-control' -ErrorAction SilentlyContinue)
$task = if ($tasks.Count -eq 1) { $tasks[0] } else { $null }
$actions = if ($null -ne $task) { @($task.Actions) } else { @() }
$action = if ($actions.Count -eq 1) { $actions[0] } else { $null }

$taskTarget = 'missing_or_ambiguous'
$taskTargetPresent = $false
$taskArgumentsRun = $false
if ($null -ne $action) {
    $target = [string]$action.Execute
    $taskTargetPresent = Test-FilePresent $target
    if ($target -ieq $expectedTaskPath) {
        $taskTarget = 'canonical'
    } elseif ($target -ieq $legacyTaskPath) {
        $taskTarget = 'legacy'
    } else {
        $taskTarget = 'other'
    }
    $taskArgumentsRun = ([string]$action.Arguments).Trim() -ceq 'run'
}

$processCounts = [ordered]@{}
foreach ($name in @('okx-host-control', 'okx-host-launcher', 'okx-agent')) {
    $processCounts[$name] = @(Get-Process -Name $name -ErrorAction SilentlyContinue).Count
}

[ordered]@{
    schema = 'okx.windows.install-preflight/v1'
    observed_at_utc = [DateTime]::UtcNow.ToString('o')
    roots = [ordered]@{
        product = Get-RootPresence 'C:\okx'
        legacy_control = Get-RootPresence 'C:\okx-control'
        legacy_runtime = Get-RootPresence 'C:\okx-runtime'
        control = Get-RootPresence $control
        runtime = Get-RootPresence $runtime
    }
    files = [ordered]@{
        canonical_controller = Test-FilePresent $expectedTaskPath
        canonical_launcher = Test-FilePresent (Join-Path $control 'okx-host-launcher.exe')
        canonical_agent = Test-FilePresent (Join-Path $runtime 'okx-agent.exe')
        legacy_controller = Test-FilePresent $legacyTaskPath
        legacy_agent = Test-FilePresent 'C:\okx-runtime\okx-agent.exe'
        active_controller_state = Test-FilePresent (Join-Path $control 'active-controller.json')
        migration_journal = Test-FilePresent 'C:\okx\maintenance\root-migration.json'
        product_root_is_checkout = Test-Path -LiteralPath 'C:\okx\.git'
    }
    scheduler = [ordered]@{
        task_count = $tasks.Count
        state = if ($null -ne $task) { [string]$task.State } else { 'absent_or_ambiguous' }
        action_count = $actions.Count
        action_kind = $taskTarget
        action_file_present = $taskTargetPresent
        arguments_are_run = $taskArgumentsRun
    }
    processes = $processCounts
    not_checked = @(
        'credentials', 'account_sql_contents', 'financial_ledger', 'central_riskbook',
        'github_release_identity', 'cloudflare_mcp', 'wss', 'trading'
    )
} | ConvertTo-Json -Depth 6
