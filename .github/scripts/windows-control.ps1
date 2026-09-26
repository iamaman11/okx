param(
    [Parameter(Mandatory = $true)]
    [ValidatePattern('^ctl_[A-Za-z0-9_-]{16,100}$')]
    [string]$RequestId,

    [Parameter(Mandatory = $true)]
    [ValidateSet(
        'status',
        'sync',
        'build_agent',
        'init_agent_identity',
        'agent_identity',
        'bootstrap_agent_github_token',
        'start_agent',
        'stop_agent',
        'restart_agent',
        'transport_status'
    )]
    [string]$Operation
)

$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'

$RepoRoot = 'C:\okx'
$BinaryPath = Join-Path $RepoRoot 'target\release\okx-agent.exe'
$RuntimeDir = Join-Path $RepoRoot '.runtime'
$MailboxIssue = 10
$ResultPath = Join-Path $env:RUNNER_TEMP 'okx-control-result.json'
$AllowedRemotes = @(
    'https://github.com/iamaman11/okx',
    'https://github.com/iamaman11/okx.git',
    'git@github.com:iamaman11/okx.git'
)

function Invoke-Git {
    param([Parameter(Mandatory = $true)][string[]]$Arguments)

    $output = & git -C $RepoRoot @Arguments 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw "git $($Arguments -join ' ') failed with exit code $LASTEXITCODE"
    }
    return (($output | ForEach-Object { "$_" }) -join "`n").Trim()
}

function Assert-CanonicalRepo {
    param(
        [switch]$RequireClean,
        [switch]$RequireMain
    )

    if (-not (Test-Path -LiteralPath (Join-Path $RepoRoot '.git'))) {
        throw "canonical repository was not found at $RepoRoot"
    }

    $remote = Invoke-Git -Arguments @('remote', 'get-url', 'origin')
    if ($remote -notin $AllowedRemotes) {
        throw 'origin does not identify iamaman11/okx'
    }

    if ($RequireClean) {
        $dirty = Invoke-Git -Arguments @('status', '--porcelain')
        if (-not [string]::IsNullOrWhiteSpace($dirty)) {
            throw 'canonical repository has local changes'
        }
    }

    if ($RequireMain) {
        $branch = Invoke-Git -Arguments @('rev-parse', '--abbrev-ref', 'HEAD')
        if ($branch -ne 'main') {
            throw "canonical repository is on branch '$branch', expected 'main'"
        }
    }
}

function Get-AgentProcesses {
    $processes = Get-CimInstance Win32_Process -Filter "Name = 'okx-agent.exe'" -ErrorAction SilentlyContinue
    return @(
        $processes | Where-Object {
            $_.CommandLine -and
            $_.CommandLine -match '--mailbox-issue\s+10(?:\s|$)'
        }
    )
}

function Get-StatusDetails {
    $repoPresent = Test-Path -LiteralPath (Join-Path $RepoRoot '.git')
    $details = [ordered]@{
        repo_root = $RepoRoot
        repo_present = $repoPresent
        repository = 'iamaman11/okx'
        head = $null
        branch = $null
        clean = $null
        origin = $null
        cargo_available = [bool](Get-Command cargo -ErrorAction SilentlyContinue)
        binary_present = Test-Path -LiteralPath $BinaryPath
        agent_running = $false
        agent_process_count = 0
    }

    if ($repoPresent) {
        try {
            $details.origin = Invoke-Git -Arguments @('remote', 'get-url', 'origin')
            $details.head = Invoke-Git -Arguments @('rev-parse', 'HEAD')
            $details.branch = Invoke-Git -Arguments @('rev-parse', '--abbrev-ref', 'HEAD')
            $details.clean = [string]::IsNullOrWhiteSpace(
                (Invoke-Git -Arguments @('status', '--porcelain'))
            )
        }
        catch {
            $details.repo_error = $_.Exception.Message
        }
    }

    $processes = @(Get-AgentProcesses)
    $details.agent_process_count = $processes.Count
    $details.agent_running = $processes.Count -gt 0
    return $details
}

function Invoke-Sync {
    Assert-CanonicalRepo -RequireClean

    Invoke-Git -Arguments @('fetch', '--prune', 'origin', 'main') | Out-Null
    $branch = Invoke-Git -Arguments @('rev-parse', '--abbrev-ref', 'HEAD')
    if ($branch -ne 'main') {
        Invoke-Git -Arguments @('switch', 'main') | Out-Null
    }
    Invoke-Git -Arguments @('merge', '--ff-only', 'origin/main') | Out-Null

    return [ordered]@{
        head = Invoke-Git -Arguments @('rev-parse', 'HEAD')
        branch = Invoke-Git -Arguments @('rev-parse', '--abbrev-ref', 'HEAD')
        clean = [string]::IsNullOrWhiteSpace(
            (Invoke-Git -Arguments @('status', '--porcelain'))
        )
    }
}

function Invoke-BuildAgent {
    Assert-CanonicalRepo -RequireClean -RequireMain

    Invoke-Git -Arguments @('fetch', '--prune', 'origin', 'main') | Out-Null
    $head = Invoke-Git -Arguments @('rev-parse', 'HEAD')
    $originMain = Invoke-Git -Arguments @('rev-parse', 'origin/main')
    if ($head -ne $originMain) {
        throw 'C:\okx main is not equal to origin/main; run sync first'
    }

    & cargo build --release -p okx-agent
    if ($LASTEXITCODE -ne 0) {
        throw "cargo build failed with exit code $LASTEXITCODE"
    }
    if (-not (Test-Path -LiteralPath $BinaryPath)) {
        throw 'release build completed without okx-agent.exe'
    }

    return [ordered]@{
        head = $head
        binary_present = $true
        binary_path = $BinaryPath
    }
}

function Read-AgentIdentity {
    if (-not (Test-Path -LiteralPath $BinaryPath)) {
        throw 'okx-agent.exe is not built'
    }

    $json = & $BinaryPath identity 2>$null
    if ($LASTEXITCODE -ne 0) {
        throw 'agent identity is not initialized or cannot be loaded'
    }
    return ($json -join "`n" | ConvertFrom-Json)
}

function Initialize-AgentIdentity {
    try {
        $existing = Read-AgentIdentity
        return [ordered]@{
            disposition = 'EXISTING'
            identity = $existing
        }
    }
    catch {
        $json = & $BinaryPath init-key 2>&1
        if ($LASTEXITCODE -ne 0) {
            throw "agent identity initialization failed with exit code $LASTEXITCODE"
        }
        return [ordered]@{
            disposition = 'CREATED'
            identity = ($json -join "`n" | ConvertFrom-Json)
        }
    }
}

function Bootstrap-AgentGithubToken {
    if (-not (Test-Path -LiteralPath $BinaryPath)) {
        throw 'okx-agent.exe is not built'
    }
    if (-not (Get-Command gh -ErrorAction SilentlyContinue)) {
        throw 'GitHub CLI is not available to the Windows runner account'
    }

    & gh auth status --hostname github.com *> $null
    if ($LASTEXITCODE -ne 0) {
        throw 'GitHub CLI is not authenticated for the Windows runner account'
    }

    $token = & gh auth token --hostname github.com
    if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace(($token -join ''))) {
        throw 'GitHub CLI did not provide an authenticated token'
    }

    try {
        ($token -join '') | & $BinaryPath set-github-token *> $null
        if ($LASTEXITCODE -ne 0) {
            throw 'okx-agent rejected the GitHub token bootstrap'
        }
    }
    finally {
        $token = $null
    }

    return [ordered]@{
        stored = $true
        destination = 'Windows Credential Manager'
    }
}

function Start-Agent {
    if (-not (Test-Path -LiteralPath $BinaryPath)) {
        throw 'okx-agent.exe is not built'
    }

    $existing = @(Get-AgentProcesses)
    if ($existing.Count -gt 0) {
        return [ordered]@{
            disposition = 'ALREADY_RUNNING'
            process_count = $existing.Count
        }
    }

    New-Item -ItemType Directory -Path $RuntimeDir -Force | Out-Null
    $stdoutPath = Join-Path $RuntimeDir 'okx-agent.stdout.log'
    $stderrPath = Join-Path $RuntimeDir 'okx-agent.stderr.log'

    $trackingId = $env:RUNNER_TRACKING_ID
    Remove-Item Env:RUNNER_TRACKING_ID -ErrorAction SilentlyContinue
    try {
        $process = Start-Process `
            -FilePath $BinaryPath `
            -ArgumentList @('run', '--mailbox-issue', "$MailboxIssue", '--poll-seconds', '2') `
            -WorkingDirectory $RepoRoot `
            -WindowStyle Hidden `
            -RedirectStandardOutput $stdoutPath `
            -RedirectStandardError $stderrPath `
            -PassThru
    }
    finally {
        if ($null -ne $trackingId) {
            $env:RUNNER_TRACKING_ID = $trackingId
        }
    }

    Set-Content -LiteralPath (Join-Path $RuntimeDir 'okx-agent.pid') -Value $process.Id -Encoding ascii

    return [ordered]@{
        disposition = 'STARTED'
        pid = $process.Id
        mailbox_issue = $MailboxIssue
    }
}

function Stop-Agent {
    $processes = @(Get-AgentProcesses)
    foreach ($process in $processes) {
        Stop-Process -Id $process.ProcessId -Force -ErrorAction Stop
    }

    Remove-Item -LiteralPath (Join-Path $RuntimeDir 'okx-agent.pid') -ErrorAction SilentlyContinue

    return [ordered]@{
        disposition = if ($processes.Count -eq 0) { 'ALREADY_STOPPED' } else { 'STOPPED' }
        stopped_process_count = $processes.Count
    }
}

$exitCode = 0
$details = $null
$errorText = $null

try {
    switch ($Operation) {
        'status' {
            $details = Get-StatusDetails
        }
        'sync' {
            $details = Invoke-Sync
        }
        'build_agent' {
            $details = Invoke-BuildAgent
        }
        'init_agent_identity' {
            $details = Initialize-AgentIdentity
        }
        'agent_identity' {
            $details = [ordered]@{ identity = Read-AgentIdentity }
        }
        'bootstrap_agent_github_token' {
            $details = Bootstrap-AgentGithubToken
        }
        'start_agent' {
            $details = Start-Agent
        }
        'stop_agent' {
            $details = Stop-Agent
        }
        'restart_agent' {
            $stopped = Stop-Agent
            $started = Start-Agent
            $details = [ordered]@{
                stop = $stopped
                start = $started
            }
        }
        'transport_status' {
            $details = [ordered]@{
                host = Get-StatusDetails
                identity = Read-AgentIdentity
            }
        }
    }
}
catch {
    $exitCode = 1
    $errorText = $_.Exception.Message
}

$result = [ordered]@{
    schema = 'okx.windows.control.result/v1'
    request_id = $RequestId
    operation = $Operation
    status = if ($exitCode -eq 0) { 'PASS' } else { 'FAIL' }
    observed_at = [DateTimeOffset]::UtcNow.ToString('o')
    details = $details
    error = $errorText
}

$result | ConvertTo-Json -Depth 8 -Compress |
    Set-Content -LiteralPath $ResultPath -Encoding utf8NoBOM

Write-Output "CONTROL_REQUEST_ID=$RequestId"
Write-Output "CONTROL_OPERATION=$Operation"
Write-Output "CONTROL_STATUS=$($result.status)"

exit $exitCode
