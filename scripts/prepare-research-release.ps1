param([string]$Dist, [switch]$VerifyOnly)
if (-not $VerifyOnly -and [string]::IsNullOrWhiteSpace($Dist)) { throw "DIST_REQUIRED" }
$ErrorActionPreference = 'Stop'
$configPath = '.github/research-release-base.json'
if (-not (Test-Path -LiteralPath $configPath)) { throw 'RESEARCH_BASE_CONFIG_MISSING' }
$config = Get-Content -LiteralPath $configPath -Raw | ConvertFrom-Json
if (@($config.PSObject.Properties).Count -ne 3 -or $config.schema -cne 'okx.research-release-base/v1' -or
    "$($config.base_run_id)" -cnotmatch '^[1-9][0-9]{0,18}$' -or "$($config.base_artifact_id)" -cnotmatch '^[1-9][0-9]{0,18}$') { throw 'INVALID_RESEARCH_BASE_CONFIG' }
if ($env:GITHUB_REPOSITORY -cne 'iamaman11/okx') { throw 'INVALID_REPOSITORY' }
function Api([string]$endpoint) {
    $json = & gh api $endpoint
    if ($LASTEXITCODE -ne 0) { throw 'GITHUB_BASE_LOOKUP_FAILED' }
    return ($json | ConvertFrom-Json)
}
$run = Api ("/repos/iamaman11/okx/actions/runs/" + $config.base_run_id)
if ($run.name -cne 'CI' -or $run.event -cne 'pull_request' -or $run.status -cne 'completed' -or $run.conclusion -cne 'success' -or
    $run.head_sha -cnotmatch '^[0-9a-f]{40}$' -or [string]$run.repository.id -cne '1388071566') { throw 'BASE_RUN_NOT_ACCEPTED' }
$artifact = Api ("/repos/iamaman11/okx/actions/artifacts/" + $config.base_artifact_id)
if ($artifact.expired -or $artifact.name -cne ("okx-windows-bundle-" + $run.head_sha) -or
    [long]$artifact.workflow_run.id -ne [long]$config.base_run_id -or $artifact.digest -cnotmatch '^sha256:[0-9a-f]{64}$') { throw 'BASE_ARTIFACT_NOT_ACCEPTED' }
$pulls = @(Api ("/repos/iamaman11/okx/commits/" + $run.head_sha + '/pulls'))
$merged = @($pulls | Where-Object { $_.head.sha -ceq $run.head_sha -and $_.base.ref -ceq 'main' -and $null -ne $_.merged_at -and $_.merge_commit_sha -cmatch '^[0-9a-f]{40}$' })
if ($merged.Count -ne 1) { throw 'BASE_PR_NOT_UNIQUELY_MERGED' }
$baseTree = (Api ("/repos/iamaman11/okx/git/commits/" + $run.head_sha)).tree.sha
$mergeSha = $merged[0].merge_commit_sha
$mergeTree = (Api ("/repos/iamaman11/okx/git/commits/" + $mergeSha)).tree.sha
if ($baseTree -cnotmatch '^[0-9a-f]{40}$' -or $mergeTree -cne $baseTree) { throw 'BASE_MERGED_TREE_MISMATCH' }
$event = Get-Content -LiteralPath $env:GITHUB_EVENT_PATH -Raw | ConvertFrom-Json
if ($env:GITHUB_EVENT_NAME -ceq 'pull_request') {
    $baseSha = [string]$event.pull_request.base.sha
} elseif ($env:GITHUB_EVENT_NAME -ceq 'push' -and $event.ref -ceq 'refs/heads/main' -and -not $event.forced) {
    $baseSha = [string]$event.before
} else { throw 'CURRENT_EVENT_UNSUPPORTED' }
if ($baseSha -cnotmatch '^[0-9a-f]{40}$' -or $baseSha -ceq '0000000000000000000000000000000000000000') { throw 'CURRENT_BASE_MISSING' }
$comparison = Api ("/repos/iamaman11/okx/compare/$mergeSha...$baseSha")
if ($comparison.status -cnotin @('ahead','identical')) { throw 'BASE_NOT_ON_MAIN_HISTORY' }
& git fetch --no-tags origin $mergeSha
if ($LASTEXITCODE -ne 0) { throw 'BASE_FETCH_FAILED' }
$changed = @(& git -c core.quotepath=false diff --name-only --no-renames $mergeSha HEAD)
if ($LASTEXITCODE -ne 0) { throw 'BASE_DIFF_FAILED' }
$metadataFile = Join-Path $env:RUNNER_TEMP 'research-release-metadata.json'
$changedFile = Join-Path $env:RUNNER_TEMP 'research-release-changed.json'
& cargo metadata --locked --format-version 1 | Set-Content -LiteralPath $metadataFile -Encoding utf8NoBOM
if ($LASTEXITCODE -ne 0) { throw 'CARGO_METADATA_FAILED' }
ConvertTo-Json -InputObject $changed -Compress | Set-Content -LiteralPath $changedFile -Encoding utf8NoBOM
& node scripts/test-research-release-scope.cjs
if ($LASTEXITCODE -ne 0) { throw 'SCOPE_NEGATIVE_TESTS_FAILED' }
$scope = & node scripts/research-release-scope.cjs $metadataFile $changedFile
if ($LASTEXITCODE -ne 0) { throw 'RESEARCH_DIFF_NOT_ISOLATED' }
$zip = Join-Path $env:RUNNER_TEMP 'research-release-base.zip'
$gh = (Get-Command gh -ErrorAction Stop).Source
& node scripts/download-research-base.cjs $gh ("/repos/iamaman11/okx/actions/artifacts/" + $config.base_artifact_id + '/zip') $zip
if ($LASTEXITCODE -ne 0) { throw 'BASE_DOWNLOAD_FAILED' }
if (('sha256:' + (Get-FileHash -LiteralPath $zip -Algorithm SHA256).Hash.ToLowerInvariant()) -cne $artifact.digest) { throw 'BASE_ARCHIVE_DIGEST_MISMATCH' }
Add-Type -AssemblyName System.IO.Compression.FileSystem
$archive = [IO.Compression.ZipFile]::OpenRead($zip)
$names = @('manifest.json','Cargo.lock','okx-agent.exe','okx-host-control.exe','okx-host-launcher.exe','okx-research-worker.exe')
try {
    $actual = @($archive.Entries | ForEach-Object FullName)
    if ($actual.Count -ne 6 -or @($names | Where-Object { $_ -cnotin $actual }).Count -ne 0 -or @($actual | Where-Object { $_ -cnotin $names }).Count -ne 0 -or @($archive.Entries | Where-Object { $_.Length -gt 134217728 }).Count -ne 0) { throw 'BASE_ARCHIVE_ENTRIES_INVALID' }
} finally { $archive.Dispose() }
$extract = Join-Path $env:RUNNER_TEMP 'research-release-base'
Expand-Archive -LiteralPath $zip -DestinationPath $extract
$manifest = Get-Content -LiteralPath (Join-Path $extract 'manifest.json') -Raw | ConvertFrom-Json
if ($manifest.schema -cne 'okx.windows.bundle/v2' -or [string]$manifest.repository_id -cne '1388071566' -or $manifest.source_head_sha -cne $run.head_sha -or $manifest.source_tree -cne $baseTree -or @($manifest.components.PSObject.Properties).Count -ne 4) { throw 'BASE_MANIFEST_INVALID' }
$components = [ordered]@{}
foreach ($name in @('okx-agent.exe','okx-host-control.exe','okx-host-launcher.exe','okx-research-worker.exe')) {
    $component = $manifest.components.PSObject.Properties[$name].Value
    $file = Join-Path $extract $name
    if ($null -eq $component -or $component.source_head_sha -cne $run.head_sha -or $component.source_tree -cne $baseTree -or [long]$component.run_id -ne 0 -or [long]$component.artifact_id -ne 0 -or [int]$component.protocol_version -ne 1 -or [int]$component.storage_version -ne 1 -or ([long]$component.size -le 0) -or [long]$component.size -ne (Get-Item -LiteralPath $file).Length -or $component.sha256 -cnotmatch '^[0-9a-f]{64}$' -or (Get-FileHash -LiteralPath $file -Algorithm SHA256).Hash.ToLowerInvariant() -cne $component.sha256) { throw "BASE_COMPONENT_INVALID: $name" }
    if (-not $VerifyOnly -and $name -cne 'okx-research-worker.exe') { Copy-Item -LiteralPath $file -Destination (Join-Path $Dist $name) }
    $components[$name] = [ordered]@{ sha256=$component.sha256; size=[long]$component.size; source_head_sha=$run.head_sha; source_tree=$baseTree; run_id=[long]$config.base_run_id; artifact_id=[long]$config.base_artifact_id; protocol_version=1; storage_version=1 }
}
[ordered]@{ components=$components; base_run_id=$config.base_run_id; base_artifact_id=$config.base_artifact_id; base_head_sha=$run.head_sha; base_tree=$baseTree; base_archive_digest=$artifact.digest; base_merge_sha=$mergeSha; current_head_sha=(& git rev-parse HEAD).Trim(); scope=($scope | ConvertFrom-Json) } | ConvertTo-Json -Depth 12 | Set-Content -LiteralPath (Join-Path $env:RUNNER_TEMP 'research-release-proof.json') -Encoding utf8NoBOM
