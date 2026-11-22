param([string]$Dist, [switch]$VerifyOnly, [string]$AcceptedCompositionSpec, [string]$ProofFile)
if (-not $VerifyOnly -and [string]::IsNullOrWhiteSpace($Dist)) { throw "DIST_REQUIRED" }
$ErrorActionPreference = 'Stop'
if (-not [string]::IsNullOrWhiteSpace($AcceptedCompositionSpec)) {
    if ($VerifyOnly) { throw 'COMPOSITION_CANNOT_BYPASS_RELEASE_SCOPE_CHECK' }
    $spec = Get-Content -LiteralPath $AcceptedCompositionSpec -Raw | ConvertFrom-Json
    if ($spec.schema -cne 'okx.537.accepted-research-composition/v1' -or
        $spec.installed_baseline -cnotmatch '^docs/acceptance/537/[A-Za-z0-9_.-]+\.json$') {
        throw 'INVALID_ACCEPTED_COMPOSITION_SPEC'
    }
    $repositoryRoot = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..')).Path
    $baselineFile = Join-Path $repositoryRoot $spec.installed_baseline
    $baseline = Get-Content -LiteralPath $baselineFile -Raw | ConvertFrom-Json
    $head = (& git -C $repositoryRoot rev-parse HEAD).Trim()
    $tree = (& git -C $repositoryRoot rev-parse 'HEAD^{tree}').Trim()
    if ($LASTEXITCODE -ne 0) { throw 'CANDIDATE_TREE_UNAVAILABLE' }
    if ([string]::IsNullOrWhiteSpace($ProofFile)) {
        $ProofFile = Join-Path (Split-Path -Parent $Dist) 'research-composition-proof.json'
    }
    & (Join-Path $PSScriptRoot 'prepare-accepted-research-composition.ps1') `
        -BaseRunId ([long]$baseline.run_id) -BaseArtifactId ([long]$baseline.artifact_id) `
        -ResearchRunId ([long]$spec.research_run_id) -ResearchArtifactId ([long]$spec.research_artifact_id) `
        -BaselineFile $baselineFile -CandidateHead $head -CandidateTree $tree -Dist $Dist -ProofFile $ProofFile
    return
}
if ($env:GITHUB_REPOSITORY -cne 'iamaman11/okx') { throw 'INVALID_REPOSITORY' }
function Api([string]$endpoint) {
    $json = & gh api $endpoint
    if ($LASTEXITCODE -ne 0) { throw 'GITHUB_BASE_LOOKUP_FAILED' }
    return ($json | ConvertFrom-Json)
}
$baseReleases = @(Api '/repos/iamaman11/okx/releases?per_page=100' | Where-Object {
    $_.tag_name -cmatch '^native-base-[0-9a-f]{40}$' -and -not $_.draft -and -not $_.prerelease
})
if ($baseReleases.Count -ne 1) { throw 'NATIVE_BASE_RELEASE_NOT_UNIQUE_OR_UNAVAILABLE' }
$baseRelease = $baseReleases[0]
$releaseAsset = $null
# Persistent accepted release: no dependence on Actions artifact expiry.
$mergeSha = $baseRelease.tag_name.Substring('native-base-'.Length)
if ($baseRelease.target_commitish -cne $mergeSha -or $baseRelease.immutable -notin @($true,$false) -or
    @($baseRelease.assets).Count -ne 3) { throw 'NATIVE_BASE_RELEASE_INVALID' }
foreach ($name in @('okx-windows-base.zip','published-origin.json','SHA256SUMS.txt')) {
    $asset = @($baseRelease.assets | Where-Object { $_.name -ceq $name })
    if ($asset.Count -ne 1 -or $asset[0].digest -cnotmatch '^sha256:[0-9a-f]{64}$' -or
        [long]$asset[0].size -le 0 -or [long]$asset[0].size -gt 268435456) { throw "NATIVE_BASE_ASSET_INVALID: $name" }
    if ($name -ceq 'okx-windows-base.zip') { $releaseAsset = $asset[0] }
}
$releaseDir = Join-Path $env:RUNNER_TEMP 'accepted-native-release'
New-Item -ItemType Directory -Path $releaseDir -Force | Out-Null
& gh release download $baseRelease.tag_name --repo iamaman11/okx --pattern 'published-origin.json' --dir $releaseDir
if ($LASTEXITCODE -ne 0) { throw 'RELEASE_ORIGIN_DOWNLOAD_FAILED' }
$originPath = Join-Path $releaseDir 'published-origin.json'
$originAsset = @($baseRelease.assets | Where-Object { $_.name -ceq 'published-origin.json' })[0]
if (('sha256:' + (Get-FileHash -LiteralPath $originPath -Algorithm SHA256).Hash.ToLowerInvariant()) -cne $originAsset.digest) { throw 'RELEASE_ORIGIN_DIGEST_MISMATCH' }
$origin = Get-Content -LiteralPath $originPath -Raw | ConvertFrom-Json
if ($origin.schema -cne 'okx.published-origin/v1' -or [string]$origin.repository_id -cne '1388071566' -or
    $origin.merged_main_sha -cne $mergeSha -or $origin.source_head_sha -cnotmatch '^[0-9a-f]{40}$' -or
    $origin.source_tree -cnotmatch '^[0-9a-f]{40}$' -or $origin.archive_sha256 -cnotmatch '^[0-9a-f]{64}$' -or
    "$($origin.run_id)" -cnotmatch '^[1-9][0-9]{0,18}$' -or "$($origin.artifact_id)" -cnotmatch '^[1-9][0-9]{0,18}$' -or
    @($origin.components.PSObject.Properties).Count -ne 4 -or $releaseAsset.digest -cne ('sha256:' + $origin.archive_sha256)) { throw 'RELEASE_ACCEPTED_PROVENANCE_INVALID' }
$config = [pscustomobject]@{ base_run_id=$origin.run_id; base_artifact_id=$origin.artifact_id }
$run = [pscustomobject]@{ head_sha=$origin.source_head_sha }
$artifact = [pscustomobject]@{ digest=$releaseAsset.digest }
$pulls = @(Api ("/repos/iamaman11/okx/commits/" + $run.head_sha + '/pulls'))
$merged = @($pulls | Where-Object { $_.head.sha -ceq $run.head_sha -and $_.base.ref -ceq 'main' -and $null -ne $_.merged_at -and $_.merge_commit_sha -cmatch '^[0-9a-f]{40}$' })
if ($merged.Count -ne 1) { throw 'BASE_PR_NOT_UNIQUELY_MERGED' }
$baseTree = (Api ("/repos/iamaman11/okx/git/commits/" + $run.head_sha)).tree.sha
if ($merged[0].merge_commit_sha -cne $mergeSha) { throw 'RELEASE_MERGED_PR_MISMATCH' }
$mergeSha = $merged[0].merge_commit_sha
$mergeTree = (Api ("/repos/iamaman11/okx/git/commits/" + $mergeSha)).tree.sha
if ($baseTree -cnotmatch '^[0-9a-f]{40}$' -or $mergeTree -cne $baseTree) { throw 'BASE_MERGED_TREE_MISMATCH' }
if ($origin.source_tree -cne $baseTree) { throw 'RELEASE_SOURCE_TREE_MISMATCH' }
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
& gh release download $baseRelease.tag_name --repo iamaman11/okx --pattern 'okx-windows-base.zip' --dir $releaseDir
if ($LASTEXITCODE -ne 0) { throw 'RELEASE_BASE_DOWNLOAD_FAILED' }
Copy-Item -LiteralPath (Join-Path $releaseDir 'okx-windows-base.zip') -Destination $zip
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
    $published = $origin.components.PSObject.Properties[$name].Value
    if ($null -eq $published -or $published.sha256 -cne $component.sha256 -or [long]$published.size -ne [long]$component.size -or
        $published.source_head_sha -cne $run.head_sha -or $published.source_tree -cne $baseTree -or
        [long]$published.run_id -ne [long]$config.base_run_id -or [long]$published.artifact_id -ne [long]$config.base_artifact_id -or
        [int]$published.protocol_version -ne 1 -or [int]$published.storage_version -ne 1) { throw "RELEASE_COMPONENT_ORIGIN_MISMATCH: $name" }
    if (-not $VerifyOnly -and $name -cne 'okx-research-worker.exe') { Copy-Item -LiteralPath $file -Destination (Join-Path $Dist $name) }
    $components[$name] = [ordered]@{ sha256=$component.sha256; size=[long]$component.size; source_head_sha=$run.head_sha; source_tree=$baseTree; run_id=[long]$config.base_run_id; artifact_id=[long]$config.base_artifact_id; protocol_version=1; storage_version=1 }
}
[ordered]@{ components=$components; base_run_id=$config.base_run_id; base_artifact_id=$config.base_artifact_id; base_head_sha=$run.head_sha; base_tree=$baseTree; base_archive_digest=$artifact.digest; base_merge_sha=$mergeSha; current_head_sha=(& git rev-parse HEAD).Trim(); scope=($scope | ConvertFrom-Json) } | ConvertTo-Json -Depth 12 | Set-Content -LiteralPath (Join-Path $env:RUNNER_TEMP 'research-release-proof.json') -Encoding utf8NoBOM
