param(
    [Parameter(Mandatory)][long]$BaseRunId,
    [Parameter(Mandatory)][long]$BaseArtifactId,
    [Parameter(Mandatory)][long]$ResearchRunId,
    [Parameter(Mandatory)][long]$ResearchArtifactId,
    [Parameter(Mandatory)][string]$BaselineFile,
    [Parameter(Mandatory)][string]$CandidateHead,
    [Parameter(Mandatory)][string]$CandidateTree,
    [Parameter(Mandatory)][string]$Dist,
    [Parameter(Mandatory)][string]$ProofFile
)
$ErrorActionPreference = 'Stop'
if ($env:GITHUB_REPOSITORY -and $env:GITHUB_REPOSITORY -cne 'iamaman11/okx') { throw 'REPOSITORY_IDENTITY_MISMATCH' }
if ($CandidateHead -cnotmatch '^[0-9a-f]{40}$' -or $CandidateTree -cnotmatch '^[0-9a-f]{40}$') { throw 'INVALID_CANDIDATE_SOURCE' }
if (-not [IO.Path]::IsPathFullyQualified($Dist) -or -not [IO.Path]::IsPathFullyQualified($ProofFile)) { throw 'ABSOLUTE_OUTPUT_PATH_REQUIRED' }
if (-not (Test-Path -LiteralPath $BaselineFile -PathType Leaf)) { throw 'INSTALLED_BASELINE_MISSING' }

function Api([string]$endpoint) {
    $result = & gh api --method GET $endpoint
    if ($LASTEXITCODE -ne 0) { throw "GITHUB_ORIGIN_READ_FAILED: $endpoint" }
    return ($result | ConvertFrom-Json)
}

function AcceptedBundle([long]$runId, [long]$artifactId, [string]$destination) {
    if ($runId -le 0 -or $artifactId -le 0) { throw 'INVALID_ARTIFACT_ID' }
    $run = Api "/repos/iamaman11/okx/actions/runs/$runId"
    if ([long]$run.repository.id -ne 1388071566 -or $run.name -cne 'CI' -or
        $run.event -cne 'pull_request' -or $run.status -cne 'completed' -or
        $run.conclusion -cne 'success' -or $run.head_sha -cnotmatch '^[0-9a-f]{40}$') {
        throw 'ORIGIN_NOT_SUCCESSFUL_CANONICAL_PR_CI'
    }
    $artifact = Api "/repos/iamaman11/okx/actions/artifacts/$artifactId"
    $name = "okx-windows-bundle-$($run.head_sha)"
    if ([long]$artifact.id -ne $artifactId -or $artifact.name -cne $name -or
        $artifact.expired -ne $false -or
        ($null -ne $artifact.workflow_run -and [long]$artifact.workflow_run.id -ne $runId)) {
        throw 'ORIGIN_ARTIFACT_IDENTITY_MISMATCH'
    }
    $tree = (Api "/repos/iamaman11/okx/git/commits/$($run.head_sha)").tree.sha
    if ($tree -cnotmatch '^[0-9a-f]{40}$') { throw 'ORIGIN_TREE_INVALID' }
    $pulls = @(Api "/repos/iamaman11/okx/commits/$($run.head_sha)/pulls")
    $merged = @($pulls | Where-Object { $_.head.sha -ceq $run.head_sha -and
        $_.base.ref -ceq 'main' -and $null -ne $_.merged_at -and
        $_.merge_commit_sha -cmatch '^[0-9a-f]{40}$' })
    if ($merged.Count -ne 1) { throw 'ORIGIN_NOT_UNIQUELY_MERGED_TO_MAIN' }
    $ancestry = Api "/repos/iamaman11/okx/compare/$($merged[0].merge_commit_sha)...main"
    if ($ancestry.status -cnotin @('ahead', 'identical')) { throw 'ORIGIN_NOT_ON_MAIN_HISTORY' }
    New-Item -ItemType Directory -Path $destination | Out-Null
    & gh run download $runId --repo iamaman11/okx --name $name --dir $destination
    if ($LASTEXITCODE -ne 0) { throw 'ORIGIN_ARTIFACT_DOWNLOAD_FAILED' }
    return [ordered]@{run_id=$runId;artifact_id=$artifactId;head=$run.head_sha;tree=$tree}
}

$scratch = Join-Path ([IO.Path]::GetTempPath()) ('okx-537-composition-' + [guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $scratch | Out-Null
try {
    $basePath = Join-Path $scratch 'base'
    $researchPath = Join-Path $scratch 'research'
    $base = AcceptedBundle $BaseRunId $BaseArtifactId $basePath
    $research = AcceptedBundle $ResearchRunId $ResearchArtifactId $researchPath
    $inputPath = Join-Path $scratch 'composition-input.json'
    [ordered]@{
        base_dir=$basePath;research_dir=$researchPath;
        base_origin=$base;research_origin=$research;
        baseline_file=(Resolve-Path -LiteralPath $BaselineFile).Path;
        candidate_head=$CandidateHead;candidate_tree=$CandidateTree
    } | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $inputPath -Encoding utf8NoBOM
    & node (Join-Path $PSScriptRoot 'compose-verified-research-v2.cjs') $inputPath $Dist $ProofFile
    if ($LASTEXITCODE -ne 0) { throw 'COMPOSITION_IDENTITY_OR_BYTES_REJECTED' }
} finally {
    # Only the GUID-named directory created by this invocation is temporary.
    if (Test-Path -LiteralPath $scratch) { Remove-Item -LiteralPath $scratch -Recurse -Force }
}
