# Synthetic, toolchain-independent parser check executed on the Research owner
# Windows runner before Cargo compilation. No file, cache or release mutations.
$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot 'research-cargo-fingerprint-evidence.ps1')

$counts = New-ResearchCargoFingerprintCounts
function Assert-FingerprintLine {
  param([string]$Text, [bool]$ExpectedFilter)
  $filtered = Test-ResearchCargoFingerprintLogLine -Line $Text -Counts $counts
  if ($filtered -ne $ExpectedFilter) { throw 'RESEARCH_FINGERPRINT_FILTER_MISMATCH' }
}

Assert-FingerprintLine 'Compiling okx-research v0.1.0' $false
Assert-FingerprintLine 'INFO cargo::compiler::fingerprint: fingerprint dirty for okx-research v0.1.0/Build' $true
Assert-FingerprintLine 'INFO cargo::compiler::fingerprint:     dirty: FsStatusOutdated(StaleItem(ChangedFile { reference: "C:\MissingFile\dep-lib", stale: "C:\SYNTHETIC_PRIVATE\src\lib.rs" }))' $true
Assert-FingerprintLine 'INFO cargo::compiler::fingerprint: fingerprint dirty for okx-analysis v0.1.0/Build' $true
Assert-FingerprintLine 'INFO cargo::compiler::fingerprint:     dirty: UnitDependencyInfoChanged { old_name: "dependency" }' $true
Assert-FingerprintLine 'INFO cargo::compiler::fingerprint: fingerprint dirty for okx-strategy-core v0.1.0/Build' $true
Assert-FingerprintLine 'INFO cargo::compiler::fingerprint:     dirty: FsStatusOutdated(StaleItem(MissingFile { path: "C:\SYNTHETIC_PRIVATE\dep-lib" }))' $true
Assert-FingerprintLine 'INFO cargo::compiler::fingerprint: fingerprint dirty for okx-research-domain v0.1.0/Build' $true
Assert-FingerprintLine 'INFO cargo::compiler::fingerprint:     dirty: FsStatusOutdated(StaleItem(FailedToReadMetadata { path: "C:\SYNTHETIC_PRIVATE" }))' $true
Assert-FingerprintLine 'INFO cargo::compiler::fingerprint: fingerprint dirty for okx-research-protocol v0.1.0/Build' $true
Assert-FingerprintLine 'INFO cargo::compiler::fingerprint:     dirty: FsStatusOutdated(StaleDepFingerprint { unit: 14 })' $true
Assert-FingerprintLine 'INFO cargo::compiler::fingerprint: fingerprint dirty for okx-research-service v0.1.0/Build' $true
# An incidental path containing a known enum label must not be misclassified.
Assert-FingerprintLine 'INFO cargo::compiler::fingerprint:     dirty: FsStatusOutdated(StaleItem(Unknown { path: "C:\ChangedFile\MissingFile" }))' $true

Assert-FingerprintLine 'INFO cargo::compiler::fingerprint: fingerprint error for okx-research-service v0.1.0/Build' $true
Assert-FingerprintLine 'INFO cargo::compiler::fingerprint:     err: Error { source: Os { code: 2 }, path: "C:\SYNTHETIC_PRIVATE" }' $true
Assert-FingerprintLine 'INFO cargo::compiler::fingerprint: fingerprint error for okx-research-service v0.1.0/Build' $true
Assert-FingerprintLine 'INFO cargo::compiler::fingerprint:     err: failed to deserialize json at C:\SYNTHETIC_PRIVATE' $true
Assert-FingerprintLine 'INFO cargo::compiler::fingerprint: fingerprint error for okx-research-service v0.1.0/Build' $true
Assert-FingerprintLine 'INFO cargo::compiler::fingerprint:     err: access denied at C:\SYNTHETIC_PRIVATE' $true

# Unrecognized packages are filtered for privacy, never attributed to an
# allowlisted crate. A nonadjacent line must also drop a pending association.
Assert-FingerprintLine 'INFO cargo::compiler::fingerprint: fingerprint error for untrusted-other v0.1.0/Build' $true
Assert-FingerprintLine 'INFO cargo::compiler::fingerprint:     err: os error 2' $true
Assert-FingerprintLine 'INFO cargo::compiler::fingerprint: fingerprint error for okx-research v0.1.0/Build' $true
Assert-FingerprintLine 'INFO cargo::compiler::fingerprint: checking a different unit' $true
Assert-FingerprintLine 'INFO cargo::compiler::fingerprint:     err: os error 2' $true

if ($counts.workspace['okx-research'].reasons['changed_file_mtime'] -ne 1 -or
    $counts.workspace['okx-analysis'].reasons['dependency_fingerprint'] -ne 1 -or
    $counts.workspace['okx-strategy-core'].reasons['missing_file'] -ne 1 -or
    $counts.workspace['okx-research-domain'].reasons['metadata_unreadable'] -ne 1 -or
    $counts.workspace['okx-research-protocol'].reasons['dependency_stale'] -ne 1 -or
    $counts.workspace['okx-research-service'].reasons['fs_status_unspecified'] -ne 1 -or
    $counts.workspace['okx-research-service'].error_headers -ne 3 -or
    $counts.workspace['okx-research-service'].error_reasons['missing'] -ne 1 -or
    $counts.workspace['okx-research-service'].error_reasons['corrupt'] -ne 1 -or
    $counts.workspace['okx-research-service'].error_reasons['other'] -ne 1 -or
    $counts.workspace['okx-research'].error_headers -ne 1 -or
    $counts.workspace['okx-research'].error_reasons.Count -ne 0 -or
    $counts.workspace['okx-research-domain'].error_headers -ne 0 -or
    $null -ne $counts.pending_crate -or $null -ne $counts.pending_kind) {
  throw 'RESEARCH_FINGERPRINT_REASON_CLASSIFICATION_MISMATCH'
}

# Executable Cargo 1.100 layout fixture. The old diagnostic erroneously
# looked only in .fingerprint/<pkg>-<META>, silently reporting zero anchors
# even though the current build-unit layout keeps them under
# build/<pkg>/<META>/fingerprint. Use a unique RUNNER_TEMP sandbox only;
# never touch the restored real cache, source tree or CI job summary.
if (-not $env:RUNNER_TEMP) { throw 'RESEARCH_FINGERPRINT_FIXTURE_TEMP_UNAVAILABLE' }
$fixtureRoot = Join-Path $env:RUNNER_TEMP ("okx-fingerprint-layout-" + [guid]::NewGuid().ToString('N'))
$priorWorkspace = $env:GITHUB_WORKSPACE
$priorTarget = $env:CARGO_TARGET_DIR
$priorSummary = $env:GITHUB_STEP_SUMMARY
try {
  $modernSource = Join-Path $fixtureRoot 'crates/okx-research-domain/src'
  $legacySource = Join-Path $fixtureRoot 'crates/okx-research-protocol/src'
  $modernFingerprint = Join-Path $fixtureRoot 'target/release/build/okx-research-domain/unit123/fingerprint'
  $legacyFingerprint = Join-Path $fixtureRoot 'target/release/.fingerprint/okx-research-protocol-unit456'
  foreach ($path in @($modernSource, $legacySource, $modernFingerprint, $legacyFingerprint)) {
    New-Item -ItemType Directory -Force -Path $path | Out-Null
  }
  $modernSrc = Join-Path $modernSource 'lib.rs'
  $legacySrc = Join-Path $legacySource 'lib.rs'
  $modernDep = Join-Path $modernFingerprint 'dep-lib-okx_research_domain'
  $legacyDep = Join-Path $legacyFingerprint 'dep-lib-okx_research_protocol'
  foreach ($path in @($modernSrc, $legacySrc, $modernDep, $legacyDep)) {
    New-Item -ItemType File -Path $path | Out-Null
  }
  $now = [DateTime]::UtcNow
  foreach ($path in @($modernDep, $legacyDep)) { [IO.File]::SetLastWriteTimeUtc($path, $now.AddMinutes(-10)) }
  foreach ($path in @($modernSrc, $legacySrc)) { [IO.File]::SetLastWriteTimeUtc($path, $now.AddMinutes(-1)) }

  $env:GITHUB_WORKSPACE = $fixtureRoot
  $env:CARGO_TARGET_DIR = Join-Path $fixtureRoot 'target'
  Remove-Item Env:GITHUB_STEP_SUMMARY -ErrorAction SilentlyContinue
  $snapshot = @(& { Write-ResearchCargoMtimeSnapshot -Profile release } 6>&1 |
    ForEach-Object { [string]$_ }) -join "`n"
  if ($snapshot -notmatch 'okx-research-domain: layout=cargo100 source_utc=.*depinfo_older_than_source=1/1') {
    throw 'RESEARCH_FINGERPRINT_CARGO100_ANCHOR_NOT_FOUND'
  }
  if ($snapshot -notmatch 'okx-research-protocol: layout=legacy source_utc=.*depinfo_older_than_source=1/1' -or
      $snapshot -match 'okx-research-protocol: layout=cargo100') {
    throw 'RESEARCH_FINGERPRINT_LEGACY_ANCHOR_MISLABELED'
  }
} finally {
  $env:GITHUB_WORKSPACE = $priorWorkspace
  $env:CARGO_TARGET_DIR = $priorTarget
  if ($priorSummary) { $env:GITHUB_STEP_SUMMARY = $priorSummary }
  else { Remove-Item Env:GITHUB_STEP_SUMMARY -ErrorAction SilentlyContinue }
  if (Test-Path -LiteralPath $fixtureRoot -PathType Container) {
    Remove-Item -LiteralPath $fixtureRoot -Recurse -Force
  }
}
Write-Host 'RESEARCH_FINGERPRINT_SYNTHETIC_PARSER=PASS'
