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
Assert-FingerprintLine 'INFO cargo::compiler::fingerprint:     dirty: FsStatusOutdated(StaleItem)' $true
Assert-FingerprintLine 'INFO cargo::compiler::fingerprint: fingerprint dirty for okx-analysis v0.1.0/Build' $true
Assert-FingerprintLine 'INFO cargo::compiler::fingerprint:     dirty: UnitDependencyInfoChanged { old_name: "dependency" }' $true

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

if ($counts.workspace['okx-research'].reasons['mtime_or_output'] -ne 1 -or
    $counts.workspace['okx-analysis'].reasons['dependency_fingerprint'] -ne 1 -or
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
Write-Host 'RESEARCH_FINGERPRINT_SYNTHETIC_PARSER=PASS'
