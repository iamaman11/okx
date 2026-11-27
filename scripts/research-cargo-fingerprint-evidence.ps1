# Read-only, bounded Research Windows cache diagnostics. Dot-source only inside
# the three Research build/test steps, after rust-cache has been restored.
# Never add CARGO_LOG to a job env: rust-cache hashes environment variables.

function Write-ResearchCargoMtimeSnapshot {
  param([ValidateSet('debug', 'release')][string]$Profile)

  $crates = @(
    'okx-strategy-core', 'okx-research', 'okx-research-service',
    'okx-analysis', 'okx-research-domain', 'okx-research-protocol'
  )
  $fingerprints = Join-Path (Join-Path $env:CARGO_TARGET_DIR $Profile) '.fingerprint'
  $rows = @('Research cache pre-build illustrative mtime sample (UTC; src/*.rs versus Cargo dep-info anchors; NOT per-unit freshness proof):')
  foreach ($crate in $crates) {
    try {
      $sourceRoot = Join-Path (Join-Path (Join-Path $env:GITHUB_WORKSPACE 'crates') $crate) 'src'
      # The extra item detects truncation; an incomplete sample is never
      # presented as proof of freshness. Never log absolute checkout paths.
      $sources = @(Get-ChildItem -LiteralPath $sourceRoot -Recurse -File -Filter '*.rs' -ErrorAction Stop |
        Select-Object -First 513)
      $dirs = @(Get-ChildItem -LiteralPath $fingerprints -Directory -Filter "$crate-*" -ErrorAction SilentlyContinue |
        Select-Object -First 33)
      $depInfo = @(
        foreach ($dir in $dirs) {
          Get-ChildItem -LiteralPath $dir.FullName -File -Filter 'dep-*' -ErrorAction SilentlyContinue
        }
      )
      if ($sources.Count -gt 512 -or $dirs.Count -gt 32 -or $depInfo.Count -gt 64) {
        $rows += "$crate`: sample_limit_reached (no freshness conclusion)"
        continue
      }
      if ($sources.Count -eq 0 -or $depInfo.Count -eq 0) {
        $rows += "$crate`: source_count=$($sources.Count) depinfo_count=$($depInfo.Count) comparison=unavailable"
        continue
      }
      $latestSource = ($sources | Sort-Object -Property LastWriteTimeUtc -Descending | Select-Object -First 1).LastWriteTimeUtc
      $latestDepInfo = ($depInfo | Sort-Object -Property LastWriteTimeUtc -Descending | Select-Object -First 1).LastWriteTimeUtc
      $olderAnchors = @($depInfo | Where-Object { $_.LastWriteTimeUtc -lt $latestSource }).Count
      $rows += ('{0}: source_utc={1:yyyy-MM-ddTHH:mm:ss.fffZ} depinfo_latest_utc={2:yyyy-MM-ddTHH:mm:ss.fffZ} depinfo_older_than_source={3}/{4}' -f
        $crate, $latestSource, $latestDepInfo, $olderAnchors, $depInfo.Count)
    } catch {
      # Diagnostic failure never changes the required Cargo/tests outcome.
      # Exceptions may contain machine paths or environment details.
      $rows += "$crate`: snapshot_unavailable"
    }
  }
  $rows | ForEach-Object { Write-Host $_ }
  if ($env:GITHUB_STEP_SUMMARY) {
    @('### Research Cargo cache illustrative mtime snapshot (not unit proof)', '```text') + $rows + @('```') |
      Add-Content -LiteralPath $env:GITHUB_STEP_SUMMARY
  }
}

function New-ResearchCargoFingerprintCounts {
  $workspace = @{}
  foreach ($crate in @('okx-strategy-core', 'okx-research', 'okx-research-service',
      'okx-analysis', 'okx-research-domain', 'okx-research-protocol')) {
    $workspace[$crate] = @{ dirty_headers = 0; reasons = @{};
      error_headers = 0; error_reasons = @{} }
  }
  return @{ total = 0; mtime = 0; dirty = 0; missing = 0; other = 0;
    workspace = $workspace; pending_crate = $null; pending_kind = $null }
}

function Test-ResearchCargoFingerprintLogLine {
  param([AllowNull()][object]$Line, [hashtable]$Counts)
  $value = [string]$Line
  if ($value -notmatch 'cargo::compiler::fingerprint') { return $false }
  # Never echo raw fingerprint lines: they may contain absolute runner paths,
  # feature/env details, or thousands of mostly unhelpful entries. Classification
  # is by diagnostic line, NOT the number of recompiled crates.
  $Counts.total++
  if ($value -match '(?i)mtime|timestamp|newer than|stale file') { $Counts.mtime++ }
  elseif ($value -match '(?i)dirty|stale|changed') { $Counts.dirty++ }
  elseif ($value -match '(?i)missing|not found|no fingerprint') { $Counts.missing++ }
  else { $Counts.other++ }

  # Cargo 1.100 emits adjacent INFO entries: "fingerprint dirty/error for
  # <pkg>" followed by "dirty: <ReasonVariant>" or "err: <DebugError>".
  # Pair only neighboring lines for six allowlisted crates. Keep only fixed
  # reason categories; do not retain any paths, env contents or debug errors.
  if ($value -match '(?i)\bfingerprint (dirty|error) for ([a-z0-9_-]+)\s+v[0-9]') {
    $kind = $Matches[1]
    $crate = $Matches[2]
    if ($Counts.workspace.ContainsKey($crate)) {
      if ($kind -eq 'dirty') { $Counts.workspace[$crate].dirty_headers++ }
      else { $Counts.workspace[$crate].error_headers++ }
      $Counts.pending_crate = $crate
      $Counts.pending_kind = $kind
    } else {
      $Counts.pending_crate = $null
      $Counts.pending_kind = $null
    }
  } elseif ($Counts.pending_kind -eq 'dirty' -and $value -match '(?i)\bdirty:\s*([A-Za-z][A-Za-z0-9_]{0,48})') {
    $crate = $Counts.pending_crate
    if ($crate) {
      $variant = $Matches[1]
      $reason = switch -Regex ($variant) {
        '^FsStatusOutdated$' { 'mtime_or_output'; break }
        '^UnitDependencyInfoChanged$' { 'dependency_fingerprint'; break }
        '^Local' { 'local_fingerprint'; break }
        '^PrecalculatedComponentsChanged$' { 'local_fingerprint'; break }
        '^EnvVarChanged$' { 'tracked_environment'; break }
        '^Missing' { 'missing_fingerprint'; break }
        default { 'other_reason' }
      }
      if (-not $Counts.workspace[$crate].reasons.ContainsKey($reason)) {
        $Counts.workspace[$crate].reasons[$reason] = 0
      }
      $Counts.workspace[$crate].reasons[$reason]++
    }
    $Counts.pending_crate = $null
    $Counts.pending_kind = $null
  } elseif ($Counts.pending_kind -eq 'error' -and $value -match '(?i)\berr:\s*') {
    $crate = $Counts.pending_crate
    if ($crate) {
      # The raw error may contain a user/runner path. Never serialize it.
      $reason = if ($value -match '(?i)\bos error 2\b|\bcode:\s*2\b|no such file|not found|cannot find the file') {
        'missing'
      } elseif ($value -match '(?i)deserialize|invalid data|invalid utf|corrupt|malformed|unexpected end') {
        'corrupt'
      } else { 'other' }
      if (-not $Counts.workspace[$crate].error_reasons.ContainsKey($reason)) {
        $Counts.workspace[$crate].error_reasons[$reason] = 0
      }
      $Counts.workspace[$crate].error_reasons[$reason]++
    }
    $Counts.pending_crate = $null
    $Counts.pending_kind = $null
  } else {
    $Counts.pending_crate = $null
    $Counts.pending_kind = $null
  }
  return $true
}

function Write-ResearchCargoFingerprintCounts {
  param([hashtable]$Counts, [ValidateSet('ipc', 'owner', 'release')][string]$Job)
  $line = "Research Cargo fingerprint INFO ($Job): lines=$($Counts.total) mtime_mentions=$($Counts.mtime) dirty_or_changed=$($Counts.dirty) missing=$($Counts.missing) other=$($Counts.other); counts are log lines, not crates"
  Write-Host $line
  if ($env:GITHUB_STEP_SUMMARY) { Add-Content -LiteralPath $env:GITHUB_STEP_SUMMARY -Value $line }
  foreach ($crate in @('okx-strategy-core', 'okx-research', 'okx-research-service',
      'okx-analysis', 'okx-research-domain', 'okx-research-protocol')) {
    $record = $Counts.workspace[$crate]
    $reasonSummary = @($record.reasons.Keys | Sort-Object | ForEach-Object {
      "$_=$($record.reasons[$_])"
    }) -join ','
    $errorSummary = @($record.error_reasons.Keys | Sort-Object | ForEach-Object {
      "$_=$($record.error_reasons[$_])"
    }) -join ','
    if (-not $reasonSummary) { $reasonSummary = 'none_or_unpaired' }
    if (-not $errorSummary) { $errorSummary = 'none_or_unpaired' }
    $detail = "Research Cargo fingerprint $Job $crate`: dirty_headers=$($record.dirty_headers) paired_reasons=$reasonSummary error_headers=$($record.error_headers) paired_error_reasons=$errorSummary"
    Write-Host $detail
    if ($env:GITHUB_STEP_SUMMARY) { Add-Content -LiteralPath $env:GITHUB_STEP_SUMMARY -Value $detail }
  }
}
