function Read-CohortReady($Path, [int]$Expected, [int]$Offset) {
  if (!(Test-Path -LiteralPath $Path)) { return $false }
  # Save creates the file before writing it. Empty or partial JSON is not a
  # completed readiness report; retry until the bounded controller deadline.
  try { $report = Get-Content -LiteralPath $Path -Raw | ConvertFrom-Json } catch { return $false }
  if ($null -eq $report -or $null -eq $report.connected -or $null -eq $report.target -or $null -eq $report.user_offset) { return $false }
  if ($report.target -ne $Expected -or $report.user_offset -ne $Offset) { throw 'Readiness cohort identity mismatch' }
  if ($report.connected -ne $Expected) { throw "Cohort at offset $Offset connected $($report.connected)/$Expected users" }
  return $true
}
