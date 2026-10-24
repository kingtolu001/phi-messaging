param(
  [string]$Before = 'before-shared-v3',
  [string]$After = 'after-shared-v3',
  [string]$Output = 'docs/results/shared-comparison.json'
)
$ErrorActionPreference = 'Stop'
Set-Location (Split-Path $PSScriptRoot -Parent)
if (Test-Path -LiteralPath $Output) { throw "Refusing to overwrite $Output" }
function MemoryMiB([string]$usage) {
  $value = ($usage -split ' / ')[0]
  if ($value -notmatch '^([0-9.]+)(B|kB|MB|GB|KiB|MiB|GiB)$') { throw "Unknown memory unit: $value" }
  $scales = @{B=1; kB=1000; MB=1000000; GB=1000000000; KiB=1024; MiB=1048576; GiB=1073741824}
  return [double]::Parse($Matches[1], [Globalization.CultureInfo]::InvariantCulture) * $scales[$Matches[2]] / 1048576
}
$rows = foreach ($prefix in @($Before, $After)) {
  foreach ($suffix in @('300-rate2', '1000-rate2', '100-rate10')) {
    $path = "docs/results/$prefix-$suffix.json"
    $r = Get-Content -LiteralPath $path -Raw | ConvertFrom-Json
    $peaks = @{}
    $appsCPU = 0.0; $appsMemory = 0.0
    foreach ($sample in $r.resource_samples) {
      $sumCPU = 0.0; $sumMemory = 0.0
      foreach ($line in ($sample.docker_stats -split "`n")) {
        if (!$line.Trim()) { continue }
        $s = $line | ConvertFrom-Json
        $cpu = [double]::Parse($s.CPUPerc.TrimEnd('%'), [Globalization.CultureInfo]::InvariantCulture)
        $memory = MemoryMiB $s.MemUsage
        if (!$peaks.ContainsKey($s.Name)) { $peaks[$s.Name] = @{cpu_percent=0.0;memory_mib=0.0} }
        $peaks[$s.Name].cpu_percent = [Math]::Max($peaks[$s.Name].cpu_percent, $cpu)
        $peaks[$s.Name].memory_mib = [Math]::Max($peaks[$s.Name].memory_mib, $memory)
        if ($s.Name -match '-app[123]-') { $sumCPU += $cpu; $sumMemory += $memory }
      }
      $appsCPU = [Math]::Max($appsCPU, $sumCPU)
      $appsMemory = [Math]::Max($appsMemory, $sumMemory)
    }
    [ordered]@{
      file=$path; passed=$r.passed; rate_scope=$r.config.send_rate_scope; device_http_pool=$r.config.device_http_pool
      devices=$r.config.connections; aggregate_sends_per_second=$r.config.logical_messages_per_second
      min_resumed=$r.min_resumed_during_measurement; max_resumed=$r.max_simultaneously_resumed
      expected=$r.expected_measured_deliveries; received=$r.received_measured_deliveries
      missing=$r.missing_measured_deliveries; missing_warmup=$r.missing_warmup_deliveries
      recipient_latency=$r.recipient_delivery_latency; sender_ack_latency=$r.sender_ack_latency
      deliveries_per_second=$r.recipient_deliveries_per_second_including_drain
      acknowledgements_per_second=$r.acknowledged_messages_per_second
      connection_failures=$r.websocket_establishment_failures; connection_retries=$r.connection_retries
      http_attempt_errors=$r.http_attempt_errors; unacknowledged=$r.unacknowledged_logical_messages
      queue_drops=$r.scheduler_queue_drops; unscheduled=$r.unscheduled_rate_slots
      duplicate_callbacks=$r.duplicate_callbacks; terminal_client_errors=$r.client_terminal_errors
      audit=$r.history_audit; statements_per_second=$r.database_statements_per_second_full_run
      counter_window_seconds=$r.database_counter_window_seconds_monotonic
      resource_sample_count=@($r.resource_samples).Count
      apps_peak_cpu_percent=$appsCPU; apps_peak_memory_mib=$appsMemory; resource_peaks=$peaks
    }
  }
}
$rows | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $Output -Encoding UTF8
$rows | ForEach-Object { [pscustomobject]$_ } | Format-Table file,passed,received,missing,http_attempt_errors,statements_per_second -AutoSize
