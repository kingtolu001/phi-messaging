$ErrorActionPreference = 'Stop'
Set-Location (Split-Path $PSScriptRoot -Parent)
$summary = foreach ($file in Get-ChildItem docs/results/load-*.json) {
  $r = Get-Content $file.FullName -Raw | ConvertFrom-Json
  $warmupMissing = 0
  foreach ($record in $r.records) { if (!$record.measured) { $warmupMissing += $r.config.connections - $record.delivered_devices } }
  $peaks = @{}
  foreach ($sample in $r.resource_samples) {
    foreach ($line in ($sample.docker_stats -split "`n")) {
      if (!$line.Trim()) { continue }
      $stat = $line | ConvertFrom-Json
      $cpu = [double]::Parse($stat.CPUPerc.TrimEnd('%'), [Globalization.CultureInfo]::InvariantCulture)
      if (!$peaks.ContainsKey($stat.Name) -or $peaks[$stat.Name].cpu_percent -lt $cpu) {
        $peaks[$stat.Name] = @{cpu_percent=$cpu; memory_at_peak_cpu=$stat.MemUsage}
      }
    }
  }
  [ordered]@{
    file=$file.Name; passed=$r.passed; requested=$r.config.connections
    simultaneous_max=$r.max_simultaneously_resumed; min_during_measurement=$r.min_resumed_during_measurement
    acknowledged=$r.measured_acknowledged_messages; expected=$r.expected_measured_deliveries
    received=$r.received_measured_deliveries; missing=$r.missing_measured_deliveries
    missing_warmup=$warmupMissing
    recipient_p50_ms=$r.recipient_delivery_latency.p50_ms
    recipient_p95_ms=$r.recipient_delivery_latency.p95_ms
    recipient_p99_ms=$r.recipient_delivery_latency.p99_ms
    ack_p99_ms=$r.sender_ack_latency.p99_ms
    ack_per_second=$r.acknowledged_messages_per_second
    deliveries_per_second=$r.recipient_deliveries_per_second_including_drain
    connection_failures=$r.websocket_establishment_failures; reconnect_retries=$r.connection_retries
    http_errors=$r.http_attempt_errors; message_errors=$r.unacknowledged_logical_messages
    queue_drops=$r.scheduler_queue_drops; unscheduled=$r.unscheduled_rate_slots
    cpu_samples_peak=$peaks
  }
}
$summary | ConvertTo-Json -Depth 8 | Set-Content docs/results/summary.json -Encoding UTF8
$summary | ForEach-Object { [pscustomobject]$_ } | Format-Table file,passed,requested,simultaneous_max,received,missing,recipient_p99_ms,http_errors -AutoSize
