param(
  [int]$Users=20000,
  [int]$Shards=4,
  [int]$AggregateRate=40,
  [int]$DurationSeconds=60,
  [int]$RampMilliseconds=8,
  [string]$FixtureDirectory,
  [string]$Prefix=('users-sharded-'+(Get-Date -Format 'yyyyMMdd-HHmmss')),
  [switch]$BuildApps,
  [switch]$KillNode
)
$ErrorActionPreference='Stop'
. "$PSScriptRoot/read-ready.ps1"
Set-Location (Split-Path $PSScriptRoot -Parent)
if($Prefix -notmatch '^[a-zA-Z0-9-]+$' -or $Users -lt 1 -or $Users -gt 20000 -or $Shards -lt 2 -or $Shards -gt 8 -or $Users%$Shards -ne 0 -or $AggregateRate%$Shards -ne 0 -or $AggregateRate -lt $Shards) { throw 'Invalid shard parameters' }
if($DurationSeconds -lt 1 -or $DurationSeconds -gt 300) { throw 'Duration must be 1-300 seconds within the controller deadline' }
if($RampMilliseconds -lt 1 -or $RampMilliseconds -gt 20) { throw 'Ramp must be 1-20 ms per driver' }
if($env:COMPOSE_PROJECT_NAME -and $env:COMPOSE_PROJECT_NAME -ne 'phi-messaging') { throw 'This failure controller requires the phi-messaging project name' }
if(!$FixtureDirectory -or !(Test-Path "$FixtureDirectory/tokens.json")) { throw 'Provide the fixture directory from users-scale.ps1' }
$output="docs/results/$Prefix-summary.json"
if(Test-Path $output) { throw 'Refusing to overwrite evidence' }
$perShard=[int]($Users/$Shards)
$rate=[int]($AggregateRate/$Shards)
$barrier=".artifacts/$Prefix-start"
if(Test-Path $barrier) { throw 'Start barrier already exists; use a new prefix' }
$previous=$env:SCALE_FIXTURE_DIR
$env:SCALE_FIXTURE_DIR="./$FixtureDirectory"
$compose=@('compose','-f','compose.yaml','-f','compose.scale.yaml')
$jobs=@()
$collector=$null
$status=@{started_at=[DateTimeOffset]::UtcNow.ToString('o');status='RUNNING';users=$Users;shards=$Shards;global_aggregate_sends_per_second=$AggregateRate;ramp_milliseconds_per_driver=$RampMilliseconds;fault_requested=[bool]$KillNode}
function Save-Status { $status|ConvertTo-Json -Depth 8|Set-Content "docs/results/$Prefix-status.json" -Encoding UTF8 }
Save-Status
$keepAwake=$false
try {
  # Scoped idle-sleep prevention; this does not change saved power settings or
  # prevent manual sleep. Restore the calling thread's state in finally.
  if(!('PhiMessagingValidationPower' -as [type])) {
    Add-Type -TypeDefinition 'using System.Runtime.InteropServices; public static class PhiMessagingValidationPower { [DllImport("kernel32.dll")] public static extern uint SetThreadExecutionState(uint flags); }'
  }
  $keepAwake=[PhiMessagingValidationPower]::SetThreadExecutionState([uint32]2147483649) -ne 0
  $status.idle_sleep_prevention=$keepAwake; Save-Status
  if(!$keepAwake) { throw 'Could not acquire temporary idle-sleep prevention' }
  docker compose stop app1 app2 app3 load-balancer
  if($LASTEXITCODE -ne 0) { throw 'Could not stop idle app pools' }
  $ErrorActionPreference='Continue'
  docker compose --profile test run --rm -T -v "${PWD}:/src" tests sh -c 'go test -race -count=1 -timeout=180s -v ./... && CGO_ENABLED=0 go build -o .artifacts/exercise-linux ./cmd/exercise' 2>&1|ForEach-Object {$_.ToString()}|Tee-Object "docs/results/$Prefix-tests.log"
  $code=$LASTEXITCODE; $ErrorActionPreference='Stop'
  if($code -ne 0) { throw 'Current-source tests/build failed' }
  if($BuildApps) { & docker @compose up --build -d app1 app2 app3 load-balancer } else { & docker @compose up -d app1 app2 app3 load-balancer }
  if($LASTEXITCODE -ne 0) { throw 'App startup failed' }
  & docker @compose restart load-balancer
  if($LASTEXITCODE -ne 0) { throw 'Proxy restart failed' }
  $collector=Start-Job -ScriptBlock {
    while($true) {
      $stats=docker stats --no-stream --format '{{json .}}' 2>&1|Out-String
      @{at=[DateTimeOffset]::UtcNow.ToString('o');exit_code=$LASTEXITCODE;stats=$stats}|ConvertTo-Json -Compress
      Start-Sleep -Seconds 3
    }
  }
  $querySQL="SELECT coalesce(sum(calls),0) FROM pg_stat_statements WHERE dbid=(SELECT oid FROM pg_database WHERE datname='phi')"
  $before=& docker @compose exec -T postgres psql -U phi -d phi -At -c $querySQL
  $queryOK=$LASTEXITCODE -eq 0
  $clock=[System.Diagnostics.Stopwatch]::StartNew()
  for($i=0;$i -lt $Shards;$i++) {
    $jobs+=Start-Job -ArgumentList @($PWD.Path,$FixtureDirectory,$Prefix,$i,$perShard,$rate,$DurationSeconds,$barrier,[bool]$KillNode,$RampMilliseconds) -ScriptBlock {
      param($workspace,$fixtures,$prefix,$index,$count,$rate,$duration,$barrier,$fault,$ramp)
      Set-Location $workspace
      $env:SCALE_FIXTURE_DIR="./$fixtures"
      $service='shardgen'; $extra=@()
      if($fault -and $index -eq 0) { $service='shardfault'; $extra=@('-kill-after','20s','-first-websocket-url','http://app1:8080','-survivor-url','http://app2:8080') }
      docker compose -f compose.yaml -f compose.scale.yaml --profile scale run --rm -T --name "phi-messaging-$prefix-$index" $service -mode load -url http://load-balancer -distinct-users -shared-http -recovery-interval 30s -ready-timeout 2m -connections $count -user-offset ($index*$count) -rate $rate -warmup 10s -duration "$($duration)s" -ramp "$($ramp)ms" -drain 30s -ready-file ".artifacts/$prefix-ready-$index.json" -start-barrier $barrier -out "docs/results/$prefix-shard$index.json" @extra 2>&1|ForEach-Object {$_.ToString()}|Tee-Object "docs/results/$prefix-shard$index.log"
      "Controller exit code: $LASTEXITCODE"
    }
  }
  $deadline=[DateTime]::UtcNow.AddSeconds(300)
  $ready=$false
  while([DateTime]::UtcNow -lt $deadline) {
    $ready=$true
    for($i=0;$i -lt $Shards;$i++) {
      $path=".artifacts/$Prefix-ready-$i.json"
      if(!(Read-CohortReady $path $perShard ($i*$perShard))) { $ready=$false; break }
    }
    if($ready) { break }
    Start-Sleep -Milliseconds 100
  }
  if(!$ready) { throw 'Generators did not reach the synchronized start barrier' }
  $status.barrier_released_at=[DateTimeOffset]::UtcNow.ToString('o'); Save-Status
  Set-Content -LiteralPath $barrier -Value 'start' -Encoding ASCII
  $jobs|Wait-Job|Receive-Job
  $after=& docker @compose exec -T postgres psql -U phi -d phi -At -c $querySQL
  $queryOK=$queryOK -and $LASTEXITCODE -eq 0
  $clock.Stop()
  $status.database_counter_window_seconds_monotonic=$clock.Elapsed.TotalSeconds
  if($queryOK -and [double]$after -ge [double]$before -and $clock.Elapsed.TotalSeconds -gt 0) { $status.database_statements_per_second_full_run=([double]$after-[double]$before)/$clock.Elapsed.TotalSeconds } else { $status.database_metrics_status='UNAVAILABLE_OR_COUNTER_RESET' }
  $mode='healthy'; if($KillNode) {$mode='fault'}
  node scripts/aggregate-shards.mjs $Prefix $Shards $output $mode
  if($LASTEXITCODE -ne 0) { throw 'Sharded validation failed; inspect all retained reports' }
  $status.status='PASS'
} catch {
  $status.status='FAIL'; $status.error=$_.Exception.Message
  $ErrorActionPreference='Continue'
  for($i=0;$i -lt $jobs.Count;$i++) { if($jobs[$i].State -eq 'Running') { docker kill --signal SIGTERM "phi-messaging-$Prefix-$i" 2>$null } }
  $ErrorActionPreference='Stop'
  throw
} finally {
  if($keepAwake) { [void][PhiMessagingValidationPower]::SetThreadExecutionState([uint32]2147483648) }
  $ErrorActionPreference='Continue'
  & docker @compose logs --no-color --timestamps --since $status.started_at app1 app2 app3 2>&1|ForEach-Object {$_.ToString()}|Set-Content "docs/results/$Prefix-app-diagnostics.log" -Encoding UTF8
  $status.diagnostics_exit_code=$LASTEXITCODE
  $ErrorActionPreference='Stop'
  if($collector) { Stop-Job $collector; @(Receive-Job $collector)|Set-Content "docs/results/$Prefix-resources.jsonl" -Encoding UTF8; Remove-Job $collector }
  foreach($job in $jobs) { if($job.State -eq 'Running') { Stop-Job $job }; Remove-Job $job }
  if($KillNode) { & docker @compose start app1; & docker @compose restart load-balancer }
  $env:SCALE_FIXTURE_DIR=$previous
  $status.finished_at=[DateTimeOffset]::UtcNow.ToString('o'); Save-Status
}
