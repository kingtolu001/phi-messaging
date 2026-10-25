param(
  [int[]]$Users = @(300,1000,3000,10000,20000),
  [string]$Prefix = ('users20k-' + (Get-Date -Format 'yyyyMMdd-HHmmss')),
  [int]$DurationSeconds = 60,
  [int]$WarmupSeconds = 10,
  [int]$AggregateRate = 0,
  [int]$RatePerThousandUsers = 2,
  [string]$RecoveryInterval = '30s',
  [switch]$UseExistingAppImages
)
$ErrorActionPreference = 'Stop'
Set-Location (Split-Path $PSScriptRoot -Parent)
if ($Prefix -notmatch '^[a-zA-Z0-9-]+$' -or $DurationSeconds -lt 1 -or $WarmupSeconds -lt 0 -or $AggregateRate -lt 0 -or $RatePerThousandUsers -lt 1) { throw 'Invalid parameters' }
if (!$Users.Count -or @($Users | Where-Object { $_ -lt 1 -or $_ -gt 20000 }).Count) { throw 'Users must be between 1 and 20000' }
$statusPath = "docs/results/$Prefix-status.json"
if (Test-Path -LiteralPath $statusPath) { throw 'Refusing to overwrite validation evidence' }
$stages = @($Users | ForEach-Object { @{ users=$_; status='SKIPPED'; reason='Not reached' } })
$status = @{ target_distinct_recipient_users=20000; started_at=[DateTimeOffset]::UtcNow.ToString('o'); status='RUNNING'; stages=$stages }
function Save-Status { $status | ConvertTo-Json -Depth 10 | Set-Content -LiteralPath $statusPath -Encoding UTF8 }
New-Item -ItemType Directory -Force docs/results,.artifacts | Out-Null
$prerequisites = @()
$freeBytes = [System.IO.DriveInfo]::new((Get-Location).Drive.Root).AvailableFreeSpace
$status.free_disk_bytes = $freeBytes
if ($freeBytes -lt 5GB) { $prerequisites += 'Less than 5 GiB free on the workspace drive' }
$ErrorActionPreference = 'Continue'
$engine = docker info --format '{{.ServerVersion}}' 2>&1 | Out-String
$engineExit = $LASTEXITCODE
$ErrorActionPreference = 'Stop'
$status.docker_probe_output = $engine.Trim()
if ($engineExit -ne 0) { $prerequisites += 'Docker engine unavailable' }
if ($prerequisites.Count) {
  $status.status='BLOCKED'; $status.reasons=$prerequisites
  foreach ($stage in $stages) { $stage.reason='Prerequisites unavailable; no user-load stage executed' }
  Save-Status
  throw "Validation BLOCKED: $($prerequisites -join '; '). See $statusPath"
}
Save-Status
$compose = @('compose','-f','compose.yaml','-f','compose.scale.yaml')
$previousFixtures = $env:SCALE_FIXTURE_DIR
$fixtureDirectory = ".artifacts/$Prefix-fixtures"
$env:SCALE_FIXTURE_DIR = "./$fixtureDirectory"
try {
  docker compose stop app1 app2 app3 load-balancer
  if ($LASTEXITCODE -ne 0) { throw 'Could not stop idle app pools before correctness tests' }
  docker compose up -d postgres redis
  if ($LASTEXITCODE -ne 0) { throw 'Dependency startup failed' }
  # Mount current source into the cached toolchain; never test stale image source.
  $ErrorActionPreference = 'Continue'
  docker compose --profile test run --rm -T -v "${PWD}:/src" tests sh -c 'go test -race -count=1 -timeout=180s -v ./... && CGO_ENABLED=0 go build -o .artifacts/exercise-linux ./cmd/exercise' 2>&1 | ForEach-Object { $_.ToString() } | Tee-Object "docs/results/$Prefix-tests.log"
  $testExit = $LASTEXITCODE
  $ErrorActionPreference = 'Stop'
  if ($testExit -ne 0) { throw 'Current-source correctness/build stage failed' }
  docker compose --profile test run --rm -T -v "${PWD}:/src" tests ./.artifacts/exercise-linux -mode fixtures -connections 20000 -fixture-dir $fixtureDirectory
  if ($LASTEXITCODE -ne 0) { throw 'Fixture generation failed' }
  Get-Content -Raw "$fixtureDirectory/users.sql" | docker compose exec -T postgres psql -v ON_ERROR_STOP=1 -U phi -d phi
  if ($LASTEXITCODE -ne 0) { throw 'Fixture seed failed' }
  if ($UseExistingAppImages) {
    & docker @compose up -d app1 app2 app3 load-balancer
  } else {
    & docker @compose up --build -d app1 app2 app3 load-balancer
  }
  if ($LASTEXITCODE -ne 0) { throw 'Scale profile build/start failed' }
  & docker @compose exec -T load-balancer nginx -t
  if ($LASTEXITCODE -ne 0) { throw 'Scale proxy configuration failed' }
  # Static upstream names must be resolved again after app replacement.
  & docker @compose restart load-balancer
  if ($LASTEXITCODE -ne 0) { throw 'Proxy restart failed' }
  foreach ($stage in $stages) {
    $count = $stage.users
    $rate = $AggregateRate
    if (!$rate) { $rate = $RatePerThousandUsers * [int][Math]::Ceiling($count / 1000.0) }
    $stage.aggregate_logical_sends_per_second=$rate
    $stage.report="docs/results/$Prefix-$count.json"
    $stage.status='RUNNING'; $stage.reason='Executing workload'; Save-Status
    # Capture external resource samples because the Linux generator has no Docker socket.
    $collector = Start-Job -ScriptBlock {
      while ($true) {
        $sample = docker stats --no-stream --format '{{json .}}' 2>&1 | Out-String
        @{ at=[DateTimeOffset]::UtcNow.ToString('o'); exit_code=$LASTEXITCODE; stats=$sample } | ConvertTo-Json -Compress
        Start-Sleep -Seconds 3
      }
    }
    $querySQL = "SELECT coalesce(sum(calls),0) FROM pg_stat_statements WHERE dbid=(SELECT oid FROM pg_database WHERE datname='phi')"
    $before = & docker @compose exec -T postgres psql -U phi -d phi -At -c $querySQL
    $queryOK = $LASTEXITCODE -eq 0
    $clock = [System.Diagnostics.Stopwatch]::StartNew()
    try {
      & docker @compose --profile scale run --rm -T loadgen -mode load -url http://load-balancer -distinct-users -shared-http -recovery-interval $RecoveryInterval -connections $count -rate $rate -warmup "$($WarmupSeconds)s" -duration "$($DurationSeconds)s" -ramp 2ms -drain 30s -out $stage.report
      $stage.exit_code=$LASTEXITCODE
    } finally {
      $after = & docker @compose exec -T postgres psql -U phi -d phi -At -c $querySQL
      $queryOK = $queryOK -and $LASTEXITCODE -eq 0
      $clock.Stop()
      Stop-Job $collector
      @(Receive-Job $collector) | Set-Content "docs/results/$Prefix-$count-resources.jsonl" -Encoding UTF8
      Remove-Job $collector
    }
    $stage.full_run_seconds_monotonic=$clock.Elapsed.TotalSeconds
    if ($queryOK) {
      $stage.database_statements_per_second_full_run=([double]$after-[double]$before)/$clock.Elapsed.TotalSeconds
    } else { $stage.database_metrics_status='UNAVAILABLE' }
    if (!(Test-Path -LiteralPath $stage.report)) { $stage.status='FAIL'; throw 'Generator did not produce a report' }
    $report = Get-Content -Raw $stage.report | ConvertFrom-Json
    if ($stage.exit_code -ne 0 -or !$report.passed) { $stage.status='FAIL'; $stage.reason=$report.failure; Save-Status; throw "Distinct-user stage $count failed; larger stages remain skipped" }
    $stage.status='PASS'; $stage.reason='All generator assertions passed'; Save-Status
  }
  $status.status='PASS'
} catch {
  $status.status='FAIL'; $status.error=$_.Exception.Message
  foreach ($stage in $stages) { if ($stage.status -eq 'RUNNING') { $stage.status='FAIL'; $stage.reason=$status.error } }
  throw
} finally {
  $status.finished_at=[DateTimeOffset]::UtcNow.ToString('o'); Save-Status
  $env:SCALE_FIXTURE_DIR = $previousFixtures
}
