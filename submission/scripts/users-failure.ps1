param(
  [int]$Users = 20000,
  [int]$AggregateRate = 40,
  [string]$FixtureDirectory,
  [string]$Prefix = ('users-failure-' + (Get-Date -Format 'yyyyMMdd-HHmmss'))
)
$ErrorActionPreference = 'Stop'
Set-Location (Split-Path $PSScriptRoot -Parent)
if ($Prefix -notmatch '^[a-zA-Z0-9-]+$' -or $Users -lt 1 -or $Users -gt 20000 -or $AggregateRate -lt 1) { throw 'Invalid workload parameters' }
if (!$FixtureDirectory -or !(Test-Path -LiteralPath "$FixtureDirectory/tokens.json")) { throw 'Provide the fixture directory from a successful users-scale.ps1 setup' }
$output = "docs/results/$Prefix.json"
if (Test-Path -LiteralPath $output) { throw 'Refusing to overwrite failure evidence' }
$previous = $env:SCALE_FIXTURE_DIR
$env:SCALE_FIXTURE_DIR = "./$FixtureDirectory"
$compose = @('compose','-f','compose.yaml','-f','compose.scale.yaml')
try {
  # Rebuild the controller from current source; the Docker socket is mounted only
  # in faultgen, which can SIGKILL this project's app1 via the Docker API.
  docker compose --profile test run --rm -T -v "${PWD}:/src" tests sh -c 'go test -race -count=1 ./internal/exercise ./internal/client && CGO_ENABLED=0 go build -o .artifacts/exercise-linux ./cmd/exercise'
  if ($LASTEXITCODE -ne 0) { throw 'Fault-controller checks/build failed' }
  & docker @compose up -d app1 app2 app3 load-balancer
  if ($LASTEXITCODE -ne 0) { throw 'Scale profile startup failed' }
  & docker @compose restart load-balancer
  if ($LASTEXITCODE -ne 0) { throw 'Proxy restart failed' }
  $collector = Start-Job -ScriptBlock {
    while ($true) {
      $sample = docker stats --no-stream --format '{{json .}}' 2>&1 | Out-String
      @{ at=[DateTimeOffset]::UtcNow.ToString('o'); exit_code=$LASTEXITCODE; stats=$sample } | ConvertTo-Json -Compress
      Start-Sleep -Seconds 3
    }
  }
  try {
    & docker @compose --profile scale run --rm -T faultgen -mode load -url http://load-balancer -distinct-users -shared-http -recovery-interval 30s -connections $Users -rate $AggregateRate -warmup 10s -duration 60s -ramp 2ms -drain 30s -kill-after 20s -first-websocket-url http://app1:8080 -survivor-url http://app2:8080 -out $output
    $runExit = $LASTEXITCODE
  } finally {
    Stop-Job $collector
    @(Receive-Job $collector) | Set-Content "docs/results/$Prefix-resources.jsonl" -Encoding UTF8
    Remove-Job $collector
  }
  if (!(Test-Path -LiteralPath $output)) { throw 'Fault generator produced no report' }
  $report = Get-Content -Raw $output | ConvertFrom-Json
  if ($runExit -ne 0 -or !$report.passed) { throw "Node-failure workload FAILED; inspect $output" }
} finally {
  & docker @compose start app1
  & docker @compose restart load-balancer
  $env:SCALE_FIXTURE_DIR = $previous
}
