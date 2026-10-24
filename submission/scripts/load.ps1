param(
  [int[]]$Connections = @(10,100,300,1000),
  [int]$Rate = 2,
  [string]$Duration = '20s',
  [string]$Warmup = '5s',
  [string]$Ramp = '10ms',
  [string]$Prefix = ''
)
$ErrorActionPreference = 'Stop'
Set-Location (Split-Path $PSScriptRoot -Parent)
New-Item -ItemType Directory -Force .artifacts, docs/results | Out-Null
if (!$Prefix) { $Prefix = 'docs/results/load-' + (Get-Date -Format 'yyyyMMdd-HHmmss') }
go build -o .artifacts/exercise.exe ./cmd/exercise
if ($LASTEXITCODE -ne 0) { throw 'Harness build failed' }
foreach ($count in $Connections) {
  & ./.artifacts/exercise.exe -mode load -connections $count -rate $Rate -warmup $Warmup -duration $Duration -ramp $Ramp -out "$Prefix-$count.json"
  if ($LASTEXITCODE -ne 0) {
    # Stop escalation on failed assertions; retain the failed report for analysis.
    throw "Load stage $count FAILED; inspect $Prefix-$count.json before increasing load"
  }
}
