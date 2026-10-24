param([string]$Prefix = ('shared-' + (Get-Date -Format 'yyyyMMdd-HHmmss')))
$ErrorActionPreference = 'Stop'
Set-Location (Split-Path $PSScriptRoot -Parent)
if ($Prefix -notmatch '^[a-zA-Z0-9-]+$') { throw 'Prefix must be a simple filename stem' }
New-Item -ItemType Directory -Force .artifacts | Out-Null
go build -o .artifacts/exercise.exe ./cmd/exercise
if ($LASTEXITCODE -ne 0) { throw 'Harness build failed' }
$failed = $false
# Rates are aggregate logical sends/second, never per device. Each stage is
# independent: retain a failed stage and still run the other requested workloads.
foreach ($stage in @(@{devices=300;rate=2}, @{devices=1000;rate=2}, @{devices=100;rate=10})) {
  & ./.artifacts/exercise.exe -mode load -connections $stage.devices -rate $stage.rate -warmup 5s -duration 20s -ramp 10ms -drain 15s -out "docs/results/$Prefix-$($stage.devices)-rate$($stage.rate).json"
  if ($LASTEXITCODE -ne 0) { $failed = $true }
}
if ($failed) { throw 'At least one load stage failed; inspect the retained reports' }
