param([string]$Output = ('docs/results/environment-' + (Get-Date -Format 'yyyyMMdd-HHmmss') + '.json'))
$ErrorActionPreference = 'Stop'
Set-Location (Split-Path $PSScriptRoot -Parent)
New-Item -ItemType Directory -Force docs/results | Out-Null
if (Test-Path -LiteralPath $Output) { throw "Refusing to overwrite $Output" }
$cpu = Get-CimInstance Win32_Processor | Select-Object Name,NumberOfCores,NumberOfLogicalProcessors
$machine = Get-CimInstance Win32_ComputerSystem | Select-Object TotalPhysicalMemory
$docker = docker info --format '{{json .}}' | ConvertFrom-Json
if ($LASTEXITCODE -ne 0) { throw 'Docker info failed' }
@{
  captured_at = [DateTimeOffset]::UtcNow.ToString('o')
  host_cpu = $cpu
  host_memory = $machine
  docker = $docker | Select-Object NCPU,MemTotal,KernelVersion,OperatingSystem,Architecture,ServerVersion
  go = (go version)
  compose = (docker compose version)
  note = 'Shared development machine; unrelated containers may consume CPU. No exclusive CPU pinning.'
} | ConvertTo-Json -Depth 6 | Set-Content -LiteralPath $Output -Encoding UTF8
