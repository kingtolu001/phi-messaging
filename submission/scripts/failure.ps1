param([string]$Output = '')
$ErrorActionPreference = 'Stop'
Set-Location (Split-Path $PSScriptRoot -Parent)
New-Item -ItemType Directory -Force .artifacts, docs/results | Out-Null
if (!$Output) { $Output = 'docs/results/failure-' + (Get-Date -Format 'yyyyMMdd-HHmmss') + '.json' }
if (Test-Path -LiteralPath $Output) { throw "Refusing to overwrite evidence: $Output" }
docker compose up --build -d
if ($LASTEXITCODE -ne 0) { throw 'Compose startup failed' }
# Nginx resolves static upstream container names at startup.
docker compose restart load-balancer
if ($LASTEXITCODE -ne 0) { throw 'Proxy restart failed' }
go build -o .artifacts/exercise.exe ./cmd/exercise
if ($LASTEXITCODE -ne 0) { throw 'Harness build failed' }
& ./.artifacts/exercise.exe -mode failure -warmup 3s -duration 15s -rate 5 -out $Output
if ($LASTEXITCODE -ne 0) { throw "Failure scenario FAILED; inspect $Output" }
