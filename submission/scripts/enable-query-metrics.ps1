$ErrorActionPreference = 'Stop'
Set-Location (Split-Path $PSScriptRoot -Parent)
docker compose up -d postgres
if ($LASTEXITCODE -ne 0) { throw 'PostgreSQL startup failed' }
docker compose exec -T postgres psql -U phi -d phi -c 'CREATE EXTENSION IF NOT EXISTS pg_stat_statements'
if ($LASTEXITCODE -ne 0) { throw 'Statement counters unavailable' }
