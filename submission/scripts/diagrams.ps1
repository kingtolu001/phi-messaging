param([string]$Review = ('regenerate-' + (Get-Date -Format 'yyyyMMdd-HHmmss')))
$ErrorActionPreference = 'Stop'
Set-Location (Split-Path $PSScriptRoot -Parent)
if ($Review -notmatch '^[a-zA-Z0-9-]+$') { throw 'Use a simple review folder name' }
$tool = if ($env:ARCHIFY_HOME) { $env:ARCHIFY_HOME } else { Join-Path (Split-Path (Get-Location) -Parent) '.tools\skills\archify' }
$cli = Join-Path $tool 'bin\archify.mjs'
if (!(Test-Path -LiteralPath $cli)) { throw 'Run scripts/install-archify.ps1 first or set ARCHIFY_HOME' }
$folder = "docs/diagrams/review/$Review"
if (Test-Path -LiteralPath $folder) { throw 'Choose an unused review name to preserve earlier evidence' }
$env:ARCHIFY_UPDATE_CHECK_DISABLED = '1'
node scripts/diagram-evidence.mjs
if ($LASTEXITCODE -ne 0) { throw 'Implementation changed: review the diagrams before recording new source hashes' }
$runs = @()
foreach ($spec in @(@{name='runtime';type='architecture'}, @{name='send';type='sequence'}, @{name='recovery';type='sequence'})) {
  $name = $spec.name
  $receiptFolder = "$folder/$name"
  & node $cli finalize $spec.type "docs/diagrams/$name.$($spec.type).json" "docs/diagrams/$name.html" --quality showcase --json --out-dir $receiptFolder
  if ($LASTEXITCODE -ne 0) { throw "Archify $name failed; inspect its retained receipt" }
  $summary = Get-Content "$receiptFolder/$name.finalize-summary.json" -Raw | ConvertFrom-Json
  $runs += @{name=$name;receipt="$receiptFolder/$name.finalize-summary.json";source_sha256=$summary.specification.sha256;html_sha256=$summary.artifact.sha256;gates=$summary.gates}
}
node scripts/export-diagrams.mjs
if ($LASTEXITCODE -ne 0) { throw 'Native static export failed' }
$ledger = @{archify='3.2.0';evidence='automated artifact and browser gates; source semantics reviewed separately';runs=$runs}
[IO.File]::WriteAllText((Join-Path (Get-Location) 'docs/diagrams/validation.json'), (($ledger | ConvertTo-Json -Depth 8) -replace "`r`n", "`n") + "`n", [Text.UTF8Encoding]::new($false))
Write-Output 'PASS: all three diagrams finalized and native SVG/PNG exports generated.'
