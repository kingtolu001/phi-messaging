param([string]$Destination = '')
$ErrorActionPreference = 'Stop'
$project = Split-Path $PSScriptRoot -Parent
$workspace = Split-Path $project -Parent
if (!$Destination) { $Destination = Join-Path $workspace '.tools\skills\archify' }
$pin = Get-Content (Join-Path $project 'docs\diagrams\archify-tool.json') -Raw | ConvertFrom-Json
if (Test-Path -LiteralPath $Destination) {
  foreach ($file in $pin.files) {
    $actual = (Get-FileHash -Algorithm SHA256 -LiteralPath (Join-Path $Destination $file.path)).Hash.ToLowerInvariant()
    if ($actual -ne $file.sha256) { throw "Installed Archify differs from the recorded tool: $($file.path)" }
  }
  Write-Output "Archify $($pin.version) already installed and matches the recorded package."
  exit 0
}
$stage = Join-Path $workspace ('.tools\archify-install-' + [guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Force (Split-Path $Destination -Parent) | Out-Null
git clone --filter=blob:none --no-checkout --depth 1 https://github.com/tt-a1i/archify.git $stage
if ($LASTEXITCODE -ne 0) { throw "Archify clone failed; inspect $stage" }
git -c "safe.directory=$stage" -C $stage sparse-checkout set archify
if ($LASTEXITCODE -ne 0) { throw 'Sparse checkout configuration failed' }
git -c "safe.directory=$stage" -C $stage fetch --depth 1 origin $pin.revision
if ($LASTEXITCODE -ne 0) { throw 'Pinned Archify fetch failed' }
git -c "safe.directory=$stage" -C $stage checkout --detach $pin.revision
if ($LASTEXITCODE -ne 0) { throw 'Pinned Archify checkout failed' }
Copy-Item -LiteralPath (Join-Path $stage 'archify') -Destination $Destination -Recurse
Copy-Item -LiteralPath (Join-Path $stage 'LICENSE') -Destination (Join-Path $Destination 'LICENSE')
foreach ($file in $pin.files) {
  $actual = (Get-FileHash -Algorithm SHA256 -LiteralPath (Join-Path $Destination $file.path)).Hash.ToLowerInvariant()
  if ($actual -ne $file.sha256) { throw "Copied tool hash mismatch: $($file.path)" }
}
# Delete only this invocation's uniquely named staging directory in the workspace.
$resolved = (Resolve-Path -LiteralPath $stage).Path
$tools = [IO.Path]::GetFullPath((Join-Path $workspace '.tools')) + [IO.Path]::DirectorySeparatorChar
if (!$resolved.StartsWith($tools, [StringComparison]::OrdinalIgnoreCase) -or (Split-Path $resolved -Leaf) -notlike 'archify-install-*') {
  throw 'Staging cleanup path escaped the project tools directory'
}
Remove-Item -LiteralPath $resolved -Recurse -Force
Write-Output "Installed Archify $($pin.version), revision $($pin.revision), in $Destination"
