$ErrorActionPreference='Stop'
. "$PSScriptRoot/read-ready.ps1"
$file=Join-Path ([System.IO.Path]::GetTempPath()) ([guid]::NewGuid().ToString()+'.json')
try {
  if(Read-CohortReady $file 5000 0) { throw 'Missing file accepted' }
  Set-Content $file ''
  if(Read-CohortReady $file 5000 0) { throw 'Empty file accepted' }
  Set-Content $file '{"connected":'
  if(Read-CohortReady $file 5000 0) { throw 'Partial JSON accepted' }
  Set-Content $file '{"connected":5000,"target":5000,"user_offset":0}'
  if(!(Read-CohortReady $file 5000 0)) { throw 'Full cohort rejected' }
  Set-Content $file '{"connected":4999,"target":5000,"user_offset":0}'
  $rejected=$false
  try { Read-CohortReady $file 5000 0 | Out-Null } catch { $rejected=$true }
  if(!$rejected) { throw 'Incomplete cohort accepted' }
  'PASS: readiness reads reject missing/partial reports and enforce full cohorts'
} finally { Remove-Item -LiteralPath $file -ErrorAction SilentlyContinue }
