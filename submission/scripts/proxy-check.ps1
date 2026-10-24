$ErrorActionPreference = 'Stop'
Set-Location (Split-Path $PSScriptRoot -Parent)
try {
  docker compose stop app1 app2 app3
  if ($LASTEXITCODE -ne 0) { throw 'Replica stop failed' }
  $exitCodes = @(docker inspect phi-messaging-app1-1 phi-messaging-app2-1 phi-messaging-app3-1 --format '{{.State.ExitCode}}')
  if ($LASTEXITCODE -ne 0 -or @($exitCodes | Where-Object { $_ -ne '0' }).Count) { throw 'Replica graceful exit was not clean' }
  $request = [Net.WebRequest]::Create('http://127.0.0.1:8080/health/ready')
  $request.Timeout = 5000
  $response = $null
  try { $response = $request.GetResponse() } catch [Net.WebException] { $response = $_.Exception.Response }
  if (!$response) { throw 'No proxy response' }
  $reader = New-Object IO.StreamReader($response.GetResponseStream())
  $body = $reader.ReadToEnd() | ConvertFrom-Json
  $status = [int]$response.StatusCode
  $reader.Close()
  $response.Close()
  if ($status -ne 503 -or $body.code -ne 'unavailable') { throw 'Proxy unavailable error violated JSON contract' }
  Write-Output 'PASS: clean replica exits and proxy 503 JSON'
} finally {
  docker compose start app1 app2 app3
  if ($LASTEXITCODE -ne 0) { throw 'Replica restore failed' }
}
