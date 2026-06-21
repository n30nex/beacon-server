param(
  [string]$ApiBase = "http://127.0.0.1:8080",
  [string[]]$WebBases = @("http://127.0.0.1:5174", "http://127.0.0.1:5173", "http://127.0.0.1:5183"),
  [int]$TimeoutSec = 20,
  [switch]$SkipWebSocket,
  [switch]$RequireLocalPorts
)

$ErrorActionPreference = "Stop"

$results = New-Object System.Collections.Generic.List[object]

function Add-Check {
  param(
    [string]$Name,
    [string]$Status,
    [string]$Detail
  )

  $results.Add([pscustomobject]@{
    Check = $Name
    Status = $Status
    Detail = $Detail
  })
}

function Format-Detail {
  param([object]$Value)
  if ($null -eq $Value) { return "" }
  $text = [string]$Value
  if ($text.Length -gt 180) { return $text.Substring(0, 177) + "..." }
  return $text
}

function Invoke-JsonCheck {
  param(
    [string]$Name,
    [string]$Path,
    [scriptblock]$Validate,
    [scriptblock]$Summarize
  )

  $uri = "$($ApiBase.TrimEnd('/'))/$($Path.TrimStart('/'))"
  try {
    $data = Invoke-RestMethod -Uri $uri -TimeoutSec $TimeoutSec
    $ok = if ($Validate) { & $Validate $data } else { $true }
    if ($ok) {
      $detail = if ($Summarize) { & $Summarize $data } else { "ok" }
      Add-Check $Name "PASS" (Format-Detail $detail)
    } else {
      Add-Check $Name "FAIL" "unexpected response from $uri"
    }
  } catch {
    Add-Check $Name "FAIL" (Format-Detail $_.Exception.Message)
  }
}

function Test-TcpPort {
  param(
    [string]$Name,
    [string]$HostName,
    [int]$Port,
    [bool]$Required = $false
  )

  $client = [System.Net.Sockets.TcpClient]::new()
  try {
    $task = $client.ConnectAsync($HostName, $Port)
    $ok = $task.Wait([TimeSpan]::FromSeconds([Math]::Min($TimeoutSec, 5)))
    if ($ok -and $client.Connected) {
      Add-Check $Name "PASS" "$HostName`:$Port accepting TCP"
    } else {
      $status = if ($Required) { "FAIL" } else { "WARN" }
      Add-Check $Name $status "$HostName`:$Port timed out"
    }
  } catch {
    $status = if ($Required) { "FAIL" } else { "WARN" }
    Add-Check $Name $status (Format-Detail $_.Exception.Message)
  } finally {
    $client.Dispose()
  }
}

function Test-Web {
  foreach ($base in $WebBases) {
    try {
      $res = Invoke-WebRequest -UseBasicParsing -Uri $base -TimeoutSec $TimeoutSec
      if ($res.StatusCode -ge 200 -and $res.StatusCode -lt 500) {
        Add-Check "web" "PASS" "$base returned HTTP $($res.StatusCode)"
        return
      }
    } catch {
      # Try the next configured Vite/prod web URL.
    }
  }
  Add-Check "web" "FAIL" "no configured web URL responded: $($WebBases -join ', ')"
}

function Test-WebSocketHello {
  if ($SkipWebSocket) {
    Add-Check "websocket" "SKIP" "skipped by -SkipWebSocket"
    return
  }

  $root = $ApiBase.TrimEnd('/') -replace '/api/v1$', ''
  $wsUrl = $root -replace '^https:', 'wss:'
  $wsUrl = $wsUrl -replace '^http:', 'ws:'
  $wsUrl = "$($wsUrl.TrimEnd('/'))/ws"

  $client = [System.Net.WebSockets.ClientWebSocket]::new()
  $cts = [System.Threading.CancellationTokenSource]::new([TimeSpan]::FromSeconds($TimeoutSec))
  try {
    $null = $client.ConnectAsync([Uri]$wsUrl, $cts.Token).GetAwaiter().GetResult()
    $buffer = [byte[]]::new(4096)
    $segment = [ArraySegment[byte]]::new($buffer)
    $msg = $client.ReceiveAsync($segment, $cts.Token).GetAwaiter().GetResult()
    $text = [System.Text.Encoding]::UTF8.GetString($buffer, 0, $msg.Count)
    if ($text -match '"type"\s*:\s*"hello"') {
      Add-Check "websocket" "PASS" "received hello from $wsUrl"
    } else {
      Add-Check "websocket" "FAIL" "first frame was not hello: $(Format-Detail $text)"
    }
  } catch {
    Add-Check "websocket" "FAIL" (Format-Detail $_.Exception.Message)
  } finally {
    try {
      if ($client.State -eq [System.Net.WebSockets.WebSocketState]::Open) {
        $null = $client.CloseAsync([System.Net.WebSockets.WebSocketCloseStatus]::NormalClosure, "smoke complete", [System.Threading.CancellationToken]::None).GetAwaiter().GetResult()
      }
    } catch {
      # Closing after a failed probe is best effort.
    }
    $client.Dispose()
    $cts.Dispose()
  }
}

Invoke-JsonCheck "healthz" "/healthz" `
  { param($data) $data.status -in @("ok", "degraded") -and $data.dependencies.database.status -eq "ok" } `
  { param($data) "status=$($data.status); db=$($data.dependencies.database.status); cache=$($data.dependencies.cache.status); brokers=$($data.brokers.Count)" }

Invoke-JsonCheck "brokers" "/api/v1/brokers" `
  { param($data) $null -ne $data } `
  { param($data) "count=$($data.Count); connected=$(($data | Where-Object { $_.connected }).Count)" }

Invoke-JsonCheck "atlas briefing" "/api/v1/atlas/briefing" `
  { param($data) $null -ne $data } `
  { param($data) "regions=$($data.regions.Count); priorities=$($data.priorities.Count); health=$($data.health.status)" }

Invoke-JsonCheck "live backfill" "/api/v1/live/backfill?afterObservationId=0&limit=1" `
  { param($data) $null -ne $data } `
  { param($data) "items=$($data.items.Count); hasMore=$($data.hasMore)" }

Invoke-JsonCheck "global search" "/api/v1/search?q=atlas&limit=3" `
  { param($data) $null -ne $data.items } `
  { param($data) "items=$($data.items.Count)" }

Test-Web
Test-WebSocketHello
Test-TcpPort "postgres port" "127.0.0.1" 5432 $RequireLocalPorts.IsPresent
Test-TcpPort "redis port" "127.0.0.1" 6379 $RequireLocalPorts.IsPresent

$results | Format-Table -AutoSize

if ($results | Where-Object { $_.Status -eq "FAIL" }) {
  exit 1
}
