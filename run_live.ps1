# one-command live run: stack up, redis backfill, scoring service and lakehouse sink in the background,
# replay, wait until kafka shows both consumers fully caught up, stop them, run the databricks live job,
# then stop.ps1 removes everything local. logs go to data\logs and are printed on failure.
# usage: .\run_live.ps1                            (3000 transactions at 1000x, sink batches of 500)
#        .\run_live.ps1 -Limit 20000 -Speed 5000
#        .\run_live.ps1 -Limit 555719 -NoPacing    (full fraudTest as fast as possible, sink batches of 25000;
#                                                   latency then includes queue time)
param(
  [int]$Limit = 3000,
  [double]$Speed = 1000,
  [switch]$NoPacing,
  [int]$SinkBatchSize = 0
)

[string]$Profile = "dbc-609647e1-c073"
[string]$Python = Join-Path $PSScriptRoot ".venv\Scripts\python.exe"
[string]$LogFolder = Join-Path $PSScriptRoot "data\logs"
[int]$CatchUpTimeoutSeconds = 300 + [int]($Limit / 20)
if ($SinkBatchSize -le 0) { $SinkBatchSize = if ($NoPacing) { 25000 } else { 500 } }

function Start-Background([string]$Name, [string]$Arguments) {
  New-Item -ItemType Directory -Path $LogFolder -Force | Out-Null
  return Start-Process -FilePath $Python -ArgumentList $Arguments -WorkingDirectory $PSScriptRoot -NoNewWindow -PassThru `
    -RedirectStandardOutput (Join-Path $LogFolder "$Name.out.log") -RedirectStandardError (Join-Path $LogFolder "$Name.log")
}

function Wait-ForLogText($Process, [string]$Name, [string]$Text, [int]$TimeoutSeconds) {
  [string]$LogFile = Join-Path $LogFolder "$Name.log"
  $Deadline = (Get-Date).AddSeconds($TimeoutSeconds)
  while ((Get-Date) -lt $Deadline) {
    if ($Process.HasExited) { throw "$Name exited early" }
    if ((Test-Path $LogFile) -and (Select-String -Path $LogFile -Pattern $Text -SimpleMatch -Quiet)) { return }
    Start-Sleep 2
  }
  throw "$Name did not log '$Text' within $TimeoutSeconds seconds"
}

function Get-ConsumerLag([string]$Group) {
  # sums LAG over the group's partitions; -1 means not known yet (no committed offset on a partition with data)
  $Lines = docker exec fraud-kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group $Group 2>$null
  [long]$TotalLag = 0
  [int]$PartitionCount = 0
  foreach ($Line in $Lines) {
    $Columns = $Line.Trim() -split '\s+'
    if ($Columns.Count -lt 6 -or $Columns[0] -ne $Group) { continue }
    $PartitionCount++
    if ($Columns[5] -eq '-') {
      if ($Columns[4] -eq '0') { continue }
      return -1
    }
    $TotalLag += [long]$Columns[5]
  }
  if ($PartitionCount -eq 0) { return -1 }
  return $TotalLag
}

function Wait-ForCatchUp([string]$Group, [int]$TimeoutSeconds) {
  $Deadline = (Get-Date).AddSeconds($TimeoutSeconds)
  while ((Get-Date) -lt $Deadline) {
    [long]$Lag = Get-ConsumerLag $Group
    if ($Lag -eq 0) { Write-Host "$Group caught up" -ForegroundColor Green ; return }
    Write-Host "$Group lag: $(if ($Lag -lt 0) { 'not committed yet' } else { $Lag })"
    Start-Sleep 5
  }
  throw "$Group did not catch up within $TimeoutSeconds seconds"
}

function Show-LogTails {
  if (-not (Test-Path $LogFolder)) { return }
  foreach ($LogFile in Get-ChildItem $LogFolder -Filter *.log) {
    Write-Host "===== $($LogFile.Name) (last 20 lines)" -ForegroundColor Yellow
    Get-Content $LogFile.FullName -Tail 20
  }
}

$ScoringService = $null
$LakehouseSink = $null
[bool]$Succeeded = $false
try {
  & (Join-Path $PSScriptRoot "start.ps1")

  & $Python streaming\redis_backfill.py
  if ($LASTEXITCODE -ne 0) { throw "redis backfill failed" }

  $ScoringService = Start-Background "scoring_service" "streaming\scoring_service.py"
  $LakehouseSink = Start-Background "lakehouse_sink" "streaming\lakehouse_sink.py --batch-size $SinkBatchSize"
  Wait-ForLogText $ScoringService "scoring_service" "waiting for transactions" 300
  Wait-ForLogText $LakehouseSink "lakehouse_sink" "sinking" 120
  Write-Host "scoring service and lakehouse sink running (sink batch size $SinkBatchSize)" -ForegroundColor Green

  [string[]]$ReplayArguments = @("streaming\replay_producer.py", "--limit", "$Limit", "--speed", "$Speed")
  if ($NoPacing) { $ReplayArguments += "--no-pacing" }
  & $Python @ReplayArguments
  if ($LASTEXITCODE -ne 0) { throw "replay producer failed" }

  # scores are published before the scoring service commits, so once it is caught up every score is in kafka;
  # the sink commits only after uploading, so once it is caught up every message is in the landing volume
  Wait-ForCatchUp "fraud-scoring-service" $CatchUpTimeoutSeconds
  Wait-ForCatchUp "fraud-lakehouse-sink" $CatchUpTimeoutSeconds
  Get-Content (Join-Path $LogFolder "scoring_service.log") -Tail 3

  Stop-Process -Id $ScoringService.Id, $LakehouseSink.Id -Force
  $ScoringService = $null
  $LakehouseSink = $null

  databricks bundle run fraud_live_lakehouse --profile $Profile
  if ($LASTEXITCODE -ne 0) { throw "fraud_live_lakehouse job failed" }

  $Succeeded = $true
}
finally {
  foreach ($BackgroundProcess in @($ScoringService, $LakehouseSink)) {
    if ($BackgroundProcess -and -not $BackgroundProcess.HasExited) { Stop-Process -Id $BackgroundProcess.Id -Force }
  }
  if (-not $Succeeded) { Show-LogTails }
  & (Join-Path $PSScriptRoot "stop.ps1")
  if ($Succeeded) { Write-Host "live run done: $Limit transactions scored, landed and loaded" -ForegroundColor Green }
  else { Write-Host "live run failed, see the log tails above" -ForegroundColor Red }
}