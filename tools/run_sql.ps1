# runs one SQL file on the project warehouse and prints each result row as column: value.
# usage: .\tools\run_sql.ps1 sql\final_evaluation.sql
param(
  [Parameter(Mandatory = $true)][string]$SqlFile
)

[string]$Profile = "dbc-609647e1-c073"
[string]$WarehouseId = "606b0c60dc21c8fd"
[string]$BodyFile = Join-Path $env:TEMP "run_sql_body.json"

# ReadAllText returns a plain string; Get-Content -Raw in PowerShell 5.1 adds properties that ConvertTo-Json serializes
[string]$Statement = [IO.File]::ReadAllText((Resolve-Path $SqlFile).Path)

$Body = @{
  warehouse_id = $WarehouseId
  statement    = $Statement
  wait_timeout = "50s"
} | ConvertTo-Json
[IO.File]::WriteAllText($BodyFile, $Body)

$Response = (databricks api post /api/2.0/sql/statements --json "@$BodyFile" --profile $Profile | ConvertFrom-Json)
Remove-Item $BodyFile -ErrorAction SilentlyContinue
if ($LASTEXITCODE -ne 0 -or -not $Response) { Write-Host "statement request failed" -ForegroundColor Red ; exit 1 }

while ($Response.status.state -in @("PENDING", "RUNNING")) {
  Start-Sleep 5
  $Response = (databricks api get "/api/2.0/sql/statements/$($Response.statement_id)" --profile $Profile | ConvertFrom-Json)
}

if ($Response.status.state -ne "SUCCEEDED") {
  Write-Host "statement $($Response.status.state)" -ForegroundColor Red
  $Response.status | ConvertTo-Json -Depth 5
  exit 1
}

$Columns = $Response.manifest.schema.columns
foreach ($Row in $Response.result.data_array) {
  for ($ColumnIndex = 0; $ColumnIndex -lt $Columns.Count; $ColumnIndex++) {
    Write-Host "$($Columns[$ColumnIndex].name): $($Row[$ColumnIndex])"
  }
  Write-Host ""
}