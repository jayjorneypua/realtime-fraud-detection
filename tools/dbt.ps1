# runs dbt from .venv-dbt against the project warehouse, with a short-lived token from the databricks cli login.
# the token only lives in this script's environment and is cleared at the end.
# usage: .\tools\dbt.ps1 build    |    .\tools\dbt.ps1 debug
param(
  [Parameter(ValueFromRemainingArguments = $true)][string[]]$DbtArguments
)

[string]$DatabricksProfile = "dbc-609647e1-c073"
[string]$WorkspaceUrl = "https://dbc-609647e1-c073.cloud.databricks.com"
[string]$RepositoryRoot = Split-Path $PSScriptRoot -Parent
[string]$DbtExecutable = Join-Path $RepositoryRoot ".venv-dbt\Scripts\dbt.exe"
[string]$DbtFolder = Join-Path $RepositoryRoot "dbt"

$TokenResponse = (databricks auth token --profile $DatabricksProfile | ConvertFrom-Json)
if (-not $TokenResponse.access_token) { Write-Host "could not get a token from the databricks cli" -ForegroundColor Red ; exit 1 }

try {
  $env:DATABRICKS_HOST = $WorkspaceUrl
  $env:DATABRICKS_TOKEN = $TokenResponse.access_token
  & $DbtExecutable @DbtArguments --project-dir $DbtFolder --profiles-dir $DbtFolder
  $DbtExitCode = $LASTEXITCODE
}
finally {
  Remove-Item Env:DATABRICKS_HOST -ErrorAction SilentlyContinue
  Remove-Item Env:DATABRICKS_TOKEN -ErrorAction SilentlyContinue
}
exit $DbtExitCode