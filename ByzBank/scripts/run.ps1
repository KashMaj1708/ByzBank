<#
.SYNOPSIS
  Process-orchestration harness for the 2pcbyz BFT sharded 2PC system (Windows).

.DESCRIPTION
  Replaces the Unix Makefile on Windows/PowerShell. Targets:
    build   -> compile everything (go build ./...) and produce bin\server.exe, bin\client.exe
    up      -> launch all servers as background processes, recording PIDs in run\pids.txt
    down    -> stop every server recorded in run\pids.txt, cleanly
    keys    -> generate per-server ed25519 keypairs (Phase 1; currently a stub)
    test    -> go test ./...
    health  -> probe every server's /health endpoint via the client
    status  -> show which recorded PIDs are still alive

.EXAMPLE
  .\scripts\run.ps1 build
  .\scripts\run.ps1 up
  .\scripts\run.ps1 health
  .\scripts\run.ps1 down
#>
param(
  [Parameter(Mandatory = $true, Position = 0)]
  [ValidateSet('build', 'up', 'down', 'keys', 'test', 'health', 'status')]
  [string]$Target
)

$ErrorActionPreference = 'Stop'

# Repo root = parent of the scripts directory.
$RepoRoot = Split-Path -Parent $PSScriptRoot
$RunDir = Join-Path $RepoRoot 'run'
$BinDir = Join-Path $RepoRoot 'bin'
$PidFile = Join-Path $RunDir 'pids.txt'
$LogDir = Join-Path $RunDir 'logs'

# Resolve the go binary: prefer PATH, else fall back to the user SDK install.
function Get-Go {
  $g = Get-Command go -ErrorAction SilentlyContinue
  if ($g) { return $g.Source }
  $fallback = Join-Path $env:USERPROFILE 'sdk\go\bin\go.exe'
  if (Test-Path $fallback) { return $fallback }
  throw "go toolchain not found on PATH or at $fallback"
}

function Invoke-Build {
  $go = Get-Go
  New-Item -ItemType Directory -Force -Path $BinDir | Out-Null
  Push-Location $RepoRoot
  try {
    Write-Host "go build ./..."
    & $go build ./...
    if ($LASTEXITCODE -ne 0) { throw "go build failed ($LASTEXITCODE)" }
    & $go build -o (Join-Path $BinDir 'server.exe') ./cmd/server
    if ($LASTEXITCODE -ne 0) { throw "server build failed ($LASTEXITCODE)" }
    & $go build -o (Join-Path $BinDir 'client.exe') ./cmd/client
    if ($LASTEXITCODE -ne 0) { throw "client build failed ($LASTEXITCODE)" }
    Write-Host "build OK -> $BinDir"
  }
  finally { Pop-Location }
}

function Get-ServerIds {
  # Mirror config.Default(): 3 clusters x 12 = 36 servers, overridable via env.
  $numClusters = if ($env:BYZ_NUM_CLUSTERS) { [int]$env:BYZ_NUM_CLUSTERS } else { 3 }
  $clusterSize = if ($env:BYZ_CLUSTER_SIZE) { [int]$env:BYZ_CLUSTER_SIZE } else { 12 }
  1..($numClusters * $clusterSize)
}

function Invoke-Up {
  $server = Join-Path $BinDir 'server.exe'
  if (-not (Test-Path $server)) {
    Write-Host "server.exe not found; building first..."
    Invoke-Build
  }
  New-Item -ItemType Directory -Force -Path $RunDir | Out-Null
  New-Item -ItemType Directory -Force -Path $LogDir | Out-Null
  if (Test-Path $PidFile) {
    Write-Host "pids.txt already exists; run 'down' first." -ForegroundColor Yellow
    return
  }
  $pids = @()
  foreach ($n in Get-ServerIds) {
    $id = "S$n"
    $out = Join-Path $LogDir "$id.out.log"
    $err = Join-Path $LogDir "$id.err.log"
    $p = Start-Process -FilePath $server -ArgumentList @('--id', $id) `
      -RedirectStandardOutput $out -RedirectStandardError $err `
      -WindowStyle Hidden -PassThru
    $pids += "$id $($p.Id)"
  }
  $pids | Set-Content -Path $PidFile -Encoding ASCII
  Write-Host "started $($pids.Count) servers; pids in $PidFile"
}

function Invoke-Down {
  if (-not (Test-Path $PidFile)) {
    Write-Host "no pids.txt; nothing to stop."
    return
  }
  $stopped = 0
  foreach ($line in Get-Content $PidFile) {
    $parts = $line -split '\s+'
    if ($parts.Count -lt 2) { continue }
    $procId = [int]$parts[1]
    $proc = Get-Process -Id $procId -ErrorAction SilentlyContinue
    if ($proc) {
      Stop-Process -Id $procId -Force -ErrorAction SilentlyContinue
      $stopped++
    }
  }
  Remove-Item $PidFile -ErrorAction SilentlyContinue
  Write-Host "stopped $stopped servers; removed $PidFile"
}

function Invoke-Status {
  if (-not (Test-Path $PidFile)) { Write-Host "no pids.txt; servers not tracked."; return }
  $alive = 0; $dead = 0
  foreach ($line in Get-Content $PidFile) {
    $parts = $line -split '\s+'
    if ($parts.Count -lt 2) { continue }
    $procId = [int]$parts[1]
    if (Get-Process -Id $procId -ErrorAction SilentlyContinue) {
      $alive++
    } else { $dead++ }
  }
  Write-Host "tracked servers: $alive alive, $dead dead"
}

function Invoke-Keys {
  $go = Get-Go
  Push-Location $RepoRoot
  try {
    & $go run ./cmd/keygen --out (Join-Path $RepoRoot 'config\keys')
    if ($LASTEXITCODE -ne 0) { throw "keygen failed ($LASTEXITCODE)" }
  }
  finally { Pop-Location }
}

function Invoke-Test {
  $go = Get-Go
  Push-Location $RepoRoot
  try {
    & $go test ./...
    if ($LASTEXITCODE -ne 0) { throw "go test failed ($LASTEXITCODE)" }
  }
  finally { Pop-Location }
}

function Invoke-Health {
  $client = Join-Path $BinDir 'client.exe'
  if (-not (Test-Path $client)) { Invoke-Build }
  & $client --healthcheck
}

switch ($Target) {
  'build' { Invoke-Build }
  'up' { Invoke-Up }
  'down' { Invoke-Down }
  'status' { Invoke-Status }
  'keys' { Invoke-Keys }
  'test' { Invoke-Test }
  'health' { Invoke-Health }
}
