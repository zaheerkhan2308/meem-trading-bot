# Restarts only Meem's local backend (8000) and Vite frontend (5173).
$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
$logs = Join-Path $root "logs"
$python = Join-Path $root ".venv\Scripts\python.exe"
$env:MEEM_ENV_FILE = ".env.local"

if (-not (Test-Path $python)) {
    throw "Missing .venv. Run: python -m venv .venv; .\.venv\Scripts\python.exe -m pip install -r backend\requirements.txt"
}

New-Item -ItemType Directory -Force -Path $logs | Out-Null
$listenerPids = netstat -ano -p tcp |
    Select-String "LISTENING" |
    Where-Object { $_.Line -match ":(8000|5173)\s+.*LISTENING" } |
    ForEach-Object { [int](($_.Line -split "\s+")[-1]) } |
    Sort-Object -Unique

foreach ($listenerPid in $listenerPids) {
    Write-Host "Stopping existing Meem listener (PID $listenerPid)"
    Stop-Process -Id $listenerPid -Force -ErrorAction SilentlyContinue
}

Start-Sleep -Milliseconds 750
Start-Process -FilePath $python -ArgumentList "-m", "backend.main" `
    -WorkingDirectory $root -WindowStyle Hidden `
    -RedirectStandardOutput (Join-Path $logs "backend-console.log") `
    -RedirectStandardError (Join-Path $logs "backend-error.log")

Start-Process -FilePath $env:ComSpec -ArgumentList "/c", "npm run dev -- --host 127.0.0.1" `
    -WorkingDirectory (Join-Path $root "frontend") -WindowStyle Hidden

Write-Host "Meem is restarting."
Write-Host "Dashboard: http://localhost:5173"
Write-Host "API:       http://localhost:8000/docs"
Write-Host "Logs:      $logs"
