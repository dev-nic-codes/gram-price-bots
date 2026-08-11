$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot
if (-not (Test-Path ".env")) { throw "Missing .env. Copy .env.example to .env and configure it first." }
python main.py
