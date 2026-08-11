#!/usr/bin/env sh
set -eu
cd "$(dirname "$0")"
[ -f .env ] || { echo "Missing .env. Copy .env.example to .env and configure it first." >&2; exit 1; }
exec python3 main.py
