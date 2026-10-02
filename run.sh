#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"

if [[ ! -f .env ]]; then
  echo "Missing .env file. Copy .env.example to .env and set MSSQL_SA_PASSWORD." >&2
  exit 1
fi

docker compose up -d "$@"
