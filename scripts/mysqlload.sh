#!/usr/bin/env bash
#
# Runs scripts/mysqlload. All arguments are passed through.
#
#   ./scripts/mysqlload.sh --clients=100 --requests=10000
#   ./scripts/mysqlload.sh --clients=100 --requests=10000 --pool=10

set -euo pipefail

command -v go >/dev/null || { echo "go is not installed" >&2; exit 1; }

exec go run -C "$(dirname "$0")/mysqlload" . "$@"
