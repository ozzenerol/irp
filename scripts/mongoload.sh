#!/usr/bin/env bash
#
# Runs scripts/mongoload. All arguments are passed through.
#
#   ./scripts/mongoload.sh --clients=100 --requests=10000
#   ./scripts/mongoload.sh --clients=100 --requests=10000 --pool=10

set -euo pipefail

command -v go >/dev/null || { echo "go is not installed" >&2; exit 1; }

exec go run -C "$(dirname "$0")/mongoload" . "$@"
