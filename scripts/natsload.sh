#!/usr/bin/env bash
#
# Runs scripts/natsload. All arguments are passed through.
#
#   ./scripts/natsload.sh --clients=100 --requests=10000
#   ./scripts/natsload.sh --clients=10 --requests=1000 --size=65536

set -euo pipefail

command -v go >/dev/null || { echo "go is not installed" >&2; exit 1; }

exec go run -C "$(dirname "$0")/natsload" . "$@"
