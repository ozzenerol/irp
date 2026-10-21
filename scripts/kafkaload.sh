#!/usr/bin/env bash
#
# Runs scripts/kafkaload. All arguments are passed through.
#
#   ./scripts/kafkaload.sh --clients=100 --requests=10000
#   ./scripts/kafkaload.sh --clients=10 --requests=1000 --size=65536

set -euo pipefail

command -v go >/dev/null || { echo "go is not installed" >&2; exit 1; }

exec go run -C "$(dirname "$0")/kafkaload" . "$@"
