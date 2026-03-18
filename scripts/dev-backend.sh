#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

cleanup() {
  kill 0
}

trap cleanup EXIT INT TERM

cd "$ROOT_DIR"

npm run dev:api &
npm run dev:whatsapp &

wait
