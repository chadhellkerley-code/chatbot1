#!/usr/bin/env bash
set -euo pipefail
# Activate venv and run the app
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

if [ ! -d "venv311" ]; then
  echo "No virtualenv found. Run ./setup_mac.sh first."
  exit 1
fi

source "venv311/bin/activate"
python3 app.py
