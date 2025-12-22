#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Choose Python 3.11 if available
PY_BIN="$(command -v python3.11 || command -v python3.10 || command -v python3)"
echo "Using Python: $PY_BIN"

$PY_BIN -m venv venv311
source venv311/bin/activate
python -m pip install --upgrade pip wheel

# Install requirements
if [ -f requirements.txt ]; then
  pip install -r requirements.txt
else
  pip install instagrapi==1.19.6 pydantic==1.10.13 Pillow typing_extensions>=4.8.0 "requests<3" rich openai
fi

echo "✅ Setup complete. Run ./run_mac.sh to start."
