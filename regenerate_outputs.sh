#!/usr/bin/env bash
set -euo pipefail

# Prefer python3 (guaranteed present on modern macOS/Linux per PEP 394; a bare
# "python" often isn't on PATH outside an activated venv) but respect an
# explicit override and fall back to "python" if that's all a system has.
PYTHON="${PYTHON:-python3}"
command -v "$PYTHON" >/dev/null 2>&1 || PYTHON="python"

"$PYTHON" phase_2/regenerate_all_outputs.py \
  --db 23727550-sq26-combined.db \
  "$@"
