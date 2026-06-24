#!/usr/bin/env bash
# Run the data engineering pipelines end to end: A.3 -> A.4 -> A.5.
set -euo pipefail

cd "$(dirname "$0")/.."

echo "==> A.3 Ingestion (Landing Zone)"
python -m src.ingestion --source all

echo "==> A.4 Formatting (Formatted Zone)"
python -m src.formatting --dataset all

echo "==> A.5 Exploitation (Exploitation Zone)"
python -m src.exploitation

echo "==> Done."
