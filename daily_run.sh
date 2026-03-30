#!/usr/bin/env bash
# Daily automation: scrape today's report then validate new addresses.
# Scheduled via cron to run at 8am.

set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON="$DIR/.venv/bin/python"
LOG="$DIR/output/daily.log"

mkdir -p "$DIR/output"

echo "======================================" >> "$LOG"
echo "$(date '+%Y-%m-%d %H:%M:%S') — daily run starting" >> "$LOG"

# 1. Scrape
echo "--- scraper ---" >> "$LOG"
"$PYTHON" "$DIR/scraper.py" >> "$LOG" 2>&1 && \
  echo "Scraper OK" >> "$LOG" || \
  echo "Scraper FAILED (exit $?)" >> "$LOG"

# 2. Validate new addresses (skip if no API key set)
if [ -f "$DIR/.env" ] && grep -q "GOOGLE_API_KEY=." "$DIR/.env"; then
  echo "--- address validation ---" >> "$LOG"
  "$PYTHON" "$DIR/validate_addresses.py" >> "$LOG" 2>&1 && \
    echo "Validation OK" >> "$LOG" || \
    echo "Validation FAILED (exit $?)" >> "$LOG"
else
  echo "--- address validation skipped (no GOOGLE_API_KEY) ---" >> "$LOG"
fi

echo "$(date '+%Y-%m-%d %H:%M:%S') — daily run complete" >> "$LOG"
