#!/usr/bin/env bash
# R4 run-skill (pr-hero-i5i): ONE owner-facing command for the telegram-mcp contour.
# Runs the doctor, renders green/red + next action per red layer, writes a STOP-flag
# the SwitchBar reads. --deploy also runs the deploy first (agent side; owner still
# enters the 2 SMS codes once). This is the «90% через run skill» entrypoint.
#
#   deploy/run-lifecycle.sh              # just check + status
#   deploy/run-lifecycle.sh --deploy     # deploy then check
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
APP="$(cd "$HERE/.." && pwd)"

if [ "${1:-}" = "--deploy" ]; then
  echo "[run-lifecycle] deploying (agent side)…"
  bash "$HERE/deploy-sandbox-ik.sh" "${@:2}"
fi

echo "[run-lifecycle] checking contour…"
python3 "$APP/telegram_mcp_doctor.py" --json 2>/dev/null | python3 "$HERE/render_lifecycle_status.py"
