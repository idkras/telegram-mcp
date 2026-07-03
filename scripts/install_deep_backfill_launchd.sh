#!/usr/bin/env bash
# Install per-profile launchd agent для hourly deep_backfill.
#
# Usage:
#   bash scripts/install_deep_backfill_launchd.sh ikrasinsky
#   bash scripts/install_deep_backfill_launchd.sh lisa
#
# Универсально: новый профиль = один запуск с alias'ом. Никаких client-specific
# patches: tmpl читается из com.rickai.telegram-deep-backfill.plist, заменяются
# {{PROFILE}} и {{WORKSPACE_ROOT}}, кладётся в ~/Library/LaunchAgents,
# регистрируется через launchctl bootstrap.
set -euo pipefail

# Bug M4 (pr-hero-1u1): this installer rendered com.rickai.telegram-deep-backfill.plist,
# which is a DEPRECATED /bin/echo stub (Disabled=true, Label mismatched). Running it
# registered a disabled echo — the operator saw «registered: …» and believed hourly
# backfill was scheduled, while nothing ran. deep-backfill now runs INSIDE the periodic
# catch-up cycle. Refuse loudly with a redirect instead of registering a dead stub.
cat >&2 <<'DEPRECATED'
[deprecated] install_deep_backfill_launchd.sh no longer schedules anything.
deep-backfill now runs inside the periodic catch-up cycle. The real scheduler is:
  com.rickai.telegram-periodic-catchup  (install_periodic_catchup.sh, parent skill
  4-rick-kj-rule-create/scripts/telegram_periodic_catchup.py)
This script is kept only to fail loudly instead of registering a disabled /bin/echo stub.
DEPRECATED
exit 2

PROFILE="${1:-}"
if [[ -z "$PROFILE" ]]; then
  echo "usage: $0 <profile-alias>" >&2
  exit 64
fi

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
WORKSPACE_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
TEMPLATE="$SCRIPT_DIR/com.rickai.telegram-deep-backfill.plist"
TARGET_DIR="$HOME/Library/LaunchAgents"
TARGET="$TARGET_DIR/com.rickai.telegram-deep-backfill-${PROFILE}.plist"
LABEL="com.rickai.telegram-deep-backfill-${PROFILE}"

if [[ ! -f "$TEMPLATE" ]]; then
  echo "Template not found: $TEMPLATE" >&2
  exit 1
fi
if [[ ! -x "$WORKSPACE_ROOT/.venv/bin/python" ]]; then
  echo "Workspace venv not found: $WORKSPACE_ROOT/.venv/bin/python" >&2
  exit 1
fi

mkdir -p "$TARGET_DIR"

# Render plist (POSIX-safe sed)
sed -e "s|{{PROFILE}}|${PROFILE}|g" \
    -e "s|{{WORKSPACE_ROOT}}|${WORKSPACE_ROOT}|g" \
    "$TEMPLATE" > "$TARGET"

chmod 644 "$TARGET"
echo "wrote: $TARGET"

# Reload (idempotent: bootout перед bootstrap игнорит "not loaded")
launchctl bootout "gui/$UID/${LABEL}" 2>/dev/null || true
launchctl bootstrap "gui/$UID" "$TARGET"
echo "registered: $LABEL"

echo "verify: launchctl list | grep $LABEL"
launchctl list | grep "$LABEL" || echo "(not yet visible — first interval=3600s)"
