#!/usr/bin/env bash
# Idempotent deploy of Telegram MCP ingest listeners to sandbox-ik (pr-hero-xf6, R1).
#
# Ubuntu 24.04, systemd, user idkras. One systemd service per profile
# (telegram-mcp-<profile>) so the doctor + SwitchBar see per-account status.
# Safe to re-run: pulls latest code, re-renders units, restarts changed ones.
#
# Usage (on sandbox-ik, or via ssh):
#   deploy/deploy-sandbox-ik.sh [--profiles ikrasinsky,lisa] [--dry-run]
#
# Secrets are NOT baked in: each /etc/telegram-mcp/env.d/<profile>.env is created
# from *.env.example (Keychain key NAMES) and must be filled with the real
# TELEGRAM_SESSION_STRING per endpoint (owner does SMS re-auth once — §session-per-endpoint).
set -euo pipefail

REPO_URL="${TELEGRAM_MCP_REPO:-https://github.com/idkras/telegram-mcp.git}"
APP_USER="${TELEGRAM_MCP_USER:-idkras}"
APP_DIR="${TELEGRAM_MCP_APP_DIR:-/home/$APP_USER/telegram-mcp}"
ENV_DIR="/etc/telegram-mcp/env.d"
PROFILES="ikrasinsky,lisa"
DRY_RUN=0

while [ $# -gt 0 ]; do
  case "$1" in
    --profiles) PROFILES="$2"; shift 2 ;;
    --dry-run)  DRY_RUN=1; shift ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done

# ── RCE guard (pr-hero-xf6, found by code-reviewer squad 2026-07-03) ──────────
# run() below uses `eval` (needed for the `&&`/redirect command strings), so EVERY
# value that gets interpolated into a run-string MUST be validated first — otherwise
# a crafted --profiles / TELEGRAM_MCP_USER / TELEGRAM_MCP_APP_DIR (e.g. `x;rm -rf ~`
# or a $(...) / backtick) executes under sudo. Validate the untrusted inputs to safe
# shapes; after this, no interpolated var can carry shell metacharacters.
_die() { echo "[deploy-sandbox-ik] REFUSED: $1" >&2; exit 2; }
_valid_ident() { case "$1" in ''|*[!a-z0-9_]*) return 1 ;; *) return 0 ;; esac; }
_valid_ident "$APP_USER" || _die "unsafe TELEGRAM_MCP_USER='$APP_USER' (allowed: a-z0-9_)"
case "$APP_DIR"  in /*) : ;; *) _die "TELEGRAM_MCP_APP_DIR must be absolute: '$APP_DIR'" ;; esac
case "$APP_DIR"  in *['`$();|&<>*?'\''"'\ ]*) _die "unsafe TELEGRAM_MCP_APP_DIR='$APP_DIR'" ;; esac
case "$REPO_URL" in https://github.com/[A-Za-z0-9._/-]*.git) : ;; *) _die "unexpected TELEGRAM_MCP_REPO='$REPO_URL'" ;; esac
IFS=',' read -r -a _PROF_CHECK <<< "$PROFILES"
for _p in "${_PROF_CHECK[@]}"; do
  _valid_ident "$_p" || _die "unsafe profile '$_p' in --profiles (allowed: a-z0-9_)"
done

run() { if [ "$DRY_RUN" = 1 ]; then echo "DRY: $*"; else eval "$@"; fi; }
log() { echo "[deploy-sandbox-ik] $*"; }

HERE="$(cd "$(dirname "$0")" && pwd)"
TEMPLATE="$HERE/telegram-mcp.service.template"
[ -f "$TEMPLATE" ] || { echo "missing $TEMPLATE" >&2; exit 1; }

# 1. code: clone or pull (idempotent)
if [ -d "$APP_DIR/.git" ]; then
  log "pull latest into $APP_DIR"
  run "git -C '$APP_DIR' fetch origin main && git -C '$APP_DIR' reset --hard origin/main"
else
  log "clone $REPO_URL → $APP_DIR"
  run "git clone '$REPO_URL' '$APP_DIR'"
fi

# 2. venv + deps (idempotent — pip install is a no-op if satisfied)
if [ ! -x "$APP_DIR/.venv/bin/python" ]; then
  log "create venv"
  run "python3 -m venv '$APP_DIR/.venv'"
fi
log "install deps"
run "'$APP_DIR/.venv/bin/pip' install -q --upgrade pip"
run "'$APP_DIR/.venv/bin/pip' install -q telethon psycopg2-binary supabase pyyaml"

# 3. per-profile env skeleton (does NOT overwrite an existing filled env)
run "sudo mkdir -p '$ENV_DIR'"
IFS=',' read -r -a PROF_ARR <<< "$PROFILES"
for p in "${PROF_ARR[@]}"; do
  ex="$HERE/env.d/${p}.env.example"
  [ -f "$ex" ] || ex="$HERE/env.d/profile.env.example"
  tgt="$ENV_DIR/${p}.env"
  if [ "$DRY_RUN" = 1 ]; then
    echo "DRY: install env skeleton $tgt (from $(basename "$ex"))"
  elif [ -f "$tgt" ]; then
    log "env $tgt exists — keep (не перезаписываю secrets)"
  else
    sudo cp "$ex" "$tgt"; sudo chmod 600 "$tgt"
    log "env skeleton created $tgt — FILL TELEGRAM_SESSION_STRING (owner SMS re-auth)"
  fi
done

# 4. render + install systemd unit per profile (idempotent)
for p in "${PROF_ARR[@]}"; do
  unit="/etc/systemd/system/telegram-mcp-${p}.service"
  rendered="$(sed -e "s/__PROFILE__/${p}/g" -e "s/__USER__/${APP_USER}/g" \
                  -e "s#__APP_DIR__#${APP_DIR}#g" "$TEMPLATE")"
  if [ "$DRY_RUN" = 1 ]; then
    echo "DRY: write $unit"; echo "$rendered" | sed 's/^/    /'
  else
    echo "$rendered" | sudo tee "$unit" >/dev/null
    log "unit written $unit"
  fi
done

# 5. enable + (re)start
run "sudo systemctl daemon-reload"
for p in "${PROF_ARR[@]}"; do
  run "sudo systemctl enable telegram-mcp-${p}.service"
  # only start if env has a session string (else it would crash-loop pre-auth)
  if [ "$DRY_RUN" = 1 ]; then
    echo "DRY: start telegram-mcp-${p} if TELEGRAM_SESSION_STRING present"
  elif sudo grep -q "^TELEGRAM_SESSION_STRING=.\+" "$ENV_DIR/${p}.env" 2>/dev/null; then
    sudo systemctl restart "telegram-mcp-${p}.service"
    log "started telegram-mcp-${p}"
  else
    log "telegram-mcp-${p} NOT started — env has no TELEGRAM_SESSION_STRING yet (owner re-auth pending)"
  fi
done

log "done. verify: systemctl is-active telegram-mcp-${PROF_ARR[0]} · journalctl -u telegram-mcp-${PROF_ARR[0]} -n 30"
