#!/usr/bin/env bash
# One-time migration: legacy VPS env -> Bitwarden/GPG archive + systemd credential.
# The legacy VPS env is intentionally NOT deleted here; retirement happens only
# after service health and physical Supabase readback.
set -euo pipefail

profile="${1:-}"
host="${2:-sandbox-ik.infra.node.rickai.net}"

case "$profile" in
  lisa|ikrasinsky) ;;
  *) echo "usage: $0 {lisa|ikrasinsky} [sandbox-ik-host]" >&2; exit 2 ;;
esac
case "$host" in
  ''|*[!A-Za-z0-9.-]*) echo "unsafe host" >&2; exit 2 ;;
esac

root_dir="$(cd "$(dirname "$0")/.." && pwd)"
environment="sandbox-ik-${profile}"
plaintext="$root_dir/config/${environment}.secrets.env"
archive="${plaintext}.gpg"
remote="idkras@${host}"
legacy="/etc/telegram-mcp/env.d/${profile}.env"
remote_app_dir="${TELEGRAM_MCP_REMOTE_APP_DIR:-/home/idkras/telegram-mcp-production}"
case "$remote_app_dir" in
  /home/idkras/*) ;;
  *) echo "unsafe TELEGRAM_MCP_REMOTE_APP_DIR" >&2; exit 2 ;;
esac
case "$remote_app_dir" in
  *['`$();|&<>*?'\''"'\ ]*) echo "unsafe TELEGRAM_MCP_REMOTE_APP_DIR" >&2; exit 2 ;;
esac
installer="$remote_app_dir/deploy/install-encrypted-credential.py"
scratch_dir="$(mktemp -d /tmp/telegram-mcp-secrets.XXXXXX)"
expected="$scratch_dir/expected.env"

if [ -e "$plaintext" ]; then
  echo "REFUSED: local plaintext already exists: $plaintext" >&2
  exit 1
fi
if [ -e "$archive" ]; then
  echo "REFUSED: encrypted archive already exists; use a reviewed rotation procedure" >&2
  exit 1
fi

umask 077
archive_verified=0
cleanup() {
  # Exact file created by this process; never recurse and never glob.
  if [ -f "$plaintext" ]; then
    rm -f -- "$plaintext"
  fi
  if [ -f "$expected" ]; then
    rm -f -- "$expected"
  fi
  if [ -d "$scratch_dir" ]; then
    rmdir -- "$scratch_dir"
  fi
  # A failed decrypt/readback must not leave an apparently valid new archive.
  if [ "$archive_verified" = 0 ] && [ -f "$archive" ]; then
    rm -f -- "$archive"
  fi
}
trap cleanup EXIT INT TERM

echo "[secrets-migrate] profile=$profile fetch=legacy-root-env output=gitignored-local"
ssh -T "$remote" "sudo -n cat '$legacy'" >"$plaintext"
test -s "$plaintext"
chmod 600 "$plaintext"
python3 "$root_dir/scripts/secrets-normalize-payload.py" "$plaintext"
cp "$plaintext" "$expected"
chmod 600 "$expected"

echo "[secrets-migrate] profile=$profile archive=encrypt-via-bitwarden"
"$root_dir/scripts/secrets-tool.sh" encrypt "$environment"
test -s "$archive"

echo "[secrets-migrate] profile=$profile archive=decrypt-and-compare-readback"
"$root_dir/scripts/secrets-tool.sh" clear "$environment"
"$root_dir/scripts/secrets-tool.sh" decrypt "$environment"
cmp -s "$expected" "$plaintext"
archive_verified=1

echo "[secrets-migrate] profile=$profile runtime=install-host-encrypted"
ssh -T "$remote" \
  "sudo -n /home/idkras/telegram-mcp/.venv/bin/python '$installer' --profile '$profile'" \
  <"$plaintext"

cleanup
trap - EXIT INT TERM
echo "[secrets-migrate] profile=$profile result=encrypted-archive-and-runtime-installed legacy=preserved"
echo "[secrets-migrate] next=wire-unit-restart-health-supabase-readback-then-retire-exact-legacy-file"
