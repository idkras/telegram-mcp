#!/usr/bin/env bash
# Portable Bitwarden/GPG interface shared by the just recipe and migrations.
set -euo pipefail

root_dir="$(cd "$(dirname "$0")/.." && pwd)"
command_name="${1:-}"
if [ -z "$command_name" ]; then
  echo "usage: $0 {init|status|encrypt|decrypt|clear} [arguments...]" >&2
  exit 2
fi

docker_io=(--interactive)
case "$command_name" in
  init|encrypt|decrypt)
    if [ ! -t 0 ] || [ ! -t 1 ]; then
      echo "REFUSED: Bitwarden $command_name requires an interactive terminal" >&2
      exit 1
    fi
    docker_io+=(--tty)
    ;;
esac

exec docker run --rm "${docker_io[@]}" \
  --platform linux/amd64 \
  --volume "$HOME/.bitwarden:/root/.config/Bitwarden CLI" \
  --volume "$root_dir/config:/app/config" \
  rickai/secrets "$@"
