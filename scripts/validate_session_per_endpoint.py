#!/usr/bin/env python3
"""Validate Telegram session-per-endpoint — block AuthKeyDuplicated recurrence.

RCA 2026-05-28 (ai.incidents.md:6988): одна session-строка (`lisa_tg_session`)
использовалась с ДВУХ IP одновременно — локальный Mac MCP (Keychain) + laba
docker-контейнер (`.env.laba:TELEGRAM_SESSION_STRING`). Telegram anti-abuse
навсегда убивает auth key (`AuthKeyDuplicatedError`). Submodule commit a253d5b
закрыл detect-of-detector (классификация причины), но «Permanent fix»
(session-per-endpoint) оставался ДЕКЛАРАТИВНЫМ — ничто механически не мешало
снова положить одну сессию на два endpoint.

Этот валидатор — механическое enforcement: сравнивает session string,
предназначенную для laba (`.env.laba:TELEGRAM_SESSION_STRING`), со ВСЕМИ
локальными Keychain `*_tg_session`. Если совпадают → collision → exit 2
(deploy abort), потому что один auth key с двух IP = гарантированный
AuthKeyDuplicated.

Безопасность: секреты НИКОГДА не печатаются — сравнение по SHA-256, в выводе
только 8-символьный префикс хэша. Универсально: enumerates все `*_tg_session`
keychain-ключи динамически (lisa / default / future teammates), без hardcode.

Usage:
    python3 validate_session_per_endpoint.py [--env-path .env.laba]
Exit:
    0 — no collision (или .env отсутствует / keychain пуст / ACK)
    2 — collision: laba session reuses a local session
Override:
    TG_SESSION_REUSE_ACK="<reason ≥12 chars>"  — намеренный reuse (не рекоменд.)
"""
from __future__ import annotations

import argparse
import hashlib
import os
import re
import subprocess
import sys
from pathlib import Path

ACK_ENV = "TG_SESSION_REUSE_ACK"
ACK_MIN = 12
_SESSION_KEY_RE = re.compile(r".*_tg_session$|^telegram_session$")
_ENV_SESSION_RE = re.compile(r"^\s*TELEGRAM_SESSION_STRING\s*=\s*(.+?)\s*$")


def _sha(value: str) -> str:
    return hashlib.sha256(value.strip().encode("utf-8")).hexdigest()


def enumerate_keychain_sessions() -> dict[str, str]:
    """Все Keychain `*_tg_session` / `telegram_session` → {account: sha256}.

    Имена ключей берутся динамически из `security dump-keychain` (только svce),
    значения — через `security find-generic-password -w`. Секрет не возвращается,
    только sha256. Если `security` недоступен (не macOS) → пустой dict.
    """
    out: dict[str, str] = {}
    try:
        dump = subprocess.run(
            ["security", "dump-keychain"],
            capture_output=True, text=True, timeout=30,
        ).stdout
    except (OSError, subprocess.SubprocessError):
        return out
    names: set[str] = set()
    for m in re.finditer(r'"svce"<blob>="([^"]*)"', dump):
        name = m.group(1)
        if _SESSION_KEY_RE.match(name):
            names.add(name)
    for name in sorted(names):
        try:
            val = subprocess.run(
                ["security", "find-generic-password", "-s", name, "-w"],
                capture_output=True, text=True, timeout=15,
            ).stdout.strip()
        except (OSError, subprocess.SubprocessError):
            continue
        if val:
            out[name] = _sha(val)
    return out


def read_env_session(env_path: str) -> str | None:
    """SHA256 of TELEGRAM_SESSION_STRING из .env-файла, или None если нет."""
    p = Path(env_path)
    if not p.exists():
        return None
    try:
        for line in p.read_text(encoding="utf-8", errors="replace").splitlines():
            m = _ENV_SESSION_RE.match(line)
            if m:
                raw = m.group(1).strip().strip('"').strip("'")
                if raw:
                    return _sha(raw)
    except OSError:
        return None
    return None


def detect_collision(
    env_sha: str | None, keychain_shas: dict[str, str]
) -> tuple[bool, str]:
    """True + имя совпавшего keychain-аккаунта, если laba-сессия = локальной."""
    if not env_sha:
        return False, ""
    for account, sha in keychain_shas.items():
        if sha == env_sha:
            return True, account
    return False, ""


REMEDIATION = """\
session-per-endpoint: COLLISION — laba session reuses local session «{account}»

.env.laba:TELEGRAM_SESSION_STRING (sha {env8}) == Keychain «{account}» (sha {kc8}).
Одна session-строка с двух IP (local Mac + laba host) → Telegram навсегда убьёт
auth key (AuthKeyDuplicatedError). Это корень рецидива «токен Лизы опять протух»
(RCA 2026-05-28 ai.incidents.md).

FIX (Telegram допускает N параллельных сессий на аккаунт):
  1. Сгенерируй ОТДЕЛЬНУЮ session string для laba endpoint:
       python3 scripts/update_session.py   # авторизуйся → новая строка
  2. Положи её ТОЛЬКО в .env.laba (НЕ в Keychain *_tg_session, который читает
     локальный MCP). Локальная и laba сессии обязаны быть РАЗНЫМИ.
  3. Перезапусти deploy.

Намеренный reuse (не рекомендуется): export {ack}="<reason ≥{min} chars>"."""


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--env-path",
        default=str(Path(__file__).resolve().parent.parent / ".env.laba"),
    )
    args = parser.parse_args()

    ack = os.environ.get(ACK_ENV, "")
    if len(ack.strip()) >= ACK_MIN:
        print(f"session-per-endpoint: PASS — ACK present ({len(ack)} chars)", file=sys.stderr)
        return 0

    try:
        env_sha = read_env_session(args.env_path)
        keychain = enumerate_keychain_sessions()
        collision, account = detect_collision(env_sha, keychain)
    except Exception as exc:  # noqa: BLE001 — fail-open (не ломать deploy на баге валидатора)
        print(f"session-per-endpoint: skipped (internal error: {exc})", file=sys.stderr)
        return 0

    if not env_sha:
        print(
            f"session-per-endpoint: PASS — no TELEGRAM_SESSION_STRING in {args.env_path}",
            file=sys.stderr,
        )
        return 0
    if not collision:
        print(
            f"session-per-endpoint: PASS — laba session distinct from "
            f"{len(keychain)} local Keychain session(s) (sha {env_sha[:8]})",
            file=sys.stderr,
        )
        return 0

    print(
        REMEDIATION.format(
            account=account,
            env8=env_sha[:8],
            kc8=keychain[account][:8],
            ack=ACK_ENV,
            min=ACK_MIN,
        ),
        file=sys.stderr,
    )
    return 2


if __name__ == "__main__":
    sys.exit(main())
