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
# `export `-префикс + опциональный inline-комментарий (H3, code-reviewer 2026-05-28):
# человек правит .env.laba руками → `export X=...` / `X=... # laba` иначе не матчились
# → guard молча отключался. StringSession = base64url, `#` в значение не входит,
# поэтому отрезание trailing ` #...` безопасно.
_ENV_SESSION_RE = re.compile(
    r"^\s*(?:export\s+)?TELEGRAM_SESSION_STRING\s*=\s*(.+?)\s*$"
)


def _normalize(value: str) -> str:
    """Единая нормализация для ОБЕИХ сторон (H2 symmetric, code-reviewer 2026-05-28).

    Раньше env-сторона снимала кавычки, keychain — нет → логически одна сессия
    давала разные SHA → false-negative. Теперь обе стороны: strip whitespace +
    surrounding quotes + trailing inline comment.
    """
    v = value.strip()
    # trailing inline comment (только вне кавычек; base64url не содержит '#',
    # поэтому режем от ПЕРВОГО '#' — и `val # c`, и `val#c` без пробела, иначе
    # human-edited `.env` с `KEY=val#c` давал бы другой SHA → false-negative,
    # H1 code-reviewer 2026-05-28).
    if not (v.startswith('"') or v.startswith("'")):
        v = v.split("#", 1)[0].rstrip()
    v = v.strip().strip('"').strip("'").strip()
    return v


def _sha(value: str) -> str:
    return hashlib.sha256(_normalize(value).encode("utf-8")).hexdigest()


def enumerate_keychain_sessions() -> dict[str, str] | None:
    """Все Keychain `*_tg_session` / `telegram_session` → {account: sha256}.

    Имена ключей берутся динамически из `security dump-keychain` (только svce),
    значения — через `security find-generic-password -w`. Секрет не возвращается,
    только sha256.

    Возврат (D1/H2 fix, design+code reviewer 2026-05-28 — НИКОГДА silent false-green):
      - dict  — `security` отработал, перечислены 0+ локальных сессий (пустой dict =
                достоверно «локальных сессий нет»);
      - None  — `security` НЕДОСТУПЕН (не macOS / laba-хост / нет бинаря) → перечислить
                невозможно → НЕ выдавать «PASS distinct from 0», а сигналить INCONCLUSIVE.
    Это закрывает дыру: на laba-хосте Keychain нет → раньше `{}` → guard был no-op
    именно там где идёт deploy (false confidence). Авторитетный запуск guard — на
    ЛОКАЛЬНОЙ машине (где живут local-сессии) ПЕРЕД отгрузкой .env.laba.
    """
    out: dict[str, str] = {}
    try:
        dump = subprocess.run(
            ["security", "dump-keychain"],
            capture_output=True, text=True, timeout=30,
        ).stdout
    except (OSError, subprocess.SubprocessError):
        return None  # couldn't enumerate — INCONCLUSIVE, not "0 sessions"
    # Известное ограничение (C1, code-reviewer 2026-05-28): `find-generic-password
    # -s NAME -w` возвращает значение ТОЛЬКО первого item при дублях service name
    # (login + iCloud keychain). Если у аккаунта два item с одним svce, реальная
    # коллизионная пара может быть во втором → false-negative. Перечисление всех
    # значений per service требует `dump-keychain -d` (запрашивает пароль) —
    # неприемлемо в CI/deploy preflight. Мера: runtime session_health_monitor.py
    # ловит реальную смерть ключа независимо от этого статического сравнения.
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
                # _sha сам нормализует (symmetric с keychain-стороной) — не pre-strip
                if _normalize(m.group(1)):
                    return _sha(m.group(1))
    except OSError:
        return None
    return None


def detect_collision(
    env_sha: str | None, keychain_shas: dict[str, str] | None
) -> tuple[bool, str]:
    """True + имя совпавшего keychain-аккаунта, если laba-сессия = локальной.

    keychain_shas=None (перечислить нельзя) → (False, "") — НЕ коллизия, но и НЕ
    доказанная distinctness; решение INCONCLUSIVE принимает main() по None отдельно.
    """
    if not env_sha or not keychain_shas:
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

FIX (Telegram допускает N параллельных долгоживущих сессий на аккаунт):
  1. Re-auth даёт НОВУЮ session string (нужен SMS/Telegram-код на телефон
     аккаунта — owner/Lisa effort). Канонический скрипт re-auth:
       python3 update_lisa_session.py        # для профиля lisa
       python3 update_lisa_session_via_qr.py # QR-вариант
     ⚠️ Эти скрипты сохраняют новую строку в Keychain (для ЛОКАЛЬНОГО MCP).
  2. Для laba endpoint нужна ОТДЕЛЬНАЯ session string, НЕ та что в Keychain.
     Сгенерируй вторую сессию (повторный re-auth) и положи её ТОЛЬКО в .env.laba.
     Локальная (Keychain) и laba (.env.laba) сессии обязаны быть РАЗНЫМИ.
  3. Перезапусти deploy. Runtime-смерть ключа ловит session_health_monitor.py.

Намеренный reuse (не рекомендуется — вернёт инцидент): {ack}="<reason ≥{min} chars>"."""


STRICT_ENV = "TG_SESSION_GUARD_STRICT"

INCONCLUSIVE = """\
session-per-endpoint: INCONCLUSIVE — нельзя перечислить локальные сессии на этом хосте.

`security` (macOS Keychain) недоступен → сравнить laba-сессию (.env.laba, sha {env8})
не с чем. Это НЕ значит «коллизий нет» — это значит проверка здесь бессильна (laba —
Linux-хост, у него нет local Keychain; он и есть «другой endpoint»).

ПРАВИЛЬНО: запусти guard на ЛОКАЛЬНОЙ Mac-машине (где живут local-сессии) ПЕРЕД
отгрузкой .env.laba на laba:
    python3 scripts/validate_session_per_endpoint.py --env-path /path/to/.env.laba
Там сравнение авторитетно. Здесь (на laba preflight) — лишь advisory.

Fail-closed на этом хосте (CI хочет жёсткий блок при INCONCLUSIVE): {strict}=1 → exit 2."""


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--env-path",
        default=str(Path(__file__).resolve().parent.parent / ".env.laba"),
    )
    parser.add_argument(
        "--strict", action="store_true",
        help=f"INCONCLUSIVE → exit 2 (также через env {STRICT_ENV}=1)",
    )
    args = parser.parse_args()
    strict = args.strict or os.environ.get(STRICT_ENV, "").strip() not in ("", "0")

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
    # D1/H2 fix: keychain is None → перечислить нельзя → INCONCLUSIVE, НЕ false-PASS.
    if keychain is None:
        print(INCONCLUSIVE.format(env8=env_sha[:8], strict=STRICT_ENV), file=sys.stderr)
        return 2 if strict else 0
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
