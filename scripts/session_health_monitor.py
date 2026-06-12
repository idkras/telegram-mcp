#!/usr/bin/env python3
"""Telegram session-health monitor — surface a dying session BEFORE delivery fails.

RCA 2026-05-28 (ai.incidents.md:6988) + design-review verdict 2026-05-28:
deploy-time `validate_session_per_endpoint.py` ловит ТОЛЬКО статическое равенство
строк в момент подготовки deploy на одной машине. Реальный инцидент («токен Лизы
опять протух») — это RUNTIME-свойство: один auth key с двух ЖИВЫХ коннектов
(local MCP + laba + ad-hoc скрипты) → Telegram навсегда убивает ключ. Deploy-check
структурно этот класс не видит.

Owner JTBD: «канал Лизы не умирает молча». Этот монитор — высоко-leverage root-fix:
периодически (cron / CI) прогоняет уже-исправленный `session_manager.test_session`
по КАЖДОМУ профилю, классифицирует причину (OK / REVOKED / AUTHKEY_DUPLICATED /
NO_SESSION / NETWORK / UNKNOWN) и возвращает non-zero если хоть один профиль мёртв
— так смерть канала всплывает СРАЗУ, независимо от того, какой вектор её вызвал
(deploy reuse, ad-hoc connect, два контейнера, перезапись Keychain).

Universal: НЕ hardcoded на один аккаунт. Список профилей — конфигурируемый
(DEFAULT_PROFILES = lisa / ik / rick-coposlly-linkedinhero / default), полностью
переопределяется флагом `--profiles a,b,c`. Креды каждого профиля резолвятся через
session_manager.get_profile_credential_names(profile). Новый teammate-профиль =
добавить в --profiles (или в DEFAULT_PROFILES), без правки логики.

Usage:
    python3 session_health_monitor.py [--profiles lisa,ik] [--json] [--alert-cmd CMD]
Exit:
    0 — все проверенные профили OK (или нет настроенных профилей)
    1 — ≥1 профиль broken (REVOKED / AUTHKEY_DUPLICATED / NO_SESSION / UNKNOWN)
    0 — профиль только NETWORK (transient) → не считается смертью канала

--alert-cmd: shell-команда, в которую STDIN подаётся markdown-сводка broken-профилей
    (owner wires свой канал: telegram-send / slack / pagerduty). Если не задана —
    только печать в stderr + exit code (cron сам решит что делать). Канал alert —
    OWNER DECISION, монитор его не зашивает.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import subprocess
import sys
from pathlib import Path

# session_manager лежит в родителе scripts/
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# Профили, у которых может быть настроена сессия (из get_profile_credential_names).
DEFAULT_PROFILES = ["lisa", "ik", "rick-coposlly-linkedinhero", "default"]
# Коды, которые означают «канал мёртв» (не transient).
DEAD_CODES = ("REVOKED", "AUTHKEY_DUPLICATED", "NO_SESSION", "UNKNOWN")


def classify(diagnosis: str | None) -> str:
    """Извлечь reason-код из диагноза test_session (префикс до ':')."""
    if not diagnosis:
        return "UNKNOWN"
    head = diagnosis.split(":", 1)[0].strip().upper()
    return head if head in (*DEAD_CODES, "NETWORK", "OK") else "UNKNOWN"


async def probe_profiles(profiles: list[str]) -> list[dict]:
    """Прогнать test_session по каждому профилю → список результатов."""
    from session_manager import test_session  # lazy import (telethon heavy)

    results: list[dict] = []
    for profile in profiles:
        try:
            ok, info = await test_session(profile)
        except Exception as exc:  # noqa: BLE001 — один профиль не должен ронять монитор
            ok, info = False, f"UNKNOWN: monitor error {type(exc).__name__}: {exc}"
        code = "OK" if ok else classify(info)
        results.append({"profile": profile, "ok": ok, "code": code, "detail": info or ""})
    return results


def render_table(results: list[dict]) -> str:
    lines = ["| profile | status | reason |", "|---|---|---|"]
    for r in results:
        mark = "✅" if r["ok"] else ("⚠️" if r["code"] == "NETWORK" else "🔴")
        detail = (r["detail"] or "").replace("\n", " ")[:90]
        lines.append(f"| {r['profile']} | {mark} {r['code']} | {detail} |")
    return "\n".join(lines)


def dead_profiles(results: list[dict]) -> list[dict]:
    return [r for r in results if (not r["ok"]) and r["code"] in DEAD_CODES]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--profiles", default=",".join(DEFAULT_PROFILES))
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--alert-cmd", default="")
    args = parser.parse_args()

    profiles = [p.strip() for p in args.profiles.split(",") if p.strip()]

    try:
        results = asyncio.run(probe_profiles(profiles))
    except Exception as exc:  # noqa: BLE001 — монитор не должен падать стеком в cron
        print(f"session-health-monitor: run error {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1

    table = render_table(results)
    if args.json:
        print(json.dumps(results, ensure_ascii=False, indent=2))
    else:
        print(table, file=sys.stderr)

    dead = dead_profiles(results)
    if not dead:
        print(f"session-health-monitor: all {len(results)} profile(s) healthy", file=sys.stderr)
        return 0

    summary = (
        f"🔴 Telegram session(s) DEAD ({len(dead)}): "
        + ", ".join(f"{r['profile']}={r['code']}" for r in dead)
        + "\n\n"
        + table
        + "\n\nRe-auth требуется (SMS на телефон аккаунта). См. update_lisa_session.py "
        "/ update_lisa_session_via_qr.py. Дай каждому endpoint СВОЮ session string."
    )
    print(summary, file=sys.stderr)

    if args.alert_cmd:
        try:
            subprocess.run(args.alert_cmd, shell=True, input=summary, text=True, timeout=30)
        except (OSError, subprocess.SubprocessError) as exc:
            print(f"session-health-monitor: alert-cmd failed: {exc}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
