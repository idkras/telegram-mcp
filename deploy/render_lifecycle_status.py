#!/usr/bin/env python3
"""R4 run-skill status renderer (pr-hero-i5i): turn telegram_mcp_doctor --json into a
single owner-facing contour status — what is green/red, the exact next action per red
layer, and a STOP-flag file the SwitchBar reads. So `run-lifecycle.sh` gives the owner
ONE command to see «работает / не работает + что делать дальше».

Usage:  telegram_mcp_doctor.py --json | render_lifecycle_status.py [--stop-flag PATH]
Exit:   0 if contour closed, 1 if any layer red (so cron/CI notice)."""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

# per-layer → owner-facing next action (who does it, is it agent-only or owner)
NEXT_ACTION = {
    "deploy_units": "🤖+👤 запустить `bash deploy/deploy-sandbox-ik.sh` (agent) + ввести 2 SMS-кода (owner) — юниты поднимутся",
    "ingest": "🤖 после deploy ingest оживёт; health-freshness теперь ловит стоп сразу (R3)",
    "guardian_write": "🤖 purge legacy утечки: `deploy/purge_legacy_leaks.sql` (нужен owner-фильтр: коды/карты)",
    "session_collision": "👤 re-auth задетый профиль (SMS) — session-per-endpoint",
    "classify": "🤖 перезапустить classify_chats.py (coverage упал)",
    "monitor_surface": "🤖 обновить SwiftBar cache: telegram_endpoints.py --check-telegram",
}


def render(doctor: dict, stop_flag: Path | None) -> tuple[int, str]:
    results = doctor.get("results", [])
    red = [r for r in results if r.get("ok") is False]
    lines = ["== telegram-mcp lifecycle · owner status =="]
    for r in results:
        glyph = {True: "✅", False: "🔴", None: "⚪"}.get(r.get("ok"))
        lines.append(f"  {glyph} {r.get('layer',''):16s} {r.get('detail','')[:70]}")
    if red:
        lines.append("\n🔴 CONTOUR OPEN — следующие шаги:")
        for r in red:
            layer = r.get("layer", "")
            lines.append(f"  • {layer}: {NEXT_ACTION.get(layer, 'нужна диагностика')}")
        if stop_flag is not None:
            stop_flag.write_text(",".join(r.get("layer", "") for r in red))
        rc = 1
    else:
        lines.append("\n✅ CONTOUR CLOSED — telegram-mcp собирает сообщения на 2 аккаунта безопасно")
        if stop_flag is not None and stop_flag.exists():
            stop_flag.unlink()
        rc = 0
    return rc, "\n".join(lines)


def main(argv: list[str]) -> int:
    stop = None
    if "--stop-flag" in argv:
        stop = Path(argv[argv.index("--stop-flag") + 1])
    else:
        stop = Path(os.getenv("TELEGRAM_STOP_FLAG", "/tmp/telegram_mcp_contour_open"))
    raw = sys.stdin.read()
    try:
        doctor = json.loads(raw)
    except Exception as exc:  # noqa: BLE001
        print(f"render: cannot parse doctor json ({exc})", file=sys.stderr)
        return 2
    rc, out = render(doctor, stop)
    print(out)
    return rc


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
