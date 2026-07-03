#!/usr/bin/env python3
"""Holistic telegram-mcp doctor — falsify the WHOLE contour, not one layer (pr-hero-ku7).

Existing session_health_monitor.py checks ONE layer (session alive/REVOKED).
It does NOT catch «lisa ingest застряла 9 дн» (session alive but ingest stalled).
This doctor runs every mechanically-verifiable layer of telegram_mcp_workflow_ssot.yaml
`lifecycle` and returns non-zero if ANY layer is red → feeds SwiftBar + STOP-flag.

Layers checked (no Telegram login needed):
  deploy_units       ssh systemctl is-active telegram-mcp-<profile>
  session_collision  validate_session_per_endpoint.py exit 0
  ingest             max(message_ts) per schema < INGEST_STALE_HOURS (catches 9d stall)
  classify           classify_chats coverage == 100%
  guardian_write     0 code_relay (§types id_tails) messages in *.telegram_messages_raw
  monitor_surface    swiftbar cache generated_at < SWIFTBAR_STALE_MIN

Usage:
    python3 telegram_mcp_doctor.py            # human report, exit 0/1
    python3 telegram_mcp_doctor.py --json
Env: TG_VPS_HOST (default sandbox-ik), INGEST_STALE_HOURS (6), SWIFTBAR_STALE_MIN (15)
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from pathlib import Path

import yaml

_HERE = Path(__file__).resolve().parent
_SSOT = _HERE / "telegram_mcp_workflow_ssot.yaml"
VPS = os.environ.get("TG_VPS_HOST", "sandbox-ik")
INGEST_STALE_H = float(os.environ.get("INGEST_STALE_HOURS", "6"))
SWIFTBAR_STALE_MIN = float(os.environ.get("SWIFTBAR_STALE_MIN", "15"))


def _ssot() -> dict:
    return yaml.safe_load(open(_SSOT, encoding="utf-8"))


def _profiles(s: dict) -> list[str]:
    return s.get("profiles") or ["ikrasinsky", "lisa"]


def _schemas(s: dict) -> dict:
    return s.get("supabase_schemas") or {"ikrasinsky": "rick_messages_tasks", "lisa": "tg_lisa"}


def _code_relay_ids(s: dict) -> list[str]:
    for t in s.get("types", []):
        if t["name"] == "code_relay":
            return [str(x) for x in (t.get("match", {}).get("id_tails") or [])]
    return []


def _ssh(cmd: str, timeout: int = 8) -> tuple[bool, str]:
    try:
        r = subprocess.run(["ssh", "-o", "BatchMode=yes", "-o", f"ConnectTimeout={timeout}", VPS, cmd],
                           capture_output=True, text=True, timeout=timeout + 5)
        return r.returncode == 0, (r.stdout or r.stderr).strip()
    except Exception as e:  # noqa: BLE001
        return False, str(e)[:80]


def _pg():
    try:
        from . import supabase_writer  # type: ignore
    except ImportError:
        import supabase_writer  # type: ignore
    import psycopg2
    url = supabase_writer._get_postgres_url()
    return psycopg2.connect(url, connect_timeout=20)


def check_deploy_units(s) -> dict:
    profs = _profiles(s)
    units = " ".join(f"telegram-mcp-{p}" for p in profs)
    ok, out = _ssh(f"systemctl is-active {units}")
    states = out.split() if out else []
    active = [p for p, st in zip(profs, states) if st == "active"]
    red = [p for p, st in zip(profs, states) if st != "active"] or (profs if not states else [])
    return {"layer": "deploy_units", "ok": bool(active) and not red,
            "detail": f"active={active} not_active={red or profs} raw={out[:60]!r}"}


def check_session_collision(s) -> dict:
    v = _HERE / "scripts" / "validate_session_per_endpoint.py"
    if not v.exists():
        return {"layer": "session_collision", "ok": None, "detail": "validator not found (SKIP)"}
    try:
        r = subprocess.run([sys.executable, str(v)], capture_output=True, text=True, timeout=30)
        blob = f"{r.stdout or ''}\n{r.stderr or ''}"
        last = blob.strip().splitlines()[-1][:80] if blob.strip() else "exit %d" % r.returncode
        # Bug S6 (pr-hero-i5i): validate_session_per_endpoint.py returns exit 0 for
        # BOTH «no collision» AND «INCONCLUSIVE» (Linux host without Keychain — can't
        # enumerate local sessions). Treating exit 0 as green rubber-stamped a host
        # where the collision check literally didn't run. INCONCLUSIVE → SKIP (ok=None),
        # never green.
        if "INCONCLUSIVE" in blob:
            return {"layer": "session_collision", "ok": None,
                    "detail": f"INCONCLUSIVE — collision unverifiable on this host (SKIP): {last}"}
        return {"layer": "session_collision", "ok": r.returncode == 0, "detail": last}
    except Exception as e:  # noqa: BLE001
        return {"layer": "session_collision", "ok": None, "detail": f"SKIP ({str(e)[:50]})"}


def _ingest_stale(rows: dict, threshold: float) -> dict:
    """Classify per-profile ingest staleness. Bug S5 (pr-hero-i5i): the old dict
    comprehension called round(None) when a profile's telegram_messages_raw was
    EMPTY (max(message_ts)=NULL → h=None) — that crash was swallowed into a SKIP,
    masking «no messages at all» as «couldn't check». Empty table = NO ingest =
    stale/RED, labelled 'no messages'; a number over threshold = stale with its
    hours; fresh (≤ threshold) is dropped.
    """
    stale: dict = {}
    for prof, h in rows.items():
        if h is None:
            stale[prof] = "no messages"
        elif h > threshold:
            stale[prof] = round(h, 1)
    return stale


def check_ingest(s) -> dict:
    try:
        conn = _pg(); cur = conn.cursor()
        rows = {}
        for prof, sch in _schemas(s).items():
            cur.execute(f"select extract(epoch from (now() - max(message_ts))) from {sch}.telegram_messages_raw;")
            secs = cur.fetchone()[0]
            rows[prof] = (float(secs) / 3600.0) if secs is not None else None
        conn.close()
        stale = _ingest_stale(rows, INGEST_STALE_H)
        return {"layer": "ingest", "ok": not stale,
                "detail": f"hours_since_last={ {p: (round(h,1) if h is not None else None) for p,h in rows.items()} } stale(>{INGEST_STALE_H}h or empty)={stale}"}
    except Exception as e:  # noqa: BLE001
        return {"layer": "ingest", "ok": None, "detail": f"SKIP ({str(e)[:60]})"}


def check_classify(s) -> dict:
    try:
        try:
            from . import classify_chats  # type: ignore
        except ImportError:
            import classify_chats  # type: ignore
        res = classify_chats.run(list(_schemas(s).values())[0])
        cov = classify_chats.coverage(res)
        return {"layer": "classify", "ok": cov >= 100.0,
                "detail": f"coverage={cov:.1f}% total={res['total']} unclassified={res['counts'].get('unclassified',0)}"}
    except Exception as e:  # noqa: BLE001
        return {"layer": "classify", "ok": None, "detail": f"SKIP ({str(e)[:60]})"}


def check_guardian_write(s) -> dict:
    ids = _code_relay_ids(s)
    if not ids:
        return {"layer": "guardian_write", "ok": None, "detail": "no code_relay ids in ssot (SKIP)"}
    try:
        conn = _pg(); cur = conn.cursor()
        leaked = {}
        variants = set()
        for i in ids:
            variants |= {i, f"-{i}", f"-100{i}"}
        idlist = ",".join("'%s'" % v for v in variants)
        for prof, sch in _schemas(s).items():
            cur.execute(f"select count(*) from {sch}.telegram_messages_raw where chat_id in ({idlist});")
            n = cur.fetchone()[0]
            if n:
                leaked[prof] = n
        conn.close()
        return {"layer": "guardian_write", "ok": not leaked,
                "detail": f"code_relay messages leaked into supabase={leaked or 0} (want 0)"}
    except Exception as e:  # noqa: BLE001
        return {"layer": "guardian_write", "ok": None, "detail": f"SKIP ({str(e)[:60]})"}


def _session_auth_probe(s) -> tuple[bool | None, str]:
    """Run session_health_monitor over the configured profiles → (ok, detail).

    ok=True  — every probed profile authorized (or NETWORK-only transient)
    ok=False — ≥1 profile dead (REVOKED / AUTHKEY_DUPLICATED / NO_SESSION / UNKNOWN)
    ok=None  — could not probe (monitor missing / telethon import / no creds) → SKIP
    Isolated so tests can stub the probe without touching telethon.
    """
    mon = _HERE / "scripts" / "session_health_monitor.py"
    if not mon.exists():
        return None, "session_health_monitor.py not found (SKIP)"
    profs = _profiles(s)
    try:
        r = subprocess.run(
            [sys.executable, str(mon), "--profiles", ",".join(profs), "--json"],
            capture_output=True, text=True, timeout=90,
        )
    except Exception as e:  # noqa: BLE001
        return None, f"SKIP ({str(e)[:50]})"
    # exit 1 = ≥1 dead profile; exit 0 = all healthy/transient. Distinguish a real
    # verdict from a probe that never got to run (import/setup crash on stderr).
    blob = f"{r.stdout or ''}\n{r.stderr or ''}"
    if "run error" in blob or "Traceback" in blob:
        return None, f"SKIP (monitor could not run): {blob.strip().splitlines()[-1][:60] if blob.strip() else ''}"
    if r.returncode == 0:
        return True, "all probed profiles authorized (or NETWORK transient)"
    dead = blob.strip().splitlines()[-1][:80] if blob.strip() else "exit %d" % r.returncode
    return False, f"dead session(s): {dead}"


def check_session_auth(s) -> dict:
    """8th layer (Bug S3, pr-hero-i5i): the doctor verified 6 mechanical layers but
    never the actual session auth — the exact thing that «протухает». Wire the
    existing session_health_monitor as a first-class layer so a revoked/duplicated
    key surfaces in the contour verdict, not only in a separate cron."""
    ok, detail = _session_auth_probe(s)
    return {"layer": "session_auth", "ok": ok, "detail": detail}


def check_monitor_surface(s) -> dict:
    cache = Path(os.environ.get("TELEGRAM_ENDPOINTS_CACHE", str(Path.home() / ".swiftbar" / "telegram_endpoints_cache.json")))
    if not cache.exists():
        return {"layer": "monitor_surface", "ok": False, "detail": f"swiftbar cache missing: {cache}"}
    try:
        gen = json.loads(cache.read_text()).get("generated_at", 0)
        age_min = (time.time() - float(gen)) / 60.0
        return {"layer": "monitor_surface", "ok": age_min < SWIFTBAR_STALE_MIN,
                "detail": f"cache_age={age_min:.0f}min (stale>{SWIFTBAR_STALE_MIN}min)"}
    except Exception as e:  # noqa: BLE001
        return {"layer": "monitor_surface", "ok": None, "detail": f"SKIP ({str(e)[:50]})"}


CHECKS = [check_deploy_units, check_session_collision, check_session_auth,
          check_ingest, check_classify, check_guardian_write, check_monitor_surface]


def run() -> list[dict]:
    s = _ssot()
    return [c(s) for c in CHECKS]


def main(argv: list[str]) -> int:
    results = run()
    red = [r for r in results if r["ok"] is False]
    green = [r for r in results if r["ok"] is True]
    # Bug S4 (pr-hero-i5i): the old verdict only counted RED (ok is False). If EVERY
    # layer SKIP-ped (ok=None) the doctor printed «contour closed» and exit 0 — a
    # green-when-blind: we verified nothing yet claimed the contour was fine. All-SKIP
    # (no green, no red) is INCONCLUSIVE and must exit non-zero, distinct from RED.
    inconclusive = (not green) and (not red)
    if "--json" in argv:
        print(json.dumps({
            "results": results,
            "red": [r["layer"] for r in red],
            "verdict": "RED" if red else ("INCONCLUSIVE" if inconclusive else "CLOSED"),
        }, ensure_ascii=False, indent=2))
    else:
        print(f"== telegram-mcp doctor · VPS {VPS} ==\n")
        for r in results:
            glyph = {True: "✅", False: "🔴", None: "⚪"}[r["ok"]]
            print(f"  {glyph} {r['layer']:20s} {r['detail']}")
        if red:
            verdict = "🔴 CONTOUR OPEN — " + ",".join(r["layer"] for r in red)
        elif inconclusive:
            verdict = "⚪ INCONCLUSIVE — не смог проверить ни один слой (all SKIP), contour NOT closed"
        else:
            verdict = "✅ contour closed"
        print(f"\n  VERDICT: {verdict}")
    if red:
        return 1
    if inconclusive:
        return 3  # non-zero, distinct from RED(1): verified nothing
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
