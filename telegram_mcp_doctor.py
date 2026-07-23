#!/usr/bin/env python3
"""Holistic telegram-mcp doctor — falsify the WHOLE contour, not one layer (pr-hero-ku7).

Existing session_health_monitor.py checks ONE layer (session alive/REVOKED).
It does NOT catch «lisa ingest застряла 9 дн» (session alive but ingest stalled).
This doctor runs every mechanically-verifiable layer of telegram_mcp_workflow_ssot.yaml
`lifecycle` and returns non-zero if ANY layer is red → feeds SwiftBar + STOP-flag.

Layers checked (7 of 8 lifecycle stages; provision_vps = external, covered transitively
by deploy_units ssh). Verdict is 3-state: CLOSED (exit 0) / RED (exit 1) / INCONCLUSIVE
(exit 3 — could not confirm a CRITICAL layer green). Critical layers (must be green for
CLOSED): session_auth, session_collision, ingest, guardian_write.
  deploy_units       ssh systemctl is-active telegram-mcp-<profile>
  session_collision  validate_session_per_endpoint.py (INCONCLUSIVE on Linux ≠ green)
  session_auth       session_health_monitor.py — revoked/duplicated key (transient ≠ RED)
  ingest             max(message_ts) per schema < INGEST_STALE_HOURS; empty = INCONCLUSIVE
  classify           classify_chats coverage >= CLASSIFY_COVERAGE_MIN (default 99%)
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


def check_vps_ssh(s) -> dict:
    """Классифицированная достижимость VPS (RCA 2026-07-22: агент час искал живой
    хост, попав на мёртвый heroes-laba; sandbox-ik при этом резал SSH до баннера,
    а доктор не отличал «SSH сломан» от «юниты не активны»).

    Классы: OK / PREBANNER_RESET (порт открыт, sshd рвёт до баннера — fail2ban /
    MaxStartups / деградировавший sshd; чинится ТОЛЬКО через Hetzner Console) /
    TIMEOUT (сеть/VPN/файрвол) / AUTH (ключи: ssh-add --apple-load-keychain) /
    UNKNOWN. Первый чек в цепочке: при не-OK остальные ssh-слои ждать нечего.
    """
    ok, out = _ssh("echo ok")
    if ok and out == "ok":
        return {"layer": "vps_ssh", "ok": True, "detail": f"{VPS} reachable"}
    low = out.lower()
    if ("kex_exchange_identification" in low or "connection reset" in low
            or "connection closed by" in low):
        cls, hint = "PREBANNER_RESET", "Hetzner Console: fail2ban-client status sshd / MaxStartups / systemctl status ssh"
    elif "timed out" in low or "timeout" in low:
        cls, hint = "TIMEOUT", "сеть/VPN: проверь маршрут до 176.9.39.104; hostname из SSOT vps:"
    elif "permission denied" in low or "publickey" in low:
        cls, hint = "AUTH", "ssh-add --apple-load-keychain; ключи ik_id_rsa/idkras_ed25519"
    else:
        cls, hint = "UNKNOWN", "raw stderr ниже; НЕ ходи на heroes-laba (мёртв с 2026-07)"
    return {"layer": "vps_ssh", "ok": False,
            "detail": f"{cls}: {VPS} — {hint} | raw={out[:70]!r}"}


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


def _ingest_stale(rows: dict, threshold: float):
    """Classify per-profile ingest staleness into two distinct buckets.

    Bug S5 (pr-hero-i5i): the old comprehension called round(None) when a profile's
    telegram_messages_raw was EMPTY (max(message_ts)=NULL → h=None) — that crash was
    swallowed into a SKIP, masking «no messages at all» as «couldn't check».

    Calibration (design review pr-hero-i5i iter-2 — cold-start false-RED): an EMPTY
    table is ambiguous — it can mean «ingest broke» OR «fresh/just-provisioned profile,
    no messages yet». Flagging it RED would false-alarm the owner into an unnecessary
    re-auth (which itself risks a session collision, RCA 2026-05-28). So:
      • rows WITH data but older than threshold  → stale = definitely broken → RED
      • EMPTY table (h is None)                  → empty = can't confirm flowing → INCONCLUSIVE
    Returns (stale_dict, empty_list). The old masking-as-SKIP is gone either way.
    """
    stale: dict = {}
    empty: list = []
    for prof, h in rows.items():
        if h is None:
            empty.append(prof)
        elif h > threshold:
            stale[prof] = round(h, 1)
    return stale, empty


def check_ingest(s) -> dict:
    try:
        conn = _pg(); cur = conn.cursor()
        rows = {}
        for prof, sch in _schemas(s).items():
            cur.execute(f"select extract(epoch from (now() - max(message_ts))) from {sch}.telegram_messages_raw;")
            secs = cur.fetchone()[0]
            rows[prof] = (float(secs) / 3600.0) if secs is not None else None
        conn.close()
        stale, empty = _ingest_stale(rows, INGEST_STALE_H)
        hrs = f"hours_since_last={ {p: (round(h,1) if h is not None else None) for p,h in rows.items()} }"
        if stale:  # rows exist but stale → definitely broken
            return {"layer": "ingest", "ok": False,
                    "detail": f"{hrs} STALE(>{INGEST_STALE_H}h)={stale}"}
        if empty:  # no messages at all → can't confirm the channel flows (fresh OR dead)
            return {"layer": "ingest", "ok": None,
                    "detail": f"{hrs} INCONCLUSIVE — no messages ever for {empty} (fresh profile or ingest never ran)"}
        return {"layer": "ingest", "ok": True, "detail": hrs}
    except Exception as e:  # noqa: BLE001
        return {"layer": "ingest", "ok": None, "detail": f"SKIP ({str(e)[:60]})"}


# Bug I5-consumer (design+falsifier review pr-hero-1u1): the catch-up cycle writes
# catchup_boot/catchup_heartbeat markers, but WITHOUT a reader they were theatre. This
# is the consumer. check_ingest (max message_ts, 6h) catches «messages went stale»;
# this catches «the CYCLE ITSELF stopped running» (dead-session exit-1 loop, launchd not
# firing, crash mid-cycle) within CATCHUP_STALE_MIN (default 15m ≈ 3× the 5-min cron)
# instead of the full 6h window. A last run of catchup_session_dead is an explicit RED so
# an exit-1 loop cannot hide as «just no new marker».
CATCHUP_STALE_MIN = float(os.environ.get("CATCHUP_STALE_MIN", "15"))


def check_catchup_freshness(s) -> dict:
    try:
        conn = _pg(); cur = conn.cursor()
        ages = {}
        dead = {}
        for prof, sch in _schemas(s).items():
            cur.execute(
                f"select mode, extract(epoch from (now() - max(started_at)))/60.0 "
                f"from {sch}.telegram_ingest_runs "
                f"where mode in ('catchup_boot','catchup_heartbeat','catchup_session_dead') "
                f"group by mode order by max(started_at) desc;"
            )
            rows = cur.fetchall()
            if not rows:
                ages[prof] = None  # never ran → INCONCLUSIVE (fresh profile)
                continue
            latest_mode, latest_age = rows[0]
            ages[prof] = round(float(latest_age), 1)
            if latest_mode == "catchup_session_dead":
                dead[prof] = "session_dead"
            elif float(latest_age) > CATCHUP_STALE_MIN:
                dead[prof] = round(float(latest_age), 1)
        conn.close()
        if dead:  # cycle stopped OR last run reported a dead session → RED
            return {"layer": "catchup_freshness", "ok": False,
                    "detail": f"minutes_since_last_cycle={ages} stopped(>{CATCHUP_STALE_MIN}m or dead)={dead}"}
        if all(v is None for v in ages.values()):
            return {"layer": "catchup_freshness", "ok": None,
                    "detail": f"INCONCLUSIVE — no catch-up cycle ever recorded {list(ages)}"}
        return {"layer": "catchup_freshness", "ok": True, "detail": f"minutes_since_last_cycle={ages}"}
    except Exception as e:  # noqa: BLE001
        return {"layer": "catchup_freshness", "ok": None, "detail": f"SKIP ({str(e)[:60]})"}


def check_classify(s) -> dict:
    try:
        try:
            from . import classify_chats  # type: ignore
        except ImportError:
            import classify_chats  # type: ignore
        res = classify_chats.run(list(_schemas(s).values())[0])
        cov = classify_chats.coverage(res)
        # Calibration (code+design review pr-hero-i5i iter-2): a hard 100.0 gate makes
        # classify chronically RED — ONE unclassified private chat (fallback type
        # 'unclassified' is 'always'-match by design) drops coverage below 100 → RED →
        # cry-wolf → owner adds `|| true`. Honest threshold is «near-complete», env-tunable,
        # with the exact unclassified count surfaced so the remainder is actionable, not alarming.
        min_cov = float(os.environ.get("CLASSIFY_COVERAGE_MIN", "99.0"))
        return {"layer": "classify", "ok": cov >= min_cov,
                "detail": f"coverage={cov:.1f}% (min {min_cov:.1f}%) total={res['total']} unclassified={res['counts'].get('unclassified',0)}"}
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
    # Calibration (design review pr-hero-i5i iter-2 — transient false-RED → needless
    # re-auth → session collision). A non-zero exit is only a real DEAD verdict if it is
    # NOT a transient (network/timeout/flood). Those are INCONCLUSIVE, not RED. The deep
    # retry (session_health_monitor itself must retry before declaring DEAD) is bug S11,
    # owned by R3 pr-hero-3e1.1; here the doctor at least refuses to escalate a transient.
    low = blob.lower()
    transient = any(k in low for k in ("network", "timeout", "timed out", "floodwait",
                                       "flood_wait", "connectionerror", "connection reset",
                                       "temporarily", "unreachable"))
    tail = blob.strip().splitlines()[-1][:80] if blob.strip() else "exit %d" % r.returncode
    if transient:
        return None, f"INCONCLUSIVE (transient, not a dead-key verdict): {tail}"
    return False, f"dead session(s): {tail}"


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


CHECKS = [check_vps_ssh, check_deploy_units, check_session_collision, check_session_auth,
          check_ingest, check_catchup_freshness, check_classify,
          check_guardian_write, check_monitor_surface]


def run() -> list[dict]:
    s = _ssot()
    return [c(s) for c in CHECKS]


def main(argv: list[str]) -> int:
    results = run()
    red = [r for r in results if r["ok"] is False]
    green = [r for r in results if r["ok"] is True]
    green_layers = {r["layer"] for r in green}
    # Bug S4 (pr-hero-i5i): the old verdict only counted RED (ok is False). If EVERY
    # layer SKIP-ped (ok=None) the doctor printed «contour closed» and exit 0 — a
    # green-when-blind: we verified nothing yet claimed the contour was fine.
    #
    # Bug S4-residual (adversarial falsifier pr-hero-i5i iter-2): the first fix only
    # caught ALL-SKIP. But 1 trivial green (deploy_units = `systemctl is-active`, which
    # says nothing about the session/data actually flowing) + critical layers SKIP still
    # printed «contour closed» exit 0 — while the session could be revoked and ingest
    # stuck 9 days. CLOSED must require EVERY critical layer proven green, not «≥1 green,
    # 0 red». A critical layer that could not be checked (SKIP) = INCONCLUSIVE, not CLOSED.
    CRITICAL_LAYERS = {"session_auth", "session_collision", "ingest", "guardian_write"}
    critical_not_green = sorted(CRITICAL_LAYERS - green_layers)
    inconclusive = bool(critical_not_green)
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
            verdict = ("⚪ INCONCLUSIVE — критические слои не подтверждены зелёными: "
                       + ",".join(critical_not_green) + " (contour NOT closed)")
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
