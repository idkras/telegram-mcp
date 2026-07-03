#!/usr/bin/env python3
"""Classify EVERY Telegram chat into a type per telegram_mcp_workflow_ssot.yaml (pr-hero-ei7).

Priority cascade: first matching type wins; `unclassified` (always) = 100% coverage.
Reads chats + tags from Supabase (both account schemas) + optional chat_client_mapping.json.

Usage:
    python3 classify_chats.py                 # human summary (counts per type + coverage)
    python3 classify_chats.py --json          # per-chat classification (machine)
    python3 classify_chats.py --schema tg_lisa
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import Any

import yaml

_SSOT = Path(__file__).with_name("telegram_mcp_workflow_ssot.yaml")
_MAPPING_CANDIDATES = [
    Path(__file__).resolve().parents[2] / "scripts" / "telegram" / "chat_client_mapping.json",
]


def load_ssot(path: Path | None = None) -> list[dict[str, Any]]:
    data = yaml.safe_load(open(path or _SSOT, encoding="utf-8"))
    for t in data["types"]:
        m = t.get("match", {})
        m["_title_re"] = re.compile(m["title_regex"]) if m.get("title_regex") else None
        m["_id_tails"] = set(str(x) for x in (m.get("id_tails") or []))
        m["_chat_type_in"] = set(m.get("chat_type_in") or [])
        m["_mapping_type"] = set(m.get("mapping_type") or [])
        m["_tag_in"] = set(m.get("tag_in") or [])
    return data["types"]


def load_mapping() -> dict[str, dict]:
    for p in _MAPPING_CANDIDATES:
        if p.exists():
            raw = json.load(open(p, encoding="utf-8"))
            return {k: v for k, v in raw.items() if isinstance(v, dict)}
    return {}


def _tail(chat_id: Any) -> str:
    s = str(chat_id).strip()
    if s.startswith("-100"):
        return s[4:]
    if s.startswith("-"):
        return s[1:]
    return s


def classify(chat_id: str, title: str | None, chat_type: str | None,
             tags: set[str], mapping: dict, types: list[dict]) -> str:
    tail = _tail(chat_id)
    t = title or ""
    mtype = (mapping.get(tail) or mapping.get(str(chat_id)) or {}).get("chat_type")
    for typ in types:
        m = typ["match"]
        if m.get("always"):
            return typ["name"]
        if tail in m["_id_tails"]:
            return typ["name"]
        if m["_chat_type_in"] and chat_type in m["_chat_type_in"]:
            return typ["name"]
        if m["_mapping_type"] and mtype in m["_mapping_type"]:
            return typ["name"]
        if m["_tag_in"] and (tags & m["_tag_in"]):
            return typ["name"]
        if m["_title_re"] and m["_title_re"].search(t):
            return typ["name"]
    return "unclassified"


def _conn():
    try:
        from . import supabase_writer  # type: ignore
    except ImportError:
        import supabase_writer  # type: ignore
    import psycopg2
    url = supabase_writer._get_postgres_url()
    if not url:
        raise RuntimeError("no postgres url from supabase_writer._get_postgres_url()")
    return psycopg2.connect(url, connect_timeout=25)


def coverage(res: dict[str, Any]) -> float:
    """Real classification coverage = % of chats put into a NON-fallback type.

    Bug C1 (pr-hero-i5i): the old formula was `100 * total / total` → always 100%,
    a tautology that hid 9000/9273 chats sitting in `unclassified`. Coverage now
    counts only chats that actually matched a real type (total minus unclassified).
    """
    total = res.get("total", 0) or 0
    if total <= 0:
        return 0.0
    unclassified = (res.get("counts") or {}).get("unclassified", 0)
    classified = total - unclassified
    return 100.0 * classified / total


def run(schema: str) -> dict[str, Any]:
    types = load_ssot()
    mapping = load_mapping()
    policy = {t["name"]: t["index_policy"] for t in types}
    conn = _conn()
    cur = conn.cursor()
    cur.execute(f"select chat_id, chat_title, chat_type from {schema}.telegram_chats;")
    chats = cur.fetchall()
    # tags per chat_id
    tags_by: dict[str, set[str]] = {}
    try:
        cur.execute(f"select chat_id, tag from {schema}.telegram_chat_tags;")
        for cid, tag in cur.fetchall():
            tags_by.setdefault(_tail(cid), set()).add(tag)
    except Exception:
        conn.rollback()
    conn.close()

    counts: dict[str, int] = {}
    per_chat = []
    for cid, title, ctype in chats:
        tags = tags_by.get(_tail(cid), set())
        typ = classify(cid, title, ctype, tags, mapping, types)
        counts[typ] = counts.get(typ, 0) + 1
        per_chat.append({"chat_id": cid, "title": title, "type": typ, "policy": policy.get(typ)})
    return {"schema": schema, "total": len(chats), "counts": counts,
            "policy": policy, "per_chat": per_chat}


def main(argv: list[str]) -> int:
    schema = "rick_messages_tasks"
    if "--schema" in argv:
        schema = argv[argv.index("--schema") + 1]
    res = run(schema)
    if "--json" in argv:
        print(json.dumps({k: v for k, v in res.items() if k != "per_chat"}
                         | {"per_chat_sample": res["per_chat"][:20]}, ensure_ascii=False, indent=2))
        return 0
    print(f"== chat classification · {res['schema']} · total {res['total']} ==\n")
    classified = res["total"] - res["counts"].get("unclassified", 0)
    order = [t["name"] for t in load_ssot()]
    for name in order:
        n = res["counts"].get(name, 0)
        if n:
            print(f"  {name:20s} [{res['policy'][name]:6s}] : {n}")
    cov = coverage(res)
    print(f"\n  COVERAGE: {classified}/{res['total']} = {cov:.1f}%  "
          f"(classified-non-fallback {classified}, unclassified {res['counts'].get('unclassified',0)})")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
