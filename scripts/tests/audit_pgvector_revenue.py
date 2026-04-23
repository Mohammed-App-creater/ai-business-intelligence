#!/usr/bin/env python3
"""
scripts/tests/audit_pgvector_revenue.py

Pgvector Audit — Revenue Domain
================================
Dumps every revenue chunk stored in pgvector for one or more tenants.
Built to diagnose the "April data leaking into answers" issue seen in
Step 6 — the AI cited specific April 2026 numbers ($8,200, $7,500, $500)
even though the backend only returns Jan–Mar 2026 data.

The goal: find out whether these April numbers are
  (a) hallucinated by the LLM,
  (b) leaking from stale embeddings, or
  (c) coming from another domain's chunks.

Output:
  - Full list of chunks with id, period (if any), doc_type, first 200 chars
  - Keyword scan for "april", "4/", "04-", "may", etc.
  - Period distribution — which months actually have chunks in pgvector?
  - Optional: dump suspicious chunks' FULL text to a file for inspection

Uses the app's own PGPool → settings → env vars, no guessing.

Run:
    PYTHONPATH=. python scripts/tests/audit_pgvector_revenue.py
    PYTHONPATH=. python scripts/tests/audit_pgvector_revenue.py --tenant 40
    PYTHONPATH=. python scripts/tests/audit_pgvector_revenue.py --tenant 40 --show-text
    PYTHONPATH=. python scripts/tests/audit_pgvector_revenue.py --tenant 40 --grep april
    PYTHONPATH=. python scripts/tests/audit_pgvector_revenue.py --tenant 40 --dump-suspicious audit.txt
"""
from __future__ import annotations

import argparse
import asyncio
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

# sys.path shim
_HERE = Path(__file__).resolve()
for _c in (_HERE.parent, *_HERE.parents):
    if (_c / "app").is_dir():
        if str(_c) not in sys.path:
            sys.path.insert(0, str(_c))
        break

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from app.core.config import settings
from app.services.db.db_pool import PGPool, PGTarget


# ── ANSI colors ───────────────────────────────────────────────────────────────
class C:
    RESET, BOLD, DIM = "\033[0m", "\033[1m", "\033[2m"
    RED, GREEN, YELLOW, CYAN, BLUE, MAGENTA = (
        "\033[31m", "\033[32m", "\033[33m", "\033[36m", "\033[34m", "\033[35m"
    )


def c(t: str, col: str) -> str:
    return f"{col}{t}{C.RESET}"


# ── Schema discovery ──────────────────────────────────────────────────────────

async def discover_table_and_columns(conn) -> tuple[str, dict[str, str]]:
    """
    Find the pgvector table and return (table_name, {logical_col_name: actual_col_name}).

    Tries settings.VECTOR_TABLE_NAME first, then common fallbacks. Also handles
    the common field-naming variations (tenant_id / business_id / org_id, etc.).

    Returns a double-quoted, fully-qualified identifier so schemas/tables with
    hyphens or mixed case (e.g. "ai-dev-vector-Schema".embeddings) work.
    """
    def _q(ident: str) -> str:
        """Double-quote a Postgres identifier. Escape any embedded quotes."""
        return '"' + ident.replace('"', '""') + '"'

    candidates = [
        settings.VECTOR_TABLE_NAME,
        "embeddings",
        "document_embeddings",
        "vector_store",
        "rag_embeddings",
        "documents",
    ]

    # Try each candidate in each schema — check public first, then anywhere
    for schema in ("public", None):
        for table in candidates:
            if schema:
                sql = (
                    "SELECT column_name, data_type FROM information_schema.columns "
                    "WHERE table_schema = $1 AND table_name = $2 "
                    "ORDER BY ordinal_position"
                )
                rows = await conn.fetch(sql, schema, table)
                if rows:
                    table_ref = f"{_q(schema)}.{_q(table)}"
            else:
                sql = (
                    "SELECT table_schema, column_name, data_type FROM information_schema.columns "
                    "WHERE table_name = $1 AND table_schema NOT IN "
                    "('pg_catalog', 'information_schema') "
                    "ORDER BY table_schema, ordinal_position"
                )
                rows = await conn.fetch(sql, table)
                if rows:
                    table_ref = f"{_q(rows[0]['table_schema'])}.{_q(table)}"
                else:
                    continue

            if rows:
                col_names = [r["column_name"] for r in rows]

                # Map logical → actual
                cols: dict[str, str] = {}
                for logical, options in [
                    ("tenant", ["tenant_id", "business_id", "org_id"]),
                    ("domain", ["domain"]),
                    ("doc_type", ["doc_type", "type", "document_type"]),
                    ("period", ["period", "period_start", "period_label"]),
                    ("text", ["text", "content", "chunk_text", "body", "document"]),
                    ("metadata", ["metadata", "meta", "payload"]),
                    ("id", ["id", "chunk_id", "doc_id", "uuid"]),
                    ("created", ["created_at", "inserted_at", "timestamp"]),
                ]:
                    for opt in options:
                        if opt in col_names:
                            cols[logical] = opt
                            break

                return table_ref, cols

    return "", {}


def _qcol(name: str) -> str:
    """Double-quote a column identifier — belt-and-suspenders against reserved words."""
    return '"' + name.replace('"', '""') + '"'


# ── Query helpers ─────────────────────────────────────────────────────────────

async def fetch_all_chunks(conn, table: str, cols: dict[str, str],
                           tenant: str, domain: str) -> list[dict]:
    select_cols = []
    for logical in ("id", "tenant", "domain", "doc_type", "period",
                    "text", "metadata", "created"):
        if logical in cols:
            # Quote actual column name; alias to the logical name (bare, no quote)
            select_cols.append(f"{_qcol(cols[logical])} AS {logical}")
    if not select_cols:
        return []

    sql = (
        f"SELECT {', '.join(select_cols)} "
        f"FROM {table} "
        f"WHERE {_qcol(cols['tenant'])}::text = $1"
    )
    params = [tenant]
    if "domain" in cols:
        sql += f" AND {_qcol(cols['domain'])} = $2"
        params.append(domain)

    if "created" in cols:
        sql += f" ORDER BY {_qcol(cols['created'])} DESC"
    elif "id" in cols:
        sql += f" ORDER BY {_qcol(cols['id'])}"

    rows = await conn.fetch(sql, *params)
    return [dict(r) for r in rows]


# ── Analysis ──────────────────────────────────────────────────────────────────

MONTH_PATTERNS = {
    "january":   re.compile(r"\bjanuary\b|\bjan\b|\b2026-01\b|\b01/2026\b", re.I),
    "february":  re.compile(r"\bfebruary\b|\bfeb\b|\b2026-02\b|\b02/2026\b", re.I),
    "march":     re.compile(r"\bmarch\b|\bmar\b|\b2026-03\b|\b03/2026\b", re.I),
    "april":     re.compile(r"\bapril\b|\bapr\b|\b2026-04\b|\b04/2026\b", re.I),
    "may":       re.compile(r"\bmay\b|\b2026-05\b|\b05/2026\b", re.I),
    "june":      re.compile(r"\bjune\b|\bjun\b|\b2026-06\b|\b06/2026\b", re.I),
    "july":      re.compile(r"\bjuly\b|\bjul\b|\b2026-07\b|\b07/2026\b", re.I),
    "august":    re.compile(r"\baugust\b|\baug\b|\b2026-08\b|\b08/2026\b", re.I),
    "september": re.compile(r"\bseptember\b|\bsep\b|\b2026-09\b|\b09/2026\b", re.I),
    "october":   re.compile(r"\boctober\b|\boct\b|\b2026-10\b|\b10/2026\b", re.I),
    "november":  re.compile(r"\bnovember\b|\bnov\b|\b2026-11\b|\b11/2026\b", re.I),
    "december":  re.compile(r"\bdecember\b|\bdec\b|\b2026-12\b|\b12/2026\b", re.I),
}

# Step 7 update: amounts below are the specific values that appeared in the
# original April pollution (and were never in Q1 2026). Dropped the ambiguous
# 3,036.10 and 8,459.75 from the old list — those are real Q1 data (DemoLocation
# March revenue and LEO Stripe March revenue respectively) and were false-
# positive flags. Also dropped generic 500.00 which is too common.
SUSPICIOUS_APRIL_AMOUNTS = ["8,200", "8200", "7,500", "7500",
                            "8,270.75", "8270.75"]


def months_mentioned(text: str) -> set[str]:
    return {name for name, rx in MONTH_PATTERNS.items() if rx.search(text or "")}


def is_suspicious(text: str) -> list[str]:
    """Return list of suspicious signals found."""
    signals: list[str] = []
    if MONTH_PATTERNS["april"].search(text or ""):
        signals.append("april-mention")
    for amt in SUSPICIOUS_APRIL_AMOUNTS:
        if amt in (text or ""):
            signals.append(f"amount-{amt}")
    return signals


# ── Rendering ─────────────────────────────────────────────────────────────────

def render_schema(table: str, cols: dict[str, str]) -> None:
    print(c(f"\n── Schema ──────────────────────────────────────────────────────", C.CYAN))
    print(c(f"  Table: {table}", C.DIM))
    print(c(f"  Column mapping:", C.DIM))
    for logical, actual in cols.items():
        print(c(f"    {logical:10s} → {actual}", C.DIM))
    print()


def render_summary(chunks: list[dict], tenant: str, domain: str) -> None:
    print(c(f"── Summary for tenant={tenant} domain={domain!r} ───────────────", C.CYAN))
    print(c(f"  Total chunks: {len(chunks)}", C.BOLD))

    # By doc_type
    by_type = Counter(ch.get("doc_type") for ch in chunks)
    print(c(f"\n  By doc_type:", C.DIM))
    for t, n in by_type.most_common():
        print(c(f"    {str(t):30s} {n:>3}", C.DIM))

    # By period column (if set)
    by_period = Counter(ch.get("period") for ch in chunks if ch.get("period"))
    if by_period:
        print(c(f"\n  By period column:", C.DIM))
        for p, n in sorted(by_period.items()):
            print(c(f"    {str(p):30s} {n:>3}", C.DIM))

    # By months MENTIONED in chunk text (the real question)
    month_counts: Counter = Counter()
    for ch in chunks:
        text = (ch.get("text") or "")
        for m in months_mentioned(text):
            month_counts[m] += 1

    print(c(f"\n  Months MENTIONED in chunk text:", C.DIM))
    ordered = ["january", "february", "march", "april", "may", "june",
               "july", "august", "september", "october", "november", "december"]
    for m in ordered:
        if month_counts[m]:
            color = C.YELLOW if m not in ("january", "february", "march") else C.GREEN
            print(c(f"    {m:12s} {month_counts[m]:>3} chunks reference this month", color))
    print()


def render_chunks(chunks: list[dict], show_text: bool, grep: str | None) -> None:
    print(c(f"── Chunks {'(full text)' if show_text else '(first 200 chars)'} ─────────────────────", C.CYAN))

    for i, ch in enumerate(chunks, 1):
        text = (ch.get("text") or "").strip()

        if grep and grep.lower() not in text.lower():
            continue

        signals = is_suspicious(text)
        suspicious = bool(signals)

        header_color = C.RED if suspicious else C.DIM
        id_val = str(ch.get("id") or "")[:12]
        print(c(f"\n  [{i}] id={id_val!s} doc_type={ch.get('doc_type')!r} "
                f"period={ch.get('period')!r}", header_color))
        if suspicious:
            print(c(f"      ⚠  SUSPICIOUS: {signals}", C.RED + C.BOLD))
        if ch.get("metadata"):
            md = str(ch["metadata"])
            if len(md) > 200:
                md = md[:200] + " …"
            print(c(f"      metadata: {md}", C.DIM))

        if show_text:
            # print full text, indented
            for line in text.splitlines():
                print(c(f"        {line}", C.DIM))
        else:
            preview = text[:200].replace("\n", " ")
            if len(text) > 200:
                preview += " …"
            print(c(f"      {preview}", C.DIM))


def render_suspicious_report(chunks: list[dict]) -> list[dict]:
    """Print only the suspicious chunks — what's driving the April leak."""
    suspect = [ch for ch in chunks if is_suspicious(ch.get("text") or "")]

    print(c("\n══════════════════════════════════════════════════════════════════", C.CYAN))
    if not suspect:
        print(c("  ✅ NO SUSPICIOUS CHUNKS FOUND", C.GREEN + C.BOLD))
        print(c("     No chunks mention April or the specific amounts seen in the AI's"
                " bad answers.", C.GREEN))
        print(c("     → This means the AI is HALLUCINATING the April data.", C.GREEN))
        print(c("     → Fix: prompt engineering / add explicit 'only answer from provided docs'"
                " guard.", C.DIM))
    else:
        print(c(f"  🔴 {len(suspect)} SUSPICIOUS CHUNK(S) FOUND", C.RED + C.BOLD))
        print(c("     These chunks contain April data or specific amounts the AI"
                " has been citing.", C.RED))
        print(c("     → This means the AI is pulling REAL DATA from pgvector —"
                " not hallucinating.", C.YELLOW))
        print(c("     → Root cause is upstream: either the transformer created April docs,"
                " or old chunks remain from a prior embed run.", C.DIM))

        print(c(f"\n  Suspicious chunks:", C.BOLD))
        for ch in suspect:
            id_val = str(ch.get("id") or "")[:12]
            signals = is_suspicious(ch.get("text") or "")
            print(c(f"    • id={id_val} doc_type={ch.get('doc_type')!r} "
                    f"period={ch.get('period')!r}  [{','.join(signals)}]", C.RED))

    print(c("══════════════════════════════════════════════════════════════════\n", C.CYAN))
    return suspect


# ── Main ──────────────────────────────────────────────────────────────────────

async def main_async(args: argparse.Namespace) -> int:
    vec_pool = await PGPool.from_env(PGTarget.VECTOR)

    try:
        async with vec_pool.acquire() as conn:
            # 1. Discover schema
            print(c(f"\n▶ Discovering pgvector table...", C.BOLD))
            table, cols = await discover_table_and_columns(conn)
            if not table:
                print(c("  ❌ Could not find a pgvector table in the public schema.", C.RED))
                print(c("     Tried: embeddings, document_embeddings, vector_store,"
                        " rag_embeddings, documents", C.DIM))
                print(c("     Schemas in this DB:", C.DIM))
                schemas = await conn.fetch(
                    "SELECT DISTINCT table_schema FROM information_schema.tables "
                    "WHERE table_schema NOT IN ('pg_catalog', 'information_schema')"
                )
                for s in schemas:
                    print(c(f"       - {s['table_schema']}", C.DIM))
                return 2
            render_schema(table, cols)

            if "tenant" not in cols:
                print(c("  ❌ No tenant/business/org column found. Cannot scope query.", C.RED))
                return 2
            if "text" not in cols:
                print(c("  ❌ No text/content column found. Cannot inspect chunks.", C.RED))
                return 2

            # 2. Fetch chunks for each tenant
            all_results: dict[str, list[dict]] = {}
            for tenant in args.tenants:
                print(c(f"\n▶ Fetching chunks for tenant={tenant}...", C.BOLD))
                chunks = await fetch_all_chunks(conn, table, cols, tenant, args.domain)
                all_results[tenant] = chunks

                if not chunks:
                    print(c(f"  (no chunks for tenant={tenant} domain={args.domain!r})", C.YELLOW))
                    continue

                render_summary(chunks, tenant, args.domain)

                if args.show_text or args.grep:
                    render_chunks(chunks, args.show_text, args.grep)

                # Always run the suspicious report — it's the main diagnostic
                suspects = render_suspicious_report(chunks)

                # Optional full dump of suspects
                if args.dump_suspicious and suspects:
                    out = Path(args.dump_suspicious)
                    out.parent.mkdir(parents=True, exist_ok=True)
                    with open(out, "w", encoding="utf-8") as f:
                        f.write(f"# Suspicious pgvector chunks — tenant={tenant} "
                                f"domain={args.domain}\n")
                        f.write(f"# Generated by audit_pgvector_revenue.py\n\n")
                        for i, ch in enumerate(suspects, 1):
                            f.write(f"{'='*72}\n")
                            f.write(f"CHUNK {i}\n")
                            f.write(f"  id        = {ch.get('id')}\n")
                            f.write(f"  doc_type  = {ch.get('doc_type')}\n")
                            f.write(f"  period    = {ch.get('period')}\n")
                            f.write(f"  created   = {ch.get('created')}\n")
                            f.write(f"  signals   = {is_suspicious(ch.get('text') or '')}\n")
                            f.write(f"\nTEXT:\n{ch.get('text') or ''}\n\n")
                    print(c(f"  Suspicious chunks dumped → {out}", C.DIM))

    finally:
        await vec_pool.close()

    return 0


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Pgvector audit — dump revenue chunks to diagnose leaks"
    )
    parser.add_argument("--tenants", nargs="+", default=["40"],
                        help="Tenant IDs to audit (default: 40)")
    parser.add_argument("--tenant", dest="tenants", nargs=1,
                        help="Alias for --tenants with a single value")
    parser.add_argument("--domain", default="revenue",
                        help="Domain to filter by (default: revenue)")
    parser.add_argument("--show-text", action="store_true",
                        help="Print full chunk text for every chunk")
    parser.add_argument("--grep", default=None,
                        help="Only show chunks whose text contains this substring")
    parser.add_argument("--dump-suspicious", default=None,
                        help="Write the full text of suspicious chunks to this file")
    args = parser.parse_args()

    sys.exit(asyncio.run(main_async(args)))


if __name__ == "__main__":
    main()