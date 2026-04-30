#!/usr/bin/env python3
"""
scripts/fetch_docs_by_id.py

Retrieve documents from the pgvector ``embeddings`` table by ``doc_id``.

Pass one or more doc_ids and the script prints each row's full payload
(chunk_text, doc_domain, doc_type, period_start, metadata, timestamps).

Examples
--------
    # single id
    PYTHONPATH=. python scripts/fetch_docs_by_id.py \\
        appointments:42:appt_staff_breakdown:6fcf527c6ab1

    # multiple ids as separate args
    PYTHONPATH=. python scripts/fetch_docs_by_id.py \\
        appointments:42:appt_staff_breakdown:6fcf527c6ab1 \\
        appointments:42:appt_monthly_summary:fc39b52804f6

    # JSON array (matches the format the retriever logs)
    PYTHONPATH=. python scripts/fetch_docs_by_id.py --json \\
        '["appointments:42:appt_staff_breakdown:6fcf527c6ab1",
          "appointments:42:appt_monthly_summary:fc39b52804f6"]'

    # restrict to a tenant (optional — doc_ids are globally unique already)
    PYTHONPATH=. python scripts/fetch_docs_by_id.py \\
        --tenant 42 appointments:42:appt_staff_breakdown:6fcf527c6ab1

    # only print chunk_text (handy for piping into the LLM context)
    PYTHONPATH=. python scripts/fetch_docs_by_id.py --text-only <id>
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path

# sys.path shim so `app.*` imports resolve when run directly
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


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Fetch pgvector docs by doc_id.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument(
        "doc_ids",
        nargs="*",
        help="One or more doc_ids (positional). Ignored if --json is used.",
    )
    p.add_argument(
        "--json",
        dest="json_ids",
        default=None,
        help='JSON array of doc_ids, e.g. \'["a","b"]\'.',
    )
    p.add_argument(
        "--tenant",
        default=None,
        help="Optional tenant_id filter (doc_ids are unique without it).",
    )
    p.add_argument(
        "--text-only",
        action="store_true",
        help="Print only chunk_text for each row (no headers / metadata).",
    )
    p.add_argument(
        "--as-json",
        action="store_true",
        help="Emit a JSON array of full rows instead of human-readable output.",
    )
    return p.parse_args()


def _resolve_ids(args: argparse.Namespace) -> list[str]:
    if args.json_ids:
        try:
            parsed = json.loads(args.json_ids)
        except json.JSONDecodeError as e:
            print(f"❌ --json could not be parsed: {e}", file=sys.stderr)
            sys.exit(2)
        if not isinstance(parsed, list) or not all(isinstance(x, str) for x in parsed):
            print("❌ --json must be a JSON array of strings.", file=sys.stderr)
            sys.exit(2)
        return parsed
    return list(args.doc_ids)


async def _fetch(
    pool: PGPool, doc_ids: list[str], tenant_id: str | None
) -> list[dict]:
    table = settings.VECTOR_TABLE_NAME
    sql = f"""
SELECT
    id::text       AS id,
    tenant_id,
    doc_id,
    doc_domain,
    doc_type,
    chunk_text,
    period_start,
    metadata,
    created_at,
    updated_at
FROM {table}
WHERE doc_id = ANY($1::text[])
""".strip()
    params: list = [doc_ids]
    if tenant_id is not None:
        sql += "\n  AND tenant_id = $2"
        params.append(tenant_id)
    sql += "\nORDER BY doc_id"

    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, *params)
    return [dict(r) for r in rows]


def _row_to_jsonable(row: dict) -> dict:
    out: dict = {}
    for k, v in row.items():
        if k == "metadata":
            if v is None:
                out[k] = None
            elif isinstance(v, (dict, list)):
                out[k] = v
            else:
                # asyncpg returns JSON columns as str when no codec is set
                try:
                    out[k] = json.loads(v)
                except (TypeError, ValueError):
                    out[k] = v
        elif hasattr(v, "isoformat"):
            out[k] = v.isoformat()
        else:
            out[k] = v
    return out


def _print_human(rows: list[dict], requested: list[str]) -> None:
    found = {r["doc_id"] for r in rows}
    for doc_id in requested:
        matched = [r for r in rows if r["doc_id"] == doc_id]
        if not matched:
            print(f"\n=== {doc_id} ===")
            print("  (not found)")
            continue
        for r in matched:
            print(f"\n=== {r['doc_id']} ===")
            print(f"  tenant_id    : {r['tenant_id']}")
            print(f"  doc_domain   : {r['doc_domain']}")
            print(f"  doc_type     : {r['doc_type']}")
            print(f"  period_start : {r['period_start']}")
            print(f"  created_at   : {r['created_at']}")
            print(f"  updated_at   : {r['updated_at']}")
            meta = r["metadata"]
            if isinstance(meta, str):
                try:
                    meta = json.loads(meta)
                except (TypeError, ValueError):
                    pass
            print(f"  metadata     : {json.dumps(meta, default=str, indent=2)}")
            print("  chunk_text   :")
            print("  " + "-" * 60)
            for line in (r["chunk_text"] or "").splitlines() or [""]:
                print(f"  {line}")
            print("  " + "-" * 60)

    missing = [d for d in requested if d not in found]
    if missing:
        print(f"\nMissing {len(missing)}/{len(requested)} doc_id(s):")
        for m in missing:
            print(f"  - {m}")


async def main() -> int:
    args = _parse_args()
    doc_ids = _resolve_ids(args)
    if not doc_ids:
        print("❌ No doc_ids provided. Pass them positionally or via --json.",
              file=sys.stderr)
        return 2

    pool = await PGPool.from_env(PGTarget.VECTOR)
    try:
        rows = await _fetch(pool, doc_ids, args.tenant)
    finally:
        await pool.close()

    if args.text_only:
        for doc_id in doc_ids:
            for r in rows:
                if r["doc_id"] == doc_id:
                    print(r["chunk_text"] or "")
                    print()  # blank line between chunks
        return 0 if rows else 1

    if args.as_json:
        print(json.dumps([_row_to_jsonable(r) for r in rows], indent=2,
                         default=str))
        return 0 if rows else 1

    _print_human(rows, doc_ids)
    return 0 if rows else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
