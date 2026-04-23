#!/usr/bin/env python3
"""
scripts/tests/clear_revenue_chunks.py

Deletes all revenue-domain chunks in pgvector for one or more tenants.

Built as a one-off cleanup ahead of a clean re-embed. Uses the app's
own PGPool so schema/table/column names come from settings — no guessing.

SAFETY:
  - Dry-run by default. You must pass --confirm to actually delete.
  - Scoped to domain='revenue' and the specified tenants only.
  - Prints count before and after so you see exactly what happened.

Run:
    # See what WOULD be deleted (safe, dry run)
    PYTHONPATH=. python scripts/tests/clear_revenue_chunks.py --tenant 40

    # Actually delete
    PYTHONPATH=. python scripts/tests/clear_revenue_chunks.py --tenant 40 --confirm

    # Multiple tenants
    PYTHONPATH=. python scripts/tests/clear_revenue_chunks.py --tenants 40 42 --confirm
"""
from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

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


class C:
    RESET, BOLD, DIM = "\033[0m", "\033[1m", "\033[2m"
    RED, GREEN, YELLOW, CYAN = "\033[31m", "\033[32m", "\033[33m", "\033[36m"


def c(t: str, col: str) -> str:
    return f"{col}{t}{C.RESET}"


def _q(ident: str) -> str:
    """Double-quote a Postgres identifier."""
    return '"' + ident.replace('"', '""') + '"'


# Known doc_type values per domain — used when the embeddings table has no
# 'domain' column and we must infer domain membership from doc_type.
# Keep in sync with etl/transforms/*_etl.py builders.
DOC_TYPES_BY_DOMAIN = {
    "revenue": [
        "monthly_summary",
        "payment_type_breakdown",
        "staff_revenue",
        "location_revenue",
        "promo_impact",
        "failed_refunds",
        # Step 7 additions — keep in sync with revenue_etl.py:
        "trend_summary",
        "tips_and_extras",
    ],
    # Add more domains here as they come online.
}


async def discover(conn) -> tuple[str, dict[str, str]]:
    """Find the table + column mapping. Mirrors audit_pgvector_revenue.py."""
    candidates = [
        settings.VECTOR_TABLE_NAME,
        "embeddings", "document_embeddings", "vector_store",
        "rag_embeddings", "documents",
    ]
    for schema in ("public", None):
        for table in candidates:
            if schema:
                rows = await conn.fetch(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_schema=$1 AND table_name=$2",
                    schema, table,
                )
                if rows:
                    table_ref = f"{_q(schema)}.{_q(table)}"
            else:
                rows = await conn.fetch(
                    "SELECT table_schema, column_name FROM information_schema.columns "
                    "WHERE table_name=$1 AND table_schema NOT IN "
                    "('pg_catalog', 'information_schema')",
                    table,
                )
                if rows:
                    table_ref = f"{_q(rows[0]['table_schema'])}.{_q(table)}"
                else:
                    continue

            if rows:
                names = {r["column_name"] for r in rows}
                cols: dict[str, str] = {}
                for logical, options in [
                    ("tenant", ["tenant_id", "business_id", "org_id"]),
                    ("domain", ["domain"]),
                    ("doc_type", ["doc_type", "type", "document_type"]),
                ]:
                    for opt in options:
                        if opt in names:
                            cols[logical] = opt
                            break
                return table_ref, cols

    return "", {}


async def main_async(args: argparse.Namespace) -> int:
    vec_pool = await PGPool.from_env(PGTarget.VECTOR)

    try:
        async with vec_pool.acquire() as conn:
            table, cols = await discover(conn)

            if not table:
                print(c("❌ Could not find the pgvector table.", C.RED))
                return 2
            if "tenant" not in cols:
                print(c("❌ No tenant column found on the pgvector table.", C.RED))
                return 2

            # ── Figure out how to scope by domain ─────────────────────────────
            # Preferred: use the 'domain' column if present.
            # Fallback: if there's a doc_type column, match against the known
            # doc_types for the target domain.
            scope_sql: str           # SQL fragment after tenant predicate
            scope_params: list       # additional params to append

            if "domain" in cols:
                scope_sql    = f"AND {_q(cols['domain'])} = $2"
                scope_params = [args.domain]
                scope_desc   = f"{cols['domain']} = {args.domain!r}"
            elif "doc_type" in cols:
                doc_types = DOC_TYPES_BY_DOMAIN.get(args.domain)
                if not doc_types:
                    print(c(f"❌ Table has no 'domain' column, and no doc_type "
                            f"mapping defined for domain={args.domain!r}.", C.RED))
                    print(c(f"   Known mappings: {list(DOC_TYPES_BY_DOMAIN.keys())}", C.DIM))
                    return 2
                scope_sql    = f"AND {_q(cols['doc_type'])} = ANY($2)"
                scope_params = [doc_types]
                scope_desc   = f"{cols['doc_type']} IN {doc_types}"
            else:
                print(c("❌ No domain or doc_type column — cannot scope deletion.", C.RED))
                return 2

            print(c(f"\n  Table: {table}", C.DIM))
            print(c(f"  Tenant column: {cols['tenant']}", C.DIM))
            print(c(f"  Scope: {scope_desc}", C.DIM))
            print()

            total_affected = 0

            for tenant in args.tenants:
                # Count before
                before = await conn.fetchval(
                    f"SELECT COUNT(*) FROM {table} "
                    f"WHERE {_q(cols['tenant'])}::text = $1 {scope_sql}",
                    tenant, *scope_params,
                )

                print(c(f"▶ tenant={tenant} domain={args.domain!r}: "
                        f"{before} chunks", C.BOLD))

                if before == 0:
                    print(c("  (nothing to delete)", C.DIM))
                    continue

                if not args.confirm:
                    print(c(f"  [DRY RUN] Would DELETE {before} chunks. "
                            f"Pass --confirm to actually delete.", C.YELLOW))
                    continue

                # Actually delete
                result = await conn.execute(
                    f"DELETE FROM {table} "
                    f"WHERE {_q(cols['tenant'])}::text = $1 {scope_sql}",
                    tenant, *scope_params,
                )
                deleted = int(result.split()[-1]) if result.startswith("DELETE") else 0
                total_affected += deleted
                print(c(f"  ✅ Deleted {deleted} chunks", C.GREEN))

                # Sanity check count after
                after = await conn.fetchval(
                    f"SELECT COUNT(*) FROM {table} "
                    f"WHERE {_q(cols['tenant'])}::text = $1 {scope_sql}",
                    tenant, *scope_params,
                )
                if after != 0:
                    print(c(f"  ⚠  Unexpected: {after} chunks remain after delete. "
                            f"Investigate.", C.RED))

            print()
            if args.confirm:
                print(c(f"  TOTAL DELETED: {total_affected} chunks", C.BOLD + C.GREEN))
            else:
                print(c(f"  (Dry run — nothing changed. Rerun with --confirm to delete.)",
                        C.YELLOW))
            print()

    finally:
        await vec_pool.close()

    return 0


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Clear revenue chunks from pgvector for one or more tenants"
    )
    parser.add_argument("--tenants", nargs="+", default=None)
    parser.add_argument("--tenant",  default=None,
                        help="Single tenant shortcut (alternative to --tenants)")
    parser.add_argument("--domain",  default="revenue",
                        help="Domain to clear (default: revenue)")
    parser.add_argument("--confirm", action="store_true",
                        help="Actually delete. Without this, dry run only.")
    args = parser.parse_args()

    # Normalize tenant inputs
    tenants: list[str] = []
    if args.tenant:
        tenants.append(str(args.tenant))
    if args.tenants:
        tenants.extend(str(t) for t in args.tenants)
    if not tenants:
        parser.error("Provide --tenant <ID> or --tenants <ID> <ID> ...")
    args.tenants = tenants

    sys.exit(asyncio.run(main_async(args)))


if __name__ == "__main__":
    main()