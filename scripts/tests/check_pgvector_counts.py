#!/usr/bin/env python3
"""
scripts/tests/check_pgvector_counts.py

Pgvector sanity check — uses the app's own VectorStore + PGPool so we
inherit the correct table name, DSN, and env var conventions.

Run:
    PYTHONPATH=. python scripts/tests/check_pgvector_counts.py
    PYTHONPATH=. python scripts/tests/check_pgvector_counts.py --tenants 40 42
    PYTHONPATH=. python scripts/tests/check_pgvector_counts.py --domain revenue
"""
from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

# sys.path shim (same as the transformer test)
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


async def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--tenants",
        nargs="+",
        default=["40", "42"],
        help="Tenant IDs to check (default: 40 42)",
    )
    parser.add_argument(
        "--domain",
        default="revenue",
        help="Domain filter (default: revenue)",
    )
    args = parser.parse_args()

    table = settings.VECTOR_TABLE_NAME
    print(f"\nVector table (from settings.VECTOR_TABLE_NAME): {table!r}")
    print(f"Checking tenants={args.tenants} domain={args.domain!r}\n")

    vec_pool = await PGPool.from_env(PGTarget.VECTOR)

    try:
        async with vec_pool.acquire() as conn:
            # First — does the table exist? Discover table name if not.
            exists = await conn.fetchval(
                "SELECT to_regclass($1)",
                f"public.{table}",
            )
            if not exists:
                print(f"  ❌ Table 'public.{table}' does not exist in the vector DB.")
                print("     Checking what tables DO exist...")
                rows = await conn.fetch(
                    "SELECT tablename FROM pg_tables "
                    "WHERE schemaname = 'public' ORDER BY tablename"
                )
                if not rows:
                    print("     (no user tables in public schema)")
                else:
                    print("     Tables available:")
                    for r in rows:
                        print(f"       - {r['tablename']}")
                    print()
                    print("     → Update settings.VECTOR_TABLE_NAME to the right name.")
                return 1

            # Table exists. Show schema briefly.
            cols = await conn.fetch(
                "SELECT column_name, data_type FROM information_schema.columns "
                "WHERE table_schema='public' AND table_name=$1 "
                "ORDER BY ordinal_position",
                table,
            )
            print(f"  Table '{table}' has {len(cols)} columns:")
            for col in cols:
                print(f"    - {col['column_name']:25s}  {col['data_type']}")
            print()

            # Now the actual counts. Assume business_id + domain columns exist.
            # If your schema uses different column names, we'll error cleanly.
            col_names = {c["column_name"] for c in cols}
            tenant_col = None
            for candidate in ("business_id", "tenant_id", "org_id"):
                if candidate in col_names:
                    tenant_col = candidate
                    break
            if not tenant_col:
                print("  ❌ No column matching business_id / tenant_id / org_id found.")
                return 1

            domain_col = "domain" if "domain" in col_names else None
            print(f"  Using tenant column: {tenant_col!r}")
            print(f"  Using domain column: {domain_col!r}\n")

            # Total rows per tenant
            print("  Row counts per tenant:")
            for t in args.tenants:
                total = await conn.fetchval(
                    f"SELECT COUNT(*) FROM {table} WHERE {tenant_col}::text = $1",
                    t,
                )
                print(f"    tenant {t}: {total} total rows")

                if domain_col:
                    rev = await conn.fetchval(
                        f"SELECT COUNT(*) FROM {table} "
                        f"WHERE {tenant_col}::text = $1 AND {domain_col} = $2",
                        t,
                        args.domain,
                    )
                    print(f"              {rev} rows in domain={args.domain!r}")

            # Cross-tenant canary — are there any rows where tenant is NOT one of
            # our tracked tenants? (Would be benign, just informational.)
            print()
            other = await conn.fetchval(
                f"SELECT COUNT(DISTINCT {tenant_col}) FROM {table}"
            )
            print(f"  Total distinct tenants in '{table}': {other}")

    finally:
        await vec_pool.close()

    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))