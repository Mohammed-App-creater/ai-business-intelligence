#!/usr/bin/env python3
"""
Phase 2.2 — pgvector embeddings verification for tenant 40 (appointments chunks).

Read-only. Uses VEC_PG_* pool + VECTOR_TABLE_NAME from settings.
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_ROOT))

import os

from dotenv import load_dotenv

load_dotenv(_ROOT / ".env", override=True)

# Same default as app/core/config.Settings — avoid importing pydantic in thin scripts.
VECTOR_TABLE = os.environ.get("VECTOR_TABLE_NAME", "embeddings")

TENANT_STR = "40"
EXPECTED_APPT = {
    "appt_monthly_summary": 45,
    "appt_staff_breakdown": 79,
    "appt_service_breakdown": 51,
    "appt_staff_service_cross": 89,
}


async def main() -> None:
    from app.services.db.db_pool import PGPool, PGTarget

    table = VECTOR_TABLE
    if not table.isidentifier():
        raise SystemExit(f"VECTOR_TABLE_NAME must be a simple identifier, got {table!r}")
    pool = await PGPool.from_env(PGTarget.VECTOR)

    try:
        async with pool.acquire() as conn:
            # tenant_id is TEXT (infra/init_db.sql)
            n_tenant = await conn.fetchval(
                f"SELECT COUNT(*) FROM {table} WHERE tenant_id = $1",
                TENANT_STR,
            )

            if not n_tenant:
                distinct = await conn.fetch(
                    f"SELECT DISTINCT tenant_id FROM {table} ORDER BY 1 LIMIT 50"
                )
                print("\n  No rows for tenant_id='40'. Distinct tenant_id sample:")
                for r in distinct:
                    print(f"    {r['tenant_id']!r}")
                print("\n  FLAG: Use matching tenant_id string from list above.")
                return

            print(f"\n  Rows with tenant_id = '{TENANT_STR}': {n_tenant}")

            by_type_all = await conn.fetch(
                f"""
                SELECT doc_type, COUNT(*) AS n
                FROM {table}
                WHERE tenant_id = $1
                GROUP BY doc_type
                ORDER BY doc_type
                """,
                TENANT_STR,
            )

            by_type_appt = await conn.fetch(
                f"""
                SELECT doc_type, COUNT(*) AS n
                FROM {table}
                WHERE tenant_id = $1 AND doc_domain = 'appointments'
                GROUP BY doc_type
                ORDER BY doc_type
                """,
                TENANT_STR,
            )

            samples = await conn.fetch(
                f"""
                SELECT doc_id, doc_type, period_start, LEFT(chunk_text, 200) AS snippet
                FROM {table}
                WHERE tenant_id = $1 AND doc_domain = 'appointments'
                LIMIT 3
                """,
                TENANT_STR,
            )

        appt_total = sum(int(r["n"]) for r in by_type_appt)
        exp_total = sum(EXPECTED_APPT.values())

        print("")
        print("=" * 72)
        print(f"  Phase 2.2 Task B — Vector store {table!r} (tenant_id='{TENANT_STR}')")
        print("=" * 72)
        print("\n  All doc_types (tenant_id only):")
        print(f"  {'doc_type':<36} {'n':>8}")
        print("  " + "-" * 46)
        for r in by_type_all:
            print(f"  {r['doc_type']:<36} {r['n']:>8}")

        print("\n  Appointments domain only (doc_domain = 'appointments'):")
        print(f"  {'doc_type':<36} {'n':>8} {'expected':>10} {'match':>8}")
        print("  " + "-" * 64)
        mismatch = False
        found_types = {r["doc_type"]: int(r["n"]) for r in by_type_appt}
        for dt, exp in sorted(EXPECTED_APPT.items()):
            n = found_types.get(dt, 0)
            ok = "yes" if n == exp else "NO"
            if n != exp:
                mismatch = True
            print(f"  {dt:<36} {n:>8} {exp:>10} {ok:>8}")
        for dt in sorted(found_types.keys()):
            if dt not in EXPECTED_APPT:
                print(f"  {dt:<36} {found_types[dt]:>8} {'(extra)':>10} {'':>8}")

        print("  " + "-" * 64)
        print(f"  {'TOTAL (appointments)':<36} {appt_total:>8} {exp_total:>10} {'yes' if appt_total == exp_total and not mismatch else 'NO':>8}")
        print("=" * 72)

        if mismatch or appt_total != exp_total:
            print("\n  FLAG: Vector counts differ from warehouse/embed expectations.")

        print("\n  Sample rows (doc_domain = appointments, LIMIT 3):")
        print("-" * 72)
        for i, r in enumerate(samples, 1):
            print(f"\n  [{i}] doc_id={r['doc_id']}")
            print(f"      doc_type={r['doc_type']}  period_start={r['period_start']}")
            snip = (r["snippet"] or "").replace("\n", " ")
            print(f"      chunk[:200]={snip!r}")

    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
