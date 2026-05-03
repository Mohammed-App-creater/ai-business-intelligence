#!/usr/bin/env python3
"""
Phase 2.2 — Warehouse verification for org 40 (wh_appt_* row counts + samples).

Read-only. Uses WH_PG_* pool like scripts/embed_documents.py.
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_ROOT))

from dotenv import load_dotenv

load_dotenv(_ROOT / ".env", override=True)

BUSINESS_ID = 40

EXPECTED = {
    "wh_appt_monthly_summary": 45,
    "wh_appt_staff_breakdown": 79,
    "wh_appt_service_breakdown": 51,
    "wh_appt_staff_service_cross": 89,
}


async def main() -> None:
    from app.services.db.db_pool import PGPool, PGTarget

    pool = await PGPool.from_env(PGTarget.WAREHOUSE)
    try:
        tables = list(EXPECTED.keys())
        rows_out: list[tuple[str, int, str]] = []

        async with pool.acquire() as conn:
            for tbl in tables:
                cnt = await conn.fetchval(
                    f"SELECT COUNT(*) FROM {tbl} WHERE business_id = $1",
                    BUSINESS_ID,
                )
                sample = await conn.fetchrow(
                    f"""
                    SELECT *
                    FROM {tbl}
                    WHERE business_id = $1
                    ORDER BY period_start DESC
                    LIMIT 1
                    """,
                    BUSINESS_ID,
                )
                sample_str = ""
                if sample:
                    d = dict(sample)
                    keys = list(d.keys())[:12]
                    parts = [f"{k}={d[k]!r}" for k in keys]
                    sample_str = "; ".join(parts)
                    if len(d) > 12:
                        sample_str += " ..."
                rows_out.append((tbl, int(cnt or 0), sample_str))

        exp_total = sum(EXPECTED.values())
        actual_total = sum(r[1] for r in rows_out)

        print("")
        print("=" * 72)
        print(f"  Phase 2.2 Task A — Warehouse wh_appt_* counts (business_id={BUSINESS_ID})")
        print("=" * 72)
        print(f"  {'Table':<38} {'Count':>8} {'Expected':>10} {'Match':>8}")
        print("-" * 72)
        mismatch = False
        for tbl, cnt, _ in rows_out:
            exp = EXPECTED[tbl]
            ok = "yes" if cnt == exp else "NO"
            if cnt != exp:
                mismatch = True
            print(f"  {tbl:<38} {cnt:>8} {exp:>10} {ok:>8}")
        print("-" * 72)
        print(f"  {'TOTAL':<38} {actual_total:>8} {exp_total:>10} {'yes' if actual_total == exp_total else 'NO':>8}")
        print("=" * 72)

        if mismatch or actual_total != exp_total:
            print("\n  FLAG: Warehouse counts differ from embed-run expectations — doc pipeline")
            print("        row counts may not match warehouse upserts, or business_id differs.")
        else:
            print("\n  Counts match expected embed log totals.")

        print("\n  Sample row (first 12 columns, ORDER BY period_start DESC) per table:")
        print("-" * 72)
        for tbl, cnt, sample_str in rows_out:
            print(f"\n  [{tbl}] count={cnt}")
            print(f"    {sample_str or '(no rows)'}")

    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
